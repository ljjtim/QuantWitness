using System.Buffers.Binary;
using System.Globalization;
using System.Net.Sockets;
using System.Text.Json;

namespace QuantWitness.LeanInput;

// 数据馈送与规则消费者共用一个连接；仅物理读取提前，不向策略交付预读数据。
public sealed class DirectDailyStreamStore : IBarStore
{
    private static readonly Dictionary<string, DirectDailyStreamStore> Stores = new();
    private readonly object _gate = new();
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly string[] _symbols;
    private readonly HashSet<DateTime> _dates;
    private readonly Dictionary<DateTime, (Dictionary<string, BarRow[]> Rows, JsonElement[] Rules, JsonElement Signals)> _cache = new();
    private readonly Dictionary<string, int> _loads = new();
    private long _rows;
    public string Frequency { get; }
    public decimal PriceDivisor { get; }
    public long EmittedRows { get; set; }

    public static DirectDailyStreamStore Open(string manifest)
    {
        lock (Stores)
        {
            var path = Path.GetFullPath(manifest);
            if (!Stores.TryGetValue(path, out var store)) Stores.Add(path, store = new(path));
            return store;
        }
    }

    private DirectDailyStreamStore(string manifest)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(manifest));
        var spec = doc.RootElement;
        Frequency = spec.GetProperty("frequency").GetString();
        var scale = spec.GetProperty("price_scale").GetInt32();
        if (Frequency is not ("1d" or "1m") || scale is not (2 or 3))
            throw new NotSupportedException("直接输入仅支持二位或三位价格的日线或一分钟");
        PriceDivisor = (decimal)Math.Pow(10, scale);
        _symbols = spec.GetProperty("symbols").EnumerateArray().Select(x => x.GetString()).ToArray();
        _dates = spec.GetProperty("dates").EnumerateArray().Select(x => DateTime.ParseExact(x.GetString(),
            "yyyy-MM-dd", CultureInfo.InvariantCulture)).ToHashSet();
        var endpoint = spec.GetProperty("endpoint");
        if (endpoint.GetProperty("host").GetString() != "127.0.0.1")
            throw new ArgumentException("直接输入连接必须为本机地址");
        _client = new TcpClient { NoDelay = true };
        _client.Connect("127.0.0.1", endpoint.GetProperty("port").GetInt32());
        _stream = _client.GetStream();
    }

    internal BarRow[] Rows(DateTime date, string ticker)
    {
        lock (_gate)
        {
            Load(date.Date);
            return _cache[date.Date].Rows.TryGetValue(ticker, out var rows) ? rows : Array.Empty<BarRow>();
        }
    }

    BarRow[] IBarStore.GetRows(DateTime date, string ticker) => Rows(date, ticker);

    public JsonElement[] Rules(DateTime date)
    {
        lock (_gate)
        {
            Load(date.Date);
            return _cache[date.Date].Rules;
        }
    }

    public string Signals(DateTime date)
    {
        lock (_gate)
        {
            Load(date.Date);
            return _cache[date.Date].Signals.GetRawText();
        }
    }

    private void Load(DateTime date)
    {
        if (_cache.ContainsKey(date)) return;
        if (!_dates.Contains(date)) throw new InvalidDataException("直接输入日期不在冻结范围内");
        var day = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (_loads.ContainsKey(day)) throw new InvalidDataException("直接输入不能回读已释放日期");
        var request = JsonSerializer.SerializeToUtf8Bytes(new { date = day });
        Span<byte> prefix = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(prefix, request.Length);
        _stream.Write(prefix); _stream.Write(request);
        _stream.ReadExactly(prefix);
        var length = BinaryPrimitives.ReadInt32LittleEndian(prefix);
        if (length <= 0) throw new InvalidDataException("直接输入响应头长度无效");
        var header = new byte[length];
        _stream.ReadExactly(header);
        using var document = JsonDocument.Parse(header);
        var spec = document.RootElement;
        if (spec.TryGetProperty("error", out var error) && error.ValueKind != JsonValueKind.Null)
            throw new InvalidDataException($"直接输入生产者失败：{error.GetString()}");
        if (spec.GetProperty("date").GetString() != day || spec.GetProperty("frequency").GetString() != Frequency)
            throw new InvalidDataException("直接输入响应日期或频率不匹配");
        if (Frequency == "1d" && (!spec.TryGetProperty("availability", out var availability)
            || availability.GetString() != "next_session_open"))
            throw new InvalidDataException("日线响应必须声明next_session_open可见合同");
        var count = spec.GetProperty("row_count").GetInt32();
        if (count < 0) throw new InvalidDataException("直接输入行数不能为负");
        var collected = new Dictionary<string, List<BarRow>>();
        string previousCode = null;
        DateTime previousStart = default;
        Span<byte> binary = stackalloc byte[60];
        for (var i = 0; i < count; i++)
        {
            _stream.ReadExactly(binary);
            var index = BinaryPrimitives.ReadInt32LittleEndian(binary);
            if (index < 0 || index >= _symbols.Length) throw new InvalidDataException("直接输入证券索引越界");
            var code = _symbols[index];
            var start = Local(BinaryPrimitives.ReadInt64LittleEndian(binary[4..]));
            var end = Local(BinaryPrimitives.ReadInt64LittleEndian(binary[12..]));
            var open = BinaryPrimitives.ReadInt64LittleEndian(binary[20..]);
            var high = BinaryPrimitives.ReadInt64LittleEndian(binary[28..]);
            var low = BinaryPrimitives.ReadInt64LittleEndian(binary[36..]);
            var close = BinaryPrimitives.ReadInt64LittleEndian(binary[44..]);
            var volume = BinaryPrimitives.ReadInt64LittleEndian(binary[52..]);
            if (start.Date != date || end.Date != date || end <= start
                || (Frequency == "1m" && end - start != TimeSpan.FromMinutes(1))
                || (Frequency == "1d" && (start.TimeOfDay != TimeSpan.FromHours(9.5) || end.TimeOfDay != TimeSpan.FromHours(15)))
                || open <= 0 || close <= 0 || low <= 0 || low > Math.Min(open, close)
                || high < Math.Max(open, close) || volume < 0)
                throw new InvalidDataException("直接输入时间或OHLCV无效");
            if (previousCode != null && (StringComparer.Ordinal.Compare(previousCode, code) > 0
                || previousCode == code && previousStart >= start))
                throw new InvalidDataException("直接输入未按证券、开始时间稳定排序或存在重复");
            if (Frequency == "1d" && previousCode == code) throw new InvalidDataException("同一证券每天只能有一条日线");
            if (!collected.TryGetValue(code.Split('.')[0], out var rows)) collected.Add(code.Split('.')[0], rows = new());
            rows.Add(new BarRow(start, open, high, low, close, volume, end));
            previousCode = code; previousStart = start;
        }
        _cache.Add(date, (collected.ToDictionary(x => x.Key, x => x.Value.ToArray()),
            spec.GetProperty("rules").EnumerateArray().Select(x => x.Clone()).ToArray(),
            spec.GetProperty("signals").Clone()));
        _loads.Add(day, 1); _rows += count;
        // 同步器最多领先当前时钟一个交易日；数组由正在消费它的证券枚举器持有。
        foreach (var old in _cache.Keys.Order().Take(Math.Max(0, _cache.Count - 2)).ToArray()) _cache.Remove(old);
    }

    private static DateTime Local(long microseconds) => DateTime.SpecifyKind(
        DateTime.UnixEpoch.AddTicks(checked(microseconds * 10)).AddHours(8), DateTimeKind.Unspecified);

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(new
    {
        source_kind = "direct_daily_stream", frequency = Frequency, source_rows_read = _rows,
        selected_rows = _rows, emitted_rows = EmittedRows, date_load_counts = _loads,
        intermediate_market_files = 0, intermediate_rule_files = 0
    }));

    public void Clear()
    {
        lock (_gate) { _cache.Clear(); _stream.Dispose(); _client.Dispose(); }
        lock (Stores)
            foreach (var key in Stores.Where(x => ReferenceEquals(x.Value, this)).Select(x => x.Key).ToArray()) Stores.Remove(key);
    }
}
