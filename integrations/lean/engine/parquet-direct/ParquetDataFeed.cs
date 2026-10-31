using System.Diagnostics;
using System.Text.Json;
using ParquetSharp;
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.Results;
using QuantConnect.Packets;
using QuantWitness.Lean.China;

namespace QuantWitness.LeanInput;

public sealed class ParquetDataFeed : FileSystemDataFeed
{
    private IBarStore _store;
    private IAlgorithm _algorithm;
    private string _output;

    public override void Initialize(IAlgorithm algorithm, AlgorithmNodePacket job, IResultHandler resultHandler,
        IMapFileProvider mapFileProvider, IFactorFileProvider factorFileProvider, IDataProvider dataProvider,
        IDataFeedSubscriptionManager subscriptionManager, IDataFeedTimeProvider dataFeedTimeProvider,
        IDataChannelProvider dataChannelProvider)
    {
        base.Initialize(algorithm, job, resultHandler, mapFileProvider, factorFileProvider, dataProvider,
            subscriptionManager, dataFeedTimeProvider, dataChannelProvider);
        _algorithm = algorithm;
        _output = Config.Get("results-destination-folder");
        var manifest = Config.Get("parquet-input-manifest");
        using var spec = JsonDocument.Parse(File.ReadAllText(manifest));
        _store = spec.RootElement.TryGetProperty("source_kind", out var kind) && kind.GetString() == "direct_daily_stream"
            ? DirectDailyStreamStore.Open(manifest)
            : kind.ValueKind == JsonValueKind.String && kind.GetString() == "jqdata_monthly_parquet" ? new MonthlyParquetStore(manifest) : new DailyParquetStore(manifest);
    }

    public override Subscription CreateSubscription(SubscriptionRequest request)
    {
        // 用户定义证券集合仍由上游产生，实际股票行情使用冻结的直接输入。
        if (request.IsUniverseSubscription) return base.CreateSubscription(request);
        var config = request.Configuration;
        if (_algorithm.IsWarmingUp || config.Type != typeof(TradeBar) || config.SecurityType != SecurityType.Equity
            || config.Symbol.ID.Market != "jqchina" || config.Resolution != (_store is DirectDailyStreamStore direct && direct.Frequency == "1d" ? Resolution.Daily : Resolution.Minute)
            || config.FillDataForward || config.DataNormalizationMode != DataNormalizationMode.Raw)
            throw new NotSupportedException("直接读取仅支持无预热、无填充的Raw股票日线或一分钟行情");
        // 同步枚举器让上游同步器推进各证券，避免后台预取把所有日期同时保留。
        return SubscriptionUtils.Create(request, Read(request).GetEnumerator(), _algorithm.Settings.DailyPreciseEndTime);
    }

    private IEnumerable<BaseData> Read(SubscriptionRequest request)
    {
        if (_store is DirectDailyStreamStore direct && direct.Frequency == "1d")
        {
            foreach (var data in ReadDaily(request)) yield return data;
            yield break;
        }
        var ticker = request.Configuration.Symbol.Value;
        foreach (var date in request.TradableDaysInDataTimeZone)
        {
            var rows = _store.GetRows(date.Date, ticker);
            foreach (var row in rows)
            {
                var end = row.End ?? row.Start.AddMinutes(1);
                if (end < request.StartTimeLocal || end > request.EndTimeLocal) continue;
                _store.EmittedRows++;
                yield return new TradeBar(row.Start, request.Configuration.Symbol,
                    row.Open / _store.PriceDivisor, row.High / _store.PriceDivisor,
                    row.Low / _store.PriceDivisor, row.Close / _store.PriceDivisor,
                    row.Volume, end - row.Start);
            }
            if (_store is DirectDailyStreamStore)
                yield return new SessionBoundary(date.Date.AddHours(15), request.Configuration.Symbol);
        }
    }

    private IEnumerable<BaseData> ReadDaily(SubscriptionRequest request)
    {
        var symbol = request.Configuration.Symbol;
        BarRow[] previous = Array.Empty<BarRow>();
        foreach (var date in request.TradableDaysInDataTimeZone)
        {
            var rows = _store.GetRows(date.Date, symbol.Value);
            var openAt = date.Date.AddHours(9.5);
            // 完整D日OHLCV只能在D+1开盘进入策略；尾日日线没有后续session，不交付。
            foreach (var row in previous)
            {
                var bar = new TradeBar(row.Start, symbol, row.Open / _store.PriceDivisor,
                    row.High / _store.PriceDivisor, row.Low / _store.PriceDivisor, row.Close / _store.PriceDivisor,
                    row.Volume, row.End.Value - row.Start);
                _store.EmittedRows++;
                yield return new CompletedDailyData(bar, openAt);
            }
            foreach (var row in rows) yield return new DailyOpenBar(openAt, symbol, row.Open / _store.PriceDivisor);
            if (rows.Length == 0 && previous.Length > 0)
            {
                var row = previous[^1];
                yield return new DailyCloseMark(openAt, row.End.Value, symbol, row.Close / _store.PriceDivisor);
            }
            // 为开盘后余量撤销提供独立时钟；不把内部执行phase解释为策略分钟行情。
            yield return new SessionBoundary(openAt.AddSeconds(1), symbol);
            yield return new SessionBoundary(date.Date.AddHours(15), symbol);
            previous = rows;
        }
    }

    public override void Exit()
    {
        if (_store != null)
        {
            _store.Save(Path.Combine(_output, "parquet-read.json"));
            _store.Clear();
        }
        base.Exit();
    }
}

internal readonly record struct BarRow(DateTime Start, long Open, long High, long Low, long Close, long Volume, DateTime? End = null);

internal interface IBarStore
{
    decimal PriceDivisor { get; }
    long EmittedRows { get; set; }
    BarRow[] GetRows(DateTime date, string ticker);
    void Save(string path);
    void Clear();
}

internal sealed class DailyParquetStore : IBarStore
{
    private readonly Dictionary<DateTime, string[]> _files = new();
    private readonly Dictionary<string, string> _selected = new();
    private readonly Dictionary<DateTime, Dictionary<string, BarRow[]>> _cache = new();
    private readonly List<object> _reads = new();
    private readonly Dictionary<DateTime, int> _loadCounts = new();
    private long _sourceRows;
    private long _selectedRows;
    public long EmittedRows { get; set; }
    public decimal PriceDivisor { get; }

    public DailyParquetStore(string manifestPath)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(manifestPath));
        var spec = doc.RootElement;
        var scale = spec.GetProperty("price_scale").GetInt32();
        if (scale < 0 || scale > 8) throw new ArgumentException("不支持的执行价格精度");
        PriceDivisor = 1m;
        for (var i = 0; i < scale; i++) PriceDivisor *= 10m;
        foreach (var symbol in spec.GetProperty("symbols").EnumerateArray())
        {
            var instrument = symbol.GetString();
            _selected.Add(instrument, instrument.Split('.')[0]);
        }
        foreach (var entry in spec.GetProperty("partitions").EnumerateObject())
            _files.Add(DateTime.ParseExact(entry.Name, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
                entry.Value.EnumerateArray().Select(x => x.GetString()).ToArray());
    }

    public BarRow[] GetRows(DateTime date, string ticker)
    {
        if (!_cache.TryGetValue(date, out var batch))
        {
            if (!_files.ContainsKey(date)) throw new InvalidDataException($"缺少封存日期 {date:yyyy-MM-dd}");
            // 保留当前和相邻日期；较早数组由仍在读取的枚举器持有，随后自然释放。
            foreach (var old in _cache.Keys.Where(x => x < date.AddDays(-1)).ToArray()) _cache.Remove(old);
            batch = Load(date);
            _cache.Add(date, batch);
        }
        return batch.TryGetValue(ticker, out var rows) ? rows : Array.Empty<BarRow>();
    }

    private Dictionary<string, BarRow[]> Load(DateTime day)
    {
        var watch = Stopwatch.StartNew();
        var collected = _selected.Values.ToDictionary(x => x, _ => new List<BarRow>(240));
        long sourceRows = 0;
        foreach (var path in _files[day])
        {
            using var reader = new ParquetFileReader(path);
            using var metadata = reader.FileMetaData;
            var schema = metadata.Schema;
            var columns = new Dictionary<string, int>();
            for (var i = 0; i < schema.NumColumns; i++)
            {
                var column = schema.Column(i);
                columns.Add(column.Name, i);
            }
            for (var index = 0; index < metadata.NumRowGroups; index++)
            {
                using var group = reader.RowGroup(index);
                var groupMetadata = group.MetaData;
                var count = checked((int)groupMetadata.NumRows);
                var instruments = Read<string>(group, columns["instrument_id"], count);
                var starts = Read<DateTime?>(group, columns["bar_start"], count);
                var ends = Read<DateTime?>(group, columns["bar_end"], count);
                var available = Read<DateTime?>(group, columns["available_time"], count);
                var receipt = Read<DateTime?>(group, columns["receipt_time"], count);
                var opens = Read<long?>(group, columns["open_units"], count);
                var highs = Read<long?>(group, columns["high_units"], count);
                var lows = Read<long?>(group, columns["low_units"], count);
                var closes = Read<long?>(group, columns["close_units"], count);
                var volumes = Read<long?>(group, columns["volume"], count);
                var completed = Read<bool?>(group, columns["completed"], count);
                var quality = Read<string>(group, columns["quality_status"], count);
                var interval = Read<long?>(group, columns["interval_minutes"], count);
                sourceRows += count;
                for (var i = 0; i < count; i++)
                {
                    if (!_selected.TryGetValue(instruments[i], out var ticker)) continue;
                    if (starts[i] == null || ends[i] == null || available[i] != ends[i] || receipt[i] != ends[i]
                        || ends[i] - starts[i] != TimeSpan.FromMinutes(1) || completed[i] != true
                        || quality[i] != "pass" || interval[i] != 1)
                        throw new InvalidDataException("行情未完成、时间不可见或质量不合格，禁止提前交付");
                    // Parquet UTC时间在读取边界转换一次，策略使用上海当地时间。
                    var local = DateTime.SpecifyKind(starts[i].Value.AddHours(8), DateTimeKind.Unspecified);
                    if (local.Date != day) throw new InvalidDataException("行情分区与时间不一致");
                    var minute = local.TimeOfDay.TotalMinutes;
                    if (!((minute >= 570 && minute < 690) || (minute >= 780 && minute < 900)))
                        throw new InvalidDataException("行情不在封存的常规分钟时段");
                    var list = collected[ticker];
                    if (list.Count > 0 && list[^1].Start >= local) throw new InvalidDataException("分钟行情重复或未按证券时间排序");
                    list.Add(new BarRow(local, opens[i] ?? throw new InvalidDataException("缺开盘价"),
                        highs[i] ?? throw new InvalidDataException("缺最高价"), lows[i] ?? throw new InvalidDataException("缺最低价"),
                        closes[i] ?? throw new InvalidDataException("缺收盘价"), volumes[i] ?? throw new InvalidDataException("缺成交量")));
                }
            }
        }
        var result = collected.ToDictionary(x => x.Key, x => x.Value.ToArray());
        var selectedRows = result.Values.Sum(x => x.Length);
        _sourceRows += sourceRows;
        _selectedRows += selectedRows;
        _loadCounts[day] = _loadCounts.GetValueOrDefault(day) + 1;
        watch.Stop();
        _reads.Add(new { date = day.ToString("yyyy-MM-dd"), files = _files[day].Length, source_rows = sourceRows, selected_rows = selectedRows, read_seconds = watch.Elapsed.TotalSeconds });
        return result;
    }

    private static T[] Read<T>(RowGroupReader group, int index, int count)
    {
        using var column = group.Column(index);
        using var logical = column.LogicalReader<T>();
        return logical.ReadAll(count);
    }

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(new
    {
        source_rows_read = _sourceRows, selected_rows = _selectedRows, emitted_rows = EmittedRows,
        date_load_counts = _loadCounts.ToDictionary(x => x.Key.ToString("yyyy-MM-dd"), x => x.Value),
        reads = _reads, source_kind = "existing_result_parquet", intermediate_market_files = 0
    }, new JsonSerializerOptions { WriteIndented = true }));

    public void Clear() => _cache.Clear();
}

