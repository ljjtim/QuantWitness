using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using QuantConnect;
using QuantConnect.Python;
using Python.Runtime;
using QuantWitness.Lean.China;

namespace QuantWitness.LeanInput;

public sealed record FinancialFutureRule(string InstrumentId, DateOnly EffectiveFrom, DateOnly EffectiveTo,
    DateTime AvailableAt, long LongMarginPpm, long ShortMarginPpm, long OpenFeeUnits, long OpenFeePpm,
    long CloseFeeUnits, long CloseFeePpm, long CloseTodayFeeUnits, long CloseTodayFeePpm, decimal LowerLimit,
    decimal UpperLimit, string ClosePriority, string SourceRef);

public sealed record FinancialSettlement(string InstrumentId, DateOnly TradingDate, decimal Price,
    DateTime AvailableAt, string SourceRef);

public sealed record FrozenFinancialDay(IReadOnlyList<FinancialFutureRule> Rules,
    IReadOnlyList<FinancialSettlement> Settlements);

// 数据馈送与规则消费者共用冻结输入；仅物理读取可提前。
public sealed class FrozenFinancialDayStore
{
    private const int BarBytes = 60;
    private static readonly Dictionary<string, FrozenFinancialDayStore> Stores = new();
    private readonly object _gate = new();
    private readonly string[] _symbols;
    private readonly HashSet<DateTime> _dates;
    private readonly FrozenMinuteParquetStore _minute;
    private FrozenSpotRuleStore _spotRules;
    private readonly int _dayBufferLimit;
    private readonly double? _ruleSourceLoadSeconds;
    private sealed class SpotRuleTiming
    {
        public int Materialized;
        public long ElapsedTicks;
    }

    private readonly Dictionary<DateOnly, SpotRuleTiming> _rulesByDate = new();
    private readonly string _manifestPath;
    private readonly Dictionary<DateTime, (HashSet<string> Codes, FrozenFinancialDay Header)> _cache = new();
    private readonly Dictionary<string, int> _loads = new();
    private readonly Dictionary<string, int> _dailyRows = new();
    private readonly Dictionary<string, object> _financialDays = new();
    private readonly Dictionary<DateTime, JsonElement[]> _sessions;
    private long _rows;
    public string Frequency { get; }
    public bool IsFuture { get; }
    public JsonElement Manifest { get; }
    public Dictionary<DateTime, SpotExecutionWindow> ExecutionWindows { get; }

    public static FrozenFinancialDayStore Open(string manifest)
    {
        lock (Stores)
        {
            var path = Path.GetFullPath(manifest);
            if (!Stores.TryGetValue(path, out var store)) Stores.Add(path, store = new(path));
            return store;
        }
    }

    private FrozenFinancialDayStore(string manifest)
    {
        _manifestPath = manifest;
        var spec = ManifestCache.Read(manifest);
        if (spec.GetProperty("source_kind").GetString() is not ("frozen_financial_day" or "frozen_minute_parquet"))
            throw new InvalidDataException("原生执行必须使用冻结直接来源");
        Manifest = spec;
        IsFuture = spec.TryGetProperty("asset_class", out var asset) && asset.GetString() == "cn_future";
        Frequency = spec.GetProperty("frequency").GetString();
        if (spec.TryGetProperty("minute_parquet", out _)) _minute = MonthlyParquetStore.Open(manifest);
        var scale = spec.GetProperty("price_scale").GetInt32();
        if (Frequency is not ("1d" or "1m") || (!IsFuture && scale is not (2 or 3)))
            throw new NotSupportedException("直接输入仅支持二位或三位价格的日线或一分钟");
        _symbols = spec.GetProperty("symbols").EnumerateArray().Select(x => x.GetString()).ToArray();
        _dates = spec.GetProperty("dates").EnumerateArray().Select(x => DateTime.ParseExact(x.GetString(),
            "yyyy-MM-dd", CultureInfo.InvariantCulture)).ToHashSet();
        if (IsFuture)
            _sessions = Manifest.GetProperty("sessions").EnumerateArray()
                .GroupBy(session => DateTime.ParseExact(session.GetProperty("trading_date").GetString(),
                    "yyyy-MM-dd", CultureInfo.InvariantCulture))
                .ToDictionary(group => group.Key, group => group.ToArray());
        if (!IsFuture)
        {
            ExecutionWindows = SpotExecutionWindow.Read(spec, Frequency == "1d");
            _spotRules = new FrozenSpotRuleStore(spec.GetProperty("rules_parquet"), _symbols, asset.GetString());
            _ruleSourceLoadSeconds = _spotRules.LoadSeconds;
        }
        if (Frequency == "1m" && _minute == null)
            throw new InvalidDataException("缺少冻结分钟行情");
        if (IsFuture || Frequency == "1d")
            _dayBufferLimit = (int)Math.Min(int.MaxValue,
                spec.GetProperty("financial_day").GetProperty("memory_bytes").GetInt64() / 8);

    }

    public void PrepareSpotRules(DateOnly day)
    {
        lock (_gate)
        {
            var started = Stopwatch.GetTimestamp();
            var date = day.ToDateTime(TimeOnly.MinValue);
            if (!ExecutionWindows.ContainsKey(date))
                throw new InvalidDataException("直接规则日期不在冻结范围内");
            if (_minute != null)
                _minute.ValidateSpotRules(date, code => _spotRules.Covers(code, day));
            else
            {
                Load(date);
                foreach (var code in _cache[date].Codes)
                    if (!_spotRules.Covers(code, day))
                        throw new InvalidDataException($"正式行情缺少当日历史规则：{code} {date:yyyy-MM-dd}");
            }
            _loads.TryAdd(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), 1);
            _rulesByDate.Add(day, new SpotRuleTiming { ElapsedTicks = Stopwatch.GetTimestamp() - started });
        }
    }

    public ChinaRule[] SpotRules(DateOnly day, string code)
    {
        lock (_gate)
        {
            var started = Stopwatch.GetTimestamp();
            var timing = _rulesByDate[day];
            var window = ExecutionWindows[day.ToDateTime(TimeOnly.MinValue)];
            var result = _spotRules.ReadInstrument(code, day, window);
            timing.Materialized += result.Length;
            timing.ElapsedTicks += Stopwatch.GetTimestamp() - started;
            return result;
        }
    }

    public FrozenFinancialDay Header(DateTime date)
    {
        lock (_gate) { Load(date.Date); return _cache[date.Date].Header; }
    }

    internal JsonElement[] Sessions(DateTime date) => _sessions[date.Date];

    private void Load(DateTime date)
    {
        if (_cache.ContainsKey(date)) return;
        if (!_dates.Contains(date)) throw new InvalidDataException("直接输入日期不在冻结范围内");
        var day = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (_loads.ContainsKey(day)) throw new InvalidDataException("直接输入不能回读已释放日期");
        List<FinancialFutureRule> rules;
        List<FinancialSettlement> settlements;
        double barsSeconds, rulesSeconds;
        byte[] bars;
        PythonInitializer.Initialize();
        using (Py.GIL())
        using (var module = Py.Import("research_pipeline.runtime.lean_financial_day"))
        using (var path = new PyString(_manifestPath))
        using (var dayText = new PyString(day))
        using (var result = module.InvokeMethod("read_day", path, dayText))
        using (var data = result.GetItem(0))
        using (var ruleRows = result.GetItem(1))
        using (var settlementRows = result.GetItem(2))
        using (var barsTime = result.GetItem(3))
        using (var rulesTime = result.GetItem(4))
        using (var buffer = data.GetBuffer())
        {
            if (buffer.Length < 0 || buffer.Length > _dayBufferLimit || buffer.Length % BarBytes != 0)
                throw new InvalidDataException("单日冻结行情整数缓冲区无效");
            bars = new byte[checked((int)buffer.Length)];
            buffer.Read(bars, 0, bars.Length, 0);
            rules = (List<FinancialFutureRule>)ruleRows.AsManagedObject(typeof(List<FinancialFutureRule>));
            settlements = (List<FinancialSettlement>)settlementRows.AsManagedObject(typeof(List<FinancialSettlement>));
            barsSeconds = barsTime.As<double>();
            rulesSeconds = rulesTime.As<double>();
        }
        var count = bars.Length / BarBytes;
        if (_minute != null && count != 0)
            throw new InvalidDataException("冻结分钟行情由分钟 Parquet 读取");
        var observed = new HashSet<string>(StringComparer.Ordinal);
        string previousCode = null;
        DateTime previousStart = default;
        for (var offset = 0; offset < bars.Length; offset += BarBytes)
        {
            var record = bars.AsSpan(offset, BarBytes);
            var index = BinaryPrimitives.ReadInt32LittleEndian(record);
            if (index < 0 || index >= _symbols.Length)
                throw new InvalidDataException("冻结金融输入含未声明证券");
            var code = _symbols[index];
            var start = Local(BinaryPrimitives.ReadInt64LittleEndian(record[4..]));
            var end = Local(BinaryPrimitives.ReadInt64LittleEndian(record[12..]));
            var open = BinaryPrimitives.ReadInt64LittleEndian(record[20..]);
            var high = BinaryPrimitives.ReadInt64LittleEndian(record[28..]);
            var low = BinaryPrimitives.ReadInt64LittleEndian(record[36..]);
            var close = BinaryPrimitives.ReadInt64LittleEndian(record[44..]);
            var volume = BinaryPrimitives.ReadInt64LittleEndian(record[52..]);
            if ((!IsFuture && (start.Date != date || end.Date != date)) || end <= start
                || (Frequency == "1m" && end - start != TimeSpan.FromMinutes(1))
                || (!IsFuture && Frequency == "1d" && (start.TimeOfDay != TimeSpan.FromHours(9.5) || end.TimeOfDay != TimeSpan.FromHours(15)))
                || open <= 0 || close <= 0 || low <= 0 || low > Math.Min(open, close)
                || high < Math.Max(open, close) || volume < 0)
                throw new InvalidDataException("直接输入时间或OHLCV无效");
            if (IsFuture)
            {
                var session = Sessions(date).Single(x => x.GetProperty("instrument_id").GetString() == code);
                if (session.GetProperty("trading_date").GetString() != day
                    || start < DateTimeOffset.Parse(session.GetProperty("start_at").GetString()).DateTime
                    || end > DateTimeOffset.Parse(session.GetProperty("end_at").GetString()).DateTime)
                    throw new InvalidDataException("期货行情超出冻结交易日会话");
                if (Frequency == "1m" && !session.GetProperty("segments").EnumerateArray().Any(x =>
                    start >= DateTimeOffset.Parse(x.GetProperty("start_at").GetString()).DateTime
                    && end <= DateTimeOffset.Parse(x.GetProperty("end_at").GetString()).DateTime))
                    throw new InvalidDataException("期货分钟跨过休市小节");
            }
            if (previousCode != null && (StringComparer.Ordinal.Compare(previousCode, code) > 0
                || previousCode == code && previousStart >= start))
                throw new InvalidDataException("直接输入未按证券、开始时间稳定排序或存在重复");
            if (Frequency == "1d" && previousCode == code) throw new InvalidDataException("同一证券每天只能有一条日线");
            observed.Add(code);
            previousCode = code; previousStart = start;
        }
        _cache.Add(date, (observed, new(rules, settlements)));
        _loads.Add(day, 1);
        _dailyRows.Add(day, count);
        _financialDays.Add(day, new { bars_seconds = barsSeconds, rules_seconds = rulesSeconds });
        _rows += count;
        // 同步器最多领先当前时钟一个交易日；数组由正在消费它的证券枚举器持有。
        foreach (var old in _cache.Keys.Order().Take(Math.Max(0, _cache.Count - 2)).ToArray()) _cache.Remove(old);
    }

    private static DateTime Local(long microseconds) => DateTime.SpecifyKind(
        DateTime.UnixEpoch.AddTicks(checked(microseconds * 10)).AddHours(8), DateTimeKind.Unspecified);

    public void Save(string path)
    {
        if (_minute != null)
        {
            var rules = _ruleSourceLoadSeconds.HasValue ? _rulesByDate.ToDictionary(
                entry => entry.Key.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                entry => (object)new { rows = entry.Value.Materialized,
                    seconds = (double)entry.Value.ElapsedTicks / Stopwatch.Frequency }) : null;
            _minute.Save(path, _loads, rules, _ruleSourceLoadSeconds, _financialDays);
            return;
        }
        File.WriteAllText(path, JsonSerializer.Serialize(new
        {
            source_kind = "frozen_financial_day", frequency = Frequency, source_rows_read = _rows,
            selected_rows = _rows, emitted_rows = 0, date_load_counts = _loads,
            daily_selected_rows = _dailyRows, financial_days = _financialDays,
            intermediate_market_files = 0, intermediate_rule_files = 0
        }));
    }

    public void Clear()
    {
        lock (_gate)
        {
            _cache.Clear();
            _spotRules = null;
        }
        if (_minute != null) MonthlyParquetStore.Close(_manifestPath);
        lock (Stores)
            foreach (var key in Stores.Where(x => ReferenceEquals(x.Value, this)).Select(x => x.Key).ToArray()) Stores.Remove(key);
    }
}
