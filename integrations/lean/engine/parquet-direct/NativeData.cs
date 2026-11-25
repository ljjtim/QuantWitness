using System.Globalization;
using System.Text.Json;
using NodaTime;
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Algorithm;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Data.UniverseSelection;
using QuantWitness.Lean.China;

namespace QuantWitness.LeanInput;

/// <summary>按可见时点交付的财报、公告或业务数据，保留原始观测与全部声明字段。</summary>
public class ResearchData : BaseData
{
    public DateTime ObservationTime { get; set; }
    public DateTime ObservationEndTime { get; set; }
    public DateTime AvailableAt => Time;
    public string BindingId { get; set; }
    private string[] _names = Array.Empty<string>();
    private object[] _values = Array.Empty<object>();
    private Dictionary<string, JsonElement> _fields;

    public void SetTypedFields(string[] names, object[] values)
    {
        if (names.Length != values.Length) throw new InvalidDataException("研究数据字段数量不一致");
        _names = names;
        _values = values;
        _fields = null;
    }

    public string[] FieldNames => _fields == null ? _names : _fields.Keys.ToArray();
    public object[] FieldValues => _fields == null ? _values : _fields.Values.Cast<object>().ToArray();
    public Dictionary<string, JsonElement> Fields
    {
        get => _fields ??= _names.Zip(_values).ToDictionary(item => item.First,
            item => JsonSerializer.SerializeToElement(item.Second));
        set => _fields = value;
    }

    public override DateTimeZone DataTimeZone() => TimeZones.Shanghai;
    public override bool RequiresMapping() => false;
    public override BaseData Clone() => new ResearchData { Time = Time, Symbol = Symbol, Value = Value,
        ObservationTime = ObservationTime, ObservationEndTime = ObservationEndTime, BindingId = BindingId,
        _names = _names, _values = _values, _fields = _fields == null ? null : new(_fields) };
}

/// <summary>日线可见时钟与源bar区间独立保存，History与订阅使用同一投影。</summary>
public sealed class ResearchTradeBar : TradeBar
{
    public DateTime ObservationTime { get; }
    public DateTime ObservationEndTime { get; }
    private DateTime _availableAt;
    public DateTime AvailableAt => _availableAt;
    public override DateTime EndTime { get => _availableAt; set => _availableAt = value; }
    public ResearchTradeBar(DateTime observed, DateTime observedEnd, DateTime available, Symbol symbol,
        decimal open, decimal high, decimal low, decimal close, decimal volume)
        : base(observed, symbol, open, high, low, close, volume, observedEnd - observed)
    { ObservationTime = observed; ObservationEndTime = observedEnd; _availableAt = available; }
    public override BaseData Clone() => new ResearchTradeBar(ObservationTime, ObservationEndTime, EndTime,
        Symbol, Open, High, Low, Close, Volume);
}

/// <summary>独立于证券池的研究时钟；空证券池也能推进预热和正式会话。</summary>
public sealed class ResearchClock : BaseData
{
    public override DateTimeZone DataTimeZone() => TimeZones.Shanghai;
    public override bool RequiresMapping() => false;
    public override BaseData Clone() => new ResearchClock { Time = Time, Symbol = Symbol };
}

public static class NativeClockSubscription
{
    internal static void ConfigureTimeZones()
    {
        // Python AddData 的 Symbol 重载可把完整供应商代码转为 empty 市场；其通配会话须与原生来源同区。
        QuantConnect.Securities.MarketHoursDatabase.FromDataFolder().SetEntryAlwaysOpen(
            "empty", null, SecurityType.Base, TimeZones.Shanghai);
    }

    public static void Install(QCAlgorithm algorithm, string manifestPath)
    {
        var store = new NativeDataStore(manifestPath);
        if (!store.UsesSpotClock && store.Native.GetProperty("calendar_events").GetArrayLength() == 0)
            throw new InvalidDataException("原生运行必须冻结独立日历时钟");
        ConfigureTimeZones();
        algorithm.AddData<ResearchClock>("RP-CLOCK", Resolution.Minute, timeZone: TimeZones.Shanghai, fillForward: false);
    }
}

public sealed class NativeDataStore
{
    private readonly string _path;
    private readonly BindingIndex[] _bindings;

    private sealed record BindingIndex(JsonElement Spec, string Name, string Kind, string Resolution,
        Dictionary<string, string> Codes, Dictionary<(string Market, string Product, DateTime Expiry), string> Futures);

    public JsonElement Manifest { get; }
    public JsonElement Native { get; }
    public NativeDataStore(string path)
    {
        _path = path;
        Manifest = ManifestCache.Read(path);
        Native = Manifest.GetProperty("native_data");
        var contracts = Manifest.TryGetProperty("contracts", out var frozen) ? frozen : default;
        var common = Manifest.TryGetProperty("symbols", out var symbols) ? symbols : default;
        var commonIndex = common.ValueKind == JsonValueKind.Array ? Index(common, contracts) : default;
        _bindings = Native.GetProperty("bindings").EnumerateArray().Select(binding =>
        {
            var lookup = binding.TryGetProperty("symbols", out var declared) ? Index(declared, contracts) : commonIndex;
            return new BindingIndex(binding, binding.GetProperty("binding_id").GetString(),
                binding.GetProperty("kind").GetString(), binding.GetProperty("resolution").GetString(),
                lookup.Codes, lookup.Futures);
        }).ToArray();
    }

    private static (Dictionary<string, string> Codes, Dictionary<(string, string, DateTime), string> Futures)
        Index(JsonElement symbols, JsonElement contracts)
    {
        var codes = new Dictionary<string, string>(StringComparer.Ordinal);
        var futures = new Dictionary<(string, string, DateTime), string>();
        foreach (var item in symbols.EnumerateArray())
        {
            var code = item.GetString();
            codes.TryAdd(code, code);
            var separator = code.IndexOf('.');
            if (separator >= 0) codes.TryAdd(code[..separator], code);
            if (contracts.ValueKind == JsonValueKind.Object && contracts.TryGetProperty(code, out var contract))
                futures.TryAdd((contract.GetProperty("market").GetString().ToUpperInvariant(),
                    contract.GetProperty("product").GetString().ToUpperInvariant(),
                    DateTime.Parse(contract.GetProperty("expiry_date").GetString(), CultureInfo.InvariantCulture).Date), code);
        }
        return (codes, futures);
    }

    internal bool UsesSpotClock => Native.TryGetProperty("spot_execution_clock", out var spot) && spot.GetBoolean();

    public static string ManifestPath => Config.Get("parquet-input-manifest");
    public static DateTime Local(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture)
        .ToOffset(TimeSpan.FromHours(8)).DateTime;
    public static string Stamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc)
        .ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);

    public JsonElement Binding(SubscriptionDataConfig config, bool universe = false) => Resolve(config, universe).Binding;

    private (JsonElement Binding, string Code) Resolve(SubscriptionDataConfig config, bool universe)
    {
        var symbol = config.Symbol;
        var value = symbol.Value;
        var marker = value.IndexOf(':');
        var name = marker > 0 ? value[..marker] : null;
        var actual = marker > 0 ? value[(marker + 1)..] : value;
        var resolution = config.Resolution == Resolution.Minute ? "1m" : config.Resolution == Resolution.Daily ? "1d" : "";
        var bars = config.Type == typeof(TradeBar);
        var future = symbol.SecurityType == SecurityType.Future
            ? (symbol.ID.Market.ToUpperInvariant(), symbol.ID.Symbol.ToUpperInvariant(), symbol.ID.Date) : default;
        BindingIndex match = null;
        string resolvedCode = null;
        foreach (var binding in _bindings)
        {
            if (bars != (binding.Kind == "bars") || (binding.Resolution != resolution && binding.Resolution != "event")
                || (name != null && !string.Equals(name, binding.Name, StringComparison.OrdinalIgnoreCase))) continue;
            string code;
            if (universe)
            {
                if (binding.Kind != "universe" || !string.Equals(value, binding.Name, StringComparison.OrdinalIgnoreCase))
                    continue;
                code = "*";
            }
            else if (symbol.SecurityType == SecurityType.Future)
            {
                if (!binding.Futures.TryGetValue(future, out code)) continue;
            }
            else if (!binding.Codes.TryGetValue(actual, out code)
                && (symbol.Underlying == null || !binding.Codes.TryGetValue(symbol.Underlying.Value, out code))) continue;
            if (match != null) throw BindingError(config);
            match = binding;
            resolvedCode = code;
        }
        if (match == null) throw BindingError(config);
        if (bars && config.DataNormalizationMode != DataNormalizationMode.Raw)
            throw new NotSupportedException("原生来源使用Raw数据，复权须显式研究来源");
        return (match.Spec, resolvedCode);
    }

    private static InvalidDataException BindingError(SubscriptionDataConfig config) =>
        new($"原生订阅须唯一绑定已准入来源：{config.Symbol}/{config.Type.Name}/{config.Resolution}");

    public IEnumerable<BaseData> Read(SubscriptionDataConfig config, DateTime startUtc, DateTime endUtc,
        DateTime consumerUtc, string purpose, bool universe = false)
    {
        if (config.Type == typeof(ResearchClock))
        {
            var points = new SortedSet<DateTime>(Native.GetProperty("calendar_events").EnumerateArray()
                .Select(point => Local(point.GetString())));
            var windows = new List<(DateTime Start, DateTime End)>();
            if (UsesSpotClock)
            {
                foreach (var window in SpotExecutionWindow.Read(Manifest, false).Values)
                {
                    AddWindow(window.StartAt, window.EndAt);
                    points.Add(window.StartAt);
                    points.Add(window.EndAt);
                    if (window.SessionOpenAt >= window.StartAt) points.Add(window.SessionOpenAt);
                    if (window.SessionCloseAt <= window.EndAt) points.Add(window.SessionCloseAt);
                }
            }
            if (Native.TryGetProperty("calendar_windows", out var ranges))
                foreach (var range in ranges.EnumerateArray())
                    AddWindow(Local(range.GetProperty("start_at").GetString()), Local(range.GetProperty("end_at").GetString()));

            void AddWindow(DateTime start, DateTime end)
            {
                if (!UsesSpotClock || start.Date != end.Date)
                {
                    windows.Add((start, end));
                    return;
                }
                var lunch = start.Date.AddHours(11.5);
                var afternoon = start.Date.AddHours(13);
                if (start <= lunch) windows.Add((start, end < lunch ? end : lunch));
                if (end >= afternoon) windows.Add((start > afternoon ? start : afternoon, end));
            }
            windows.Sort((left, right) => left.Start.CompareTo(right.Start));
            var boundaries = points.GetEnumerator();
            var hasBoundary = boundaries.MoveNext();
            DateTime? last = null;
            foreach (var window in windows)
            {
                for (var at = window.Start; at <= window.End; at = at.AddMinutes(1))
                {
                    while (hasBoundary && boundaries.Current <= at)
                    {
                        var point = boundaries.Current;
                        if ((!last.HasValue || point > last.Value) && point.ConvertToUtc(TimeZones.Shanghai) > startUtc
                            && point.ConvertToUtc(TimeZones.Shanghai) <= endUtc)
                        { last = point; yield return new ResearchClock { Symbol = config.Symbol, Time = point }; }
                        hasBoundary = boundaries.MoveNext();
                    }
                    if ((!last.HasValue || at > last.Value) && at.ConvertToUtc(TimeZones.Shanghai) > startUtc
                        && at.ConvertToUtc(TimeZones.Shanghai) <= endUtc)
                    { last = at; yield return new ResearchClock { Symbol = config.Symbol, Time = at }; }
                }
            }
            while (hasBoundary)
            {
                var point = boundaries.Current;
                var utc = point.ConvertToUtc(TimeZones.Shanghai);
                if ((!last.HasValue || point > last.Value) && utc > startUtc && utc <= endUtc)
                { last = point; yield return new ResearchClock { Symbol = config.Symbol, Time = point }; }
                hasBoundary = boundaries.MoveNext();
            }
            yield break;
        }
        var (binding, code) = Resolve(config, universe);
        var lower = DateTimeOffset.Parse(binding.GetProperty("history_start_at").GetString()).UtcDateTime;
        var upper = DateTimeOffset.Parse(binding.GetProperty("end_at").GetString()).UtcDateTime;
        if (endUtc <= lower || startUtc >= upper) yield break;
        startUtc = startUtc < lower ? lower : startUtc; endUtc = endUtc > upper ? upper : endUtc;
        if (binding.TryGetProperty("direct_minute", out var minute) && minute.GetBoolean())
        {
            foreach (var row in MonthlyParquetStore.Open(_path).Read(config, code, startUtc, endUtc, consumerUtc))
                yield return row;
            yield break;
        }
        var rows = NativeDirectReader.Read(_path, binding, config.Symbol, code, startUtc, endUtc, consumerUtc, purpose, universe);
        if (!universe)
        {
            foreach (var row in rows) yield return row;
            yield break;
        }
        // 同一可见时点的完整截面交给一次 Universe 回调。
        var group = new List<BaseData>();
        DateTime? timestamp = null;
        foreach (var row in rows)
        {
            if (timestamp.HasValue && timestamp != row.Time)
            {
                yield return new BaseDataCollection(timestamp.Value, config.Symbol, group);
                group = new();
            }
            timestamp = row.Time;
            group.Add(row);
        }
        if (group.Count > 0)
            yield return new BaseDataCollection(timestamp.Value, config.Symbol, group);
    }
}
