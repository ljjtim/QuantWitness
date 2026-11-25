using System.Globalization;
using System.Text.Json;

namespace QuantWitness.Lean.China;

/// <summary>消费已冻结的逐日或时段规则；金额单位为分，费率单位为百万分之一。</summary>
public sealed class ChinaRuleBook
{
    private Dictionary<string, ChinaRule[]> _rules;
    private readonly HashSet<DateOnly> _sessions;
    private readonly DateOnly[] _orderedSessions;
    private readonly Action<DateOnly>? _prepareFrozenDay;
    private readonly Func<DateOnly, string, ChinaRule[]>? _readFrozenInstrument;
    private readonly HashSet<DateOnly> _preparedDays = new();
    private readonly Dictionary<string, (DateOnly Date, ChinaRule[] Rules)> _lastHistoricalRules = new();
    private readonly object _dayLock = new();
    private DateOnly? _loadedDay;
    public IReadOnlyCollection<DateOnly> TradingDates => _sessions;

    public ChinaRuleBook(IEnumerable<DateOnly> tradingDates, IEnumerable<ChinaRule> rules)
    {
        _sessions = tradingDates.ToHashSet();
        if (_sessions.Count == 0) throw new ArgumentException("交易日历不能为空");
        _orderedSessions = _sessions.OrderBy(day => day).ToArray();
        _rules = rules.GroupBy(x => x.InstrumentId).ToDictionary(x => x.Key, x => x.OrderByDescending(r => r.AvailableAt).ToArray());
        if (_rules.Count == 0) throw new ArgumentException("历史规则不能为空");
    }

    public ChinaRuleBook(IEnumerable<DateOnly> tradingDates, Action<DateOnly> prepareFrozenDay,
        Func<DateOnly, string, ChinaRule[]> readFrozenInstrument)
    {
        _sessions = tradingDates.ToHashSet();
        if (_sessions.Count == 0) throw new ArgumentException("交易日历不能为空");
        _orderedSessions = _sessions.OrderBy(day => day).ToArray();
        _rules = new();
        _prepareFrozenDay = prepareFrozenDay;
        _readFrozenInstrument = readFrozenInstrument;
    }

    public void AdvanceTime(DateTime utc) => EnsureDay(utc);

    private void EnsureDay(DateTime utc)
    {
        if (_prepareFrozenDay == null) return;
        var day = DateOnly.FromDateTime(ShanghaiTime(utc).DateTime);
        if (!_sessions.Contains(day) || _loadedDay == day) return;
        lock (_dayLock)
        {
            if (_loadedDay == day) return;
            if (_loadedDay.HasValue && day < _loadedDay) throw new InvalidOperationException("规则消费时钟不能倒退");
            _prepareFrozenDay(day);
            _preparedDays.Add(day);
            if (_loadedDay is { } previous)
                foreach (var (code, rows) in _rules)
                    if (rows.Length != 0) _lastHistoricalRules[code] = (previous, rows);
            _rules = new();
            _loadedDay = day;
        }
    }

    private ChinaRule[] RulesFor(string code, DateOnly? day)
    {
        if (_readFrozenInstrument == null)
            return _rules.TryGetValue(code, out var rows) ? rows : Array.Empty<ChinaRule>();
        if (day.HasValue)
            lock (_dayLock)
            {
                if (!_preparedDays.Contains(day.Value)) return Array.Empty<ChinaRule>();
                if (day != _loadedDay)
                {
                    var hasPrevious = _lastHistoricalRules.TryGetValue(code, out var previous);
                    if (hasPrevious && previous.Date == day) return previous.Rules;
                    var selected = _readFrozenInstrument(day.Value, code);
                    if (selected.Length != 0 && (!hasPrevious || day > previous.Date))
                        _lastHistoricalRules[code] = (day.Value, selected);
                    return selected;
                }
                if (!_rules.TryGetValue(code, out var rows))
                    _rules.Add(code, rows = _readFrozenInstrument(day.Value, code));
                return rows;
            }
        return Array.Empty<ChinaRule>();
    }

    public static DateTimeOffset ShanghaiTime(DateTime utc)
    {
        if (utc.Kind != DateTimeKind.Utc) throw new ArgumentException("必须传入 UTC 时间");
        return new DateTimeOffset(utc).ToOffset(TimeSpan.FromHours(8));
    }

    public DateOnly TradingDate(DateTime utc)
    {
        var date = DateOnly.FromDateTime(ShanghaiTime(utc).DateTime);
        if (!_sessions.Contains(date)) throw new InvalidOperationException($"交易日历未覆盖 {date}");
        return date;
    }

    public ChinaRule Resolve(string instrumentId, DateTime utc)
    {
        EnsureDay(utc);
        var session = TradingDate(utc);
        var at = new DateTimeOffset(utc);
        var rows = RulesFor(instrumentId, session);
        if (_readFrozenInstrument == null && rows.Length == 0)
            throw new InvalidOperationException($"缺少证券规则 {instrumentId}");
        var candidates = rows.Where(r => r.EffectiveFrom <= session && session <= r.EffectiveTo
            && r.AvailableAt <= at && (r.ValidFrom == null || r.ValidFrom <= at)
            && (r.ValidTo == null || at < r.ValidTo)).ToArray();
        if (candidates.Length == 0) throw new InvalidOperationException($"规则尚不可见或未覆盖 {instrumentId} {at:O}");
        var latest = candidates[0];
        if (candidates.Skip(1).Any(r => r.AvailableAt == latest.AvailableAt))
            throw new InvalidOperationException($"同一时点有歧义规则 {instrumentId} {at:O}");
        return latest;
    }

    /// <summary>估算平仓费用时沿用最近已可见交易日费率，不授予当前时点的交易资格。</summary>
    public ChinaRule ResolveForFeeEstimate(string instrumentId, DateTime utc)
    {
        EnsureDay(utc);
        var today = DateOnly.FromDateTime(ShanghaiTime(utc).DateTime);
        var at = new DateTimeOffset(utc);
        var fixedRows = _readFrozenInstrument == null ? RulesFor(instrumentId, today) : null;
        if (fixedRows?.Length == 0) throw new InvalidOperationException($"缺少证券规则 {instrumentId}");
        var index = Array.BinarySearch(_orderedSessions, today);
        if (index < 0) index = ~index - 1;
        for (; index >= 0; index--)
        {
            var session = _orderedSessions[index];
            // 已结束时段可以供历史估费；尚未开始或尚不可见的规则不能供估费。
            ChinaRule? latest = null;
            var ambiguous = false;
            foreach (var row in fixedRows ?? RulesFor(instrumentId, session))
            {
                if (row.EffectiveFrom > session || session > row.EffectiveTo
                    || row.AvailableAt > at || (row.ValidFrom != null && row.ValidFrom > at)) continue;
                var order = latest == null ? 1 : row.AvailableAt.CompareTo(latest.AvailableAt);
                if (order == 0) order = Nullable.Compare(row.ValidFrom, latest!.ValidFrom);
                if (order > 0)
                {
                    latest = row;
                    ambiguous = false;
                }
                else if (order == 0) ambiguous = true;
            }
            if (latest == null) continue;
            if (ambiguous)
                throw new InvalidOperationException($"估费时点有歧义规则 {instrumentId} {at:O}");
            return latest;
        }
        throw new InvalidOperationException($"估费时点之前没有已可见历史规则 {instrumentId} {at:O}");
    }
    /// <summary>检查整个已完成分钟，避免跨过停牌或不可见规则缺口。</summary>
    public bool SuspendedDuring(string instrumentId, DateTime startUtc, DateTime endUtc)
    {
        EnsureDay(endUtc);
        var points = new SortedSet<DateTime> { startUtc, endUtc };
        var rows = RulesFor(instrumentId, TradingDate(endUtc));
        if (rows.Length == 0) throw new InvalidOperationException("缺少证券规则");
        foreach (var row in rows)
            foreach (var point in new DateTimeOffset?[] { row.AvailableAt, row.ValidFrom, row.ValidTo })
                if (point.HasValue && startUtc < point.Value.UtcDateTime && point.Value.UtcDateTime < endUtc)
                    points.Add(point.Value.UtcDateTime);
        return points.Any(at => Resolve(instrumentId, at).Paused);
    }
}

public sealed record QuantityGrid(decimal Minimum, decimal Step, decimal? Maximum, bool RemainderAllowed)
{
    public bool Accepts(decimal quantity, decimal sellable = 0)
    {
        if (quantity <= 0 || quantity != decimal.Truncate(quantity) || (Maximum.HasValue && quantity > Maximum)) return false;
        if (quantity >= Minimum && (quantity - Minimum) % Step == 0) return true;
        if (!RemainderAllowed || quantity > sellable) return false;
        if (sellable < Minimum) return quantity == sellable;
        var remainder = (sellable - Minimum) % Step;
        return remainder > 0 && quantity % Step == remainder;
    }
}

public sealed class ChinaRule
{
    private static readonly string[] ExplicitPriceFields =
        { "reference_price_kind", "reference_price_units", "price_limit_up_ratio_ppm", "price_limit_down_ratio_ppm" };
    public string InstrumentId { get; }
    public DateOnly EffectiveFrom { get; }
    public DateOnly EffectiveTo { get; }
    public DateTimeOffset AvailableAt { get; }
    public DateTimeOffset? ValidFrom { get; }
    public DateTimeOffset? ValidTo { get; }
    internal class FrozenTemplate(IReadOnlyDictionary<string, JsonElement> parameters)
    {
        internal IReadOnlyDictionary<string, JsonElement> Parameters { get; } = parameters;
        internal Terms? ValidatedTerms { get; set; }
    }

    internal readonly struct ParameterValue
    {
        private readonly JsonElement _json;
        private readonly long _number;
        private readonly string? _text;
        public JsonValueKind ValueKind { get; }

        public ParameterValue(JsonElement value) { _json = value; ValueKind = value.ValueKind; }
        public ParameterValue(long value) { _number = value; ValueKind = JsonValueKind.Number; }
        public ParameterValue(bool value) { ValueKind = value ? JsonValueKind.True : JsonValueKind.False; }
        public ParameterValue(string value) { _text = value; ValueKind = JsonValueKind.String; }

        public decimal GetDecimal() => _json.ValueKind != JsonValueKind.Undefined
            ? _json.GetDecimal() : ValueKind == JsonValueKind.Number ? _number
            : throw new InvalidOperationException("冻结规则字段不是数字");

        public bool GetBoolean() => _json.ValueKind != JsonValueKind.Undefined
            ? _json.GetBoolean() : ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                _ => throw new InvalidOperationException("冻结规则字段不是布尔值"),
            };

        public string? GetString() => _json.ValueKind != JsonValueKind.Undefined
            ? _json.GetString() : ValueKind == JsonValueKind.String ? _text
            : throw new InvalidOperationException("冻结规则字段不是字符串");

        public bool Matches(JsonElement value) => ValueKind == value.ValueKind && ValueKind switch
        {
            JsonValueKind.Number => value.TryGetDecimal(out var number) && number == GetDecimal(),
            JsonValueKind.String => GetString() == value.GetString(),
            JsonValueKind.True or JsonValueKind.False => true,
            _ => JsonElement.DeepEquals(_json, value),
        };

        public JsonElement ToJson() => _json.ValueKind != JsonValueKind.Undefined ? _json : ValueKind switch
        {
            JsonValueKind.Number => JsonSerializer.SerializeToElement(_number),
            JsonValueKind.String => JsonSerializer.SerializeToElement(_text),
            JsonValueKind.True or JsonValueKind.False => JsonSerializer.SerializeToElement(GetBoolean()),
            _ => _json,
        };
    }

    internal abstract class FrozenOverrides
    {
        public abstract ParameterValue Find(string name);
        public abstract IEnumerable<KeyValuePair<string, ParameterValue>> Fields();
    }

    internal sealed record Terms(int SettlementDays, decimal CommissionPpm, decimal MinimumCommissionFen,
        decimal SellTaxPpm, decimal TransferFeePpm, bool FundQuantityRule, bool MarketProtectionRequired,
        QuantityGrid LimitBuy, QuantityGrid MarketBuy, QuantityGrid LimitSell, QuantityGrid MarketSell);

    private readonly FrozenTemplate? _template;
    private readonly FrozenOverrides? _overrides;
    private readonly Terms _terms;
    private readonly DateTimeOffset? _referenceAvailableAt;
    private JsonElement _parameters;
    public JsonElement Parameters
    {
        get
        {
            if (_parameters.ValueKind == JsonValueKind.Undefined)
            {
                var fields = new Dictionary<string, JsonElement>(_template!.Parameters, StringComparer.Ordinal);
                if (_overrides != null)
                    foreach (var (name, value) in _overrides.Fields()) fields[name] = value.ToJson();
                _parameters = JsonSerializer.SerializeToElement(fields);
            }
            return _parameters;
        }
    }

    private bool TryParameter(string name, out ParameterValue value)
    {
        if (_template == null ? _parameters.TryGetProperty(name, out var json)
            : _template.Parameters.TryGetValue(name, out json))
        {
            value = new ParameterValue(json);
            return true;
        }
        value = _overrides?.Find(name) ?? default;
        return value.ValueKind != JsonValueKind.Undefined;
    }

    private ParameterValue Parameter(string name) => TryParameter(name, out var value)
        ? value : throw new KeyNotFoundException(name);
    public bool Paused { get; }
    public string PriceLimitMode { get; }
    public bool MarketProtectionRequired => _terms.MarketProtectionRequired;
    public bool MarketOrderAllowed => !TryParameter("market_order_allowed", out var value) || value.GetBoolean();
    public int PriceScale { get; }
    public int SettlementDays => _terms.SettlementDays;
    public decimal PriceTickUnits { get; }
    public decimal CommissionPpm => _terms.CommissionPpm;
    public decimal MinimumCommissionFen => _terms.MinimumCommissionFen;
    public decimal SellTaxPpm => _terms.SellTaxPpm;
    public decimal TransferFeePpm => _terms.TransferFeePpm;
    public decimal? HighLimit { get; }
    public decimal? LowLimit { get; }

    private ChinaRule(string instrument, DateOnly from, DateOnly to, DateTimeOffset available,
        DateTimeOffset? validFrom, DateTimeOffset? validTo,
        JsonElement parameters, FrozenTemplate? template, FrozenOverrides? overrides, bool validatedLimits = false)
    {
        InstrumentId = instrument;
        EffectiveFrom = from;
        EffectiveTo = to;
        AvailableAt = available;
        ValidFrom = validFrom;
        ValidTo = validTo;
        _parameters = parameters;
        _template = template;
        _overrides = overrides;
        if (EffectiveFrom > EffectiveTo || (ValidFrom.HasValue && ValidTo.HasValue && ValidFrom >= ValidTo))
            throw new ArgumentException("规则有效区间倒置");
        if (string.IsNullOrWhiteSpace(InstrumentId))
            throw new ArgumentException("现货结算必须明确为T+0或T+1");
        // 逐日列只包含生命周期、价格和停牌事实。数量、结算及费率来自所选模板。
        _terms = template?.ValidatedTerms ?? ReadTerms();
        if (template != null) template.ValidatedTerms = _terms;

        PriceTickUnits = OptionalNumber("price_tick_units") ?? 1m;
        if (PriceTickUnits <= 0) throw new ArgumentException("价格格点必须为正整数报价单位");
        PriceScale = checked((int)Number("price_scale"));
        if (PriceScale is < 0 or > 8) throw new ArgumentException("报价精度无效");
        var scale = (decimal)Math.Pow(10, PriceScale);
        var highUnits = OptionalNumber("high_limit_units");
        var lowUnits = OptionalNumber("low_limit_units");
        HighLimit = highUnits / scale;
        LowLimit = lowUnits / scale;
        Paused = TryParameter("paused", out var paused) ? paused.GetBoolean()
            : _terms.FundQuantityRule ? false : throw new ArgumentException("股票规则缺少停牌事实");
        PriceLimitMode = TryParameter("price_limit_mode", out var mode) ? mode.GetString()!
            : _terms.FundQuantityRule ? "bounded" : throw new ArgumentException("股票规则缺少价格限制模式");
        var explicitPrice = ExplicitPriceFields.Any(name => TryParameter(name, out _));
        if (explicitPrice && PriceLimitMode != "bounded")
            throw new ArgumentException("独立上下比例只适用于有界价格模式");
        if (PriceLimitMode == "unbounded")
        {
            if (HighLimit != null || LowLimit != null || OptionalNumber("price_limit_ratio_ppm") != null)
                throw new ArgumentException("无涨跌幅限制阶段不能携带固定上下限");
        }
        else if (PriceLimitMode is "bounded" or "suspended")
        {
            if (HighLimit == null || LowLimit == null || LowLimit > HighLimit) throw new ArgumentException("缺少涨跌停上下限");
            if (PriceLimitMode == "suspended")
            {
                if (!Paused || Parameter("trading_status").GetString() != "suspended") throw new ArgumentException("停牌模式与停牌事实不一致");
                if (LowLimit < 0 || HighLimit < 0 || Number("reference_previous_close_units") <= 0)
                    throw new ArgumentException("停牌价格边界和前收参考价无效");
            }
            else
            {
                decimal reference, upRate, downRate;
                if (explicitPrice)
                {
                    if (ExplicitPriceFields.Any(name => !TryParameter(name, out _))
                        || TryParameter("reference_previous_close_units", out _)
                        || TryParameter("price_limit_ratio_ppm", out _))
                        throw new ArgumentException("独立上下比例字段必须完整，且不能混用对称比例字段");
                    var kind = Parameter("reference_price_kind").GetString();
                    if (kind is not ("previous_close" or "issue_price"))
                        throw new ArgumentException("参考价格类型无效");
                    if (kind == "issue_price" &&
                        (TryParameter("etf_category", out _)
                         || !TryParameter("stock_board", out var board)
                         || board.GetString() is not ("sh_main" or "sz_main" or "chinext" or "star" or "bse")
                         || !TryParameter("listing_phase", out var phase) || phase.GetString() != "ipo"))
                        throw new ArgumentException("发行价参考只适用于明确的股票首次上市阶段");
                    reference = Number("reference_price_units");
                    upRate = Number("price_limit_up_ratio_ppm");
                    downRate = Number("price_limit_down_ratio_ppm");
                }
                else
                {
                    reference = Number("reference_previous_close_units");
                    upRate = downRate = Number("price_limit_ratio_ppm");
                }
                if (reference <= 0 || upRate <= 0 || downRate <= 0 || downRate >= 1_000_000 || LowLimit <= 0
                    || (TryParameter("price_limit_rounding", out var rounding)
                        ? rounding.GetString() != "half_up_to_quote_unit" : explicitPrice || !_terms.FundQuantityRule)
                    || !validatedLimits &&
                        (Math.Round(reference * (1 + upRate / 1_000_000), 0, MidpointRounding.AwayFromZero) != highUnits
                         || Math.Round(reference * (1 - downRate / 1_000_000), 0, MidpointRounding.AwayFromZero) != lowUnits))
                    throw new ArgumentException("涨跌停值与冻结参考价格、比例和舍位口径不一致");
            }
            _referenceAvailableAt = Timestamp(Parameter("reference_price_available_at").GetString()!);
        }
        else throw new ArgumentException("不支持的价格限制模式");
    }

    public void RequireVisibleReference(DateTime utc)
    {
        if (_referenceAvailableAt?.UtcDateTime > utc)
            throw new InvalidOperationException("参考价格在消费时尚不可见");
    }

    private Terms ReadTerms()
    {
        var settlement = checked((int)Number("settlement_days"));
        if (settlement is not (0 or 1)) throw new ArgumentException("现货结算必须明确为T+0或T+1");

        var fund = TryParameter("buy_lot_units", out _);
        var limitBuy = ReadGrid("buy", fund);
        var limitSell = ReadGrid("sell", fund);
        if (limitBuy.Minimum <= 0 || limitBuy.Step <= 0 || limitBuy.Maximum < limitBuy.Minimum
            || limitSell.Minimum <= 0 || limitSell.Step <= 0 || limitSell.Maximum < limitSell.Minimum)
            throw new ArgumentException("数量格点无效");
        var marketBuy = limitBuy with { Maximum = OptionalNumber("market_buy_max_quantity") ?? limitBuy.Maximum };
        var marketSell = limitSell with { Maximum = OptionalNumber("market_sell_max_quantity") ?? limitSell.Maximum };
        if (marketBuy.Maximum < marketBuy.Minimum || marketSell.Maximum < marketSell.Minimum)
            throw new ArgumentException("市价数量上限无效");

        var commission = Number("commission_ppm");
        var minimum = Number("min_commission_units");
        var tax = Number("sell_tax_ppm");
        var transfer = Number("transfer_fee_ppm");
        if (commission < 0 || minimum < 0 || tax < 0 || transfer < 0)
            throw new ArgumentException("费用不能为负数");
        var protection = TryParameter("market_protection_required", out var required) ? required.GetBoolean()
            : fund ? false : throw new ArgumentException("股票规则缺少市价保护要求");
        return new Terms(settlement, commission, minimum, tax, transfer, fund, protection,
            limitBuy, marketBuy, limitSell, marketSell);
    }

    public QuantityGrid Grid(bool buy, bool market) => (buy, market) switch
    {
        (true, true) => _terms.MarketBuy,
        (true, false) => _terms.LimitBuy,
        (false, true) => _terms.MarketSell,
        _ => _terms.LimitSell,
    };

    private QuantityGrid ReadGrid(string side, bool fund)
    {
        var minimum = OptionalNumber($"{side}_min_quantity") ?? (fund ? Number("buy_lot_units") : Number($"{side}_min_quantity"));
        var step = OptionalNumber($"{side}_quantity_step") ?? (fund ? Number("buy_lot_units") : Number($"{side}_quantity_step"));
        return new QuantityGrid(minimum, step,
            OptionalNumber($"{side}_max_quantity"), side == "sell" && Parameter("sell_remainder_allowed").GetBoolean());
    }

    public decimal Number(string name) => Integer(name, Parameter(name));

    private static decimal Integer(string name, ParameterValue value)
    {
        var n = value.GetDecimal();
        if (n != decimal.Truncate(n)) throw new ArgumentException($"参数必须是整数 {name}");
        return n;
    }
    private decimal? OptionalNumber(string name) => TryParameter(name, out var e) && e.ValueKind != JsonValueKind.Null ? Integer(name, e) : null;
    private static DateTimeOffset? OptionalTimestamp(JsonElement row, string name) => row.TryGetProperty(name, out var e) && e.ValueKind != JsonValueKind.Null ? Timestamp(e.GetString()!) : null;
    private static DateTimeOffset Timestamp(string text)
    {
        if (!(text.EndsWith('Z') || (text.Length >= 6 && (text[^6] == '+' || text[^6] == '-'))))
            throw new ArgumentException("规则时间必须带时区");
        return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
    }
    public static ChinaRule Parse(JsonElement row) => new(
        row.GetProperty("instrument_id").GetString() ?? throw new ArgumentException("缺少证券身份"),
        DateOnly.ParseExact(row.GetProperty("effective_from").GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture),
        DateOnly.ParseExact(row.GetProperty("effective_to").GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture),
        Timestamp(row.GetProperty("available_at").GetString()!),
        OptionalTimestamp(row, "valid_from"), OptionalTimestamp(row, "valid_to"),
        row.GetProperty("parameters").Clone(), null, null);

    internal static ChinaRule FromFrozenDay(string instrument, DateOnly day, DateTimeOffset available,
        DateTimeOffset validFrom, DateTimeOffset validTo, FrozenTemplate template, FrozenOverrides? overrides,
        bool validatedLimits)
        => new(instrument, day, day, available, validFrom, validTo, default, template, overrides, validatedLimits);
}


