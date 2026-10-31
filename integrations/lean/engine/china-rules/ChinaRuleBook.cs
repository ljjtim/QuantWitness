using System.Globalization;
using System.Text.Json;

namespace QuantWitness.Lean.China;

/// <summary>消费已冻结的逐日或时段规则；金额单位为分，费率单位为百万分之一。</summary>
public sealed class ChinaRuleBook
{
    private Dictionary<string, ChinaRule[]> _rules;
    private readonly HashSet<DateOnly> _sessions;
    private readonly Func<DateOnly, IEnumerable<ChinaRule>>? _dailyLoader;
    private DateOnly? _loadedDay;
    public IReadOnlyCollection<DateOnly> TradingDates => _sessions;

    public static ChinaRuleBook Load(string path) => Parse(File.ReadAllText(path));

    public static ChinaRuleBook Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return new ChinaRuleBook(
            doc.RootElement.GetProperty("trading_dates").EnumerateArray()
                .Select(x => DateOnly.ParseExact(x.GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture)),
            doc.RootElement.GetProperty("rules").EnumerateArray().Select(ChinaRule.Parse));
    }

    public ChinaRuleBook(IEnumerable<DateOnly> tradingDates, IEnumerable<ChinaRule> rules)
    {
        _sessions = tradingDates.ToHashSet();
        if (_sessions.Count == 0) throw new ArgumentException("交易日历不能为空");
        _rules = rules.GroupBy(x => x.InstrumentId).ToDictionary(x => x.Key, x => x.OrderByDescending(r => r.AvailableAt).ToArray());
        if (_rules.Count == 0) throw new ArgumentException("历史规则不能为空");
    }

    public ChinaRuleBook(IEnumerable<DateOnly> tradingDates, Func<DateOnly, IEnumerable<ChinaRule>> dailyLoader)
    {
        _sessions = tradingDates.ToHashSet();
        if (_sessions.Count == 0) throw new ArgumentException("交易日历不能为空");
        _rules = new();
        _dailyLoader = dailyLoader;
    }

    private void EnsureDay(DateTime utc)
    {
        if (_dailyLoader == null) return;
        var day = DateOnly.FromDateTime(ShanghaiTime(utc).DateTime);
        if (!_sessions.Contains(day) || _loadedDay == day) return;
        if (_loadedDay.HasValue && day < _loadedDay) throw new InvalidOperationException("规则消费时钟不能倒退");
        var rows = _dailyLoader(day).ToList();
        // 仅保留最近已可见的历史估费依据，不把它当成当日交易规则。
        foreach (var previous in _rules.Values)
        {
            var fee = previous.Where(r => r.AvailableAt.UtcDateTime <= utc
                && (r.ValidFrom == null || r.ValidFrom.Value.UtcDateTime <= utc)
                && r.EffectiveFrom < day).OrderByDescending(r => r.EffectiveTo)
                .ThenByDescending(r => r.AvailableAt).ThenByDescending(r => r.ValidFrom).FirstOrDefault();
            if (fee != null && !rows.Any(r => r.InstrumentId == fee.InstrumentId
                && r.EffectiveFrom == fee.EffectiveFrom && r.EffectiveTo == fee.EffectiveTo
                && r.AvailableAt == fee.AvailableAt && r.ValidFrom == fee.ValidFrom && r.ValidTo == fee.ValidTo)) rows.Add(fee);
        }
        _rules = rows.GroupBy(r => r.InstrumentId).ToDictionary(g => g.Key,
            g => g.OrderByDescending(r => r.AvailableAt).ToArray());
        _loadedDay = day;
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
        if (!_rules.TryGetValue(instrumentId, out var rows)) throw new InvalidOperationException($"缺少证券规则 {instrumentId}");
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
        if (!_rules.TryGetValue(instrumentId, out var rows)) throw new InvalidOperationException($"缺少证券规则 {instrumentId}");
        foreach (var session in _sessions.Where(day => day <= today).OrderByDescending(day => day))
        {
            // 已结束时段可以供历史估费；尚未开始或尚不可见的规则不能供估费。
            var candidates = rows.Where(r => r.EffectiveFrom <= session && session <= r.EffectiveTo
                && r.AvailableAt <= at && (r.ValidFrom == null || r.ValidFrom <= at))
                .OrderByDescending(r => r.AvailableAt).ThenByDescending(r => r.ValidFrom).ToArray();
            if (candidates.Length == 0) continue;
            if (candidates.Skip(1).Any(r => r.AvailableAt == candidates[0].AvailableAt && r.ValidFrom == candidates[0].ValidFrom))
                throw new InvalidOperationException($"估费时点有歧义规则 {instrumentId} {at:O}");
            return candidates[0];
        }
        throw new InvalidOperationException($"估费时点之前没有已可见历史规则 {instrumentId} {at:O}");
    }
    /// <summary>检查整个已完成分钟，避免跨过停牌或不可见规则缺口。</summary>
    public bool SuspendedDuring(string instrumentId, DateTime startUtc, DateTime endUtc)
    {
        EnsureDay(endUtc);
        var points = new SortedSet<DateTime> { startUtc, endUtc };
        if (!_rules.TryGetValue(instrumentId, out var rows)) throw new InvalidOperationException("缺少证券规则");
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
    public string InstrumentId { get; }
    public DateOnly EffectiveFrom { get; }
    public DateOnly EffectiveTo { get; }
    public DateTimeOffset AvailableAt { get; }
    public DateTimeOffset? ValidFrom { get; }
    public DateTimeOffset? ValidTo { get; }
    public JsonElement Parameters { get; }
    private bool FundQuantityRule => Parameters.TryGetProperty("buy_lot_units", out _);
    public bool Paused => Parameters.TryGetProperty("paused", out var paused) ? paused.GetBoolean()
        : FundQuantityRule ? false : throw new ArgumentException("股票规则缺少停牌事实");
    public string PriceLimitMode => Parameters.TryGetProperty("price_limit_mode", out var mode) ? mode.GetString()!
        : FundQuantityRule ? "bounded" : throw new ArgumentException("股票规则缺少价格限制模式");
    public bool MarketProtectionRequired => Parameters.TryGetProperty("market_protection_required", out var protection)
        ? protection.GetBoolean() : FundQuantityRule ? false : throw new ArgumentException("股票规则缺少市价保护要求");
    public int PriceScale => checked((int)Number("price_scale"));
    public int SettlementDays => checked((int)Number("settlement_days"));
    public decimal PriceTickUnits => OptionalNumber("price_tick_units") ?? 1m;
    public decimal CommissionPpm => Number("commission_ppm");
    public decimal MinimumCommissionFen => Number("min_commission_units");
    public decimal SellTaxPpm => Number("sell_tax_ppm");
    public decimal TransferFeePpm => Number("transfer_fee_ppm");
    public decimal? HighLimit => OptionalNumber("high_limit_units") / Scale;
    public decimal? LowLimit => OptionalNumber("low_limit_units") / Scale;
    private decimal Scale => (decimal)Math.Pow(10, PriceScale);

    private ChinaRule(JsonElement row)
    {
        InstrumentId = row.GetProperty("instrument_id").GetString() ?? throw new ArgumentException("缺少证券身份");
        EffectiveFrom = DateOnly.ParseExact(row.GetProperty("effective_from").GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        EffectiveTo = DateOnly.ParseExact(row.GetProperty("effective_to").GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        AvailableAt = Timestamp(row.GetProperty("available_at").GetString()!);
        ValidFrom = OptionalTimestamp(row, "valid_from");
        ValidTo = OptionalTimestamp(row, "valid_to");
        Parameters = row.GetProperty("parameters").Clone();
        if (EffectiveFrom > EffectiveTo || (ValidFrom.HasValue && ValidTo.HasValue && ValidFrom >= ValidTo))
            throw new ArgumentException("规则有效区间倒置");
        if (string.IsNullOrWhiteSpace(InstrumentId) || SettlementDays is not (0 or 1))
            throw new ArgumentException("现货结算必须明确为T+0或T+1");
        if (PriceTickUnits <= 0) throw new ArgumentException("价格格点必须为正整数报价单位");
        foreach (var side in new[] { true, false })
        {
            var grid = Grid(side, false);
            if (grid.Minimum <= 0 || grid.Step <= 0 || grid.Maximum < grid.Minimum) throw new ArgumentException("数量格点无效");
            grid = Grid(side, true);
            if (grid.Maximum < grid.Minimum) throw new ArgumentException("市价数量上限无效");
        }
        if (CommissionPpm < 0 || MinimumCommissionFen < 0 || SellTaxPpm < 0 || TransferFeePpm < 0)
            throw new ArgumentException("费用不能为负数");
        if (PriceScale is < 0 or > 8) throw new ArgumentException("报价精度无效");
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
                if (!Paused || Parameters.GetProperty("trading_status").GetString() != "suspended") throw new ArgumentException("停牌模式与停牌事实不一致");
            }
            else
            {
                var reference = Number("reference_previous_close_units");
                var rate = Number("price_limit_ratio_ppm");
                if (reference <= 0 || rate <= 0 || rate >= 1_000_000 || LowLimit <= 0
                    || (Parameters.TryGetProperty("price_limit_rounding", out var rounding)
                        ? rounding.GetString() != "half_up_to_quote_unit" : !FundQuantityRule)
                    || Math.Round(reference * (1 + rate / 1_000_000), 0, MidpointRounding.AwayFromZero) != Number("high_limit_units")
                    || Math.Round(reference * (1 - rate / 1_000_000), 0, MidpointRounding.AwayFromZero) != Number("low_limit_units"))
                    throw new ArgumentException("涨跌停值与冻结参考价格、比例和舍位口径不一致");
            }
            _ = Timestamp(Parameters.GetProperty("reference_price_available_at").GetString()!);
        }
        else throw new ArgumentException("不支持的价格限制模式");
        _ = Paused;
        _ = MarketProtectionRequired;
    }

    public void RequireVisibleReference(DateTime utc)
    {
        if (PriceLimitMode != "unbounded" && Timestamp(Parameters.GetProperty("reference_price_available_at").GetString()!).UtcDateTime > utc)
            throw new InvalidOperationException("前收盘参考价在消费时尚不可见");
    }

    public QuantityGrid Grid(bool buy, bool market)
    {
        var side = buy ? "buy" : "sell";
        var maximum = market ? OptionalNumber($"market_{side}_max_quantity") : null;
        var minimum = OptionalNumber($"{side}_min_quantity") ?? (FundQuantityRule ? Number("buy_lot_units") : Number($"{side}_min_quantity"));
        var step = OptionalNumber($"{side}_quantity_step") ?? (FundQuantityRule ? Number("buy_lot_units") : Number($"{side}_quantity_step"));
        return new QuantityGrid(minimum, step,
            maximum ?? OptionalNumber($"{side}_max_quantity"), !buy && Parameters.GetProperty("sell_remainder_allowed").GetBoolean());
    }

    public decimal Number(string name)
    {
        var n = Parameters.GetProperty(name).GetDecimal();
        if (n != decimal.Truncate(n)) throw new ArgumentException($"参数必须是整数 {name}");
        return n;
    }
    private decimal? OptionalNumber(string name) => Parameters.TryGetProperty(name, out var e) && e.ValueKind != JsonValueKind.Null ? Number(name) : null;
    private static DateTimeOffset? OptionalTimestamp(JsonElement row, string name) => row.TryGetProperty(name, out var e) && e.ValueKind != JsonValueKind.Null ? Timestamp(e.GetString()!) : null;
    private static DateTimeOffset Timestamp(string text)
    {
        if (!(text.EndsWith('Z') || (text.Length >= 6 && (text[^6] == '+' || text[^6] == '-'))))
            throw new ArgumentException("规则时间必须带时区");
        return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
    }
    public static ChinaRule Parse(JsonElement row) => new(row);
}


