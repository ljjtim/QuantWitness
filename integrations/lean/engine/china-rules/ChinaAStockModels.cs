using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.Fills;
using QuantConnect.Securities;

namespace QuantWitness.Lean.China;

/// <summary>共用只读规则与成交确认状态；公司行动锁股数量由外部服务提供。</summary>
public sealed partial class ChinaAStockModels
{
    private sealed class OrderCost
    {
        public decimal Quantity;
        public decimal CommissionNumerator;
        public decimal CommissionFen;
        public decimal SpentCny;
    }
    private readonly ChinaRuleBook _book;
    private readonly Func<Symbol, string> _instrumentId;
    private readonly Func<Symbol, decimal> _lockedBonusQuantity;
    private readonly Func<Symbol, string> _assetClass;
    private readonly Dictionary<(Symbol, DateOnly), decimal> _buys = new();
    private readonly Dictionary<int, OrderCost> _costs = new();
    private readonly HashSet<(int, int)> _confirmedEvents = new();
    public ChinaAStockFillModel FillModel { get; }
    public ChinaAStockFeeModel FeeModel { get; }
    public ChinaAStockBrokerageModel BrokerageModel { get; }
    public Func<DateTime, int, bool>? BeforeDailyOpen { get; set; }

    public ChinaAStockModels(ChinaRuleBook ruleBook, Func<Symbol, string> instrumentId, Func<Symbol, decimal> lockedBonusQuantity,
        Func<Symbol, string>? assetClass = null)
    {
        _book = ruleBook;
        _instrumentId = instrumentId;
        _lockedBonusQuantity = lockedBonusQuantity;
        _assetClass = assetClass ?? (_ => "cn_stock");
        FillModel = new ChinaAStockFillModel(this);
        FeeModel = new ChinaAStockFeeModel(this);
        BrokerageModel = new ChinaAStockBrokerageModel(this);
    }

    public void Install(QCAlgorithm algorithm)
    {
        BindPortfolio(algorithm.Portfolio);
        _installed = true;
        algorithm.SetBrokerageModel(BrokerageModel);
        algorithm.AddSecurityInitializer(InitializeSecurity);
        foreach (var security in algorithm.Securities.Values) InitializeSecurity(security);
    }

    public void InitializeSecurity(Security security)
    {
        if (security.Type != SecurityType.Equity || security.QuoteCurrency.Symbol != "CNY"
            || _assetClass(security.Symbol) is not ("cn_stock" or "cn_etf"))
            throw new NotSupportedException("中国现货规则仅接收人民币A股或ETF");
        if (security.SymbolProperties.LotSize != 1 || (security.SymbolProperties.MinimumOrderSize.HasValue && security.SymbolProperties.MinimumOrderSize != 1))
            throw new InvalidOperationException("LEAN 基础 LotSize 必须为 1、MinimumOrderSize 必须为 1 或未设置，历史数量规则由本扩展检查");
        security.SetFillModel(FillModel);
        security.SetFeeModel(FeeModel);
        security.SetSettlementModel(new ImmediateSettlementModel());
        security.SetBuyingPowerModel(new ChinaBudgetBuyingPowerModel(this));
        security.PriceVariationModel = new ChinaPriceVariationModel(this);

    }

    public decimal SellableQuantity(Security security, DateTime utcTime)
    {
        var day = _book.TradingDate(utcTime);
        var locked = _lockedBonusQuantity(security.Symbol);
        if (locked < 0 || locked != decimal.Truncate(locked)) throw new InvalidOperationException("外部锁定股份必须为非负整数");
        var rule = Rule(security.Symbol, utcTime);
        var today = rule.SettlementDays == 0 ? 0 : _buys.GetValueOrDefault((security.Symbol, day));
        return Math.Max(0, security.Holdings.Quantity - today - locked);
    }

    internal static bool Supported(OrderType type) => type is OrderType.Market or OrderType.Limit or OrderType.MarketOnOpen;
    internal static DateTime Now(Security security) => security.LocalTime.ConvertToUtc(security.Exchange.TimeZone);
    internal ChinaRule Rule(Symbol symbol, DateTime utc) => _book.Resolve(_instrumentId(symbol), utc);
    private OrderCost Cost(int id) => _costs.TryGetValue(id, out var state) ? state : new OrderCost();
    internal decimal Remaining(Order order) => Math.Max(0, Math.Abs(order.Quantity) - Cost(order.Id).Quantity);

    internal string? Validate(Security security, Order order, DateTime utc, decimal? requested = null, decimal? limitPrice = null)
    {
        if (!Supported(order.Type)) return $"不支持的订单类型：{order.Type}；仅支持 Market、Limit、MarketOnOpen";
        if (security.Type != SecurityType.Equity || security.QuoteCurrency.Symbol != "CNY") return "仅支持人民币 A 股现货";
        var rule = Rule(security.Symbol, utc);
        if (_assetClass(security.Symbol) == "cn_stock" && rule.SettlementDays != 1)
            return "A股股票必须使用T+1规则";
        rule.RequireVisibleReference(utc);
        if (rule.Paused) return "当时处于停牌状态";

        var signed = requested ?? order.Quantity;
        if (signed == 0 || Math.Sign(signed) != Math.Sign(order.Quantity)) return "不支持零数量或修改买卖方向";
        var buy = signed > 0;
        var budget = (order.Properties as ChinaOrderProperties)?.BudgetCny;
        if (budget.HasValue && (!buy || budget <= 0 || budget * 100 != decimal.Truncate(budget.Value * 100)))
            return "BudgetCny 仅支持买单，须为正数且精确到分";
        var quantity = Math.Abs(signed);
        var sellable = buy ? 0 : SellableQuantity(security, utc);
        if (!buy && quantity - Cost(order.Id).Quantity > sellable) return "超过 T+1 可卖数量或存在锁定送股";
        if (!rule.Grid(buy, order.Type != OrderType.Limit).Accepts(quantity, sellable + Cost(order.Id).Quantity))
            return "申报数量不满足历史最小量、步长、上限或零股规则";
        var protection = (order.Properties as ChinaOrderProperties)?.ProtectionPrice;
        if (order.Type != OrderType.Limit && rule.MarketProtectionRequired && !protection.HasValue)
            return "历史规则要求 Market／MOO 提供 ProtectionPrice";
        if (protection.HasValue)
        {
            if (order.Type == OrderType.Limit) return "ProtectionPrice 仅用于 Market／MOO，Limit 使用自身限价";
            var units = protection.Value * (decimal)Math.Pow(10, rule.PriceScale);
            if (protection <= 0 || units % rule.PriceTickUnits != 0) return "保护价必须为正数且满足历史价格格点";
            if (rule.PriceLimitMode == "bounded" && (protection < rule.LowLimit || protection > rule.HighLimit))
                return "保护价超出当时可见涨跌停范围";
        }
        var price = limitPrice ?? (order as LimitOrder)?.LimitPrice;
        if (price.HasValue && (price <= 0 || price * (decimal)Math.Pow(10, rule.PriceScale) % rule.PriceTickUnits != 0))
            return "限价不满足报价精度";
        if (price.HasValue && rule.PriceLimitMode == "bounded" && (price < rule.LowLimit || price > rule.HighLimit))
            return "申报限价超出当时涨跌停范围";
        return null;
    }

    private static decimal FenNotional(decimal price, decimal quantity) => Math.Round(price * Math.Abs(quantity) * 100, 0, MidpointRounding.AwayFromZero);
    private static decimal CeilPpm(decimal numerator) => Math.Ceiling(numerator / 1_000_000);
    private static (decimal FeeFen, decimal CommissionFen, decimal Numerator) Quote(ChinaRule rule, OrderCost before, decimal price, decimal signedQuantity)
    {
        var amount = FenNotional(price, signedQuantity);
        if (amount == 0) return (0, before.CommissionFen, before.CommissionNumerator);
        var numerator = before.CommissionNumerator + amount * rule.CommissionPpm;
        var commission = Math.Max(before.CommissionFen, Math.Max(rule.MinimumCommissionFen, CeilPpm(numerator)));
        var fee = commission - before.CommissionFen + CeilPpm(amount * rule.TransferFeePpm)
            + (signedQuantity < 0 ? CeilPpm(amount * rule.SellTaxPpm) : 0);
        return (fee, commission, numerator);
    }

    public OrderFee QuoteFee(Security security, Order order, decimal price, decimal signedQuantity, DateTime utcTime)
    {
        if (!Supported(order.Type)) throw new NotSupportedException($"不支持的订单类型 {order.Type}");
        var rule = Rule(security.Symbol, utcTime);
        var quote = Quote(rule, Cost(order.Id), price, signedQuantity);
        return new OrderFee(new CashAmount(quote.FeeFen / 100, "CNY"));
    }

    /// <summary>供购买力与未实现盈亏估费调用；不检查当日交易准入，也不推进成交状态。</summary>
    public OrderFee EstimateFee(Security security, Order order)
    {
        if (!Supported(order.Type)) throw new NotSupportedException($"不支持的订单类型 {order.Type}");
        var rule = _book.ResolveForFeeEstimate(_instrumentId(security.Symbol), Now(security));
        var price = (order as LimitOrder)?.LimitPrice ?? security.Price;
        var quantity = Math.Sign(order.Quantity) * Remaining(order);
        var quote = Quote(rule, Cost(order.Id), price, quantity);
        return new OrderFee(new CashAmount(quote.FeeFen / 100, "CNY"));
    }
    /// <summary>由算法真实成交回调调用一次；重复交付同一订单事件不会再次占用股份或佣金。</summary>
    public void OnOrderEvent(OrderEvent fill)
    {
        if (fill.Status is OrderStatus.Canceled or OrderStatus.Invalid) { ReleaseExecution(fill.OrderId); return; }
        if (fill.Status is not (OrderStatus.Filled or OrderStatus.PartiallyFilled) || fill.FillQuantity == 0) return;
        if (_confirmedEvents.Contains((fill.OrderId, fill.Id))) return;
        if (fill.FillQuantity != decimal.Truncate(fill.FillQuantity) || fill.FillPrice <= 0) throw new InvalidOperationException("成交数量或价格无效");
        var day = _book.TradingDate(fill.UtcTime);
        var rule = Rule(fill.Symbol, fill.UtcTime);
        var state = Cost(fill.OrderId);
        var quote = Quote(rule, state, fill.FillPrice, fill.FillQuantity);
        if (fill.OrderFee.Value.Currency != "CNY" || fill.OrderFee.Value.Amount != quote.FeeFen / 100)
            throw new InvalidOperationException("实际成交费用与时点规则不一致");
        if (fill.FillQuantity > 0)
            _buys[(fill.Symbol, day)] = _buys.GetValueOrDefault((fill.Symbol, day)) + fill.FillQuantity;
        state.Quantity += Math.Abs(fill.FillQuantity);
        state.CommissionFen = quote.CommissionFen;
        state.CommissionNumerator = quote.Numerator;
        state.SpentCny += FenNotional(fill.FillPrice, fill.FillQuantity) / 100 + quote.FeeFen / 100;
        _costs[fill.OrderId] = state;
        _confirmedEvents.Add((fill.OrderId, fill.Id));
        ConfirmExecution(fill);
    }

    internal OrderEvent Apply(Security security, Order order, OrderEvent fill)
    {
        if (fill.FillQuantity == 0) return fill;
        var utc = Now(security);
        var bar = security.Cache.GetData<TradeBar>();
        var dailyOpen = bar is DailyOpenBar;
        if (bar == null || (!dailyOpen && bar.Period != TimeSpan.FromMinutes(1)) || bar.IsFillForward)
            throw new InvalidOperationException("仅支持真实、完整、非填充的一分钟 TradeBar");
        var start = bar.Time.ConvertToUtc(security.Exchange.TimeZone);
        var end = bar.EndTime.ConvertToUtc(security.Exchange.TimeZone);
        if (end > utc || end != utc) throw new InvalidOperationException("行情分钟尚未完成或未与当前时钟对齐");
        var rule = Rule(security.Symbol, utc);
        rule.RequireVisibleReference(utc);
        if (order.Type == OrderType.MarketOnOpen && (rule.AvailableAt.UtcDateTime > start || rule.ValidFrom?.UtcDateTime > start))
            throw new InvalidOperationException("MOO 开盘消费不能使用分钟内才出现的规则");
        if (order.Type == OrderType.MarketOnOpen) rule.RequireVisibleReference(start);
        var reason = Validate(security, order, utc);
        // 订单只能使用提交或修改之后开始的分钟，不能回填已完成分钟成交。
        if (order.Time > start || (order.LastUpdateTime.HasValue && order.LastUpdateTime.Value > start))
            reason = "订单提交或修改晚于该分钟开始，等待下一完整分钟";
        if (!dailyOpen && _book.SuspendedDuring(_instrumentId(security.Symbol), start, end)) reason = "分钟跨过停牌区间";
        if (!dailyOpen && bar.Volume <= 0) reason = "当前已完成分钟没有成交量";
        if (!dailyOpen && !security.Exchange.Hours.IsOpen(bar.Time, bar.EndTime, false)) reason = "分钟不处于常规交易时段";
        if (order.Type == OrderType.MarketOnOpen && bar.Time != security.Exchange.Hours.GetNextMarketOpen(bar.Time.Date, false))
            reason = "MOO 仅使用正式开市的首个完整分钟，缺失开盘分钟不能用较晚分钟替代";
        if (_executionConfigured) fill.FillPrice = ExecutionPrice(fill.FillPrice, order.Quantity > 0, rule);
        if (fill.FillPrice <= 0) reason = "滑点后价格非正";
        if (order is LimitOrder limit && (order.Quantity > 0 ? fill.FillPrice > limit.LimitPrice : fill.FillPrice < limit.LimitPrice))
            reason = "滑点后价格超出限价";
        if (rule.PriceLimitMode == "bounded")
        {
            if (fill.FillPrice < rule.LowLimit || fill.FillPrice > rule.HighLimit) reason = "候选成交价格超出涨跌停范围";
            if (order.Direction == OrderDirection.Buy && fill.FillPrice >= rule.HighLimit) reason = "涨停买入受限";
            if (order.Direction == OrderDirection.Sell && fill.FillPrice <= rule.LowLimit) reason = "跌停卖出受限";
        }
        var protection = (order.Properties as ChinaOrderProperties)?.ProtectionPrice;
        if (protection.HasValue && ((order.Direction == OrderDirection.Buy && fill.FillPrice > protection)
            || (order.Direction == OrderDirection.Sell && fill.FillPrice < protection)))
            reason = "原生候选成交价超出市价保护价";
        if (reason != null)
        {
            fill.FillQuantity = 0;
            fill.FillPrice = 0;
            fill.Status = order.Status;
            fill.OrderFee = OrderFee.Zero;
            fill.Message = reason;
            _executionStatus[order.Id] = reason;
            return fill;
        }
        var quantity = ExecutionQuantity(security, order, fill.FillPrice, bar, utc);
        fill.FillQuantity = Math.Sign(order.Quantity) * quantity;
        fill.Status = quantity == Remaining(order) && quantity > 0 ? OrderStatus.Filled : quantity > 0 ? OrderStatus.PartiallyFilled : order.Status;
        if (quantity == 0) { fill.FillPrice = 0; fill.OrderFee = OrderFee.Zero; fill.Message = ExecutionStatus(order.Id); return fill; }
        fill.OrderFee = QuoteFee(security, order, fill.FillPrice, fill.FillQuantity, utc);
        TrackCandidate(security, order, fill, bar, utc);
        return fill;
    }

    internal OrderEvent DailyOpenFill(Security security, Order order)
    {
        var utc = Now(security);
        var fill = new OrderEvent(order.Id, order.Symbol, utc, order.Status, order.Direction, 0, 0, OrderFee.Zero);
        var bar = security.Cache.GetData<TradeBar>();
        var sameOpenDecision = (order.Properties as ChinaOrderProperties)?.DailyOpenDecision == true;
        if (bar is not DailyOpenBar || bar.Time.ConvertToUtc(security.Exchange.TimeZone) != utc
            || order.Time > utc || order.Time == utc && !sameOpenDecision || order.LastUpdateTime >= utc) return fill;
        if (BeforeDailyOpen != null && !BeforeDailyOpen(utc, order.Id)) return fill;
        fill.FillPrice = bar.Open;
        fill.FillQuantity = Math.Sign(order.Quantity) * Remaining(order);
        fill.Status = OrderStatus.Filled;
        return Apply(security, order, fill);
    }
}

public sealed class ChinaAStockFeeModel(ChinaAStockModels models) : FeeModel
{
    public override OrderFee GetOrderFee(OrderFeeParameters parameters) => models.EstimateFee(parameters.Security, parameters.Order);
}

public sealed class ChinaPriceVariationModel(ChinaAStockModels models) : IPriceVariationModel
{
    public decimal GetMinimumPriceVariation(GetMinimumPriceVariationParameters parameters)
    {
        var rule = models.Rule(parameters.Security.Symbol, ChinaAStockModels.Now(parameters.Security));
        return rule.PriceTickUnits / (decimal)Math.Pow(10, rule.PriceScale);
    }
}

public sealed class ChinaAStockFillModel(ChinaAStockModels models) : EquityFillModel
{
    public override Fill Fill(FillModelParameters parameters)
    {
        if (!ChinaAStockModels.Supported(parameters.Order.Type)) throw Unsupported(parameters.Order);
        var configs = parameters.ConfigProvider.GetSubscriptionDataConfigs(parameters.Security.Symbol);
        if (configs.Count == 0 || configs.Any(c => c.Type != typeof(TradeBar) || c.Resolution is not (Resolution.Minute or Resolution.Daily)
            || c.DataNormalizationMode != DataNormalizationMode.Raw || c.FillDataForward || c.ExtendedMarketHours))
            throw new NotSupportedException("仅支持 Raw、非填充、非延长时段的股票日线或一分钟 TradeBar 订阅");
        if (configs.Any(c => c.Resolution == Resolution.Daily) && (parameters.Order.Type != OrderType.Market
            || (parameters.Order.Properties as ChinaOrderProperties)?.DailyOpenDecision != true))
            throw new NotSupportedException("日线仅支持已可见前日日线决策后的原生Market开盘执行");
        return base.Fill(parameters);
    }
    public override OrderEvent MarketFill(Security security, MarketOrder order)
        => security.Subscriptions.Any(c => c.Resolution == Resolution.Daily)
            ? models.DailyOpenFill(security, order) : models.Apply(security, order, base.MarketFill(security, order));
    public override OrderEvent LimitFill(Security security, LimitOrder order) => models.Apply(security, order, base.LimitFill(security, order));
    public override OrderEvent MarketOnOpenFill(Security security, MarketOnOpenOrder order)
        => security.Subscriptions.Any(c => c.Resolution == Resolution.Daily)
            ? models.DailyOpenFill(security, order) : models.Apply(security, order, base.MarketOnOpenFill(security, order));
    private static NotSupportedException Unsupported(Order order) => new($"中国 A 股扩展不支持 {order.Type}；仅支持 Market、Limit、MarketOnOpen");
    public override OrderEvent MarketOnCloseFill(Security security, MarketOnCloseOrder order) => throw Unsupported(order);
    public override OrderEvent StopMarketFill(Security security, StopMarketOrder order) => throw Unsupported(order);
    public override OrderEvent StopLimitFill(Security security, StopLimitOrder order) => throw Unsupported(order);
    public override OrderEvent LimitIfTouchedFill(Security security, LimitIfTouchedOrder order) => throw Unsupported(order);
    public override OrderEvent TrailingStopFill(Security security, TrailingStopOrder order) => throw Unsupported(order);
    public override List<OrderEvent> ComboMarketFill(Order order, FillModelParameters parameters) => throw Unsupported(order);
    public override List<OrderEvent> ComboLimitFill(Order order, FillModelParameters parameters) => throw Unsupported(order);
    public override List<OrderEvent> ComboLegLimitFill(Order order, FillModelParameters parameters) => throw Unsupported(order);
}

public sealed class ChinaAStockBrokerageModel(ChinaAStockModels models) : DefaultBrokerageModel(AccountType.Cash)
{
    public override bool CanSubmitOrder(Security security, Order order, out BrokerageMessageEvent message)
    {
        string? reason;
        try { reason = models.Validate(security, order, order.Time); }
        catch (Exception error) when (error is InvalidOperationException or ArgumentException or KeyNotFoundException)
        { reason = error.Message; }
        message = reason == null ? null! : new BrokerageMessageEvent(BrokerageMessageType.Warning, "ChinaRuleRejected", reason);
        return reason == null && base.CanSubmitOrder(security, order, out message);
    }
    public override bool CanUpdateOrder(Security security, Order order, UpdateOrderRequest request, out BrokerageMessageEvent message)
    {
        string? reason;
        try { reason = models.Validate(security, order, request.Time, request.Quantity, request.LimitPrice); }
        catch (Exception error) when (error is InvalidOperationException or ArgumentException or KeyNotFoundException)
        { reason = error.Message; }
        message = reason == null ? null! : new BrokerageMessageEvent(BrokerageMessageType.Warning, "ChinaRuleRejected", reason);
        return reason == null && base.CanUpdateOrder(security, order, request, out message);
    }
    public override IFillModel GetFillModel(Security security) => models.FillModel;
    public override IFeeModel GetFeeModel(Security security) => models.FeeModel;
    public override ISettlementModel GetSettlementModel(Security security) => new ImmediateSettlementModel();
    public override IBuyingPowerModel GetBuyingPowerModel(Security security) => new ChinaBudgetBuyingPowerModel(models);
}





