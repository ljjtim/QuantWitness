using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.Fills;
using QuantConnect.Securities;

namespace QuantWitness.Lean.China;

public sealed partial class ChinaFutureModels
{
    private sealed class Reservation
    {
        public decimal Money;
        public decimal Remaining;
        public Dictionary<(string Direction, string Bucket), decimal> Quantities = new();
    }
    private readonly Dictionary<int, Reservation> _reservations = new();
    private readonly Dictionary<(Symbol, DateTime), decimal> _capacityUsed = new();
    private readonly HashSet<int> _iocAttempted = new();
    private decimal _slippageBps;
    private int _slippageTicks;
    private int _participationPpm = 1_000_000;
    public decimal Frozen => _reservations.Values.Sum(x => x.Money);
    public decimal Available => _portfolio.TotalPortfolioValue - _portfolio.TotalMarginUsed - Frozen;
    public bool RiskActive { get; set; }
    public event Action<string, Order, decimal>? ReservationChanged;
    public IReadOnlyCollection<int> IocAttempted => _iocAttempted;
    internal decimal Remaining(Order order) => Math.Max(0, order.AbsoluteQuantity - _filled.GetValueOrDefault(order.Id));
    internal decimal ReservedMoney(int id) => _reservations.TryGetValue(id, out var state) ? state.Money : 0;
    private decimal ReservedQuantity(Symbol symbol, string direction, string bucket, int except) =>
        _reservations.Where(x => x.Key != except && _orders[x.Key].Symbol == symbol)
            .Sum(x => x.Value.Quantities.GetValueOrDefault((direction, bucket)));
    public decimal ReservedClose(Symbol symbol, string direction, string bucket) => ReservedQuantity(symbol, direction, bucket, -1);
    public decimal OrderFrozen(int orderId) => ReservedMoney(orderId);

    /// <summary>当前机会的减仓容量；尚无当前行情返回null，不消费容量或改变订单状态。</summary>
    public decimal? RiskCloseCapacity(Security security, decimal signedQuantity)
    {
        var utc = security.LocalTime.ConvertToUtc(security.Exchange.TimeZone);
        var bar = security.Cache.GetData<TradeBar>();
        if (bar == null || bar.IsFillForward || bar.EndTime.ConvertToUtc(security.Exchange.TimeZone) != utc
            || (bar is not DailyOpenBar && bar.Period != TimeSpan.FromMinutes(1))) return null;
        var rule = _rules(security.Symbol, utc);
        var sign = Math.Sign(signedQuantity);
        var raw = bar is DailyOpenBar ? bar.Open : bar.Close;
        var shifted = raw * (1 + sign * _slippageBps / 10_000m) + sign * _slippageTicks * rule.Tick;
        var price = (sign > 0 ? decimal.Ceiling(shifted / rule.Tick) : decimal.Floor(shifted / rule.Tick)) * rule.Tick;
        if (price <= 0 || price < rule.LowerLimit || price > rule.UpperLimit
            || sign > 0 && price >= rule.UpperLimit || sign < 0 && price <= rule.LowerLimit) return 0;
        return bar is DailyOpenBar ? Math.Abs(signedQuantity)
            : Math.Max(0, decimal.Floor(bar.Volume * _participationPpm / 1_000_000m)
                - _capacityUsed.GetValueOrDefault((security.Symbol, utc)));
    }

    public void Install(QCAlgorithm algorithm)
    {
        algorithm.SetBrokerageModel(new ChinaFutureBrokerageModel(this));
        algorithm.Portfolio.MarginCallModel = MarginCallModel.Null;
        algorithm.AddSecurityInitializer(InitializeSecurity);
        foreach (var security in algorithm.Securities.Values) InitializeSecurity(security);
        InstallGroups();
    }

    public void ConfigureExecution(decimal slippageBps, int slippageTicks, int participationPpm)
    {
        if (slippageBps < 0 || slippageTicks < 0 || participationPpm is < 0 or > 1_000_000)
            throw new ArgumentException("期货滑点或参与率无效");
        _slippageBps = slippageBps; _slippageTicks = slippageTicks; _participationPpm = participationPpm;
    }

    private static decimal OpeningFunds(Security security, FutureRule rule, decimal price, decimal quantity)
    {
        var count = Math.Abs(quantity);
        return count * price * security.SymbolProperties.ContractMultiplier
            * (quantity > 0 ? rule.LongMarginRate : rule.ShortMarginRate)
            + decimal.Round(rule.OpenFee.Quote(count, price, security.SymbolProperties.ContractMultiplier),
                2, MidpointRounding.AwayFromZero);
    }

    public decimal AffordableOpeningQuantity(Security security, decimal requested, decimal limit, DateTime utc)
    {
        var rule = _rules(security.Symbol, utc);
        var price = limit > 0 ? limit : rule.UpperLimit ?? security.Holdings.Price;
        decimal low = 0, high = Math.Abs(requested);
        while (low < high)
        {
            var middle = decimal.Floor((low + high + 1) / 2);
            if (OpeningFunds(security, rule, price, Math.Sign(requested) * middle) <= Available) low = middle;
            else high = middle - 1;
        }
        return Math.Sign(requested) * low;
    }

    public string? Reserve(Security security, Order order)
    {
        if (_reservations.ContainsKey(order.Id)) return null;
        if (order.Properties is not ChinaFutureOrderProperties props) return "期货订单缺少显式开平";
        if (order.Type is not (OrderType.Market or OrderType.Limit) || props.Duration is not ("DAY" or "IOC"))
            return "期货仅支持market/limit及DAY/IOC";
        if (RiskActive && !props.RiskOrder) return "账户风险处置期间暂停策略交易";
        if (props.PositionEffect == "open" && OpenAllowed != null && !OpenAllowed(security.Symbol)) return "合约已进入退出阶段，只能减仓";
        if (order.AbsoluteQuantity == 0 || order.AbsoluteQuantity != decimal.Truncate(order.AbsoluteQuantity))
            return "期货数量必须为正整数手";
        var rule = _rules(security.Symbol, order.Time);
        var price = (order as LimitOrder)?.LimitPrice ?? security.Holdings.Price;
        if (price <= 0 || price % rule.Tick != 0) return "期货价格不满足历史格点";
        if (props.ProtectionPrice.HasValue && (props.ProtectionPrice <= 0 || props.ProtectionPrice % rule.Tick != 0))
            return "期货保护价不满足历史格点";
        if (order is LimitOrder && (price < rule.LowerLimit || price > rule.UpperLimit)) return "限价超出涨跌停";
        IReadOnlyList<FutureFillLeg> legs;
        try { legs = Preview(security, order, price, order.Quantity); }
        catch (InvalidOperationException error) { return error.Message; }
        var state = new Reservation { Remaining = order.AbsoluteQuantity };
        if (props.PositionEffect == "open")
        {
            // 市价开仓按可见涨停价冻结，避免低估下一成交机会所需资金。
            var reservePrice = order is LimitOrder ? price : rule.UpperLimit ?? price;
            state.Money = OpeningFunds(security, rule, reservePrice, order.Quantity);
            if (state.Money > Available) return "共享期货账户可用保证金不足";
        }
        else foreach (var leg in legs) state.Quantities[(leg.Direction, leg.Bucket)] = leg.Quantity;
        Register(order);
        _reservations[order.Id] = state;
        ReservationChanged?.Invoke("reserve", order, state.Money);
        return null;
    }

    private void CommitReservation(Security security, Order order, IReadOnlyList<FutureFillLeg> legs)
    {
        if (!_reservations.TryGetValue(order.Id, out var state)) return;
        var quantity = legs.Sum(x => x.Quantity);
        var released = state.Money * quantity / state.Remaining;
        state.Money -= released;
        state.Remaining -= quantity;
        foreach (var leg in legs)
        {
            var key = (leg.Direction, leg.Bucket);
            if (state.Quantities.ContainsKey(key)) state.Quantities[key] -= leg.Quantity;
        }
        if (state.Remaining == 0) _reservations.Remove(order.Id);
        ReservationChanged?.Invoke("fill_release", order, released);
    }

    public void OnOrderEvent(OrderEvent value)
    {
        if (value.Status is not (OrderStatus.Canceled or OrderStatus.Invalid or OrderStatus.Filled)) return;
        if (_reservations.Remove(value.OrderId, out var state))
            ReservationChanged?.Invoke("release", _orders[value.OrderId], state.Money);
        _iocAttempted.Remove(value.OrderId);
    }

    public OrderEvent Candidate(Security security, Order order)
    {
        var utc = security.LocalTime.ConvertToUtc(security.Exchange.TimeZone);
        var fill = new OrderEvent(order, utc, OrderFee.Zero) { Status = order.Status };
        var bar = security.Cache.GetData<TradeBar>();
        if (bar == null || bar.IsFillForward || order.Properties is not ChinaFutureOrderProperties props) return fill;
        var daily = bar is DailyOpenBar;
        var start = bar.Time.ConvertToUtc(security.Exchange.TimeZone);
        if (bar.EndTime.ConvertToUtc(security.Exchange.TimeZone) != utc || order.Time > start
            || order.LastUpdateTime > start || (!daily && bar.Period != TimeSpan.FromMinutes(1))
            || (daily && order.Time == start && !props.DailyOpenDecision) || utc > props.ValidUntilUtc) return fill;
        if (BeforeExecution != null && !BeforeExecution(utc, order)) return fill;
        if (RiskActive && !props.RiskOrder) return fill;
        if (_iocAttempted.Contains(order.Id)) return fill;
        if (props.Duration == "IOC") _iocAttempted.Add(order.Id);
        var rule = _rules(security.Symbol, utc);
        var raw = daily ? bar.Open : bar.Close;
        var shifted = raw * (1 + Math.Sign(order.Quantity) * _slippageBps / 10_000m)
            + Math.Sign(order.Quantity) * _slippageTicks * rule.Tick;
        var price = (order.Quantity > 0 ? decimal.Ceiling(shifted / rule.Tick) : decimal.Floor(shifted / rule.Tick)) * rule.Tick;
        if (price <= 0 || price < rule.LowerLimit || price > rule.UpperLimit
            || order.Quantity > 0 && price >= rule.UpperLimit || order.Quantity < 0 && price <= rule.LowerLimit
            || props.ProtectionPrice.HasValue && (order.Quantity > 0 ? price > props.ProtectionPrice : price < props.ProtectionPrice)
            || order is LimitOrder limit && (order.Quantity > 0 ? price > limit.LimitPrice : price < limit.LimitPrice)) return fill;
        var key = (security.Symbol, utc);
        var capacity = daily ? Remaining(order) : Math.Max(0, decimal.Floor(bar.Volume * _participationPpm / 1_000_000m)
            - _capacityUsed.GetValueOrDefault(key));
        var quantity = Math.Min(Remaining(order), capacity);
        if (props.PositionEffect == "open")
        {
            var perHand = price * security.SymbolProperties.ContractMultiplier * (order.Quantity > 0 ? rule.LongMarginRate : rule.ShortMarginRate)
                + rule.OpenFee.Quote(1, price, security.SymbolProperties.ContractMultiplier);
            var releasedPerHand = _reservations.TryGetValue(order.Id, out var reserved)
                ? reserved.Money / reserved.Remaining : 0;
            if (Available < 0) quantity = 0;
            else if (perHand > releasedPerHand)
                quantity = Math.Min(quantity, decimal.Floor(Available / (perHand - releasedPerHand)));
        }
        if (quantity == 0) return fill;
        var signed = Math.Sign(order.Quantity) * quantity;
        Preview(security, order, price, signed);
        fill.FillPrice = price; fill.FillQuantity = signed;
        fill.OrderFee = new OrderFee(new CashAmount(QuoteFee(security, order, price, signed), "CNY"));
        fill.Status = quantity == Remaining(order) ? OrderStatus.Filled : OrderStatus.PartiallyFilled;
        if (!daily) _capacityUsed[key] = _capacityUsed.GetValueOrDefault(key) + quantity;
        foreach (var expired in _capacityUsed.Keys.Where(x => x.Item2 < utc).ToArray()) _capacityUsed.Remove(expired);
        return fill;
    }
}

public sealed class ChinaFutureFillModel(ChinaFutureModels models) : FillModel
{
    public override OrderEvent MarketFill(Security security, MarketOrder order) => models.Candidate(security, order);
    public override OrderEvent LimitFill(Security security, LimitOrder order) => models.Candidate(security, order);
}

public sealed class ChinaFutureBrokerageModel(ChinaFutureModels models) : DefaultBrokerageModel(AccountType.Margin)
{
    public override bool CanSubmitOrder(Security security, Order order, out BrokerageMessageEvent message)
    {
        var reason = models.Reserve(security, order);
        message = reason == null ? null! : new BrokerageMessageEvent(BrokerageMessageType.Warning, "ChinaFutureRejected", reason);
        return reason == null;
    }
    public override bool CanUpdateOrder(Security security, Order order, UpdateOrderRequest request, out BrokerageMessageEvent message)
    {
        message = new BrokerageMessageEvent(BrokerageMessageType.Warning, "ChinaFutureRejected", "期货改单须先撤单再提交");
        return false;
    }
}

/// <summary>DAY以冻结交易日结束为界，跨午夜及小节休市不提前到期。</summary>
public sealed class ChinaFutureTimeInForce : TimeInForce
{
    public override bool IsOrderExpired(Security security, Order order) =>
        security.LocalTime.ConvertToUtc(security.Exchange.TimeZone) > ((ChinaFutureOrderProperties)order.Properties).ValidUntilUtc;
    public override bool IsFillValid(Security security, Order order, OrderEvent fill) =>
        fill.UtcTime <= ((ChinaFutureOrderProperties)order.Properties).ValidUntilUtc;
}
