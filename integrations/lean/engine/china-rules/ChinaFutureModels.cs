using QuantConnect;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using QuantConnect.Securities.Future;
using QuantConnect.Securities.Positions;

namespace QuantWitness.Lean.China;

public sealed class ChinaFutureOrderProperties : OrderProperties
{
    public string PositionEffect { get; set; } = "open";
    public string ClosePriority { get; set; } = "yesterday_first";
    public bool RiskOrder { get; set; }
    public bool DailyOpenDecision { get; set; }
    public string? RollPlanId { get; set; }
    public string Duration { get; set; } = "DAY";
    public decimal? ProtectionPrice { get; set; }
    public DateTime ValidUntilUtc { get; set; } = DateTime.MaxValue;
    public override IOrderProperties Clone() => (ChinaFutureOrderProperties)MemberwiseClone();
}

public sealed record FutureFee(decimal PerContract, decimal NotionalRate)
{
    public decimal Quote(decimal quantity, decimal price, decimal multiplier) =>
        quantity * (PerContract + price * multiplier * NotionalRate);
}
public sealed record FutureRule(decimal LongMarginRate, decimal ShortMarginRate, FutureFee OpenFee,
    FutureFee CloseFee, FutureFee CloseTodayFee, decimal Tick, decimal? LowerLimit = null,
    decimal? UpperLimit = null, string ClosePriority = "yesterday_first", string RuleReference = "");

/// <summary>期货真实成交与显式结算共同使用原生现金及四桶持仓。</summary>
public sealed partial class ChinaFutureModels
{
    private readonly SecurityPortfolioManager _portfolio;
    private readonly Func<Symbol, DateTime, FutureRule> _rules;
    private readonly Dictionary<int, Order> _orders = new();
    private readonly Dictionary<int, decimal> _filled = new();
    private readonly HashSet<(Symbol, DateOnly)> _settled = new();
    private readonly HashSet<(int, int)> _committedFills = new();
    public event Action<Order, OrderEvent, IReadOnlyList<FutureFillLeg>>? FillCommitted;
    public Func<DateTime, Order, bool>? BeforeExecution { get; set; }
    public Func<Symbol, bool>? OpenAllowed { get; set; }

    public ChinaFutureModels(SecurityPortfolioManager portfolio, Func<Symbol, DateTime, FutureRule> rules)
    {
        _portfolio = portfolio;
        _rules = rules;
    }

    public void InstallGroups()
    {
        var groups = new FutureGroups();
        _portfolio.SetPositions(groups);
        foreach (var security in _portfolio.Securities.Values)
            if (security.Holdings is ChinaFutureHolding holding) holding.RefreshProjection();
    }

    public void InitializeSecurity(Security security)
    {
        if (security.Type != SecurityType.Future || security.Symbol.IsCanonical())
            throw new InvalidOperationException("期货模型必须安装到真实 Future 合约");
        security.Holdings = new ChinaFutureHolding(security, _portfolio.CashBook);
        security.PortfolioModel = new PortfolioModel(this);
        security.SetBuyingPowerModel(new BuyingPower(this));
        security.SetFeeModel(new FeeModel(this));
        security.SetSettlementModel(new ExplicitSettlement());
        security.SetFillModel(new ChinaFutureFillModel(this));
    }

    public void Register(Order order)
    {
        if (order.Properties is not ChinaFutureOrderProperties)
            throw new InvalidOperationException("期货订单缺少开平仓属性");
        _orders[order.Id] = order;
    }

    internal IReadOnlyList<FutureFillLeg> Preview(Security security, Order order, decimal price, decimal signed)
    {
        var props = (ChinaFutureOrderProperties)order.Properties;
        var holding = (ChinaFutureHolding)security.Holdings;
        var direction = ChinaFutureHolding.Direction(signed, props.PositionEffect);
        return holding.Preview(signed, props.PositionEffect, price, props.ClosePriority,
            bucket => holding.QuantityFor(direction, bucket) - ReservedQuantity(security.Symbol, direction, bucket, order.Id));
    }

    public decimal Margin(Security security)
    {
        var holding = (ChinaFutureHolding)security.Holdings;
        if (!holding.Invested) return 0;
        var rule = _rules(security.Symbol, security.LocalTime.ConvertToUtc(security.Exchange.TimeZone));
        return holding.Positions().Sum(p => p.Quantity * p.MarkPrice * security.SymbolProperties.ContractMultiplier
            * (p.Direction == "long" ? rule.LongMarginRate : rule.ShortMarginRate));
    }

    public decimal QuoteFee(Security security, Order order, decimal price, decimal quantity)
    {
        var props = (ChinaFutureOrderProperties)order.Properties;
        var rule = _rules(security.Symbol, security.LocalTime.ConvertToUtc(security.Exchange.TimeZone));
        var legs = Preview(security, order, price, quantity);
        return decimal.Round(legs.Sum(x => (props.PositionEffect == "open" ? rule.OpenFee
            : x.Bucket == "today" ? rule.CloseTodayFee : rule.CloseFee)
            .Quote(x.Quantity, price, security.SymbolProperties.ContractMultiplier)), 2, MidpointRounding.AwayFromZero);
    }

    public decimal Settle(Security security, DateOnly tradingDate, decimal price, DateTime availableUtc, DateTime nowUtc)
    {
        if (availableUtc > nowUtc) throw new InvalidOperationException("结算价尚不可见");
        if (_settled.Contains((security.Symbol, tradingDate))) throw new InvalidOperationException("同合约交易日结算重复");
        var holding = (ChinaFutureHolding)security.Holdings;
        if (holding.TradingDate != tradingDate) throw new InvalidOperationException("结算交易日与持仓桶不一致");
        var amount = holding.RealizeCash(holding.Settle(price));
        _portfolio.CashBook["CNY"].AddAmount(amount);
        security.Holdings.AddNewProfit(amount);
        _portfolio.InvalidateTotalPortfolioValue();
        _settled.Add((security.Symbol, tradingDate));
        return amount;
    }

    private sealed class ExplicitSettlement : ImmediateSettlementModel
    {
        public override void Scan(ScanSettlementModelParameters parameters) { }
    }

    private sealed class PortfolioModel(ChinaFutureModels owner) : ISecurityPortfolioModel
    {
        public void ProcessFill(SecurityPortfolioManager portfolio, Security security, OrderEvent fill)
        {
            if (fill.FillQuantity == 0) return;
            if (owner._committedFills.Contains((fill.OrderId, fill.Id))) return;
            if (!owner._orders.TryGetValue(fill.OrderId, out var order))
                throw new InvalidOperationException("成交缺少已登记期货订单");
            var props = (ChinaFutureOrderProperties)order.Properties;
            var quantity = owner._filled.GetValueOrDefault(order.Id) + Math.Abs(fill.FillQuantity);
            if (quantity > order.AbsoluteQuantity || Math.Sign(fill.FillQuantity) != Math.Sign(order.Quantity))
                throw new InvalidOperationException("期货累计成交量或方向不符");
            var holding = (ChinaFutureHolding)security.Holdings;
            var legs = owner.Preview(security, order, fill.FillPrice, fill.FillQuantity);
            var pnl = holding.RealizeCash(legs.Sum(x => x.RealizedPnl));
            var fee = fill.OrderFee.Value;
            if (fee.Currency != "CNY") throw new InvalidOperationException("期货成交费用必须为人民币");
            var rule = owner._rules(security.Symbol, fill.UtcTime);
            var accounted = legs.Select(x => x with {
                RealizedPnl = decimal.Round(x.RealizedPnl, 2, MidpointRounding.AwayFromZero),
                Fee = decimal.Round((props.PositionEffect == "open" ? rule.OpenFee : x.Bucket == "today"
                    ? rule.CloseTodayFee : rule.CloseFee).Quote(x.Quantity, fill.FillPrice, security.SymbolProperties.ContractMultiplier),
                    2, MidpointRounding.AwayFromZero), RuleReference = rule.RuleReference }).ToArray();
            accounted[^1] = accounted[^1] with { RealizedPnl = accounted[^1].RealizedPnl + pnl - accounted.Sum(x => x.RealizedPnl),
                Fee = accounted[^1].Fee + fee.Amount - accounted.Sum(x => x.Fee) };
            holding.Apply(legs, props.PositionEffect);
            portfolio.CashBook["CNY"].AddAmount(pnl - fee.Amount);
            holding.AddNewFee(fee.Amount);
            holding.AddNewProfit(pnl);
            holding.AddNewSale(fill.AbsoluteFillQuantity * fill.FillPrice * security.SymbolProperties.ContractMultiplier);
            owner._filled[order.Id] = quantity;
            owner._committedFills.Add((fill.OrderId, fill.Id));
            owner.CommitReservation(security, order, legs);
            portfolio.InvalidateTotalPortfolioValue();
            owner.FillCommitted?.Invoke(order, fill, accounted);
        }
    }

    private sealed class FeeModel(ChinaFutureModels owner) : QuantConnect.Orders.Fees.FeeModel
    {
        public override OrderFee GetOrderFee(OrderFeeParameters parameters) => new(new CashAmount(
            owner.QuoteFee(parameters.Security, parameters.Order, parameters.Security.Holdings.Price, parameters.Order.Quantity), "CNY"));
    }

    private sealed class BuyingPower(ChinaFutureModels owner) : SecurityMarginModel
    {
        public override MaintenanceMargin GetMaintenanceMargin(MaintenanceMarginParameters parameters) =>
            new(owner.Margin(parameters.Security));
        public override InitialMargin GetInitialMarginRequirement(InitialMarginParameters parameters)
        {
            var security = parameters.Security;
            var rule = owner._rules(security.Symbol, security.LocalTime.ConvertToUtc(security.Exchange.TimeZone));
            return new InitialMargin(Math.Abs(parameters.Quantity) * security.Holdings.Price
                * security.SymbolProperties.ContractMultiplier * (parameters.Quantity >= 0 ? rule.LongMarginRate : rule.ShortMarginRate));
        }
        public override HasSufficientBuyingPowerForOrderResult HasSufficientBuyingPowerForOrder(HasSufficientBuyingPowerForOrderParameters parameters)
        {
            if (parameters.Order.Properties is not ChinaFutureOrderProperties props)
                return parameters.Insufficient("期货订单缺少显式开平");
            var security = parameters.Security;
            try
            {
                owner.Preview(security, parameters.Order, security.Holdings.Price,
                    Math.Sign(parameters.Order.Quantity) * owner.Remaining(parameters.Order));
            }
            catch (InvalidOperationException exception) { return parameters.Insufficient(exception.Message); }
            if (props.PositionEffect != "open") return parameters.Sufficient();
            // 已受理订单由成交模型按实际价格与剩余冻结裁剪，不能用全余量阻断部分成交或风险取消。
            if (owner._reservations.ContainsKey(parameters.Order.Id)) return parameters.Sufficient();
            var signed = Math.Sign(parameters.Order.Quantity) * owner.Remaining(parameters.Order);
            var margin = GetInitialMarginRequirement(new InitialMarginParameters(security, signed)).Value;
            var fee = owner.QuoteFee(security, parameters.Order, security.Holdings.Price, signed);
            return owner.Available + owner.ReservedMoney(parameters.Order.Id) >= margin + fee ? parameters.Sufficient()
                : parameters.Insufficient("共享期货账户可用保证金不足");
        }
    }

    private sealed class FutureGroups : SecurityPositionGroupModel
    {
        protected override IPositionGroupResolver GetPositionGroupResolver() =>
            new SecurityPositionGroupResolver(new SecurityPositionGroupBuyingPowerModel());
    }
}
