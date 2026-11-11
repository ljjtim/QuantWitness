using QuantConnect.Securities;
using QuantConnect.Securities.Future;

namespace QuantWitness.Lean.China;

public sealed record FuturePosition(string Direction, string Bucket, decimal Quantity, decimal Cost,
    decimal BasePrice, decimal MarkPrice, decimal UnrealizedPnl);
public sealed record FutureFillLeg(string Direction, string Bucket, decimal Quantity, decimal Price,
    decimal RealizedPnl, decimal Fee = 0, string RuleReference = "");

/// <summary>同一真实合约的方向及今昨仓；原生 Quantity 只投影净数量。</summary>
public sealed class ChinaFutureHolding : FutureHolding
{
    private sealed class Lot
    {
        public decimal Quantity;
        public decimal Cost;
    }
    private readonly Dictionary<(string Direction, string Bucket), Lot> _lots = new();
    private readonly decimal _multiplier;
    private decimal _cashRemainder;
    public DateOnly? TradingDate { get; private set; }
    public decimal GrossQuantity => _lots.Values.Sum(x => x.Quantity);
    public override bool Invested => GrossQuantity > 0;
    public override bool HoldStock => Invested;
    public override decimal AbsoluteQuantity => GrossQuantity;
    public override decimal UnrealizedProfit => Positions().Sum(x => x.UnrealizedPnl) + _cashRemainder;
    public override decimal UnsettledProfit => UnrealizedProfit;

    public ChinaFutureHolding(Security security, ICurrencyConverter converter) : base(security, converter)
    {
        _multiplier = security.SymbolProperties.ContractMultiplier;
        foreach (var direction in new[] { "long", "short" })
        foreach (var bucket in new[] { "today", "yesterday" }) _lots[(direction, bucket)] = new Lot();
    }

    public IReadOnlyList<FuturePosition> Positions() => _lots.Where(x => x.Value.Quantity != 0)
        .Select(x => new FuturePosition(x.Key.Direction, x.Key.Bucket, x.Value.Quantity, x.Value.Cost,
            x.Value.Cost / x.Value.Quantity, Price,
            (Price * x.Value.Quantity - x.Value.Cost) * (x.Key.Direction == "long" ? 1 : -1) * _multiplier))
        .ToArray();

    public decimal QuantityFor(string direction, string bucket) => _lots[(direction, bucket)].Quantity;

    public decimal RealizeCash(decimal pnl)
    {
        var exact = pnl + _cashRemainder;
        var cash = decimal.Round(exact, 2, MidpointRounding.AwayFromZero);
        _cashRemainder = exact - cash;
        return cash;
    }

    public void AdvanceTradingDate(DateOnly day)
    {
        if (TradingDate.HasValue && day < TradingDate) throw new InvalidOperationException("期货交易日不能倒退");
        if (TradingDate == day) return;
        foreach (var direction in new[] { "long", "short" })
        {
            var today = _lots[(direction, "today")];
            var yesterday = _lots[(direction, "yesterday")];
            yesterday.Quantity += today.Quantity;
            yesterday.Cost += today.Cost;
            today.Quantity = today.Cost = 0;
        }
        TradingDate = day;
        Project();
    }

    public static string Direction(decimal signedQuantity, string effect) =>
        (signedQuantity > 0) == (effect == "open") ? "long" : "short";

    public IReadOnlyList<FutureFillLeg> Preview(decimal signedQuantity, string effect, decimal price,
        string closePriority = "yesterday_first", Func<string, decimal>? available = null)
    {
        var quantity = Math.Abs(signedQuantity);
        if (quantity <= 0 || quantity != decimal.Truncate(quantity) || price <= 0)
            throw new InvalidOperationException("期货成交必须为正价格和整数手数");
        if (effect is not ("open" or "close" or "close_today" or "close_yesterday"))
            throw new InvalidOperationException("期货必须明确开平仓类型");
        var direction = Direction(signedQuantity, effect);
        if (effect == "open") return new[] { new FutureFillLeg(direction, "today", quantity, price, 0) };
        var buckets = effect == "close_today" ? new[] { "today" }
            : effect == "close_yesterday" ? new[] { "yesterday" }
            : closePriority == "today_first" ? new[] { "today", "yesterday" }
            : new[] { "yesterday", "today" };
        var remaining = quantity;
        var result = new List<FutureFillLeg>();
        foreach (var bucket in buckets)
        {
            var lot = _lots[(direction, bucket)];
            var take = Math.Min(remaining, available == null ? lot.Quantity : Math.Min(lot.Quantity, available(bucket)));
            if (take == 0) continue;
            var pnl = (price - lot.Cost / lot.Quantity) * take * _multiplier * (direction == "long" ? 1 : -1);
            result.Add(new FutureFillLeg(direction, bucket, take, price, pnl));
            remaining -= take;
        }
        if (remaining != 0) throw new InvalidOperationException("期货可平仓桶数量不足");
        return result;
    }

    public void Apply(IReadOnlyList<FutureFillLeg> legs, string effect)
    {
        foreach (var leg in legs)
        {
            var lot = _lots[(leg.Direction, leg.Bucket)];
            if (effect == "open")
            {
                lot.Quantity += leg.Quantity;
                lot.Cost += leg.Quantity * leg.Price;
            }
            else
            {
                lot.Cost -= lot.Cost / lot.Quantity * leg.Quantity;
                lot.Quantity -= leg.Quantity;
                if (lot.Quantity == 0) lot.Cost = 0;
            }
        }
        Project();
    }

    /// <summary>显式结算更新基价，现金由唯一 PortfolioModel 划转。</summary>
    public decimal Settle(decimal price)
    {
        if (price <= 0) throw new InvalidOperationException("结算价格必须为正");
        var pnl = _lots.Sum(x => (price * x.Value.Quantity - x.Value.Cost)
            * (x.Key.Direction == "long" ? 1 : -1) * _multiplier);
        foreach (var lot in _lots.Values) lot.Cost = price * lot.Quantity;
        UpdateMarketPrice(price);
        Project();
        return pnl;
    }

    public void RefreshProjection() => Project();

    private void Project()
    {
        var net = _lots.Sum(x => x.Value.Quantity * (x.Key.Direction == "long" ? 1 : -1));
        var direction = net < 0 ? "short" : "long";
        var side = _lots.Where(x => x.Key.Direction == direction).Select(x => x.Value).ToArray();
        var count = side.Sum(x => x.Quantity);
        // 每次毛仓变化都通知原生分组，即使净数量没有变化。
        base.SetHoldings(count == 0 ? 0 : side.Sum(x => x.Cost) / count, net);
    }
}
