using QuantConnect;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Securities;

namespace QuantWitness.Lean.China;

public sealed partial class ChinaAStockModels
{
    private bool _installed;
    private bool _executionConfigured;
    private decimal _slippageBps;
    private int _slippageTicks;
    private int _participationCapPpm = 1000000;
    private SecurityPortfolioManager? _portfolio;
    private sealed record Candidate(Symbol Symbol, DateTime End, decimal Quantity, decimal Cash);
    private readonly Dictionary<int, Candidate> _pending = new();
    private readonly Dictionary<Symbol, (DateTime End, decimal Quantity)> _capacity = new();
    private readonly Dictionary<int, string> _executionStatus = new();
    private readonly Dictionary<int, decimal> _budgets = new();

    /// <summary>安装前冻结执行政策；原缺省不限制参与率，不添加滑点。</summary>
    public void ConfigureExecution(decimal slippageBps, int slippageTicks, int participationCapPpm)
    {
        if (_installed) throw new InvalidOperationException("执行参数必须在 Install 之前冻结");
        if (slippageBps < 0 || slippageBps >= 10000 || slippageTicks < 0 || participationCapPpm <= 0 || participationCapPpm > 1000000)
            throw new ArgumentOutOfRangeException(nameof(participationCapPpm), "执行参数超出范围");
        _slippageBps = slippageBps;
        _slippageTicks = slippageTicks;
        _participationCapPpm = participationCapPpm;
        _executionConfigured = true;
    }

    /// <summary>内存合同与独立接线可绑定原生账户；Install 自动绑定。</summary>
    public void BindPortfolio(SecurityPortfolioManager portfolio) => _portfolio = portfolio;

    public string ExecutionStatus(int orderId) => _executionStatus.GetValueOrDefault(orderId, "尚无候选成交");
    public decimal RemainingBudgetCny(int orderId) => _budgets.TryGetValue(orderId, out var budget)
        ? Math.Max(0, budget - Cost(orderId).SpentCny) : 0;

    private decimal ExecutionPrice(decimal price, bool buy, ChinaRule rule)
    {
        var scale = (decimal)Math.Pow(10, rule.PriceScale);
        var direction = buy ? 1 : -1;
        var units = price * scale * (1 + direction * _slippageBps / 10000) + direction * _slippageTicks * rule.PriceTickUnits;
        var ticks = units / rule.PriceTickUnits;
        return (buy ? Math.Ceiling(ticks) : Math.Floor(ticks)) * rule.PriceTickUnits / scale;
    }

    private static decimal FloorGrid(decimal cap, QuantityGrid grid, decimal sellable = 0)
    {
        cap = Math.Floor(Math.Min(cap, grid.Maximum ?? cap));
        var regular = cap < grid.Minimum ? 0 : grid.Minimum + Math.Floor((cap - grid.Minimum) / grid.Step) * grid.Step;
        if (!grid.RemainderAllowed) return regular;
        var remainder = sellable < grid.Minimum ? sellable : (sellable - grid.Minimum) % grid.Step;
        var odd = remainder > 0 && cap >= remainder ? remainder + Math.Floor((cap - remainder) / grid.Step) * grid.Step : 0;
        return Math.Max(regular, grid.Accepts(odd, sellable) ? odd : 0);
    }

    private static decimal Affordable(ChinaRule rule, OrderCost state, decimal price, decimal cash, decimal cap, QuantityGrid grid)
    {
        if (cap < grid.Minimum || cash <= 0) return 0;
        var lo = 0m;
        var hi = Math.Floor((cap - grid.Minimum) / grid.Step) + 1;
        while (lo < hi)
        {
            var mid = Math.Floor((lo + hi + 1) / 2);
            var quantity = grid.Minimum + (mid - 1) * grid.Step;
            var cost = (FenNotional(price, quantity) + Quote(rule, state, price, quantity).FeeFen) / 100;
            if (cost <= cash) lo = mid; else hi = mid - 1;
        }
        return lo == 0 ? 0 : grid.Minimum + (lo - 1) * grid.Step;
    }

    /// <summary>使用可见报价及冻结滑点保守估量，按单笔最低佣金和历史申报格点换算。</summary>
    public decimal CalculateQuantityForBudget(Security security, decimal budgetCny, decimal visiblePrice, DateTime utc)
    {
        if (budgetCny <= 0 || visiblePrice <= 0) return 0;
        var rule = Rule(security.Symbol, utc);
        rule.RequireVisibleReference(utc);
        var price = _executionConfigured ? ExecutionPrice(visiblePrice, true, rule) : visiblePrice;
        var grid = rule.Grid(true, true);
        var cap = FloorGrid(Math.Min(grid.Maximum ?? decimal.MaxValue, Math.Floor(budgetCny / price)), grid);
        return Affordable(rule, new OrderCost(), price, budgetCny, cap, grid);
    }

    private decimal ExecutionQuantity(Security security, Order order, decimal price, TradeBar bar, DateTime utc)
    {
        var remaining = Remaining(order);
        var budget = (order.Properties as ChinaOrderProperties)?.BudgetCny;
        if (budget.HasValue) _budgets.TryAdd(order.Id, budget.Value);
        if (!_executionConfigured && !budget.HasValue) return remaining;
        var end = bar.EndTime.ConvertToUtc(security.Exchange.TimeZone);
        foreach (var id in _pending.Where(pair => pair.Value.End < utc).Select(pair => pair.Key).ToArray()) _pending.Remove(id);
        if (_pending.ContainsKey(order.Id))
        {
            _executionStatus[order.Id] = "等待当前候选的真实成交确认";
            return 0;
        }
        var used = _capacity.TryGetValue(security.Symbol, out var value) && value.End == end ? value.Quantity : 0;
        used += _pending.Values.Where(p => p.Symbol == security.Symbol && p.End == end).Sum(p => p.Quantity);
        // 日线开盘没有可见的全天容量，禁止用日成交量约束当时的成交。
        var cap = bar is DailyOpenBar ? remaining
            : Math.Min(remaining, Math.Max(0, Math.Floor(bar.Volume * _participationCapPpm / 1000000) - used));
        var rule = Rule(security.Symbol, utc);
        var buy = order.Quantity > 0;
        var grid = rule.Grid(buy, order.Type != OrderType.Limit);
        var quantity = FloorGrid(cap, grid, buy ? 0 : SellableQuantity(security, utc));
        if (buy)
        {
            var cash = budget.HasValue ? RemainingBudgetCny(order.Id) : decimal.MaxValue;
            if (_portfolio != null) cash = Math.Min(cash, Math.Max(0, _portfolio.CashBook["CNY"].Amount - _pending.Values.Sum(p => p.Cash)));
            else if (_executionConfigured) throw new InvalidOperationException("执行限制需要 Install 或 BindPortfolio 绑定原生账户");
            quantity = Affordable(rule, Cost(order.Id), price, cash, quantity, grid);
        }
        _executionStatus[order.Id] = quantity > 0 ? "候选成交等待确认" : cap < grid.Minimum
            ? "当前分钟剩余容量或订单余量不足最低量；等待后续分钟或DAY撤单"
            : "剩余含费预算或现金不足最低量；需撤单或DAY终止";
        return quantity;
    }

    private void TrackCandidate(Security security, Order order, OrderEvent fill, TradeBar bar, DateTime utc)
    {
        if (!_executionConfigured && (order.Properties as ChinaOrderProperties)?.BudgetCny == null) return;
        var cash = fill.FillQuantity > 0 ? FenNotional(fill.FillPrice, fill.FillQuantity) / 100 + fill.OrderFee.Value.Amount : 0;
        _pending[order.Id] = new Candidate(security.Symbol, bar.EndTime.ConvertToUtc(security.Exchange.TimeZone), Math.Abs(fill.FillQuantity), cash);
    }

    private void ConfirmExecution(OrderEvent fill)
    {
        if (_pending.Remove(fill.OrderId, out var pending))
        {
            var used = _capacity.TryGetValue(fill.Symbol, out var old) && old.End == pending.End ? old.Quantity : 0;
            _capacity[fill.Symbol] = (pending.End, used + Math.Abs(fill.FillQuantity));
        }
        _executionStatus[fill.OrderId] = fill.Status == OrderStatus.Filled ? "已全部成交" : "已确认部分成交";
        if (fill.Status == OrderStatus.Filled) ReleaseExecution(fill.OrderId);
    }

    private void ReleaseExecution(int orderId) => _pending.Remove(orderId);

    internal decimal BudgetBuyingPowerQuantity(HasSufficientBuyingPowerForOrderParameters parameters)
    {
        var order = parameters.Order;
        var budget = ((ChinaOrderProperties)order.Properties).BudgetCny!.Value;
        if (budget <= 0 || budget * 100 != Math.Truncate(budget * 100)) return 0;
        _budgets.TryAdd(order.Id, budget);
        var rule = Rule(order.Symbol, Now(parameters.Security));
        var reference = (order as LimitOrder)?.LimitPrice ?? parameters.Security.Price;
        if (reference <= 0) return 0;
        var price = _executionConfigured ? ExecutionPrice(reference, true, rule) : reference;
        var cash = Math.Max(0, parameters.Portfolio.CashBook["CNY"].Amount - _pending.Values.Sum(p => p.Cash));
        var grid = rule.Grid(true, order.Type != OrderType.Limit);
        var cap = FloorGrid(Remaining(order), grid);
        return Affordable(rule, Cost(order.Id), price, Math.Min(RemainingBudgetCny(order.Id), cash), cap, grid);
    }
}

/// <summary>只将显式金额买单的可负担余量交给原生一倍购买力检查。</summary>
public sealed class ChinaBudgetBuyingPowerModel(ChinaAStockModels models) : SecurityMarginModel(1m)
{
    public override HasSufficientBuyingPowerForOrderResult HasSufficientBuyingPowerForOrder(HasSufficientBuyingPowerForOrderParameters parameters)
    {
        if (parameters.Order.Direction != OrderDirection.Buy || (parameters.Order.Properties as ChinaOrderProperties)?.BudgetCny == null)
            return base.HasSufficientBuyingPowerForOrder(parameters);
        if (!ChinaAStockModels.Supported(parameters.Order.Type)) return parameters.Insufficient("金额委托类型不支持");
        decimal quantity;
        try { quantity = models.BudgetBuyingPowerQuantity(parameters); }
        catch (Exception error) when (error is InvalidOperationException or ArgumentException)
        { return parameters.Insufficient(error.Message); }
        if (quantity <= 0) return parameters.Insufficient("剩余含费预算或现金不足历史最低申报量，金额订单终止");
        var ticket = parameters.Portfolio.Transactions.GetOrderTicket(parameters.Order.Id);
        if (ticket == null) return parameters.Insufficient("原生订单票据不存在");
        var projected = parameters.Order.Clone();
        projected.ApplyUpdateOrderRequest(new UpdateOrderRequest(ChinaAStockModels.Now(parameters.Security), projected.Id,
            new UpdateOrderFields { Quantity = Math.Abs(ticket.QuantityFilled) + quantity }));
        return base.HasSufficientBuyingPowerForOrder(new HasSufficientBuyingPowerForOrderParameters(parameters.Portfolio, parameters.Security, projected));
    }
}
