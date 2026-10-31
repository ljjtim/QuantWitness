using QuantConnect;
using QuantConnect.Data.Market;
using QuantConnect.Securities;

namespace QuantWitness.Lean.CorporateActions;

/// <summary>公司行动权益与 LEAN 账户的分阶段衔接，所有调用在算法线程串行执行。</summary>
public sealed class CorporateActionProcessor
{
    /// <summary>延迟可见的历史报价使用原价格时点判断除权，不能当成本日真实行情。</summary>
    public Func<Symbol, DateTime?>? MarketPriceSourceTime { get; set; }
    private sealed class Entitlement
    {
        public required CorporateActionEvent Action;
        public required Symbol Symbol;
        public decimal Registered;
        public decimal Cash;
        public decimal Bonus;
        public bool Ex;
        public bool Paid;
        public bool Arrived;
        public bool Released;
    }

    private readonly SecurityPortfolioManager _portfolio;
    private readonly Func<string, Symbol> _resolve;
    private readonly CorporateActionEvent[] _events;
    private readonly Dictionary<string, Entitlement> _rights = new(StringComparer.Ordinal);
    private readonly List<ActionTransition> _transitions = new();
    private DateTimeOffset? _clock;
    private bool _faulted;
    public DateTimeOffset? AsOf => _clock;
    public IReadOnlyList<ActionTransition> Transitions => _transitions.AsReadOnly();

    public CorporateActionProcessor(SecurityPortfolioManager portfolio, Func<string, Symbol> symbolResolver,
        IEnumerable<CorporateActionEvent> events)
    {
        _portfolio = portfolio;
        _resolve = symbolResolver;
        if (portfolio.CashBook.AccountCurrency != "CNY")
            throw new NotSupportedException("公司行动服务仅支持人民币账户");
        _events = events.ToArray();
        foreach (var action in _events) action.Validate();
        foreach (var versions in _events.GroupBy(x => x.ActionId))
        {
            if (versions.Select(x => (x.Symbol, x.Kind)).Distinct().Count() != 1)
                throw new ArgumentException("同一行动的证券与类型必须稳定");
            foreach (var revision in versions.GroupBy(x => x.Revision))
                if (revision.Distinct().Count() != 1)
                    throw new ArgumentException("同一行动版本内容冲突");
        }
    }

    private CorporateActionEvent? Visible(string id, DateTimeOffset at) => _events
        .Where(x => x.ActionId == id && x.AvailableAt <= at)
        .OrderByDescending(x => x.Revision).FirstOrDefault();

    /// <summary>只允许以显式历史登记证据衔接窗口开始前登记、窗口内除权的行动。</summary>
    public void SeedEntitlement(InitialEntitlement seed, DateTimeOffset startAt)
    {
        if (_clock is not null || _faulted) throw new InvalidOperationException("初始权益须在首次推进前声明");
        var action = Visible(seed.ActionId, seed.RecordAt)
            ?? throw new ArgumentException("登记时没有可见行动版本");
        if (seed.Revision != action.Revision || seed.RecordAt != action.RecordAt
            || seed.RecordAt >= startAt || action.ExAt < startAt || seed.Quantity < 0
            || decimal.Truncate(seed.Quantity) != seed.Quantity || string.IsNullOrWhiteSpace(seed.SourceRef)
            || _rights.ContainsKey(seed.ActionId))
            throw new ArgumentException("初始权益证据、数量或窗口时点无效");
        var right = NewEntitlement(action, seed.Quantity);
        _rights.Add(action.ActionId, right);
        Log(right, "initial_registered", seed.RecordAt);
    }

    /// <summary>盘前和登记收盘推进。遗漏的真实登记时点拒绝补记，到账可按已知时点顺序补交付。</summary>
    public void Advance(DateTimeOffset now)
    {
        if (_faulted) throw new InvalidOperationException("失败的公司行动会话不能继续");
        if (_clock is not null && now < _clock) throw new ArgumentException("公司行动时间不能倒退");
        var times = _events.SelectMany(x => new DateTimeOffset?[] { x.AvailableAt, x.RecordAt,
                x.ExAt, x.CashPayAt, x.SharesArrivalAt, x.SharesSellableAt })
            .Where(x => x.HasValue && x <= now && (_clock is null || x > _clock))
            .Select(x => x!.Value).Distinct().OrderBy(x => x).ToArray();
        try
        {
            foreach (var at in times) ProcessAt(at, now);
            _clock = now;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private void ProcessAt(DateTimeOffset at, DateTimeOffset now)
    {
        var visible = _events.Select(x => x.ActionId).Distinct().Select(x => Visible(x, at))
            .Where(x => x is not null).Cast<CorporateActionEvent>().ToArray();
        foreach (var action in visible)
        {
            if (_rights.TryGetValue(action.ActionId, out var existing))
            {
                if (existing.Action != action)
                {
                    if (existing.Ex || action.RecordAt != existing.Action.RecordAt || action.ExAt < at)
                        throw new NotSupportedException("已生效行动修订或登记时点变更需要独立更正结算，当前不支持");
                    existing.Action = action;
                }
            }
            else if (action.RecordAt <= at)
            {
                if (action.RecordAt != at || at != now)
                    throw new InvalidOperationException($"{action.ActionId} 缺少真实登记收盘权益，不得用当前持仓补记");
                var symbol = _resolve(action.Symbol);
                var right = NewEntitlement(action, _portfolio.Securities[symbol].Holdings.Quantity);
                _rights.Add(action.ActionId, right);
                Log(right, "registered", at);
            }
        }

        var ex = _rights.Values.Where(x => !x.Ex && x.Action.ExAt == at).ToArray();
        foreach (var group in ex.GroupBy(x => x.Symbol)) ApplyEx(group.ToArray(), at, now);
        foreach (var right in _rights.Values.OrderBy(x => x.Action.ActionId, StringComparer.Ordinal))
        {
            if (!right.Ex) continue;
            var action = right.Action;
            if (action.IsCash && !right.Paid && action.CashPayAt == at)
            {
                _portfolio.CashBook["CNY"].AddAmount(right.Cash);
                _portfolio.Securities[right.Symbol].Holdings.AddNewDividend(right.Cash);
                right.Paid = true;
                Log(right, "cash_paid", at, right.Cash);
            }
            if (!action.IsCash && !right.Arrived && action.SharesArrivalAt == at)
            {
                var holding = _portfolio.Securities[right.Symbol].Holdings;
                var quantity = holding.Quantity + right.Bonus;
                // 送转股不新占现金，现存原股成本分摊到到账后的总股数。
                var average = quantity == 0 ? 0 : holding.AveragePrice * holding.Quantity / quantity;
                holding.SetHoldings(average, quantity);
                _portfolio.InvalidateTotalPortfolioValue();
                right.Arrived = true;
                Log(right, "shares_arrived", at, shares: right.Bonus);
            }
            if (!action.IsCash && right.Arrived && !right.Released && action.SharesSellableAt == at)
            {
                right.Released = true;
                Log(right, "shares_sellable", at);
            }
        }
    }

    private Entitlement NewEntitlement(CorporateActionEvent action, decimal quantity)
    {
        var symbol = _resolve(action.Symbol);
        var security = _portfolio.Securities[symbol];
        if (security.Type != SecurityType.Equity || security.QuoteCurrency.Symbol != "CNY"
            || security.QuoteCurrency.ConversionRate != 1 || quantity < 0 || decimal.Truncate(quantity) != quantity)
            throw new NotSupportedException("仅支持人民币整数多头股票权益");
        return new Entitlement { Action = action, Symbol = symbol, Registered = quantity };
    }

    private void ApplyEx(Entitlement[] rights, DateTimeOffset at, DateTimeOffset now)
    {
        var security = _portfolio.Securities[rights[0].Symbol];
        if (_portfolio.Transactions.GetOpenOrders(security.Symbol).Count > 0)
        {
            _portfolio.Transactions.CancelOpenOrders(security.Symbol, "公司行动除权前取消旧价旧量委托");
            if (_portfolio.Transactions.GetOpenOrders(security.Symbol).Count > 0)
                throw new InvalidOperationException("跨除权委托撤销未确认，公司行动不能继续应用");
        }
        var cashPerShare = rights.Sum(x => x.Action.IsCash ? x.Action.CashPerShare : 0);
        var bonusRatio = rights.Sum(x => x.Action.BonusRatio);
        var reference = rights.Select(x => x.Action.ExReferencePrice).Where(x => x.HasValue).Distinct().ToArray();
        if (reference.Length > 1) throw new ArgumentException("同一除权组的正式参考价不一致");
        foreach (var right in rights)
        {
            right.Cash = right.Registered * (right.Action.IsCash ? right.Action.CashPerShare : 0);
            right.Bonus = right.Registered * right.Action.BonusRatio;
            if (decimal.Truncate(right.Bonus) != right.Bonus)
                throw new NotSupportedException("零股分配需明确规则，当前拒绝非整数送转权益");
        }
        var last = security.GetLastData();
        if (last is not null)
        {
            var sourceTime = MarketPriceSourceTime?.Invoke(security.Symbol) ?? last.EndTime;
            var lastAt = new DateTimeOffset(DateTime.SpecifyKind(sourceTime, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
            if (lastAt > now) throw new InvalidOperationException("市场标记包含未来行情");
            // 早于除权时点才调整旧价，延迟调度遇到当日真实行情时不能再除权一次。
            if (lastAt < at)
            {
                var price = reference.FirstOrDefault() ?? (security.Price - cashPerShare) / (1m + bonusRatio);
                if (price <= 0) throw new InvalidOperationException("除权参考价非正，需提供正式参考价");
                var local = DateTime.SpecifyKind(at.ToOffset(TimeSpan.FromHours(8)).DateTime, DateTimeKind.Unspecified);
                security.SetMarketPrice(new TradeBar(local, security.Symbol, price, price, price, price, 0, TimeSpan.Zero));
            }
        }
        else if (rights.Any(x => x.Registered > 0))
            throw new InvalidOperationException("有登记权益的证券缺少除权前价格标记");
        foreach (var right in rights) { right.Ex = true; Log(right, "ex", at); }
        _portfolio.InvalidateTotalPortfolioValue();
    }

    public void CaptureRecord(DateTimeOffset now) => Advance(now);

    public decimal LockedBonusQuantity(Symbol symbol) => _rights.Values
        .Where(x => x.Symbol == symbol && x.Arrived && !x.Released).Sum(x => x.Bonus);

    public decimal RegisteredQuantity(string actionId) => _rights.TryGetValue(actionId, out var right) ? right.Registered : 0;

    public IEnumerable<Symbol> ValuationSymbols => _rights.Values
        .Where(x => x.Ex && x.Bonus > 0 && (!x.Arrived || !x.Released)).Select(x => x.Symbol).Distinct();

    public EntitlementValuation GetValuation(DateTimeOffset? asOf = null)
    {
        var clock = _clock ?? throw new InvalidOperationException("估值前须推进公司行动时钟");
        var at = asOf ?? clock;
        if (at < clock) throw new InvalidOperationException("权益估值时间不能早于已处理公司行动");
        var active = _rights.Values.Where(x => x.Ex).ToArray();
        decimal Value(Entitlement x)
        {
            var security = _portfolio.Securities[x.Symbol];
            var last = security.GetLastData();
            if (last is null && x.Bonus > 0) throw new InvalidOperationException("股份权益缺少可见价格");
            if (last is not null && new DateTimeOffset(DateTime.SpecifyKind(last.EndTime, DateTimeKind.Unspecified), at.Offset) > at)
                throw new InvalidOperationException("权益估值不能使用未来价格");
            return x.Bonus * security.Price;
        }
        var pending = active.Where(x => !x.Action.IsCash && !x.Arrived).ToArray();
        var locked = active.Where(x => x.Arrived && !x.Released).Sum(Value);
        return new EntitlementValuation(at, active.Where(x => x.Action.IsCash && !x.Paid).Sum(x => x.Cash),
            pending.Sum(Value), pending.Sum(x => x.Bonus), locked, _portfolio.TotalPortfolioValue);
    }

    private void Log(Entitlement right, string phase, DateTimeOffset at, decimal cash = 0, decimal shares = 0)
        => _transitions.Add(new ActionTransition(right.Action.ActionId, right.Action.Revision,
            phase, at, right.Registered, cash, shares));
}
