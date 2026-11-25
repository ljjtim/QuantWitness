using QuantConnect;
using QuantConnect.Data.Market;
using QuantConnect.Securities;

namespace QuantWitness.Lean.CorporateActions;

/// <summary>公司行动权益与 LEAN 账户的分阶段衔接，所有调用在算法线程串行执行。</summary>
public sealed class CorporateActionProcessor
{
    /// <summary>延迟可见的历史报价使用原价格时点判断除权，不能当成本日真实行情。</summary>
    public Func<Symbol, DateTime?>? MarketPriceSourceTime { get; set; }
    public Func<Symbol, DateTimeOffset, bool>? IsDelisted { get; set; }
    private sealed class Entitlement
    {
        public required CorporateActionEvent Action;
        public required Symbol Symbol;
        public int RegistrationOrder;
        public decimal Registered;
        public decimal Cash;
        public decimal Bonus;
        public bool Ex;
        public bool Paid;
        public bool Arrived;
        public bool Credited;
        public bool Released;
    }

    private readonly SecurityPortfolioManager _portfolio;
    private readonly Func<string, Symbol> _resolve;
    private readonly CorporateActionEvent[] _events;
    private readonly Dictionary<string, CorporateActionEvent[]> _versions;
    private readonly DateTimeOffset[] _eventTimes;
    private readonly Dictionary<DateTimeOffset, string[]> _actionsAt;
    private int _nextEventTime;
    private readonly Dictionary<string, Entitlement> _rights = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, Entitlement> _unsettled = new(StringComparer.Ordinal);
    private sealed record ExReference(DateTimeOffset At, decimal? Price, decimal Cash, decimal Bonus);
    private readonly Dictionary<Symbol, List<ExReference>> _pendingReferences = new();
    private readonly Dictionary<Symbol, TradeBar> _syntheticReferences = new();
    private readonly List<ActionTransition> _transitions = new();
    private DateTimeOffset? _clock;
    private bool _faulted;
    private DateTimeOffset? _pendingRecord;
    public DateTimeOffset? AsOf => _clock;
    public bool HasPendingMarketReferences => _pendingReferences.Count > 0;
    public IReadOnlyList<ActionTransition> Transitions { get; }
    public bool HasUnsettledEntitlements => _unsettled.Count != 0;
    public IEnumerable<Symbol> UnsettledSymbols => _unsettled.Values.Select(x => x.Symbol).Distinct();
    /// <summary>读取当前采用版本的登记权益账簿，不计算市场价值或推进时钟。</summary>
    public IReadOnlyList<RegisteredEntitlementState> RegisteredEntitlements => _rights.Values
        .OrderBy(x => x.Action.ActionId, StringComparer.Ordinal)
        .Select(x => new RegisteredEntitlementState(x.Action.ActionId, x.Action.Revision, x.Symbol,
            x.Registered, x.Cash, x.Bonus, x.Ex, x.Paid, x.Arrived, x.Released,
            x.Credited, x.Action.SharesArrivalAt, x.Action.SharesArrivalDate,
            x.Action.SharesSellableAt, x.Action.ShareSettlementPolicy)).ToArray();

    public CorporateActionProcessor(SecurityPortfolioManager portfolio, Func<string, Symbol> symbolResolver,
        IEnumerable<CorporateActionEvent> events)
    {
        _portfolio = portfolio;
        _resolve = symbolResolver;
        Transitions = _transitions.AsReadOnly();
        if (portfolio.CashBook.AccountCurrency != "CNY")
            throw new NotSupportedException("公司行动服务仅支持人民币账户");
        _events = events.ToArray();
        foreach (var action in _events) action.Validate();
        _versions = _events.GroupBy(x => x.ActionId, StringComparer.Ordinal).ToDictionary(group => group.Key,
            group => group.OrderByDescending(x => x.Revision).ToArray(), StringComparer.Ordinal);
        _actionsAt = _versions.SelectMany(pair => pair.Value.SelectMany(action => new DateTimeOffset?[] {
                action.AvailableAt, action.RecordAt, action.ExAt, action.CashPayAt,
                action.SharesArrivalAt, action.SharesSellableAt })
            .OfType<DateTimeOffset>().Distinct().Select(at => (At: at, Id: pair.Key)))
            .GroupBy(item => item.At).ToDictionary(group => group.Key, group => group.Select(item => item.Id).ToArray());
        _eventTimes = _actionsAt.Keys.Order().ToArray();
        foreach (var versions in _versions.Values)
        {
            if (versions.Select(x => (x.Symbol, x.Kind)).Distinct().Count() != 1)
                throw new ArgumentException("同一行动的证券与类型必须稳定");
            foreach (var revision in versions.GroupBy(x => x.Revision))
                if (revision.Distinct().Count() != 1)
                    throw new ArgumentException("同一行动版本内容冲突");
        }
    }

    private CorporateActionEvent? Visible(string id, DateTimeOffset at) =>
        _versions.TryGetValue(id, out var versions) ? versions.FirstOrDefault(x => x.AvailableAt <= at) : null;

    private void AddEntitlement(Entitlement right)
    {
        right.RegistrationOrder = _rights.Count;
        _rights.Add(right.Action.ActionId, right);
        if (right.Registered > 0) _unsettled.Add(right.Action.ActionId, right);
    }

    /// <summary>初始空仓按实际登记时可见版本逐行动登记一次，不继承窗口前持仓。</summary>
    public void SeedInitialEmptyPortfolio(DateTimeOffset startAt)
    {
        foreach (var versions in _events.GroupBy(x => x.ActionId)
            .Where(x => x.Any(action => action.RecordAt < startAt)).OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            CorporateActionEvent? registered = null;
            foreach (var at in versions.Select(x => x.RecordAt).Where(x => x < startAt).Distinct().Order())
            {
                var visible = Visible(versions.Key, at);
                if (visible != null && visible.RecordAt == at) { registered = visible; break; }
            }
            if (registered == null) throw new ArgumentException($"登记时没有可见行动版本：{versions.Key}");
            SeedEntitlement(new InitialEntitlement(registered.ActionId, registered.Revision, 0m, registered.RecordAt,
                "initial_empty_portfolio:" + registered.SourceRef), startAt);
        }
    }

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
        AddEntitlement(right);
        Log(right, "initial_registered", seed.RecordAt);
    }

    /// <summary>盘前和登记收盘推进。遗漏的真实登记时点拒绝补记，到账可按已知时点顺序补交付。</summary>
    public void Advance(DateTimeOffset now)
        => AdvanceCore(now, true);

    public void AdvanceBeforeTrading(DateTimeOffset now)
        => AdvanceCore(now, false);

    public void CompleteTimeStep(DateTimeOffset now)
    {
        AdvanceCore(now, false);
        if (_pendingRecord != now) return;
        try
        {
            ProcessAt(now, now);
            _pendingRecord = null;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private void AdvanceCore(DateTimeOffset now, bool register)
    {
        if (_faulted) throw new InvalidOperationException("失败的公司行动会话不能继续");
        if (_clock is not null && now < _clock) throw new ArgumentException("公司行动时间不能倒退");
        if (_pendingRecord < now)
            throw new InvalidOperationException("缺少真实登记收盘后的持仓快照");
        if (_clock == now && (!register || _pendingRecord is null)) return;
        try
        {
            if (_pendingRecord == now)
            {
                ProcessAt(now, now);
                _pendingRecord = null;
            }
            while (_nextEventTime < _eventTimes.Length && _eventTimes[_nextEventTime] <= now)
            {
                var at = _eventTimes[_nextEventTime++];
                // 窗口前已有登记证据的行动继续处理除权和到账，只对尚未登记的行动等待实际收盘。
                var record = !register && _actionsAt[at].Any(id =>
                    !_rights.ContainsKey(id) && Visible(id, at)?.RecordAt == at);
                if (!register && record)
                {
                    if (at != now) throw new InvalidOperationException("缺少真实登记收盘后的持仓快照");
                    _pendingRecord = at;
                }
                ProcessAt(at, now, register || !record);
            }
            _clock = now;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private void ProcessAt(DateTimeOffset at, DateTimeOffset now, bool register = true)
    {
        var visible = _actionsAt[at].Select(x => Visible(x, at))
            .Where(x => x is not null).Cast<CorporateActionEvent>().ToArray();
        foreach (var action in visible)
        {
            if (_rights.TryGetValue(action.ActionId, out var existing))
            {
                if (at >= existing.Action.RecordAt && existing.Action != action)
                {
                    if (existing.Ex || action.RecordAt != existing.Action.RecordAt || action.ExAt < at
                        || (existing.Arrived && action.SharesArrivalAt != existing.Action.SharesArrivalAt))
                        throw new NotSupportedException("已生效行动修订或登记时点变更需要独立更正结算，当前不支持");
                    existing.Action = action;
                }
            }
            else if (action.RecordAt <= at)
            {
                if (!register && action.RecordAt == at) continue;
                if (action.RecordAt != at || at != now)
                    throw new InvalidOperationException($"{action.ActionId} 缺少真实登记收盘权益，不得用当前持仓补记");
                var symbol = _resolve(action.Symbol);
                foreach (var prior in _unsettled.Values.Where(x => x.Symbol == symbol && x.Action.Kind == "stock_dividend"
                    && x.Action.RecordAt < at && x.Registered > 0 && !x.Credited))
                {
                    ApplyShares(prior, at);
                    if (!prior.Credited)
                        throw new NotSupportedException("前次送转尚未结转，后续登记缺少应收股份受益资格依据");
                }
                var right = NewEntitlement(action, _portfolio.Securities.TryGetValue(symbol, out var security)
                    ? security.Holdings.Quantity : 0m);
                AddEntitlement(right);
                Log(right, "registered", at);
            }
        }

        var due = visible.Select(action => _rights.GetValueOrDefault(action.ActionId)).OfType<Entitlement>().ToArray();
        var ex = due.Where(x => !x.Ex && x.Action.ExAt == at).OrderBy(x => x.RegistrationOrder);
        foreach (var group in ex.GroupBy(x => x.Symbol)) ApplyEx(group.ToArray(), at, now);
        foreach (var right in due.OrderBy(x => x.Action.ActionId, StringComparer.Ordinal))
        {
            ApplyShares(right, at);
            if (!right.Ex) continue;
            var action = right.Action;
            if (action.IsCash && !right.Paid && action.CashPayAt == at)
            {
                _portfolio.CashBook["CNY"].AddAmount(right.Cash);
                if (right.Cash != 0) _portfolio.Securities[right.Symbol].Holdings.AddNewDividend(right.Cash);
                right.Paid = true;
                Log(right, "cash_paid", at, right.Cash);
            }
            if (right.Action.IsCash ? right.Paid : right.Released)
                _unsettled.Remove(right.Action.ActionId);
        }
    }

    private void ApplyShares(Entitlement right, DateTimeOffset at)
    {
        var action = right.Action;
        if (action.IsCash) return;
        if (action.Kind == "stock_dividend" && !right.Arrived && action.SharesArrivalAt is not null && action.SharesArrivalAt <= at)
        {
            right.Arrived = true;
            Log(right, "shares_arrived", at);
        }
        if (right.Ex && !right.Credited && action.SharesCreditAt <= at)
        {
            if (right.Bonus == 0 && !_portfolio.Securities.ContainsKey(right.Symbol))
            {
                right.Credited = true;
                right.Released = true;
                return;
            }
            var holding = _portfolio.Securities[right.Symbol].Holdings;
            var quantity = holding.Quantity + right.Bonus;
            // 送转不占现金；原始到账事实与经济股份入账分别保留。
            var average = quantity == 0 ? 0 : holding.AveragePrice * holding.Quantity / quantity;
            holding.SetHoldings(average, quantity);
            _portfolio.InvalidateTotalPortfolioValue();
            right.Credited = true;
            if (action.Kind == "split") right.Arrived = true;
            var phase = action.Kind == "split" ? "shares_arrived"
                : action.ShareSettlementPolicy == "actual_arrival" ? "shares_credited" : "shares_receivable_settled";
            Log(right, phase,
                at, shares: right.Bonus);
        }
        if (right.Credited && !right.Released && action.SharesSellableAt <= at)
        {
            right.Released = true;
            Log(right, "shares_sellable", at);
        }
    }

    private Entitlement NewEntitlement(CorporateActionEvent action, decimal quantity)
    {
        var symbol = _resolve(action.Symbol);
        if (quantity == 0 && !_portfolio.Securities.ContainsKey(symbol))
            return new Entitlement { Action = action, Symbol = symbol, Registered = 0m };
        var security = _portfolio.Securities[symbol];
        if (security.Type != SecurityType.Equity || security.QuoteCurrency.Symbol != "CNY"
            || security.QuoteCurrency.ConversionRate != 1 || quantity < 0 || decimal.Truncate(quantity) != quantity)
            throw new NotSupportedException("仅支持人民币整数多头股票权益");
        return new Entitlement { Action = action, Symbol = symbol, Registered = quantity };
    }

    private void ApplyEx(Entitlement[] rights, DateTimeOffset at, DateTimeOffset now)
    {
        var symbol = rights[0].Symbol;
        var reference = ReadReference(rights, at);
        if (!_portfolio.Securities.ContainsKey(rights[0].Symbol) && rights.All(right => right.Registered == 0))
        {
            TrackReference(symbol, reference);
            foreach (var right in rights) { right.Ex = true; Log(right, "ex", at); }
            return;
        }
        var security = _portfolio.Securities[symbol];
        var canceledOpenOrders = _portfolio.Transactions.GetOpenOrders(security.Symbol).Count > 0;
        if (canceledOpenOrders)
        {
            _portfolio.Transactions.CancelOpenOrders(security.Symbol, "公司行动除权前取消旧价旧量委托");
            if (_portfolio.Transactions.GetOpenOrders(security.Symbol).Count > 0)
                throw new InvalidOperationException("跨除权委托撤销未确认，公司行动不能继续应用");
        }
        foreach (var right in rights)
        {
            right.Cash = right.Registered * (right.Action.IsCash ? right.Action.CashPerShare : 0);
            right.Bonus = right.Registered * right.Action.BonusRatio;
            if (decimal.Truncate(right.Bonus) != right.Bonus)
                throw new NotSupportedException("零股分配需明确规则，当前拒绝非整数送转权益");
        }
        var last = security.GetLastData();
        var markAffected = false;
        if (IsDelisted?.Invoke(security.Symbol, at) == true)
        {
            markAffected = security.Holdings.Price != 0m;
            security.Holdings.UpdateMarketPrice(0m);
        }
        else if (last is not null)
        {
            var lastAt = SourceAt(security, last.EndTime);
            if (lastAt > now) throw new InvalidOperationException("市场标记包含未来行情");
            // 早于除权时点才调整旧价，延迟调度遇到当日真实行情时不能再除权一次。
            if (lastAt < at)
            {
                TrackReference(symbol, reference);
                SetReference(security, reference);
                markAffected = true;
            }
        }
        else if (rights.Any(x => x.Registered > 0))
            throw new InvalidOperationException("有登记权益的证券缺少除权前价格标记");
        else
        {
            TrackReference(symbol, reference);
            markAffected = true;
        }
        // 零登记也可能改变之后买入的持仓估值，或影响已登记的股份应收。
        var changesValuation = markAffected && (security.Holdings.Quantity != 0m || ValuationSymbols.Contains(symbol));
        foreach (var right in rights)
        {
            right.Ex = true;
            Log(right, "ex", at, affectsAccount: canceledOpenOrders || changesValuation);
        }
        _portfolio.InvalidateTotalPortfolioValue();
    }

    private static ExReference ReadReference(Entitlement[] rights, DateTimeOffset at)
    {
        var prices = rights.Select(x => x.Action.ExReferencePrice).Where(x => x.HasValue).Distinct().ToArray();
        if (prices.Length > 1) throw new ArgumentException("同一除权组的正式参考价不一致");
        return new ExReference(at, prices.FirstOrDefault(),
            rights.Sum(x => x.Action.IsCash ? x.Action.CashPerShare : 0), rights.Sum(x => x.Action.BonusRatio));
    }

    private void TrackReference(Symbol symbol, ExReference reference)
    {
        if (!_pendingReferences.TryGetValue(symbol, out var references))
            _pendingReferences[symbol] = references = new();
        references.Add(reference);
    }

    private DateTimeOffset SourceAt(Security security, DateTime fallback)
    {
        var source = MarketPriceSourceTime?.Invoke(security.Symbol) ?? fallback;
        return new DateTimeOffset(DateTime.SpecifyKind(source, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
    }

    private void SetReference(Security security, ExReference reference)
    {
        var price = reference.Price ?? (security.Price - reference.Cash) / (1m + reference.Bonus);
        if (price <= 0) throw new InvalidOperationException("除权参考价非正，需提供正式参考价");
        var local = reference.At.ToOffset(TimeSpan.FromHours(8)).DateTime;
        var mark = new TradeBar(local, security.Symbol, price, price, price, price, 0, TimeSpan.Zero);
        security.SetMarketPrice(mark);
        _syntheticReferences[security.Symbol] = mark;
    }

    /// <summary>只检查本步收到行情的证券，以真实除权后价格结束参考价刷新。</summary>
    public void RefreshMarketReferences(DateTimeOffset now, IEnumerable<Symbol>? observedSymbols = null)
    {
        if (_pendingReferences.Count == 0) return;
        var changed = false;
        foreach (var symbol in (observedSymbols ?? _pendingReferences.Keys.ToArray()).Distinct())
        {
            if (!_pendingReferences.TryGetValue(symbol, out var references)) continue;
            if (IsDelisted?.Invoke(symbol, now) == true)
            {
                _pendingReferences.Remove(symbol);
                _syntheticReferences.Remove(symbol);
                continue;
            }
            if (!_portfolio.Securities.TryGetValue(symbol, out var security)) continue;
            var last = security.GetLastData();
            if (last == null || ReferenceEquals(last, _syntheticReferences.GetValueOrDefault(symbol))) continue;
            var sourceAt = SourceAt(security, last.EndTime);
            references.RemoveAll(reference => reference.At <= sourceAt);
            if (references.Count == 0)
            {
                _pendingReferences.Remove(symbol);
                _syntheticReferences.Remove(symbol);
                continue;
            }
            foreach (var reference in references)
            {
                if (reference.At > now) break;
                SetReference(security, reference);
                changed = true;
            }
        }
        if (changed) _portfolio.InvalidateTotalPortfolioValue();
    }

    public void CaptureRecord(DateTimeOffset now) => Advance(now);

    public decimal LockedBonusQuantity(Symbol symbol) => _unsettled.Values
        .Where(x => x.Symbol == symbol && x.Credited && !x.Released).Sum(x => x.Bonus);

    public decimal RegisteredQuantity(string actionId) => _rights.TryGetValue(actionId, out var right) ? right.Registered : 0;

    public IEnumerable<Symbol> ValuationSymbols => _unsettled.Values
        .Where(x => x.Ex && x.Bonus > 0 && (!x.Credited || !x.Released)).Select(x => x.Symbol).Distinct();

    public IReadOnlyList<ShareEntitlementState> GetShareEntitlements(DateTimeOffset asOf)
    {
        RequireValuationTime(asOf);
        return _unsettled.Values.Where(x => x.Ex && !x.Action.IsCash && !x.Credited && x.Bonus > 0)
            .Select(x => new ShareEntitlementState(x.Action.ActionId, x.Action.Revision, x.Symbol,
                x.Bonus, ShareValue(x, asOf), x.Action.SharesSellableAt!.Value,
                x.Action.AvailableAt, x.Action.SharesArrivalAt, x.Action.ShareSettlementPolicy,
                x.Action.SourceRef)).ToArray();
    }

    private void RequireValuationTime(DateTimeOffset at)
    {
        var clock = _clock ?? throw new InvalidOperationException("估值前须推进公司行动时钟");
        if (at < clock) throw new InvalidOperationException("权益估值时间不能早于已处理公司行动");
    }

    private decimal ShareValue(Entitlement right, DateTimeOffset at)
    {
        if (right.Bonus == 0m) return 0m;
        if (IsDelisted?.Invoke(right.Symbol, at) == true) return 0m;
        var security = _portfolio.Securities[right.Symbol];
        var last = security.GetLastData();
        if (last is null && right.Bonus > 0) throw new InvalidOperationException("股份权益缺少可见价格");
        if (last is not null && new DateTimeOffset(DateTime.SpecifyKind(last.EndTime, DateTimeKind.Unspecified),
            TimeSpan.FromHours(8)) > at)
            throw new InvalidOperationException("权益估值不能使用未来价格");
        return right.Bonus * security.Price;
    }

    public EntitlementValuation GetValuation(DateTimeOffset? asOf = null)
    {
        var clock = _clock ?? throw new InvalidOperationException("估值前须推进公司行动时钟");
        var at = asOf ?? clock;
        var value = GetEntitlementAmounts(at);
        return new EntitlementValuation(at, value.CashReceivable, value.PendingBonusValue,
            value.PendingBonusQuantity, value.LockedBonusValue, _portfolio.TotalPortfolioValue);
    }

    public (decimal CashReceivable, decimal PendingBonusValue, decimal PendingBonusQuantity, decimal LockedBonusValue)
        GetEntitlementAmounts(DateTimeOffset at)
    {
        RequireValuationTime(at);
        decimal cash = 0, pendingValue = 0, pendingQuantity = 0, lockedValue = 0;
        foreach (var right in _unsettled.Values)
        {
            if (!right.Ex) continue;
            if (right.Action.IsCash) cash += right.Cash;
            else if (right.Credited) lockedValue += ShareValue(right, at);
            else
            {
                pendingQuantity += right.Bonus;
                pendingValue += ShareValue(right, at);
            }
        }
        return (cash, pendingValue, pendingQuantity, lockedValue);
    }

    private void Log(Entitlement right, string phase, DateTimeOffset at, decimal cash = 0, decimal shares = 0,
        bool affectsAccount = false)
        => _transitions.Add(new ActionTransition(right.Action.ActionId, right.Action.Revision,
            phase, at, right.Registered, cash, shares) { AffectsAccount = affectsAccount });
}

public sealed record RegisteredEntitlementState(string ActionId, int Revision, Symbol Symbol,
    decimal RegisteredQuantity, decimal CashEntitlement, decimal BonusQuantity,
    bool ExApplied, bool CashPaid, bool SharesArrived, bool SharesSellable,
    bool SharesCredited, DateTimeOffset? SourceSharesArrivalAt, string? SharesArrivalDate,
    DateTimeOffset? SharesSellableAt, string ShareSettlementPolicy);

public sealed record ShareEntitlementState(string ActionId, int Revision, Symbol Symbol,
    decimal PendingQuantity, decimal PendingValue, DateTimeOffset SharesSellableAt,
    DateTimeOffset AvailableAt, DateTimeOffset? SourceSharesArrivalAt, string ShareSettlementPolicy,
    string SourceRef);
