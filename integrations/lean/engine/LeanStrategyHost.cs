using System.Diagnostics;
using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Text.Json;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantWitness.Lean.China;
using QuantWitness.Lean.CorporateActions;
using QuantWitness.LeanInput;

namespace QuantWitness.Lean.Research;

// 只翻译原策略需要的账户、报价和委托，交易计算仍由LEAN及中国模型处理。
public sealed partial class LeanStrategyHost
{
    private readonly QCAlgorithm _algorithm;
    private readonly Dictionary<string, Symbol> _symbols = new();
    private readonly Dictionary<Symbol, string> _codes = new();
    private readonly Dictionary<string, JsonElement> _instruments = new();
    private readonly Dictionary<string, StockLifecycle> _lifecycle = new();
    private readonly KeyValuePair<string, StockLifecycle>[] _lifecycleByTime;
    private int _nextSpotLifecycle;
    private DateTimeOffset? _spotClock;
    private readonly HashSet<Symbol> _zeroMarked = new();
    private readonly DateTimeOffset? _terminalObservationAt;
    private bool _terminalObserved;
    private readonly ConcurrentDictionary<Symbol, SecurityHolding> _heldSpot = new();
    private readonly Dictionary<int, decimal> _spent = new();
    private readonly Dictionary<int, decimal> _filled = new();
    private readonly int _sessionCount;
    private readonly HashSet<DateTime> _completedWindows = new();
    private readonly Dictionary<DateTime, SpotExecutionWindow> _executionWindows;
    private readonly ChinaRuleBook _spotRuleBook;
    private DateTimeOffset? _lastObservationAt;
    private string _lastEquitySemantics;
    private bool _lastSessionComplete;
    private bool _failureAccountSaved;
    private readonly StreamWriter _orders;
    private readonly StreamWriter _daily;
    private readonly string _output;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private long _rows;
    private long _dayRows;
    private long _orderEventCount;
    private double _previousClose;
    private readonly SortedDictionary<int, (long Calls, long CacheHits, long ElapsedTicks)> _accountViewTiming = new();
    private readonly record struct AccountPosition(string Code, Symbol Symbol, decimal Quantity,
        decimal Sellable, StockLifecycle Ending);
    private sealed record CachedAccount(DateTime Day, decimal Cash, AccountPosition[] Positions, string Json);
    private readonly CachedAccount[] _accountViews = new CachedAccount[2];
    private readonly List<AccountPosition> _accountPositions = new();
    private readonly FrozenFinancialDayStore _direct;
    public bool IsDaily { get; }
    public ChinaAStockModels Models { get; }
    public CorporateActionProcessor Actions { get; }
    public string EngineStatus => _algorithm.Status.ToString();
    public bool HasRuntimeError => _algorithm.RunTimeError != null;
    public string RuntimeErrorText => _algorithm.RunTimeError?.ToString() ?? "";
    private DateTimeOffset Now => new(DateTime.SpecifyKind(_algorithm.Time, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    public sealed record StockLifecycle(DateOnly DelistedDate, DateTimeOffset AvailableAt, string SourceRef)
    {
        public DateTimeOffset EffectiveAt => new(DelistedDate.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.FromHours(8));
        public DateTimeOffset ConfirmedAt => AvailableAt > EffectiveAt ? AvailableAt : EffectiveAt;
        public bool IsEffective(DateTimeOffset at) => at >= ConfirmedAt;
    }

    private StockLifecycle ConfirmedLifecycle(Symbol symbol, DateTimeOffset at) =>
        _lifecycle.TryGetValue(_codes[symbol], out var ending) && ending.IsEffective(at) ? ending : null;

    public bool IsDelisted(Symbol symbol, DateTime utc) => ConfirmedLifecycle(symbol, new DateTimeOffset(utc)) != null;

    private object ZeroMark(Symbol symbol, StockLifecycle ending) => new {
        instrument_id = _codes[symbol], price = 0m, source = "delisted_zero",
        price_source_at = ending.EffectiveAt,
        price_available_at = ending.ConfirmedAt,
        is_current_session = false, is_current_session_close = false,
        delisted_date = ending.DelistedDate.ToString("yyyy-MM-dd"), source_ref = ending.SourceRef
    };

    private void ApplySpotLifecycle(DateTimeOffset now)
    {
        while (_nextSpotLifecycle < _lifecycleByTime.Length && _lifecycleByTime[_nextSpotLifecycle].Value.ConfirmedAt <= now)
        {
            var pair = _lifecycleByTime[_nextSpotLifecycle++];
            var symbol = _symbols[pair.Key];
            if (_algorithm.Securities.TryGetValue(symbol, out var security)) MarkSpotDelisted(security, pair.Value, now);
        }
    }

    private void MarkSpotDelisted(Security security, StockLifecycle ending, DateTimeOffset now)
    {
        var symbol = security.Symbol;
        // 这是持仓估值假设，不向行情缓存或历史窗口写入零价bar。
        if (security.Holdings.Price != 0m)
        {
            security.Holdings.UpdateMarketPrice(0m);
            _algorithm.Portfolio.InvalidateTotalPortfolioValue();
        }
        security.IsTradable = false;
        if (!_zeroMarked.Add(symbol)) return;
        AppendSpotResult("lifecycle", new {
            at = now, instrument_id = _codes[symbol], kind = "delisted_zero_mark",
            quantity = security.Holdings.Quantity, cash_delta = 0m, valuation_mark = ZeroMark(symbol, ending)
        });
        _algorithm.Transactions.CancelOpenOrders(symbol, "退市生效后停止交易，保留股份并按零估值");
    }

    internal void RefreshSpotMarketReferences(Slice slice)
    {
        if (_zeroMarked.Count == 0 && !Actions.HasPendingMarketReferences) return;
        var now = Now;
        var symbols = slice.AllData.Select(data => data.Symbol).Where(_codes.ContainsKey).Distinct().ToArray();
        foreach (var symbol in symbols)
            if (ConfirmedLifecycle(symbol, now) is { } ending && _algorithm.Securities.TryGetValue(symbol, out var security))
                MarkSpotDelisted(security, ending, now);
        Actions.RefreshMarketReferences(now, symbols);
    }

    public void AdvanceSpotClock()
    {
        if (IsFuture) return;
        var now = Now;
        if (_spotClock == now) return;
        _spotRuleBook?.AdvanceTime(_algorithm.UtcTime);
        ApplySpotLifecycle(now);
        Actions.AdvanceBeforeTrading(now);
        _spotClock = now;
    }

    public LeanStrategyHost(QCAlgorithm algorithm, string directory)
    {
        _algorithm = algorithm;
        _output = directory;
        var data = ManifestCache.Read(Path.Combine(directory, "input.json"));
        var settings = ManifestCache.Read(Path.Combine(directory, "strategy-input.json"));
        IsFuture = settings.GetProperty("declaration").TryGetProperty("asset_class", out var asset)
            && asset.GetString() == "cn_future";
        var priceScale = data.GetProperty("price_scale").GetInt32();
        if (!IsFuture && priceScale is not (2 or 3)) throw new ArgumentException("现货价格精度仅支持二位或三位");
        if (data.TryGetProperty("rules_parquet", out var frozenRules)
            && (!JsonElement.DeepEquals(frozenRules.GetProperty("scope"), settings.GetProperty("minute_scope"))
                || data.GetProperty("asset_class").GetString() != asset.GetString()))
            throw new InvalidDataException("现货规则来源与已准入分钟范围不一致");
        _direct = FrozenFinancialDayStore.Open(Path.Combine(directory, "input.json"));
        IsDaily = _direct.Frequency == "1d";
        foreach (var entry in settings.GetProperty("instruments").EnumerateObject()) _instruments.Add(entry.Name, entry.Value);
        if (!IsFuture && settings.TryGetProperty("lifecycle", out var lifecycle))
            foreach (var row in lifecycle.EnumerateObject())
                _lifecycle.Add(row.Name, new StockLifecycle(
                    DateOnly.ParseExact(row.Value.GetProperty("delisted_date").GetString(), "yyyy-MM-dd"),
                    DateTimeOffset.Parse(row.Value.GetProperty("available_at").GetString()),
                    row.Value.GetProperty("source_ref").GetString()));
        _lifecycleByTime = _lifecycle.OrderBy(row => row.Value.ConfirmedAt).ToArray();
        var dates = data.GetProperty("dates").EnumerateArray().Select(x => DateTime.Parse(x.GetString())).ToArray();
        if (!IsFuture && data.TryGetProperty("terminal_observation_at", out var terminal))
            _terminalObservationAt = DateTimeOffset.Parse(terminal.GetString());
        if (!IsFuture) _executionWindows = _direct.ExecutionWindows;
        _sessionCount = dates.Length;
        algorithm.SetStartDate(dates[0]); algorithm.SetEndDate(dates[^1]);
        if (_terminalObservationAt.HasValue) algorithm.SetEndDate(_terminalObservationAt.Value.ToOffset(TimeSpan.FromHours(8)).Date);
        algorithm.SetTimeZone("Asia/Shanghai");
        var initialCash = settings.GetProperty("initial_cash_units").GetInt64();
        if (initialCash < 0) throw new ArgumentException("初始现金不能为负");
        algorithm.SetAccountCurrency("CNY", initialCash / 100m);
        algorithm.SetBenchmark(t => 1m);
        algorithm.SetRiskFreeInterestRateModel(new ConstantRiskFreeRateInterestRateModel(0m));
        algorithm.SetAvailableDataTypes(new Dictionary<SecurityType, List<TickType>> {
            [IsFuture ? SecurityType.Future : SecurityType.Equity] = new() { TickType.Trade } });
        _orders = new StreamWriter(Path.Combine(directory, "orders.jsonl"));
        File.WriteAllText(Path.Combine(directory, "intents.jsonl"), "");
        _daily = new StreamWriter(Path.Combine(directory, "daily.jsonl"));
        if (IsFuture) { InitializeFutures(data, settings); return; }
        foreach (var name in new[] { "fills", "positions", "account_events", "lifecycle" })
            File.WriteAllText(Path.Combine(directory, name + ".jsonl"), "");
        Market.Add("jqchina", 999);
        var tickerSymbols = new Dictionary<string, Symbol>(StringComparer.Ordinal);
        foreach (var code in data.GetProperty("symbols").EnumerateArray().Select(x => x.GetString()))
        {
            var ticker = code.Split('.')[0];
            var symbol = Symbol.Create(ticker, SecurityType.Equity, "jqchina");
            _symbols.Add(code, symbol);
            _codes.Add(symbol, code);
            tickerSymbols.TryAdd(ticker, symbol);
            tickerSymbols.TryAdd(code, symbol);
        }
        algorithm.Securities.CollectionChanged += SpotSecurityChanged;
        foreach (var security in algorithm.Securities.Values) TrackSpotSecurity(security);
        var actions = CorporateActionJson.Load(Path.Combine(directory, "actions.json"));
        Actions = new CorporateActionProcessor(algorithm.Portfolio,
            ticker => tickerSymbols[ticker], actions);
        Actions.MarketPriceSourceTime = symbol =>
            _algorithm.Securities[symbol].GetLastData() switch {
                ResearchTradeBar bar => bar.ObservationEndTime,
                _ => null };
        Actions.IsDelisted = (symbol, at) => ConfirmedLifecycle(symbol, at) != null;
        var start = new DateTimeOffset(_executionWindows[dates[0]].StartAt, TimeSpan.FromHours(8));
        Actions.SeedInitialEmptyPortfolio(start);
        var book = new ChinaRuleBook(dates.Select(DateOnly.FromDateTime), _direct.PrepareSpotRules, _direct.SpotRules);
        _spotRuleBook = book;
        Models = new ChinaAStockModels(book,
            NativeCode, symbol => Actions.LockedBonusQuantity(symbol),
            symbol => _instruments[NativeCode(symbol)].GetProperty("asset_class").GetString());
        Models.IsDelisted = IsDelisted;
        var parameters = settings.GetProperty("parameters");
        Models.ConfigureExecution(parameters.GetProperty("slippage_bps").GetDecimal(),
            parameters.GetProperty("slippage_ticks").GetInt32(), parameters.GetProperty("participation_cap_ppm").GetInt32());
        Models.Install(algorithm);
        algorithm.DefaultOrderProperties = new ChinaOrderProperties {
            TimeInForce = TimeInForce.Day, DailyOpenDecision = IsDaily };
        Models.BeforeDailyOpen = (utc, id) =>
        {
            AdvanceSpotClock();
            return _algorithm.Transactions.GetOpenOrders().Any(order => order.Id == id);
        };
    }

    public void FreezeNativeModels()
    {
        if (IsFuture ? _algorithm.BrokerageModel is not ChinaFutureBrokerageModel
            : !ReferenceEquals(_algorithm.BrokerageModel, Models.BrokerageModel))
            throw new InvalidOperationException("原生算法必须保留已准入的中国交易模型");
        _algorithm.FreezeResearchBrokerageModel();
        foreach (var security in _algorithm.Securities.Values) FreezeNativeSecurity(security);
        _algorithm.Securities.CollectionChanged += NativeSecurityChanged;
    }

    private void NativeSecurityChanged(object sender, NotifyCollectionChangedEventArgs change)
    {
        if (change.NewItems != null)
            foreach (Security security in change.NewItems) FreezeNativeSecurity(security);
    }

    private void FreezeNativeSecurity(Security security)
    {
        if (security.Type == SecurityType.Base) return;
        if (!_codes.ContainsKey(security.Symbol)
            || (IsFuture ? !FutureModels.OwnsTradingModels(security) :
                !ReferenceEquals(security.FillModel, Models.FillModel) || !ReferenceEquals(security.FeeModel, Models.FeeModel)))
            throw new InvalidOperationException("证券或交易模型不属于冻结的中国市场输入");
        security.FreezeResearchTradingModels();
    }

    private string NativeCode(Symbol symbol) => _codes.TryGetValue(symbol, out var code) ? code
        : throw new InvalidOperationException($"证券未包含在冻结输入中：{symbol}");

    public bool IsNativeInstrument(Symbol symbol) => _codes.ContainsKey(symbol);

    internal NativeMarketLifecycle.SpotOrderRuleView SpotOrderRules(Symbol symbol)
    {
        var utc = _algorithm.UtcTime;
        var rule = _spotRuleBook.Resolve(_codes[symbol], utc);
        rule.RequireVisibleReference(utc);
        return new(rule.PriceTickUnits / (decimal)Math.Pow(10, rule.PriceScale), rule.LowLimit, rule.HighLimit,
            rule.MarketProtectionRequired, rule.MarketOrderAllowed, rule.Paused, rule.SettlementDays,
            rule.Grid(true, true), rule.Grid(false, true), rule.Grid(true, false), rule.Grid(false, false));
    }

    public bool ExecutionWindowEndsNow => !IsFuture && _executionWindows.TryGetValue(_algorithm.Time.Date, out var window)
        && _algorithm.Time == window.EndAt;
    public bool TerminalObservationNow => !IsFuture && _terminalObservationAt == Now;

    public void ObserveTerminalAccount()
    {
        if (!TerminalObservationNow || _terminalObserved) throw new InvalidOperationException("终点账户观察时点不匹配或重复");
        AdvanceSpotClock();
        var marks = ValuationMarks();
        var value = Actions.GetValuation(Now);
        _lastObservationAt = Now;
        _lastEquitySemantics = IsDaily ? "latest_visible_mark_not_session_close" : "execution_window_mark";
        _lastSessionComplete = false;
        AppendSpotResult("account_events", new {
            reason = "terminal", valuation_at = Now, cash = _algorithm.Portfolio.CashBook["CNY"].Amount,
            equity = value.TotalEquity, additional_equity = value.AdditionalEquity,
            equity_semantics = _lastEquitySemantics, valuation_marks = marks
        });
        _terminalObserved = true;
    }
    public bool SessionClosesNow => !IsFuture && _executionWindows.TryGetValue(_algorithm.Time.Date, out var window)
        && _algorithm.Time == window.SessionCloseAt;
    public DateTime NativeExecutionStartUtc => IsFuture
        ? _futureSessions.Min(row => FutureTime(row, "start_at"))
        : _executionWindows.Values.Min(window => window.StartAt).ConvertToUtc(TimeZones.Shanghai);
    public void ObserveBars(Slice data)
    {
        var count = 0;
        if (IsDaily) count = data.Bars.Values.OfType<ResearchTradeBar>().Count();
        else
        {
            foreach (var bar in data.Bars.Values)
            {
                if (bar is DailyOpenBar || bar.IsFillForward) continue;
                if (bar.EndTime > _algorithm.Time)
                    throw new InvalidOperationException("未完成行情不能进入策略执行");
                count++;
            }
        }
        _rows += count;
        _dayRows += count;
    }

    public void OnOrderEvent(OrderEvent e)
    {
        if (IsFuture) { FutureOrderEvent(e); return; }
        Models.OnOrderEvent(e);
        var order = _algorithm.Transactions.GetOrderById(e.OrderId);
        if (e.FillQuantity != 0)
        {
            _filled[e.OrderId] = _filled.GetValueOrDefault(e.OrderId) + Math.Abs(e.FillQuantity);
            _spent[e.OrderId] = _spent.GetValueOrDefault(e.OrderId) + e.FillQuantity * e.FillPrice + e.OrderFee.Value.Amount;
        }
        _orders.WriteLine(JsonSerializer.Serialize(new {
            order_event_count=++_orderEventCount,
            order_id=e.OrderId, event_id=e.Id, instrument_id=_codes[e.Symbol], status=e.Status.ToString(),
            order_type=order.Type.ToString(), time_in_force=order.Properties.TimeInForce.ToString(),
            order_quantity=order.Quantity, tag=order.Tag,
            limit_price=(order as LimitOrder)?.LimitPrice, stop_price=(order as StopMarketOrder)?.StopPrice,
            submitted_at=new DateTimeOffset(DateTime.SpecifyKind(order.Time,DateTimeKind.Utc)), at=Now,
            quantity=e.FillQuantity, price=e.FillPrice, fee=e.OrderFee.Value.Amount, message=e.Message,
            cash=_algorithm.Portfolio.CashBook["CNY"].Amount,
            holding=_algorithm.Portfolio[e.Symbol].Quantity,
            budget_cny=(order.Properties as ChinaOrderProperties)?.BudgetCny
        }, JsonOptions));
        _orders.Flush();
        RecordSpotFill(e);
    }


    private decimal ReservedCash(IEnumerable<Order> orders) => orders
        .Where(x => x.Direction == OrderDirection.Buy)
        .Sum(x => Math.Max(0m, ((x.Properties as ChinaOrderProperties)?.BudgetCny ?? 0m) - _spent.GetValueOrDefault(x.Id)));

    internal string AccountView(bool includeRegistrations)
    {
        if (IsFuture) return FutureAccountView();
        var started = Stopwatch.GetTimestamp();
        AdvanceSpotClock();
        lock (_accountPositions) return ReadSpotAccount(includeRegistrations, started);
    }

    private string ReadSpotAccount(bool includeRegistrations, long started)
    {
        var now = Now;
        var utc = _algorithm.UtcTime;
        _accountPositions.Clear();
        foreach (var (symbol, holding) in _heldSpot)
        {
            if (holding.Quantity == 0) continue;
            var ending = ConfirmedLifecycle(symbol, now);
            _accountPositions.Add(new(_codes[symbol], symbol, holding.Quantity,
                ending != null ? 0m : Models.SellableQuantity(_algorithm.Securities[symbol], utc), ending));
        }
        _accountPositions.Sort((left, right) => StringComparer.Ordinal.Compare(left.Code, right.Code));
        var openOrders = _algorithm.Transactions.GetOpenOrders();
        var cash = _algorithm.Portfolio.CashBook["CNY"].Amount;
        var index = includeRegistrations ? 1 : 0;
        var cached = _accountViews[index];
        // 活跃委托与公司行动权益允许在同一时点变化；仅复用可完整判等的空委托、空权益账户。
        var reusable = openOrders.Count == 0 && !Actions.HasUnsettledEntitlements;
        if (reusable && cached != null && cached.Day == now.Date && cached.Cash == cash
            && System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_accountPositions).SequenceEqual(cached.Positions))
        {
            RecordTiming(cacheHit: true);
            return cached.Json;
        }
        var positions = _accountPositions.Select(row => new
        {
            instrument = _instruments[row.Code], quantity = row.Quantity, sellable_quantity = row.Sellable,
            valuation_mark = row.Ending == null ? null : ZeroMark(row.Symbol, row.Ending),
        });
        var active = openOrders.OrderBy(x => x.Id).Select(order => new {
            instrument=_instruments[_codes[order.Symbol]], order_id=order.Id.ToString(),
            side=order.Direction == OrderDirection.Buy ? "buy" : "sell", quantity=Math.Abs(order.Quantity),
            filled_quantity=_filled.GetValueOrDefault(order.Id)
        }).ToArray();
        var entitlements = Actions.GetEntitlementAmounts(now);
        var result = JsonSerializer.Serialize(new {positions,active_orders=active,
            unsettled_registration_codes=includeRegistrations ? Actions.UnsettledSymbols.Select(s => _codes[s]).OrderBy(c => c, StringComparer.Ordinal).ToArray() : null,
            share_receivables=ShareReceivables(Now),
            corporate_entitlements=new {cash_receivable=entitlements.CashReceivable,
                pending_bonus_value=entitlements.PendingBonusValue,pending_bonus_quantity=entitlements.PendingBonusQuantity,
                locked_bonus_value=entitlements.LockedBonusValue,
                additional_equity=entitlements.CashReceivable + entitlements.PendingBonusValue},
            available_cash=new {units=(long)decimal.Floor(Math.Max(0,cash-ReservedCash(openOrders))*100),scale=2,currency="CNY"}},JsonOptions);
        if (reusable) _accountViews[index] = new(now.Date, cash, _accountPositions.ToArray(), result);
        RecordTiming();
        return result;

        void RecordTiming(bool cacheHit = false)
        {
            var month = now.Year * 100 + now.Month;
            var timing = _accountViewTiming.GetValueOrDefault(month);
            _accountViewTiming[month] = (timing.Calls + 1, timing.CacheHits + (cacheHit ? 1 : 0),
                timing.ElapsedTicks + Stopwatch.GetTimestamp() - started);
        }
    }

    private void SpotSecurityChanged(object sender, NotifyCollectionChangedEventArgs change)
    {
        if (change.OldItems != null)
            foreach (Security security in change.OldItems)
            {
                security.Holdings.QuantityChanged -= SpotHoldingChanged;
                _heldSpot.TryRemove(security.Symbol, out _);
            }
        if (change.NewItems != null)
            foreach (Security security in change.NewItems) TrackSpotSecurity(security);
    }

    private void TrackSpotSecurity(Security security)
    {
        if (!_codes.ContainsKey(security.Symbol)) return;
        security.Holdings.QuantityChanged += SpotHoldingChanged;
        UpdateSpotHolding(security.Holdings);
        if (_spotClock.HasValue && ConfirmedLifecycle(security.Symbol, Now) is { } ending)
            MarkSpotDelisted(security, ending, Now);
    }

    private void SpotHoldingChanged(object sender, SecurityHoldingQuantityChangedEventArgs change) =>
        UpdateSpotHolding(change.Security.Holdings);

    private void UpdateSpotHolding(SecurityHolding holding)
    {
        if (holding.Quantity == 0) _heldSpot.TryRemove(holding.Symbol, out _);
        else _heldSpot[holding.Symbol] = holding;
    }

    private object[] ShareReceivables(DateTimeOffset at) => Actions.GetShareEntitlements(at).Select(row => (object)new {
        action_id=row.ActionId, revision=row.Revision, instrument=_instruments[_codes[row.Symbol]],
        quantity=(long)row.PendingQuantity,
        value=new {units=(long)decimal.Round(row.PendingValue*100,0,MidpointRounding.AwayFromZero),scale=2,currency="CNY"},
        sellable_at=row.SharesSellableAt, available_at=row.AvailableAt,
        source_shares_arrival_at=row.SourceSharesArrivalAt,
        share_settlement_policy=row.ShareSettlementPolicy, source_ref=row.SourceRef
    }).ToArray();

    public void CloseOrders() => _algorithm.Transactions.CancelOpenOrders();
    private IEnumerable<Symbol> ValuationSymbols => _heldSpot.Where(x => x.Value.Quantity != 0).Select(x => x.Key)
        .Concat(Actions.ValuationSymbols).Distinct().OrderBy(x => _codes[x], StringComparer.Ordinal);
    private string EquitySemantics => IsDaily ? "latest_visible_mark_not_session_close"
        : SessionClosesNow && ValuationSymbols.All(symbol =>
            ConfirmedLifecycle(symbol, Now) != null || _algorithm.Securities[symbol].GetLastData() is TradeBar bar && bar.Period > TimeSpan.Zero
            && bar.EndTime == _algorithm.Time) ? "session_close_mark" : "execution_window_mark";

    private object ValuationMark(Symbol symbol)
    {
        var ending = ConfirmedLifecycle(symbol, Now);
        if (ending != null) return ZeroMark(symbol, ending);
        var security = _algorithm.Securities[symbol];
        var data = security.GetLastData();
        _spotRuleBook.Resolve(_codes[symbol], _algorithm.UtcTime);
        if (data == null || data.EndTime > _algorithm.Time || (!IsDaily && SessionClosesNow && (data.EndTime.Date != _algorithm.Time.Date
            || data is not TradeBar minute || minute.Period == TimeSpan.Zero)))
            throw new InvalidOperationException($"{_codes[symbol]} {_algorithm.Time:yyyy-MM-ddTHH:mm:ss} 估值缺少当日可见市场价格");
        var at = new DateTimeOffset(DateTime.SpecifyKind(data.EndTime, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
        var sourceAt = data is ResearchTradeBar observed ? observed.ObservationEndTime : data.EndTime;
        var source = data is DailyOpenBar ? "session_open"
            : IsDaily && data is ResearchTradeBar ? "completed_daily_close"
            : data is TradeBar bar && bar.Period == TimeSpan.Zero ? "corporate_action_reference"
            : "completed_minute_close";
        return new { instrument_id = _codes[symbol], price = security.Price, source,
            price_available_at = at,
            price_source_at = new DateTimeOffset(DateTime.SpecifyKind(sourceAt, DateTimeKind.Unspecified), TimeSpan.FromHours(8)),
            is_current_session = sourceAt.Date == _algorithm.Time.Date,
            is_current_session_close = !IsDaily && data.EndTime.Date == _algorithm.Time.Date
                && data.EndTime.TimeOfDay == TimeSpan.FromHours(15) };
    }

    private object[] ValuationMarks() => ValuationSymbols.Select(ValuationMark).ToArray();

    public void CloseSession()
    {
        if (IsFuture) { CloseFutureSession(); return; }
        if (!ExecutionWindowEndsNow) throw new InvalidOperationException("账户观察必须位于声明执行窗口终点");
        if (_completedWindows.Contains(_algorithm.Time.Date)) throw new InvalidOperationException("执行窗口不能重复结束");
        Actions.CompleteTimeStep(Now);
        var marks = ValuationMarks();
        var positions=new List<object>();
        foreach(var (symbol, holding) in _heldSpot.OrderBy(x=>_codes[x.Key],StringComparer.Ordinal))
        {
            if(holding.Quantity==0) continue;
            positions.Add(new {instrument_id=_codes[symbol],quantity=holding.Quantity,
                price=ConfirmedLifecycle(symbol, Now) != null ? 0m : _algorithm.Securities[symbol].Price});
        }
        var value=Actions.GetValuation(Now); var elapsed=_clock.Elapsed.TotalSeconds;
        _lastEquitySemantics = EquitySemantics;
        _lastSessionComplete = SessionClosesNow;
        _daily.WriteLine(JsonSerializer.Serialize(new {date=_algorithm.Time.ToString("yyyy-MM-dd"),rows=_dayRows,
            cash=_algorithm.Portfolio.CashBook["CNY"].Amount,equity=value.TotalEquity,additional_equity=value.AdditionalEquity,
            equity_semantics=_lastEquitySemantics,valuation_at=Now,valuation_marks=marks,
            session_complete=_lastSessionComplete,
            positions,active_orders=_algorithm.Transactions.GetOpenOrders().Count,day_wall_seconds=elapsed-_previousClose},JsonOptions));
        _daily.Flush();_dayRows=0;_previousClose=elapsed;
        _lastObservationAt = Now;
        _completedWindows.Add(_algorithm.Time.Date);
    }

    public void Finish()
    {
        if (IsFuture) { FinishFutures(); return; }
        try
        {
            if (HasRuntimeError || !_executionWindows.Keys.All(_completedWindows.Contains)
                || _terminalObservationAt.HasValue && !_terminalObserved)
                throw new InvalidOperationException("存在运行错误或声明执行窗口未完成，不能结束Host");
            ApplySpotLifecycle(Now);
            RecordSpotTransitions();
        }
        finally { DisposeSpotWriters(); }
        WriteSpotFinalAccount();
    }

    private void WriteSpotFinalAccount()
    {
        var value=Actions.GetValuation(_lastObservationAt);
        File.WriteAllText(Path.Combine(_output, "spot-account-view.json"), JsonSerializer.Serialize(
            _accountViewTiming.Select(row => new {
                trading_month = row.Key, calls = row.Value.Calls, cache_hits = row.Value.CacheHits,
                elapsed_seconds = (double)row.Value.ElapsedTicks / Stopwatch.Frequency
            }), JsonOptions));
        File.WriteAllText(Path.Combine(_output,"account.json"),JsonSerializer.Serialize(new {cash=_algorithm.Portfolio.CashBook["CNY"].Amount,
            equity=value.TotalEquity,additional_equity=value.AdditionalEquity,rows=_rows,
            equity_semantics=_lastEquitySemantics,valuation_at=_lastObservationAt,valuation_marks=ValuationMarks(),
            session_complete=_lastSessionComplete,
            terminal_observation_at=_terminalObserved ? _terminalObservationAt : null,
            share_receivables=ShareReceivables(_lastObservationAt!.Value),
            positions=_algorithm.Portfolio.Where(x=>x.Value.Quantity!=0).ToDictionary(x=>_codes[x.Key],x=>x.Value.Quantity),
            active_orders=OpenOrderState(),
            transitions=Actions.Transitions,algorithm_wall_seconds=_clock.Elapsed.TotalSeconds},new JsonSerializerOptions{WriteIndented=true}));
    }

    private object[] OpenOrderState() => _algorithm.Transactions.GetOpenOrders().OrderBy(x => x.Id).Select(order => (object)new {
        order_id=order.Id, instrument_id=_codes[order.Symbol], status=order.Status.ToString(), quantity=order.Quantity,
        filled_quantity=_filled.GetValueOrDefault(order.Id),
        remaining_quantity=Math.Abs(order.Quantity)-_filled.GetValueOrDefault(order.Id)
    }).ToArray();

    public void SaveFailureAccount(string reason)
    {
        if (IsFuture || _failureAccountSaved) return;
        try
        {
            RecordSpotTransitions();
            WriteSpotFailureAccount(reason);
            _failureAccountSaved = true;
        }
        finally { DisposeSpotWriters(); }
    }

    private void WriteSpotFailureAccount(string reason)
    {
        // 失败材料只保存账簿事实，不调用估值，也不把缺价持仓解释为零资产。
        var transitions = Actions?.Transitions ?? Array.Empty<ActionTransition>();
        var registered = Actions == null ? Array.Empty<object>() : Actions.RegisteredEntitlements.Select(row => (object)new {
                action_id=row.ActionId, revision=row.Revision, instrument_id=_codes[row.Symbol],
                registered_quantity=row.RegisteredQuantity,
                cash_entitlement=row.CashEntitlement, bonus_quantity=row.BonusQuantity,
                ex_applied=row.ExApplied, cash_paid=row.CashPaid,
                shares_arrived=row.SharesArrived, shares_credited=row.SharesCredited, shares_sellable=row.SharesSellable,
                source_shares_arrival_at=row.SourceSharesArrivalAt, shares_arrival_date=row.SharesArrivalDate,
                shares_sellable_at=row.SharesSellableAt, share_settlement_policy=row.ShareSettlementPolicy
        }).ToArray();
        File.WriteAllText(Path.Combine(_output,"failure-account.json"), JsonSerializer.Serialize(new {
            status="failed", artifact_type="research.lean-failure-account-diagnostic", is_result=false,
            failed_at=Now, reason, cash=_algorithm.Portfolio.CashBook["CNY"].Amount,
            positions=_algorithm.Portfolio.Where(x=>x.Value.Quantity!=0).ToDictionary(x=>_codes[x.Key],x=>x.Value.Quantity),
            registered_entitlements=registered, active_orders=OpenOrderState(), transitions,
            corporate_actions_source="actions.json", orders_source="orders.jsonl"
        }, new JsonSerializerOptions {WriteIndented=true}));
    }
}
