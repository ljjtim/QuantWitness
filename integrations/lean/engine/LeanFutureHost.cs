using System.Text.Json;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantWitness.Lean.China;

namespace QuantWitness.Lean.Research;

public sealed partial class LeanStrategyHost
{
    public bool IsFuture { get; private set; }
    public ChinaFutureModels FutureModels { get; private set; }
    private JsonElement _futureContracts;
    private JsonElement[] _futureSessions;
    private readonly Dictionary<string, JsonElement> _currentFutureSessions = new();
    private (DateTime Day, DateTime Start)[] _futureLoadSchedule;
    private Dictionary<string, DateTime> _futureDayEnds;
    private int _futureLoadIndex, _futureSessionIndex;
    private DateTime _lastFutureEnd;
    private readonly Dictionary<DateTime, JsonElement> _futureHeaders = new();
    private readonly HashSet<(string, string)> _startedFutureSessions = new();
    private readonly HashSet<(string, string)> _futureSettled = new();
    private readonly HashSet<string> _futureClosedDays = new();
    private readonly Dictionary<string, DateTimeOffset> _futureMarks = new();
    private readonly Dictionary<string, string> _futureMarkSources = new();
    private readonly Dictionary<string, string> _settledThrough = new();
    private StreamWriter _fillsOutput, _positionsOutput, _accountOutput, _lifecycleOutput;
    private long _futureSequence;
    private readonly HashSet<(int, int)> _futureOrderEvents = new();
    private bool _advancingFutureClock;
    private bool _maintainingFutureRisk;
    private DateTime? _riskLastAttemptAt;
    private DateTime? _riskRecoveredAt;
    private string _futureTradingDate;
    private const string FutureAccountId = "futures-CNY";
    public string TradingDateText => IsFuture ? _futureTradingDate : _algorithm.Time.ToString("yyyy-MM-dd");
    public bool FutureObservationOnly => IsFuture && _lastFutureEnd < _algorithm.UtcTime;
    public bool FutureDayCanClose => IsFuture && _futureTradingDate != null && !_futureClosedDays.Contains(_futureTradingDate)
        && _futureDayEnds[_futureTradingDate] <= _algorithm.UtcTime;
    public bool FutureStrategyAllowed => !IsFuture || (!FutureObservationOnly && !FutureModels.RiskActive
        && (!_riskRecoveredAt.HasValue || _algorithm.UtcTime > _riskRecoveredAt.Value));
    private static DateTime FutureTime(JsonElement row, string name) => DateTimeOffset.Parse(row.GetProperty(name).GetString()).UtcDateTime;
    private static decimal FutureDecimal(JsonElement row, string name)
    {
        var value = row.GetProperty(name);
        return value.ValueKind == JsonValueKind.String
            ? decimal.Parse(value.GetString(), System.Globalization.CultureInfo.InvariantCulture) : value.GetDecimal();
    }
    private int FuturePriceScale(string code) => _futureContracts.GetProperty(code).GetProperty("price_scale").GetInt32();
    private static decimal RoundMoney(decimal amount) => decimal.Round(amount, 2, MidpointRounding.AwayFromZero);
    private static object Money(decimal amount) => new { units = checked((long)(RoundMoney(amount) * 100)), scale = 2, currency = "CNY" };

    private void InitializeFutures(JsonElement input, JsonElement settings)
    {
        if (_direct == null) throw new InvalidOperationException("期货行情必须通过已准入的逐交易日交接");
        _futureContracts = input.GetProperty("contracts").Clone();
        _futureSessions = input.GetProperty("sessions").EnumerateArray().Select(x => x.Clone())
            .OrderBy(x => FutureTime(x, "start_at")).ThenBy(x => x.GetProperty("instrument_id").GetString(), StringComparer.Ordinal).ToArray();
        foreach (var session in _futureSessions)
        {
            var code = session.GetProperty("instrument_id").GetString();
            var contract = _futureContracts.GetProperty(code);
            var day = DateOnly.Parse(session.GetProperty("trading_date").GetString());
            if (day < DateOnly.Parse(contract.GetProperty("listed_date").GetString())
                || day > DateOnly.Parse(contract.GetProperty("expiry_date").GetString()))
                throw new InvalidOperationException($"期货会话不在上市至最后交易日范围：{code} {day}");
        }
        _futureLoadSchedule = _futureSessions.GroupBy(x => x.GetProperty("trading_date").GetString())
            .Select(x => (Day: DateTime.Parse(x.Key), Start: x.Min(y => FutureTime(y, "start_at")))).OrderBy(x => x.Start).ToArray();
        _futureDayEnds = _futureSessions.GroupBy(x => x.GetProperty("trading_date").GetString())
            .ToDictionary(x => x.Key, x => x.Max(y => FutureTime(y, "end_at")));
        _lastFutureEnd = _futureDayEnds.Values.Max();
        _algorithm.SetStartDate(DateTimeOffset.Parse(input.GetProperty("start_at").GetString()).DateTime.Date);
        _algorithm.SetEndDate(DateTimeOffset.Parse(input.GetProperty("end_at").GetString()).DateTime.Date);
        _fillsOutput = new(Path.Combine(_output, "fills.jsonl"));
        _positionsOutput = new(Path.Combine(_output, "positions.jsonl"));
        _accountOutput = new(Path.Combine(_output, "account_events.jsonl"));
        _lifecycleOutput = new(Path.Combine(_output, "lifecycle.jsonl"));
        var markets = _futureContracts.EnumerateObject().Select(x => x.Value.GetProperty("market").GetString()).Distinct().Order().ToArray();
        var marketIndex = 810;
        foreach (var market in markets) if (!Market.Encode(market).HasValue) Market.Add(market, marketIndex++);
        foreach (var row in _futureContracts.EnumerateObject())
        {
            var code = row.Name; var contract = row.Value;
            var market = contract.GetProperty("market").GetString();
            var product = contract.GetProperty("product").GetString();
            var multiplier = FutureDecimal(contract, "multiplier");
            var tick = FutureDecimal(contract, "tick_size");
            if (multiplier <= 0 || tick <= 0) throw new InvalidOperationException("期货乘数及tick必须为正");
            // 引擎时段只提供订阅包络，交易资格使用冻结的历史会话小节。
            MarketHoursDatabase.FromDataFolder().SetEntry(market, product, SecurityType.Future,
                SecurityExchangeHours.AlwaysOpen(TimeZones.Shanghai));
            SymbolPropertiesDatabase.FromDataFolder().SetEntry(market, product, SecurityType.Future,
                new SymbolProperties(code, "CNY", multiplier, tick, 1, product, 1));
            var symbol = Symbol.CreateFuture(product, market, DateTime.Parse(contract.GetProperty("expiry_date").GetString()), code);
            var security = _algorithm.AddSecurity(symbol, IsDaily ? Resolution.Daily : Resolution.Minute,
                fillForward: false, extendedMarketHours: false, dataNormalizationMode: DataNormalizationMode.Raw);
            _symbols.Add(code, security.Symbol); _codes.Add(security.Symbol, code); _history.Add(code, new());
        }
        FutureModels = new(_algorithm.Portfolio, FutureRuleAt);
        var parameters = settings.GetProperty("parameters");
        FutureModels.ConfigureExecution(parameters.GetProperty("slippage_bps").GetDecimal(),
            parameters.GetProperty("slippage_ticks").GetInt32(), parameters.GetProperty("participation_cap_ppm").GetInt32());
        FutureModels.Install(_algorithm);
        InitializeFutureLifecycle(settings);
        FutureModels.BeforeExecution = (_, order) =>
        {
            AdvanceFutureClock();
            return !FutureObservationOnly && order.Status is not (OrderStatus.Canceled or OrderStatus.Invalid or OrderStatus.Filled)
                && IsFutureTradingTime(_codes[order.Symbol], _algorithm.UtcTime);
        };
        FutureModels.FillCommitted += FutureFillCommitted;
        FutureModels.ReservationChanged += (kind, order, amount) => {
            WriteFutureLifecycle(kind, new { order_id = order.Id, instrument_id = _codes[order.Symbol], amount });
            WriteFutureAccount(kind);
        };
    }

    private JsonElement SessionAt(string code, DateTime utc) =>
        _currentFutureSessions.TryGetValue(code, out var session) && FutureTime(session, "start_at") <= utc && utc <= FutureTime(session, "end_at")
            ? session : default;

    private bool IsFutureTradingTime(string code, DateTime utc)
    {
        var session = SessionAt(code, utc);
        return session.ValueKind != JsonValueKind.Undefined && session.GetProperty("segments").EnumerateArray()
            .Any(x => FutureTime(x, "start_at") <= utc && utc <= FutureTime(x, "end_at"));
    }

    private FutureRule FutureRuleAt(Symbol symbol, DateTime utc)
    {
        var code = _codes[symbol];
        var currentDay = ((ChinaFutureHolding)_algorithm.Securities[symbol].Holdings).TradingDate?.ToString("yyyy-MM-dd") ?? _futureTradingDate;
        var candidates = _futureHeaders.Values.SelectMany(x => x.GetProperty("rules").EnumerateArray())
            .Where(x => x.GetProperty("instrument_id").GetString() == code && FutureTime(x, "available_at") <= utc
                && string.CompareOrdinal(x.GetProperty("effective_from").GetString(), currentDay) <= 0
                && string.CompareOrdinal(x.GetProperty("effective_to").GetString(), currentDay) >= 0)
            .OrderBy(x => FutureTime(x, "available_at")).ToArray();
        if (candidates.Length == 0) throw new InvalidOperationException($"期货缺少当时可见有效规则：{code} {utc:O}");
        var row = candidates[^1]; var p = row.GetProperty("parameters");
        FutureFee Fee(string prefix) => new(p.GetProperty(prefix + "_fee_per_contract_units").GetDecimal() / 100,
            p.GetProperty(prefix + "_fee_rate_ppm").GetDecimal() / 1_000_000);
        return new(p.GetProperty("long_margin_ppm").GetDecimal() / 1_000_000,
            p.GetProperty("short_margin_ppm").GetDecimal() / 1_000_000,
            Fee("open"), Fee("close"), Fee("close_today"), FutureDecimal(_futureContracts.GetProperty(code), "tick_size"),
            FutureDecimal(p, "lower_limit"), FutureDecimal(p, "upper_limit"),
            p.GetProperty("close_priority").GetString(), row.GetProperty("source_ref").GetString());
    }

    public void AdvanceFutureClock()
    {
        if (!IsFuture || _advancingFutureClock) return;
        _advancingFutureClock = true;
        try
        {
            var now = _algorithm.UtcTime;
            while (_futureLoadIndex < _futureLoadSchedule.Length && _futureLoadSchedule[_futureLoadIndex].Start <= now)
            {
                var day = _futureLoadSchedule[_futureLoadIndex++].Day;
                _futureHeaders.Add(day, _direct.Header(day));
            }
            foreach (var settlement in _futureHeaders.Values.SelectMany(x => x.GetProperty("settlements").EnumerateArray())
                .Where(x => FutureTime(x, "available_at") <= now).OrderBy(x => FutureTime(x, "available_at"))
                .ThenBy(x => x.GetProperty("instrument_id").GetString(), StringComparer.Ordinal))
            {
                var code = settlement.GetProperty("instrument_id").GetString();
                var day = settlement.GetProperty("trading_date").GetString();
                if (!_futureSettled.Add((code, day))) continue;
                var security = _algorithm.Securities[_symbols[code]];
                var holding = (ChinaFutureHolding)security.Holdings;
                if (!holding.TradingDate.HasValue) holding.AdvanceTradingDate(DateOnly.Parse(day));
                var amount = FutureModels.Settle(security, DateOnly.Parse(day), FutureDecimal(settlement, "price"),
                    FutureTime(settlement, "available_at"), now);
                _futureMarks[code] = new DateTimeOffset(FutureTime(settlement, "available_at"));
                _futureMarkSources[code] = "published_settlement"; _settledThrough[code] = day;
                WriteFutureLifecycle("settlement", new { instrument_id = code, trading_date = day, amount,
                    price = FutureDecimal(settlement, "price"), source_ref = settlement.GetProperty("source_ref").GetString(),
                    available_at = settlement.GetProperty("available_at").GetString() });
                WriteFutureAccount("settlement", day);
            }
            while (_futureSessionIndex < _futureSessions.Length && FutureTime(_futureSessions[_futureSessionIndex], "start_at") <= now)
            {
                var session = _futureSessions[_futureSessionIndex++];
                var code = session.GetProperty("instrument_id").GetString();
                var day = session.GetProperty("trading_date").GetString();
                if (!_startedFutureSessions.Add((code, day))) continue;
                var holding = (ChinaFutureHolding)_algorithm.Securities[_symbols[code]].Holdings;
                if (holding.TradingDate.HasValue && holding.Invested && !_futureSettled.Contains((code, holding.TradingDate.Value.ToString("yyyy-MM-dd"))))
                    throw new InvalidOperationException($"下一交易日前持仓必要结算缺失：{code} {holding.TradingDate}");
                holding.AdvanceTradingDate(DateOnly.Parse(day));
                _currentFutureSessions[code] = session;
                if (_futureTradingDate == null || string.CompareOrdinal(day, _futureTradingDate) > 0) _futureTradingDate = day;
            }
            if (_futureTradingDate == null) return;
            var marked = false;
            foreach (var security in _algorithm.Securities.Values)
            {
                var last = security.GetLastData();
                if (last is TradeBar bar && bar.EndTime <= _algorithm.Time && bar is not DailyCloseMark)
                {
                    var code = _codes[security.Symbol];
                    var at = new DateTimeOffset(DateTime.SpecifyKind(bar.EndTime, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
                    var source = bar is DailyOpenBar ? "session_open" : "completed_minute_close";
                    if (!_futureMarks.TryGetValue(code, out var before) || at > before
                        || at == before && _futureMarkSources[code] == "published_settlement")
                    {
                        security.Holdings.UpdateMarketPrice(bar.Close);
                        _futureMarks[code] = at; _futureMarkSources[code] = source; marked = true;
                    }
                }
            }
            _algorithm.Portfolio.InvalidateTotalPortfolioValue();
            if (marked) WriteFutureAccount("mark");
            MaintainFutureRisk();
            MaintainFutureLifecycle();
            foreach (var old in _futureHeaders.Keys.Order().Take(Math.Max(0, _futureHeaders.Count - 2)).ToArray())
                _futureHeaders.Remove(old);
        }
        finally { _advancingFutureClock = false; }
    }

    private object[] FuturePositions(bool protocol = false)
    {
        var rows = new List<object>();
        foreach (var pair in _symbols.OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            var security = _algorithm.Securities[pair.Value]; var holding = (ChinaFutureHolding)security.Holdings;
            foreach (var item in holding.Positions())
            {
                var reserved = FutureModels.ReservedClose(pair.Value, item.Direction, item.Bucket);
                var scale = FuturePriceScale(pair.Key);
                if (protocol) rows.Add(new { instrument = _instruments[pair.Key], direction = item.Direction, bucket = item.Bucket,
                    quantity = item.Quantity, sellable_quantity = item.Quantity - reserved, reserved_sell_quantity = reserved,
                    mark_price = new { units = (long)(item.MarkPrice * (decimal)Math.Pow(10, scale)), scale, currency = "CNY" },
                    mark_available_at = _futureMarks.GetValueOrDefault(pair.Key, Now) });
                else rows.Add(new { account_id = FutureAccountId, event_sequence = _futureSequence, instrument_id = pair.Key,
                    direction = item.Direction, bucket = item.Bucket, quantity = item.Quantity, cost = item.Cost, base_price = item.BasePrice,
                    price = item.MarkPrice, price_scale = scale, multiplier = security.SymbolProperties.ContractMultiplier,
                    unrealized_pnl = item.UnrealizedPnl, reserved_quantity = reserved, valuation_at = _futureMarks.GetValueOrDefault(pair.Key, Now),
                    mark_source = _futureMarkSources.GetValueOrDefault(pair.Key), trading_date = holding.TradingDate?.ToString("yyyy-MM-dd") });
            }
        }
        return rows.ToArray();
    }

    private string FutureSnapshot(string codesJson)
    {
        AdvanceFutureClock();
        var needed = JsonSerializer.Deserialize<string[]>(codesJson).ToHashSet();
        foreach (var pair in _symbols.Where(x => _algorithm.Securities[x.Value].Holdings.Invested)) needed.Add(pair.Key);
        var quotes = new Dictionary<string, object>(); var missing = new List<string>();
        foreach (var code in needed.Order())
        {
            RememberCurrent(code); var visible = _history[code].LastOrDefault();
            if (visible == null || visible.AvailableAt > _algorithm.Time) missing.Add(code);
            else quotes.Add(code, Quote(code, visible));
        }
        var cash = RoundMoney(_algorithm.Portfolio.CashBook["CNY"].Amount);
        var equity = RoundMoney(_algorithm.Portfolio.TotalPortfolioValue);
        var margin = RoundMoney(_algorithm.Portfolio.TotalMarginUsed); var frozen = RoundMoney(FutureModels.Frozen);
        var available = equity - margin - frozen;
        var orders = _algorithm.Transactions.GetOpenOrders().OrderBy(x => x.Id).Select(x => new {
            order_id = x.Id.ToString(), instrument = _instruments[_codes[x.Symbol]], side = x.Quantity > 0 ? "buy" : "sell",
            quantity = x.AbsoluteQuantity, filled_quantity = _filled.GetValueOrDefault(x.Id),
            position_effect = ((ChinaFutureOrderProperties)x.Properties).PositionEffect,
            status = _filled.GetValueOrDefault(x.Id) > 0 ? "partially_filled" : "accepted", submitted_at = new DateTimeOffset(x.Time, TimeSpan.Zero),
            reserved_cash = Money(FutureModels.OrderFrozen(x.Id)) }).ToArray();
        return JsonSerializer.Serialize(new { order_event_count = _orderEventCount, event_sequence = _futureSequence,
            account = new { asset_class = "cn_future", account_seq = _futureSequence, as_of = Now,
                total_cash = Money(cash), available_cash = Money(available), frozen_cash = Money(frozen), withdrawable_cash = Money(Math.Max(0, available)),
                net_asset = Money(equity), settled_cash = Money(cash), unrealized_pnl = Money(equity - cash), equity = Money(equity),
                margin = Money(margin), frozen_funds = Money(frozen), available_funds = Money(available),
                positions = FuturePositions(true), active_orders = orders, risk_active = FutureModels.RiskActive },
            quotes, missing_instruments = missing }, JsonOptions);
    }

    private void WriteFutureLifecycle(string kind, object detail)
    {
        _lifecycleOutput.WriteLine(JsonSerializer.Serialize(new { account_id = FutureAccountId, event_sequence = ++_futureSequence,
            at = Now, available_at = Now, trading_date = _futureTradingDate, kind, detail }, JsonOptions));
        _lifecycleOutput.Flush();
    }

    private void WriteFutureAccount(string reason, string tradingDate = null)
    {
        _futureSequence++;
        _accountOutput.WriteLine(JsonSerializer.Serialize(FutureAccount(reason, tradingDate), JsonOptions));
        _accountOutput.Flush();
        foreach (var position in FuturePositions()) _positionsOutput.WriteLine(JsonSerializer.Serialize(position, JsonOptions));
        _positionsOutput.Flush();
    }

    private object FutureAccount(string reason, string tradingDate = null) => new {
        account_id = FutureAccountId, asset_class = "cn_future", event_sequence = _futureSequence, at = Now, available_at = Now,
        trading_date = tradingDate ?? _futureTradingDate, reason, cash = _algorithm.Portfolio.CashBook["CNY"].Amount,
        unrealized_pnl = _algorithm.Portfolio.TotalPortfolioValue - _algorithm.Portfolio.CashBook["CNY"].Amount,
        equity = _algorithm.Portfolio.TotalPortfolioValue, margin = _algorithm.Portfolio.TotalMarginUsed,
        frozen_funds = FutureModels.Frozen, available_funds = FutureModels.Available, risk_active = FutureModels.RiskActive,
        settled_through = _settledThrough, valuation_at = Now };

    private void FutureFillCommitted(Order order, OrderEvent fill, IReadOnlyList<FutureFillLeg> legs)
    {
        var code = _codes[order.Symbol]; var security = _algorithm.Securities[order.Symbol];
        var props = (ChinaFutureOrderProperties)order.Properties;
        _futureSequence++;
        var index = 0;
        foreach (var leg in legs)
            _fillsOutput.WriteLine(JsonSerializer.Serialize(new { account_id = FutureAccountId, event_sequence = _futureSequence,
                order_id = order.Id, fill_id = $"{order.Id}:{fill.Id}", leg_id = $"{order.Id}:{fill.Id}:{index++}", instrument_id = code,
                at = Now, available_at = Now, trading_date = ((ChinaFutureHolding)security.Holdings).TradingDate?.ToString("yyyy-MM-dd"),
                direction = leg.Direction, bucket = leg.Bucket, quantity = leg.Quantity, price = leg.Price, price_scale = FuturePriceScale(code),
                multiplier = security.SymbolProperties.ContractMultiplier, fee = leg.Fee, realized_pnl = leg.RealizedPnl,
                position_effect = props.PositionEffect, rule_reference = leg.RuleReference, risk_order = props.RiskOrder, roll_plan_id = props.RollPlanId }, JsonOptions));
        _fillsOutput.Flush();
        RecordFutureLifecycleFill(order, legs);
        WriteFutureAccount("fill");
    }

    private void FutureOrderEvent(OrderEvent value)
    {
        if (!_futureOrderEvents.Add((value.OrderId, value.Id))) return;
        FutureModels.OnOrderEvent(value);
        var order = _algorithm.Transactions.GetOrderById(value.OrderId);
        var props = (ChinaFutureOrderProperties)order.Properties;
        _filled[value.OrderId] = _filled.GetValueOrDefault(value.OrderId) + Math.Abs(value.FillQuantity);
        _orders.WriteLine(JsonSerializer.Serialize(new { account_id = FutureAccountId, event_sequence = ++_futureSequence,
            order_event_count = ++_orderEventCount, order_id = value.OrderId, event_id = value.Id, instrument_id = _codes[value.Symbol],
            status = value.Status.ToString(), submitted_at = new DateTimeOffset(order.Time, TimeSpan.Zero), at = Now, trading_date = _futureTradingDate,
            quantity = value.FillQuantity, price = value.FillPrice, fee = value.OrderFee.Value.Amount, message = value.Message,
            position_effect = props.PositionEffect, time_in_force = props.Duration, remaining_quantity = order.AbsoluteQuantity - _filled[value.OrderId],
            direction = ChinaFutureHolding.Direction(order.Quantity, props.PositionEffect), risk_order = props.RiskOrder, roll_plan_id = props.RollPlanId,
            cash = _algorithm.Portfolio.CashBook["CNY"].Amount }, JsonOptions));
        _orders.Flush();
        if (value.Status is OrderStatus.Canceled or OrderStatus.Invalid) WriteFutureAccount("order_release");
        if (!_advancingFutureClock) { MaintainFutureRisk(); MaintainFutureLifecycle(); }
    }

    private void ValidateFutureIntent(string json)
    {
        using var document = JsonDocument.Parse(json); var intent = document.RootElement;
        if (intent.GetProperty("kind").GetString() == "cancel") return;
        if (FutureObservationOnly) throw new InvalidOperationException("结算观察阶段不接受策略交易");
        if (intent.GetProperty("kind").GetString() != "quantity"
            || intent.GetProperty("position_effect").GetString() is not ("open" or "close" or "close_today" or "close_yesterday")
            || intent.GetProperty("order_type").GetString() is not ("market" or "limit")
            || intent.GetProperty("time_in_force").GetString() is not ("DAY" or "IOC"))
            throw new InvalidOperationException("期货必须为数量单、显式开平及market/limit、DAY/IOC");
        var code = intent.GetProperty("instrument").GetProperty("instrument_id").GetString();
        if (!_symbols.ContainsKey(code) || !IsFutureTradingTime(code, _algorithm.UtcTime))
            throw new InvalidOperationException("期货合约不在当前可交易会话");
        if (IsDaily && _dailyDecisionTime != _algorithm.Time) throw new InvalidOperationException("日线委托仅在独立开盘决策阶段提交");
        var quantity = intent.GetProperty("quantity").GetDecimal();
        if (quantity <= 0 || quantity != decimal.Truncate(quantity)) throw new InvalidOperationException("期货手数必须为正整数");
    }

    private void SubmitFutureIntent(string json)
    {
        AdvanceFutureClock(); ValidateFutureIntent(json);
        using var document = JsonDocument.Parse(json); var intent = document.RootElement;
        if (intent.GetProperty("kind").GetString() == "cancel")
        {
            var id = int.Parse(intent.GetProperty("order_id").GetString());
            _algorithm.Transactions.GetOrderTicket(id).Cancel("策略主动撤单");
            _intents.WriteLine(JsonSerializer.Serialize(new { at = Now, event_sequence = ++_futureSequence, intent = intent.Clone(), order_id = id }));
        }
        else
        {
            var code = intent.GetProperty("instrument").GetProperty("instrument_id").GetString();
            var quantity = intent.GetProperty("quantity").GetDecimal() * (intent.GetProperty("side").GetString() == "buy" ? 1 : -1);
            var requested = quantity;
            decimal limit = 0;
            if (intent.GetProperty("order_type").GetString() == "limit")
            {
                var value = intent.GetProperty("limit_price");
                limit = value.GetProperty("units").GetDecimal() / (decimal)Math.Pow(10, value.GetProperty("scale").GetInt32());
            }
            decimal? protection = null;
            if (intent.TryGetProperty("protection_price", out var protectedValue) && protectedValue.ValueKind != JsonValueKind.Null)
                protection = protectedValue.GetProperty("units").GetDecimal() / (decimal)Math.Pow(10, protectedValue.GetProperty("scale").GetInt32());
            var effect = intent.GetProperty("position_effect").GetString();
            if (effect == "open" && intent.TryGetProperty("funds_policy", out var policy) && policy.GetString() == "resize")
                quantity = FutureModels.AffordableOpeningQuantity(_algorithm.Securities[_symbols[code]], quantity, limit, _algorithm.UtcTime);
            var ticket = SubmitFutureOrder(code, quantity, effect,
                intent.GetProperty("intent_id").GetString(), limit, intent.GetProperty("time_in_force").GetString(), protection: protection);
            _intents.WriteLine(JsonSerializer.Serialize(new { at = Now, event_sequence = ++_futureSequence,
                intent = intent.Clone(), order_id = ticket.OrderId, requested_quantity = requested, submitted_quantity = quantity }));
        }
        _intents.Flush();
    }

    private OrderTicket SubmitFutureOrder(string code, decimal quantity, string effect, string tag,
        decimal limit = 0, string duration = "DAY", bool risk = false, string roll = null, decimal? protection = null)
    {
        var session = SessionAt(code, _algorithm.UtcTime);
        if (session.ValueKind == JsonValueKind.Undefined) throw new InvalidOperationException("期货委托缺少当时会话");
        var rule = FutureRuleAt(_symbols[code], _algorithm.UtcTime);
        var properties = new ChinaFutureOrderProperties { PositionEffect = effect, ClosePriority = rule.ClosePriority,
            Duration = duration, ValidUntilUtc = FutureTime(session, "end_at"), TimeInForce = new ChinaFutureTimeInForce(),
            DailyOpenDecision = IsDaily && (!risk || !_futureExecutedOpens.Contains((code, _algorithm.UtcTime))),
            RiskOrder = risk, RollPlanId = roll, ProtectionPrice = protection };
        return _algorithm.SubmitOrderRequest(new SubmitOrderRequest(limit > 0 ? OrderType.Limit : OrderType.Market,
            SecurityType.Future, _symbols[code], quantity, 0, limit, _algorithm.UtcTime, tag, properties));
    }

    public void FinishFutureOpportunity()
    {
        foreach (var id in FutureModels.IocAttempted.ToArray())
            _algorithm.Transactions.GetOrderTicket(id)?.Cancel("IOC首次合格机会结束");
        foreach (var order in _algorithm.Transactions.GetOpenOrders())
            if (_algorithm.UtcTime >= ((ChinaFutureOrderProperties)order.Properties).ValidUntilUtc)
                _algorithm.Transactions.GetOrderTicket(order.Id).Cancel("DAY交易日结束");
        MaintainFutureRisk();
    }

    private void MaintainFutureRisk()
    {
        if (_futureTradingDate == null || _maintainingFutureRisk) return;
        _maintainingFutureRisk = true;
        try { MaintainFutureRiskCore(); }
        finally { _maintainingFutureRisk = false; }
    }

    private void MaintainFutureRiskCore()
    {
        if (FutureModels.Available < 0 && !FutureModels.RiskActive)
        {
            FutureModels.RiskActive = true;
            WriteFutureLifecycle("risk_enter", new { available_funds = FutureModels.Available });
        }
        if (!FutureModels.RiskActive) return;
        var active = _algorithm.Transactions.GetOpenOrders();
        foreach (var order in active.Where(x => !((ChinaFutureOrderProperties)x.Properties).RiskOrder))
            _algorithm.Transactions.GetOrderTicket(order.Id).Cancel("风险阶段取消策略订单");
        if (_algorithm.Transactions.GetOpenOrders().Any(x => !((ChinaFutureOrderProperties)x.Properties).RiskOrder)) return;
        if (FutureModels.Available >= 0)
        {
            foreach (var order in _algorithm.Transactions.GetOpenOrders())
                _algorithm.Transactions.GetOrderTicket(order.Id).Cancel("风险缺口解除");
            if (_algorithm.Transactions.GetOpenOrders().Count != 0) return;
            FutureModels.RiskActive = false; _riskRecoveredAt = _algorithm.UtcTime;
            WriteFutureLifecycle("risk_recovered", new { available_funds = FutureModels.Available });
            return;
        }
        if (FutureObservationOnly || _riskLastAttemptAt == _algorithm.UtcTime) return;
        var candidates = _symbols.Where(x => IsFutureTradingTime(x.Key, _algorithm.UtcTime)).SelectMany(pair =>
        {
            var security = _algorithm.Securities[pair.Value]; var rule = FutureRuleAt(pair.Value, _algorithm.UtcTime);
            return ((ChinaFutureHolding)security.Holdings).Positions().Select(position => new { code = pair.Key, symbol = pair.Value, position,
                available = position.Quantity - FutureModels.ReservedClose(pair.Value, position.Direction, position.Bucket),
                capacity = FutureModels.RiskCloseCapacity(security, position.Quantity * (position.Direction == "long" ? -1 : 1)),
                fee = (position.Bucket == "today" ? rule.CloseTodayFee : rule.CloseFee)
                    .Quote(1, position.MarkPrice, security.SymbolProperties.ContractMultiplier),
                release = position.MarkPrice * security.SymbolProperties.ContractMultiplier
                    * (position.Direction == "long" ? rule.LongMarginRate : rule.ShortMarginRate) });
        }).OrderByDescending(x => x.release).ThenBy(x => x.code, StringComparer.Ordinal)
            .ThenBy(x => x.position.Direction, StringComparer.Ordinal).ThenBy(x => x.position.Bucket == "yesterday" ? 0 : 1).ToArray();
        var chosen = candidates.FirstOrDefault(x => x.available > 0 && x.capacity > 0)
            // 日线已知规则或结算可先触发风险委托，不能等到拿到本次Open才允许挂单。
            ?? (IsDaily ? candidates.FirstOrDefault(x => x.available > 0 && !x.capacity.HasValue) : null);
        var pending = _algorithm.Transactions.GetOpenOrders();
        if (pending.Count != 0)
        {
            // 受阻风险单先撤销并等待平仓预占释放，下一项才能使用真实剩余缺口。
            if (chosen != null && chosen.capacity > 0)
                foreach (var order in pending.Where(x => x.Time < _algorithm.UtcTime
                    && FutureModels.RiskCloseCapacity(_algorithm.Securities[x.Symbol], x.Quantity).GetValueOrDefault() == 0))
                    _algorithm.Transactions.GetOrderTicket(order.Id).Cancel("风险减仓改用当前可交易持仓");
            return;
        }
        if (chosen == null) return;
        _riskLastAttemptAt = _algorithm.UtcTime;
        var netRelease = chosen.release - chosen.fee;
        var quantity = netRelease > 0
            ? Math.Min(chosen.available, Math.Max(1, decimal.Ceiling(-FutureModels.Available / netRelease)))
            : chosen.available;
        SubmitFutureOrder(chosen.code, quantity * (chosen.position.Direction == "long" ? -1 : 1),
            "close_" + chosen.position.Bucket, "risk", risk: true);
    }

    private void CloseFutureSession()
    {
        if (!FutureDayCanClose) return;
        _futureClosedDays.Add(_futureTradingDate);
        WriteFutureAccount("session_close");
        var elapsed = _clock.Elapsed.TotalSeconds;
        _daily.WriteLine(JsonSerializer.Serialize(new { account_id = FutureAccountId, asset_class = "cn_future", date = _futureTradingDate,
            trading_date = _futureTradingDate, event_sequence = _futureSequence, rows = _dayRows,
            cash = _algorithm.Portfolio.CashBook["CNY"].Amount, equity = _algorithm.Portfolio.TotalPortfolioValue,
            unrealized_pnl = _algorithm.Portfolio.TotalPortfolioValue - _algorithm.Portfolio.CashBook["CNY"].Amount,
            margin = _algorithm.Portfolio.TotalMarginUsed, frozen_funds = FutureModels.Frozen, available_funds = FutureModels.Available,
            equity_semantics = "latest_visible_mark_not_session_close", valuation_at = Now, settled_through = _settledThrough,
            positions = FuturePositions(), active_orders = _algorithm.Transactions.GetOpenOrders().Count,
            day_wall_seconds = elapsed - _previousClose }, JsonOptions));
        _daily.Flush(); _dayRows = 0; _previousClose = elapsed;
    }

    private void FinishFutures()
    {
        if (HasRuntimeError || _futureClosedDays.Count != _sessions.Count || _algorithm.Transactions.GetOpenOrders().Count != 0)
            throw new InvalidOperationException("期货交易日、订单或引擎终态未完成");
        AdvanceFutureClock();
        foreach (var pair in _symbols)
        {
            var holding = (ChinaFutureHolding)_algorithm.Securities[pair.Value].Holdings;
            if (holding.Invested && (!_settledThrough.TryGetValue(pair.Key, out var day)
                || day != holding.TradingDate?.ToString("yyyy-MM-dd")))
                throw new InvalidOperationException($"尾部持仓缺少正式结算：{pair.Key}");
            if (holding.Invested && _algorithm.UtcTime >= FutureTime(_futureContracts.GetProperty(pair.Key), "exit_deadline"))
                throw new InvalidOperationException($"到期退出截止仍有持仓：{pair.Key}");
        }
        if (_algorithm.Portfolio.TotalPortfolioValue < 0 || FutureModels.Available < 0 || FutureModels.RiskActive)
            throw new InvalidOperationException("尾部期货风险缺口未解除");
        WriteFutureAccount("terminal");
        File.WriteAllText(Path.Combine(_output, "account.json"), JsonSerializer.Serialize(new {
            account_id = FutureAccountId, asset_class = "cn_future", cash = _algorithm.Portfolio.CashBook["CNY"].Amount,
            equity = _algorithm.Portfolio.TotalPortfolioValue, unrealized_pnl = _algorithm.Portfolio.TotalPortfolioValue - _algorithm.Portfolio.CashBook["CNY"].Amount,
            margin = _algorithm.Portfolio.TotalMarginUsed, frozen_funds = FutureModels.Frozen, available_funds = FutureModels.Available,
            positions = FuturePositions(), settled_through = _settledThrough, valuation_at = Now, trading_date = _futureTradingDate,
            equity_semantics = "latest_visible_mark_not_session_close", rows = _rows,
            algorithm_wall_seconds = _clock.Elapsed.TotalSeconds }, JsonOptions));
        _orders.Dispose(); _intents.Dispose(); _daily.Dispose(); _fillsOutput.Dispose(); _positionsOutput.Dispose();
        _accountOutput.Dispose(); _lifecycleOutput.Dispose();
    }
}
