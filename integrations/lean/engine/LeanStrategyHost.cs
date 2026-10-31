using System.Diagnostics;
using System.Text.Json;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Orders.Slippage;
using QuantWitness.Lean.China;
using QuantWitness.Lean.CorporateActions;
using QuantWitness.LeanInput;

namespace QuantWitness.Lean.Research;

// 只翻译原策略需要的账户、报价和委托，交易计算仍由LEAN及中国模型处理。
public sealed class LeanStrategyHost
{
    private readonly QCAlgorithm _algorithm;
    private readonly Dictionary<string, Symbol> _symbols = new();
    private readonly Dictionary<Symbol, string> _codes = new();
    private readonly Dictionary<string, JsonElement> _instruments = new();
    private readonly HashSet<Symbol> _touched = new();
    private readonly Dictionary<int, decimal> _spent = new();
    private readonly Dictionary<int, decimal> _filled = new();
    private readonly Dictionary<DateTime, int> _sessions = new();
    private readonly HashSet<DateTime> _closedSessions = new();
    private readonly StreamWriter _orders;
    private readonly StreamWriter _intents;
    private readonly StreamWriter _daily;
    private readonly string _output;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private long _rows;
    private long _dayRows;
    private long _orderEventCount;
    private double _previousClose;
    private sealed record VisibleBar(TradeBar Bar, DateTime AvailableAt);
    private readonly Dictionary<string, Queue<VisibleBar>> _history = new();
    private readonly int _lookback;
    private readonly int _priceScale;
    private readonly decimal _priceDivisor;
    private DateTime? _dailyDecisionTime;
    private readonly DirectDailyStreamStore _direct;
    public bool IsDaily { get; }
    public ChinaAStockModels Models { get; }
    public CorporateActionProcessor Actions { get; }
    public Symbol ClockSymbol => _symbols.Values.First();
    public string EngineStatus => _algorithm.Status.ToString();
    public bool HasRuntimeError => _algorithm.RunTimeError != null;
    public string NowText => Now.ToString("yyyy-MM-ddTHH:mm:sszzz");
    private DateTimeOffset Now => new(DateTime.SpecifyKind(_algorithm.Time, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    public LeanStrategyHost(QCAlgorithm algorithm, string directory)
    {
        _algorithm = algorithm;
        _output = directory;
        using var data = JsonDocument.Parse(File.ReadAllText(Path.Combine(directory, "input.json")));
        using var strategy = JsonDocument.Parse(File.ReadAllText(Path.Combine(directory, "strategy-input.json")));
        var settings = strategy.RootElement;
        _priceScale = data.RootElement.GetProperty("price_scale").GetInt32();
        if (_priceScale is not (2 or 3)) throw new ArgumentException("现货价格精度仅支持二位或三位");
        _priceDivisor = (decimal)Math.Pow(10, _priceScale);
        var direct = data.RootElement.TryGetProperty("source_kind", out var sourceKind) && sourceKind.GetString() == "direct_daily_stream"
            ? DirectDailyStreamStore.Open(Path.Combine(directory, "input.json")) : null;
        _direct = direct;
        IsDaily = direct?.Frequency == "1d";
        var binding = settings.GetProperty("declaration").GetProperty("input_bindings").EnumerateArray()
            .Single(x => x.GetProperty("source").GetString() == "bars");
        _lookback = binding.GetProperty("lookback_bars").GetInt32();
        if (_lookback <= 0) throw new ArgumentException("历史窗口必须为正整数");
        foreach (var entry in settings.GetProperty("instruments").EnumerateObject()) _instruments.Add(entry.Name, entry.Value.Clone());
        var dates = data.RootElement.GetProperty("dates").EnumerateArray().Select(x => DateTime.Parse(x.GetString())).ToArray();
        for (var i = 0; i < dates.Length; i++) _sessions.Add(dates[i], i);
        algorithm.SetStartDate(dates[0]); algorithm.SetEndDate(dates[^1]);
        algorithm.SetTimeZone("Asia/Shanghai");
        var initialCash = settings.GetProperty("initial_cash_units").GetInt64();
        if (initialCash < 0) throw new ArgumentException("初始现金不能为负");
        algorithm.SetAccountCurrency("CNY", initialCash / 100m);
        algorithm.SetBenchmark(t => 1m);
        algorithm.SetRiskFreeInterestRateModel(new ConstantRiskFreeRateInterestRateModel(0m));
        algorithm.SetAvailableDataTypes(new Dictionary<SecurityType, List<TickType>> { [SecurityType.Equity] = new() { TickType.Trade } });
        _orders = new StreamWriter(Path.Combine(directory, "orders.jsonl"));
        _intents = new StreamWriter(Path.Combine(directory, "intents.jsonl"));
        _daily = new StreamWriter(Path.Combine(directory, "daily.jsonl"));
        Market.Add("jqchina", 999);
        foreach (var code in data.RootElement.GetProperty("symbols").EnumerateArray().Select(x => x.GetString()))
        {
            var security = algorithm.AddEquity(code.Split('.')[0], IsDaily ? Resolution.Daily : Resolution.Minute, "jqchina", fillForward: false,
                leverage: 1m, extendedMarketHours: false, dataNormalizationMode: DataNormalizationMode.Raw);
            security.SetSlippageModel(new ConstantSlippageModel(0m));
            _symbols.Add(code, security.Symbol); _codes.Add(security.Symbol, code);
            _history.Add(code, new());
        }
        var actions = CorporateActionJson.Load(Path.Combine(directory, "actions.json"));
        Actions = new CorporateActionProcessor(algorithm.Portfolio,
            ticker => _symbols.First(x => x.Value.Value == ticker || x.Key == ticker).Value, actions);
        Actions.MarketPriceSourceTime = symbol =>
            (_algorithm.Securities[symbol].GetLastData() as DailyCloseMark)?.SourceTime;
        var start = new DateTimeOffset(dates[0].AddHours(9.5), TimeSpan.FromHours(8));
        foreach (var action in actions.Where(x => x.RecordAt < start))
            Actions.SeedEntitlement(new InitialEntitlement(action.ActionId, action.Revision, 0m, action.RecordAt,
                "研究窗口初始空仓，不继承历史回测登记持仓"), start);
        ChinaRuleBook book;
        if (direct != null)
            book = new ChinaRuleBook(dates.Select(DateOnly.FromDateTime),
                day => direct.Rules(day.ToDateTime(TimeOnly.MinValue)).Select(ChinaRule.Parse));
        else
        {
            var ruleRows = new List<ChinaRule>();
            foreach(var path in settings.GetProperty("rule_files").EnumerateObject())
            {
                using var ruleDocument=JsonDocument.Parse(File.ReadAllText(Path.IsPathFullyQualified(path.Value.GetString())
                    ? path.Value.GetString() : Path.Combine(directory, path.Value.GetString())));
                foreach(var row in ruleDocument.RootElement.GetProperty("rules").EnumerateArray()) ruleRows.Add(ChinaRule.Parse(row));
            }
            book = new ChinaRuleBook(dates.Select(DateOnly.FromDateTime), ruleRows);
        }
        Models = new ChinaAStockModels(book,
            symbol => _codes[symbol], symbol => Actions.LockedBonusQuantity(symbol),
            symbol => _instruments[_codes[symbol]].GetProperty("asset_class").GetString());
        var parameters = settings.GetProperty("parameters");
        Models.ConfigureExecution(parameters.GetProperty("slippage_bps").GetDecimal(),
            parameters.GetProperty("slippage_ticks").GetInt32(), parameters.GetProperty("participation_cap_ppm").GetInt32());
        Models.Install(algorithm);
        Models.BeforeDailyOpen = (utc, id) =>
        {
            Actions.Advance(new DateTimeOffset(utc));
            return _algorithm.Transactions.GetOpenOrders().Any(order => order.Id == id);
        };
    }

    public void OpenSession() => Actions.Advance(Now);
    public bool HasDirectInputs => _direct != null;
    public string DailySignals(string date)
    {
        var day = DateTime.ParseExact(date, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        if (_direct == null || day != _algorithm.Time.Date)
            throw new InvalidOperationException("只能请求当前模拟交易日的直接信号");
        return _direct.Signals(day);
    }
    public void CountBars(int count) { _rows += count; _dayRows += count; }

    public int ObserveBars(Slice data)
    {
        var count = 0;
        if (IsDaily)
        {
            foreach (var item in data.AllData.OfType<CompletedDailyData>())
            {
                Remember(_codes[item.Symbol], item.SourceBar, item.Time);
                count++;
            }
            if (count > 0) _dailyDecisionTime = _algorithm.Time;
            CountBars(count);
            return count;
        }
        foreach (var pair in data.Bars)
        {
            var bar = pair.Value;
            if (bar is DailyOpenBar) continue;
            if (bar.IsFillForward || bar.EndTime > _algorithm.Time)
                throw new InvalidOperationException("未完成行情不能进入历史窗口");
            Remember(_codes[pair.Key], bar, bar.EndTime);
            count++;
        }
        CountBars(count);
        return count;
    }

    private void Remember(string code, TradeBar bar, DateTime availableAt)
    {
        if (bar == null || bar is DailyOpenBar || bar.IsFillForward || availableAt > _algorithm.Time
            || (IsDaily ? bar.Period <= TimeSpan.Zero : bar.Period != TimeSpan.FromMinutes(1))) return;
        var rows = _history[code];
        if (rows.Count > 0 && rows.Last().Bar.EndTime >= bar.EndTime) return;
        rows.Enqueue(new(bar, availableAt));
        while (rows.Count > _lookback) rows.Dequeue();
    }

    private void RememberCurrent(string code)
    {
        var security = _algorithm.Securities[_symbols[code]];
        if (IsDaily)
        {
            var completed = security.Cache.GetData<CompletedDailyData>();
            if (completed != null) Remember(code, completed.SourceBar, completed.Time);
        }
        else
        {
            var bar = security.Cache.GetData<TradeBar>();
            if (bar != null) Remember(code, bar, bar.EndTime);
        }
    }

    private object Quote(string code, VisibleBar visible)
    {
        var bar = visible.Bar;
        long Units(decimal price)
        {
            if (price * _priceDivisor != decimal.Truncate(price * _priceDivisor)) throw new InvalidOperationException("报价超过声明价格精度");
            return checked((long)(price * _priceDivisor));
        }
        var at = new DateTimeOffset(DateTime.SpecifyKind(visible.AvailableAt, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
        var identity = $"lean:{code}:{at:O}";
        return new { instrument = _instruments[code], available_at = at,
            price = new { units = Units(bar.Close), scale = _priceScale, currency = "CNY" }, input_id = identity, price_input_id = identity,
            bar_start = new DateTimeOffset(DateTime.SpecifyKind(bar.Time, DateTimeKind.Unspecified), TimeSpan.FromHours(8)),
            bar_end = new DateTimeOffset(DateTime.SpecifyKind(bar.EndTime, DateTimeKind.Unspecified), TimeSpan.FromHours(8)),
            values = new Dictionary<string, long> { ["open"] = Units(bar.Open), ["high"] = Units(bar.High),
                ["low"] = Units(bar.Low), ["close"] = Units(bar.Close), ["volume"] = checked((long)bar.Volume) } };
    }

    public string History(string code, int count)
    {
        if (count <= 0 || count > _lookback) throw new ArgumentException("历史请求超出声明窗口");
        if (!_history.TryGetValue(code, out var rows)) throw new ArgumentException("证券未声明");
        RememberCurrent(code);
        return JsonSerializer.Serialize(rows.Where(x => x.AvailableAt <= _algorithm.Time).TakeLast(count).Select(x => Quote(code, x)), JsonOptions);
    }

    public void OnOrderEvent(OrderEvent e)
    {
        Models.OnOrderEvent(e);
        var order = _algorithm.Transactions.GetOrderById(e.OrderId);
        if (e.FillQuantity != 0)
        {
            _touched.Add(e.Symbol);
            _filled[e.OrderId] = _filled.GetValueOrDefault(e.OrderId) + Math.Abs(e.FillQuantity);
            _spent[e.OrderId] = _spent.GetValueOrDefault(e.OrderId) + e.FillQuantity * e.FillPrice + e.OrderFee.Value.Amount;
        }
        _orders.WriteLine(JsonSerializer.Serialize(new {
            order_event_count=++_orderEventCount,
            order_id=e.OrderId, event_id=e.Id, instrument_id=_codes[e.Symbol], status=e.Status.ToString(),
            submitted_at=new DateTimeOffset(DateTime.SpecifyKind(order.Time,DateTimeKind.Utc)), at=Now,
            quantity=e.FillQuantity, price=e.FillPrice, fee=e.OrderFee.Value.Amount, message=e.Message,
            cash=_algorithm.Portfolio.CashBook["CNY"].Amount,
            holding=_algorithm.Portfolio[e.Symbol].Quantity,
            budget_cny=(order.Properties as ChinaOrderProperties)?.BudgetCny
        }, JsonOptions));
        _orders.Flush();
    }

    private decimal ReservedCash() => _algorithm.Transactions.GetOpenOrders()
        .Where(x => x.Direction == OrderDirection.Buy)
        .Sum(x => Math.Max(0m, ((x.Properties as ChinaOrderProperties)?.BudgetCny ?? 0m) - _spent.GetValueOrDefault(x.Id)));

    public string Snapshot(string codesJson)
    {
        var needed = JsonSerializer.Deserialize<string[]>(codesJson).ToHashSet();
        foreach (var symbol in Actions.ValuationSymbols) needed.Add(_codes[symbol]);
        var positions = new List<object>();
        foreach (var symbol in _touched.OrderBy(x => _codes[x], StringComparer.Ordinal))
        {
            var holding = _algorithm.Portfolio[symbol];
            if (holding.Quantity == 0) continue;
            var code = _codes[symbol]; needed.Add(code);
            positions.Add(new { instrument=_instruments[code], quantity=holding.Quantity,
                sellable_quantity=Models.SellableQuantity(_algorithm.Securities[symbol],_algorithm.UtcTime) });
        }
        var active = _algorithm.Transactions.GetOpenOrders().OrderBy(x => x.Id).Select(order => new {
            instrument=_instruments[_codes[order.Symbol]], order_id=order.Id.ToString(),
            side=order.Direction == OrderDirection.Buy ? "buy" : "sell", quantity=Math.Abs(order.Quantity),
            filled_quantity=_filled.GetValueOrDefault(order.Id)
        }).ToArray();
        var quotes = new Dictionary<string, object>();
        var missing = new List<string>();
        foreach (var code in needed.Order())
        {
            var symbol = _symbols[code];
            RememberCurrent(code);
            var bar = _history[code].LastOrDefault();
            if (bar == null || bar.AvailableAt > _algorithm.Time)
            { missing.Add(code); continue; }
            quotes.Add(code, Quote(code, bar));
        }
        var cash = _algorithm.Portfolio.CashBook["CNY"].Amount;
        var entitlements = Actions.GetValuation(Now);
        return JsonSerializer.Serialize(new { order_event_count=_orderEventCount, account=new {positions,active_orders=active,
            corporate_entitlements=new {cash_receivable=entitlements.CashReceivable,
                pending_bonus_value=entitlements.PendingBonusValue,pending_bonus_quantity=entitlements.PendingBonusQuantity,
                locked_bonus_value=entitlements.LockedBonusValue,additional_equity=entitlements.AdditionalEquity},
            available_cash=new {units=(long)decimal.Floor(Math.Max(0,cash-ReservedCash())*100),scale=2,currency="CNY"}},
            quotes,missing_instruments=missing },JsonOptions);
    }

    public void ValidateIntent(string intentJson)
    {
        using var doc = JsonDocument.Parse(intentJson);
        var intent = doc.RootElement;
        var kind = intent.GetProperty("kind").GetString();
        if (kind == "cancel") return;
        if (IsDaily && (_algorithm.Time.TimeOfDay != TimeSpan.FromHours(9.5) || _dailyDecisionTime != _algorithm.Time))
            throw new NotSupportedException("日线新增委托仅在前日日线可见的开盘决策阶段提交");
        var side = intent.GetProperty("side").GetString();
        if (kind is not ("amount" or "quantity") || side is not ("buy" or "sell")
            || (kind == "amount" && side != "buy")
            || intent.GetProperty("order_type").GetString() != "market"
            || intent.GetProperty("time_in_force").GetString() != "DAY"
            || (intent.TryGetProperty("limit_price", out var limit) && limit.ValueKind != JsonValueKind.Null))
            throw new NotSupportedException("当前仅支持金额买入、数量买卖、撤单及 market DAY");
        var code = intent.GetProperty("instrument").GetProperty("instrument_id").GetString();
        if (!_symbols.ContainsKey(code)) throw new ArgumentException("委托证券不在冻结范围内");
        if (kind == "quantity")
        {
            var quantity = intent.GetProperty("quantity").GetDecimal();
            if (quantity <= 0 || quantity != decimal.Truncate(quantity))
                throw new ArgumentException("数量委托必须为正整数股");
        }
        else
        {
            var amount = intent.GetProperty("amount");
            if (amount.GetProperty("currency").GetString() != "CNY" || amount.GetProperty("units").GetDecimal() <= 0)
                throw new ArgumentException("金额买入必须声明正人民币预算");
        }
    }

    public void Submit(string intentJson)
    {
        ValidateIntent(intentJson);
        using var doc=JsonDocument.Parse(intentJson); var intent=doc.RootElement;
        var kind=intent.GetProperty("kind").GetString();
        if(kind=="cancel")
        {
            var id=int.Parse(intent.GetProperty("order_id").GetString());
            var ticket=_algorithm.Transactions.GetOrderTicket(id);
            ticket.Cancel("策略主动撤单");
            _intents.WriteLine(JsonSerializer.Serialize(new {at=Now,intent=intent.Clone(),order_id=id},JsonOptions));
            _intents.Flush();
            return;
        }
        var code=intent.GetProperty("instrument").GetProperty("instrument_id").GetString();
        var symbol=_symbols[code]; var security=_algorithm.Securities[symbol];
        // 日线Market在同一开盘的决策之后执行；一分钟仍等待下一完整分钟。
        var properties=new ChinaOrderProperties {TimeInForce=TimeInForce.Day, DailyOpenDecision=IsDaily};
        if(intent.TryGetProperty("protection_price",out var protection) && protection.ValueKind!=JsonValueKind.Null)
            properties.ProtectionPrice=protection.GetProperty("units").GetDecimal()/(decimal)Math.Pow(10,protection.GetProperty("scale").GetInt32());
        decimal quantity;
        if(kind=="amount")
        {
            var amount=intent.GetProperty("amount");
            var budget=amount.GetProperty("units").GetDecimal()/(decimal)Math.Pow(10,amount.GetProperty("scale").GetInt32());
            budget=Math.Min(budget,Math.Max(0,_algorithm.Portfolio.CashBook["CNY"].Amount-ReservedCash()));
            properties.BudgetCny=budget;
            quantity=Models.CalculateQuantityForBudget(security,budget,security.Price,_algorithm.UtcTime);
        }
        else if(kind=="quantity") quantity=intent.GetProperty("quantity").GetDecimal();
        else throw new NotSupportedException($"策略委托不支持{kind}");
        if(intent.GetProperty("side").GetString()=="sell") quantity=-quantity;
        int? orderId=null;
        if(quantity!=0)
        {
            var ticket=IsDaily
                ? _algorithm.SubmitOrderRequest(new SubmitOrderRequest(OrderType.Market, SecurityType.Equity,
                    symbol, quantity, 0, 0, _algorithm.UtcTime, intent.GetProperty("intent_id").GetString(), properties))
                : _algorithm.MarketOrder(symbol,quantity,asynchronous:true,tag:intent.GetProperty("intent_id").GetString(),orderProperties:properties);
            orderId=ticket.OrderId;
        }
        _intents.WriteLine(JsonSerializer.Serialize(new {at=Now,intent=intent.Clone(),order_id=orderId,
            submitted_quantity=quantity,budget_cny=properties.BudgetCny,skip_reason=quantity==0 ? "预算不足历史最小申报量" : null},JsonOptions));
        _intents.Flush();
    }

    public void CloseOrders() => _algorithm.Transactions.CancelOpenOrders();
    private string EquitySemantics => IsDaily ? "latest_visible_mark_not_session_close" : "session_close_mark";

    private object ValuationMark(Symbol symbol)
    {
        var security = _algorithm.Securities[symbol];
        var data = security.GetLastData();
        if (data == null || data.EndTime > _algorithm.Time)
            throw new InvalidOperationException("估值缺少当时可见的市场价格");
        var at = new DateTimeOffset(DateTime.SpecifyKind(data.EndTime, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
        var sourceAt = data is DailyCloseMark previous ? previous.SourceTime : data.EndTime;
        var source = data is DailyOpenBar ? "session_open"
            : data is DailyCloseMark ? "completed_daily_close"
            : data is TradeBar bar && bar.Period == TimeSpan.Zero ? "corporate_action_reference"
            : "completed_minute_close";
        return new { instrument_id = _codes[symbol], price = security.Price, source,
            price_available_at = at,
            price_source_at = new DateTimeOffset(DateTime.SpecifyKind(sourceAt, DateTimeKind.Unspecified), TimeSpan.FromHours(8)),
            is_current_session = sourceAt.Date == _algorithm.Time.Date,
            is_current_session_close = !IsDaily && data.EndTime.Date == _algorithm.Time.Date
                && data.EndTime.TimeOfDay == TimeSpan.FromHours(15) };
    }

    private object[] ValuationMarks() => _algorithm.Portfolio.Where(x => x.Value.Quantity != 0).Select(x => x.Key)
        .Concat(Actions.ValuationSymbols).Distinct().OrderBy(x => _codes[x], StringComparer.Ordinal)
        .Select(ValuationMark).ToArray();

    public void CloseSession()
    {
        if (!_closedSessions.Add(_algorithm.Time.Date)) throw new InvalidOperationException("交易日不能重复收盘");
        Actions.CaptureRecord(Now);
        foreach(var pair in _algorithm.Portfolio.Where(x=>x.Value.Quantity!=0)) _touched.Add(pair.Key);
        var positions=new List<object>();
        foreach(var symbol in _touched.OrderBy(x=>_codes[x],StringComparer.Ordinal))
        {
            var holding=_algorithm.Portfolio[symbol]; if(holding.Quantity==0) continue;
            var bar=_algorithm.Securities[symbol].Cache.GetData<TradeBar>();
            if(bar==null || (!IsDaily && bar.EndTime.Date!=_algorithm.Time.Date) || bar.EndTime>_algorithm.Time)
                throw new InvalidOperationException("持仓日终缺少当日可见价格");
            positions.Add(new {instrument_id=_codes[symbol],quantity=holding.Quantity,
                price=_algorithm.Securities[symbol].Price,valuation_mark=ValuationMark(symbol)});
        }
        var value=Actions.GetValuation(); var elapsed=_clock.Elapsed.TotalSeconds;
        _daily.WriteLine(JsonSerializer.Serialize(new {date=_algorithm.Time.ToString("yyyy-MM-dd"),rows=_dayRows,
            cash=_algorithm.Portfolio.CashBook["CNY"].Amount,equity=value.TotalEquity,additional_equity=value.AdditionalEquity,
            equity_semantics=EquitySemantics,valuation_at=Now,valuation_marks=ValuationMarks(),
            positions,active_orders=_algorithm.Transactions.GetOpenOrders().Count,day_wall_seconds=elapsed-_previousClose},JsonOptions));
        _daily.Flush();_dayRows=0;_previousClose=elapsed;
    }

    public void Finish()
    {
        if (HasRuntimeError || _closedSessions.Count != _sessions.Count)
            throw new InvalidOperationException("存在运行错误或冻结交易日未完成日终，不能结束Host");
        _orders.Dispose();_intents.Dispose();_daily.Dispose();
        var value=Actions.GetValuation();
        File.WriteAllText(Path.Combine(_output,"account.json"),JsonSerializer.Serialize(new {cash=_algorithm.Portfolio.CashBook["CNY"].Amount,
            equity=value.TotalEquity,additional_equity=value.AdditionalEquity,rows=_rows,
            equity_semantics=EquitySemantics,valuation_at=Now,valuation_marks=ValuationMarks(),
            positions=_algorithm.Portfolio.Where(x=>x.Value.Quantity!=0).ToDictionary(x=>_codes[x.Key],x=>x.Value.Quantity),
            transitions=Actions.Transitions,algorithm_wall_seconds=_clock.Elapsed.TotalSeconds},new JsonSerializerOptions{WriteIndented=true}));
    }
}
