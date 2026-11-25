using System.Text.Json;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.AlgorithmFactory.Python.Wrappers;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.Research;
using QuantConnect.Lean.Engine.Setup;
using QuantConnect.Lean.Engine.TransactionHandlers;
using QuantWitness.Lean.China;
using QuantWitness.LeanInput;

namespace QuantWitness.Lean.Research;

/// <summary>中国市场状态独立于项目算法回调，由引擎阶段统一推进。</summary>
public sealed class NativeMarketLifecycle : IResearchLifecycle
{
    private static NativeMarketLifecycle _active;
    private QCAlgorithm _algorithm;
    private LeanStrategyHost _host;
    private NativeResultRecorder _recorder;
    private DateTime _start;
    private DateTime _end;
    private decimal _cash;
    private DateTime _executionStartUtc;
    private bool _finished;
    private SetupHandlerParameters _setupParameters;
    private readonly HashSet<DateTime> _closed = new();

    internal static QCAlgorithm BaseAlgorithm(IAlgorithm algorithm) =>
        algorithm is AlgorithmPythonWrapper python ? python.BaseAlgorithm : (QCAlgorithm)algorithm;

    internal static NativeMarketLifecycle Active => _active ??= new NativeMarketLifecycle();
    internal void Bind(SetupHandlerParameters parameters) => _setupParameters = parameters;

    public void BeforeInitialize(IAlgorithm algorithm) => Active.Initialize(Active._setupParameters);

    public static string AccountView(bool includeRegistrations = false)
    {
        var service = Active;
        if (service._host == null || !service.IsExecution(service._algorithm, service._algorithm.UtcTime))
            throw new InvalidOperationException("账户视图仅在正式执行时点可用");
        return service._host.AccountView(includeRegistrations);
    }

    public static decimal QuantityForBudget(Symbol symbol, decimal budgetCny)
    {
        var service = Active;
        if (service._host == null || service._host.IsFuture
            || !service.IsExecution(service._algorithm, service._algorithm.UtcTime)
            || !service._host.IsNativeInstrument(symbol))
            throw new InvalidOperationException("预算数量仅用于当前已准入的现货证券");
        var security = service._algorithm.Securities[symbol];
        return service._host.Models.CalculateQuantityForBudget(security, budgetCny, security.Price, service._algorithm.UtcTime);
    }

    public sealed record SpotOrderRuleView(decimal PriceTick, decimal? LowerLimit, decimal? UpperLimit,
        bool MarketProtectionRequired, bool MarketOrderAllowed, bool Paused, int SettlementDays,
        QuantityGrid MarketBuy, QuantityGrid MarketSell, QuantityGrid LimitBuy, QuantityGrid LimitSell);

    public sealed record FutureOrderRuleView(string TradingDate, FutureRule Rule);

    public static SpotOrderRuleView CurrentSpotOrderRules(Symbol symbol)
    {
        var service = ForCurrentRules(symbol, future: false);
        return service._host.SpotOrderRules(symbol);
    }

    public static FutureOrderRuleView CurrentFutureOrderRules(Symbol symbol)
    {
        var service = ForCurrentRules(symbol, future: true);
        return service._host.FutureOrderRules(symbol);
    }

    private static NativeMarketLifecycle ForCurrentRules(Symbol symbol, bool future)
    {
        var service = Active;
        if (service._host == null || service._host.IsFuture != future
            || !service.IsExecution(service._algorithm, service._algorithm.UtcTime)
            || !service._host.IsNativeInstrument(symbol))
            throw new InvalidOperationException("规则视图仅用于当前正式执行的已准入证券");
        return service;
    }

    internal void Initialize(SetupHandlerParameters parameters)
    {
        _active = this;
        _algorithm = BaseAlgorithm(parameters.Algorithm);
        var directory = Config.Get("results-destination-folder");
        _host = new LeanStrategyHost(_algorithm, directory);
        _host.FreezeNativeModels();
        _start = _algorithm.StartDate;
        _end = _algorithm.EndDate;
        _cash = _algorithm.Portfolio.CashBook["CNY"].Amount;
        _executionStartUtc = _host.NativeExecutionStartUtc;
        NativeClockSubscription.Install(_algorithm, Path.Combine(directory, "input.json"));
        _recorder = new NativeResultRecorder(_algorithm, directory, _host);
        _recorder.Initialize();
        ((BrokerageTransactionHandler)parameters.TransactionHandler).NewOrderEvent += (_, value) =>
        {
            if (_host.IsNativeInstrument(value.Symbol)) _host.OnOrderEvent(value);
            _recorder.OnOrderEvent(value);
        };
    }

    public void AfterInitialize(IAlgorithm algorithm)
    {
        var service = Active;
        if (service._algorithm.StartDate != service._start || service._algorithm.EndDate != service._end
            || service._algorithm.Portfolio.CashBook.AccountCurrency != "CNY"
            || service._algorithm.Portfolio.CashBook["CNY"].Amount != service._cash)
            throw new InvalidOperationException("算法初始化不得修改冻结的日期、账户币种或初始现金");
    }

    private bool IsExecution(IAlgorithm algorithm, DateTime utc) =>
        _host != null && !algorithm.IsWarmingUp && utc >= _executionStartUtc;

    public void PrepareTime(IAlgorithm algorithm, DateTime utc)
    {
        var service = Active;
        if (!service.IsExecution(algorithm, utc)) return;
        if (service._host.IsFuture) service._host.AdvanceFutureClock();
        else service._host.AdvanceSpotClock();
    }

    public void AfterData(IAlgorithm algorithm)
    {
        var service = Active;
        if (!service.IsExecution(algorithm, algorithm.UtcTime)) return;
        service.PrepareTime(algorithm, algorithm.UtcTime);
        service._host.ObserveBars(algorithm.CurrentSlice);
        if (!service._host.IsFuture) service._host.RefreshSpotMarketReferences(algorithm.CurrentSlice);
    }

    public void AfterTimeStep(IAlgorithm algorithm)
    {
        var service = Active;
        if (!service.IsExecution(algorithm, algorithm.UtcTime)) return;
        var host = service._host;
        var sessionClosed = false;
        if (host.IsFuture)
        {
            host.FinishFutureOpportunity();
            service._setupParameters.TransactionHandler.ProcessSynchronousEvents();
            if (host.FutureDayCanClose)
            {
                host.CloseSession();
                sessionClosed = true;
            }
        }
        else
        {
            host.Actions.CompleteTimeStep(new DateTimeOffset(algorithm.UtcTime));
            if (host.ExecutionWindowEndsNow && service._closed.Add(algorithm.Time))
            {
                if (host.SessionClosesNow)
                {
                    host.CloseOrders();
                    service._setupParameters.TransactionHandler.ProcessSynchronousEvents();
                }
                host.CloseSession();
                sessionClosed = true;
            }
            if (host.TerminalObservationNow) host.ObserveTerminalAccount();
            host.RecordSpotTransitions();
        }
        service._recorder.AfterTimeStep(sessionClosed);
    }

    public void Complete(IAlgorithm algorithm)
    {
        var service = Active;
        if (service._host == null || service._finished) return;
        service._finished = true;
        try
        {
            if (algorithm.RunTimeError == null) service._host.Finish();
            else service._host.SaveFailureAccount(algorithm.RunTimeError.ToString());
        }
        finally { service._recorder?.Complete(); }
    }
}

public sealed class NativeSetupHandler : BacktestingSetupHandler
{
    public NativeSetupHandler()
    {
        InitializationTimeOut = TimeSpan.FromSeconds(Config.GetInt("research-initialize-seconds", 60));
        AlgorithmCreationTimeout = InitializationTimeOut;
    }

    public override bool Setup(SetupHandlerParameters parameters)
    {
        NativeMarketLifecycle.Active.Bind(parameters);
        return base.Setup(parameters);
    }
}

