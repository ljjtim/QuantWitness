using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Lean.Engine.Results;
using QuantConnect.Packets;

namespace QuantWitness.LeanInput;

public sealed class ParquetDataFeed : FileSystemDataFeed
{
    private FrozenFinancialDayStore _store;
    private IAlgorithm _algorithm;
    private string _output;
    private NativeDataStore _native;

    public override void Initialize(IAlgorithm algorithm, AlgorithmNodePacket job, IResultHandler resultHandler,
        IMapFileProvider mapFileProvider, IFactorFileProvider factorFileProvider, IDataProvider dataProvider,
        IDataFeedSubscriptionManager subscriptionManager, IDataFeedTimeProvider dataFeedTimeProvider,
        IDataChannelProvider dataChannelProvider)
    {
        base.Initialize(algorithm, job, resultHandler, mapFileProvider, factorFileProvider, dataProvider,
            subscriptionManager, dataFeedTimeProvider, dataChannelProvider);
        _algorithm = algorithm;
        _output = Config.Get("results-destination-folder");
        var manifest = Config.Get("parquet-input-manifest");
        _native = new NativeDataStore(manifest);
        _store = FrozenFinancialDayStore.Open(manifest);
    }

    public override Subscription CreateSubscription(SubscriptionRequest request)
    {
        // 手动 AddEquity 和定时 Universe 只生成引擎时钟。
        if (request.IsUniverseSubscription && request.Universe.TryGetUniverse(out ITimeTriggeredUniverse _))
            return SubscriptionUtils.Create(request, CreateUniverseEnumerator(request), _algorithm.Settings.DailyPreciseEndTime);
        var reader = _native.Read(request.Configuration, request.StartTimeUtc, request.EndTimeUtc,
            request.EndTimeUtc, request.IsUniverseSubscription ? "universe" : "feed", request.IsUniverseSubscription);
        IEnumerator<BaseData> iterator = reader.GetEnumerator();
        if (!request.IsUniverseSubscription && request.Configuration.FillDataForward
            && request.Configuration.Type != typeof(ResearchClock))
            iterator = new FillForwardEnumerator(iterator, request.Security.Exchange,
                QuantConnect.Util.Ref.CreateReadOnly(() => request.Configuration.Increment),
                request.Configuration.ExtendedMarketHours, request.StartTimeLocal, request.EndTimeLocal,
                request.Configuration.Increment, request.Configuration.DataTimeZone, false, request.Configuration.Type);
        return SubscriptionUtils.Create(request, iterator, _algorithm.Settings.DailyPreciseEndTime);
    }

    public override void Exit()
    {
        var store = Interlocked.Exchange(ref _store, null);
        if (store != null)
        {
            try { store.Save(Path.Combine(_output, "parquet-read.json")); }
            finally { store.Clear(); }
        }
        base.Exit();
    }
}
