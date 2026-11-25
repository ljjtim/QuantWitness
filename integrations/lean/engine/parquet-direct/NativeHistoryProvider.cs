using NodaTime;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Securities;

namespace QuantWitness.LeanInput;

/// <summary>原生历史、指标预热和新证券seed共同读取冻结来源，禁止默认文件回退。</summary>
public sealed class NativeHistoryProvider : HistoryProviderBase
{
    private NativeDataStore _store;
    private int _count;
    public override int DataPointCount => _count;
    public override void Initialize(HistoryProviderInitializeParameters parameters)
        => _store = new NativeDataStore(NativeDataStore.ManifestPath);

    public override IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
    {
        var iterators = new List<IEnumerator<BaseData>>();
        try
        {
            foreach (var request in requests)
            {
                IEnumerator<BaseData> iterator = _store.Read(request.ToSubscriptionDataConfig(), request.StartTimeUtc,
                    request.EndTimeUtc, request.EndTimeUtc, "history").GetEnumerator();
                if (request.FillForwardResolution.HasValue)
                    iterator = new FillForwardEnumerator(iterator, new SecurityExchange(request.ExchangeHours),
                        QuantConnect.Util.Ref.CreateReadOnly(() => request.FillForwardResolution.Value.ToTimeSpan()),
                        request.IncludeExtendedMarketHours, request.StartTimeLocal, request.EndTimeLocal,
                        request.Resolution.ToTimeSpan(), request.DataTimeZone, false, request.DataType);
                if (iterator.MoveNext()) iterators.Add(iterator); else iterator.Dispose();
            }
            while (iterators.Count != 0)
            {
                var at = iterators.Min(x => x.Current.EndTime);
                var rows = new List<BaseData>();
                for (var i = iterators.Count - 1; i >= 0; i--)
                {
                    var iterator = iterators[i];
                    while (iterator.Current.EndTime == at)
                    {
                        rows.Add(iterator.Current); _count++;
                        if (!iterator.MoveNext()) { iterator.Dispose(); iterators.RemoveAt(i); break; }
                    }
                }
                var utc = at.ConvertToUtc(TimeZones.Shanghai);
                yield return new Slice(utc.ConvertFromUtc(sliceTimeZone), rows, utc);
            }
        }
        finally { foreach (var iterator in iterators) iterator.Dispose(); }
    }
}
