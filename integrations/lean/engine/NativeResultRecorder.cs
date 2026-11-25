using System.Text.Json;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Orders;
using QuantWitness.LeanInput;

namespace QuantWitness.Lean.Research;

/// <summary>记录原生算法、订单请求及引擎终态，不构造策略意图。</summary>
public sealed class NativeResultRecorder
{
    private readonly QCAlgorithm _algorithm;
    private readonly LeanStrategyHost _host;
    private readonly string _directory;
    private readonly string _algorithmType;
    private readonly string _algorithmLanguage;
    private readonly bool _strictAudit = Environment.GetEnvironmentVariable("QUANTWITNESS_STRICT_AUDIT") == "1";
    private readonly object _gate = new();
    private StreamWriter _trace;
    private long _sequence;

    public NativeResultRecorder(QCAlgorithm algorithm, string directory, LeanStrategyHost host)
    {
        _algorithm = algorithm;
        _directory = directory;
        _host = host;
        var algorithmIdentity = ManifestCache.Read(Path.Combine(directory, "strategy-input.json")).GetProperty("algorithm");
        _algorithmType = algorithmIdentity.GetProperty("type_name").GetString();
        _algorithmLanguage = algorithmIdentity.GetProperty("language").GetString();
    }

    public void Initialize()
    {
        _trace = new StreamWriter(Path.Combine(_directory, "strategy-trace.jsonl"));
        Record("algorithm_initialized", new { algorithm_type = _algorithmType, algorithm_language = _algorithmLanguage });
        WriteStatus("running", false);
        OrderRequest.ResponseChanged += OnRequestResponse;
    }

    public void OnOrderEvent(OrderEvent orderEvent)
    {
        Record("order_event", new { order_id = orderEvent.OrderId, event_id = orderEvent.Id,
            status = orderEvent.Status.ToString(), quantity = orderEvent.FillQuantity,
            price = orderEvent.FillPrice, fee = orderEvent.OrderFee.Value.Amount,
            fee_currency = orderEvent.OrderFee.Value.Currency, message = orderEvent.Message });
    }

    public void AfterTimeStep(bool sessionClosed)
    {
        if (!_host.IsFuture && _host.ExecutionWindowEndsNow) _host.RecordSpotAccount("window_end");
        if (sessionClosed)
        {
            lock (_gate) _trace.Flush();
        }
    }

    private void OnRequestResponse(OrderRequest request)
    {
        // 无效订单号的改单和撤单使用空提交请求构造返回票据。只记录真实请求。
        if (request is SubmitOrderRequest placeholder
            && placeholder.Symbol == Symbol.Empty && request.Time == DateTime.MaxValue)
            return;
        var response = request.Response;
        Record("order_request", new {
            order_id = request.OrderId, request_type = request.OrderRequestType.ToString(),
            request_at = new DateTimeOffset(DateTime.SpecifyKind(request.Time, DateTimeKind.Utc)),
            request_status = request.Status.ToString(), tag = request.Tag,
            quantity = request is SubmitOrderRequest submit ? (decimal?)submit.Quantity
                : request is UpdateOrderRequest update ? update.Quantity : null,
            limit_price = request is SubmitOrderRequest s ? (decimal?)s.LimitPrice
                : request is UpdateOrderRequest u ? u.LimitPrice : null,
            stop_price = request is SubmitOrderRequest ss ? (decimal?)ss.StopPrice
                : request is UpdateOrderRequest uu ? uu.StopPrice : null,
            order_type = request is SubmitOrderRequest order ? order.OrderType.ToString() : null,
            response_error_code = response.ErrorCode.ToString(), response_message = response.ErrorMessage,
            response_is_error = response.IsError });
    }

    private void Record(string kind, object payload)
    {
        lock (_gate)
        {
            if (_trace == null) return;
            _trace.WriteLine(JsonSerializer.Serialize(new { event_seq = ++_sequence, kind,
                at = new DateTimeOffset(DateTime.SpecifyKind(_algorithm.UtcTime, DateTimeKind.Utc)), payload }));
            if (_strictAudit) _trace.Flush();
        }
    }

    private void WriteStatus(string status, bool hostFinished)
    {
        File.WriteAllText(Path.Combine(_directory, "engine-status.json"), JsonSerializer.Serialize(new {
            status, host_finished = hostFinished, engine_status = _host.EngineStatus,
            runtime_error = _host.HasRuntimeError ? _host.RuntimeErrorText : null }));
    }

    public void Complete()
    {
        OrderRequest.ResponseChanged -= OnRequestResponse;
        var successful = !_host.HasRuntimeError && File.Exists(Path.Combine(_directory, "account.json"));
        lock (_gate)
        {
            Record(successful ? "algorithm_completed" : "algorithm_failed", new { engine_status = _host.EngineStatus });
            _trace?.Dispose();
            _trace = null;
        }
        File.WriteAllText(Path.Combine(_directory, "algorithm-state.json"), JsonSerializer.Serialize(new {
            algorithm_type = _algorithmType, algorithm_language = _algorithmLanguage,
            status = successful ? "completed" : "failed",
            at = new DateTimeOffset(DateTime.SpecifyKind(_algorithm.UtcTime, DateTimeKind.Utc)),
            object_store = "storage", state_kind = "native_algorithm_terminal_metadata" }));
        WriteStatus(successful ? "completed" : "failed", successful);
    }
}
