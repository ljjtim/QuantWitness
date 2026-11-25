using System.Text.Json;
using QuantConnect.Orders;

namespace QuantWitness.Lean.Research;

public sealed partial class LeanStrategyHost
{
    private long _spotAccountSequence;
    private int _spotTransitionCount;
    private readonly object _spotResultGate = new();
    private readonly Dictionary<string, StreamWriter> _spotResultWriters = new();

    private void AppendSpotResult(string table, object payload)
    {
        var line = JsonSerializer.Serialize(payload, JsonOptions);
        lock (_spotResultGate)
        {
            if (!_spotResultWriters.TryGetValue(table, out var writer))
                _spotResultWriters[table] = writer = new StreamWriter(Path.Combine(_output, table + ".jsonl"), append: true);
            writer.WriteLine(line);
        }
    }

    private void DisposeSpotWriters()
    {
        lock (_spotResultGate)
        {
            foreach (var writer in _spotResultWriters.Values) writer.Dispose();
            _spotResultWriters.Clear();
        }
        _orders.Dispose();
        _daily.Dispose();
    }

    public void RecordSpotTransitions()
    {
        if (IsFuture || Actions == null) return;
        lock (_spotResultGate)
            while (_spotTransitionCount < Actions.Transitions.Count)
            {
                var transition = Actions.Transitions[_spotTransitionCount++];
                if (transition.RegisteredQuantity == 0m && transition.CashDelta == 0m && transition.ShareDelta == 0m
                    && !transition.AffectsAccount)
                    continue;
                AppendSpotResult("lifecycle", new {
                    kind = "corporate_action", action_id = transition.ActionId, revision = transition.Revision,
                    phase = transition.Phase, at = transition.At, registered_quantity = transition.RegisteredQuantity,
                    cash_delta = transition.CashDelta, share_delta = transition.ShareDelta });
            }
    }

    public void RecordSpotAccount(string reason)
    {
        if (IsFuture) return;
        RecordSpotTransitions();
        var value = Actions.GetValuation(Now);
        var sequence = ++_spotAccountSequence;
        AppendSpotResult("account_events", new {
            account_id = "CNY", event_sequence = sequence, reason, at = Now, available_at = Now,
            valuation_at = Now, trading_date = _algorithm.Time.ToString("yyyy-MM-dd"),
            cash = _algorithm.Portfolio.CashBook["CNY"].Amount, equity = value.TotalEquity,
            additional_equity = value.AdditionalEquity, share_receivables = ShareReceivables(Now) });
        foreach (var symbol in ValuationSymbols)
        {
            var security = _algorithm.Securities[symbol];
            AppendSpotResult("positions", new {
                account_id = "CNY", event_sequence = sequence, at = Now,
                trading_date = _algorithm.Time.ToString("yyyy-MM-dd"), instrument_id = _codes[symbol],
                quantity = security.Holdings.Quantity, average_price = security.Holdings.AveragePrice,
                price = security.Holdings.Price, sellable_quantity = Models.SellableQuantity(security, _algorithm.UtcTime),
                locked_bonus_quantity = Actions.LockedBonusQuantity(symbol) });
        }
    }

    private void RecordSpotFill(OrderEvent value)
    {
        if (value.FillQuantity == 0) return;
        AppendSpotResult("fills", new {
            fill_id = $"{value.OrderId}:{value.Id}", order_id = value.OrderId, event_id = value.Id,
            account_id = "CNY", instrument_id = _codes[value.Symbol], at = Now,
            trading_date = _algorithm.Time.ToString("yyyy-MM-dd"), quantity = value.FillQuantity,
            price = value.FillPrice, fee = value.OrderFee.Value.Amount, fee_currency = value.OrderFee.Value.Currency,
            status = value.Status.ToString() });
        RecordSpotAccount("fill");
    }
}
