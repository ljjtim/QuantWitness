using System.Text.Json;
using QuantConnect.Orders;
using QuantWitness.Lean.China;

namespace QuantWitness.Lean.Research;

public sealed partial class LeanStrategyHost
{
    private sealed class FutureRoll
    {
        public string Id, Old, New, Direction;
        public decimal Quantity, Closed, Opened;
        public DateTime AvailableAt, Start, End;
        public string State = "pending";
    }
    private readonly List<FutureRoll> _futureRolls = new();
    private readonly HashSet<string> _exitingFutureCodes = new();
    private readonly HashSet<(string, DateTime)> _futureExecutedOpens = new();
    private readonly Dictionary<string, DateTime> _futureExitStarts = new();
    private readonly Dictionary<string, DateTime> _futureLifecycleAttempts = new();
    private bool _maintainingFutureLifecycle;

    private void InitializeFutureLifecycle(JsonElement settings)
    {
        FutureModels.OpenAllowed = symbol => !_exitingFutureCodes.Contains(_codes[symbol]);
        foreach (var contract in _futureContracts.EnumerateObject())
        {
            var deadline = FutureTime(contract.Value, "exit_deadline");
            if (contract.Value.TryGetProperty("exit_start_at", out var start))
                _futureExitStarts[contract.Name] = DateTimeOffset.Parse(start.GetString()).UtcDateTime;
            else
            {
                var session = _futureSessions.LastOrDefault(x => x.GetProperty("instrument_id").GetString() == contract.Name
                    && FutureTime(x, "start_at") <= deadline && deadline <= FutureTime(x, "end_at"));
                if (session.ValueKind != JsonValueKind.Undefined) _futureExitStarts[contract.Name] = FutureTime(session, "start_at");
            }
        }
        if (!settings.TryGetProperty("roll_plans", out var plans)) return;
        foreach (var row in plans.EnumerateArray())
        {
            var plan = new FutureRoll { Id = row.GetProperty("plan_id").GetString(), Old = row.GetProperty("old_instrument_id").GetString(),
                New = row.GetProperty("new_instrument_id").GetString(), Direction = row.GetProperty("direction").GetString(),
                Quantity = row.GetProperty("quantity").GetDecimal(), AvailableAt = FutureTime(row, "available_at"),
                Start = FutureTime(row, "start_at"), End = FutureTime(row, "end_at") };
            if (plan.Quantity <= 0 || plan.Quantity != decimal.Truncate(plan.Quantity) || plan.Direction is not ("long" or "short")
                || plan.Old == plan.New || plan.Start > plan.End || plan.AvailableAt > plan.Start
                || _futureRolls.Any(x => x.Id == plan.Id)) throw new InvalidOperationException("换月计划数量、方向、身份或可见时刻无效");
            var old = _futureContracts.GetProperty(plan.Old); var next = _futureContracts.GetProperty(plan.New);
            foreach (var field in new[] { "multiplier", "price_scale", "tick_size" })
                if (FutureDecimal(old, field) != FutureDecimal(next, field))
                    throw new InvalidOperationException("当前换月只支持同规格合约按1:1手数迁移");
            if (_futureRolls.Any(x => x.Old == plan.Old && x.Direction == plan.Direction && x.Start <= plan.End && plan.Start <= x.End))
                throw new InvalidOperationException("同合约同方向换月窗口不能重叠");
            _futureRolls.Add(plan);
        }
        _futureRolls.Sort((left, right) => { var order = left.Start.CompareTo(right.Start); return order != 0 ? order : string.CompareOrdinal(left.Id, right.Id); });
    }

    private void RecordFutureLifecycleFill(Order order, IReadOnlyList<FutureFillLeg> legs)
    {
        var code = _codes[order.Symbol];
        if (IsDaily) _futureExecutedOpens.Add((code, _algorithm.UtcTime));
        var properties = (ChinaFutureOrderProperties)order.Properties;
        if (properties.RollPlanId == null) return;
        var plan = _futureRolls.Single(x => x.Id == properties.RollPlanId);
        var quantity = legs.Sum(x => x.Quantity);
        if (code == plan.Old && properties.PositionEffect != "open") plan.Closed += quantity;
        else if (code == plan.New && properties.PositionEffect == "open") plan.Opened += quantity;
        else throw new InvalidOperationException("换月成交与冻结两腿计划不一致");
        if (plan.Opened > plan.Closed || plan.Closed > plan.Quantity) throw new InvalidOperationException("换月实际成交超过平旧额度");
        WriteFutureLifecycle("roll_fill", new { plan_id = plan.Id, order_id = order.Id, closed = plan.Closed, opened = plan.Opened });
        if (plan.Opened == plan.Quantity) { plan.State = "completed"; WriteFutureLifecycle("roll_completed", new { plan_id = plan.Id }); }
    }

    private bool FutureLifecycleAttempt(string key)
    {
        if (_futureLifecycleAttempts.GetValueOrDefault(key) == _algorithm.UtcTime) return false;
        _futureLifecycleAttempts[key] = _algorithm.UtcTime;
        return true;
    }

    private void MaintainFutureLifecycle()
    {
        if (_maintainingFutureLifecycle || _futureTradingDate == null) return;
        _maintainingFutureLifecycle = true;
        try
        {
            var now = _algorithm.UtcTime;
            foreach (var pair in _futureExitStarts.Where(x => x.Value <= now).OrderBy(x => x.Key, StringComparer.Ordinal))
            {
                var code = pair.Key; var symbol = _symbols[code];
                if (_exitingFutureCodes.Add(code)) WriteFutureLifecycle("expiry_exit_started", new { instrument_id = code });
                CancelFutureOpens(code);
                var holding = (ChinaFutureHolding)_algorithm.Securities[symbol].Holdings;
                if (now > FutureTime(_futureContracts.GetProperty(code), "exit_deadline") && holding.Invested)
                    throw new InvalidOperationException($"退出截止仍有持仓：{code}");
                if (FutureModels.RiskActive || !IsFutureTradingTime(code, now)) continue;
                foreach (var direction in new[] { "long", "short" })
                {
                    var quantity = holding.Positions().Where(x => x.Direction == direction).Sum(x =>
                        x.Quantity - FutureModels.ReservedClose(symbol, direction, x.Bucket));
                    if (quantity > 0 && FutureLifecycleAttempt("expiry:" + code + ":" + direction))
                        SubmitFutureOrder(code, quantity * (direction == "long" ? -1 : 1), "close", "expiry-exit");
                }
            }
            foreach (var plan in _futureRolls.Where(x => x.AvailableAt <= now && x.Start <= now && x.State is "pending" or "active"))
            {
                if (plan.State == "pending")
                {
                    plan.State = "active"; _exitingFutureCodes.Add(plan.Old);
                    WriteFutureLifecycle("roll_started", new { plan_id = plan.Id, old_instrument_id = plan.Old,
                        new_instrument_id = plan.New, direction = plan.Direction, quantity = plan.Quantity, available_at = plan.AvailableAt });
                }
                CancelFutureOpens(plan.Old);
                if (now > plan.End)
                {
                    foreach (var order in _algorithm.Transactions.GetOpenOrders().Where(x => ((ChinaFutureOrderProperties)x.Properties).RollPlanId == plan.Id))
                        _algorithm.Transactions.GetOrderTicket(order.Id).Cancel("换月执行窗口结束");
                    plan.State = "expired";
                    WriteFutureLifecycle("roll_expired", new { plan_id = plan.Id, closed = plan.Closed, opened = plan.Opened });
                    continue;
                }
                if (FutureModels.RiskActive || FutureObservationOnly) continue;
                var active = _algorithm.Transactions.GetOpenOrders().Where(x => ((ChinaFutureOrderProperties)x.Properties).RollPlanId == plan.Id).ToArray();
                var oldPending = active.Where(x => _codes[x.Symbol] == plan.Old).Sum(x => x.AbsoluteQuantity - _filled.GetValueOrDefault(x.Id));
                var newPending = active.Where(x => _codes[x.Symbol] == plan.New).Sum(x => x.AbsoluteQuantity - _filled.GetValueOrDefault(x.Id));
                var oldSymbol = _symbols[plan.Old];
                var oldHolding = (ChinaFutureHolding)_algorithm.Securities[oldSymbol].Holdings;
                var closeQuantity = Math.Min(Math.Max(0, plan.Quantity - plan.Closed - oldPending), oldHolding.Positions()
                    .Where(x => x.Direction == plan.Direction).Sum(x => x.Quantity - FutureModels.ReservedClose(oldSymbol, x.Direction, x.Bucket)));
                if (closeQuantity > 0 && IsFutureTradingTime(plan.Old, now) && FutureLifecycleAttempt(plan.Id + ":old"))
                    SubmitFutureOrder(plan.Old, closeQuantity * (plan.Direction == "long" ? -1 : 1), "close", "roll-old", roll: plan.Id);
                var openQuantity = Math.Min(plan.Quantity - plan.Opened, Math.Max(0, plan.Closed - plan.Opened - newPending));
                if (openQuantity > 0 && IsFutureTradingTime(plan.New, now) && FutureLifecycleAttempt(plan.Id + ":new"))
                    SubmitFutureOrder(plan.New, openQuantity * (plan.Direction == "long" ? 1 : -1), "open", "roll-new", roll: plan.Id);
            }
        }
        finally { _maintainingFutureLifecycle = false; }
    }

    private void CancelFutureOpens(string code)
    {
        foreach (var order in _algorithm.Transactions.GetOpenOrders().Where(x => _codes[x.Symbol] == code
            && ((ChinaFutureOrderProperties)x.Properties).PositionEffect == "open"))
            _algorithm.Transactions.GetOrderTicket(order.Id).Cancel("退出阶段取消增仓订单");
    }
}
