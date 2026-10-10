"""执行分钟订单意图，确认成交事件并交结果收集器投影。"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from research_pipeline.domain import (
    InstrumentKey, PortfolioTarget, Price, MinuteRuleResolver,
    MinuteRuleSnapshotBundle, OrderIntent, TradingRuleBinding,
)
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform import typed_canonical_hash
from .cash_market import (
    OpeningSnapshot, execute_cash_order,
)
from .execution_market import MinuteExecutionBar, IntradayExecutionPolicy
from .market_rules import CashMarketPolicy, _require_futures_lifecycle
from .minute_rules import (
    _ResolvedRules, resolve_minute_execution_rules, _positive_integer, _futures_margin_ppm,
    minute_cash_price_limits, resolve_minute_cash_policy, minute_cash_bar_suspended,
)
from .matching import _remaining_capacity, match_minute_futures_order
from .events import FinancialEvent, ExecutionOutcome
from .intent_port import CASH_DAILY_BACKEND, CN_FUTURES_DAILY_BACKEND, IntentToOrderPort
from .ledger import ExecutionGroup, FuturesLedgerState, SpotLedgerState, reduce_futures
from .result_collector import _append_formal_result
from .orders import SimulationContractError

from .target_execution import _ActiveTarget, minute_target_intents

def _minute_rule_binding(
    active: _ActiveTarget, bar: MinuteExecutionBar, rules: _ResolvedRules,
) -> TradingRuleBinding:
    instrument = active.prepared.instrument
    if instrument.asset_class != "cn_future":
        action_snapshot_hash = rules.parameters.get("corporate_action_snapshot_hash")
        if instrument.asset_class == "cn_stock" and action_snapshot_hash is None:
            raise SimulationContractError("分钟股票缺少正式公司行动快照绑定")
        return TradingRuleBinding(
            instrument_hash=instrument.instrument_hash,
            rule_snapshot_hash=rules.identity_hash,
            available_at=rules.available_at,
            corporate_action_snapshot_hash=action_snapshot_hash or typed_canonical_hash({
                "scope": "minute-corporate-action-binding-v1",
                "instrument_hash": instrument.instrument_hash,
                "target_hash": active.prepared.target.target_hash,
            }),
        )
    binding_by_id = {item.rule.rule_id: item.identity_hash for item in rules.bindings}
    return TradingRuleBinding(
        instrument_hash=instrument.instrument_hash,
        rule_snapshot_hash=rules.identity_hash,
        available_at=rules.available_at,
        multiplier_rule_hash=binding_by_id["rule.cn_futures.contract_multiplier.v1"],
        fee_rule_hash=binding_by_id["rule.cn_futures.fee_schedule.v1"],
        margin_rule_hash=binding_by_id["rule.cn_futures.margin.v1"],
        settlement_rule_hash=binding_by_id["rule.cn_futures.session.v1"],
    )


def _reconcile_spot_target(
    active: _ActiveTarget,
    bar: MinuteExecutionBar,
    *,
    resolver: MinuteRuleResolver,
    bundle: MinuteRuleSnapshotBundle,
    policy: IntradayExecutionPolicy,
    execute_order: Callable,
    state: SpotLedgerState | None,
    initial_cash_units: int,
    used_capacity: dict[str, int],
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> SpotLedgerState | None:
    instrument = active.prepared.instrument
    current, sellable = _spot_quantities(state, instrument.instrument_hash)
    desired = active.prepared.desired_quantity
    if current == desired:
        active.t1_rejection_key = None
        return state
    rules, cash_policy = resolve_minute_cash_policy(
        resolver, bundle=bundle, instrument=instrument,
        trading_date=bar.trading_date, decision_at=bar.bar_start,
    )
    if instrument.asset_class == "cn_stock":
        # 延迟执行可以采用新费用，但不能改变目标实际消费的复权与公司行动事实。
        target_binding = resolver.resolve(
            rule_id="rule.cn_stock.adjustment_factor_snapshot.v1",
            instrument_id=instrument.instrument_id,
            effective_on=active.prepared.target.decision_time.date(),
            as_of=active.prepared.target.decision_time,
        )
        target_parameters = dict(target_binding.rule.parameters)
        if any(rules.parameters.get(key) != target_parameters.get(key) for key in (
            "adjustment_snapshot_identity_hash", "corporate_action_snapshot_hash",
        )):
            raise SimulationContractError("分钟股票执行规则与目标实际 PIT 复权工件不一致")
    limits = minute_cash_price_limits(rules.parameters, decision_at=bar.bar_start)
    execution_price = bar.avg_units if bar.avg_units is not None else bar.close_units
    if limits.mode == "bounded" and not limits.low_limit_units <= execution_price <= limits.high_limit_units:
        raise SimulationContractError("分钟执行价格超出当时有效的涨跌停范围")
    intents = minute_target_intents(
        active, bar, rule_binding=_minute_rule_binding(active, bar, rules),
        current_quantity=current, sellable_quantity=sellable, cash_policy=cash_policy,
    )
    if not intents:
        return state
    intent = intents[0]
    side, quantity = intent.side, intent.quantity
    capacity = _remaining_capacity(
        bar, policy, used_capacity, lot_size=1,
    )
    bar_suspended = minute_cash_bar_suspended(
        resolver, bundle=bundle, asset_class=instrument.asset_class,
        instrument_id=instrument.instrument_id, trading_date=bar.trading_date,
        bar_start=bar.bar_start, bar_end=bar.bar_end, available_at=bar.available_time,
    )
    if bar_suspended:
        capacity = 0
    result_state, reason, filled = _execute_spot_order(
        intent=intent,
        target=active.prepared.target,
        instrument=instrument,
        side=side,
        quantity=quantity,
        bar=bar,
        rules=rules,
        cash_policy=cash_policy,
        execute_order=execute_order,
        state=state,
        initial_cash_units=initial_cash_units,
        visible_capacity=capacity,
        bar_suspended=bar_suspended,
        order_rows=order_rows,
        fill_rows=fill_rows,
        cost_rows=cost_rows,
    )
    if filled:
        used_capacity[bar.bar_hash] = used_capacity.get(bar.bar_hash, 0) + filled
    if reason == "t1_sell_blocked":
        active.t1_rejection_key = (bar.trading_date, sellable, desired)
    elif filled or reason != "t1_sell_blocked":
        active.t1_rejection_key = None
    return result_state


def _execute_spot_order(
    *,
    intent: OrderIntent,
    target: PortfolioTarget,
    instrument: InstrumentKey,
    side: str,
    quantity: int,
    bar: MinuteExecutionBar,
    rules: _ResolvedRules,
    cash_policy: CashMarketPolicy,
    execute_order: Callable,
    state: SpotLedgerState | None,
    initial_cash_units: int,
    visible_capacity: int,
    bar_suspended: bool,
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> tuple[SpotLedgerState, str | None, int]:
    market = instrument.asset_class
    settlement_days = cash_policy.settlement_days
    current = state or SpotLedgerState(
        ExecutionGroup(
            f"minute-default-{market}", market, "CNY", f"t{settlement_days}"
        ),
        initial_cash_units,
    )
    order = IntentToOrderPort(CASH_DAILY_BACKEND).to_order(
        intent,
        ordinal=0,
        time_in_force="IOC",
    )
    price_units = bar.avg_units if bar.avg_units is not None else bar.close_units
    limits = minute_cash_price_limits(rules.parameters, decision_at=bar.bar_start)
    price_scale = limits.price_scale
    snapshot = OpeningSnapshot(
        instrument.instrument_hash,
        Price(price_units, price_scale, "CNY"),
        None if limits.high_limit_units is None else Price(limits.high_limit_units, price_scale, "CNY"),
        None if limits.low_limit_units is None else Price(limits.low_limit_units, price_scale, "CNY"),
        rules.parameters["paused"] or bar_suspended,
        visible_capacity,
        bar.available_time,
    )
    def execute() -> ExecutionOutcome:
        result = execute_cash_order(
            order, policy=cash_policy, snapshot=snapshot, state=current,
            execution_at=bar.available_time,
        )
        return ExecutionOutcome(result.filled_quantity, result.reason_code, result)

    execution = execute_order(
        order, trading_session=bar.trading_date, event_time=bar.available_time,
        execute=execute,
    )
    reason = execution.reason_code
    if execution.filled_quantity < quantity and reason in {None, "partially_filled"}:
        reason = "participation_cap"
    _append_formal_result(
        order=order,
        target=target,
        bar=bar,
        requested_quantity=quantity,
        filled_quantity=execution.filled_quantity,
        reason=reason,
        execution_price_units=price_units,
        price_scale=price_scale,
        contract_multiplier=1,
        fee_units=_cash_execution_fee(execution.events),
        realized_pnl_units=0,
        position_effect="auto",
        event_hashes=tuple(item.event_hash for item in execution.events),
        order_rows=order_rows,
        fill_rows=fill_rows,
        cost_rows=cost_rows,
    )
    return execution.state, reason, execution.filled_quantity


def _reconcile_futures_target(
    active: _ActiveTarget,
    bar: MinuteExecutionBar,
    *,
    resolver: MinuteRuleResolver,
    bundle: MinuteRuleSnapshotBundle,
    policy: IntradayExecutionPolicy,
    execute_order: Callable,
    state: FuturesLedgerState | None,
    initial_cash_units: int,
    used_capacity: dict[str, int],
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> FuturesLedgerState | None:
    instrument = active.prepared.instrument
    current_quantity = _futures_quantity(state, instrument.instrument_hash)
    desired = active.prepared.desired_quantity
    if current_quantity == desired:
        return state
    rules = resolve_minute_execution_rules(
        resolver,
        bundle=bundle,
        asset_class="cn_future",
        instrument_id=instrument.instrument_id,
        effective_on=bar.trading_date,
        as_of=bar.bar_start,
    )
    actual = rules.parameters.get("actual_contract_id")
    if actual != instrument.instrument_id:
        raise SimulationContractError("连续期货合约不能进入正式成交")
    _require_futures_lifecycle(rules.parameters, bar.trading_date)
    _require_visible_price_limits(
        rules.parameters,
        bar=bar,
        reference_key="reference_previous_settlement_units",
        rounding="floor_to_price_tick",
    )
    tick = _positive_integer(rules.parameters, "price_tick_units")
    price_units = bar.avg_units if bar.avg_units is not None else bar.close_units
    if price_units % tick:
        raise SimulationContractError("期货成交价不符合最小变动价位")
    intents = minute_target_intents(
        active, bar, rule_binding=_minute_rule_binding(active, bar, rules),
        current_quantity=current_quantity,
    )
    current_state = state
    for intent in intents:
        side, quantity, position_effect = intent.side, intent.quantity, intent.position_effect
        capacity = _remaining_capacity(bar, policy, used_capacity, lot_size=1)
        if bar.open_interest is not None:
            capacity = min(capacity, max(0, bar.open_interest - used_capacity.get(bar.bar_hash, 0)))
        result_state, filled = _execute_futures_order(
            intent=intent,
            target=active.prepared.target,
            instrument=instrument,
            side=side,
            quantity=quantity,
            position_effect=position_effect,
            bar=bar,
            rules=rules,
            execute_order=execute_order,
            state=current_state,
            initial_cash_units=initial_cash_units,
            visible_capacity=capacity,
            order_rows=order_rows,
            fill_rows=fill_rows,
            cost_rows=cost_rows,
        )
        current_state = result_state
        if filled:
            used_capacity[bar.bar_hash] = used_capacity.get(bar.bar_hash, 0) + filled
        if filled < quantity:
            break
    return current_state


def _execute_futures_order(
    *,
    intent: OrderIntent,
    target: PortfolioTarget,
    instrument: InstrumentKey,
    side: str,
    quantity: int,
    position_effect: str,
    bar: MinuteExecutionBar,
    rules: _ResolvedRules,
    execute_order: Callable,
    state: FuturesLedgerState | None,
    initial_cash_units: int,
    visible_capacity: int,
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> tuple[FuturesLedgerState, int]:
    multiplier = _positive_integer(rules.parameters, "contract_unit_kg")
    price_scale = _price_scale(rules.parameters)
    margin_policy_id = str(rules.parameters.get("margin_policy_id", "")).strip()
    if not margin_policy_id:
        raise SimulationContractError("期货分钟规则缺少 margin_policy_id")
    margin_ppm = _futures_margin_ppm(rules.parameters)
    current = state or FuturesLedgerState(
        ExecutionGroup(
            "minute-default-cn-futures",
            "cn_future",
            "CNY",
            "daily-settlement",
            margin_policy_id,
        ),
        initial_cash_units,
    )
    order = IntentToOrderPort(CN_FUTURES_DAILY_BACKEND).to_order(
        intent,
        ordinal=0,
        time_in_force="IOC",
    )
    price_units = bar.avg_units if bar.avg_units is not None else bar.close_units
    def execute() -> ExecutionOutcome:
        candidate = match_minute_futures_order(
            instrument_hash=instrument.instrument_hash, side=side, quantity=quantity,
            position_effect=position_effect, visible_capacity=visible_capacity,
            price_units=price_units, multiplier=multiplier, margin_ppm=margin_ppm,
            parameters=rules.parameters, state=current,
        )
        return ExecutionOutcome(candidate.filled_quantity, candidate.reason_code, candidate)

    candidate = execute_order(
        order, trading_session=bar.trading_date, event_time=bar.available_time,
        execute=execute,
    )
    filled = candidate.filled_quantity
    reason = candidate.reason_code
    realized_pnl = candidate.realized_pnl_units
    fee = candidate.fee_units
    required_margin = candidate.required_margin_units
    events: list[FinancialEvent] = []
    if filled:
        if required_margin < current.margin_units:
            release = FinancialEvent(
                f"{order.order_id}:margin-release",
                "mark_to_market",
                bar.available_time,
                bar.trading_date.isoformat(),
                current.group.group_id,
                rules.identity_hash,
                (("pnl_units", 0), ("required_margin_units", required_margin)),
                order.order_id,
            )
            current = reduce_futures(current, release)
            events.append(release)
        fill_event = FinancialEvent(
            f"{order.order_id}:fill",
            "fill",
            bar.available_time,
            bar.trading_date.isoformat(),
            current.group.group_id,
            rules.identity_hash,
            tuple(sorted({
                "contracts_delta": candidate.contracts_delta,
                "fee_units": fee,
                "instrument_hash": instrument.instrument_hash,
                "settlement_price_units": price_units,
                "position_margin_units": candidate.position_margin_units,
                "pnl_remainder_numerator": candidate.pnl_remainder_numerator,
                "pnl_remainder_denominator": candidate.pnl_remainder_denominator,
            }.items())),
            order.order_id,
        )
        current = reduce_futures(current, fill_event)
        events.append(fill_event)
        margin_event = FinancialEvent(
            f"{order.order_id}:valuation",
            "mark_to_market",
            bar.available_time,
            bar.trading_date.isoformat(),
            current.group.group_id,
            rules.identity_hash,
            tuple(sorted({
                "pnl_units": realized_pnl,
                "required_margin_units": required_margin,
            }.items())),
            fill_event.event_id,
        )
        current = reduce_futures(current, margin_event)
        events.append(margin_event)
        if filled < quantity:
            reason = "participation_cap"
    fee_units = candidate.fee_units
    _append_formal_result(
        order=order,
        target=target,
        bar=bar,
        requested_quantity=quantity,
        filled_quantity=filled,
        reason=reason,
        execution_price_units=price_units,
        price_scale=price_scale,
        contract_multiplier=multiplier,
        fee_units=fee_units,
        realized_pnl_units=realized_pnl,
        position_effect=position_effect,
        event_hashes=tuple(item.event_hash for item in events),
        order_rows=order_rows,
        fill_rows=fill_rows,
        cost_rows=cost_rows,
    )
    return current, filled


def _spot_quantities(
    state: SpotLedgerState | None,
    instrument_hash: str,
) -> tuple[int, int]:
    if state is None:
        return 0, 0
    lot = next((
        item for item in state.positions if item.instrument_hash == instrument_hash
    ), None)
    if lot is None:
        return 0, 0
    return lot.sellable + lot.unsettled + lot.frozen, lot.sellable


def _futures_quantity(
    state: FuturesLedgerState | None,
    instrument_hash: str,
) -> int:
    if state is None:
        return 0
    return next((
        item.contracts
        for item in state.positions
        if item.instrument_hash == instrument_hash
    ), 0)


def _cash_execution_fee(events: tuple[FinancialEvent, ...]) -> int:
    fill = next((item for item in events if item.kind == "fill"), None)
    return 0 if fill is None else int(fill.values()["fee_units"])


def _require_visible_price_limits(
    parameters: Mapping[str, object],
    *,
    bar: MinuteExecutionBar,
    reference_key: str = "reference_previous_close_units",
    rounding: str = "half_up_to_quote_unit",
) -> None:
    reference = _positive_integer(parameters, reference_key)
    ratio_ppm = _positive_integer(parameters, "price_limit_ratio_ppm")
    if ratio_ppm > 1_000_000:
        raise SimulationContractError("分钟涨跌停比例超出支持范围")
    price_scale = _price_scale(parameters)
    raw_available_at = parameters.get("reference_price_available_at")
    if not isinstance(raw_available_at, str):
        raise SimulationContractError("分钟前收参考值缺少可见时间")
    try:
        available_at = datetime.fromisoformat(raw_available_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SimulationContractError("分钟前收参考值可见时间无效") from exc
    require_aware_datetime(available_at, "reference_price_available_at")
    if available_at > bar.bar_start:
        raise SimulationContractError("分钟前收参考值在订单提交时尚不可见")
    ratio = Decimal(ratio_ppm) / Decimal(1_000_000)
    if rounding == "half_up_to_quote_unit":
        expected_high = int(
            (Decimal(reference) * (Decimal(1) + ratio)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
        expected_low = int(
            (Decimal(reference) * (Decimal(1) - ratio)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
    elif rounding == "floor_to_price_tick":
        tick = _positive_integer(parameters, "price_tick_units")
        expected_high = int(Decimal(reference) * (Decimal(1) + ratio)) // tick * tick
        expected_low = int(Decimal(reference) * (Decimal(1) - ratio)) // tick * tick
    else:
        raise SimulationContractError("分钟涨跌停舍位规则不受支持")
    declared_high = _positive_integer(parameters, "high_limit_units")
    declared_low = _positive_integer(parameters, "low_limit_units")
    if (declared_high, declared_low) != (expected_high, expected_low):
        raise SimulationContractError("分钟涨跌停值与前收、比例和报价精度不一致")
    execution_price = bar.avg_units if bar.avg_units is not None else bar.close_units
    if not declared_low <= execution_price <= declared_high:
        raise SimulationContractError("分钟执行价格超出当时有效的涨跌停范围")
    if not 0 <= price_scale <= 9:
        raise SimulationContractError("分钟价格精度超出支持范围")


def _price_scale(parameters: Mapping[str, object]) -> int:
    value = parameters.get("price_scale")
    if type(value) is not int or not 0 <= value <= 9:
        raise SimulationContractError("分钟规则缺少有效 price_scale")
    return value
