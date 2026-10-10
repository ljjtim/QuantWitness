"""A 股与 ETF 共用的定点现货执行骨架。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction, Price

from .events import FinancialEvent
from .ledger import SpotLedgerState, reduce_spot
from .orders import Order, SimulationContractError
from .corporate_actions import CorporateActionRecordPosition, compile_corporate_action
from .costs import cash_price_amount_units
from .market_rules import CashMarketPolicy
from .matching import match_cash_order
from .execution_market import OpeningSnapshot


_CASH_DAILY_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class CashExecutionResult:
    state: SpotLedgerState
    events: tuple[FinancialEvent, ...]
    filled_quantity: int
    reason_code: str | None
    rule_hash: str


def cash_daily_preopen_at(session: date) -> datetime:
    """返回日频现金结算和公司行动唯一的盘前事件时点。"""

    return datetime.combine(session, time(9, 15), _CASH_DAILY_TIMEZONE)


def execute_cash_order(
    order: Order,
    *,
    policy: CashMarketPolicy,
    snapshot: OpeningSnapshot,
    state: SpotLedgerState,
    execution_at: datetime | None = None,
    cash_scale: int | None = None,
) -> CashExecutionResult:
    candidate = match_cash_order(
        order, policy=policy, snapshot=snapshot, state=state,
        execution_at=execution_at, cash_scale=cash_scale,
    )
    if candidate.filled_quantity == 0:
        return CashExecutionResult(state, (), 0, candidate.reason_code, policy.rule.content_hash)
    execution_price = candidate.execution_price
    filled = candidate.filled_quantity
    notional_units = candidate.notional_units
    fee = candidate.fee_units
    fill_time = order.submitted_at if execution_at is None else execution_at
    events: list[FinancialEvent] = []
    base = {"effective_time": fill_time, "session": fill_time.date().isoformat(), "group_id": state.group.group_id, "rule_hash": policy.rule.content_hash, "parent_id": order.order_id}
    if order.side == "buy":
        events.append(FinancialEvent(f"{order.order_id}:reserve", "cash_reserved", payload=(("cash_units", notional_units + fee),), **base))
    else:
        events.append(FinancialEvent(f"{order.order_id}:reserve", "position_reserved", payload=(("instrument_hash", snapshot.instrument_hash), ("quantity", filled)), **base))
    events.append(FinancialEvent(f"{order.order_id}:fill", "fill", payload=tuple(sorted({"fee_units": fee, "instrument_hash": snapshot.instrument_hash, "notional_units": notional_units, "quantity": filled, "side": order.side, **({"execution_price_units": execution_price.units, "price_scale": execution_price.scale} if cash_scale is not None else {})}.items())), **base))
    if order.side == "buy" and policy.settlement_days == 0:
        events.append(FinancialEvent(f"{order.order_id}:settle", "settlement", payload=(("cash_units", 0), ("instrument_hash", snapshot.instrument_hash), ("quantity", filled)), **base))
    current = state
    for event in events:
        current = reduce_spot(current, event)
    return CashExecutionResult(current, tuple(events), filled, None if filled == order.quantity else "partially_filled", policy.rule.content_hash)


def settle_cash_daily_open(
    state: SpotLedgerState,
    *,
    effective_time: datetime,
    rule_hash: str,
    protected_quantities: Mapping[str, int] | None = None,
    managed_receivables: frozenset[str] = frozenset(),
    managed_position_entitlements: frozenset[str] = frozenset(),
) -> tuple[SpotLedgerState, tuple[FinancialEvent, ...]]:
    """结算上一交易会话的现金、普通持仓和到期公司行动权益。"""

    events: list[FinancialEvent] = []
    session = effective_time.date().isoformat()
    base = {
        "effective_time": effective_time,
        "session": session,
        "group_id": state.group.group_id,
        "rule_hash": rule_hash,
    }
    if state.unsettled_cash_units:
        events.append(FinancialEvent(
            f"settle-cash-{session}-{state.group.group_id}",
            "settlement",
            payload=(("cash_units", state.unsettled_cash_units), ("quantity", 0)),
            **base,
        ))
    entitled_by_instrument: dict[str, int] = {}
    for entitlement in state.position_entitlements:
        entitled_by_instrument[entitlement.instrument_hash] = (
            entitled_by_instrument.get(entitlement.instrument_hash, 0)
            + entitlement.quantity
        )
    for lot in state.positions:
        trade_unsettled = lot.unsettled - entitled_by_instrument.get(
            lot.instrument_hash, 0
        )
        if trade_unsettled < 0:
            raise SimulationContractError("待上市权益超过未结算持仓")
        if protected_quantities is not None:
            trade_unsettled = max(0, trade_unsettled - protected_quantities.get(lot.instrument_hash, 0))
        if trade_unsettled:
            events.append(FinancialEvent(
                f"settle-{session}-{lot.instrument_hash}",
                "settlement",
                payload=(
                    ("cash_units", 0),
                    ("instrument_hash", lot.instrument_hash),
                    ("quantity", trade_unsettled),
                ),
                **base,
            ))
    for receivable in state.cash_receivables:
        if receivable.due_date <= effective_time.date() and receivable.receivable_id not in managed_receivables:
            events.append(FinancialEvent(
                f"settle-{session}-{receivable.receivable_id}",
                "settlement",
                payload=(
                    ("cash_units", 0),
                    ("quantity", 0),
                    ("receivable_id", receivable.receivable_id),
                ),
                **base,
            ))
    for entitlement in state.position_entitlements:
        if entitlement.due_date <= effective_time.date() and entitlement.entitlement_id not in managed_position_entitlements:
            events.append(FinancialEvent(
                f"settle-{session}-{entitlement.entitlement_id}",
                "settlement",
                payload=(
                    ("cash_units", 0),
                    ("entitlement_id", entitlement.entitlement_id),
                    ("quantity", 0),
                ),
                **base,
            ))
    current = state
    for event in events:
        current = reduce_spot(current, event)
    return current, tuple(events)


def apply_cash_corporate_actions(
    state: SpotLedgerState,
    actions: Sequence[CorporateAction],
    *,
    effective_time: datetime,
    rule_hash: str,
    record_positions: Mapping[tuple[str, date], CorporateActionRecordPosition] | None = None,
) -> tuple[SpotLedgerState, tuple[FinancialEvent, ...]]:
    """只应用生效且在当时已经可见的公司行动。"""

    current = state
    events: list[FinancialEvent] = []
    for action in sorted(actions, key=lambda item: item.action_id):
        if action.effective_date != effective_time.date():
            raise SimulationContractError("公司行动生效日与日频会话不一致")
        if action.announcement_available_time > effective_time:
            raise SimulationContractError("公司行动生效时仍不可见")
        held = next(
            (
                lot.sellable + lot.unsettled + lot.frozen
                for lot in current.positions
                if lot.instrument_hash == action.instrument_hash
            ),
            0,
        )
        if held <= 0 and action.contract_version == 1:
            continue
        lot = next((item for item in current.positions if item.instrument_hash == action.instrument_hash), None)
        if action.contract_version == 2 and action.kind in {"split", "reverse_split"} and any(
            item.instrument_hash == action.instrument_hash for item in current.position_entitlements
        ):
            raise SimulationContractError("拆并股前尚有未结算公司行动权益，缺少承接依据")
        compiled = compile_corporate_action(
            action,
            held_quantity=held,
            effective_time=effective_time,
            group_id=current.group.group_id,
            rule_hash=rule_hash,
            record_position=(record_positions or {}).get((action.instrument_hash, action.record_date)),
            position_buckets={name: 0 if lot is None else getattr(lot, name)
                              for name in ("sellable", "unsettled", "frozen")},
        )
        for event in compiled:
            current = reduce_spot(current, event)
            events.append(event)
    return current, tuple(events)


def cash_daily_nav_units(
    state: SpotLedgerState,
    price_by_instrument: Mapping[str, Price],
) -> int:
    """以显式未复权价格计算日频现货账户净值。"""

    values = state.total_cash_units - state.payable_tax_units
    for lot in state.positions:
        if lot.sellable + lot.unsettled + lot.frozen == 0:
            continue
        try:
            price = price_by_instrument[lot.instrument_hash]
        except KeyError as exc:
            raise SimulationContractError("日频估值缺少持仓价格") from exc
        values += cash_price_amount_units(price, lot.sellable + lot.unsettled + lot.frozen, cash_scale=2)
    return values


def cash_position_quantities(
    state: SpotLedgerState,
    instrument_hash_by_code: Mapping[str, str],
) -> dict[str, int]:
    """以经济总持仓作为目标调仓基数，避免未结算或冻结头寸被重复买入。"""

    positions = {lot.instrument_hash: lot for lot in state.positions}
    return {
        code: 0
        if (lot := positions.get(instrument_hash)) is None
        else lot.sellable + lot.unsettled + lot.frozen
        for code, instrument_hash in instrument_hash_by_code.items()
    }


def cash_target_quantity(
    *,
    nav_units: int,
    target_weight: object,
    price: Price,
    lot_size: int,
) -> int:
    """用同一整数手数规则把权重目标转换为股数/份额。"""

    try:
        weight = Decimal(str(target_weight))
    except InvalidOperation as exc:
        raise SimulationContractError("target_weight 不是有效数值") from exc
    if (
        type(nav_units) is not int
        or nav_units < 0
        or not isinstance(price, Price)
        or type(lot_size) is not int
        or lot_size <= 0
        or not weight.is_finite()
        or weight < 0
        or weight > 1
    ):
        raise SimulationContractError("现金目标数量参数无效")
    quantity = int(Decimal(nav_units).scaleb(-2) * weight / price.decimal)
    return quantity // lot_size * lot_size


def cash_rebalance_deltas(
    current: Mapping[str, int],
    desired: Mapping[str, int],
) -> tuple[tuple[str, str, int], ...]:
    """统一生成先卖后买、代码稳定排序的日频现金调仓差额。"""

    codes = set(current) | set(desired)
    if any(
        type(values.get(code, 0)) is not int or values.get(code, 0) < 0
        for values in (current, desired)
        for code in codes
    ):
        raise SimulationContractError("现金调仓持仓数量无效")
    rows = [
        (code, "sell", current.get(code, 0) - desired.get(code, 0))
        for code in codes
        if current.get(code, 0) > desired.get(code, 0)
    ]
    rows.extend(
        (code, "buy", desired.get(code, 0) - current.get(code, 0))
        for code in codes
        if desired.get(code, 0) > current.get(code, 0)
    )
    return tuple(sorted(rows, key=lambda item: (item[1] != "sell", item[0])))


__all__ = [
    "CashExecutionResult",
    "CashMarketPolicy",
    "OpeningSnapshot",
    "apply_cash_corporate_actions",
    "cash_daily_nav_units",
    "cash_daily_preopen_at",
    "cash_position_quantities",
    "cash_rebalance_deltas",
    "cash_target_quantity",
    "execute_cash_order",
    "settle_cash_daily_open",
]
