"""A 股与 ETF 共用的定点现货执行骨架。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction, MarketRuleSnapshot, Price
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.domain.trading import InstrumentKey, instrument_key_from_legacy
from research_pipeline.platform import typed_canonical_hash

from .events import FinancialEvent
from .ledger import SpotLedgerState, reduce_spot
from .orders import Order, SimulationContractError
from .corporate_actions import compile_corporate_action


_CASH_DAILY_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class CashMarketPolicy:
    market: str
    rule: MarketRuleSnapshot
    lot_size: int
    settlement_days: int
    commission_ppm: int
    min_commission_units: int
    sell_tax_ppm: int
    transfer_fee_ppm: int
    slippage_units_per_share: int = 0
    cash_shortage_policy: str = "reject_v1"

    def __post_init__(self) -> None:
        if self.market not in {"cn_stock", "cn_etf"}:
            raise SimulationContractError("现货 policy market 无效")
        if self.lot_size < 1 or self.settlement_days not in {0, 1}:
            raise SimulationContractError("lot/settlement policy 无效")
        if min(self.commission_ppm, self.min_commission_units, self.sell_tax_ppm, self.transfer_fee_ppm) < 0:
            raise SimulationContractError("费用 policy 不能为负")
        if self.slippage_units_per_share < 0:
            raise SimulationContractError("每股滑点不能为负")
        if self.cash_shortage_policy not in {"reject_v1", "clip_current_lot_continue_v1"}:
            raise SimulationContractError("现金不足 policy 无效")

    @property
    def policy_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "rule_hash": self.rule.content_hash,
            "lot_size": self.lot_size,
            "settlement_days": self.settlement_days,
            "commission_ppm": self.commission_ppm,
            "min_commission_units": self.min_commission_units,
            "sell_tax_ppm": self.sell_tax_ppm,
            "transfer_fee_ppm": self.transfer_fee_ppm,
            "slippage_units_per_share": self.slippage_units_per_share,
            "cash_shortage_policy": self.cash_shortage_policy,
        }


@dataclass(frozen=True)
class OpeningSnapshot:
    instrument_hash: str
    open_price: Price
    high_limit: Price
    low_limit: Price
    paused: bool
    visible_capacity: int
    available_time: datetime
    adjustment: str = "none"

    def __post_init__(self) -> None:
        require_aware_datetime(self.available_time, "available_time")
        if self.visible_capacity < 0:
            raise SimulationContractError("visible_capacity 不能为负")
        if self.adjustment != "none":
            raise SimulationContractError("成交必须使用未复权价格")
        identities = {(item.scale, item.currency) for item in (self.open_price, self.high_limit, self.low_limit)}
        if len(identities) != 1:
            raise SimulationContractError("开盘和涨跌停价格精度/币种不一致")


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
    order_market = getattr(order.instrument, "asset_class", None) or getattr(
        order.instrument,
        "market",
        None,
    )
    if order_market != policy.market or state.group.market != policy.market:
        raise SimulationContractError("订单、policy 与 execution group 市场不一致")
    if order.instrument.currency != state.group.currency:
        raise SimulationContractError("订单、execution group 币种不一致")
    if state.group.currency != snapshot.open_price.currency:
        raise SimulationContractError("订单组与价格币种不一致")
    fill_time = order.submitted_at if execution_at is None else execution_at
    if fill_time < order.submitted_at:
        raise SimulationContractError("execution_at 早于订单提交")
    if snapshot.available_time > fill_time:
        raise SimulationContractError("开盘快照在成交时尚不可见")
    execution_price = _execution_price(order.side, snapshot.open_price, policy, cash_scale=cash_scale)
    reason = _reject_reason(order, policy, snapshot, state, execution_price)
    if reason is not None:
        return CashExecutionResult(state, (), 0, reason, policy.rule.content_hash)
    capacity = min(order.quantity, snapshot.visible_capacity)
    filled = capacity // policy.lot_size * policy.lot_size
    if (order.side == "sell" and order.quantity % policy.lot_size
            and _sell_remainder_allowed(order, policy)):
        sellable = next((item.sellable for item in state.positions if item.instrument_hash == snapshot.instrument_hash), 0)
        remainder = sellable % policy.lot_size
        # 一次成交可以带走全部零股；容量不足时只成交整手，不能拆分余额。
        if remainder and capacity >= remainder:
            filled = max(filled, (capacity - remainder) // policy.lot_size * policy.lot_size + remainder)
    if filled <= 0:
        return CashExecutionResult(state, (), 0, "capacity_exceeded", policy.rule.content_hash)
    if order.side == "buy":
        requested_cost = cash_price_amount_units(execution_price, filled, cash_scale=cash_scale)
        requested_cost += _fee_units(policy, "buy", requested_cost)
        if requested_cost > state.available_cash_units:
            if policy.cash_shortage_policy == "reject_v1":
                return CashExecutionResult(state, (), 0, "insufficient_cash", policy.rule.content_hash)
            filled = _max_affordable_quantity(
                available_cash_units=state.available_cash_units,
                requested=filled,
                lot_size=policy.lot_size,
                price=execution_price,
                policy=policy,
                cash_scale=cash_scale,
            )
            if filled <= 0:
                return CashExecutionResult(state, (), 0, "insufficient_cash", policy.rule.content_hash)
    notional_units = cash_price_amount_units(execution_price, filled, cash_scale=cash_scale)
    fee = _fee_units(policy, order.side, notional_units)
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
        if receivable.due_date <= effective_time.date():
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
        if entitlement.due_date <= effective_time.date():
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
        if held <= 0:
            continue
        compiled = compile_corporate_action(
            action,
            held_quantity=held,
            effective_time=effective_time,
            group_id=current.group.group_id,
            rule_hash=rule_hash,
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

    values = state.total_cash_units
    for lot in state.positions:
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


def _sell_remainder_allowed(order: Order, policy: CashMarketPolicy) -> bool:
    """使用显式历史规则，或沪深交易所自 2006 年交易规则明确的整手零股政策。"""
    declared = dict(policy.rule.parameters).get("sell_remainder_allowed")
    if declared is not None:
        return declared is True
    return (
        (order.instrument if isinstance(order.instrument, InstrumentKey) else instrument_key_from_legacy(order.instrument)).venue in {"XSHG", "XSHE"}
        and policy.lot_size == 100
        and order.submitted_at.date() >= date(2006, 7, 1)
    )


def _reject_reason(order: Order, policy: CashMarketPolicy, snapshot: OpeningSnapshot, state: SpotLedgerState, execution_price: Price) -> str | None:
    remainder = order.quantity % policy.lot_size
    if remainder:
        sellable = next((item.sellable for item in state.positions if item.instrument_hash == snapshot.instrument_hash), 0)
        if order.side != "sell" or not _sell_remainder_allowed(order, policy) or remainder != sellable % policy.lot_size:
            return "invalid_lot"
    if snapshot.paused:
        return "suspended"
    if order.side == "buy" and execution_price.units >= snapshot.high_limit.units:
        return "limit_up_buy_blocked"
    if order.side == "sell" and execution_price.units <= snapshot.low_limit.units:
        return "limit_down_sell_blocked"
    if order.limit_price is not None:
        if order.side == "buy" and execution_price.units > order.limit_price.units:
            return "limit_price_not_reached"
        if order.side == "sell" and execution_price.units < order.limit_price.units:
            return "limit_price_not_reached"
    if order.side == "sell":
        sellable = next((item.sellable for item in state.positions if item.instrument_hash == snapshot.instrument_hash), 0)
        if sellable < order.quantity:
            return "t1_sell_blocked" if policy.settlement_days == 1 else "insufficient_position"
    return None


def _execution_price(side: str, reference: Price, policy: CashMarketPolicy, *, cash_scale: int | None = None) -> Price:
    direction = 1 if side == "buy" else -1
    slippage = policy.slippage_units_per_share
    if cash_scale is not None:
        slippage = int(Decimal(slippage).scaleb(reference.scale - cash_scale))
    units = reference.units + direction * slippage
    if units <= 0:
        raise SimulationContractError("滑点后的成交价格必须为正")
    return Price(units, reference.scale, reference.currency)


def _max_affordable_quantity(
    *,
    available_cash_units: int,
    requested: int,
    lot_size: int,
    price: Price,
    policy: CashMarketPolicy,
    cash_scale: int | None = None,
) -> int:
    low, high = 0, requested // lot_size
    while low < high:
        middle = (low + high + 1) // 2
        quantity = middle * lot_size
        notional = cash_price_amount_units(price, quantity, cash_scale=cash_scale)
        if notional + _fee_units(policy, "buy", notional) <= available_cash_units:
            low = middle
        else:
            high = middle - 1
    return low * lot_size


def cash_price_amount_units(price: Price, quantity: int, *, cash_scale: int | None) -> int:
    """数量乘价格后统一换算金额；日频现金按分，分钟沿用价格单位。"""
    if cash_scale is None:
        return price.units * quantity
    return int((price.decimal * quantity).scaleb(cash_scale).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _fee_units(policy: CashMarketPolicy, side: str, notional_units: int) -> int:
    commission = max(policy.min_commission_units, _ceil_ratio(notional_units * policy.commission_ppm, 1_000_000))
    transfer = _ceil_ratio(notional_units * policy.transfer_fee_ppm, 1_000_000)
    tax = _ceil_ratio(notional_units * policy.sell_tax_ppm, 1_000_000) if side == "sell" else 0
    return commission + transfer + tax


def _ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


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
