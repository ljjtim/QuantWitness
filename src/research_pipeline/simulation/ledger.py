"""金融事件到现货/期货账本的纯折叠器。"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from functools import cached_property
import hashlib

from research_pipeline.domain import require_integer_quantity
from research_pipeline.platform.canonical import typed_canonical_hash

from .events import FinancialEvent
from .orders import SimulationContractError


@dataclass(frozen=True)
class ExecutionGroup:
    group_id: str
    market: str
    currency: str
    settlement_policy_id: str
    margin_policy_id: str | None = None

    def __post_init__(self) -> None:
        if not all(isinstance(value, str) and value.strip() for value in (self.group_id, self.market, self.currency, self.settlement_policy_id)):
            raise SimulationContractError("execution group 身份字段不能为空")

    @property
    def is_futures(self) -> bool:
        return self.margin_policy_id is not None


@dataclass(frozen=True)
class PositionLot:
    instrument_hash: str
    sellable: int = 0
    unsettled: int = 0
    frozen: int = 0

    def __post_init__(self) -> None:
        if not self.instrument_hash or min(self.sellable, self.unsettled, self.frozen) < 0:
            raise SimulationContractError("现货持仓 bucket 不能为负")


@dataclass(frozen=True)
class CashReceivable:
    receivable_id: str
    due_date: date
    cash_units: int

    def __post_init__(self) -> None:
        if not self.receivable_id or self.cash_units < 0:
            raise SimulationContractError("应收现金身份或金额无效")


@dataclass(frozen=True)
class PositionEntitlement:
    entitlement_id: str
    instrument_hash: str
    due_date: date
    quantity: int

    def __post_init__(self) -> None:
        if not self.entitlement_id or not self.instrument_hash or self.quantity < 0:
            raise SimulationContractError("待上市权益身份或数量无效")


@dataclass(frozen=True)
class _EventIndexNode:
    """按 event_id 内容确定形状的持久 Treap，插入只复制对数级节点。"""

    event_id: str
    priority: int
    left: "_EventIndexNode | None" = None
    right: "_EventIndexNode | None" = None
    size: int = field(init=False)
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not self.event_id:
            raise SimulationContractError("event_id 不能为空")
        size = 1 + _event_index_size(self.left) + _event_index_size(self.right)
        digest = hashlib.sha256()
        for value in (
            "research-event-index-v1",
            _event_index_hash(self.left),
            self.event_id,
            _event_index_hash(self.right),
        ):
            digest.update(value.encode("utf-8"))
            digest.update(b"\0")
        object.__setattr__(self, "size", size)
        object.__setattr__(self, "content_hash", digest.hexdigest())


_EMPTY_EVENT_INDEX_HASH = hashlib.sha256(b"research-event-index-v1:empty").hexdigest()


def _event_priority(event_id: str) -> int:
    return int.from_bytes(hashlib.sha256(event_id.encode("utf-8")).digest()[:8], "big")


def _event_index_size(node: _EventIndexNode | None) -> int:
    return 0 if node is None else node.size


def _event_index_hash(node: _EventIndexNode | None) -> str:
    return _EMPTY_EVENT_INDEX_HASH if node is None else node.content_hash


def _event_index_contains(node: _EventIndexNode | None, event_id: str) -> bool:
    current = node
    while current is not None:
        if event_id == current.event_id:
            return True
        current = current.left if event_id < current.event_id else current.right
    return False


def _event_index_add(
    node: _EventIndexNode | None,
    event_id: str,
) -> _EventIndexNode:
    if node is None:
        return _EventIndexNode(event_id, _event_priority(event_id))
    if event_id == node.event_id:
        return node
    if event_id < node.event_id:
        left = _event_index_add(node.left, event_id)
        updated = _EventIndexNode(node.event_id, node.priority, left, node.right)
        if (left.priority, left.event_id) > (updated.priority, updated.event_id):
            return _EventIndexNode(
                left.event_id,
                left.priority,
                left.left,
                _EventIndexNode(updated.event_id, updated.priority, left.right, updated.right),
            )
        return updated
    right = _event_index_add(node.right, event_id)
    updated = _EventIndexNode(node.event_id, node.priority, node.left, right)
    if (right.priority, right.event_id) > (updated.priority, updated.event_id):
        return _EventIndexNode(
            right.event_id,
            right.priority,
            _EventIndexNode(updated.event_id, updated.priority, updated.left, right.left),
            right.right,
        )
    return updated


def _event_index_ids(node: _EventIndexNode | None) -> tuple[str, ...]:
    if node is None:
        return ()
    return (*_event_index_ids(node.left), node.event_id, *_event_index_ids(node.right))


@dataclass(frozen=True)
class SpotLedgerState:
    group: ExecutionGroup
    available_cash_units: int
    frozen_cash_units: int = 0
    unsettled_cash_units: int = 0
    positions: tuple[PositionLot, ...] = ()
    cash_receivables: tuple[CashReceivable, ...] = ()
    position_entitlements: tuple[PositionEntitlement, ...] = ()
    _applied_event_index: _EventIndexNode | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.group.is_futures:
            raise SimulationContractError("现货账本不能使用保证金组")
        if min(self.available_cash_units, self.frozen_cash_units, self.unsettled_cash_units) < 0:
            raise SimulationContractError("现金 bucket 不能为负")
        if tuple(item.receivable_id for item in self.cash_receivables) != tuple(sorted({item.receivable_id for item in self.cash_receivables})):
            raise SimulationContractError("应收现金身份必须唯一并排序")
        if tuple(item.entitlement_id for item in self.position_entitlements) != tuple(sorted({item.entitlement_id for item in self.position_entitlements})):
            raise SimulationContractError("待上市权益身份必须唯一并排序")

    @property
    def total_cash_units(self) -> int:
        return (
            self.available_cash_units
            + self.frozen_cash_units
            + self.unsettled_cash_units
            + sum(item.cash_units for item in self.cash_receivables)
        )

    @property
    def applied_event_ids(self) -> tuple[str, ...]:
        """仅供审计和测试按需展开；热路径使用持久索引。"""
        return _event_index_ids(self._applied_event_index)

    @property
    def applied_event_count(self) -> int:
        return _event_index_size(self._applied_event_index)

    @cached_property
    def state_hash(self) -> str:
        return typed_canonical_hash({
            "group": self.group.__dict__, "available_cash_units": self.available_cash_units,
            "frozen_cash_units": self.frozen_cash_units, "unsettled_cash_units": self.unsettled_cash_units,
            "positions": [item.__dict__ for item in self.positions],
            "applied_event_count": self.applied_event_count,
            "applied_event_index_hash": _event_index_hash(self._applied_event_index),
            "cash_receivables": [
                {**item.__dict__, "due_date": item.due_date.isoformat()}
                for item in self.cash_receivables
            ],
            "position_entitlements": [
                {**item.__dict__, "due_date": item.due_date.isoformat()}
                for item in self.position_entitlements
            ],
        })


@dataclass(frozen=True)
class FuturesPosition:
    instrument_hash: str
    contracts: int
    settlement_price_units: int

    def __post_init__(self) -> None:
        if not self.instrument_hash or self.settlement_price_units <= 0:
            raise SimulationContractError("期货持仓身份或结算价无效")


@dataclass(frozen=True)
class FuturesLedgerState:
    group: ExecutionGroup
    equity_units: int
    margin_units: int = 0
    realized_pnl_units: int = 0
    positions: tuple[FuturesPosition, ...] = ()
    _applied_event_index: _EventIndexNode | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.group.is_futures:
            raise SimulationContractError("期货账本必须使用保证金组")
        if self.equity_units < 0 or self.margin_units < 0 or self.margin_units > self.equity_units:
            raise SimulationContractError("期货权益/保证金恒等式不成立")

    @property
    def applied_event_ids(self) -> tuple[str, ...]:
        return _event_index_ids(self._applied_event_index)

    @property
    def applied_event_count(self) -> int:
        return _event_index_size(self._applied_event_index)

    @property
    def free_equity_units(self) -> int:
        return self.equity_units - self.margin_units


def reduce_spot(state: SpotLedgerState, event: FinancialEvent) -> SpotLedgerState:
    if event.group_id != state.group.group_id:
        raise SimulationContractError("事件与 execution group 不一致")
    if _event_index_contains(state._applied_event_index, event.event_id):
        return state
    event_index = _event_index_add(state._applied_event_index, event.event_id)
    values = event.values()
    available, frozen, unsettled = state.available_cash_units, state.frozen_cash_units, state.unsettled_cash_units
    positions = {item.instrument_hash: item for item in state.positions}
    receivables = {item.receivable_id: item for item in state.cash_receivables}
    entitlements = {item.entitlement_id: item for item in state.position_entitlements}
    if event.kind == "cash_reserved":
        units = _positive(values, "cash_units")
        if units > available:
            raise SimulationContractError("可用现金不足")
        available -= units
        frozen += units
    elif event.kind == "position_reserved":
        key, quantity = str(values["instrument_hash"]), _positive(values, "quantity")
        lot = positions.get(key, PositionLot(key))
        if quantity > lot.sellable:
            raise SimulationContractError("可卖数量不足")
        positions[key] = replace(lot, sellable=lot.sellable - quantity, frozen=lot.frozen + quantity)
    elif event.kind == "fill":
        key, side = str(values["instrument_hash"]), str(values["side"])
        quantity, notional, fee = _positive(values, "quantity"), _positive(values, "notional_units"), _nonnegative(values, "fee_units")
        lot = positions.get(key, PositionLot(key))
        if side == "buy":
            debit = notional + fee
            from_frozen = min(frozen, debit)
            frozen -= from_frozen
            available -= debit - from_frozen
            if available < 0:
                raise SimulationContractError("成交后现金为负")
            positions[key] = replace(lot, unsettled=lot.unsettled + quantity)
        elif side == "sell":
            if quantity > lot.frozen + lot.sellable:
                raise SimulationContractError("卖出数量超过已冻结/可卖持仓")
            use_frozen = min(quantity, lot.frozen)
            positions[key] = replace(lot, frozen=lot.frozen - use_frozen, sellable=lot.sellable - (quantity - use_frozen))
            # A 股卖出资金当日可继续交易；T+1 约束作用于可取资金，不应阻断再买入。
            available += notional - fee
        else:
            raise SimulationContractError("fill side 无效")
    elif event.kind == "settlement":
        receivable_id = values.get("receivable_id")
        if receivable_id is not None:
            receivable = receivables.get(str(receivable_id))
            if receivable is None or receivable.due_date > event.effective_time.date():
                raise SimulationContractError("应收现金不存在或尚未到期")
            available += receivable.cash_units
            del receivables[receivable.receivable_id]
        entitlement_id = values.get("entitlement_id")
        if entitlement_id is not None:
            entitlement = entitlements.get(str(entitlement_id))
            if entitlement is None or entitlement.due_date > event.effective_time.date():
                raise SimulationContractError("待上市权益不存在或尚未到期")
            lot = positions.get(entitlement.instrument_hash, PositionLot(entitlement.instrument_hash))
            if entitlement.quantity > lot.unsettled:
                raise SimulationContractError("待上市权益数量超过未结算持仓")
            positions[entitlement.instrument_hash] = replace(
                lot,
                unsettled=lot.unsettled - entitlement.quantity,
                sellable=lot.sellable + entitlement.quantity,
            )
            del entitlements[entitlement.entitlement_id]
        cash_units = _nonnegative(values, "cash_units")
        if cash_units > unsettled:
            raise SimulationContractError("待结算现金不足")
        unsettled -= cash_units
        available += cash_units
        key = values.get("instrument_hash")
        quantity = _nonnegative(values, "quantity")
        if key is not None:
            lot = positions.get(str(key), PositionLot(str(key)))
            if quantity > lot.unsettled:
                raise SimulationContractError("未结算数量不足")
            positions[str(key)] = replace(lot, unsettled=lot.unsettled - quantity, sellable=lot.sellable + quantity)
    elif event.kind == "corporate_action":
        available += int(values.get("cash_delta_units", 0))
        key = str(values["instrument_hash"])
        lot = positions.get(key, PositionLot(key))
        position_receivable = int(values.get("position_entitlement_quantity", 0))
        positions[key] = replace(
            lot,
            sellable=lot.sellable + int(values.get("sellable_delta", 0)),
            unsettled=lot.unsettled + int(values.get("unsettled_delta", 0)) + position_receivable,
        )
        cash_receivable = int(values.get("cash_receivable_units", 0))
        if cash_receivable:
            receivable_id = str(values["receivable_id"])
            if receivable_id in receivables:
                raise SimulationContractError("应收现金身份重复")
            receivables[receivable_id] = CashReceivable(
                receivable_id,
                date.fromisoformat(str(values["cash_due_date"])),
                cash_receivable,
            )
        if position_receivable:
            entitlement_id = str(values["entitlement_id"])
            if entitlement_id in entitlements:
                raise SimulationContractError("待上市权益身份重复")
            entitlements[entitlement_id] = PositionEntitlement(
                entitlement_id,
                key,
                date.fromisoformat(str(values["position_due_date"])),
                position_receivable,
            )
    else:
        raise SimulationContractError("事件不能应用到现货账本")
    return SpotLedgerState(
        group=state.group,
        available_cash_units=available,
        frozen_cash_units=frozen,
        unsettled_cash_units=unsettled,
        positions=tuple(sorted(positions.values(), key=lambda item: item.instrument_hash)),
        cash_receivables=tuple(sorted(receivables.values(), key=lambda item: item.receivable_id)),
        position_entitlements=tuple(sorted(entitlements.values(), key=lambda item: item.entitlement_id)),
        _applied_event_index=event_index,
    )


def reduce_futures(state: FuturesLedgerState, event: FinancialEvent) -> FuturesLedgerState:
    if event.group_id != state.group.group_id:
        raise SimulationContractError("事件与 execution group 不一致")
    if _event_index_contains(state._applied_event_index, event.event_id):
        return state
    event_index = _event_index_add(state._applied_event_index, event.event_id)
    values = event.values()
    equity, margin, realized = state.equity_units, state.margin_units, state.realized_pnl_units
    positions = {item.instrument_hash: item for item in state.positions}
    if event.kind == "fill":
        key = str(values["instrument_hash"])
        contracts = int(values["contracts_delta"])
        fee = _nonnegative(values, "fee_units")
        price = _positive(values, "settlement_price_units")
        old = positions.get(key, FuturesPosition(key, 0, price))
        positions[key] = FuturesPosition(key, old.contracts + contracts, price)
        equity -= fee
        realized -= fee
    elif event.kind == "mark_to_market":
        pnl = int(values["pnl_units"])
        margin = _nonnegative(values, "required_margin_units")
        equity += pnl
        realized += pnl
    elif event.kind in {"margin_call", "forced_liquidation"}:
        margin = _nonnegative(values, "required_margin_units")
    else:
        raise SimulationContractError("事件不能应用到期货账本")
    return FuturesLedgerState(
        group=state.group,
        equity_units=equity,
        margin_units=margin,
        realized_pnl_units=realized,
        positions=tuple(sorted(positions.values(), key=lambda item: item.instrument_hash)),
        _applied_event_index=event_index,
    )


def _positive(values: dict[str, object], field: str) -> int:
    return require_integer_quantity(values[field], field)


def _nonnegative(values: dict[str, object], field: str) -> int:
    return require_integer_quantity(values.get(field, 0), field, allow_zero=True)


__all__ = ["CashReceivable", "ExecutionGroup", "FuturesLedgerState", "FuturesPosition", "PositionEntitlement", "PositionLot", "SpotLedgerState", "reduce_futures", "reduce_spot"]
