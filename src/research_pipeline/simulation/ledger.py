"""金融事件到现货/期货账本的纯折叠器。"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from functools import cached_property
from fractions import Fraction
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
class SuccessorEntitlement:
    entitlement_id: str
    instrument_hash: str
    quantity: int

    def __post_init__(self) -> None:
        if not self.entitlement_id or not self.instrument_hash or type(self.quantity) is not int or self.quantity <= 0:
            raise SimulationContractError("待登记后继权益身份或数量无效")


@dataclass(frozen=True)
class CashPayable:
    """已经核定、尚未扣收的税款；扣收时不再重复确认费用。"""

    payable_id: str
    cash_units: int

    def __post_init__(self) -> None:
        if not self.payable_id or type(self.cash_units) is not int or self.cash_units < 0:
            raise SimulationContractError("应付税款身份或金额无效")


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
class OrderReservation:
    """同一订单独占的资金及持仓桶；数量不另计入账户持仓。"""

    order_id: str
    instrument_hash: str | None = None
    cash_units: int = 0
    margin_units: int = 0
    sell_quantity: int = 0
    close_today_quantity: int = 0
    close_yesterday_quantity: int = 0

    def __post_init__(self) -> None:
        if not self.order_id or any(type(value) is not int or value < 0 for value in (
            self.cash_units, self.margin_units, self.sell_quantity,
            self.close_today_quantity, self.close_yesterday_quantity,
        )):
            raise SimulationContractError("订单预占身份或余额无效")
        if (self.sell_quantity or self.close_today_quantity or self.close_yesterday_quantity) and not self.instrument_hash:
            raise SimulationContractError("持仓预占必须绑定标的")

    @property
    def is_empty(self) -> bool:
        return not any((self.cash_units, self.margin_units, self.sell_quantity,
                        self.close_today_quantity, self.close_yesterday_quantity))


def _reservation_for(reservations: tuple[OrderReservation, ...], order_id: str) -> OrderReservation | None:
    return next((item for item in reservations if item.order_id == order_id), None)


def _store_reservation(reservations: tuple[OrderReservation, ...], item: OrderReservation) -> tuple[OrderReservation, ...]:
    updated = {value.order_id: value for value in reservations}
    if item.is_empty:
        updated.pop(item.order_id, None)
    else:
        updated[item.order_id] = item
    return tuple(updated[key] for key in sorted(updated))


def _order_id(values: dict[str, object]) -> str | None:
    value = values.get("order_id")
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise SimulationContractError("order_id 必须是非空字符串")
    return value


@dataclass(frozen=True)
class SpotLedgerState:
    group: ExecutionGroup
    available_cash_units: int
    frozen_cash_units: int = 0
    unsettled_cash_units: int = 0
    positions: tuple[PositionLot, ...] = ()
    cash_receivables: tuple[CashReceivable, ...] = ()
    position_entitlements: tuple[PositionEntitlement, ...] = ()
    cash_payables: tuple[CashPayable, ...] = ()
    successor_entitlements: tuple[SuccessorEntitlement, ...] = ()
    _applied_event_index: _EventIndexNode | None = field(default=None, repr=False)

    order_reservations: tuple[OrderReservation, ...] = ()
    withdrawal_reservations: tuple[tuple[str, int], ...] = ()
    cashflow_tracking: bool = False
    unwithdrawable_sale_units: int = 0
    credit_state: object | None = None

    @property
    def withdrawal_reserved_units(self) -> int:
        return sum(units for _, units in self.withdrawal_reservations)

    def withdrawable_cash_units(self, event_id: str | None = None) -> int:
        own = dict(self.withdrawal_reservations).get(event_id, 0)
        return max(0, self.available_cash_units + own - self.unwithdrawable_sale_units - self.payable_tax_units)

    def reservation_for(self, order_id: str) -> OrderReservation | None:
        return _reservation_for(self.order_reservations, order_id)

    def available_position_quantity(self, instrument_hash: str, *, order_id: str | None = None) -> int:
        lot = next((item for item in self.positions if item.instrument_hash == instrument_hash), PositionLot(instrument_hash))
        own = self.reservation_for(order_id) if order_id is not None else None
        return lot.sellable + (own.sell_quantity if own is not None and own.instrument_hash == instrument_hash else 0)

    def __post_init__(self) -> None:
        if self.group.is_futures:
            raise SimulationContractError("现货账本不能使用保证金组")
        if min(self.available_cash_units, self.frozen_cash_units, self.unsettled_cash_units) < 0:
            raise SimulationContractError("现金 bucket 不能为负")
        if self.unwithdrawable_sale_units < 0:
            raise SimulationContractError("未结算卖出资金不能为负")
        if tuple(key for key, _ in self.withdrawal_reservations) != tuple(sorted({key for key, _ in self.withdrawal_reservations})) or any(type(units) is not int or units <= 0 for _, units in self.withdrawal_reservations):
            raise SimulationContractError("出金预占须按唯一身份排序且金额为正整数")
        if tuple(item.receivable_id for item in self.cash_receivables) != tuple(sorted({item.receivable_id for item in self.cash_receivables})):
            raise SimulationContractError("应收现金身份必须唯一并排序")
        if tuple(item.entitlement_id for item in self.position_entitlements) != tuple(sorted({item.entitlement_id for item in self.position_entitlements})):
            raise SimulationContractError("待上市权益身份必须唯一并排序")
        if tuple(item.payable_id for item in self.cash_payables) != tuple(sorted({item.payable_id for item in self.cash_payables})):
            raise SimulationContractError("应付税款身份必须唯一并排序")
        if tuple(item.entitlement_id for item in self.successor_entitlements) != tuple(sorted({item.entitlement_id for item in self.successor_entitlements})):
            raise SimulationContractError("后继权益身份必须唯一并排序")

    @property
    def total_cash_units(self) -> int:
        return (
            self.available_cash_units
            + self.frozen_cash_units
            + self.unsettled_cash_units
            + self.withdrawal_reserved_units
            + sum(item.cash_units for item in self.cash_receivables)
        )

    @property
    def payable_tax_units(self) -> int:
        return sum(item.cash_units for item in self.cash_payables)

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
            **({"credit_state": self.credit_state.to_dict()} if self.credit_state is not None else {}),
            **({"order_reservations": [item.__dict__ for item in self.order_reservations]} if self.order_reservations else {}),
            **({"withdrawal_reservations": [list(item) for item in self.withdrawal_reservations],
                "cashflow_tracking": True, "unwithdrawable_sale_units": self.unwithdrawable_sale_units} if self.cashflow_tracking or self.withdrawal_reservations or self.unwithdrawable_sale_units else {}),
            "group": self.group.__dict__, "available_cash_units": self.available_cash_units,
            "frozen_cash_units": self.frozen_cash_units, "unsettled_cash_units": self.unsettled_cash_units,
            "positions": [item.__dict__ for item in self.positions],
            "applied_event_count": self.applied_event_count,
            "applied_event_index_hash": _event_index_hash(self._applied_event_index),
            "cash_receivables": [
                {**item.__dict__, "due_date": item.due_date.isoformat()}
                for item in self.cash_receivables
            ],
            **({"successor_entitlements": [item.__dict__ for item in self.successor_entitlements]} if self.successor_entitlements else {}),
            **({"cash_payables": [item.__dict__ for item in self.cash_payables]} if self.cash_payables else {}),
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
    cost_numerator: int | None = None
    cost_denominator: int = 1
    pnl_remainder_numerator: int = 0
    pnl_remainder_denominator: int = 1
    margin_units: int = 0
    opened_today: int = 0
    price_scale: int = 2
    pnl_rounding_policy: str = "toward_zero_with_remainder"
    contract_code: str | None = None

    @property
    def yesterday_contracts(self) -> int:
        return abs(self.contracts) - self.opened_today

    @property
    def cost_price(self) -> Fraction:
        return Fraction(self.cost_numerator, self.cost_denominator)

    def with_fill(
        self, *, contracts_delta: int, price_units: int,
        pnl_remainder: Fraction, margin_units: int,
    ) -> "FuturesPosition":
        remaining = self.contracts + contracts_delta
        if self.contracts * contracts_delta > 0:
            basis = (self.cost_price * abs(self.contracts) + price_units * abs(contracts_delta)) / abs(remaining)
        elif self.contracts * remaining > 0:
            basis = self.cost_price
        else:
            basis = Fraction(price_units)
        return FuturesPosition(
            self.instrument_hash, remaining, int(basis), basis.numerator, basis.denominator,
            pnl_remainder.numerator, pnl_remainder.denominator, margin_units,
            min(self.opened_today, abs(remaining)), self.price_scale,
            self.pnl_rounding_policy, self.contract_code,
        )

    def after_settlement(self, *, price_units: int, multiplier: int, margin_units: int) -> "FuturesPosition":
        _, remainder = self.realize(price_units=price_units, contracts=self.contracts, multiplier=multiplier)
        return FuturesPosition(
            self.instrument_hash, self.contracts, price_units,
            pnl_remainder_numerator=remainder.numerator,
            pnl_remainder_denominator=remainder.denominator,
            margin_units=margin_units, opened_today=self.opened_today,
            price_scale=self.price_scale, pnl_rounding_policy=self.pnl_rounding_policy,
            contract_code=self.contract_code,
        )

    def realize(self, *, price_units: int, contracts: int, multiplier: int) -> tuple[int, Fraction]:
        exact = (price_units - self.cost_price) * contracts * multiplier * Fraction(100, 10 ** self.price_scale)
        if self.pnl_rounding_policy == "half_up_per_event":
            magnitude = abs(exact) + Fraction(1, 2)
            units = magnitude.numerator // magnitude.denominator
            return (units if exact >= 0 else -units), Fraction(0)
        exact += Fraction(self.pnl_remainder_numerator, self.pnl_remainder_denominator)
        units = int(exact)
        return units, exact - units

    def __post_init__(self) -> None:
        if not self.instrument_hash or self.settlement_price_units <= 0:
            raise SimulationContractError("期货持仓身份或结算价无效")
        if self.cost_numerator is None:
            object.__setattr__(self, "cost_numerator", self.settlement_price_units)
        if self.cost_denominator <= 0 or self.pnl_remainder_denominator <= 0 or self.cost_price <= 0 or self.margin_units < 0:
            raise SimulationContractError("期货持仓成本、精度余数或保证金无效")
        if type(self.opened_today) is not int or not 0 <= self.opened_today <= abs(self.contracts):
            raise SimulationContractError("期货今仓数量不能超过总持仓")
        if type(self.price_scale) is not int or self.price_scale < 0:
            raise SimulationContractError("期货价格精度无效")
        if self.pnl_rounding_policy not in {"toward_zero_with_remainder", "half_up_per_event"}:
            raise SimulationContractError("期货盈亏舍入策略无效")


@dataclass(frozen=True)
class FuturesAccountPosting:
    """一次核算的持仓、现金和保证金变化；由账户账本确认入账。"""

    position: FuturesPosition
    realized_pnl_units: int
    cash_delta_units: int
    margin_delta_units: int


class FuturesAccountCore:
    """独立及共享账户共同使用的核算操作，不决定成交与风险执行政策。"""

    @staticmethod
    def fill(
        position: FuturesPosition, *, contracts_delta: int, price_units: int,
        realized_contracts: int, multiplier: int | Fraction | Decimal,
        fee_units: int, margin_units: int, opened_today: int | None = None,
        pnl_remainder: Fraction | None = None,
    ) -> FuturesAccountPosting:
        realized = 0
        remainder = (Fraction(position.pnl_remainder_numerator, position.pnl_remainder_denominator)
                     if pnl_remainder is None else pnl_remainder)
        if realized_contracts:
            realized, remainder = position.realize(
                price_units=price_units, contracts=realized_contracts, multiplier=Fraction(multiplier),
            )
        updated = position.with_fill(
            contracts_delta=contracts_delta, price_units=price_units,
            pnl_remainder=remainder, margin_units=margin_units,
        )
        if opened_today is not None:
            updated = replace(updated, opened_today=opened_today)
        return FuturesAccountPosting(updated, realized, realized - fee_units,
                                     margin_units - position.margin_units)

    @staticmethod
    def settle(
        position: FuturesPosition, *, price_units: int,
        multiplier: int | Fraction | Decimal, margin_units: int,
    ) -> FuturesAccountPosting:
        pnl, remainder = position.realize(
            price_units=price_units, contracts=position.contracts, multiplier=Fraction(multiplier),
        )
        updated = replace(position, settlement_price_units=price_units,
                          cost_numerator=price_units, cost_denominator=1,
                          pnl_remainder_numerator=remainder.numerator,
                          pnl_remainder_denominator=remainder.denominator,
                          margin_units=margin_units)
        return FuturesAccountPosting(updated, pnl, pnl, margin_units - position.margin_units)

    @staticmethod
    def available(*, equity_units: int, margin_units: int, frozen_units: int = 0) -> int:
        """扣除持仓保证金及活动订单预占；是否允许负数由执行政策决定。"""
        return equity_units - margin_units - frozen_units


@dataclass(frozen=True)
class FuturesLedgerState:
    group: ExecutionGroup
    equity_units: int
    margin_units: int = 0
    realized_pnl_units: int = 0
    positions: tuple[FuturesPosition, ...] = ()
    _applied_event_index: _EventIndexNode | None = field(default=None, repr=False)
    trading_date: date | None = None
    order_reservations: tuple[OrderReservation, ...] = ()

    def reservation_for(self, order_id: str) -> OrderReservation | None:
        return _reservation_for(self.order_reservations, order_id)

    @property
    def frozen_cash_units(self) -> int:
        return sum(item.cash_units + item.margin_units for item in self.order_reservations)

    def available_position_quantity(self, instrument_hash: str, *, position_effect: str,
                                    order_id: str | None = None) -> int:
        if position_effect not in {"close_today", "close_yesterday"}:
            raise SimulationContractError("期货预占必须明确今昨仓桶")
        position = next((item for item in self.positions if item.instrument_hash == instrument_hash), None)
        if position is None:
            return 0
        field_name = position_effect + "_quantity"
        total = position.opened_today if position_effect == "close_today" else position.yesterday_contracts
        return total - sum(getattr(item, field_name) for item in self.order_reservations
                           if item.instrument_hash == instrument_hash and item.order_id != order_id)

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
        return FuturesAccountCore.available(equity_units=self.equity_units,
                                            margin_units=self.margin_units,
                                            frozen_units=self.frozen_cash_units)


class FuturesDailySessionLedger:
    """结算检查策略下的唯一工作账本；完整会话结束才发布严格状态。"""

    margin_check_policy = "settlement_margin_check"
    close_bucket_order = ("close_yesterday", "close_today")
    pnl_rounding_policy = "half_up_per_event"

    def __init__(self, state: FuturesLedgerState, trading_date: date) -> None:
        if state.trading_date is not None and trading_date <= state.trading_date:
            raise SimulationContractError("期货日频会话必须严格递增")
        self.group = state.group
        self.trading_date = trading_date
        self.equity_units = state.equity_units
        self.margin_units = state.margin_units
        self.realized_pnl_units = state.realized_pnl_units
        self.positions = {
            item.instrument_hash: replace(item, opened_today=0)
            for item in state.positions if item.contracts
        }
        self._applied_event_index = state._applied_event_index
        if state.order_reservations:
            raise SimulationContractError("新交易会话开始前必须释放上一会话 DAY 预占")
        self.order_reservations = state.order_reservations

    def apply_reservation(self, event: FinancialEvent) -> None:
        """接收同一金融事件信封，预占归属只读取 payload.order_id。"""
        if event.kind not in {"cash_reserved", "position_reserved"}:
            raise SimulationContractError("apply_reservation 仅接受预占或释放事件")
        updated = reduce_futures(self.publish(), event)
        self.order_reservations = updated.order_reservations
        self._applied_event_index = updated._applied_event_index

    def apply_event(self, event: FinancialEvent) -> int:
        """按同一事件信封确认日频预占或成交，返回本笔已实现盈亏。"""
        if event.group_id != self.group.group_id or event.session != self.trading_date.isoformat():
            raise SimulationContractError("事件与期货工作账本组或会话不一致")
        if event.kind in {"cash_reserved", "position_reserved"}:
            self.apply_reservation(event)
            return 0
        if event.kind != "fill":
            raise SimulationContractError("工作账本事件必须是预占、释放或成交")
        values = event.values()
        order_id = _order_id(values)
        if order_id is None:
            raise SimulationContractError("显式期货成交必须提供 order_id")
        delta = values["contracts_delta"]
        require_integer_quantity(abs(delta), "contracts_delta")
        return self.apply_fill(
            instrument_hash=str(values["instrument_hash"]), contract_code=str(values["contract_code"]),
            side="buy" if delta > 0 else "sell", quantity=abs(delta),
            position_effect=str(values["position_effect"]),
            price=Decimal(_positive(values, "settlement_price_units")).scaleb(-int(values.get("price_scale", 2))),
            multiplier=_positive(values, "multiplier"), fee_units=_nonnegative(values, "fee_units"),
            order_id=order_id, event_id=event.event_id,
            position_margin_units=_nonnegative(values, "position_margin_units") if "position_margin_units" in values else None,
        )

    @property
    def position(self) -> FuturesPosition | None:
        active = [item for item in self.positions.values() if item.contracts]
        if len(active) > 1:
            raise SimulationContractError("单品种日频账本必须先平旧合约再开新合约")
        return active[0] if active else None

    def _priced_position(self, *, instrument_hash: str, contract_code: str,
                         price: Decimal) -> tuple[FuturesPosition, int]:
        if not price.is_finite() or price <= 0:
            raise SimulationContractError("期货账本成交或结算价格必须为正数")
        old = self.positions.get(instrument_hash)
        scale = max(2, -price.as_tuple().exponent, 0 if old is None else old.price_scale)
        price_units = int(Fraction(price) * 10 ** scale)
        if old is None:
            old = FuturesPosition(
                instrument_hash, 0, price_units, price_scale=scale,
                pnl_rounding_policy=self.pnl_rounding_policy, contract_code=contract_code,
            )
        elif scale != old.price_scale:
            factor = 10 ** (scale - old.price_scale)
            basis = old.cost_price * factor
            old = replace(old, settlement_price_units=old.settlement_price_units * factor,
                          cost_numerator=basis.numerator, cost_denominator=basis.denominator,
                          price_scale=scale)
        return old, price_units

    def apply_fill(self, *, instrument_hash: str, contract_code: str, side: str,
                   quantity: int, position_effect: str, price: Decimal,
                   multiplier: int, fee_units: int, order_id: str | None = None,
                   event_id: str | None = None, position_margin_units: int | None = None) -> int:
        if order_id is not None:
            if not event_id:
                raise SimulationContractError("显式订单成交必须提供唯一 event_id")
            if _event_index_contains(self._applied_event_index, event_id):
                return 0
        elif self.order_reservations:
            raise SimulationContractError("存在活动预占时日频成交必须提供 order_id")
        require_integer_quantity(quantity)
        require_integer_quantity(multiplier, "multiplier")
        require_integer_quantity(fee_units, "fee_units", allow_zero=True)
        if side not in {"buy", "sell"}:
            raise SimulationContractError("期货成交方向无效")
        old, price_units = self._priced_position(
            instrument_hash=instrument_hash, contract_code=contract_code, price=price,
        )
        signed = quantity if side == "buy" else -quantity
        today = old.opened_today
        realized_contracts = 0
        margin = old.margin_units
        if position_effect == "open":
            active = self.position
            if active is not None and active.instrument_hash != instrument_hash:
                raise SimulationContractError("换月必须先平旧合约再开新合约")
            if old.contracts and old.contracts * signed < 0:
                raise SimulationContractError("open 不能隐式平掉反向持仓")
            today += quantity
        elif position_effect in {"close", "close_yesterday", "close_today"}:
            if not old.contracts or old.contracts * signed > 0:
                raise SimulationContractError("平仓方向或持仓无效")
            if quantity > abs(old.contracts):
                raise SimulationContractError("平仓数量超过持仓")
            if position_effect == "close_today":
                if quantity > today:
                    raise SimulationContractError("close_today 只能消费同交易日新仓")
                today -= quantity
            elif position_effect == "close_yesterday":
                if quantity > old.yesterday_contracts:
                    raise SimulationContractError("close_yesterday 不能消费同交易日新仓")
            else:
                today -= max(0, quantity - old.yesterday_contracts)
            realized_contracts = quantity if old.contracts > 0 else -quantity
            margin = old.margin_units * (abs(old.contracts) - quantity) // abs(old.contracts)
        else:
            raise SimulationContractError("期货 position_effect 不受支持")
        if order_id is not None and position_margin_units is not None:
            require_integer_quantity(position_margin_units, "position_margin_units", allow_zero=True)
            margin = position_margin_units
        posting = FuturesAccountCore.fill(
            old, contracts_delta=signed, price_units=price_units,
            realized_contracts=realized_contracts, multiplier=multiplier,
            fee_units=fee_units, margin_units=margin, opened_today=today,
        )
        reservations = self.order_reservations
        if order_id is not None:
            own = _reservation_for(reservations, order_id)
            if own is not None and own.instrument_hash not in {None, instrument_hash}:
                raise SimulationContractError("成交与订单预占标的不一致")
            if position_effect != "open":
                if position_effect not in {"close_today", "close_yesterday"}:
                    raise SimulationContractError("预占平仓必须明确今昨仓桶")
                field_name = position_effect + "_quantity"
                total = old.opened_today if position_effect == "close_today" else old.yesterday_contracts
                other = sum(getattr(item, field_name) for item in reservations
                            if item.instrument_hash == instrument_hash and item.order_id != order_id)
                if quantity > total - other:
                    raise SimulationContractError("期货成交超过本单可平桶")
                if own is not None:
                    own = replace(own, **{field_name: max(0, getattr(own, field_name) - quantity)})
            if position_effect == "open" and position_margin_units is None:
                raise SimulationContractError("预占开仓必须提供成交后持仓保证金")
            if own is not None:
                own = replace(own, cash_units=max(0, own.cash_units - fee_units),
                              margin_units=max(0, own.margin_units - max(0, margin - old.margin_units)))
                reservations = _store_reservation(reservations, own)
            remaining_free = FuturesAccountCore.available(
                equity_units=self.equity_units + posting.cash_delta_units,
                margin_units=self.margin_units + posting.margin_delta_units,
                frozen_units=sum(item.cash_units + item.margin_units for item in reservations),
            )
            if remaining_free < 0:
                raise SimulationContractError("期货成交不能占用其他订单预占")
        self.positions[instrument_hash] = posting.position
        self.order_reservations = reservations
        if order_id is not None:
            self._applied_event_index = _event_index_add(self._applied_event_index, event_id)
        self.equity_units += posting.cash_delta_units
        self.realized_pnl_units += posting.cash_delta_units
        self.margin_units += posting.margin_delta_units
        return posting.realized_pnl_units

    def settle(self, *, price: Decimal, multiplier: int, margin_units: int) -> int:
        require_integer_quantity(multiplier, "multiplier")
        require_integer_quantity(margin_units, "margin_units", allow_zero=True)
        active = self.position
        pnl = 0
        if active is not None:
            old, price_units = self._priced_position(
                instrument_hash=active.instrument_hash, contract_code=active.contract_code,
                price=price,
            )
            posting = FuturesAccountCore.settle(
                old, price_units=price_units, multiplier=multiplier, margin_units=margin_units,
            )
            pnl = posting.cash_delta_units
            self.positions[old.instrument_hash] = posting.position
        self.equity_units += pnl
        self.realized_pnl_units += pnl
        self.margin_units = margin_units
        return pnl

    def publish(self) -> FuturesLedgerState:
        return FuturesLedgerState(
            self.group, self.equity_units, self.margin_units, self.realized_pnl_units,
            tuple(sorted(self.positions.values(), key=lambda item: item.instrument_hash)),
            self._applied_event_index, self.trading_date, self.order_reservations,
        )


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
    payables = {item.payable_id: item for item in state.cash_payables}
    successors = {item.entitlement_id: item for item in state.successor_entitlements}
    reservations = state.order_reservations
    withdrawals = dict(state.withdrawal_reservations)
    sale_units = state.unwithdrawable_sale_units
    order_id = _order_id(values)
    own = state.reservation_for(order_id) if order_id is not None else None
    credit = state.credit_state
    if credit is not None:
        from .credit_account import reduce_credit_event
        credit = reduce_credit_event(credit, event)
    if event.kind.startswith("credit_"):
        if credit is None:
            raise SimulationContractError("普通现金账户不能应用信用事件")
        if event.kind == "credit_repayment":
            paid = _nonnegative(values, "cash_units")
            if paid > max(0, available - sale_units):
                raise SimulationContractError("直接还款可用已结算自有现金不足")
            available -= paid
        elif event.kind == "credit_sale_settled":
            paid = _nonnegative(values, "cash_units")
            if values.get("cash_already_settled", False):
                if paid > available:
                    raise SimulationContractError("公司行动还款超过本批次到账资金")
                available -= paid
            else:
                claim = receivables.get(values["claim_id"])
                if claim is None or claim.due_date > event.effective_time.date():
                    raise SimulationContractError("还债受限卖款不存在或尚未清算")
                if paid > claim.cash_units:
                    raise SimulationContractError("卖券还款超过已到账价款")
                available += claim.cash_units-paid
                del receivables[claim.receivable_id]
    elif event.kind == "external_cashflow_reserved":
        identity = str(values["cashflow_id"])
        units = _positive(values, "cash_units")
        if identity in withdrawals or units > state.withdrawable_cash_units():
            raise SimulationContractError("出金申请可提款现金不足或身份重复")
        available -= units
        withdrawals[identity] = units
    elif event.kind == "external_cashflow":
        identity = str(values["cashflow_id"])
        units = _positive(values, "cash_units")
        status, direction = str(values["status"]), str(values["direction"])
        if status not in {"settled", "cancelled", "failed"} or direction not in {"deposit", "withdrawal"}:
            raise SimulationContractError("外部资金流终态或方向无效")
        reserved = withdrawals.get(identity, 0)
        if direction == "deposit":
            if reserved:
                raise SimulationContractError("入金不能携带出金预占")
            if status == "settled":
                available += units
        elif status == "settled":
            if reserved != units or units > state.withdrawable_cash_units(identity):
                raise SimulationContractError("出金生效时现金、结算或应付约束不满足")
            del withdrawals[identity]
        elif reserved:
            if reserved != units:
                raise SimulationContractError("出金释放金额与本笔预占不一致")
            available += withdrawals.pop(identity)
    elif event.kind == "external_cashflow_settlement":
        sale_units = 0
    elif event.kind in {"cash_reserved", "position_reserved"} and order_id is not None:
        own = own or OrderReservation(order_id)
        if values.get("action") == "release":
            available += own.cash_units
            frozen -= own.cash_units
            if own.sell_quantity:
                lot = positions[own.instrument_hash]
                positions[own.instrument_hash] = replace(lot, sellable=lot.sellable + own.sell_quantity,
                                                        frozen=lot.frozen - own.sell_quantity)
            reservations = _store_reservation(reservations, OrderReservation(order_id))
        elif event.kind == "cash_reserved":
            units = _nonnegative(values, "cash_units")
            delta = units - own.cash_units
            if delta > available:
                raise SimulationContractError("可用现金不足")
            available -= delta
            frozen += delta
            reservations = _store_reservation(reservations, replace(own, cash_units=units))
        else:
            key, quantity = str(values["instrument_hash"]), _nonnegative(values, "quantity")
            if own.instrument_hash not in {None, key}:
                raise SimulationContractError("订单预占不能更换标的")
            lot = positions.get(key, PositionLot(key))
            delta = quantity - own.sell_quantity
            if delta > lot.sellable:
                raise SimulationContractError("可卖数量不足")
            positions[key] = replace(lot, sellable=lot.sellable - delta, frozen=lot.frozen + delta)
            reservations = _store_reservation(reservations, replace(own, instrument_hash=key, sell_quantity=quantity))
    elif event.kind == "cash_reserved":
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
        if own is not None and own.instrument_hash not in {None, key}:
            raise SimulationContractError("成交与订单预占标的不一致")
        if side == "buy":
            borrowed = 0 if values.get("credit_drawdown") is None else int(values["credit_drawdown"]["principal_units"])
            debit = notional + fee - borrowed
            if state.cashflow_tracking:
                sale_units = max(0, sale_units - debit)
            usable_frozen = (own.cash_units if own is not None else 0) if order_id is not None else frozen - sum(item.cash_units for item in reservations)
            from_frozen = min(usable_frozen, debit)
            frozen -= from_frozen
            available -= debit - from_frozen
            if available < 0:
                raise SimulationContractError("成交后现金为负")
            if own is not None:
                reservations = _store_reservation(reservations, replace(own, cash_units=own.cash_units - from_frozen))
            positions[key] = replace(lot, unsettled=lot.unsettled + quantity)
        elif side == "sell":
            usable_frozen = (own.sell_quantity if own is not None else 0) if order_id is not None else lot.frozen - sum(item.sell_quantity for item in reservations if item.instrument_hash == key)
            if quantity > usable_frozen + lot.sellable:
                raise SimulationContractError("卖出数量超过本单冻结/可卖持仓")
            use_frozen = min(quantity, usable_frozen)
            positions[key] = replace(lot, frozen=lot.frozen - use_frozen, sellable=lot.sellable - (quantity - use_frozen))
            from_frozen_cash = min(own.cash_units, fee) if own is not None else 0
            if own is not None:
                reservations = _store_reservation(reservations, replace(
                    own, sell_quantity=own.sell_quantity - use_frozen,
                    cash_units=own.cash_units - from_frozen_cash,
                ))
            frozen -= from_frozen_cash
            # A 股卖出资金当日可继续交易；本单费用预占可补足低价零股的最低佣金。
            restricted = 0 if values.get("credit_sale") is None else int(values["credit_sale"]["cash_units"])
            available += notional - fee + from_frozen_cash - restricted
            if values.get("credit_sale") is not None:
                sale = values["credit_sale"]
                receivables[sale["claim_id"]] = CashReceivable(sale["claim_id"], date.fromisoformat(sale["due_date"]), restricted)
            if state.cashflow_tracking:
                sale_units = max(0, sale_units + notional - fee - restricted)
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
    elif event.kind == "security_conversion":
        key = str(values["old_instrument_hash"])
        lot = positions.get(key, PositionLot(key))
        if lot.sellable + lot.unsettled + lot.frozen != _nonnegative(values, "old_quantity"):
            raise SimulationContractError("证券注销数量与账本持仓不符")
        if lot.frozen:
            raise SimulationContractError("证券注销前必须解除活动订单预占")
        positions[key] = PositionLot(key)
        entitlements = {key_: item for key_, item in entitlements.items() if item.instrument_hash != key}
        quantity = _nonnegative(values, "successor_quantity")
        identity = str(values["successor_entitlement_id"])
        if identity in successors:
            raise SimulationContractError("后继权益身份重复")
        if quantity:
            successors[identity] = SuccessorEntitlement(identity, str(values["new_instrument_hash"]), quantity)
        cash_units = _nonnegative(values, "cash_receivable_units")
        if cash_units:
            claim = str(values["receivable_id"])
            if claim in receivables:
                raise SimulationContractError("换股现金应收身份重复")
            receivables[claim] = CashReceivable(claim, date.fromisoformat(str(values["cash_due_date"])), cash_units)
    elif event.kind == "successor_registered":
        identity = str(values["entitlement_id"])
        right = successors.get(identity)
        if right is None or right.instrument_hash != values["instrument_hash"] or right.quantity != values["quantity"]:
            raise SimulationContractError("后继登记与待登记权益不符")
        lot = positions.get(right.instrument_hash, PositionLot(right.instrument_hash))
        positions[right.instrument_hash] = replace(lot, unsettled=lot.unsettled + right.quantity)
        del successors[identity]
    elif event.kind == "tax_assessed":
        assessment_id = str(values["assessment_id"])
        if assessment_id in payables:
            raise SimulationContractError("税款核定身份重复")
        payables[assessment_id] = CashPayable(assessment_id, _positive(values, "tax_units"))
    elif event.kind == "tax_collected":
        assessment_id = str(values["assessment_id"])
        units = _positive(values, "cash_units")
        payable = payables.get(assessment_id)
        if payable is None or units > payable.cash_units:
            raise SimulationContractError("税款扣收超过已核定未付金额")
        if units > available:
            raise SimulationContractError("税款扣收可用现金不足")
        available -= units
        if state.cashflow_tracking:
            sale_units = max(0, sale_units - units)
        remainder = payable.cash_units - units
        if remainder:
            payables[assessment_id] = CashPayable(assessment_id, remainder)
        else:
            del payables[assessment_id]
    elif event.kind == "corporate_action":
        cash_delta = int(values.get("cash_delta_units", 0))
        available += cash_delta
        if state.cashflow_tracking and cash_delta < 0:
            sale_units = max(0, sale_units + cash_delta)
        key = str(values["instrument_hash"])
        if values.get("cancel_position_entitlements"):
            entitlements = {identity: item for identity, item in entitlements.items() if item.instrument_hash != key}
        lot = positions.get(key, PositionLot(key))
        position_receivable = int(values.get("position_entitlement_quantity", 0))
        positions[key] = replace(
            lot,
            sellable=lot.sellable + int(values.get("sellable_delta", 0)),
            unsettled=lot.unsettled + int(values.get("unsettled_delta", 0)) + position_receivable,
            frozen=lot.frozen + int(values.get("frozen_delta", 0)),
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
        cash_payables=tuple(sorted(payables.values(), key=lambda item: item.payable_id)),
        successor_entitlements=tuple(sorted(successors.values(), key=lambda item: item.entitlement_id)),
        order_reservations=reservations,
        withdrawal_reservations=tuple(sorted(withdrawals.items())),
        cashflow_tracking=state.cashflow_tracking,
        unwithdrawable_sale_units=sale_units,
        credit_state=credit,
        _applied_event_index=event_index,
    )


def reduce_futures(
    state: FuturesLedgerState, event: FinancialEvent,
    *, settlement_posting: FuturesAccountPosting | None = None,
) -> FuturesLedgerState:
    if event.group_id != state.group.group_id:
        raise SimulationContractError("事件与 execution group 不一致")
    if _event_index_contains(state._applied_event_index, event.event_id):
        return state
    event_index = _event_index_add(state._applied_event_index, event.event_id)
    values = event.values()
    equity, margin, realized = state.equity_units, state.margin_units, state.realized_pnl_units
    positions = {item.instrument_hash: item for item in state.positions}
    reservations = state.order_reservations
    order_id = _order_id(values)
    own = state.reservation_for(order_id) if order_id is not None else None
    if event.kind in {"cash_reserved", "position_reserved"}:
        if order_id is None:
            raise SimulationContractError("期货预占必须提供 order_id")
        own = own or OrderReservation(order_id)
        if values.get("action") == "release":
            updated = OrderReservation(order_id)
        elif event.kind == "cash_reserved":
            updated = replace(own, cash_units=_nonnegative(values, "cash_units"),
                              margin_units=_nonnegative(values, "margin_units"))
        else:
            key = str(values["instrument_hash"])
            effect = str(values["position_effect"])
            if own.instrument_hash not in {None, key}:
                raise SimulationContractError("订单预占不能更换标的")
            quantity = _nonnegative(values, "quantity")
            if quantity > state.available_position_quantity(key, position_effect=effect, order_id=order_id):
                raise SimulationContractError("期货可平桶数量不足")
            updated = replace(own, instrument_hash=key, **{effect + "_quantity": quantity})
        reservations = _store_reservation(reservations, updated)
        if FuturesAccountCore.available(
            equity_units=equity, margin_units=margin,
            frozen_units=sum(item.cash_units + item.margin_units for item in reservations),
        ) < 0:
            raise SimulationContractError("期货可用权益不足")
    elif event.kind == "fill":
        key = str(values["instrument_hash"])
        if order_id is not None or reservations:
            delta = values["contracts_delta"]
            require_integer_quantity(abs(delta), "contracts_delta")
            if type(delta) is not int:
                raise SimulationContractError("期货成交数量必须为整数")
            if "position_margin_units" not in values:
                raise SimulationContractError("预占成交必须提供成交后持仓保证金")
            _nonnegative(values, "position_margin_units")
        contracts = int(values["contracts_delta"])
        fee = _nonnegative(values, "fee_units")
        price = _positive(values, "settlement_price_units")
        old = positions.get(key, FuturesPosition(key, 0, price))
        effect = values.get("position_effect")
        today = old.opened_today
        realized_contracts = 0
        multiplier = 1
        remainder = Fraction(
            int(values.get("pnl_remainder_numerator", old.pnl_remainder_numerator)),
            int(values.get("pnl_remainder_denominator", old.pnl_remainder_denominator)),
        )
        if own is not None and own.instrument_hash not in {None, key}:
            raise SimulationContractError("成交与订单预占标的不一致")
        if effect == "open":
            if old.contracts * contracts < 0:
                raise SimulationContractError("open 不能隐式平掉反向持仓")
            today += abs(contracts)
        elif effect in {"close_today", "close_yesterday"}:
            if not old.contracts or old.contracts * contracts >= 0:
                raise SimulationContractError("平仓方向或持仓无效")
            if abs(contracts) > state.available_position_quantity(key, position_effect=effect, order_id=order_id):
                raise SimulationContractError("期货成交超过本单可平桶")
            if effect == "close_today":
                today -= abs(contracts)
            realized_contracts = -contracts
            multiplier = _positive(values, "multiplier")
            if own is not None:
                field_name = effect + "_quantity"
                own = replace(own, **{field_name: max(0, getattr(own, field_name) - abs(contracts))})
        elif order_id is not None or reservations:
            raise SimulationContractError("预占期货成交必须明确 open/close_today/close_yesterday")
        position_margin = int(values.get("position_margin_units", old.margin_units))
        if order_id is not None or reservations:
            margin += position_margin - old.margin_units
            if own is not None:
                own = replace(own, cash_units=max(0, own.cash_units - fee),
                              margin_units=max(0, own.margin_units - max(0, position_margin - old.margin_units)))
                reservations = _store_reservation(reservations, own)
        posting = FuturesAccountCore.fill(
            old, contracts_delta=contracts, price_units=price,
            realized_contracts=realized_contracts, multiplier=multiplier,
            fee_units=fee, margin_units=position_margin,
            opened_today=today if effect is not None else None, pnl_remainder=remainder,
        )
        positions[key] = posting.position
        equity += posting.cash_delta_units
        realized += posting.cash_delta_units
        if (order_id is not None or reservations) and FuturesAccountCore.available(
            equity_units=equity, margin_units=margin,
            frozen_units=sum(item.cash_units + item.margin_units for item in reservations),
        ) < 0:
            raise SimulationContractError("期货成交不能占用其他订单预占")
    elif event.kind == "mark_to_market":
        margin = _nonnegative(values, "required_margin_units")
        if settlement_posting is None:
            pnl = int(values["pnl_units"])
            equity += pnl
            realized += pnl
        else:
            # 会话结算的基价与余数随同一事件确认；保证金仍按全账户聚合值入账。
            positions[settlement_posting.position.instrument_hash] = settlement_posting.position
            equity += settlement_posting.cash_delta_units
            realized += settlement_posting.realized_pnl_units
    elif event.kind in {"margin_call", "forced_liquidation"}:
        margin = _nonnegative(values, "required_margin_units")
    else:
        raise SimulationContractError("事件不能应用到期货账本")
    return FuturesLedgerState(
        group=state.group, equity_units=equity, margin_units=margin,
        realized_pnl_units=realized,
        positions=tuple(sorted(positions.values(), key=lambda item: item.instrument_hash)),
        _applied_event_index=event_index, trading_date=state.trading_date,
        order_reservations=reservations,
    )


def spot_order_execution_view(state: SpotLedgerState, order_id: str) -> SpotLedgerState:
    """只读撮合视图：本单预占恢复为可用，他单预占仍被冻结。"""
    _order_id({"order_id": order_id})
    own = state.reservation_for(order_id)
    if own is None:
        return state
    positions = tuple(
        replace(item, sellable=item.sellable + own.sell_quantity,
                frozen=item.frozen - own.sell_quantity)
        if item.instrument_hash == own.instrument_hash else item
        for item in state.positions
    )
    return replace(state, available_cash_units=state.available_cash_units + own.cash_units,
                   frozen_cash_units=state.frozen_cash_units - own.cash_units,
                   positions=positions,
                   order_reservations=_store_reservation(state.order_reservations, OrderReservation(order_id)))


@dataclass(frozen=True)
class FuturesOrderExecutionView:
    """只计算本单购买力，不持有第二份资金或仓位状态。"""

    state: FuturesLedgerState
    order_id: str

    @property
    def group(self) -> ExecutionGroup:
        return self.state.group

    @property
    def equity_units(self) -> int:
        others = sum(item.cash_units + item.margin_units for item in self.state.order_reservations
                     if item.order_id != self.order_id)
        return self.state.equity_units - others

    @property
    def margin_units(self) -> int:
        return self.state.margin_units

    @property
    def positions(self) -> tuple[FuturesPosition, ...]:
        return self.state.positions

    @property
    def free_equity_units(self) -> int:
        return self.equity_units - self.margin_units

    def available_position_quantity(self, instrument_hash: str, *, position_effect: str) -> int:
        return self.state.available_position_quantity(instrument_hash, position_effect=position_effect,
                                                      order_id=self.order_id)


def futures_order_execution_view(state: FuturesLedgerState, order_id: str) -> FuturesOrderExecutionView:
    """本单可用权益视图；保留真实持仓用于损益与保证金试算。

    平仓数量先由 available_position_quantity 按本单及今昨桶限定。
    此视图不得用于成交确认、结算或发布。
    """
    _order_id({"order_id": order_id})
    return FuturesOrderExecutionView(state, order_id)


def _positive(values: dict[str, object], field: str) -> int:
    return require_integer_quantity(values[field], field)


def _nonnegative(values: dict[str, object], field: str) -> int:
    return require_integer_quantity(values.get(field, 0), field, allow_zero=True)


__all__ = ["spot_order_execution_view", "futures_order_execution_view", "FuturesOrderExecutionView", "OrderReservation", "CashReceivable", "ExecutionGroup", "FuturesLedgerState", "FuturesPosition", "FuturesDailySessionLedger", "PositionEntitlement", "PositionLot", "SpotLedgerState", "reduce_futures", "reduce_spot"]
