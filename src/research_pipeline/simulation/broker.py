"""订单状态和逐会话生命周期事实；资金只由账本持有。"""
from __future__ import annotations

from datetime import date, datetime

from research_pipeline.domain import Price
from research_pipeline.domain.models import InstrumentId
from research_pipeline.domain.trading import InstrumentKey
from research_pipeline.domain.order_stream import ExplicitOrderCommand

from .orders import ORDER_TERMINAL_STATES, Order, SimulationContractError, transition_order


class Broker:
    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}
        self._sessions: dict[str, date] = {}
        self._sequences: dict[str, int] = {}
        self._rows: list[dict[str, object]] = []

    def submit(self, order: Order, session: date) -> None:
        if order.order_id in self.orders or order.status != "created":
            raise SimulationContractError("订单必须以唯一 created 状态提交")
        self.orders[order.order_id] = order
        self._sessions[order.order_id] = session
        self._sequences[order.order_id] = 0
        self.advance(order.order_id, "submit", order.submitted_at)

    def submit_command(self, command: ExplicitOrderCommand, session: date) -> Order:
        """从已校验的显式提交命令建立唯一订单。"""
        if command.action != "submit":
            raise SimulationContractError("创建订单必须使用 submit 命令")
        return self.submit_fields(
            order_id=command.order_id, instrument=command.instrument,
            side=command.side, quantity=command.quantity, order_type=command.order_type,
            time_in_force=command.time_in_force, submitted_at=command.submitted_at,
            session=session, position_effect=command.position_effect,
            limit_price=command.limit_price, intent_hash=command.command_hash,
        )

    def submit_fields(
        self, *, order_id: str, instrument: InstrumentKey | InstrumentId,
        side: str, quantity: int, order_type: str, time_in_force: str,
        submitted_at: datetime, session: date, position_effect: str = "auto",
        limit_price: Price | None = None, intent_hash: str | None = None,
    ) -> Order:
        """已准入的不同命令合同共用订单创建和提交。"""
        order = Order(
            order_id, instrument, side, quantity, order_type, time_in_force,
            submitted_at, position_effect=position_effect, limit_price=limit_price,
            intent_hash=intent_hash,
        )
        self.submit(order, session)
        return order

    def advance(self, order_id: str, action: str, at: datetime, *,
                quantity: int = 0, reason: str | None = None) -> Order:
        previous = self.orders[order_id]
        current = transition_order(previous, action=action, fill_quantity=quantity,
                                   rejection_code=reason)
        self._sequences[order_id] += 1
        self._rows.append({
            "portfolio_id": "default", "order_id": order_id,
            "trading_session": self._sessions[order_id],
            "time_in_force": current.time_in_force, "order_type": current.order_type,
            "sequence": self._sequences[order_id], "event_time": at,
            "from_state": previous.status, "to_state": current.status,
            "cumulative_filled_quantity": current.filled_quantity,
            "remaining_quantity": current.quantity - current.filled_quantity,
            "reason": reason,
        })
        self.orders[order_id] = current
        return current

    def close_session(self, session: date, at: datetime) -> None:
        for order_id, order in tuple(self.orders.items()):
            if self._sessions[order_id] != session or order.status in ORDER_TERMINAL_STATES:
                continue
            if order.time_in_force == "IOC":
                raise SimulationContractError("IOC 订单必须在执行事件内结束")
            self.advance(order_id, "expire", at, reason="session_end")

    def drain_lifecycle(self) -> tuple[dict[str, object], ...]:
        rows = tuple(self._rows)
        self._rows.clear()
        for order_id, order in tuple(self.orders.items()):
            if order.status in ORDER_TERMINAL_STATES:
                del self.orders[order_id]
                del self._sessions[order_id]
                del self._sequences[order_id]
        return rows
