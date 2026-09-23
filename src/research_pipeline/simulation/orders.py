"""订单合同与唯一生命周期状态机。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from research_pipeline.domain.models import InstrumentId
from research_pipeline.domain.trading import InstrumentKey
from research_pipeline.domain import Price, require_integer_quantity
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError


class SimulationContractError(MainlineError):
    """金融仿真合同或状态转换不成立。"""

    error_code = "simulation_contract_invalid"


ORDER_TERMINAL_STATES = frozenset({"filled", "rejected", "cancelled", "expired"})


@dataclass(frozen=True)
class Order:
    order_id: str
    instrument: InstrumentKey | InstrumentId
    side: str
    quantity: int
    order_type: str
    time_in_force: str
    submitted_at: datetime
    position_effect: str = "auto"
    intent_hash: str | None = None
    limit_price: Price | None = None
    status: str = "created"
    filled_quantity: int = 0
    rejection_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.order_id, str) or not self.order_id.strip():
            raise SimulationContractError("order_id 不能为空")
        if not isinstance(self.instrument, (InstrumentKey, InstrumentId)):
            raise SimulationContractError("订单 instrument 合同不受支持")
        if self.side not in {"buy", "sell"}:
            raise SimulationContractError("side 只能是 buy/sell")
        if self.position_effect not in {"auto", "open", "close", "close_today", "close_yesterday"}:
            raise SimulationContractError("position_effect 不受支持")
        if isinstance(self.instrument, InstrumentKey):
            if self.instrument.asset_class == "cn_future" and self.position_effect == "auto":
                raise SimulationContractError("期货订单必须显式声明 position_effect")
            if self.instrument.asset_class != "cn_future" and self.position_effect != "auto":
                raise SimulationContractError("股票和 ETF 订单 position_effect 只能是 auto")
            _require_hash(self.intent_hash, "intent_hash")
        elif self.position_effect != "auto" or self.intent_hash is not None:
            raise SimulationContractError("旧 InstrumentId 订单不能声明新写 intent 字段")
        require_integer_quantity(self.quantity)
        require_integer_quantity(self.filled_quantity, "filled_quantity", allow_zero=True)
        if self.filled_quantity > self.quantity:
            raise SimulationContractError("filled_quantity 超过订单数量")
        if self.order_type not in {"market", "limit"}:
            raise SimulationContractError("首版只支持 market/limit")
        if self.order_type == "limit" and self.limit_price is None:
            raise SimulationContractError("limit 订单必须提供限价")
        if self.order_type == "market" and self.limit_price is not None:
            raise SimulationContractError("market 订单不能提供限价")
        if self.time_in_force not in {"DAY", "IOC"}:
            raise SimulationContractError("首版 TIF 只支持 DAY/IOC")
        require_aware_datetime(self.submitted_at, "submitted_at")
        if self.status not in {"created", "submitted", "accepted", "partially_filled", *ORDER_TERMINAL_STATES}:
            raise SimulationContractError("订单状态不受支持")

    @property
    def order_hash(self) -> str:
        payload = {
            "order_id": self.order_id,
            "instrument": self.instrument.to_dict(),
            "side": self.side,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "submitted_at": self.submitted_at.isoformat(),
            "limit_price": None if self.limit_price is None else self.limit_price.to_dict(),
            "status": self.status,
            "filled_quantity": self.filled_quantity,
            "rejection_code": self.rejection_code,
        }
        if isinstance(self.instrument, InstrumentKey):
            payload["position_effect"] = self.position_effect
            payload["intent_hash"] = self.intent_hash
        return typed_canonical_hash(payload)


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SimulationContractError(f"{field} 必须是小写 sha256")
    return value


def transition_order(order: Order, *, action: str, fill_quantity: int = 0, rejection_code: str | None = None) -> Order:
    if order.status in ORDER_TERMINAL_STATES:
        raise SimulationContractError("终态订单不能再次转换")
    if action == "submit" and order.status == "created":
        return replace(order, status="submitted")
    if action == "accept" and order.status == "submitted":
        return replace(order, status="accepted")
    if action == "reject" and order.status == "submitted" and rejection_code:
        return replace(order, status="rejected", rejection_code=rejection_code)
    if action == "fill" and order.status in {"accepted", "partially_filled"}:
        quantity = require_integer_quantity(fill_quantity, "fill_quantity")
        total = order.filled_quantity + quantity
        if total > order.quantity:
            raise SimulationContractError("累计成交量超过订单量")
        return replace(order, status="filled" if total == order.quantity else "partially_filled", filled_quantity=total)
    if action in {"cancel", "expire"} and order.status in {"submitted", "accepted", "partially_filled"}:
        return replace(order, status="cancelled" if action == "cancel" else "expired")
    raise SimulationContractError(f"非法订单转换: {order.status}->{action}")


__all__ = ["ORDER_TERMINAL_STATES", "Order", "SimulationContractError", "transition_order"]
