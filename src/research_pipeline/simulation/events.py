"""不可变金融事件信封。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform.canonical import typed_canonical_hash

from .orders import SimulationContractError


FINANCIAL_EVENT_KINDS = frozenset({
    "cash_reserved", "position_reserved", "fill", "settlement", "mark_to_market",
    "corporate_action", "margin_call", "forced_liquidation", "tax_assessed", "tax_collected", "security_conversion", "successor_registered",
    "credit_reserved", "credit_interest", "credit_repayment", "credit_sale_settled", "credit_extension", "credit_risk",
    "external_cashflow_reserved", "external_cashflow", "external_cashflow_settlement",
})


@dataclass(frozen=True)
class FinancialEvent:
    event_id: str
    kind: str
    effective_time: datetime
    session: str
    group_id: str
    rule_hash: str
    payload: tuple[tuple[str, object], ...]
    parent_id: str | None = None

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.group_id.strip() or not self.session.strip():
            raise SimulationContractError("金融事件身份字段不能为空")
        if self.kind not in FINANCIAL_EVENT_KINDS:
            raise SimulationContractError("金融事件 kind 不受支持")
        require_aware_datetime(self.effective_time, "effective_time")
        if len(self.rule_hash) != 64:
            raise SimulationContractError("rule_hash 必须是 sha256")
        if self.payload != tuple(sorted(self.payload, key=lambda item: item[0])):
            raise SimulationContractError("事件 payload 必须按字段名排序")
        if len({key for key, _ in self.payload}) != len(self.payload):
            raise SimulationContractError("事件 payload 字段不能重复")

    @property
    def event_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "effective_time": self.effective_time.isoformat(),
            "session": self.session,
            "group_id": self.group_id,
            "rule_hash": self.rule_hash,
            "payload": [[key, value] for key, value in self.payload],
            "parent_id": self.parent_id,
        }

    def values(self) -> dict[str, object]:
        return dict(self.payload)


__all__ = ["FINANCIAL_EVENT_KINDS", "FinancialEvent"]


T = TypeVar("T")


@dataclass(frozen=True)
class ExecutionOutcome(Generic[T]):
    """一次已执行撮合的成交量、原因和账本结果。"""

    filled_quantity: int
    reason: str | None
    value: T
