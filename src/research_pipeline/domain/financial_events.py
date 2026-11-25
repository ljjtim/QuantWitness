"""进入权益计算的不可变公司行动事件。"""

from __future__ import annotations

from functools import cached_property

from dataclasses import dataclass
from datetime import datetime

from research_pipeline.platform.canonical import typed_canonical_hash

from .corporate_actions import CorporateActionError
from .time import require_aware_datetime


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
            raise CorporateActionError("金融事件身份字段不能为空")
        if self.kind != "corporate_action":
            raise CorporateActionError("金融事件 kind 不受支持")
        require_aware_datetime(self.effective_time, "effective_time")
        if len(self.rule_hash) != 64:
            raise CorporateActionError("rule_hash 必须是 sha256")
        if self.payload != tuple(sorted(self.payload, key=lambda item: item[0])):
            raise CorporateActionError("事件 payload 必须按字段名排序")
        if len({key for key, _ in self.payload}) != len(self.payload):
            raise CorporateActionError("事件 payload 字段不能重复")

    @cached_property
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


__all__ = ["FinancialEvent"]
