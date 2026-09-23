"""分钟 Bar、Tick 与金融状态事件共用的稳定 tagged union。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from research_pipeline.platform.canonical import typed_canonical_hash

from .models import DomainContractError
from .time import require_aware_datetime


MARKET_EVENT_KINDS = frozenset({
    "session", "bar_completed", "trade_tick", "quote_tick", "order", "fill", "settlement", "corporate_action",
})


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    kind: str
    event_time: datetime
    available_time: datetime
    receipt_time: datetime
    source_id: str
    source_sequence: int
    payload: tuple[tuple[str, object], ...] = ()
    completed: bool = True

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.source_id.strip() or self.kind not in MARKET_EVENT_KINDS:
            raise DomainContractError("market event 身份或 kind 无效")
        for field in ("event_time", "available_time", "receipt_time"):
            require_aware_datetime(getattr(self, field), field)
        if self.source_sequence < 0:
            raise DomainContractError("source_sequence 不能为负")
        if self.event_time > self.receipt_time:
            raise DomainContractError("事件时间不能晚于接收时间")
        if self.kind == "bar_completed" and not self.completed:
            raise DomainContractError("未完成 Bar 不能标记为 BarCompleted")
        if self.payload != tuple(sorted(self.payload, key=lambda item: item[0])):
            raise DomainContractError("market event payload 必须排序")

    @property
    def event_hash(self) -> str:
        return typed_canonical_hash({
            "event_id": self.event_id, "kind": self.kind, "event_time": self.event_time.isoformat(),
            "available_time": self.available_time.isoformat(), "receipt_time": self.receipt_time.isoformat(),
            "source_id": self.source_id, "source_sequence": self.source_sequence,
            "payload": [[key, value] for key, value in self.payload], "completed": self.completed,
        })

    @property
    def sort_key(self) -> tuple[datetime, int, str]:
        return self.event_time, self.source_sequence, self.event_id


__all__ = ["MARKET_EVENT_KINDS", "MarketEvent"]
