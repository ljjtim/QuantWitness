"""包含夜盘归属的显式交易时段合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from research_pipeline.platform.canonical import typed_canonical_hash

from .time import TimeContractError, require_aware_datetime


@dataclass(frozen=True)
class SessionSegment:
    segment_id: str
    starts_at: datetime
    ends_at: datetime
    trading_date: date
    phase: str
    bar_eligible: bool | None = None
    bucket_anchor_at: datetime | None = None
    aggregation_boundary: str | None = None

    def __post_init__(self) -> None:
        require_aware_datetime(self.starts_at, "starts_at")
        require_aware_datetime(self.ends_at, "ends_at")
        if self.starts_at >= self.ends_at:
            raise TimeContractError("session segment 时间倒置")
        if self.phase not in {"day", "night", "auction", "settlement"}:
            raise TimeContractError("session phase 不受支持")
        if not self.segment_id.strip():
            raise TimeContractError("segment_id 不能为空")
        enriched = (
            self.bar_eligible is not None,
            self.bucket_anchor_at is not None,
            self.aggregation_boundary is not None,
        )
        if any(enriched) and not all(enriched):
            raise TimeContractError("分钟 segment 聚合合同必须完整声明")
        if all(enriched):
            require_aware_datetime(self.bucket_anchor_at, "bucket_anchor_at")
            if not self.starts_at <= self.bucket_anchor_at <= self.ends_at:
                raise TimeContractError("bucket anchor 必须位于 segment 内")
            if self.aggregation_boundary != "completed-bar-right-closed-v1":
                raise TimeContractError("segment 聚合边界版本不受支持")

    def contains_instant(self, value: datetime) -> bool:
        current = require_aware_datetime(value, "session instant")
        return self.starts_at <= current < self.ends_at

    def contains_completed_bar_end(self, value: datetime) -> bool:
        if self.bar_eligible is not True:
            return False
        current = require_aware_datetime(value, "bar_end")
        return self.starts_at < current <= self.ends_at


@dataclass(frozen=True)
class TradingSession:
    session_id: str
    market: str
    trading_date: date
    segments: tuple[SessionSegment, ...]
    calendar_policy_id: str
    calendar_policy_revision: int | None = None
    instrument_id: str | None = None
    instrument_classification: tuple[str, ...] | None = None
    policy_hash: str | None = None
    scope_binding_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.session_id.strip() or not self.market.strip() or not self.calendar_policy_id.strip():
            raise TimeContractError("session 身份字段不能为空")
        if not self.segments:
            raise TimeContractError("session segments 不能为空")
        ordered = tuple(sorted(self.segments, key=lambda item: (item.starts_at, item.segment_id)))
        if ordered != self.segments or len({item.segment_id for item in ordered}) != len(ordered):
            raise TimeContractError("session segments 必须排序且唯一")
        for index, item in enumerate(ordered):
            if item.trading_date != self.trading_date:
                raise TimeContractError("segment 必须显式归属同一 trading_date")
            if index and ordered[index - 1].ends_at > item.starts_at:
                raise TimeContractError("session segments 不能重叠")
        enriched = (
            self.calendar_policy_revision is not None,
            self.instrument_id is not None,
            self.instrument_classification is not None,
            self.policy_hash is not None,
            self.scope_binding_hash is not None,
        )
        if any(enriched) and not all(enriched):
            raise TimeContractError("版本化 session 身份必须完整声明")
        if all(enriched):
            if self.calendar_policy_revision < 1:
                raise TimeContractError("calendar policy revision 无效")
            if not self.instrument_id.strip():
                raise TimeContractError("session instrument_id 不能为空")
            if (
                len(self.instrument_classification) != 5
                or any(not item.strip() for item in self.instrument_classification)
            ):
                raise TimeContractError("instrument classification 无效")
            for value in (self.policy_hash, self.scope_binding_hash):
                if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                    raise TimeContractError("session 绑定摘要必须是 sha256")

    def segment_for_instant(self, value: datetime) -> SessionSegment:
        matches = tuple(item for item in self.segments if item.contains_instant(value))
        if len(matches) != 1:
            raise TimeContractError("instant 不属于唯一 session segment")
        return matches[0]

    def segment_for_completed_bar_end(self, value: datetime) -> SessionSegment:
        matches = tuple(
            item for item in self.segments if item.contains_completed_bar_end(value)
        )
        if len(matches) != 1:
            raise TimeContractError("bar_end 不属于唯一可聚合 session segment")
        return matches[0]

    @property
    def session_hash(self) -> str:
        payload: dict[str, object] = {
            "session_id": self.session_id,
            "market": self.market,
            "trading_date": self.trading_date.isoformat(),
            "calendar_policy_id": self.calendar_policy_id,
            "segments": [
                {
                    "segment_id": item.segment_id,
                    "starts_at": item.starts_at.isoformat(),
                    "ends_at": item.ends_at.isoformat(),
                    "trading_date": item.trading_date.isoformat(),
                    "phase": item.phase,
                    **(
                        {
                            "bar_eligible": item.bar_eligible,
                            "bucket_anchor_at": item.bucket_anchor_at.isoformat(),
                            "aggregation_boundary": item.aggregation_boundary,
                        }
                        if item.bar_eligible is not None
                        else {}
                    ),
                }
                for item in self.segments
            ],
        }
        if self.calendar_policy_revision is not None:
            payload.update(
                {
                    "calendar_policy_revision": self.calendar_policy_revision,
                    "instrument_id": self.instrument_id,
                    "instrument_classification": list(self.instrument_classification),
                    "policy_hash": self.policy_hash,
                    "scope_binding_hash": self.scope_binding_hash,
                }
            )
        return typed_canonical_hash(payload)


__all__ = ["SessionSegment", "TradingSession"]
