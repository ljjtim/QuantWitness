"""分钟数据的 PIT 复权质量审计。"""

from __future__ import annotations

from functools import cached_property

from dataclasses import dataclass
from datetime import datetime
import math
from zoneinfo import ZoneInfo

from research_pipeline.catalog import AdjustmentFactorSnapshot
from research_pipeline.domain import CN_MARKET_TIMEZONE, CorporateAction
from research_pipeline.platform import typed_canonical_hash

from .errors import QualityGateError


MINUTE_ADJUSTMENT_AUDIT_VERSION = "minute-adjustment-audit-v1"
_ZONE = ZoneInfo(CN_MARKET_TIMEZONE)


@dataclass(frozen=True)
class AdjustmentRelationSample:
    instrument_id: str
    observed_at: datetime
    raw_price: float
    pre_price: float
    post_price: float
    factor: float
    anchor_factor: float


@dataclass(frozen=True)
class MinuteAdjustmentAudit:
    status: str
    source_adjustment_mode: str
    snapshot_identity_hash: str | None
    corporate_action_snapshot_hash: str | None
    included_action_hashes: tuple[str, ...]
    checked_relation_count: int
    reason: str | None
    contract_version: str = MINUTE_ADJUSTMENT_AUDIT_VERSION

    @cached_property
    def audit_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "source_adjustment_mode": self.source_adjustment_mode,
            "snapshot_identity_hash": self.snapshot_identity_hash,
            "corporate_action_snapshot_hash": self.corporate_action_snapshot_hash,
            "included_action_hashes": list(self.included_action_hashes),
            "checked_relation_count": self.checked_relation_count,
            "reason": self.reason,
            "contract_version": self.contract_version,
        }


def audit_adjustment_gate(
    *,
    source_adjustment_mode: str,
    snapshot: AdjustmentFactorSnapshot | None,
    corporate_action_snapshot_hash: str | None = None,
    included_actions: tuple[CorporateAction, ...] = (),
    relation_samples: tuple[AdjustmentRelationSample, ...] = (),
    relative_tolerance: float = 1e-9,
) -> MinuteAdjustmentAudit:
    """验证 PIT 因子快照没有纳入研究时钟之后才可见的公司行为。"""

    if source_adjustment_mode not in {"raw", "pre", "post"}:
        raise QualityGateError("分钟复权模式不受支持")
    if snapshot is None:
        if source_adjustment_mode == "raw":
            return MinuteAdjustmentAudit("pass", "raw", None, None, (), 0, None)
        return MinuteAdjustmentAudit(
            "unsupported",
            source_adjustment_mode,
            None,
            None,
            (),
            0,
            "pit_adjustment_factor_snapshot_missing",
        )
    as_of = datetime.fromisoformat(snapshot.as_of)
    event_available = datetime.fromisoformat(snapshot.event_available_at)
    start = datetime.fromisoformat(snapshot.applicable_start)
    end = datetime.fromisoformat(snapshot.applicable_end)
    ordered_actions = tuple(
        sorted(included_actions, key=lambda item: (item.action_id, item.revision))
    )
    if len({item.action_id for item in ordered_actions}) != len(ordered_actions):
        raise QualityGateError("PIT 复权公司行为快照包含重复 action_id")
    computed_action_snapshot_hash = typed_canonical_hash(
        [item.to_dict() for item in ordered_actions]
    )
    if corporate_action_snapshot_hash != computed_action_snapshot_hash:
        raise QualityGateError("PIT 复权公司行为快照摘要不一致")
    for action in ordered_actions:
        if (
            action.announcement_available_time > as_of
            or action.announcement_available_time > event_available
        ):
            raise QualityGateError("PIT 复权快照包含研究时钟之后才可见的公司行为")
    observed_action_hashes = tuple(sorted(item.action_hash for item in ordered_actions))
    if observed_action_hashes != snapshot.included_action_hashes:
        raise QualityGateError("PIT 复权快照与实际纳入公司行为身份不一致")
    if relative_tolerance <= 0 or not math.isfinite(relative_tolerance):
        raise QualityGateError("复权关系容差无效")
    for sample in relation_samples:
        if sample.observed_at.tzinfo is None or sample.observed_at.utcoffset() is None:
            raise QualityGateError("复权关系样本 observed_at 必须带时区")
        observed = sample.observed_at.astimezone(_ZONE)
        if not start <= observed < end:
            raise QualityGateError("复权关系样本超出 factor snapshot 适用区间")
        values = (
            sample.raw_price,
            sample.pre_price,
            sample.post_price,
            sample.factor,
            sample.anchor_factor,
        )
        if any(not math.isfinite(item) for item in values) or min(
            sample.factor, sample.anchor_factor
        ) <= 0:
            raise QualityGateError("复权关系样本含无效数值")
        expected_pre = sample.raw_price * sample.factor / sample.anchor_factor
        expected_post = sample.raw_price * sample.factor
        if not math.isclose(sample.pre_price, expected_pre, rel_tol=relative_tolerance) or not math.isclose(
            sample.post_price, expected_post, rel_tol=relative_tolerance
        ):
            raise QualityGateError("raw/pre/post 与 factor snapshot 关系不一致")
    return MinuteAdjustmentAudit(
        "pass",
        source_adjustment_mode,
        snapshot.snapshot_identity_hash,
        computed_action_snapshot_hash,
        tuple(action.action_hash for action in ordered_actions),
        len(relation_samples),
        None,
    )


__all__ = [
    "MINUTE_ADJUSTMENT_AUDIT_VERSION",
    "AdjustmentRelationSample",
    "MinuteAdjustmentAudit",
    "audit_adjustment_gate",
]
