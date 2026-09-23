"""分钟数据的只读结构、session、数值与 PIT 复权质量审计。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from research_pipeline.catalog import AdjustmentFactorSnapshot
from research_pipeline.domain import (
    CN_MARKET_TIMEZONE,
    CorporateAction,
    SessionCalendarResolver,
    SessionInstrumentMetadata,
    SessionPolicyBundle,
)
from research_pipeline.platform import typed_canonical_hash

from .errors import QualityGateError
from .minute_scan import MinuteScanPlan


MINUTE_QUALITY_PROFILE_VERSION = "minute-quality-profile-v1"
MINUTE_QUALITY_REPORT_VERSION = "minute-quality-report-v1"
MINUTE_ADJUSTMENT_AUDIT_VERSION = "minute-adjustment-audit-v1"
_ZONE = ZoneInfo(CN_MARKET_TIMEZONE)
_QUALITY_REFS = {
    "cn_stock": (
        "quality.minute.common_fields.v1",
        "quality.minute.stock.v1",
    ),
    "cn_etf": (
        "quality.minute.common_fields.v1",
        "quality.minute.fund.v1",
    ),
    "cn_index": (
        "quality.minute.common_fields.v1",
        "quality.minute.index.v1",
    ),
    "cn_future": (
        "quality.minute.common_fields.v1",
        "quality.minute.futures.avg_coverage.v1",
        "quality.minute.futures.open_interest.v1",
    ),
}


@dataclass(frozen=True)
class MinuteQualityProfile:
    profile_id: str = "quality.minute.reference.v1"
    revision: int = 1
    enforcement: str = "report_only"
    max_issue_samples: int = 20
    max_expected_slots: int = 250_000
    contract_version: str = MINUTE_QUALITY_PROFILE_VERSION

    def __post_init__(self) -> None:
        if (
            self.contract_version != MINUTE_QUALITY_PROFILE_VERSION
            or not self.profile_id.strip()
            or self.revision < 1
            or self.enforcement not in {"report_only", "hard_fail"}
            or not 1 <= self.max_issue_samples <= 1000
            or not 1 <= self.max_expected_slots <= 5_000_000
        ):
            raise QualityGateError("分钟质量 profile 无效")

    @property
    def profile_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "profile_id": self.profile_id,
            "revision": self.revision,
            "enforcement": self.enforcement,
            "max_issue_samples": self.max_issue_samples,
            "max_expected_slots": self.max_expected_slots,
            "quality_policy_refs_by_asset": {
                key: list(value) for key, value in sorted(_QUALITY_REFS.items())
            },
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class MinuteQualityIssue:
    error_code: str
    severity: str
    instrument_id: str
    bar_end: str | None
    detail: str

    def to_dict(self) -> dict[str, str | None]:
        return {
            "error_code": self.error_code,
            "severity": self.severity,
            "instrument_id": self.instrument_id,
            "bar_end": self.bar_end,
            "detail": self.detail,
        }


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

    @property
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


@dataclass(frozen=True)
class MinuteQualityReport:
    scan_plan_hash: str
    source_revision_hash: str
    session_bundle_hash: str
    quality_profile_hash: str
    quality_policy_refs: tuple[str, ...]
    scan_start_at: str
    scan_end_at: str
    as_of: str
    row_count: int
    expected_bar_count: int
    observed_bar_count: int
    missing_bar_count: int
    issue_counts: tuple[tuple[str, int], ...]
    issue_samples: tuple[MinuteQualityIssue, ...]
    status: str
    claim_impact: str
    adjustment_audit_hash: str | None
    contract_version: str = MINUTE_QUALITY_REPORT_VERSION

    @property
    def report_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "scan_plan_hash": self.scan_plan_hash,
            "source_revision_hash": self.source_revision_hash,
            "session_bundle_hash": self.session_bundle_hash,
            "quality_profile_hash": self.quality_profile_hash,
            "quality_policy_refs": list(self.quality_policy_refs),
            "scan_start_at": self.scan_start_at,
            "scan_end_at": self.scan_end_at,
            "as_of": self.as_of,
            "row_count": self.row_count,
            "expected_bar_count": self.expected_bar_count,
            "observed_bar_count": self.observed_bar_count,
            "missing_bar_count": self.missing_bar_count,
            "coverage_ratio": (
                1.0
                if self.expected_bar_count == 0
                else self.observed_bar_count / self.expected_bar_count
            ),
            "issue_counts": dict(self.issue_counts),
            "issue_samples": [item.to_dict() for item in self.issue_samples],
            "status": self.status,
            "claim_impact": self.claim_impact,
            "adjustment_audit_hash": self.adjustment_audit_hash,
            "contract_version": self.contract_version,
        }


class _Issues:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.counts: dict[str, int] = {}
        self.severities: dict[str, str] = {}
        self.samples: list[MinuteQualityIssue] = []

    def add(
        self,
        code: str,
        *,
        instrument_id: str,
        bar_end: datetime | None,
        detail: str,
        count: int = 1,
        severity: str = "fail",
    ) -> None:
        self.counts[code] = self.counts.get(code, 0) + count
        previous = self.severities.setdefault(code, severity)
        if previous != severity:
            raise QualityGateError("同一质量错误码不能同时使用不同 severity")
        if len(self.samples) < self.limit:
            self.samples.append(
                MinuteQualityIssue(
                    code,
                    severity,
                    instrument_id,
                    None if bar_end is None else bar_end.isoformat(timespec="seconds"),
                    detail,
                )
            )


def audit_minute_quality(
    source: Iterable[Any],
    *,
    scan_plan: MinuteScanPlan,
    session_bundle: SessionPolicyBundle,
    instruments: tuple[SessionInstrumentMetadata, ...],
    profile: MinuteQualityProfile | None = None,
    adjustment_audit: MinuteAdjustmentAudit | None = None,
) -> MinuteQualityReport:
    """只读扫描分钟批次；报告异常但不修复、不插值。"""

    profile = profile or MinuteQualityProfile()
    expected_refs = _QUALITY_REFS.get(scan_plan.minute_asset_class)
    if expected_refs is None or scan_plan.minute_quality_policy_refs != expected_refs:
        raise QualityGateError("分钟扫描计划的 quality policy refs 与资产不一致")
    by_id = {item.instrument_id: item for item in instruments}
    if set(by_id) != set(scan_plan.instruments):
        raise QualityGateError("质量审计 instrument 集合与扫描计划不一致")
    expected = _expected_slots(scan_plan, session_bundle, by_id, profile)
    issues = _Issues(profile.max_issue_samples)
    observed: set[tuple[str, datetime]] = set()
    previous: tuple[str, datetime] | None = None
    row_count = 0
    expected_schema = tuple(scan_plan.source_columns)

    for batch in source:
        if tuple(batch.schema.names) != expected_schema:
            raise QualityGateError("分钟质量审计输入 schema 漂移")
        for row in batch.to_pylist():
            row_count += 1
            code = str(row.get("code", ""))
            bar_end = row.get("dt")
            if code not in by_id or not isinstance(bar_end, datetime) or bar_end.tzinfo is not None:
                issues.add(
                    "minute.structural.invalid_identity_or_time",
                    instrument_id=code or "<missing>",
                    bar_end=None,
                    detail="code 未准入或 dt 不是 naive 上海 completed-bar 时点",
                )
                continue
            key = (code, bar_end)
            if previous is not None and key < previous:
                issues.add(
                    "minute.structural.sort_regression",
                    instrument_id=code,
                    bar_end=bar_end,
                    detail="code,dt 未严格递增",
                )
            if key in observed:
                issues.add(
                    "minute.structural.duplicate_primary_key",
                    instrument_id=code,
                    bar_end=bar_end,
                    detail="重复 code+dt",
                )
            previous = key
            observed.add(key)
            if key not in expected:
                issues.add(
                    "minute.session.outside_expected_slot",
                    instrument_id=code,
                    bar_end=bar_end,
                    detail="bar 不在查询窗口内的批准 session 槽位",
                )
            _check_market_values(row, scan_plan.minute_asset_class, issues, code, bar_end)

    missing = expected - observed
    if missing:
        first_code, first_bar = min(missing)
        issues.add(
            "minute.session.missing_expected_bar",
            instrument_id=first_code,
            bar_end=first_bar,
            detail="批准 session 槽位缺少 bar；不自动插值",
            count=len(missing),
        )
    counts = tuple(sorted(issues.counts.items()))
    has_failures = any(issues.severities[code] == "fail" for code in issues.counts)
    has_warnings = any(issues.severities[code] == "warn" for code in issues.counts)
    if has_failures:
        status = "fail"
        impact = "blocks_execution_and_verification"
    elif adjustment_audit is not None and adjustment_audit.status == "unsupported":
        status = "unsupported"
        impact = "capability_limited"
    elif has_warnings:
        status = "warn"
        impact = "requires_review_no_claim_upgrade"
    else:
        status = "pass"
        impact = "eligible_for_downstream_gate"
    report = MinuteQualityReport(
        scan_plan.plan_hash,
        scan_plan.source_revision_hash,
        session_bundle.bundle_hash,
        profile.profile_hash,
        scan_plan.minute_quality_policy_refs,
        scan_plan.start_at.isoformat(timespec="seconds"),
        scan_plan.end_at.isoformat(timespec="seconds"),
        scan_plan.as_of.isoformat(timespec="seconds"),
        row_count,
        len(expected),
        len(observed & expected),
        len(missing),
        counts,
        tuple(issues.samples),
        status,
        impact,
        None if adjustment_audit is None else adjustment_audit.audit_hash,
    )
    if profile.enforcement == "hard_fail" and report.status == "fail":
        raise QualityGateError(f"分钟质量门禁失败: {report.report_hash}")
    return report


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


def require_minute_quality_pass(
    report: MinuteQualityReport,
    *,
    scan_plan_hash: str,
    session_bundle_hash: str,
    quality_profile_hash: str,
    adjustment_audit_hash: str | None,
) -> str:
    """供 plan/run/verify 共用的失败关闭消费门；返回已复验报告摘要。"""

    expected = (
        scan_plan_hash,
        session_bundle_hash,
        quality_profile_hash,
        adjustment_audit_hash,
    )
    observed = (
        report.scan_plan_hash,
        report.session_bundle_hash,
        report.quality_profile_hash,
        report.adjustment_audit_hash,
    )
    if observed != expected:
        raise QualityGateError("分钟质量报告输入身份不一致")
    if report.status != "pass" or report.claim_impact != "eligible_for_downstream_gate":
        raise QualityGateError("分钟质量报告未达到可执行 pass")
    return report.report_hash


def _expected_slots(
    scan_plan: MinuteScanPlan,
    bundle: SessionPolicyBundle,
    instruments: Mapping[str, SessionInstrumentMetadata],
    profile: MinuteQualityProfile,
) -> set[tuple[str, datetime]]:
    resolver = SessionCalendarResolver(bundle)
    result: set[tuple[str, datetime]] = set()
    for code, instrument in instruments.items():
        policies = tuple(
            item
            for item in bundle.policies
            if item.instrument == instrument
            and item.policy_id == scan_plan.minute_session_policy_ref
            and item.revision == 1
        )
        if len(policies) != 1:
            raise QualityGateError("质量审计没有唯一 session policy")
        (policy,) = policies
        for trading_date in policy.trading_dates:
            session = resolver.resolve_trading_date(
                instrument,
                trading_date,
                policy_revision=1,
            )
            for segment in session.segments:
                cursor = segment.starts_at + timedelta(minutes=1)
                while cursor <= segment.ends_at:
                    if (
                        scan_plan.start_at <= cursor < scan_plan.end_at
                        and cursor <= scan_plan.as_of
                    ):
                        result.add((code, cursor.replace(tzinfo=None)))
                        if len(result) > profile.max_expected_slots:
                            raise QualityGateError("分钟质量审计预期槽位超过 profile 上限")
                    cursor += timedelta(minutes=1)
    return result


def _check_market_values(
    row: Mapping[str, object],
    asset_class: str,
    issues: _Issues,
    code: str,
    bar_end: datetime,
) -> None:
    values: dict[str, float] = {}
    for field in ("open", "high", "low", "close", "volume", "money"):
        value = row.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            issues.add(
                "minute.value.non_finite",
                instrument_id=code,
                bar_end=bar_end,
                detail=f"{field} 不是有限数",
            )
            return
        values[field] = float(value)
    if not (
        values["low"] <= values["open"] <= values["high"]
        and values["low"] <= values["close"] <= values["high"]
    ):
        issues.add(
            "minute.value.ohlc_invariant",
            instrument_id=code,
            bar_end=bar_end,
            detail="low <= open/close <= high 不成立",
        )
    if values["volume"] < 0 or values["money"] < 0:
        issues.add(
            "minute.value.negative_flow",
            instrument_id=code,
            bar_end=bar_end,
            detail="volume/money 不能为负",
        )
    avg = row.get("avg")
    if avg is None and values["volume"] > 0:
        issues.add(
            (
                "minute.value.avg_unavailable"
                if asset_class == "cn_future"
                else "minute.value.missing_avg_positive_volume"
            ),
            instrument_id=code,
            bar_end=bar_end,
            detail="正成交量 bar 缺少源 avg",
            severity="warn" if asset_class == "cn_future" else "fail",
        )
    elif avg is not None and (
        isinstance(avg, bool)
        or not isinstance(avg, (int, float))
        or not math.isfinite(float(avg))
    ):
        issues.add(
            "minute.value.invalid_avg",
            instrument_id=code,
            bar_end=bar_end,
            detail="avg 必须为空或有限数",
        )
    if asset_class == "cn_future":
        open_interest = row.get("open_interest")
        if (
            isinstance(open_interest, bool)
            or not isinstance(open_interest, (int, float))
            or not math.isfinite(float(open_interest))
            or float(open_interest) < 0
        ):
            issues.add(
                "minute.value.invalid_open_interest",
                instrument_id=code,
                bar_end=bar_end,
                detail="期货 open_interest 必须是非负有限数",
            )


__all__ = [
    "MINUTE_ADJUSTMENT_AUDIT_VERSION",
    "MINUTE_QUALITY_PROFILE_VERSION",
    "MINUTE_QUALITY_REPORT_VERSION",
    "AdjustmentRelationSample",
    "MinuteAdjustmentAudit",
    "MinuteQualityIssue",
    "MinuteQualityProfile",
    "MinuteQualityReport",
    "audit_adjustment_gate",
    "audit_minute_quality",
    "require_minute_quality_pass",
]
