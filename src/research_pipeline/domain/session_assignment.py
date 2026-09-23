"""把已完成分钟 bar 归入版本化交易日和交易时段。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Iterable, Literal, Mapping
from zoneinfo import ZoneInfo

from .session_calendar import (
    SessionCalendarError,
    SessionPolicyRevision,
)
from .sessions import SessionSegment, TradingSession
from .time import CN_MARKET_TIMEZONE, require_aware_datetime


_SESSION_TIMEZONE = ZoneInfo(CN_MARKET_TIMEZONE)
SESSION_EFFECTIVE_INTERVAL_BOUNDARY = "[effective_from,effective_to_exclusive)"
SESSION_INSTANT_BOUNDARY = "[start,end)"
SESSION_COMPLETED_BAR_BOUNDARY = "(start,end]"


class SessionAssignmentError(SessionCalendarError):
    """分钟 bar 不能唯一归入当时可见的交易时段。"""

    error_code = "session_assignment_invalid"


@dataclass(frozen=True)
class SessionAssignmentPolicy:
    """带可见时间和 Scope 绑定的交易时段规则快照。"""

    policy: SessionPolicyRevision
    available_at: datetime
    scope_binding_hash: str
    timezone: str = CN_MARKET_TIMEZONE
    effective_interval_boundary: str = SESSION_EFFECTIVE_INTERVAL_BOUNDARY
    instant_boundary: str = SESSION_INSTANT_BOUNDARY
    completed_bar_boundary: str = SESSION_COMPLETED_BAR_BOUNDARY
    visibility_basis: Literal["bar_end", "decision_time"] = "bar_end"

    def __post_init__(self) -> None:
        require_aware_datetime(self.available_at, "session policy available_at")
        if self.timezone != CN_MARKET_TIMEZONE:
            raise SessionAssignmentError("session policy timezone 必须是 Asia/Shanghai")
        if self.effective_interval_boundary != SESSION_EFFECTIVE_INTERVAL_BOUNDARY:
            raise SessionAssignmentError("session policy 生效区间必须左闭右开")
        if self.instant_boundary != SESSION_INSTANT_BOUNDARY:
            raise SessionAssignmentError("普通 instant 边界必须左闭右开")
        if self.completed_bar_boundary != SESSION_COMPLETED_BAR_BOUNDARY:
            raise SessionAssignmentError("completed bar 边界必须左开右闭")
        if self.visibility_basis not in {"bar_end", "decision_time"}:
            raise SessionAssignmentError(
                "session policy visibility_basis 只能是 bar_end 或 decision_time"
            )
        if (
            not isinstance(self.scope_binding_hash, str)
            or len(self.scope_binding_hash) != 64
            or any(char not in "0123456789abcdef" for char in self.scope_binding_hash)
        ):
            raise SessionAssignmentError("session policy scope_binding_hash 无效")

    @property
    def effective_to_exclusive(self) -> date:
        """把历史兼容的闭区间 policy 投影为明确的左闭右开区间。"""

        return self.policy.effective_to + timedelta(days=1)

    def to_dict(self) -> dict[str, object]:
        """返回可进入 lineage/验收收据的完整时间合同。"""

        return {
            "policy": self.policy.to_dict(),
            "available_at": self.available_at.isoformat(),
            "scope_binding_hash": self.scope_binding_hash,
            "timezone": self.timezone,
            "effective_from": self.policy.effective_from.isoformat(),
            "effective_to_exclusive": self.effective_to_exclusive.isoformat(),
            "effective_interval_boundary": self.effective_interval_boundary,
            "instant_boundary": self.instant_boundary,
            "completed_bar_boundary": self.completed_bar_boundary,
            "visibility_basis": self.visibility_basis,
        }


def assign_completed_bars(
    rows: Iterable[Mapping[str, object]],
    *,
    policies: Iterable[SessionAssignmentPolicy],
    decision_time: datetime,
) -> tuple[dict[str, object], ...]:
    """按左开右闭的 completed-bar 边界分配 trading_day/session_id。"""

    decision = require_aware_datetime(decision_time, "decision_time")
    snapshots = tuple(policies)
    if not snapshots:
        raise SessionAssignmentError("session assignment policies 不能为空")
    segment_index = _build_segment_index(snapshots)
    outputs = []
    seen_bar_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise SessionAssignmentError("分钟 bar 必须是 mapping")
        bar_id = _text(row.get("bar_id"), "bar_id")
        instrument_id = _text(row.get("instrument_id"), "instrument_id")
        if bar_id in seen_bar_ids:
            raise SessionAssignmentError(f"bar_id 重复: {bar_id}")
        seen_bar_ids.add(bar_id)
        if row.get("completed") is not True or row.get("quality_pass") is not True:
            raise SessionAssignmentError(f"{bar_id} 必须 completed 且 quality pass")
        bar_end = require_aware_datetime(row.get("bar_end"), "bar_end")
        available_at = require_aware_datetime(row.get("available_at"), "available_at")
        if bar_end > available_at:
            raise SessionAssignmentError(f"{bar_id} 的 bar_end 不能晚于 available_at")
        if available_at > decision:
            raise SessionAssignmentError(f"{bar_id} 在 decision_time 尚不可见")

        matches = []
        hidden_rule = False
        natural_date = bar_end.astimezone(_SESSION_TIMEZONE).date()
        for snapshot, session, segment in segment_index.get(
            (instrument_id, natural_date),
            (),
        ):
            visibility_cutoff = (
                bar_end if snapshot.visibility_basis == "bar_end" else decision
            )
            if snapshot.available_at > visibility_cutoff:
                hidden_rule = True
                continue
            if segment.contains_completed_bar_end(bar_end):
                matches.append((snapshot, session, segment))
        if len(matches) != 1:
            suffix = (
                "；规则在声明的可见性截止时点尚不可见"
                if hidden_rule and not matches
                else ""
            )
            raise SessionAssignmentError(
                f"{bar_id} 没有唯一 session，禁止按自然日兜底{suffix}"
            )
        snapshot, session, segment = matches[0]
        policy = snapshot.policy
        outputs.append(
            {
                "bar_id": bar_id,
                "instrument_id": instrument_id,
                "bar_end": bar_end.isoformat(),
                "available_at": available_at.isoformat(),
                "trading_day": session.trading_date.isoformat(),
                "session_id": session.session_id,
                "segment_id": segment.segment_id,
                "phase": segment.phase,
                "calendar_policy_id": policy.policy_id,
                "calendar_policy_revision": policy.revision,
                "calendar_policy_available_at": snapshot.available_at.isoformat(),
                "calendar_policy_effective_from": policy.effective_from.isoformat(),
                "calendar_policy_effective_to": policy.effective_to.isoformat(),
                "calendar_policy_effective_to_exclusive": (
                    snapshot.effective_to_exclusive.isoformat()
                ),
                "calendar_policy_effective_interval_boundary": (
                    snapshot.effective_interval_boundary
                ),
                "calendar_policy_timezone": snapshot.timezone,
                "calendar_policy_instant_boundary": snapshot.instant_boundary,
                "calendar_policy_completed_bar_boundary": (
                    snapshot.completed_bar_boundary
                ),
                "calendar_policy_visibility_basis": snapshot.visibility_basis,
                "calendar_policy_hash": policy.policy_hash,
                "scope_binding_hash": snapshot.scope_binding_hash,
                "volume": _decimal_text(row.get("volume"), "volume"),
                "money": _decimal_text(row.get("money"), "money"),
                "open_interest": _decimal_text(
                    row.get("open_interest"),
                    "open_interest",
                ),
            }
        )
    return tuple(outputs)


def reconcile_session_aggregates(
    assigned_rows: Iterable[Mapping[str, object]],
    daily_rows: Iterable[Mapping[str, object]],
    *,
    money_tolerance: Decimal = Decimal("0.01"),
    open_interest_policy: Literal["strict", "diagnostic"] = "strict",
) -> tuple[dict[str, object], ...]:
    """核对分钟 volume/money；OI 可按合同严格阻断或仅作快照诊断。"""

    tolerance = _decimal(money_tolerance, "money_tolerance")
    if open_interest_policy not in {"strict", "diagnostic"}:
        raise SessionAssignmentError("open_interest_policy 只能是 strict 或 diagnostic")
    minute_groups: dict[tuple[str, str], list[Mapping[str, object]]] = {}
    for row in assigned_rows:
        key = (
            _text(row.get("instrument_id"), "instrument_id"),
            _text(row.get("trading_day"), "trading_day"),
        )
        minute_groups.setdefault(key, []).append(row)
    daily_by_key: dict[tuple[str, str], Mapping[str, object]] = {}
    for row in daily_rows:
        key = (
            _text(row.get("instrument_id"), "instrument_id"),
            _text(row.get("trading_day"), "trading_day"),
        )
        if key in daily_by_key:
            raise SessionAssignmentError(f"日线聚合键重复: {key}")
        daily_by_key[key] = row
    if set(minute_groups) != set(daily_by_key):
        raise SessionAssignmentError("分钟与日线交易日覆盖不一致")

    results = []
    for key, group in sorted(minute_groups.items()):
        ordered = sorted(group, key=lambda item: _aware(item.get("bar_end"), "bar_end"))
        minute_volume = sum(
            (_decimal(item.get("volume"), "volume") for item in ordered), Decimal(0)
        )
        minute_money = sum(
            (_decimal(item.get("money"), "money") for item in ordered), Decimal(0)
        )
        minute_oi = _decimal(ordered[-1].get("open_interest"), "open_interest")
        daily = daily_by_key[key]
        daily_volume = _decimal(daily.get("volume"), "daily.volume")
        daily_money = _decimal(daily.get("money"), "daily.money")
        daily_oi = _decimal(daily.get("open_interest"), "daily.open_interest")
        blocking_reason_codes = []
        diagnostic_reason_codes = []
        if minute_volume != daily_volume:
            blocking_reason_codes.append("session_reconciliation.volume_mismatch")
        if abs(minute_money - daily_money) > tolerance:
            blocking_reason_codes.append("session_reconciliation.money_mismatch")
        if minute_oi != daily_oi:
            reason = "session_reconciliation.open_interest_mismatch"
            if open_interest_policy == "strict":
                blocking_reason_codes.append(reason)
            else:
                diagnostic_reason_codes.append(reason)
        if blocking_reason_codes:
            raise SessionAssignmentError(
                f"{key[0]}/{key[1]} 日线对账失败: {','.join(blocking_reason_codes)}"
            )
        results.append(
            {
                "instrument_id": key[0],
                "trading_day": key[1],
                "minute_row_count": len(ordered),
                "volume": str(minute_volume),
                "daily_volume": str(daily_volume),
                "volume_delta": str(minute_volume - daily_volume),
                "money": str(minute_money),
                "daily_money": str(daily_money),
                "money_delta": str(minute_money - daily_money),
                "money_tolerance": str(tolerance),
                "open_interest": str(minute_oi),
                "daily_open_interest": str(daily_oi),
                "open_interest_delta": str(minute_oi - daily_oi),
                "open_interest_policy": open_interest_policy,
                "diagnostic_reason_codes": diagnostic_reason_codes,
                "status": (
                    "pass_with_diagnostic" if diagnostic_reason_codes else "pass"
                ),
            }
        )
    return tuple(results)


def _build_segment_index(
    snapshots: tuple[SessionAssignmentPolicy, ...],
) -> dict[
    tuple[str, date],
    tuple[tuple[SessionAssignmentPolicy, TradingSession, SessionSegment], ...],
]:
    """预构建品种与自然日索引，避免每根 bar 扫描全部交易日。"""

    mutable: dict[
        tuple[str, date],
        list[tuple[SessionAssignmentPolicy, TradingSession, SessionSegment]],
    ] = {}
    for snapshot in snapshots:
        policy = snapshot.policy
        instrument_id = policy.instrument.instrument_id
        for trading_date in policy.trading_dates:
            session = policy.build_session(
                trading_date,
                scope_binding_hash=snapshot.scope_binding_hash,
            )
            for segment in session.segments:
                if segment.bar_eligible is not True:
                    continue
                natural_date = segment.starts_at.astimezone(_SESSION_TIMEZONE).date()
                final_date = segment.ends_at.astimezone(_SESSION_TIMEZONE).date()
                while natural_date <= final_date:
                    mutable.setdefault((instrument_id, natural_date), []).append(
                        (snapshot, session, segment)
                    )
                    natural_date += timedelta(days=1)
    return {key: tuple(value) for key, value in mutable.items()}


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SessionAssignmentError(f"{field} 必须是非空字符串")
    return value


def _aware(value: object, field: str) -> datetime:
    current = datetime.fromisoformat(value) if isinstance(value, str) else value
    return require_aware_datetime(current, field)


def _decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise SessionAssignmentError(f"{field} 必须是有限非负数")
    try:
        current = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise SessionAssignmentError(f"{field} 必须是有限非负数") from None
    if not current.is_finite() or current < 0:
        raise SessionAssignmentError(f"{field} 必须是有限非负数")
    return current


def _decimal_text(value: object, field: str) -> str:
    return str(_decimal(value, field))


__all__ = [
    "SESSION_COMPLETED_BAR_BOUNDARY",
    "SESSION_EFFECTIVE_INTERVAL_BOUNDARY",
    "SESSION_INSTANT_BOUNDARY",
    "SessionAssignmentError",
    "SessionAssignmentPolicy",
    "assign_completed_bars",
    "reconcile_session_aggregates",
]
