"""有界期货 session-close 供应商语义及正式发布 bundle。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from importlib.resources import files
import json
from pathlib import Path
import re
from typing import Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from research_pipeline.platform.canonical import typed_canonical_hash

from .session_calendar import (
    SessionCalendarError,
    SessionPolicyRevision,
    load_session_policy_bundle,
)


FUTURES_SESSION_CLOSE_BUNDLE_VERSION = "futures-session-close-bundle-v2"
FUTURES_SESSION_CLOSE_BUNDLE_RESOURCE = (
    "session_policies/futures_session_close.v2.json"
)
FUTURES_SESSION_CLOSE_POLICY_IDS = frozenset(
    {
        "available.futures.daily.session-close.v1",
        "available.futures.session_close.v1",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class FuturesSessionCloseError(SessionCalendarError):
    """期货 session-close 来源、覆盖或发布身份不闭合。"""

    error_code = "futures_session_close_invalid"


@dataclass(frozen=True)
class FuturesSessionClosePolicyBinding:
    availability_policy_id: str
    session_policy_id: str
    session_policy_revision: int

    def __post_init__(self) -> None:
        if self.availability_policy_id not in FUTURES_SESSION_CLOSE_POLICY_IDS:
            raise FuturesSessionCloseError("session-close Catalog policy 不受支持")
        if not self.session_policy_id.strip() or self.session_policy_revision < 1:
            raise FuturesSessionCloseError("session-close policy revision 无效")

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_policy_id": self.availability_policy_id,
            "session_policy_id": self.session_policy_id,
            "session_policy_revision": self.session_policy_revision,
        }

    @classmethod
    def from_dict(cls, value: object) -> "FuturesSessionClosePolicyBinding":
        payload = _mapping(value, "catalog_policy_binding")
        _exact(
            payload,
            {
                "availability_policy_id",
                "session_policy_id",
                "session_policy_revision",
            },
            "catalog_policy_binding",
        )
        revision = payload["session_policy_revision"]
        if type(revision) is not int:
            raise FuturesSessionCloseError("session_policy_revision 必须是整数")
        return cls(
            _text(payload["availability_policy_id"], "availability_policy_id"),
            _text(payload["session_policy_id"], "session_policy_id"),
            revision,
        )


@dataclass(frozen=True)
class FuturesSessionCloseCalendarBinding:
    session_policy_id: str
    session_policy_revision: int
    calendar_bundle_ref: str
    calendar_bundle_hash: str
    calendar_policy_id: str
    calendar_policy_revision: int
    instrument_id: str
    timezone: str
    trading_dates: tuple[str, ...]

    def __post_init__(self) -> None:
        for field, value in (
            ("session_policy_id", self.session_policy_id),
            ("calendar_bundle_ref", self.calendar_bundle_ref),
            ("calendar_policy_id", self.calendar_policy_id),
            ("instrument_id", self.instrument_id),
            ("timezone", self.timezone),
        ):
            _text(value, field)
        if self.session_policy_revision < 1 or self.calendar_policy_revision < 1:
            raise FuturesSessionCloseError("session-close calendar revision 无效")
        _hash(self.calendar_bundle_hash, "calendar bundle_hash")
        if self.calendar_bundle_ref != (
            "research_pipeline.domain.session_policies/"
            "minute_reference_sessions.v4.json"
        ):
            raise FuturesSessionCloseError("session-close calendar 来源无效")
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise FuturesSessionCloseError("session-close calendar timezone 无效") from exc
        if (
            not self.trading_dates
            or self.trading_dates != tuple(sorted(set(self.trading_dates)))
        ):
            raise FuturesSessionCloseError("session-close calendar dates 必须非空、排序且唯一")
        try:
            tuple(date.fromisoformat(item) for item in self.trading_dates)
        except ValueError as exc:
            raise FuturesSessionCloseError("session-close calendar date 无效") from exc

    @property
    def session_policy_key(self) -> tuple[str, int]:
        return self.session_policy_id, self.session_policy_revision

    def to_dict(self) -> dict[str, object]:
        return {
            "session_policy_id": self.session_policy_id,
            "session_policy_revision": self.session_policy_revision,
            "calendar_bundle_ref": self.calendar_bundle_ref,
            "calendar_bundle_hash": self.calendar_bundle_hash,
            "calendar_policy_id": self.calendar_policy_id,
            "calendar_policy_revision": self.calendar_policy_revision,
            "instrument_id": self.instrument_id,
            "timezone": self.timezone,
            "trading_dates": list(self.trading_dates),
        }

    @classmethod
    def from_dict(cls, value: object) -> "FuturesSessionCloseCalendarBinding":
        payload = _mapping(value, "calendar_binding")
        expected = {
            "session_policy_id",
            "session_policy_revision",
            "calendar_bundle_ref",
            "calendar_bundle_hash",
            "calendar_policy_id",
            "calendar_policy_revision",
            "instrument_id",
            "timezone",
            "trading_dates",
        }
        _exact(payload, expected, "calendar_binding")
        dates = payload["trading_dates"]
        if not isinstance(dates, list) or any(not isinstance(item, str) for item in dates):
            raise FuturesSessionCloseError("calendar trading_dates 必须是字符串列表")
        revisions = (
            payload["session_policy_revision"],
            payload["calendar_policy_revision"],
        )
        if any(type(item) is not int for item in revisions):
            raise FuturesSessionCloseError("session-close calendar revision 必须是整数")
        return cls(
            _text(payload["session_policy_id"], "session_policy_id"),
            revisions[0],
            _text(payload["calendar_bundle_ref"], "calendar_bundle_ref"),
            _hash(payload["calendar_bundle_hash"], "calendar_bundle_hash"),
            _text(payload["calendar_policy_id"], "calendar_policy_id"),
            revisions[1],
            _text(payload["instrument_id"], "instrument_id"),
            _text(payload["timezone"], "timezone"),
            tuple(dates),
        )


@dataclass(frozen=True)
class FuturesSessionClosePolicyBundle:
    bundle_id: str
    source_artifact_ref: str
    source_acquired_at: datetime
    supplier_semantics: str
    calendar_bindings: tuple[FuturesSessionCloseCalendarBinding, ...]
    catalog_policy_bindings: tuple[FuturesSessionClosePolicyBinding, ...]
    policies: tuple[SessionPolicyRevision, ...]
    bundle_hash: str
    contract_version: str = FUTURES_SESSION_CLOSE_BUNDLE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != FUTURES_SESSION_CLOSE_BUNDLE_VERSION:
            raise FuturesSessionCloseError("session-close bundle 版本不受支持")
        if not self.bundle_id.strip() or not self.source_artifact_ref.strip():
            raise FuturesSessionCloseError("session-close bundle 来源身份不完整")
        if (
            self.source_acquired_at.tzinfo is None
            or self.source_acquired_at.utcoffset() is None
        ):
            raise FuturesSessionCloseError("session-close 来源取得时点必须带时区")
        if self.supplier_semantics != "supplier_replication_not_exchange_proof":
            raise FuturesSessionCloseError("session-close claim 上限不受支持")
        calendars = tuple(
            sorted(
                self.calendar_bindings,
                key=lambda item: (
                    item.session_policy_id,
                    item.session_policy_revision,
                ),
            )
        )
        if (
            not calendars
            or calendars != self.calendar_bindings
            or len({item.session_policy_key for item in calendars}) != len(calendars)
        ):
            raise FuturesSessionCloseError(
                "session-close calendar bindings 必须非空、排序且唯一"
            )
        try:
            current_calendar = load_session_policy_bundle()
        except SessionCalendarError as exc:
            raise FuturesSessionCloseError("session-close 绑定日历无法验证") from exc
        for calendar in calendars:
            calendar_policies = tuple(
                item
                for item in current_calendar.policies
                if item.policy_id == calendar.calendar_policy_id
                and item.revision == calendar.calendar_policy_revision
                and item.instrument.instrument_id == calendar.instrument_id
            )
            if current_calendar.bundle_hash != calendar.calendar_bundle_hash or len(
                calendar_policies
            ) != 1:
                raise FuturesSessionCloseError(
                    "session-close 绑定日历已漂移，必须重新发布"
                )
            (calendar_policy,) = calendar_policies
            if not set(calendar.trading_dates) <= {
                item.isoformat() for item in calendar_policy.trading_dates
            }:
                raise FuturesSessionCloseError(
                    "session-close 绑定日历已漂移，必须重新发布"
                )
        bindings = tuple(
            sorted(
                self.catalog_policy_bindings,
                key=lambda item: (
                    item.availability_policy_id,
                    item.session_policy_id,
                    item.session_policy_revision,
                ),
            )
        )
        binding_keys = {
            (
                item.availability_policy_id,
                item.session_policy_id,
                item.session_policy_revision,
            )
            for item in bindings
        }
        if (
            not bindings
            or bindings != self.catalog_policy_bindings
            or len(binding_keys) != len(bindings)
            or {item.availability_policy_id for item in bindings}
            != FUTURES_SESSION_CLOSE_POLICY_IDS
        ):
            raise FuturesSessionCloseError("session-close Catalog policy binding 不完整")
        policies = tuple(
            sorted(
                self.policies,
                key=lambda item: (item.policy_id, item.revision),
            )
        )
        policy_keys = {(item.policy_id, item.revision) for item in policies}
        if not policies or policies != self.policies or len(policy_keys) != len(policies):
            raise FuturesSessionCloseError("session-close policies 必须非空、排序且唯一")
        calendar_by_policy = {item.session_policy_key: item for item in calendars}
        if set(calendar_by_policy) != policy_keys:
            raise FuturesSessionCloseError("session-close policy 与日历映射不闭合")
        if {key[1:] for key in binding_keys} != policy_keys:
            raise FuturesSessionCloseError("Catalog policy 未覆盖全部 session revision")
        coverage: set[tuple[str, str, str]] = set()
        for policy in policies:
            calendar = calendar_by_policy[(policy.policy_id, policy.revision)]
            dates = tuple(item.isoformat() for item in policy.trading_dates)
            if (
                policy.instrument.instrument_id != calendar.instrument_id
                or dates != calendar.trading_dates
            ):
                raise FuturesSessionCloseError("session-close policy 与日历覆盖不一致")
            if policy.instrument.asset_class != "cn_future":
                raise FuturesSessionCloseError("session-close policy 只允许中国期货")
            if not any(
                segment.phase == "night"
                and segment.start_day_offset < segment.end_day_offset
                for segment in policy.segments
            ):
                raise FuturesSessionCloseError("session-close 来源未保留真实跨午夜夜盘")
            availability_ids = {
                item.availability_policy_id
                for item in bindings
                if (
                    item.session_policy_id,
                    item.session_policy_revision,
                )
                == (policy.policy_id, policy.revision)
            }
            for availability_id in availability_ids:
                for trading_date in dates:
                    key = (
                        availability_id,
                        policy.instrument.instrument_id,
                        trading_date,
                    )
                    if key in coverage:
                        raise FuturesSessionCloseError(
                            "session-close policy 对同一合约日期存在重复匹配"
                        )
                    coverage.add(key)
        if self.bundle_hash != typed_canonical_hash(self.identity_payload()):
            raise FuturesSessionCloseError("session-close bundle hash 不一致")

    def identity_payload(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "source_artifact_ref": self.source_artifact_ref,
            "source_acquired_at": self.source_acquired_at.isoformat(),
            "supplier_semantics": self.supplier_semantics,
            "calendar_bindings": [item.to_dict() for item in self.calendar_bindings],
            "catalog_policy_bindings": [
                item.to_dict() for item in self.catalog_policy_bindings
            ],
            "policies": [item.to_dict() for item in self.policies],
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.identity_payload(), "bundle_hash": self.bundle_hash}

    def resolve(
        self,
        *,
        availability_policy_id: str,
        instruments: tuple[str, ...],
        start: date,
        end: date,
    ) -> tuple[SessionPolicyRevision, ...]:
        """按 Catalog policy、显式合约和有界日期选择唯一正式 policy。"""

        if not instruments:
            raise FuturesSessionCloseError(
                "session-close 查询必须声明显式合约，snapshot universe 不受支持"
            )
        bindings = tuple(
            item
            for item in self.catalog_policy_bindings
            if item.availability_policy_id == availability_policy_id
        )
        if not bindings:
            raise FuturesSessionCloseError("Catalog availability policy 没有正式 bundle binding")
        policy_by_key = {
            (item.policy_id, item.revision): item for item in self.policies
        }
        candidates = tuple(policy_by_key[(item.session_policy_id, item.session_policy_revision)] for item in bindings)
        expected_dates = tuple(
            date.fromordinal(ordinal)
            for ordinal in range(start.toordinal(), end.toordinal() + 1)
        )
        selected: dict[tuple[str, int], SessionPolicyRevision] = {}
        for instrument in instruments:
            for trading_date in expected_dates:
                matches = tuple(
                    policy
                    for policy in candidates
                    if policy.instrument.instrument_id == instrument
                    and trading_date in policy.trading_dates
                    and policy.effective_from <= trading_date <= policy.effective_to
                )
                if not matches:
                    raise FuturesSessionCloseError(
                        "session-close bundle 缺少同一供应商期货开市日覆盖"
                    )
                if len(matches) != 1:
                    raise FuturesSessionCloseError(
                        "session-close policy 对同一合约日期存在重复匹配"
                    )
                policy = matches[0]
                selected[(policy.policy_id, policy.revision)] = policy
        if {item.instrument.instrument_id for item in selected.values()} != set(instruments):
                raise FuturesSessionCloseError(
                    "session-close bundle 缺少合约映射覆盖"
                )
        return tuple(
            sorted(
                selected.values(),
                key=lambda item: (
                    item.instrument.instrument_id,
                    item.trading_dates[0],
                    item.policy_id,
                    item.revision,
                ),
            )
        )

    def calendar_binding_for(
        self,
        policy: SessionPolicyRevision,
    ) -> FuturesSessionCloseCalendarBinding:
        matches = tuple(
            item
            for item in self.calendar_bindings
            if item.session_policy_key == (policy.policy_id, policy.revision)
        )
        if len(matches) != 1:
            raise FuturesSessionCloseError("session-close policy 缺少唯一日历映射")
        return matches[0]

    @classmethod
    def from_dict(cls, value: object) -> "FuturesSessionClosePolicyBundle":
        payload = _mapping(value, "FuturesSessionClosePolicyBundle")
        _exact(
            payload,
            {
                "bundle_id",
                "source_artifact_ref",
                "source_acquired_at",
                "supplier_semantics",
                "calendar_bindings",
                "catalog_policy_bindings",
                "policies",
                "bundle_hash",
                "contract_version",
            },
            "FuturesSessionClosePolicyBundle",
        )
        try:
            acquired = datetime.fromisoformat(
                _text(payload["source_acquired_at"], "source_acquired_at")
            )
        except ValueError as exc:
            raise FuturesSessionCloseError("source_acquired_at 不是有效时间") from exc
        calendars = payload["calendar_bindings"]
        bindings = payload["catalog_policy_bindings"]
        policies = payload["policies"]
        if (
            not isinstance(calendars, list)
            or not isinstance(bindings, list)
            or not isinstance(policies, list)
        ):
            raise FuturesSessionCloseError("session-close policies 必须是列表")
        return cls(
            _text(payload["bundle_id"], "bundle_id"),
            _text(payload["source_artifact_ref"], "source_artifact_ref"),
            acquired,
            _text(payload["supplier_semantics"], "supplier_semantics"),
            tuple(FuturesSessionCloseCalendarBinding.from_dict(item) for item in calendars),
            tuple(FuturesSessionClosePolicyBinding.from_dict(item) for item in bindings),
            tuple(SessionPolicyRevision.from_dict(item) for item in policies),
            _hash(payload["bundle_hash"], "bundle_hash"),
            _text(payload["contract_version"], "contract_version"),
        )


def load_futures_session_close_bundle(
    path: str | Path | None = None,
) -> FuturesSessionClosePolicyBundle:
    """加载正式 bundle；JSON 内发布摘要必须与内容一致。"""

    if path is None:
        resource = files("research_pipeline.domain").joinpath(
            FUTURES_SESSION_CLOSE_BUNDLE_RESOURCE
        )
        try:
            raw = resource.read_text(encoding="utf-8")
        except OSError as exc:
            raise FuturesSessionCloseError("默认 session-close bundle 无法读取") from exc
    else:
        try:
            raw = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise FuturesSessionCloseError("session-close bundle 无法读取") from exc
    try:
        return FuturesSessionClosePolicyBundle.from_dict(json.loads(raw))
    except json.JSONDecodeError as exc:
        raise FuturesSessionCloseError("session-close bundle JSON 无效") from exc


def require_current_futures_session_close_binding(
    *,
    bundle_hash: str,
    policy_identities: tuple[tuple[str, int, str], ...],
) -> None:
    """执行旧计划前复核其 session-close 发布 revision 仍是当前值。"""

    current = load_futures_session_close_bundle()
    if current.bundle_hash != bundle_hash:
        raise FuturesSessionCloseError("session-close bundle 已漂移，必须重新 admit")
    current_by_key = {
        (item.policy_id, item.revision): item.policy_hash for item in current.policies
    }
    if (
        not policy_identities
        or len({item[:2] for item in policy_identities}) != len(policy_identities)
        or any(current_by_key.get(item[:2]) != item[2] for item in policy_identities)
    ):
        raise FuturesSessionCloseError(
            "session-close policy revision 已漂移，必须重新 admit"
        )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise FuturesSessionCloseError(f"{field} 必须是字符串键 mapping")
    return value


def _exact(value: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise FuturesSessionCloseError(f"{field} schema 不匹配")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FuturesSessionCloseError(f"{field} 必须是非空字符串")
    return value


def _hash(value: object, field: str) -> str:
    text = _text(value, field)
    if _SHA256.fullmatch(text) is None:
        raise FuturesSessionCloseError(f"{field} 必须是 sha256")
    return text


__all__ = [
    "FUTURES_SESSION_CLOSE_BUNDLE_RESOURCE",
    "FUTURES_SESSION_CLOSE_BUNDLE_VERSION",
    "FUTURES_SESSION_CLOSE_POLICY_IDS",
    "FuturesSessionCloseError",
    "FuturesSessionClosePolicyBinding",
    "FuturesSessionClosePolicyBundle",
    "load_futures_session_close_bundle",
    "require_current_futures_session_close_binding",
]
