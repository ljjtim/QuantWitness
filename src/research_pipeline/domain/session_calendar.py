"""平台分钟能力覆盖内的版本化交易时段解析。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from importlib.resources import files
import json
from pathlib import Path
import re
from typing import Mapping
from zoneinfo import ZoneInfo

from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    normalize_asset_class,
    require_canonical_asset_class,
    require_instrument_type,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.minute_reference import (
    load_minute_capability_manifest,
    require_current_minute_capability_binding,
)

from .sessions import SessionSegment, TradingSession
from .time import CN_MARKET_TIMEZONE, TimeContractError, require_aware_datetime


SESSION_POLICY_BUNDLE_VERSION = "minute-session-policy-bundle-v1"
SESSION_POLICY_VERSION = "minute-session-policy-v1"
SESSION_SEGMENT_TEMPLATE_VERSION = "minute-session-segment-template-v1"
SESSION_INSTRUMENT_VERSION = "minute-session-instrument-v1"
SESSION_CAPABILITY_CONSUMER = "domain.minute.session_calendar"
CURRENT_SESSION_POLICY_BUNDLE_HASH = (
    "ae4be8044e5c8bfdc07aed7d7a8b19d1066ea0eb1038e777b1b6f76fb439bac2"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ZONE = ZoneInfo(CN_MARKET_TIMEZONE)


class SessionCalendarError(TimeContractError):
    """分钟 session policy 缺失、冲突、越界或证据失配。"""

    error_code = "session_calendar_invalid"


def _normalize_session_asset_class(value: object) -> str:
    try:
        return normalize_asset_class(value)
    except AssetTaxonomyError as exc:
        raise SessionCalendarError(str(exc)) from exc


@dataclass(frozen=True)
class SessionInstrumentMetadata:
    instrument_id: str
    asset_class: str
    instrument_class: str
    exchange: str
    product_class: str
    role: str
    metadata_hash: str
    contract_version: str = SESSION_INSTRUMENT_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != SESSION_INSTRUMENT_VERSION:
            raise SessionCalendarError("session instrument version 不受支持")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (
                self.instrument_id,
                self.asset_class,
                self.instrument_class,
                self.exchange,
                self.product_class,
                self.role,
            )
        ):
            raise SessionCalendarError("session instrument metadata 不完整")
        try:
            require_canonical_asset_class(self.asset_class)
            require_instrument_type(self.asset_class, self.instrument_class)
        except AssetTaxonomyError as exc:
            raise SessionCalendarError(str(exc)) from exc
        expected_role = "benchmark" if self.asset_class == "cn_index" else "tradable"
        if self.role != expected_role:
            raise SessionCalendarError("asset_class 与 session role 不一致")
        if _SHA256.fullmatch(self.metadata_hash) is None:
            raise SessionCalendarError("session instrument metadata_hash 无效")

    @property
    def classification(self) -> tuple[str, str, str, str, str]:
        return (
            self.asset_class,
            self.instrument_class,
            self.role,
            self.exchange,
            self.product_class,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "instrument_class": self.instrument_class,
            "exchange": self.exchange,
            "product_class": self.product_class,
            "role": self.role,
            "metadata_hash": self.metadata_hash,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionInstrumentMetadata":
        payload = _mapping(value, "SessionInstrumentMetadata")
        _exact(
            payload,
            {
                "instrument_id",
                "asset_class",
                "instrument_class",
                "exchange",
                "product_class",
                "role",
                "metadata_hash",
                "contract_version",
            },
            "SessionInstrumentMetadata",
        )
        return cls(
            _text(payload["instrument_id"], "instrument_id"),
            _normalize_session_asset_class(payload["asset_class"]),
            _text(payload["instrument_class"], "instrument_class"),
            _text(payload["exchange"], "exchange"),
            _text(payload["product_class"], "product_class"),
            _text(payload["role"], "role"),
            _hash(payload["metadata_hash"], "metadata_hash"),
            _text(payload["contract_version"], "contract_version"),
        )


@dataclass(frozen=True)
class SessionSegmentTemplate:
    segment_id: str
    start_day_offset: int
    start_time: time
    end_day_offset: int
    end_time: time
    phase: str
    bar_eligible: bool
    bucket_anchor_day_offset: int
    bucket_anchor_time: time
    contract_version: str = SESSION_SEGMENT_TEMPLATE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != SESSION_SEGMENT_TEMPLATE_VERSION:
            raise SessionCalendarError("segment template version 不受支持")
        if not self.segment_id.strip() or self.phase not in {
            "day",
            "night",
            "auction",
            "settlement",
        }:
            raise SessionCalendarError("segment template 身份或 phase 无效")
        if type(self.bar_eligible) is not bool:
            raise SessionCalendarError("bar_eligible 必须是 bool")
        if not all(
            type(value) is int
            for value in (
                self.start_day_offset,
                self.end_day_offset,
                self.bucket_anchor_day_offset,
            )
        ):
            raise SessionCalendarError("segment day offset 必须是整数")
        anchor = date(2000, 1, 3)
        starts_at = self._datetime(anchor, self.start_day_offset, self.start_time)
        ends_at = self._datetime(anchor, self.end_day_offset, self.end_time)
        bucket = self._datetime(
            anchor,
            self.bucket_anchor_day_offset,
            self.bucket_anchor_time,
        )
        if starts_at >= ends_at or not starts_at <= bucket <= ends_at:
            raise SessionCalendarError("segment template 时间或 bucket anchor 无效")

    @staticmethod
    def _datetime(trading_date: date, offset: int, value: time) -> datetime:
        return datetime.combine(trading_date + timedelta(days=offset), value, tzinfo=_ZONE)

    def build(self, trading_date: date) -> SessionSegment:
        return SessionSegment(
            self.segment_id,
            self._datetime(trading_date, self.start_day_offset, self.start_time),
            self._datetime(trading_date, self.end_day_offset, self.end_time),
            trading_date,
            self.phase,
            self.bar_eligible,
            self._datetime(
                trading_date,
                self.bucket_anchor_day_offset,
                self.bucket_anchor_time,
            ),
            "completed-bar-right-closed-v1",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "segment_id": self.segment_id,
            "start_day_offset": self.start_day_offset,
            "start_time": self.start_time.isoformat(timespec="seconds"),
            "end_day_offset": self.end_day_offset,
            "end_time": self.end_time.isoformat(timespec="seconds"),
            "phase": self.phase,
            "bar_eligible": self.bar_eligible,
            "bucket_anchor_day_offset": self.bucket_anchor_day_offset,
            "bucket_anchor_time": self.bucket_anchor_time.isoformat(timespec="seconds"),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionSegmentTemplate":
        payload = _mapping(value, "SessionSegmentTemplate")
        expected = {
            "segment_id",
            "start_day_offset",
            "start_time",
            "end_day_offset",
            "end_time",
            "phase",
            "bar_eligible",
            "bucket_anchor_day_offset",
            "bucket_anchor_time",
            "contract_version",
        }
        _exact(payload, expected, "SessionSegmentTemplate")
        return cls(
            _text(payload["segment_id"], "segment_id"),
            _integer(payload["start_day_offset"], "start_day_offset"),
            _time(payload["start_time"], "start_time"),
            _integer(payload["end_day_offset"], "end_day_offset"),
            _time(payload["end_time"], "end_time"),
            _text(payload["phase"], "phase"),
            _boolean(payload["bar_eligible"], "bar_eligible"),
            _integer(
                payload["bucket_anchor_day_offset"],
                "bucket_anchor_day_offset",
            ),
            _time(payload["bucket_anchor_time"], "bucket_anchor_time"),
            _text(payload["contract_version"], "contract_version"),
        )


@dataclass(frozen=True)
class SessionPolicyRevision:
    policy_id: str
    revision: int
    instrument: SessionInstrumentMetadata
    effective_from: date
    effective_to: date
    trading_dates: tuple[date, ...]
    segments: tuple[SessionSegmentTemplate, ...]
    evidence_hashes: tuple[tuple[str, str], ...]
    auction_bar_policy: str
    previous_policy_hash: str | None
    claim_ceiling: str = "research_observation"
    contract_version: str = SESSION_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != SESSION_POLICY_VERSION:
            raise SessionCalendarError("session policy version 不受支持")
        if not self.policy_id.strip() or self.revision < 1:
            raise SessionCalendarError("session policy id/revision 无效")
        if self.effective_from > self.effective_to:
            raise SessionCalendarError("session policy 生效期倒置")
        if (
            not self.trading_dates
            or self.trading_dates != tuple(sorted(set(self.trading_dates)))
            or any(
                item < self.effective_from or item > self.effective_to
                for item in self.trading_dates
            )
        ):
            raise SessionCalendarError("trading_dates 必须在生效期内严格递增")
        if not self.segments or len({item.segment_id for item in self.segments}) != len(
            self.segments
        ):
            raise SessionCalendarError("session policy segments 不能为空或重复")
        built = tuple(item.build(self.trading_dates[0]) for item in self.segments)
        if built != tuple(sorted(built, key=lambda item: (item.starts_at, item.segment_id))):
            raise SessionCalendarError("session policy segments 必须按时间排序")
        for index, item in enumerate(built):
            if index and built[index - 1].ends_at > item.starts_at:
                raise SessionCalendarError("session policy segments 不能重叠")
        if not self.evidence_hashes or self.evidence_hashes != tuple(
            sorted(self.evidence_hashes)
        ):
            raise SessionCalendarError("session evidence_hashes 必须排序且非空")
        if len({source_id for source_id, _ in self.evidence_hashes}) != len(
            self.evidence_hashes
        ) or any(
            not source_id.strip() or _SHA256.fullmatch(content_hash) is None
            for source_id, content_hash in self.evidence_hashes
        ):
            raise SessionCalendarError("session evidence source/hash 无效")
        if self.auction_bar_policy not in {
            "no_independent_auction_bar_observed",
            "not_in_reference_scope",
        }:
            raise SessionCalendarError("auction_bar_policy 不受支持")
        if self.revision == 1 and self.previous_policy_hash is not None:
            raise SessionCalendarError("首个 session policy revision 不能有前序 hash")
        if self.revision > 1 and (
            self.previous_policy_hash is None
            or _SHA256.fullmatch(self.previous_policy_hash) is None
        ):
            raise SessionCalendarError("后续 session policy revision 必须绑定前序 hash")
        if self.claim_ceiling != "research_observation":
            raise SessionCalendarError("参考 session claim_ceiling 只能是 research_observation")

    @property
    def policy_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "revision": self.revision,
            "instrument": self.instrument.to_dict(),
            "effective_from": self.effective_from.isoformat(),
            "effective_to": self.effective_to.isoformat(),
            "trading_dates": [item.isoformat() for item in self.trading_dates],
            "segments": [item.to_dict() for item in self.segments],
            "evidence_hashes": [
                {"source_id": source_id, "content_hash": content_hash}
                for source_id, content_hash in self.evidence_hashes
            ],
            "auction_bar_policy": self.auction_bar_policy,
            "previous_policy_hash": self.previous_policy_hash,
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionPolicyRevision":
        payload = _mapping(value, "SessionPolicyRevision")
        expected = {
            "policy_id",
            "revision",
            "instrument",
            "effective_from",
            "effective_to",
            "trading_dates",
            "segments",
            "evidence_hashes",
            "auction_bar_policy",
            "previous_policy_hash",
            "claim_ceiling",
            "contract_version",
        }
        _exact(payload, expected, "SessionPolicyRevision")
        return cls(
            _text(payload["policy_id"], "policy_id"),
            _integer(payload["revision"], "revision"),
            SessionInstrumentMetadata.from_dict(payload["instrument"]),
            _date(payload["effective_from"], "effective_from"),
            _date(payload["effective_to"], "effective_to"),
            _dates(payload["trading_dates"], "trading_dates"),
            tuple(
                SessionSegmentTemplate.from_dict(item)
                for item in _list(payload["segments"], "segments")
            ),
            _evidence_hashes(payload["evidence_hashes"]),
            _text(payload["auction_bar_policy"], "auction_bar_policy"),
            _optional_hash(payload["previous_policy_hash"], "previous_policy_hash"),
            _text(payload["claim_ceiling"], "claim_ceiling"),
            _text(payload["contract_version"], "contract_version"),
        )

    def build_session(self, trading_date: date, *, scope_binding_hash: str) -> TradingSession:
        if trading_date not in self.trading_dates:
            raise SessionCalendarError("目标日期不是 policy 明确支持的交易日")
        return TradingSession(
            f"{self.policy_id}@{self.revision}:{self.instrument.instrument_id}:{trading_date.isoformat()}",
            self.instrument.asset_class,
            trading_date,
            tuple(item.build(trading_date) for item in self.segments),
            self.policy_id,
            self.revision,
            self.instrument.instrument_id,
            self.instrument.classification,
            self.policy_hash,
            scope_binding_hash,
        )


@dataclass(frozen=True)
class SessionPolicyBundle:
    bundle_id: str
    capability_binding: tuple[tuple[str, str], ...]
    inventory_evidence_hash: str
    policies: tuple[SessionPolicyRevision, ...]
    bundle_hash: str
    contract_version: str = SESSION_POLICY_BUNDLE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != SESSION_POLICY_BUNDLE_VERSION:
            raise SessionCalendarError("session policy bundle version 不受支持")
        if not self.bundle_id.strip() or _SHA256.fullmatch(self.inventory_evidence_hash) is None:
            raise SessionCalendarError("session policy bundle 身份或证据摘要无效")
        binding = dict(self.capability_binding)
        if set(binding) != {
            "consumer_id",
            "minute_capability_manifest_hash",
            "contract_version",
            "binding_hash",
        }:
            raise SessionCalendarError("session capability binding schema 无效")
        try:
            require_current_minute_capability_binding(
                consumer_id=binding["consumer_id"],
                manifest_hash=binding["minute_capability_manifest_hash"],
                contract_version=binding["contract_version"],
                binding_hash=binding["binding_hash"],
            )
        except Exception as exc:
            raise SessionCalendarError("session policy 未绑定当前分钟能力 manifest") from exc
        if binding["consumer_id"] != SESSION_CAPABILITY_CONSUMER:
            raise SessionCalendarError("session policy consumer 不一致")
        ordered = tuple(sorted(self.policies, key=lambda item: (item.policy_id, item.revision)))
        if not ordered or ordered != self.policies:
            raise SessionCalendarError("session policies 必须排序且非空")
        keys = {(item.policy_id, item.revision) for item in ordered}
        if len(keys) != len(ordered):
            raise SessionCalendarError("session policy id/revision 重复")
        by_key = {(item.policy_id, item.revision): item for item in ordered}
        for item in ordered:
            evidence = dict(item.evidence_hashes)
            if (
                evidence.get("minute.capability.inventory.v1")
                != self.inventory_evidence_hash
                or evidence.get("minute.capability.manifest.v1")
                != binding["minute_capability_manifest_hash"]
            ):
                raise SessionCalendarError("session policy 证据未闭合到能力 manifest")
            if item.revision == 1:
                continue
            previous = by_key.get((item.policy_id, item.revision - 1))
            if previous is None or item.previous_policy_hash != previous.policy_hash:
                raise SessionCalendarError("session policy revision chain 不闭合")
        manifest = load_minute_capability_manifest()
        by_instrument = {
            item.instrument.instrument_id: item for item in manifest.coverages
        }
        if set(by_instrument) != {item.instrument.instrument_id for item in ordered}:
            raise SessionCalendarError("session policy 未完整覆盖平台分钟能力")
        for item in ordered:
            coverage = by_instrument[item.instrument.instrument_id]
            observation = coverage.instrument.observation
            if (
                item.instrument.asset_class != coverage.instrument.asset_class
                or item.instrument.instrument_class != coverage.instrument.asset_subtype
                or item.instrument.role != coverage.instrument.role
                or item.instrument.metadata_hash != coverage.instrument.identity_hash
                or item.effective_from.isoformat() != observation.trading_date_start
                or item.effective_to.isoformat() != observation.trading_date_end
                or tuple(day.isoformat() for day in item.trading_dates)
                != tuple(day for day, _ in observation.bars_per_trading_date)
            ):
                raise SessionCalendarError("session policy 与平台分钟能力覆盖不一致")
        if self.bundle_hash != typed_canonical_hash(self.identity_payload()):
            raise SessionCalendarError("session policy bundle hash 不一致")

    @property
    def capability_binding_hash(self) -> str:
        return dict(self.capability_binding)["binding_hash"]

    def identity_payload(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "capability_binding": dict(self.capability_binding),
            "inventory_evidence_hash": self.inventory_evidence_hash,
            "policies": [item.to_dict() for item in self.policies],
            "contract_version": self.contract_version,
        }

    def scope_coverage_payload(self) -> dict[str, object]:
        coverage = {
            (
                item.instrument.instrument_id,
                item.instrument.asset_class,
                item.instrument.instrument_class,
                item.instrument.role,
                item.instrument.metadata_hash,
                item.effective_from.isoformat(),
                item.effective_to.isoformat(),
                tuple(day.isoformat() for day in item.trading_dates),
            )
            for item in self.policies
        }
        return {
            "instruments": [
                {
                    "instrument_id": instrument_id,
                    "asset_class": asset_class,
                    "instrument_class": instrument_class,
                    "role": role,
                    "metadata_hash": metadata_hash,
                    "effective_from": effective_from,
                    "effective_to": effective_to,
                    "trading_dates": list(trading_dates),
                }
                for (
                    instrument_id,
                    asset_class,
                    instrument_class,
                    role,
                    metadata_hash,
                    effective_from,
                    effective_to,
                    trading_dates,
                ) in sorted(coverage)
            ]
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.identity_payload(), "bundle_hash": self.bundle_hash}

    @classmethod
    def from_dict(cls, value: object) -> "SessionPolicyBundle":
        payload = _mapping(value, "SessionPolicyBundle")
        expected = {
            "bundle_id",
            "capability_binding",
            "inventory_evidence_hash",
            "policies",
            "bundle_hash",
            "contract_version",
        }
        _exact(payload, expected, "SessionPolicyBundle")
        binding = _mapping(payload["capability_binding"], "capability_binding")
        return cls(
            _text(payload["bundle_id"], "bundle_id"),
            tuple(sorted((str(key), str(item)) for key, item in binding.items())),
            _hash(payload["inventory_evidence_hash"], "inventory_evidence_hash"),
            tuple(
                SessionPolicyRevision.from_dict(item)
                for item in _list(payload["policies"], "policies")
            ),
            _hash(payload["bundle_hash"], "bundle_hash"),
            _text(payload["contract_version"], "contract_version"),
        )


class SessionCalendarResolver:
    def __init__(self, bundle: SessionPolicyBundle) -> None:
        self.bundle = bundle

    def resolve_trading_date(
        self,
        instrument: SessionInstrumentMetadata,
        trading_date: object,
        *,
        policy_revision: int,
    ) -> TradingSession:
        current = _strict_trading_date(trading_date, "trading_date")
        policies = self._instrument_policies(instrument, policy_revision)
        matches = tuple(item for item in policies if current in item.trading_dates)
        if len(matches) != 1:
            raise SessionCalendarError("session policy 缺失或冲突，不允许自然日兜底")
        return matches[0].build_session(
            current,
            scope_binding_hash=self.bundle.capability_binding_hash,
        )

    def resolve_instant(
        self,
        instrument: SessionInstrumentMetadata,
        instant: datetime,
        *,
        policy_revision: int,
    ) -> TradingSession:
        current = _session_instant(instant, "instant")
        matches: list[TradingSession] = []
        for policy in self._instrument_policies(instrument, policy_revision):
            for trading_date in policy.trading_dates:
                session = policy.build_session(
                    trading_date,
                    scope_binding_hash=self.bundle.capability_binding_hash,
                )
                if any(item.contains_instant(current) for item in session.segments):
                    matches.append(session)
        if len(matches) != 1:
            raise SessionCalendarError("instant 没有唯一 session，禁止使用 dt.date() 兜底")
        return matches[0]

    def resolve_completed_bar_end(
        self,
        instrument: SessionInstrumentMetadata,
        bar_end: datetime,
        *,
        policy_revision: int,
    ) -> TradingSession:
        current = _session_instant(bar_end, "bar_end")
        matches: list[TradingSession] = []
        for policy in self._instrument_policies(instrument, policy_revision):
            for trading_date in policy.trading_dates:
                session = policy.build_session(
                    trading_date,
                    scope_binding_hash=self.bundle.capability_binding_hash,
                )
                if any(
                    item.contains_completed_bar_end(current)
                    for item in session.segments
                ):
                    matches.append(session)
        if len(matches) != 1:
            raise SessionCalendarError(
                "bar_end 没有唯一可聚合 session，禁止使用 dt.date() 兜底"
            )
        return matches[0]

    def _instrument_policies(
        self,
        instrument: SessionInstrumentMetadata,
        revision: int,
    ) -> tuple[SessionPolicyRevision, ...]:
        if type(revision) is not int or revision < 1:
            raise SessionCalendarError("policy_revision 必须是正整数")
        matches = tuple(
            item
            for item in self.bundle.policies
            if item.instrument == instrument and item.revision == revision
        )
        if not matches:
            raise SessionCalendarError("instrument classification 没有 session policy")
        return matches


def load_session_policy_bundle(
    path: str | Path | None = None,
    *,
    expected_bundle_hash: str | None = None,
) -> SessionPolicyBundle:
    is_default = path is None
    if path is None:
        resource = files("research_pipeline.domain").joinpath(
            "session_policies/minute_reference_sessions.v4.json"
        )
        try:
            raw = resource.read_text(encoding="utf-8")
        except OSError as exc:
            raise SessionCalendarError("默认 session policy bundle 无法读取") from exc
    else:
        try:
            raw = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise SessionCalendarError("session policy bundle 无法读取") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SessionCalendarError("session policy bundle JSON 无效") from exc
    bundle = SessionPolicyBundle.from_dict(payload)
    if is_default:
        expected_bundle_hash = CURRENT_SESSION_POLICY_BUNDLE_HASH
    elif expected_bundle_hash is None:
        raise SessionCalendarError("外部 session policy bundle 必须声明预期发布摘要")
    if bundle.bundle_hash != _hash(expected_bundle_hash, "expected_bundle_hash"):
        raise SessionCalendarError("session policy bundle 与预期发布锚点不一致")
    return bundle


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SessionCalendarError(f"{field} 必须是字符串键 mapping")
    return value


def _exact(value: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise SessionCalendarError(
            f"{field} schema 不匹配；缺失={sorted(expected-set(value))}，"
            f"未知={sorted(set(value)-expected)}"
        )


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SessionCalendarError(f"{field} 必须是非空字符串")
    return value


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise SessionCalendarError(f"{field} 必须是整数")
    return value


def _boolean(value: object, field: str) -> bool:
    if type(value) is not bool:
        raise SessionCalendarError(f"{field} 必须是 bool")
    return value


def _hash(value: object, field: str) -> str:
    text = _text(value, field)
    if _SHA256.fullmatch(text) is None:
        raise SessionCalendarError(f"{field} 必须是 sha256")
    return text


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list) or not value:
        raise SessionCalendarError(f"{field} 必须是非空列表")
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    values = tuple(_text(item, field) for item in _list(value, field))
    if values != tuple(sorted(set(values))):
        raise SessionCalendarError(f"{field} 必须排序且唯一")
    return values


def _optional_hash(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _hash(value, field)


def _evidence_hashes(value: object) -> tuple[tuple[str, str], ...]:
    entries = []
    for item in _list(value, "evidence_hashes"):
        payload = _mapping(item, "evidence_hash")
        _exact(payload, {"source_id", "content_hash"}, "evidence_hash")
        entries.append(
            (
                _text(payload["source_id"], "source_id"),
                _hash(payload["content_hash"], "content_hash"),
            )
        )
    return tuple(entries)


def _date(value: object, field: str) -> date:
    try:
        return date.fromisoformat(_text(value, field))
    except ValueError as exc:
        raise SessionCalendarError(f"{field} 必须是 ISO 日期") from exc


def _strict_trading_date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        raise SessionCalendarError(f"{field} 不接受 datetime，禁止隐式取自然日")
    if isinstance(value, date):
        return value
    return _date(value, field)


def _session_instant(value: datetime, field: str) -> datetime:
    try:
        return require_aware_datetime(value, field).astimezone(_ZONE)
    except TimeContractError as exc:
        raise SessionCalendarError(f"{field} 必须是带时区时点") from exc


def _dates(value: object, field: str) -> tuple[date, ...]:
    return tuple(_date(item, field) for item in _list(value, field))


def _time(value: object, field: str) -> time:
    text = _text(value, field)
    try:
        parsed = time.fromisoformat(text)
    except ValueError as exc:
        raise SessionCalendarError(f"{field} 必须是 ISO 时间") from exc
    if parsed.tzinfo is not None or text != parsed.isoformat(timespec="seconds"):
        raise SessionCalendarError(f"{field} 必须是无时区秒精度时间")
    return parsed


__all__ = [
    "SESSION_POLICY_BUNDLE_VERSION",
    "SESSION_POLICY_VERSION",
    "SessionCalendarError",
    "SessionCalendarResolver",
    "SessionInstrumentMetadata",
    "SessionPolicyBundle",
    "SessionPolicyRevision",
    "SessionSegmentTemplate",
    "load_session_policy_bundle",
]
