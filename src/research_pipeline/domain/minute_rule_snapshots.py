"""平台分钟能力覆盖内的市场规则快照与 PIT 解析。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from importlib.resources import files
import json
from pathlib import Path
import re
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.minute_reference import (
    load_minute_capability_manifest,
    require_current_minute_capability_binding,
)
from .models import DomainContractError
from .session_calendar import (
    CURRENT_SESSION_POLICY_BUNDLE_HASH,
    SessionCalendarError,
    SessionInstrumentMetadata,
)


MINUTE_RULE_BUNDLE_VERSION = "minute-rule-snapshot-bundle-v1"
MINUTE_RULE_VERSION = "minute-rule-snapshot-v1"
MINUTE_RULE_SOURCE_VERSION = "minute-rule-source-v1"
MINUTE_RULE_CAPABILITY_CONSUMER = "domain.minute.market_rule_snapshots"
CURRENT_MINUTE_RULE_BUNDLE_HASH = (
    "781d7dde9d15aa003ee23ebf6d49f3371ab966e9eb3d56c8f48a63203dd70051"
)
CURRENT_MINUTE_RULE_COVERAGE_HASH = (
    "6039a2bcb3322061507a99fd13a6f1d093b457fb2697c9e556ed38d0347fbbb4"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class MinuteRuleSnapshotError(DomainContractError):
    """规则快照缺失、冲突、不支持或在决策时尚不可见。"""

    error_code = "rule_snapshot_unsupported"


@dataclass(frozen=True)
class MinuteRuleSource:
    source_id: str
    source_kind: str
    locator: str
    published_on: date
    claim_ceiling: str
    contract_version: str = MINUTE_RULE_SOURCE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != MINUTE_RULE_SOURCE_VERSION:
            raise MinuteRuleSnapshotError("规则来源合同版本不受支持")
        if any(not isinstance(item, str) or not item.strip() for item in (
            self.source_id, self.source_kind, self.locator, self.claim_ceiling
        )):
            raise MinuteRuleSnapshotError("规则来源字段不完整")
        if self.source_kind not in {
            "official_citation", "repository_evidence", "repository_snapshot",
            "platform_capability", "scope_observation",
        }:
            raise MinuteRuleSnapshotError("规则来源类型不受支持")
        if type(self.published_on) is not date:
            raise MinuteRuleSnapshotError("published_on 必须是明确日期")
        if self.claim_ceiling != "research_observation":
            raise MinuteRuleSnapshotError("分钟规则只能声明 research_observation")

    @property
    def source_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "source_kind": self.source_kind,
            "locator": self.locator,
            "published_on": self.published_on.isoformat(),
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> "MinuteRuleSource":
        payload = _mapping(value, "MinuteRuleSource")
        _exact(payload, {
            "source_id", "source_kind", "locator", "published_on",
            "claim_ceiling", "contract_version",
        }, "MinuteRuleSource")
        return cls(
            _text(payload["source_id"], "source_id"),
            _text(payload["source_kind"], "source_kind"),
            _text(payload["locator"], "locator"),
            _date(payload["published_on"], "published_on"),
            _text(payload["claim_ceiling"], "claim_ceiling"),
            _text(payload["contract_version"], "contract_version"),
        )


@dataclass(frozen=True)
class MinuteRuleSnapshot:
    rule_id: str
    revision: int
    instrument_id: str
    asset_class: str
    status: str
    effective_from: date
    effective_to: date
    available_at: datetime | None
    source_ids: tuple[str, ...]
    parameters: tuple[tuple[str, object], ...]
    unsupported_reason: str | None
    contract_version: str = MINUTE_RULE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != MINUTE_RULE_VERSION or self.revision < 1:
            raise MinuteRuleSnapshotError("规则合同版本或 revision 无效")
        if any(not isinstance(item, str) or not item.strip() for item in (
            self.rule_id, self.instrument_id, self.asset_class
        )):
            raise MinuteRuleSnapshotError("规则身份字段不完整")
        if self.status not in {"supported", "unsupported"}:
            raise MinuteRuleSnapshotError("规则状态不受支持")
        if self.effective_from > self.effective_to:
            raise MinuteRuleSnapshotError("规则有效期倒置")
        if self.source_ids != tuple(sorted(set(self.source_ids))):
            raise MinuteRuleSnapshotError("source_ids 必须排序且唯一")
        if self.parameters != tuple(sorted(self.parameters, key=lambda item: item[0])):
            raise MinuteRuleSnapshotError("规则参数必须按名称排序")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise MinuteRuleSnapshotError("规则参数不能重复")
        if self.status == "supported":
            if self.available_at is None or not self.source_ids or self.unsupported_reason is not None:
                raise MinuteRuleSnapshotError("支持规则必须有可见时点和来源，且不能有拒绝原因")
            _aware(self.available_at, "available_at")
        elif (
            self.available_at is not None
            or self.parameters
            or not self.unsupported_reason
        ):
            raise MinuteRuleSnapshotError("不支持规则只能携带来源和稳定拒绝原因")

    @property
    def snapshot_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "rule_id": self.rule_id,
            "revision": self.revision,
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "status": self.status,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": self.effective_to.isoformat(),
            "available_at": None if self.available_at is None else self.available_at.isoformat(timespec="seconds"),
            "source_ids": list(self.source_ids),
            "parameters": {key: value for key, value in self.parameters},
            "unsupported_reason": self.unsupported_reason,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> "MinuteRuleSnapshot":
        payload = _mapping(value, "MinuteRuleSnapshot")
        _exact(payload, {
            "rule_id", "revision", "instrument_id", "asset_class", "status",
            "effective_from", "effective_to", "available_at", "source_ids",
            "parameters", "unsupported_reason", "contract_version",
        }, "MinuteRuleSnapshot")
        parameters = _mapping(payload["parameters"], "parameters")
        return cls(
            _text(payload["rule_id"], "rule_id"),
            _integer(payload["revision"], "revision"),
            _text(payload["instrument_id"], "instrument_id"),
            _text(payload["asset_class"], "asset_class"),
            _text(payload["status"], "status"),
            _date(payload["effective_from"], "effective_from"),
            _date(payload["effective_to"], "effective_to"),
            None if payload["available_at"] is None else _datetime(payload["available_at"], "available_at"),
            _strings(payload["source_ids"], "source_ids", allow_empty=True),
            tuple(sorted(parameters.items())),
            _optional_text(payload["unsupported_reason"], "unsupported_reason"),
            _text(payload["contract_version"], "contract_version"),
        )


@dataclass(frozen=True)
class MinuteRuleBinding:
    rule: MinuteRuleSnapshot
    source_hashes: tuple[tuple[str, str], ...]
    decision_at: datetime

    @property
    def identity_hash(self) -> str:
        return typed_canonical_hash({
            "snapshot_hash": self.rule.snapshot_hash,
            "source_hashes": dict(self.source_hashes),
        })


@dataclass(frozen=True)
class MinuteRuleSnapshotBundle:
    bundle_id: str
    capability_binding: tuple[tuple[str, str], ...]
    sources: tuple[MinuteRuleSource, ...]
    instruments: tuple[SessionInstrumentMetadata, ...]
    rules: tuple[MinuteRuleSnapshot, ...]
    bundle_hash: str
    contract_version: str = MINUTE_RULE_BUNDLE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != MINUTE_RULE_BUNDLE_VERSION:
            raise MinuteRuleSnapshotError("规则 bundle 版本不受支持")
        binding = dict(self.capability_binding)
        if set(binding) != {
            "consumer_id", "minute_capability_manifest_hash",
            "contract_version", "binding_hash",
        }:
            raise MinuteRuleSnapshotError("规则 capability binding schema 无效")
        try:
            require_current_minute_capability_binding(
                consumer_id=binding["consumer_id"],
                manifest_hash=binding["minute_capability_manifest_hash"],
                contract_version=binding["contract_version"],
                binding_hash=binding["binding_hash"],
            )
        except Exception as exc:
            raise MinuteRuleSnapshotError("规则 bundle 未绑定当前分钟能力 manifest") from exc
        if binding["consumer_id"] != MINUTE_RULE_CAPABILITY_CONSUMER:
            raise MinuteRuleSnapshotError("规则 bundle consumer 不一致")
        if self.sources != tuple(sorted(self.sources, key=lambda item: item.source_id)):
            raise MinuteRuleSnapshotError("规则来源必须排序")
        source_ids = {item.source_id for item in self.sources}
        if len(source_ids) != len(self.sources):
            raise MinuteRuleSnapshotError("规则来源 ID 重复")
        for source in self.sources:
            if source.source_kind == "repository_snapshot":
                _require_locator_hash(
                    source.locator,
                    CURRENT_SESSION_POLICY_BUNDLE_HASH,
                    "session repository source",
                )
            elif source.source_kind == "platform_capability":
                _require_locator_hash(
                    source.locator,
                    binding["minute_capability_manifest_hash"],
                    "platform capability source",
                )
        if self.instruments != tuple(sorted(self.instruments, key=lambda item: item.instrument_id)):
            raise MinuteRuleSnapshotError("规则 instrument classification 必须排序")
        instruments = {item.instrument_id: item for item in self.instruments}
        if len(instruments) != len(self.instruments):
            raise MinuteRuleSnapshotError("规则 instrument_id 重复")
        if self.rules != tuple(sorted(self.rules, key=lambda item: (item.instrument_id, item.rule_id, item.revision))):
            raise MinuteRuleSnapshotError("规则快照必须排序")
        keys = {(item.instrument_id, item.rule_id, item.revision) for item in self.rules}
        if len(keys) != len(self.rules):
            raise MinuteRuleSnapshotError("规则快照身份重复")
        if any(not set(item.source_ids) <= source_ids for item in self.rules):
            raise MinuteRuleSnapshotError("规则引用了未知来源")
        if any(
            item.instrument_id not in instruments
            or item.asset_class != instruments[item.instrument_id].asset_class
            for item in self.rules
        ):
            raise MinuteRuleSnapshotError("规则与 instrument classification 不一致")
        manifest = load_minute_capability_manifest()
        expected_instruments = {
            item.instrument.instrument_id: item for item in manifest.coverages
        }
        if set(instruments) != set(expected_instruments):
            raise MinuteRuleSnapshotError("规则 instrument 未完整覆盖平台分钟能力")
        for instrument_id, instrument in instruments.items():
            coverage = expected_instruments[instrument_id]
            if (
                instrument.asset_class != coverage.instrument.asset_class
                or instrument.instrument_class != coverage.instrument.asset_subtype
                or instrument.role != coverage.instrument.role
                or instrument.metadata_hash != coverage.instrument.identity_hash
            ):
                raise MinuteRuleSnapshotError("规则 instrument 与平台分钟能力不一致")
            actual_rule_ids = {
                item.rule_id for item in self.rules
                if item.instrument_id == instrument_id
            }
            if actual_rule_ids != set(coverage.required_rule_ids):
                raise MinuteRuleSnapshotError("规则矩阵缺少能力 manifest 声明的覆盖")
        if self.bundle_hash != typed_canonical_hash(self.identity_payload()):
            raise MinuteRuleSnapshotError("规则 bundle hash 不一致")

    def identity_payload(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "capability_binding": dict(self.capability_binding),
            "sources": [item.to_dict() for item in self.sources],
            "instruments": [item.to_dict() for item in self.instruments],
            "rules": [item.to_dict() for item in self.rules],
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.identity_payload(), "bundle_hash": self.bundle_hash}

    def scope_coverage_payload(self) -> dict[str, object]:
        return {
            "rules": [
                {
                    "instrument_id": item.instrument_id,
                    "asset_class": item.asset_class,
                    "rule_id": item.rule_id,
                    "effective_from": item.effective_from.isoformat(),
                    "effective_to": item.effective_to.isoformat(),
                    "status": item.status,
                }
                for item in self.rules
            ]
        }

    @classmethod
    def from_dict(cls, value: object) -> "MinuteRuleSnapshotBundle":
        payload = _mapping(value, "MinuteRuleSnapshotBundle")
        _exact(payload, {"bundle_id", "capability_binding", "sources", "instruments", "rules", "bundle_hash", "contract_version"}, "MinuteRuleSnapshotBundle")
        binding = _mapping(payload["capability_binding"], "capability_binding")
        try:
            instruments = tuple(
                SessionInstrumentMetadata.from_dict(item)
                for item in _list(payload["instruments"], "instruments")
            )
        except SessionCalendarError as exc:
            raise MinuteRuleSnapshotError("规则 instrument classification 无效") from exc
        return cls(
            _text(payload["bundle_id"], "bundle_id"),
            tuple(sorted((str(key), str(item)) for key, item in binding.items())),
            tuple(MinuteRuleSource.from_dict(item) for item in _list(payload["sources"], "sources")),
            instruments,
            tuple(MinuteRuleSnapshot.from_dict(item) for item in _list(payload["rules"], "rules")),
            _hash(payload["bundle_hash"], "bundle_hash"),
            _text(payload["contract_version"], "contract_version"),
        )


class MinuteRuleResolver:
    def __init__(self, bundle: MinuteRuleSnapshotBundle) -> None:
        self.bundle = bundle

    def resolve(self, *, rule_id: str, instrument_id: str, effective_on: date, as_of: datetime) -> MinuteRuleBinding:
        current = _aware(as_of, "as_of")
        if type(effective_on) is not date:
            raise MinuteRuleSnapshotError("effective_on 必须是明确交易日，不能传 datetime")
        candidates = tuple(
            item for item in self.bundle.rules
            if item.rule_id == rule_id
            and item.instrument_id == instrument_id
            and item.effective_from <= effective_on <= item.effective_to
        )
        if len(candidates) != 1:
            raise MinuteRuleSnapshotError("rule_snapshot_missing_or_conflicting")
        rule = candidates[0]
        if rule.status != "supported":
            raise MinuteRuleSnapshotError(f"rule_snapshot_unsupported:{rule.unsupported_reason}")
        if rule.available_at is None or rule.available_at > current:
            raise MinuteRuleSnapshotError("rule_snapshot_unavailable_at_decision")
        sources = {item.source_id: item.source_hash for item in self.bundle.sources}
        return MinuteRuleBinding(
            rule,
            tuple((source_id, sources[source_id]) for source_id in rule.source_ids),
            current,
        )

    def resolve_actual_contract(self, *, requested_id: str, effective_on: date, as_of: datetime) -> str:
        binding = self.resolve(
            rule_id="rule.cn_futures.actual_contract_mapping.v1",
            instrument_id=requested_id,
            effective_on=effective_on,
            as_of=as_of,
        )
        actual = dict(binding.rule.parameters).get("actual_contract_id")
        if not isinstance(actual, str) or not actual.strip():
            raise MinuteRuleSnapshotError("actual_contract_mapping 参数无效")
        return actual


def load_minute_rule_snapshot_bundle(
    path: str | Path | None = None,
    *,
    expected_bundle_hash: str | None = None,
) -> MinuteRuleSnapshotBundle:
    is_default = path is None
    if path is None:
        resource = files("research_pipeline.domain").joinpath(
            "rule_snapshots/minute_reference_rules.json"
        )
        raw = resource.read_text(encoding="utf-8")
    else:
        raw = Path(path).read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MinuteRuleSnapshotError("规则 bundle 不是有效 JSON") from exc
    bundle = MinuteRuleSnapshotBundle.from_dict(payload)
    anchor = CURRENT_MINUTE_RULE_BUNDLE_HASH if is_default else expected_bundle_hash
    if anchor is None or bundle.bundle_hash != _hash(anchor, "expected_bundle_hash"):
        raise MinuteRuleSnapshotError("规则 bundle 与预期发布锚点不一致")
    return bundle


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise MinuteRuleSnapshotError(f"{field} 必须是字符串键 mapping")
    return value


def _exact(value: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise MinuteRuleSnapshotError(f"{field} schema 不匹配")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MinuteRuleSnapshotError(f"{field} 必须是非空字符串")
    return value


def _optional_text(value: object, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise MinuteRuleSnapshotError(f"{field} 必须是整数")
    return value


def _list(value: object, field: str, *, allow_empty: bool = False) -> list[object]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise MinuteRuleSnapshotError(f"{field} 必须是列表")
    return value


def _strings(value: object, field: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    return tuple(_text(item, field) for item in _list(value, field, allow_empty=allow_empty))


def _date(value: object, field: str) -> date:
    try:
        return date.fromisoformat(_text(value, field))
    except ValueError as exc:
        raise MinuteRuleSnapshotError(f"{field} 必须是 ISO 日期") from exc


def _datetime(value: object, field: str) -> datetime:
    raw = _text(value, field)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise MinuteRuleSnapshotError(f"{field} 必须是带时区 ISO 时间") from exc
    parsed = _aware(parsed, field)
    if raw != parsed.isoformat(timespec="seconds"):
        raise MinuteRuleSnapshotError(f"{field} 必须使用规范秒精度 ISO 时间")
    return parsed


def _aware(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MinuteRuleSnapshotError(f"{field} 必须是带时区时点")
    return value


def _hash(value: object, field: str) -> str:
    text = _text(value, field)
    if _SHA256.fullmatch(text) is None:
        raise MinuteRuleSnapshotError(f"{field} 必须是 sha256")
    return text


def _require_locator_hash(locator: str, expected_hash: str, field: str) -> None:
    parts = locator.rsplit("#", 1)
    if len(parts) != 2 or not parts[0].strip() or parts[1] != expected_hash:
        raise MinuteRuleSnapshotError(f"{field} 定位符未绑定已发布摘要")


__all__ = [
    "CURRENT_MINUTE_RULE_BUNDLE_HASH",
    "CURRENT_MINUTE_RULE_COVERAGE_HASH",
    "MINUTE_RULE_BUNDLE_VERSION",
    "MinuteRuleBinding",
    "MinuteRuleResolver",
    "MinuteRuleSnapshot",
    "MinuteRuleSnapshotBundle",
    "MinuteRuleSnapshotError",
    "MinuteRuleSource",
    "load_minute_rule_snapshot_bundle",
]
