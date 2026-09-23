"""可编译数据目录的不可变声明合同。"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    normalize_asset_class,
    require_instrument_type,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from .errors import CatalogFinancialSemanticsError, CatalogParseError
from .policy_validators import validate_policy_payload


CATALOG_CONTRACT_VERSION = "catalog-contract-v2"
LEGACY_CATALOG_CONTRACT_VERSION = "catalog-contract-v1"
_LIFECYCLES = {"draft", "approved", "deprecated", "removed", "blocked"}
_DECISIONS = {"approved", "blocked", "rejected", "retired"}
_POLICY_TYPES = {
    "availability", "adjustment", "reference_scope", "revision", "schema_drift"
}
_FINANCE_REQUIRED = {
    "report_period",
    "publication_date",
    "revision_id",
    "accounting_scope",
    "value_semantics",
    "unit",
    "currency",
    "ttm_policy",
}
_OBSERVATION_MODELS = {
    "market_event",
    "point_in_time",
    "interval_valid",
    "static_reference",
}
_OBSERVATION_KEYS = {
    "market_event": {"event_time_field", "available_time_field"},
    "point_in_time": {"available_time_field", "revision_field"},
    "interval_valid": {
        "effective_from_field",
        "effective_to_field",
        "available_time_field",
    },
    "static_reference": set(),
}


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise CatalogParseError(f"{name} 必须是非空字符串")


def _version(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise CatalogParseError(f"{name} 必须是正整数")


def _tuple(values: tuple[str, ...], name: str, *, allow_empty: bool = False) -> None:
    if not isinstance(values, tuple) or (not values and not allow_empty):
        raise CatalogParseError(f"{name} 必须是{'可空' if allow_empty else '非空'}字符串 tuple")
    if any(not isinstance(item, str) or not item.strip() for item in values):
        raise CatalogParseError(f"{name} 只能包含非空字符串")
    if len(values) != len(set(values)):
        raise CatalogParseError(f"{name} 不能重复")


def _mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping) or any(not isinstance(k, str) for k in value):
        raise CatalogParseError("mapping 必须使用字符串 key")
    return MappingProxyType({key: _freeze(item) for key, item in value.items()})


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


class CatalogRecord:
    def to_dict(self) -> dict[str, Any]:
        return {item.name: _plain(getattr(self, item.name)) for item in fields(self)}

    @property
    def content_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


@dataclass(frozen=True)
class FieldContract(CatalogRecord):
    field_id: str
    logical_name: str
    field_version: int
    data_type: str
    semantic_type: str
    unit: str
    nullable: bool
    availability_policy: str
    frequency_scope: tuple[str, ...]
    instrument_scope: tuple[str, ...]
    adjustment_allowed: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    status: str = "approved"
    evidence_refs: tuple[str, ...] = ()
    finance_semantics: Mapping[str, Any] = field(default_factory=dict)
    observation_model: str = "static_reference"
    observation_keys: Mapping[str, Any] = field(default_factory=dict)
    contract_version: str = CATALOG_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for name in ("field_id", "logical_name", "data_type", "semantic_type", "unit", "availability_policy", "contract_version"):
            _text(getattr(self, name), name)
        _version(self.field_version, "field_version")
        if not isinstance(self.nullable, bool):
            raise CatalogParseError("nullable 必须是 boolean")
        _tuple(self.frequency_scope, "frequency_scope")
        _tuple(self.instrument_scope, "instrument_scope")
        _tuple(self.adjustment_allowed, "adjustment_allowed", allow_empty=True)
        _tuple(self.aliases, "aliases", allow_empty=True)
        _tuple(self.evidence_refs, "evidence_refs", allow_empty=True)
        if self.status not in _LIFECYCLES:
            raise CatalogParseError("field status 非法")
        frozen = _mapping(self.finance_semantics)
        object.__setattr__(self, "finance_semantics", frozen)
        if self.observation_model not in _OBSERVATION_MODELS:
            raise CatalogParseError("observation_model 非法")
        observation_keys = _mapping(self.observation_keys)
        expected_keys = _OBSERVATION_KEYS[self.observation_model]
        if set(observation_keys) != expected_keys:
            raise CatalogParseError(
                f"{self.observation_model} observation_keys schema 不匹配"
            )
        if any(not isinstance(value, str) or not value.strip() for value in observation_keys.values()):
            raise CatalogParseError("observation_keys 值必须是非空字段引用")
        object.__setattr__(self, "observation_keys", observation_keys)
        if self.contract_version not in {
            CATALOG_CONTRACT_VERSION,
            LEGACY_CATALOG_CONTRACT_VERSION,
        }:
            raise CatalogParseError("FieldContract contract_version 不受支持")
        if self.contract_version == LEGACY_CATALOG_CONTRACT_VERSION and (
            self.observation_model != "static_reference" or observation_keys
        ):
            raise CatalogParseError("v1 FieldContract 不能携带 v2 observation 语义")
        if (
            self.contract_version == LEGACY_CATALOG_CONTRACT_VERSION
            and self.logical_name.startswith("finance.")
        ) or (
            self.contract_version == CATALOG_CONTRACT_VERSION
            and self.observation_model == "point_in_time"
            and self.semantic_type == "financial"
        ):
            missing = _FINANCE_REQUIRED - set(frozen)
            if missing:
                raise CatalogFinancialSemanticsError(f"财务字段缺少语义: {sorted(missing)}")

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        if self.contract_version == LEGACY_CATALOG_CONTRACT_VERSION:
            payload.pop("observation_model")
            payload.pop("observation_keys")
        return payload


@dataclass(frozen=True)
class DatasetContract(CatalogRecord):
    dataset_id: str
    dataset_version: int
    market: str
    instrument_type: str
    frequency: str
    primary_key: tuple[str, ...]
    fields: tuple[str, ...]
    event_time_field: str
    available_time_policy: str
    drift_policy_id: str
    sort_order: tuple[str, ...]
    revision_policy_id: str = "revision.none.v1"
    status: str = "approved"
    contract_version: str = CATALOG_CONTRACT_VERSION
    minute_semantics: Mapping[str, Any] = field(default_factory=dict)
    result_cardinality: str = "one_or_more"
    entity_axis: str = "instrument"

    def __post_init__(self) -> None:
        for name in ("dataset_id", "market", "instrument_type", "frequency", "event_time_field", "available_time_policy", "drift_policy_id", "revision_policy_id", "contract_version"):
            _text(getattr(self, name), name)
        _version(self.dataset_version, "dataset_version")
        try:
            canonical_market = normalize_asset_class(self.market, field="dataset market")
            require_instrument_type(canonical_market, self.instrument_type)
        except AssetTaxonomyError as exc:
            raise CatalogParseError(str(exc)) from exc
        object.__setattr__(self, "market", canonical_market)
        for name in ("primary_key", "fields", "sort_order"):
            _tuple(getattr(self, name), name)
        if not set(self.primary_key) <= set(self.fields):
            raise CatalogParseError("primary_key 必须引用 dataset fields")
        if self.event_time_field not in self.fields:
            raise CatalogParseError("event_time_field 必须引用 dataset fields")
        if self.status not in _LIFECYCLES:
            raise CatalogParseError("dataset status 非法")
        if self.result_cardinality not in {"one_or_more", "zero_or_more"}:
            raise CatalogParseError("dataset result_cardinality 非法")
        if self.entity_axis not in {"instrument", "relation"}:
            raise CatalogParseError("dataset entity_axis 非法")
        frozen = _mapping(self.minute_semantics)
        if frozen:
            from .minute import MinuteDatasetSemantics

            semantics = MinuteDatasetSemantics.from_mapping(frozen)
            if self.frequency != "minute":
                raise CatalogParseError("minute_semantics 只能用于 minute dataset")
            if semantics.availability_policy_ref != self.available_time_policy:
                raise CatalogParseError("分钟 availability policy 引用不一致")
            frozen = _mapping(semantics.to_dict())
        object.__setattr__(self, "minute_semantics", frozen)

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        if self.result_cardinality == "one_or_more":
            payload.pop("result_cardinality")
        if self.entity_axis == "instrument":
            payload.pop("entity_axis")
        if not self.minute_semantics:
            payload.pop("minute_semantics")
        return payload


@dataclass(frozen=True)
class PolicyContract(CatalogRecord):
    policy_id: str
    policy_version: int
    policy_type: str
    rules: Mapping[str, Any]
    status: str = "approved"
    contract_version: str = CATALOG_CONTRACT_VERSION
    validator_id: str | None = None
    applicability: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _text(self.policy_id, "policy_id")
        _version(self.policy_version, "policy_version")
        if self.policy_type not in _POLICY_TYPES:
            raise CatalogParseError("policy_type 非法")
        object.__setattr__(self, "rules", _mapping(self.rules))
        applicability = _mapping(self.applicability)
        object.__setattr__(self, "applicability", applicability)
        if self.status not in _LIFECYCLES:
            raise CatalogParseError("policy status 非法")
        if self.validator_id is not None:
            _text(self.validator_id, "validator_id")
        if self.contract_version not in {
            CATALOG_CONTRACT_VERSION,
            LEGACY_CATALOG_CONTRACT_VERSION,
        }:
            raise CatalogParseError("PolicyContract contract_version 不受支持")
        if self.contract_version == LEGACY_CATALOG_CONTRACT_VERSION and (
            self.validator_id is not None or applicability
        ):
            raise CatalogParseError("v1 PolicyContract 不能携带 v2 validator 语义")
        validate_policy_payload(self.validator_id, self.rules, applicability)

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        if self.contract_version == LEGACY_CATALOG_CONTRACT_VERSION:
            payload.pop("validator_id")
            payload.pop("applicability")
        return payload


@dataclass(frozen=True)
class PhysicalBindingContract(CatalogRecord):
    binding_id: str
    binding_version: int
    dataset_id: str
    dataset_version: int
    source_profile: str
    environment: str
    object_name: str
    expected_schema_revision: str
    drift_policy_id: str
    column_bindings: Mapping[str, Any]
    dataset_revision_constraint: str = "any"
    status: str = "approved"
    contract_version: str = CATALOG_CONTRACT_VERSION
    minute_source_semantics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("binding_id", "dataset_id", "source_profile", "environment", "object_name", "expected_schema_revision", "drift_policy_id", "dataset_revision_constraint", "contract_version"):
            _text(getattr(self, name), name)
        _version(self.binding_version, "binding_version")
        _version(self.dataset_version, "dataset_version")
        frozen = _mapping(self.column_bindings)
        if not frozen:
            raise CatalogParseError("column_bindings 不能为空")
        object.__setattr__(self, "column_bindings", frozen)
        if self.status not in _LIFECYCLES:
            raise CatalogParseError("binding status 非法")
        source_semantics = _mapping(self.minute_source_semantics)
        if source_semantics:
            from .minute import MinuteSourceSemantics

            source_semantics = _mapping(
                MinuteSourceSemantics.from_mapping(source_semantics).to_dict()
            )
        object.__setattr__(self, "minute_source_semantics", source_semantics)

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        if not self.minute_source_semantics:
            payload.pop("minute_source_semantics")
        return payload


@dataclass(frozen=True)
class TransformContract(CatalogRecord):
    transform_id: str
    transform_version: int
    implementation_id: str
    code_hash: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    parameters_schema: Mapping[str, Any]
    time_policy: Mapping[str, Any]
    deterministic: bool
    resource_hint: Mapping[str, Any] = field(default_factory=dict)
    status: str = "approved"
    contract_version: str = CATALOG_CONTRACT_VERSION
    input_schema: Mapping[str, Any] = field(default_factory=dict)
    output_schema: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("transform_id", "implementation_id", "code_hash", "contract_version"):
            _text(getattr(self, name), name)
        _version(self.transform_version, "transform_version")
        if not isinstance(self.deterministic, bool):
            raise CatalogParseError("deterministic 必须是 boolean")
        _tuple(self.inputs, "inputs")
        _tuple(self.outputs, "outputs")
        object.__setattr__(self, "parameters_schema", _mapping(self.parameters_schema))
        object.__setattr__(self, "time_policy", _mapping(self.time_policy))
        object.__setattr__(self, "resource_hint", _mapping(self.resource_hint))
        input_schema = _mapping(self.input_schema)
        output_schema = _mapping(self.output_schema)
        if bool(input_schema) != bool(output_schema):
            raise CatalogParseError("Transform input/output schema 必须同时声明")
        if input_schema and (
            set(input_schema) != set(self.inputs) or set(output_schema) != set(self.outputs)
        ):
            raise CatalogParseError("Transform 端口 schema 必须完整覆盖 inputs/outputs")
        object.__setattr__(self, "input_schema", input_schema)
        object.__setattr__(self, "output_schema", output_schema)
        if self.status not in _LIFECYCLES:
            raise CatalogParseError("transform status 非法")

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        if not self.resource_hint:
            payload.pop("resource_hint")
        if not self.input_schema:
            payload.pop("input_schema")
            payload.pop("output_schema")
        return payload


@dataclass(frozen=True)
class CatalogCoverageBaseline(CatalogRecord):
    baseline_id: str
    baseline_version: int
    required_slots: tuple[str, ...]
    contract_version: str = CATALOG_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _text(self.baseline_id, "baseline_id")
        _version(self.baseline_version, "baseline_version")
        _tuple(self.required_slots, "required_slots")


@dataclass(frozen=True)
class CatalogSourceManifest(CatalogRecord):
    manifest_id: str
    manifest_version: int
    baseline_hash: str
    previous_manifest_hash: str | None
    active_slots: tuple[str, ...]
    retired_slots: tuple[str, ...]
    sources: Mapping[str, str]
    slot_targets: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    contract_version: str = CATALOG_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _text(self.manifest_id, "manifest_id")
        _version(self.manifest_version, "manifest_version")
        _text(self.baseline_hash, "baseline_hash")
        if self.previous_manifest_hash is not None:
            _text(self.previous_manifest_hash, "previous_manifest_hash")
        _tuple(self.active_slots, "active_slots")
        _tuple(self.retired_slots, "retired_slots", allow_empty=True)
        if set(self.active_slots) & set(self.retired_slots):
            raise CatalogParseError("active_slots 与 retired_slots 不能重叠")
        object.__setattr__(self, "sources", _mapping(self.sources))
        frozen_targets = _mapping(self.slot_targets)
        for slot, targets in frozen_targets.items():
            _tuple(targets, f"slot_targets[{slot}]")
        unknown = set(frozen_targets) - set(self.active_slots) - set(self.retired_slots)
        if unknown:
            raise CatalogParseError(f"slot_targets 引用未知 slot: {sorted(unknown)}")
        object.__setattr__(self, "slot_targets", frozen_targets)


@dataclass(frozen=True)
class FieldFamilySource(CatalogRecord):
    family_id: str
    family_version: int
    dataset_id: str
    candidate_revision: str
    column_regex: str
    logical_name_template: str
    defaults: Mapping[str, Any]
    expected_count: int
    exclude: tuple[str, ...] = ()
    overrides: Mapping[str, Any] = field(default_factory=dict)
    contract_version: str = CATALOG_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for name in ("family_id", "dataset_id", "candidate_revision", "column_regex", "logical_name_template"):
            _text(getattr(self, name), name)
        _version(self.family_version, "family_version")
        _version(self.expected_count, "expected_count")
        _tuple(self.exclude, "exclude", allow_empty=True)
        object.__setattr__(self, "defaults", _mapping(self.defaults))
        object.__setattr__(self, "overrides", _mapping(self.overrides))


@dataclass(frozen=True)
class ApprovalDecision(CatalogRecord):
    target_kind: str
    target_id: str
    target_hash: str | None
    decision: str
    reason: str
    evidence_refs: tuple[str, ...]
    decided_by: str
    decided_at: str
    contract_version: str = CATALOG_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for name in ("target_kind", "target_id", "reason", "decided_by", "decided_at", "contract_version"):
            _text(getattr(self, name), name)
        if self.target_hash is not None:
            _text(self.target_hash, "target_hash")
        if self.decision not in _DECISIONS:
            raise CatalogParseError("decision 非法")
        _tuple(self.evidence_refs, "evidence_refs", allow_empty=True)
        try:
            parsed = datetime.fromisoformat(self.decided_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CatalogParseError("decided_at 必须是 RFC3339 时间") from exc
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            raise CatalogParseError("decided_at 必须是 UTC 时间")


CatalogContract = FieldContract | DatasetContract | PolicyContract | PhysicalBindingContract | TransformContract | CatalogCoverageBaseline | CatalogSourceManifest | FieldFamilySource | ApprovalDecision


__all__ = [name for name in globals() if name.endswith("Contract") or name in {"ApprovalDecision", "CatalogCoverageBaseline", "CatalogSourceManifest", "FieldFamilySource", "CATALOG_CONTRACT_VERSION", "LEGACY_CATALOG_CONTRACT_VERSION"}]
