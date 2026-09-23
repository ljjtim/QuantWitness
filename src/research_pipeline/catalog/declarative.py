"""声明式目录 bundle 到正式合同的通用展开。"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any, Iterable

import yaml

from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import CatalogParseError
from .families import expand_field_family
from .models import (
    ApprovalDecision, CatalogContract, CatalogCoverageBaseline, CatalogSourceManifest,
    DatasetContract, FieldContract, FieldFamilySource, PhysicalBindingContract, PolicyContract,
    TransformContract,
    CATALOG_CONTRACT_VERSION, LEGACY_CATALOG_CONTRACT_VERSION,
)


@dataclass(frozen=True)
class DeclarativeCatalog:
    baseline: CatalogCoverageBaseline
    manifest: CatalogSourceManifest
    contracts: tuple[CatalogContract, ...]
    decisions: tuple[ApprovalDecision, ...]


_TOP_KEYS = {
    "bundle_version", "coverage_slots", "policies", "datasets", "numbered_families",
    "named_families", "derived_fields", "transforms",
}
_NOW = "2026-07-13T00:00:00Z"


def _require_keys(
    payload: dict[str, Any],
    *,
    required: set[str],
    optional: set[str],
    context: str,
) -> None:
    if not isinstance(payload, dict):
        raise CatalogParseError(f"{context} 必须是 mapping")
    missing = required - set(payload)
    unknown = set(payload) - required - optional
    if missing:
        raise CatalogParseError(f"{context} 缺少字段: {sorted(missing)}")
    if unknown:
        raise CatalogParseError(f"{context} 未知字段: {sorted(unknown)}")


class _UniqueKeyLoader(yaml.SafeLoader):
    """拒绝重复键，避免声明在 YAML 解析时被静默覆盖。"""


def _construct_mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise CatalogParseError(f"目录 bundle 存在重复字段: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _approval(
    kind: str,
    identifier: str,
    payload_hash: str | None,
    decision: str,
    evidence: tuple[str, ...],
    reason: str = "目录 bundle 审批",
) -> ApprovalDecision:
    return ApprovalDecision(
        kind,
        identifier,
        payload_hash,
        decision,
        reason,
        evidence,
        "catalog-bootstrap-review",
        _NOW,
    )


def _load(path: Path) -> dict[str, Any]:
    payload = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    if not isinstance(payload, dict):
        raise CatalogParseError(f"目录 bundle 必须是 mapping: {path.name}")
    unknown = set(payload) - _TOP_KEYS
    if unknown:
        raise CatalogParseError(f"目录 bundle 未知顶层字段: {sorted(unknown)}")
    if payload.get("bundle_version") != "catalog-bundle-v1":
        raise CatalogParseError("bundle_version 不受支持")
    return payload


def load_declarative_catalog(
    paths: Iterable[str | Path],
    *,
    approval_path: str | Path | None = None,
    allow_generated_approvals: bool = False,
) -> DeclarativeCatalog:
    files = tuple(Path(path) for path in paths)
    payloads = tuple(_load(path) for path in files)
    contracts: list[CatalogContract] = []
    decisions: list[ApprovalDecision] = []
    decision_keys_by_source: dict[str, set[tuple[str, str]]] = {}
    slots: list[str] = []
    slot_targets: dict[str, tuple[str, ...]] = {}
    for path, payload in zip(files, payloads, strict=True):
        before = len(decisions)
        for slot in payload.get("coverage_slots", []):
            _require_keys(
                slot,
                required={"slot_id", "decision", "target_ids"},
                optional={"evidence_refs"},
                context="coverage slot",
            )
            slot_id = str(slot["slot_id"])
            slots.append(slot_id)
            targets = tuple(str(item) for item in slot.get("target_ids", ()))
            if not targets:
                raise CatalogParseError(f"coverage slot 必须声明 target_ids: {slot_id}")
            slot_targets[slot_id] = targets
            decisions.append(_approval("coverage_slot", slot_id, None, str(slot["decision"]), tuple(slot.get("evidence_refs", ()))))
        for raw in payload.get("policies", []):
            _require_keys(
                raw,
                required={"policy_id", "policy_type", "rules"},
                optional={
                    "policy_version",
                    "evidence_refs",
                    "validator_id",
                    "applicability",
                    "contract_version",
                    "decision",
                    "decision_reason",
                    "status",
                },
                context="policy",
            )
            item = PolicyContract(
                policy_id=str(raw["policy_id"]),
                policy_version=int(raw.get("policy_version", 1)),
                policy_type=str(raw["policy_type"]),
                rules=raw["rules"],
                status=str(raw.get("status", "approved")),
                validator_id=(
                    None if raw.get("validator_id") is None else str(raw["validator_id"])
                ),
                applicability=raw.get("applicability", {}),
                contract_version=str(
                    raw.get(
                        "contract_version",
                        (
                            CATALOG_CONTRACT_VERSION
                            if raw.get("validator_id") is not None
                            or raw.get("applicability")
                            else LEGACY_CATALOG_CONTRACT_VERSION
                        ),
                    )
                ),
            )
            contracts.append(item)
            decisions.append(
                _approval(
                    "policy",
                    item.policy_id,
                    item.content_hash,
                    str(raw.get("decision", "approved")),
                    tuple(raw.get("evidence_refs", ())),
                    str(raw.get("decision_reason", "目录 bundle 审批")),
                )
            )
        decision_keys_by_source[path.name] = {
            (item.target_kind, item.target_id)
            for item in decisions[before:]
        }
    for path, payload in zip(files, payloads, strict=True):
        before = len(decisions)
        for raw in payload.get("datasets", []):
            _expand_dataset(raw, contracts, decisions)
        for raw in payload.get("numbered_families", []):
            _expand_numbered_family(raw, contracts, decisions)
        for raw in payload.get("named_families", []):
            _expand_named_family(raw, contracts, decisions)
        for raw in payload.get("derived_fields", []):
            _expand_derived_field(raw, contracts, decisions)
        for raw in payload.get("transforms", []):
            _expand_transform(raw, contracts, decisions)
        decision_keys_by_source.setdefault(path.name, set()).update(
            (item.target_kind, item.target_id)
            for item in decisions[before:]
        )
    baseline = CatalogCoverageBaseline("research-pipeline-catalog-baseline", 1, tuple(sorted(set(slots))))
    sources = {path.name: typed_canonical_hash(payload) for path, payload in zip(files, payloads)}
    if approval_path is not None:
        approval_file = Path(approval_path)
        external_decisions, approval_payload = _load_approvals(approval_file)
        source_bundle_decisions = tuple(
            item
            for item in external_decisions
            if item.target_kind == "source_bundle"
        )
        bundle_approved_keys: set[tuple[str, str]] = set()
        for item in source_bundle_decisions:
            actual_hash = sources.get(item.target_id)
            if item.decision != "approved" or actual_hash is None:
                raise CatalogParseError("source_bundle 只允许批准现有声明源")
            if item.target_hash != actual_hash:
                raise CatalogParseError(
                    f"source_bundle hash 不匹配: {item.target_id}"
                )
            bundle_approved_keys.update(
                decision_keys_by_source.get(item.target_id, set())
            )
        explicit_decisions = tuple(
            item
            for item in external_decisions
            if item.target_kind != "source_bundle"
            and (item.target_kind, item.target_id) not in bundle_approved_keys
        )
        external_keys = {
            (item.target_kind, item.target_id) for item in explicit_decisions
        }
        decisions = [*source_bundle_decisions, *explicit_decisions] + [
            item
            for item in decisions
            if (item.target_kind, item.target_id) not in external_keys
            and (
                item.decision != "approved"
                or (item.target_kind, item.target_id) in bundle_approved_keys
            )
        ]
        sources[approval_file.name] = typed_canonical_hash(approval_payload)
    elif not allow_generated_approvals:
        decisions = []
    manifest = CatalogSourceManifest(
        "research-pipeline-catalog",
        2,
        baseline.content_hash,
        None,
        tuple(sorted(set(slots))),
        (),
        sources,
        slot_targets,
    )
    return DeclarativeCatalog(baseline, manifest, tuple(contracts), tuple(decisions))


def load_default_declarative_catalog() -> DeclarativeCatalog:
    """加载宿主额外提供的默认声明；公开发行包不附带数据源 Catalog。"""

    definitions = resources.files("research_pipeline.catalog").joinpath("definitions")
    try:
        with resources.as_file(definitions.joinpath("market.yaml")) as market_path:
            with resources.as_file(definitions.joinpath("finance_factor.yaml")) as finance_path:
                with resources.as_file(definitions.joinpath("factor_database.yaml")) as factor_path:
                    with resources.as_file(definitions.joinpath("approvals.yaml")) as approval_path:
                        return load_declarative_catalog(
                            (market_path, finance_path, factor_path),
                            approval_path=approval_path,
                        )
    except FileNotFoundError as exc:
        raise CatalogParseError(
            "公开发行包不附带默认 Catalog 声明；请使用 "
            "load_declarative_catalog(<声明文件>, approval_path=<审批文件>)"
        ) from exc


def _load_approvals(path: Path) -> tuple[tuple[ApprovalDecision, ...], object]:
    payload = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    if not isinstance(payload, dict) or set(payload) != {"approval_version", "decisions"}:
        raise CatalogParseError("审批文件只能包含 approval_version 和 decisions")
    if payload["approval_version"] != "catalog-approvals-v1":
        raise CatalogParseError("approval_version 不受支持")
    if not isinstance(payload["decisions"], list):
        raise CatalogParseError("审批 decisions 必须是列表")
    expected_keys = {
        "target_kind", "target_id", "target_hash", "decision", "reason",
        "evidence_refs", "decided_by", "decided_at", "contract_version",
    }
    decisions: list[ApprovalDecision] = []
    for raw in payload["decisions"]:
        _require_keys(raw, required=expected_keys, optional=set(), context="approval")
        normalized = dict(raw)
        normalized["evidence_refs"] = tuple(normalized["evidence_refs"])
        decisions.append(ApprovalDecision(**normalized))
    return tuple(decisions), payload


def _expand_dataset(raw: dict[str, Any], contracts: list[CatalogContract], decisions: list[ApprovalDecision]) -> None:
    _require_keys(
        raw,
        required={
            "dataset_id", "market", "instrument_type", "frequency", "object_name",
            "primary_key", "event_time_field", "available_time_policy", "fields",
        },
        optional={
            "dataset_version", "drift_policy_id", "sort_order", "revision_policy_id",
            "binding_id", "source_profile", "environment", "expected_schema_revision",
            "evidence_refs", "decision", "minute_semantics",
            "minute_source_semantics", "binding_version",
            "contract_version", "binding_status", "decision_reason",
            "materialize_blocked_contract", "result_cardinality",
            "entity_axis",
        },
        context="dataset",
    )
    evidence = tuple(raw.get("evidence_refs", ()))
    decision = str(raw.get("decision", "approved"))
    decision_reason = str(raw.get("decision_reason", "目录 bundle 审批"))
    if decision != "approved" and not raw.get("materialize_blocked_contract", False):
        decisions.append(
            _approval(
                "dataset",
                str(raw["dataset_id"]),
                None,
                decision,
                evidence,
                decision_reason,
            )
        )
        return
    field_items: list[FieldContract] = []
    column_bindings: dict[str, Any] = {}
    for source in raw["fields"]:
        _require_keys(
            source,
            required={
                "field_id", "logical_name", "data_type",
                "semantic_type", "unit", "availability_policy",
            },
            optional={
                "field_version", "nullable", "adjustment_allowed", "aliases",
                "finance_semantics", "observation_model", "observation_keys",
                "contract_version", "physical_column", "binding",
            },
            context=f"dataset {raw['dataset_id']} field",
        )
        nullable = source.get("nullable", True)
        if not isinstance(nullable, bool):
            raise CatalogParseError("field nullable 必须是 boolean")
        finance = source.get("finance_semantics", {})
        item = FieldContract(
            field_id=str(source["field_id"]),
            logical_name=str(source["logical_name"]),
            field_version=int(source.get("field_version", 1)),
            data_type=str(source["data_type"]),
            semantic_type=str(source["semantic_type"]),
            unit=str(source["unit"]),
            nullable=nullable,
            availability_policy=str(source["availability_policy"]),
            frequency_scope=(str(raw["frequency"]),),
            instrument_scope=(str(raw["instrument_type"]),),
            adjustment_allowed=tuple(source.get("adjustment_allowed", ())),
            aliases=tuple(source.get("aliases", ())),
            status="approved",
            evidence_refs=evidence,
            finance_semantics=finance,
            observation_model=str(
                source.get("observation_model", "static_reference")
            ),
            observation_keys=source.get("observation_keys", {}),
            contract_version=str(
                source.get(
                    "contract_version",
                    (
                        CATALOG_CONTRACT_VERSION
                        if "observation_model" in source or "observation_keys" in source
                        else LEGACY_CATALOG_CONTRACT_VERSION
                    ),
                )
            ),
        )
        field_items.append(item)
        has_column = "physical_column" in source
        has_binding = "binding" in source
        if has_column == has_binding:
            raise CatalogParseError(
                "field 必须且只能声明 physical_column 或 binding"
            )
        if has_column:
            column_bindings[item.field_id] = {
                "kind": "direct",
                "column": str(source["physical_column"]),
            }
        else:
            binding = source["binding"]
            if not isinstance(binding, dict) or binding.get("kind") != "missing_evidence":
                raise CatalogParseError("非直接字段 binding 只允许 missing_evidence")
            if set(binding) != {"kind", "reason"} or not str(
                binding.get("reason", "")
            ).strip():
                raise CatalogParseError("missing_evidence binding 必须声明 reason")
            column_bindings[item.field_id] = dict(binding)
        decisions.append(
            _approval(
                "field",
                item.field_id,
                item.content_hash,
                decision,
                evidence,
                decision_reason,
            )
        )
    fields = tuple(item.field_id for item in field_items)
    dataset = DatasetContract(
        str(raw["dataset_id"]), int(raw.get("dataset_version", 1)), str(raw["market"]), str(raw["instrument_type"]), str(raw["frequency"]),
        tuple(raw["primary_key"]), fields, str(raw["event_time_field"]), str(raw["available_time_policy"]), str(raw.get("drift_policy_id", "drift.strict.v1")),
        tuple(raw.get("sort_order", raw["primary_key"])), str(raw.get("revision_policy_id", "revision.none.v1")),
        contract_version=str(
            raw.get("contract_version", LEGACY_CATALOG_CONTRACT_VERSION)
        ),
        minute_semantics=raw.get("minute_semantics", {}),
        result_cardinality=str(raw.get("result_cardinality", "one_or_more")),
        entity_axis=str(raw.get("entity_axis", "instrument")),
    )
    schema_revision = str(
        raw.get(
            "expected_schema_revision",
            typed_canonical_hash(
                {"object_name": raw["object_name"], "columns": column_bindings}
            ),
        )
    )
    binding = PhysicalBindingContract(
        str(raw.get("binding_id", f"{raw['dataset_id']}.local.v1")),
        int(raw.get("binding_version", 1)), dataset.dataset_id, dataset.dataset_version,
        str(raw.get("source_profile", "source")), str(raw.get("environment", "prod")), str(raw["object_name"]), schema_revision,
        dataset.drift_policy_id, column_bindings,
        status=str(raw.get("binding_status", "approved")),
        contract_version=str(
            raw.get("contract_version", LEGACY_CATALOG_CONTRACT_VERSION)
        ),
        minute_source_semantics=raw.get("minute_source_semantics", {}),
    )
    contracts.extend((*field_items, dataset, binding))
    decisions.extend((
        _approval(
            "dataset",
            dataset.dataset_id,
            dataset.content_hash,
            decision,
            evidence,
            decision_reason,
        ),
        _approval(
            "physical_binding",
            binding.binding_id,
            binding.content_hash,
            decision,
            evidence,
            decision_reason,
        ),
    ))


def _expand_numbered_family(raw: dict[str, Any], contracts: list[CatalogContract], decisions: list[ApprovalDecision]) -> None:
    _require_keys(
        raw,
        required={
            "family_id", "dataset_id", "start", "end", "column_prefix",
            "logical_name_template", "defaults", "key_fields", "market",
            "instrument_type", "frequency", "available_time_policy", "object_name",
        },
        optional={
            "exclude", "overrides", "evidence_refs", "source_profile", "decision",
            "revision_policy_id", "expected_schema_revision", "environment",
            "blocked_columns", "blocked_reason", "dataset_version",
            "binding_version", "family_version",
        },
        context="numbered family",
    )
    start, end = int(raw["start"]), int(raw["end"])
    prefix = str(raw["column_prefix"])
    columns = tuple(f"{prefix}{number:03d}" for number in range(start, end + 1))
    blocked_columns = tuple(str(item) for item in raw.get("blocked_columns", ()))
    if not set(blocked_columns) <= set(columns):
        raise CatalogParseError("numbered family blocked_columns 引用未知列")
    excluded = tuple(sorted(set(raw.get("exclude", ())) | set(blocked_columns)))
    enabled_columns = tuple(column for column in columns if column not in excluded)
    family = FieldFamilySource(
        str(raw["family_id"]), int(raw.get("family_version", 1)),
        str(raw["dataset_id"]), typed_canonical_hash({"columns": columns}),
        rf"^{re_escape(prefix)}(?P<number>[0-9]{{3}})$",
        str(raw["logical_name_template"]),
        raw["defaults"], len(columns) - len(excluded), excluded,
        raw.get("overrides", {}),
    )
    fields = expand_field_family(family, columns)
    evidence = tuple(raw.get("evidence_refs", ()))
    for item in fields:
        decisions.append(_approval("field", item.field_id, item.content_hash, "approved", evidence))
    blocked_fields: list[FieldContract] = []
    for column in blocked_columns:
        number = column.removeprefix(prefix)
        values = dict(family.defaults)
        values.update(dict(family.overrides.get(column, {})))
        values["field_version"] = int(values.get("field_version", 1)) + 1
        values["status"] = "removed"
        field_id_template = str(values.pop("field_id_template", ""))
        item = FieldContract(
            field_id=field_id_template.format(column=column, number=number),
            logical_name=family.logical_name_template.format(
                column=column,
                number=number,
            ),
            **values,
        )
        blocked_fields.append(item)
        decisions.append(
            _approval(
                "field",
                item.field_id,
                item.content_hash,
                "blocked",
                evidence,
                str(
                    raw.get(
                        "blocked_reason",
                        "字段缺少正式运行所需证据",
                    )
                ),
            )
        )
    key_items: list[FieldContract] = []
    for source in raw["key_fields"]:
        _require_keys(
            source,
            required={
                "field_id", "logical_name", "physical_column", "data_type",
                "semantic_type", "unit",
            },
            optional=set(),
            context=f"family {raw['family_id']} key field",
        )
        item = FieldContract(
            str(source["field_id"]), str(source["logical_name"]), 1, str(source["data_type"]), str(source["semantic_type"]),
            str(source["unit"]), False, str(raw["available_time_policy"]), (str(raw["frequency"]),), (str(raw["instrument_type"]),),
        )
        key_items.append(item)
        decisions.append(_approval("field", item.field_id, item.content_hash, "approved", evidence))
    key_fields = tuple(item.field_id for item in key_items)
    dataset_fields = key_fields + tuple(item.field_id for item in fields)
    dataset_version = int(raw.get("dataset_version", 1))
    dataset = DatasetContract(
        str(raw["dataset_id"]), dataset_version, str(raw["market"]), str(raw["instrument_type"]),
        str(raw["frequency"]), key_fields, dataset_fields, key_fields[0],
        str(raw["available_time_policy"]), "drift.strict.v1", key_fields,
        str(raw.get("revision_policy_id", "revision.none.v1")),
    )
    bindings = {field_id: {"kind": "direct", "column": str(raw["key_fields"][index]["physical_column"])} for index, field_id in enumerate(key_fields)}
    bindings.update({
        item.field_id: {"kind": "direct", "column": column}
        for item, column in zip(fields, enabled_columns, strict=True)
    })
    binding = PhysicalBindingContract(
        f"{raw['dataset_id']}.local.v1", int(raw.get("binding_version", 1)),
        dataset.dataset_id, dataset_version,
        str(raw.get("source_profile", "factor")), str(raw.get("environment", "prod")),
        str(raw["object_name"]),
        str(raw.get("expected_schema_revision", typed_canonical_hash({"columns": bindings}))),
        "drift.strict.v1", bindings,
    )
    contracts.extend((*key_items, *fields, *blocked_fields, dataset, binding))
    decisions.extend((_approval("field_family_source", family.family_id, family.content_hash, "approved", evidence), _approval("dataset", dataset.dataset_id, dataset.content_hash, "approved", evidence), _approval("physical_binding", binding.binding_id, binding.content_hash, "approved", evidence)))


def _expand_named_family(
    raw: dict[str, Any],
    contracts: list[CatalogContract],
    decisions: list[ApprovalDecision],
) -> None:
    """展开列名不规则但共享合同的宽表因子族。"""

    _require_keys(
        raw,
        required={
            "family_id", "dataset_id", "columns", "logical_name_template",
            "defaults", "key_fields", "market", "instrument_type", "frequency",
            "available_time_policy", "object_name",
        },
        optional={
            "overrides", "evidence_refs", "source_profile", "environment",
            "revision_policy_id", "expected_schema_revision", "decision",
            "decision_reason", "entity_axis", "result_cardinality",
            "binding_version", "dataset_version", "contract_version",
        },
        context="named family",
    )
    columns = tuple(str(item) for item in raw["columns"])
    if not columns or any(not item.strip() for item in columns):
        raise CatalogParseError("named family columns 必须是非空字符串")
    if len(columns) != len(set(columns)):
        raise CatalogParseError("named family columns 不能重复")
    defaults = dict(raw["defaults"])
    overrides = dict(raw.get("overrides", {}))
    unknown_overrides = set(overrides) - set(columns)
    if unknown_overrides:
        raise CatalogParseError(
            f"named family overrides 引用未知列: {sorted(unknown_overrides)}"
        )
    evidence = tuple(raw.get("evidence_refs", ()))
    decision = str(raw.get("decision", "approved"))
    decision_reason = str(raw.get("decision_reason", "目录 bundle 审批"))
    field_items: list[FieldContract] = []
    for column in columns:
        values = dict(defaults)
        values.update(dict(overrides.get(column, {})))
        for tuple_field in (
            "frequency_scope",
            "instrument_scope",
            "adjustment_allowed",
            "aliases",
            "evidence_refs",
        ):
            if tuple_field in values:
                values[tuple_field] = tuple(values[tuple_field])
        field_id_template = str(values.pop("field_id_template", ""))
        if not field_id_template:
            raise CatalogParseError("named family defaults 必须提供 field_id_template")
        item = FieldContract(
            field_id=field_id_template.format(column=column),
            logical_name=str(raw["logical_name_template"]).format(column=column),
            **values,
        )
        field_items.append(item)
        decisions.append(
            _approval(
                "field",
                item.field_id,
                item.content_hash,
                decision,
                evidence,
                decision_reason,
            )
        )
    key_items: list[FieldContract] = []
    bindings: dict[str, Any] = {}
    for source in raw["key_fields"]:
        _require_keys(
            source,
            required={
                "field_id", "logical_name", "physical_column", "data_type",
                "semantic_type", "unit",
            },
            optional=set(),
            context=f"named family {raw['family_id']} key field",
        )
        item = FieldContract(
            str(source["field_id"]),
            str(source["logical_name"]),
            1,
            str(source["data_type"]),
            str(source["semantic_type"]),
            str(source["unit"]),
            False,
            str(raw["available_time_policy"]),
            (str(raw["frequency"]),),
            (str(raw["instrument_type"]),),
        )
        key_items.append(item)
        bindings[item.field_id] = {
            "kind": "direct",
            "column": str(source["physical_column"]),
        }
        decisions.append(
            _approval(
                "field",
                item.field_id,
                item.content_hash,
                decision,
                evidence,
                decision_reason,
            )
        )
    bindings.update(
        {
            item.field_id: {"kind": "direct", "column": column}
            for item, column in zip(field_items, columns, strict=True)
        }
    )
    key_fields = tuple(item.field_id for item in key_items)
    dataset_version = int(raw.get("dataset_version", 1))
    contract_version = str(
        raw.get("contract_version", LEGACY_CATALOG_CONTRACT_VERSION)
    )
    dataset = DatasetContract(
        str(raw["dataset_id"]),
        dataset_version,
        str(raw["market"]),
        str(raw["instrument_type"]),
        str(raw["frequency"]),
        key_fields,
        key_fields + tuple(item.field_id for item in field_items),
        key_fields[0],
        str(raw["available_time_policy"]),
        "drift.strict.v1",
        key_fields,
        str(raw.get("revision_policy_id", "revision.none.v1")),
        contract_version=contract_version,
        result_cardinality=str(raw.get("result_cardinality", "one_or_more")),
        entity_axis=str(raw.get("entity_axis", "instrument")),
    )
    binding = PhysicalBindingContract(
        f"{raw['dataset_id']}.local.v1",
        int(raw.get("binding_version", 1)),
        dataset.dataset_id,
        dataset.dataset_version,
        str(raw.get("source_profile", "factor")),
        str(raw.get("environment", "prod")),
        str(raw["object_name"]),
        str(
            raw.get(
                "expected_schema_revision",
                typed_canonical_hash(
                    {"object_name": raw["object_name"], "columns": bindings}
                ),
            )
        ),
        "drift.strict.v1",
        bindings,
        contract_version=contract_version,
    )
    contracts.extend((*key_items, *field_items, dataset, binding))
    decisions.extend(
        (
            _approval(
                "dataset",
                dataset.dataset_id,
                dataset.content_hash,
                decision,
                evidence,
                decision_reason,
            ),
            _approval(
                "physical_binding",
                binding.binding_id,
                binding.content_hash,
                decision,
                evidence,
                decision_reason,
            ),
        )
    )


def _expand_derived_field(
    raw: dict[str, Any],
    contracts: list[CatalogContract],
    decisions: list[ApprovalDecision],
) -> None:
    _require_keys(
        raw,
        required={
            "field_id", "logical_name", "data_type", "semantic_type", "unit",
            "nullable", "availability_policy", "frequency_scope", "instrument_scope",
        },
        optional={"field_version", "evidence_refs"},
        context="derived field",
    )
    evidence = tuple(raw.get("evidence_refs", ()))
    if type(raw["nullable"]) is not bool:
        raise CatalogParseError("derived field nullable 必须是 boolean")
    item = FieldContract(
        str(raw["field_id"]),
        str(raw["logical_name"]),
        int(raw.get("field_version", 1)),
        str(raw["data_type"]),
        str(raw["semantic_type"]),
        str(raw["unit"]),
        raw["nullable"],
        str(raw["availability_policy"]),
        tuple(str(value) for value in raw["frequency_scope"]),
        tuple(str(value) for value in raw["instrument_scope"]),
        evidence_refs=evidence,
    )
    contracts.append(item)
    decisions.append(_approval("field", item.field_id, item.content_hash, "approved", evidence))


def _expand_transform(
    raw: dict[str, Any],
    contracts: list[CatalogContract],
    decisions: list[ApprovalDecision],
) -> None:
    _require_keys(
        raw,
        required={
            "transform_id", "transform_version", "implementation_id", "code_hash",
            "inputs", "outputs", "parameters_schema", "time_policy", "deterministic",
            "resource_hint",
            "input_schema", "output_schema",
        },
        optional={"evidence_refs"},
        context="transform",
    )
    evidence = tuple(raw.get("evidence_refs", ()))
    if type(raw["deterministic"]) is not bool:
        raise CatalogParseError("transform deterministic 必须是 boolean")
    item = TransformContract(
        str(raw["transform_id"]),
        int(raw["transform_version"]),
        str(raw["implementation_id"]),
        str(raw["code_hash"]),
        tuple(str(value) for value in raw["inputs"]),
        tuple(str(value) for value in raw["outputs"]),
        raw["parameters_schema"],
        raw["time_policy"],
        raw["deterministic"],
        raw["resource_hint"],
        input_schema=raw["input_schema"],
        output_schema=raw["output_schema"],
    )
    contracts.append(item)
    decisions.append(
        _approval("transform", item.transform_id, item.content_hash, "approved", evidence)
    )


def re_escape(value: str) -> str:
    import re
    return re.escape(value)


__all__ = ["DeclarativeCatalog", "load_declarative_catalog", "load_default_declarative_catalog"]
