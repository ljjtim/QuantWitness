"""声明源到不可变 Catalog Lock 的确定性编译。"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
import json
import os
from pathlib import Path
import re
import shutil
from types import MappingProxyType
from typing import Any, Iterable

from research_pipeline.platform.canonical import canonical_json, typed_canonical_hash

from .discovery import DriftAttestation, evaluate_current_drift
from .errors import CatalogReferenceError
from .models import (
    ApprovalDecision,
    CatalogContract,
    CatalogCoverageBaseline,
    CatalogSourceManifest,
    DatasetContract,
    FieldContract,
    PhysicalBindingContract,
    PolicyContract,
    TransformContract,
)
from .schema import catalog_schema_bundle
from .source_loader import load_contract_payload
from .validation import validate_contract_set
from .transforms import TransformDescriptor, validate_transform_manifest


LOCK_FORMAT_VERSION = "catalog-lock-v2"
LEGACY_LOCK_FORMAT_VERSION = "catalog-lock-v1"
COMPILER_VERSION = "catalog-compiler-v2"
LEGACY_COMPILER_VERSION = "catalog-compiler-v1"
_SUPPORTED_LOCK_COMPILERS = {
    LOCK_FORMAT_VERSION: COMPILER_VERSION,
    LEGACY_LOCK_FORMAT_VERSION: LEGACY_COMPILER_VERSION,
}


def _identity(item: CatalogContract) -> tuple[str, str]:
    if isinstance(item, FieldContract):
        return "field", item.field_id
    if isinstance(item, DatasetContract):
        return "dataset", item.dataset_id
    if isinstance(item, PolicyContract):
        return "policy", item.policy_id
    if isinstance(item, PhysicalBindingContract):
        return "physical_binding", item.binding_id
    if isinstance(item, TransformContract):
        return "transform", item.transform_id
    raise CatalogReferenceError(f"不支持编译的运行合同: {type(item).__name__}")


def _decision_map(decisions: Iterable[ApprovalDecision]) -> dict[tuple[str, str], ApprovalDecision]:
    result: dict[tuple[str, str], ApprovalDecision] = {}
    for item in decisions:
        key = (item.target_kind, item.target_id)
        if key in result:
            raise CatalogReferenceError(f"重复治理决定: {key}")
        result[key] = item
    return result


def _read_current_release(release_root: Path) -> tuple[dict[str, Any], dict[str, Any]] | None:
    pointer = release_root / "CURRENT"
    if not pointer.is_file():
        return None
    compile_id = pointer.read_text(encoding="utf-8").strip()
    version = release_root / compile_id
    manifest = json.loads((version / "catalog.source-manifest.json").read_text(encoding="utf-8"))
    lock = json.loads((version / "catalog.lock.json").read_text(encoding="utf-8"))
    return manifest, lock


def _manifest_payload(manifest: CatalogSourceManifest) -> dict[str, Any]:
    payload = manifest.to_dict()
    payload["manifest_core_hash"] = typed_canonical_hash(payload)
    return payload


def _contract_version(item: CatalogContract) -> int:
    for name in (
        "field_version",
        "policy_version",
        "binding_version",
        "dataset_version",
        "transform_version",
    ):
        if hasattr(item, name):
            return int(getattr(item, name))
    raise CatalogReferenceError(f"运行合同缺少版本: {type(item).__name__}")


def compile_catalog(
    *,
    baseline: CatalogCoverageBaseline,
    expected_baseline_hash: str,
    manifest: CatalogSourceManifest,
    contracts: Iterable[CatalogContract],
    decisions: Iterable[ApprovalDecision],
    release_root: str | Path,
    transform_descriptors: Iterable[TransformDescriptor] | None = None,
) -> "CompiledCatalog":
    if transform_descriptors is None:
        from .builtin_transforms import build_mainline_transform_descriptors

        transform_descriptors = build_mainline_transform_descriptors()
    if baseline.content_hash != expected_baseline_hash or manifest.baseline_hash != expected_baseline_hash:
        raise CatalogReferenceError("coverage baseline hash 不匹配")
    root = Path(release_root)
    decision_items = tuple(decisions)
    previous = _read_current_release(root)
    previous_lock: dict[str, Any] | None = None
    candidate_manifest_payload = _manifest_payload(manifest)
    if previous is None:
        if manifest.previous_manifest_hash is not None:
            raise CatalogReferenceError("bootstrap manifest 的 previous 必须为空")
        required_slots = set(baseline.required_slots)
    else:
        previous_manifest, previous_lock = previous
        idempotent_recompile = candidate_manifest_payload["manifest_core_hash"] == previous_manifest["manifest_core_hash"]
        if not idempotent_recompile and manifest.previous_manifest_hash != previous_manifest["manifest_core_hash"]:
            raise CatalogReferenceError("previous manifest hash 断链")
        required_slots = set(previous_manifest["active_slots"])
        historical_retired_slots = set(previous_manifest.get("retired_slots", ()))
        reactivated = historical_retired_slots & set(manifest.active_slots)
        if reactivated:
            raise CatalogReferenceError(
                f"retired coverage slot 不得重新激活: {sorted(reactivated)}"
            )
        missing_retired = historical_retired_slots - set(manifest.retired_slots)
        if missing_retired:
            raise CatalogReferenceError(
                f"retired coverage tombstone 无痕消失: {sorted(missing_retired)}"
            )

    candidate_runtime_contracts = tuple(
        item
        for item in contracts
        if isinstance(
            item,
            (
                FieldContract,
                DatasetContract,
                PolicyContract,
                PhysicalBindingContract,
                TransformContract,
            ),
        )
    )
    decision_by_target = _decision_map(decision_items)
    from research_pipeline.platform.minute_reference import (
        MinuteCapabilityManifestError,
        require_current_minute_catalog_capability_binding,
    )

    for item in candidate_runtime_contracts:
        if (
            isinstance(item, PolicyContract)
            and item.status != "removed"
            and item.validator_id == "catalog.minute.capability-manifest.v1"
            and decision_by_target.get(("policy", item.policy_id)) is not None
            and decision_by_target[("policy", item.policy_id)].decision == "approved"
        ):
            try:
                require_current_minute_catalog_capability_binding(
                    consumer_id=str(item.rules.get("consumer_id")),
                    manifest_hash=str(
                        item.rules.get("minute_capability_manifest_hash")
                    ),
                    contract_version=str(item.rules.get("contract_version")),
                    binding_hash=str(item.rules.get("binding_hash")),
                )
            except MinuteCapabilityManifestError as exc:
                raise CatalogReferenceError(str(exc)) from exc
    validate_contract_set(candidate_runtime_contracts)
    approved_contracts: list[CatalogContract] = []
    for item in candidate_runtime_contracts:
        kind, identifier = _identity(item)
        decision = decision_by_target.get((kind, identifier))
        if decision is None or decision.target_hash != item.content_hash:
            raise CatalogReferenceError(
                f"运行合同缺少 payload 决定: {kind}/{identifier}"
            )
        if decision.decision == "approved":
            if getattr(item, "status", "approved") == "blocked":
                raise CatalogReferenceError(
                    f"blocked 合同不能通过 Approval 进入运行 Lock: {kind}/{identifier}"
                )
            approved_contracts.append(item)
        elif decision.decision in {"blocked", "rejected"}:
            if getattr(item, "status", None) == "removed":
                # removed只进入永久ID墓碑，不进入运行索引；保留blocked/rejected
                # 决定，避免把尚未具备证据的字段写成获准运行。
                approved_contracts.append(item)
        else:
            raise CatalogReferenceError(f"运行合同决定非法: {kind}/{identifier}")
    runtime_contracts = tuple(approved_contracts)
    validate_contract_set(runtime_contracts)
    approved_transforms = tuple(
        item for item in runtime_contracts if isinstance(item, TransformContract)
    )
    if approved_transforms:
        validate_transform_manifest(approved_transforms, tuple(transform_descriptors))

    candidate_by_identity = {
        _identity(item): item for item in candidate_runtime_contracts
    }
    runtime_identities = {_identity(item) for item in runtime_contracts}
    if previous_lock is not None:
        previous_active: dict[tuple[str, str], dict[str, Any]] = {}
        for group, kind, key in (
            ("fields", "field", "field_id"),
            ("datasets", "dataset", "dataset_id"),
            ("policies", "policy", "policy_id"),
            ("bindings", "physical_binding", "binding_id"),
            ("transforms", "transform", "transform_id"),
        ):
            for raw in previous_lock.get(group, []):
                previous_active[(kind, str(raw[key]))] = raw
        previous_tombstones = {
            (str(item["kind"]), str(item["id"])): str(item["payload_hash"])
            for item in previous_lock.get("tombstones", [])
        }
        for identity, previous_raw in previous_active.items():
            current = candidate_by_identity.get(identity)
            if current is None:
                raise CatalogReferenceError(
                    f"永久 ID 无痕消失，必须提交 tombstone: {identity}"
                )
            if identity not in runtime_identities and getattr(current, "status", None) != "removed":
                raise CatalogReferenceError(
                    f"运行合同退役必须使用 removed tombstone: {identity}"
                )
            previous_model = load_contract_payload(
                {"kind": "binding" if identity[0] == "physical_binding" else identity[0], **previous_raw}
            )
            if current.content_hash != previous_model.content_hash and _contract_version(
                current
            ) <= _contract_version(previous_model):
                raise CatalogReferenceError(
                    f"永久 ID 内容变化必须提高版本: {identity}"
                )
        for identity, payload_hash in previous_tombstones.items():
            current = candidate_by_identity.get(identity)
            if (
                current is None
                or getattr(current, "status", None) != "removed"
                or current.content_hash != payload_hash
                or identity not in runtime_identities
            ):
                raise CatalogReferenceError(
                    f"removed 永久 ID 不得复用或丢失: {identity}"
                )
    runtime_dataset_ids = {
        item.dataset_id for item in runtime_contracts if isinstance(item, DatasetContract)
    }
    for slot in required_slots | set(manifest.active_slots) | set(manifest.retired_slots):
        decision = decision_by_target.get(("coverage_slot", slot))
        if decision is None:
            raise CatalogReferenceError(f"coverage slot 缺少决定: {slot}")
        if slot in manifest.active_slots and decision.decision not in {"approved", "blocked", "rejected"}:
            raise CatalogReferenceError(f"active coverage slot 决定非法: {slot}")
        if slot in manifest.active_slots:
            targets = tuple(manifest.slot_targets.get(slot, ()))
            if not targets:
                raise CatalogReferenceError(f"active coverage slot 缺少目标: {slot}")
            if decision.decision == "approved":
                missing_targets = set(targets) - runtime_dataset_ids
                if missing_targets:
                    raise CatalogReferenceError(
                        f"approved coverage slot 缺少运行 dataset: {slot}/{sorted(missing_targets)}"
                    )
            else:
                unknown_targets = [
                    target
                    for target in targets
                    if target not in runtime_dataset_ids
                    and ("dataset", target) not in decision_by_target
                ]
                if unknown_targets:
                    raise CatalogReferenceError(
                        f"blocked/rejected coverage slot 缺少目标决定: {slot}/{unknown_targets}"
                    )
        if slot in manifest.retired_slots and decision.decision != "retired":
            raise CatalogReferenceError(f"retired coverage slot 缺少 retired 决定: {slot}")
    disappeared = required_slots - set(manifest.active_slots) - set(manifest.retired_slots)
    if disappeared:
        raise CatalogReferenceError(f"coverage slot 无痕消失: {sorted(disappeared)}")

    manifest_payload = candidate_manifest_payload
    decision_payload = [item.to_dict() for item in sorted(decision_items, key=lambda x: (x.target_kind, x.target_id))]
    compile_id = typed_canonical_hash({"compiler_version": COMPILER_VERSION, "manifest_core_hash": manifest_payload["manifest_core_hash"], "decisions": decision_payload})[:24]
    groups = {
        "fields": [item.to_dict() for item in runtime_contracts if isinstance(item, FieldContract) and item.status != "removed"],
        "datasets": [item.to_dict() for item in runtime_contracts if isinstance(item, DatasetContract) and item.status != "removed"],
        "policies": [item.to_dict() for item in runtime_contracts if isinstance(item, PolicyContract) and item.status != "removed"],
        "bindings": [item.to_dict() for item in runtime_contracts if isinstance(item, PhysicalBindingContract) and item.status != "removed"],
        "transforms": [item.to_dict() for item in runtime_contracts if isinstance(item, TransformContract) and item.status != "removed"],
    }
    tombstones = [{"kind": _identity(item)[0], "id": _identity(item)[1], "payload_hash": item.content_hash} for item in runtime_contracts if getattr(item, "status", None) == "removed"]
    audit_payload = {
        "compile_id": compile_id,
        "source_manifest_hash": manifest_payload["manifest_core_hash"],
        "decisions": decision_payload,
        "blocked_entries": [
            item for item in decision_payload if item["decision"] == "blocked"
        ],
        "rejected_entries": [
            item for item in decision_payload if item["decision"] == "rejected"
        ],
        "retired_slots": [
            item
            for item in decision_payload
            if item["target_kind"] == "coverage_slot"
            and item["decision"] == "retired"
        ],
    }
    audit_payload["audit_hash"] = typed_canonical_hash(audit_payload)
    lock_payload: dict[str, Any] = {
        "lock_format_version": LOCK_FORMAT_VERSION,
        "compiler_version": COMPILER_VERSION,
        "compile_id": compile_id,
        "coverage_baseline_hash": expected_baseline_hash,
        "source_manifest_hash": manifest_payload["manifest_core_hash"],
        "previous_manifest_hash": manifest.previous_manifest_hash,
        "audit_hash": audit_payload["audit_hash"],
        **groups,
        "tombstones": tombstones,
    }
    lock_payload["catalog_hash"] = typed_canonical_hash(lock_payload)
    compiled = CompiledCatalog.from_payload(lock_payload)
    _publish_release(root, compile_id, manifest_payload, lock_payload, audit_payload)
    return compiled


def _publish_release(root: Path, compile_id: str, manifest: dict[str, Any], lock: dict[str, Any], audit: dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    target = root / compile_id
    if not target.exists():
        staging = root / f".{compile_id}.staging"
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir()
        for name, payload in (
            ("catalog.source-manifest.json", manifest),
            ("catalog.lock.json", lock),
            ("catalog.audit.json", audit),
            ("catalog.schema.json", catalog_schema_bundle()),
        ):
            (staging / name).write_text(canonical_json(payload), encoding="utf-8")
        os.replace(staging, target)
    pointer_tmp = root / ".CURRENT.tmp"
    pointer_tmp.write_text(compile_id, encoding="utf-8")
    os.replace(pointer_tmp, root / "CURRENT")


@dataclass(frozen=True)
class CompiledCatalog:
    payload: MappingProxyType
    fields: MappingProxyType
    datasets: MappingProxyType
    policies: MappingProxyType
    bindings: MappingProxyType
    binding_resolutions: MappingProxyType
    transforms: MappingProxyType

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "CompiledCatalog":
        lock_format_version = payload.get("lock_format_version")
        compiler_version = payload.get("compiler_version")
        if lock_format_version not in _SUPPORTED_LOCK_COMPILERS:
            raise CatalogReferenceError("Catalog Lock format version 不受支持")
        if compiler_version != _SUPPORTED_LOCK_COMPILERS[lock_format_version]:
            raise CatalogReferenceError("Catalog Lock compiler version 不受支持")
        compile_id = payload.get("compile_id")
        if not isinstance(compile_id, str) or re.fullmatch(r"[0-9a-f]{24}", compile_id) is None:
            raise CatalogReferenceError("Catalog Lock compile ID 非法")
        expected = payload.get("catalog_hash")
        unsigned = {key: value for key, value in payload.items() if key != "catalog_hash"}
        if expected != typed_canonical_hash(unsigned):
            raise CatalogReferenceError("Catalog Lock hash 校验失败")
        reconstructed: list[CatalogContract] = []
        for group, kind in (
            ("fields", "field"),
            ("datasets", "dataset"),
            ("policies", "policy"),
            ("bindings", "binding"),
            ("transforms", "transform"),
        ):
            for item in payload.get(group, []):
                reconstructed.append(load_contract_payload({"kind": kind, **item}))
        validate_contract_set(reconstructed)
        def index(group: str, key: str) -> MappingProxyType:
            return MappingProxyType({str(item[key]): MappingProxyType(dict(item)) for item in payload.get(group, [])})
        binding_index = index("bindings", "binding_id")
        resolution_index: dict[tuple[object, ...], MappingProxyType] = {}
        for binding in binding_index.values():
            key = (
                binding["dataset_id"],
                binding["dataset_version"],
                binding["source_profile"],
                binding["environment"],
                binding["binding_version"],
            )
            if key in resolution_index:
                raise CatalogReferenceError(f"binding 复合解析键重复: {key}")
            resolution_index[key] = binding
        return cls(
            MappingProxyType(dict(payload)),
            index("fields", "field_id"),
            index("datasets", "dataset_id"),
            index("policies", "policy_id"),
            binding_index,
            MappingProxyType(resolution_index),
            index("transforms", "transform_id"),
        )

    @classmethod
    def load(cls, release_root: str | Path) -> "CompiledCatalog":
        root = Path(release_root)
        compile_id = (root / "CURRENT").read_text(encoding="utf-8").strip()
        if re.fullmatch(r"[0-9a-f]{24}", compile_id) is None:
            raise CatalogReferenceError("CURRENT compile ID 非法")
        release = root / compile_id
        payload = json.loads((release / "catalog.lock.json").read_text(encoding="utf-8"))
        manifest = json.loads(
            (release / "catalog.source-manifest.json").read_text(encoding="utf-8")
        )
        audit = json.loads((release / "catalog.audit.json").read_text(encoding="utf-8"))
        manifest_hash = manifest.get("manifest_core_hash")
        unsigned_manifest = {
            key: value for key, value in manifest.items() if key != "manifest_core_hash"
        }
        if manifest_hash != typed_canonical_hash(unsigned_manifest):
            raise CatalogReferenceError("source manifest hash 校验失败")
        if payload.get("compile_id") != compile_id or audit.get("compile_id") != compile_id:
            raise CatalogReferenceError("CURRENT/lock/audit compile ID 不匹配")
        if (
            payload.get("source_manifest_hash") != manifest_hash
            or audit.get("source_manifest_hash") != manifest_hash
        ):
            raise CatalogReferenceError("lock/audit/source manifest 不匹配")
        unsigned_audit = {
            key: value for key, value in audit.items() if key != "audit_hash"
        }
        if audit.get("audit_hash") != typed_canonical_hash(unsigned_audit):
            raise CatalogReferenceError("catalog audit hash 校验失败")
        if payload.get("audit_hash") != audit.get("audit_hash"):
            raise CatalogReferenceError("lock 与 audit hash 不匹配")
        decisions = audit.get("decisions")
        if not isinstance(decisions, list):
            raise CatalogReferenceError("catalog audit decisions 非法")
        compiler_version = payload.get("compiler_version")
        if compiler_version not in _SUPPORTED_LOCK_COMPILERS.values():
            raise CatalogReferenceError("Catalog Lock compiler version 不受支持")
        expected_compile_id = typed_canonical_hash(
            {
                "compiler_version": compiler_version,
                "manifest_core_hash": manifest_hash,
                "decisions": decisions,
            }
        )[:24]
        if expected_compile_id != compile_id:
            raise CatalogReferenceError("catalog audit decisions 与 compile ID 不匹配")
        for name, predicate in (
            ("blocked_entries", lambda item: item.get("decision") == "blocked"),
            ("rejected_entries", lambda item: item.get("decision") == "rejected"),
            (
                "retired_slots",
                lambda item: item.get("target_kind") == "coverage_slot"
                and item.get("decision") == "retired",
            ),
        ):
            if audit.get(name) != [item for item in decisions if predicate(item)]:
                raise CatalogReferenceError(f"catalog audit {name} 与 decisions 不一致")
        return cls.from_payload(payload)

    @classmethod
    def load_default(cls) -> "CompiledCatalog":
        """加载宿主额外提供的默认锁；公开发行包不附带数据源 Catalog。"""

        root = resources.files("research_pipeline.catalog").joinpath("default_lock")
        try:
            with resources.as_file(root.joinpath("CURRENT")) as pointer:
                return cls.load(pointer.parent)
        except FileNotFoundError as exc:
            raise CatalogReferenceError(
                "公开发行包不附带默认 Catalog Lock；请使用 "
                "CompiledCatalog.load(<Catalog-Lock目录>)"
            ) from exc

    @property
    def catalog_hash(self) -> str:
        return str(self.payload["catalog_hash"])

    def require_field(self, field_id: str) -> MappingProxyType:
        try:
            return self.fields[field_id]
        except KeyError as exc:
            raise CatalogReferenceError(f"未知 field: {field_id}") from exc


class CatalogPreflight:
    def __init__(self, catalog: CompiledCatalog) -> None:
        self.catalog = catalog

    def resolve_current_binding(
        self, *, inspector: Any, dataset_id: str, dataset_version: int,
        source_profile: str, environment: str, binding_version: int,
    ) -> tuple[MappingProxyType, DriftAttestation]:
        resolution_key = (
            dataset_id,
            dataset_version,
            source_profile,
            environment,
            binding_version,
        )
        resolved = self.catalog.binding_resolutions.get(resolution_key)
        if resolved is None:
            raise CatalogReferenceError("binding 解析必须唯一，实际 0")
        raw = dict(resolved)
        binding = PhysicalBindingContract(**raw)
        dataset_raw = self.catalog.datasets.get(dataset_id)
        if dataset_raw is None:
            raise CatalogReferenceError("binding 引用未知 dataset")
        self._validate_point_in_time_contract(dataset_raw, binding)
        policy_raw = self.catalog.policies.get(binding.drift_policy_id)
        if policy_raw is None:
            raise CatalogReferenceError("binding 引用未知 drift policy")
        policy = PolicyContract(**dict(policy_raw))
        inventory = inspector.observe_current_schema(binding.object_name)
        physical_columns = {item.name for item in inventory.columns}
        mapped_columns = {
            str(value["column"])
            for value in binding.column_bindings.values()
            if value.get("kind") == "direct" and value.get("column")
        }
        missing_columns = mapped_columns - physical_columns
        if missing_columns:
            raise CatalogReferenceError(
                f"binding direct 列不存在: {sorted(missing_columns)}"
            )
        _, attestation = evaluate_current_drift(catalog_hash=self.catalog.catalog_hash, binding=binding, policy=policy, inventory=inventory)
        return resolved, attestation

    def _validate_point_in_time_contract(
        self,
        dataset: MappingProxyType,
        binding: PhysicalBindingContract,
    ) -> None:
        field_items = [self.catalog.fields[field_id] for field_id in dataset["fields"]]
        availability_raw = self.catalog.policies.get(dataset["available_time_policy"])
        revision_raw = self.catalog.policies.get(dataset["revision_policy_id"])
        if availability_raw is None or revision_raw is None:
            raise CatalogReferenceError("dataset 缺少 availability/revision policy")
        direct_field_ids = {
            str(field_id)
            for field_id, value in binding.column_bindings.items()
            if value.get("kind") == "direct" and "column" in value
        }
        point_in_time_fields = [
            item
            for item in field_items
            if item.get("observation_model") == "point_in_time"
        ]
        if point_in_time_fields:
            available_time_fields = {
                str(item["observation_keys"]["available_time_field"])
                for item in point_in_time_fields
            }
            revision_fields = {
                str(item["observation_keys"]["revision_field"])
                for item in point_in_time_fields
            }
            if availability_raw["rules"].get("available_time_field") not in available_time_fields:
                raise CatalogReferenceError("PIT dataset availability policy 未绑定可见时间字段")
            if revision_raw["rules"].get("future_revision") != "reject":
                raise CatalogReferenceError("PIT dataset 必须拒绝未来修订")
            if revision_raw["rules"].get("revision_field") not in revision_fields:
                raise CatalogReferenceError("PIT dataset revision policy 未绑定修订字段")
            if not (available_time_fields | revision_fields) <= direct_field_ids:
                raise CatalogReferenceError("PIT dataset 缺少可见时间/修订物理绑定")
        if str(dataset["dataset_id"]).startswith("factor."):
            if revision_raw["rules"].get("draft_publication") != "reject":
                raise CatalogReferenceError("正式因子必须拒绝 draft publication")
            next_session_open = (
                dataset["available_time_policy"] == "available.daily.v1"
                and availability_raw["rules"].get("available_after")
                == "next_session_open"
            )
            event_session_open = (
                availability_raw["rules"].get("available_after") == "session_open"
                and availability_raw["rules"].get("available_time_field")
                == dataset["event_time_field"]
            )
            if dataset["frequency"] == "daily" and not (
                next_session_open or event_session_open
            ):
                raise CatalogReferenceError(
                    "日频正式因子必须按观测日下一交易时段开盘可见，或将"
                    "事件日字段明确绑定为当日开盘可见；"
                    "publication身份由AdmittedQueryPlan v5单独绑定"
                )


__all__ = [
    "COMPILER_VERSION",
    "LEGACY_COMPILER_VERSION",
    "LEGACY_LOCK_FORMAT_VERSION",
    "LOCK_FORMAT_VERSION",
    "CatalogPreflight",
    "CompiledCatalog",
    "compile_catalog",
]
