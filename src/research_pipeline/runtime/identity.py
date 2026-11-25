"""分层运行身份：完整审计信息与最小缓存兼容画像彼此独立。"""

from __future__ import annotations

from functools import cached_property

from dataclasses import dataclass
from enum import Enum
import hashlib
import os
from pathlib import Path
import platform
import re
import sys
from types import MappingProxyType
from typing import Mapping

from research_pipeline.platform.build_manifest import BuildManifest
from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import ArtifactRef, DeterminismContext, NodeSpec
from .errors import RuntimeIntegrityError
from .graph import DagSpec


AUDIT_ENVIRONMENT_VERSION = "research-audit-environment-v2"
CACHE_COMPATIBILITY_VERSION = "research-cache-compatibility-v1"
EXECUTION_IDENTITY_VERSION = "research-execution-identity-v2"
IDENTITY_VERSION = EXECUTION_IDENTITY_VERSION
_ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _require_digest(value: str, field: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RuntimeIntegrityError(f"{field} 必须是 sha256 小写摘要")
    return value


def _digest_mapping(value: Mapping[str, str], field: str, *, allow_empty: bool = True) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or (not allow_empty and not value):
        raise RuntimeIntegrityError(f"{field} 必须是{'非空' if not allow_empty else ''}映射")
    normalized: dict[str, str] = {}
    for key, digest in value.items():
        if not isinstance(key, str) or not key:
            raise RuntimeIntegrityError(f"{field} 包含空或非字符串键")
        normalized[key] = _require_digest(digest, f"{field}.{key}")
    return MappingProxyType(dict(sorted(normalized.items())))


def _payload_string_mapping(value: object, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in value.items()
    ):
        raise RuntimeIntegrityError(f"{field} schema 无效")
    return dict(value)


_WORKER_HELPER_FILES = (
    "project_table_input.py", "project_output.py", "walk_forward_model_execution.py", "project_causal.py",
    "project_causal_worker.py", "project_causal_execution.py", "artifact_lifecycle.py",
    "project_causal_minute.py", "causal_facts_codec.py", "causal_worker_session.py",
    "worker_process_session.py", "lean_strategy_execution.py", "lean_execution_writer.py",
    "lean_strategy_inputs.py", "lean_market_source.py", "lean_minute_parquet.py",
    "lean_financial_day.py", "lean_futures_inputs.py", "lean_native_data.py",
    "lean_native_direct.py", "lean_native_inputs.py", "portfolio_target_inputs.py",
)
_SNAPSHOT_EXCLUDED_HELPERS = frozenset({
    "walk_forward_model_execution.py", "project_causal.py",
    "project_causal_worker.py", "project_causal_execution.py",
    "project_causal_minute.py", "causal_facts_codec.py",
    "portfolio_target_inputs.py",
})
_STRATEGY_CONTRACT_FILES = (
    "domain/strategy_declaration.py", "domain/lean_algorithm.py",
    "results/contracts.py", "platform/lean_contract.py", "platform/lean_native_support.py",
    "platform/lean_execution_schema.py", "platform/lean_result_projection.py", "research/lean_metrics.py",
    "platform/lean_runtime.py", "platform/daily_projection.py", "extensions/project_bundle.py", "data_plane/dataset_files.py",
    "data_plane/dataset_artifacts.py", "data_plane/direct_sources.py", "data_plane/archive_read.py",
    "data_plane/providers/epoch_date.py", "data_plane/providers/sql.py", "lean_data.py",
)
_SNAPSHOT_EXCLUDED_CONTRACTS = frozenset({
    "lean_algorithm.py", "lean_contract.py", "lean_native_support.py",
    "lean_execution_schema.py", "lean_result_projection.py",
    "lean_metrics.py", "lean_runtime.py", "lean_data.py",
})
_CAUSAL_WORKER_HELPERS = frozenset({
    "project_causal.py", "project_causal_worker.py",
    "project_causal_execution.py", "project_causal_minute.py",
    "causal_facts_codec.py", "causal_worker_session.py",
})
_RESOURCE_CONTRACT_FILES = ("resource_budget.py", "process_resources.py")


def project_worker_adapter_hash(manifest=None) -> str:
    """绑定实际 Worker adapter 与 Runtime bridge 源码，按能力裁剪 LEAN/causal 边界。"""
    runtime_path = Path(__file__).with_name("project_operator_runtime.py").resolve(strict=True)
    worker_path = runtime_path.with_name("project_operator_worker.py")
    snapshot_type = "data.adjustment-factor-snapshot.v1"
    spec = manifest.operator_spec if manifest is not None else None
    snapshot_only = (
        spec is not None and manifest.lean_algorithm is None and not spec.strategy_roles
        and bool(spec.input_ports) and all(port.artifact_type == snapshot_type
                                          for port in (*spec.input_ports, *spec.output_ports))
    )
    uses_lean = manifest is None or manifest.lean_algorithm is not None
    uses_causal = manifest is None or (
        spec is not None and any(param.name == "causal_plan" for param in spec.parameters)
    )
    # 共享 Worker 和桥接负责快照端口校验、文件交付与资源验收，继续绑定其实现。
    def source_digest(path):
        source = path.read_bytes()
        return hashlib.sha256(source.replace(b"\r\n", b"\n") if snapshot_only else source).hexdigest()

    payload = {
        "contract_version": (
            "project-snapshot-worker-adapter-code-v2" if snapshot_only
            else "project-operator-worker-adapter-code-v1"
        ),
        "runtime_bridge_sha256": source_digest(runtime_path),
        "worker_adapter_sha256": source_digest(worker_path),
        "helpers": {
            name: source_digest(runtime_path.with_name(name))
            for name in _WORKER_HELPER_FILES
            if (not snapshot_only or name not in _SNAPSHOT_EXCLUDED_HELPERS)
            and (uses_lean or not name.startswith("lean_"))
            and (uses_causal or name not in _CAUSAL_WORKER_HELPERS)
        },
        "strategy_contracts": {
            name: hashlib.sha256(runtime_path.parent.parent.joinpath(name).read_bytes()).hexdigest()
            for name in _STRATEGY_CONTRACT_FILES
            if (uses_lean and not snapshot_only) or name.rsplit("/", 1)[-1] not in _SNAPSHOT_EXCLUDED_CONTRACTS
        },
        "resource_contracts": {
            name: hashlib.sha256(
                runtime_path.parent.parent.joinpath("platform", name).read_bytes()
            ).hexdigest()
            for name in _RESOURCE_CONTRACT_FILES
        },
    }
    if uses_causal:
        payload["causal_contract"] = hashlib.sha256(
            runtime_path.parent.parent.joinpath("platform", "project_causal_contract.py").read_bytes()
        ).hexdigest()
        payload["causal_time_contract"] = hashlib.sha256(
            runtime_path.parent.parent.joinpath("platform", "causal_time.py").read_bytes()
        ).hexdigest()
    return typed_canonical_hash(payload)

class CacheCompatibilityMode(str, Enum):
    BYTE_EXACT = "byte_exact"
    SEMANTIC = "semantic"
    NUMERICAL = "numerical"


@dataclass(frozen=True)
class AuditEnvironmentManifest:
    """可解释一次执行环境；环境值只保存摘要，不保存明文。"""

    python_implementation: str
    python_version: tuple[int, int, int]
    python_cache_tag: str
    platform_system: str
    platform_release: str
    platform_machine: str
    byteorder: str
    build_manifest_digest: str
    build_artifact_digest: str
    dependency_distribution_digests: Mapping[str, str]
    environment_input_digests: Mapping[str, str]
    worker_profile_digest: str | None = None
    contract_version: str = AUDIT_ENVIRONMENT_VERSION

    def __post_init__(self) -> None:
        text_fields = (
            self.python_implementation,
            self.python_cache_tag,
            self.platform_system,
            self.platform_release,
            self.platform_machine,
            self.byteorder,
        )
        if any(not isinstance(value, str) or not value for value in text_fields):
            raise RuntimeIntegrityError("AuditEnvironmentManifest 平台字段不完整")
        if (
            len(self.python_version) != 3
            or any(type(value) is not int or value < 0 for value in self.python_version)
        ):
            raise RuntimeIntegrityError("AuditEnvironmentManifest Python 版本无效")
        if self.contract_version != AUDIT_ENVIRONMENT_VERSION:
            raise RuntimeIntegrityError("AuditEnvironmentManifest 版本不受支持")
        if self.worker_profile_digest is not None:
            _require_digest(self.worker_profile_digest, "worker_profile_digest")
        _require_digest(self.build_manifest_digest, "build_manifest_digest")
        _require_digest(self.build_artifact_digest, "build_artifact_digest")
        object.__setattr__(
            self,
            "dependency_distribution_digests",
            _digest_mapping(self.dependency_distribution_digests, "dependency_distribution_digests"),
        )
        environment = _digest_mapping(self.environment_input_digests, "environment_input_digests")
        if any(not _ENVIRONMENT_NAME.fullmatch(name) for name in environment):
            raise RuntimeIntegrityError("AuditEnvironmentManifest 环境变量名无效")
        object.__setattr__(self, "environment_input_digests", environment)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "python_implementation": self.python_implementation,
            "python_version": list(self.python_version),
            "python_cache_tag": self.python_cache_tag,
            "platform_system": self.platform_system,
            "platform_release": self.platform_release,
            "platform_machine": self.platform_machine,
            "byteorder": self.byteorder,
            "build_manifest_digest": self.build_manifest_digest,
            "build_artifact_digest": self.build_artifact_digest,
            "dependency_distribution_digests": dict(self.dependency_distribution_digests),
            "environment_input_digests": dict(self.environment_input_digests),
            "worker_profile_digest": self.worker_profile_digest,
            "contract_version": self.contract_version,
        }
        return payload

    @cached_property
    def manifest_digest(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AuditEnvironmentManifest":
        expected = {
            "python_implementation", "python_version", "python_cache_tag", "platform_system",
            "platform_release", "platform_machine", "byteorder", "build_artifact_digest",
            "build_manifest_digest", "dependency_distribution_digests",
            "environment_input_digests", "worker_profile_digest", "contract_version",
        }
        if payload.get("contract_version") != AUDIT_ENVIRONMENT_VERSION:
            raise RuntimeIntegrityError("AuditEnvironmentManifest 版本不受支持")
        version = payload.get("python_version")
        dependencies = payload.get("dependency_distribution_digests")
        environment = payload.get("environment_input_digests")
        if (
            set(payload) != expected
            or not isinstance(version, list)
            or len(version) != 3
            or any(type(value) is not int for value in version)
            or not isinstance(dependencies, Mapping)
            or not isinstance(environment, Mapping)
            or (
                payload.get("worker_profile_digest") is not None
                and not isinstance(payload.get("worker_profile_digest"), str)
            )
        ):
            raise RuntimeIntegrityError("AuditEnvironmentManifest schema 无效")
        string_fields = (
            "python_implementation", "python_cache_tag", "platform_system", "platform_release",
            "platform_machine", "byteorder", "build_manifest_digest", "build_artifact_digest",
            "contract_version",
        )
        if any(not isinstance(payload[field], str) for field in string_fields):
            raise RuntimeIntegrityError("AuditEnvironmentManifest 字段类型无效")
        return cls(
            str(payload["python_implementation"]),
            tuple(int(value) for value in version),
            str(payload["python_cache_tag"]),
            str(payload["platform_system"]),
            str(payload["platform_release"]),
            str(payload["platform_machine"]),
            str(payload["byteorder"]),
            str(payload["build_manifest_digest"]),
            str(payload["build_artifact_digest"]),
            _payload_string_mapping(dependencies, "dependency_distribution_digests"),
            _payload_string_mapping(environment, "environment_input_digests"),
            payload.get("worker_profile_digest") if isinstance(payload.get("worker_profile_digest"), str) else None,
            str(payload["contract_version"]),
        )

    @classmethod
    def capture(
        cls,
        *,
        build_artifact_digest: str,
        build_manifest_digest: str | None = None,
        dependency_distribution_digests: Mapping[str, str] | None = None,
        allowed_environment_names: tuple[str, ...] = (),
        environment: Mapping[str, str] | None = None,
        worker_profile_digest: str | None = None,
    ) -> "AuditEnvironmentManifest":
        names = tuple(sorted(set(allowed_environment_names)))
        if len(names) != len(allowed_environment_names) or any(
            not _ENVIRONMENT_NAME.fullmatch(name) for name in names
        ):
            raise RuntimeIntegrityError("允许的环境变量名必须唯一、排序后可规范化")
        source = os.environ if environment is None else environment
        environment_digests = {
            name: typed_canonical_hash({"is_set": name in source, "value": source.get(name)})
            for name in names
        }
        return cls(
            platform.python_implementation(),
            (sys.version_info.major, sys.version_info.minor, sys.version_info.micro),
            sys.implementation.cache_tag,
            platform.system().lower() or "unknown",
            platform.release() or "unknown",
            platform.machine().lower() or "unknown",
            sys.byteorder,
            build_manifest_digest or build_artifact_digest,
            build_artifact_digest,
            dependency_distribution_digests or {},
            environment_digests,
            worker_profile_digest,
        )

    @classmethod
    def from_build_manifest(
        cls,
        manifest: BuildManifest,
        *,
        allowed_environment_names: tuple[str, ...] = (),
        environment: Mapping[str, str] | None = None,
        worker_profile_digest: str | None = None,
    ) -> "AuditEnvironmentManifest":
        return cls.capture(
            build_manifest_digest=manifest.manifest_hash,
            build_artifact_digest=manifest.wheel_digest,
            dependency_distribution_digests=manifest.dependency_distribution_digests,
            allowed_environment_names=allowed_environment_names,
            environment=environment,
            worker_profile_digest=worker_profile_digest,
        )


@dataclass(frozen=True)
class CacheCompatibilityProfile:
    """只包含声明会影响缓存兼容性的环境摘要。"""

    mode: CacheCompatibilityMode
    implementation_digest: str
    runtime_compatibility: Mapping[str, str]
    dependency_digests: Mapping[str, str]
    numerical_backend_digests: Mapping[str, str]
    environment_input_digests: Mapping[str, str]
    build_artifact_digest: str | None = None
    contract_version: str = CACHE_COMPATIBILITY_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.mode, CacheCompatibilityMode):
            raise RuntimeIntegrityError("CacheCompatibilityProfile mode 无效")
        _require_digest(self.implementation_digest, "implementation_digest")
        if self.contract_version != CACHE_COMPATIBILITY_VERSION:
            raise RuntimeIntegrityError("CacheCompatibilityProfile 版本不受支持")
        runtime = {
            str(key): str(value)
            for key, value in self.runtime_compatibility.items()
            if isinstance(key, str) and key and isinstance(value, str) and value
        }
        if not runtime or len(runtime) != len(self.runtime_compatibility):
            raise RuntimeIntegrityError("CacheCompatibilityProfile runtime_compatibility 无效")
        object.__setattr__(self, "runtime_compatibility", MappingProxyType(dict(sorted(runtime.items()))))
        object.__setattr__(self, "dependency_digests", _digest_mapping(self.dependency_digests, "dependency_digests"))
        object.__setattr__(
            self,
            "numerical_backend_digests",
            _digest_mapping(self.numerical_backend_digests, "numerical_backend_digests"),
        )
        object.__setattr__(
            self,
            "environment_input_digests",
            _digest_mapping(self.environment_input_digests, "environment_input_digests"),
        )
        if self.mode is CacheCompatibilityMode.BYTE_EXACT:
            if self.build_artifact_digest is None or not self.dependency_digests:
                raise RuntimeIntegrityError("byte_exact 画像必须绑定构建制品和关键依赖")
            _require_digest(self.build_artifact_digest, "build_artifact_digest")
        elif self.mode is CacheCompatibilityMode.SEMANTIC:
            if self.build_artifact_digest is not None or not self.dependency_digests:
                raise RuntimeIntegrityError("semantic 画像必须绑定实际依赖身份")
        elif not self.numerical_backend_digests:
            raise RuntimeIntegrityError("numerical 画像必须绑定数值后端")
        if any(not _ENVIRONMENT_NAME.fullmatch(name) for name in self.environment_input_digests):
            raise RuntimeIntegrityError("CacheCompatibilityProfile 环境变量名无效")

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "implementation_digest": self.implementation_digest,
            "runtime_compatibility": dict(self.runtime_compatibility),
            "dependency_digests": dict(self.dependency_digests),
            "numerical_backend_digests": dict(self.numerical_backend_digests),
            "environment_input_digests": dict(self.environment_input_digests),
            "build_artifact_digest": self.build_artifact_digest,
            "contract_version": self.contract_version,
        }

    @cached_property
    def profile_digest(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "CacheCompatibilityProfile":
        expected = {
            "mode", "implementation_digest", "runtime_compatibility", "dependency_digests",
            "numerical_backend_digests", "environment_input_digests", "build_artifact_digest",
            "contract_version",
        }
        mapping_fields = (
            "runtime_compatibility", "dependency_digests", "numerical_backend_digests",
            "environment_input_digests",
        )
        if set(payload) != expected or any(not isinstance(payload.get(field), Mapping) for field in mapping_fields):
            raise RuntimeIntegrityError("CacheCompatibilityProfile schema 无效")
        build_digest = payload["build_artifact_digest"]
        if build_digest is not None and not isinstance(build_digest, str):
            raise RuntimeIntegrityError("CacheCompatibilityProfile build_artifact_digest 类型无效")
        try:
            mode = CacheCompatibilityMode(payload["mode"])
        except (TypeError, ValueError) as exc:
            raise RuntimeIntegrityError("CacheCompatibilityProfile mode 无效") from exc
        return cls(
            mode,
            str(payload["implementation_digest"]),
            _payload_string_mapping(payload["runtime_compatibility"], "runtime_compatibility"),
            _payload_string_mapping(payload["dependency_digests"], "dependency_digests"),
            _payload_string_mapping(payload["numerical_backend_digests"], "numerical_backend_digests"),
            _payload_string_mapping(payload["environment_input_digests"], "environment_input_digests"),
            build_digest,
            str(payload["contract_version"]),
        )

    @classmethod
    def from_audit(
        cls,
        audit: AuditEnvironmentManifest,
        *,
        mode: CacheCompatibilityMode,
        implementation_digest: str,
        numerical_backend_names: tuple[str, ...] = (),
        cache_environment_names: tuple[str, ...] = (),
    ) -> "CacheCompatibilityProfile":
        environment = {
            name: audit.environment_input_digests[name]
            for name in sorted(set(cache_environment_names))
            if name in audit.environment_input_digests
        }
        if len(environment) != len(cache_environment_names):
            raise RuntimeIntegrityError("缓存环境变量未进入审计白名单或名称重复")
        common_runtime = {
            "python_implementation": audit.python_implementation,
            "python_major_minor": ".".join(str(value) for value in audit.python_version[:2]),
            "platform_system": audit.platform_system,
            "platform_machine": audit.platform_machine,
            "byteorder": audit.byteorder,
        }
        if mode in {CacheCompatibilityMode.BYTE_EXACT, CacheCompatibilityMode.SEMANTIC}:
            return cls(
                mode,
                implementation_digest,
                {
                    **common_runtime,
                    "python_full": ".".join(str(value) for value in audit.python_version),
                    "python_cache_tag": audit.python_cache_tag,
                    "platform_release": audit.platform_release,
                },
                audit.dependency_distribution_digests,
                {},
                environment,
                audit.build_artifact_digest if mode is CacheCompatibilityMode.BYTE_EXACT else None,
            )
        names = tuple(sorted(set(numerical_backend_names)))
        backends = {
            name: audit.dependency_distribution_digests[name]
            for name in names
            if name in audit.dependency_distribution_digests
        }
        if len(backends) != len(numerical_backend_names):
            raise RuntimeIntegrityError("声明的数值后端未进入审计依赖或名称重复")
        return cls(mode, implementation_digest, common_runtime, {}, backends, environment)


@dataclass(frozen=True)
class ExecutionIdentity:
    """完整执行审计身份；`node_execution_id` 只使用兼容层，不使用审计附加信息。"""

    audit_manifest_digest: str
    cache_profile_digest: str
    operator_definition_digest: str
    input_digests: Mapping[str, str]
    contract_version: str = EXECUTION_IDENTITY_VERSION
    audit_implementation_digest: str | None = None

    def __post_init__(self) -> None:
        _require_digest(self.audit_manifest_digest, "audit_manifest_digest")
        _require_digest(self.cache_profile_digest, "cache_profile_digest")
        _require_digest(self.operator_definition_digest, "operator_definition_digest")
        if self.audit_implementation_digest is not None:
            _require_digest(self.audit_implementation_digest, "audit_implementation_digest")
        object.__setattr__(self, "input_digests", _digest_mapping(self.input_digests, "input_digests", allow_empty=False))
        if self.contract_version != EXECUTION_IDENTITY_VERSION:
            raise RuntimeIntegrityError("ExecutionIdentity 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "audit_manifest_digest": self.audit_manifest_digest,
            "cache_profile_digest": self.cache_profile_digest,
            "operator_definition_digest": self.operator_definition_digest,
            "input_digests": dict(self.input_digests),
            "contract_version": self.contract_version,
            **({"audit_implementation_digest": self.audit_implementation_digest}
               if self.audit_implementation_digest is not None else {}),
        }

    @cached_property
    def identity_digest(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @property
    def node_execution_id(self) -> str:
        return typed_canonical_hash({
            "cache_profile_digest": self.cache_profile_digest,
            "operator_definition_digest": self.operator_definition_digest,
            "input_digests": dict(self.input_digests),
            "contract_version": EXECUTION_IDENTITY_VERSION,
        })

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExecutionIdentity":
        expected = {
            "audit_manifest_digest", "cache_profile_digest", "operator_definition_digest",
            "input_digests", "contract_version",
        }
        audit_implementation = payload.get("audit_implementation_digest")
        if isinstance(audit_implementation, str):
            expected.add("audit_implementation_digest")
        input_digests = payload.get("input_digests")
        if set(payload) != expected or not isinstance(input_digests, Mapping):
            raise RuntimeIntegrityError("ExecutionIdentity schema 无效")
        string_fields = (
            "audit_manifest_digest", "cache_profile_digest", "operator_definition_digest",
            "contract_version",
        )
        if any(not isinstance(payload[field], str) for field in string_fields):
            raise RuntimeIntegrityError("ExecutionIdentity 字段类型无效")
        return cls(
            str(payload["audit_manifest_digest"]),
            str(payload["cache_profile_digest"]),
            str(payload["operator_definition_digest"]),
            _payload_string_mapping(input_digests, "input_digests"),
            str(payload["contract_version"]),
            audit_implementation,
        )

    @classmethod
    def build(
        cls,
        *,
        node: NodeSpec,
        inputs: tuple[ArtifactRef, ...],
        context: DeterminismContext,
        policy_payload: Mapping[str, object],
        audit_manifest: AuditEnvironmentManifest,
        cache_profile: CacheCompatibilityProfile,
        operator_definition_digest: str,
        partition_key: str | None = None,
        audit_implementation_digest: str | None = None,
    ) -> "ExecutionIdentity":
        digests = {
            f"artifact.{index:04d}.{item.name}": typed_canonical_hash(item.to_dict())
            for index, item in enumerate(sorted(inputs, key=lambda value: value.name))
        }
        digests.update({
            "node": typed_canonical_hash(node.to_dict()),
            "determinism": typed_canonical_hash(context.to_dict()),
            "policy": typed_canonical_hash(dict(policy_payload)),
            "partition": typed_canonical_hash({"partition_key": partition_key}),
        })
        return cls(
            audit_manifest.manifest_digest,
            cache_profile.profile_digest,
            operator_definition_digest,
            digests,
            audit_implementation_digest=audit_implementation_digest,
        )


def derive_run_id(
    dag: DagSpec,
    handoff_id: str,
    context: DeterminismContext,
    mode: str,
    *,
    audit_manifest_digest: str,
    parent_run_id: str | None = None,
    project_id: str | None = None,
) -> str:
    _require_digest(audit_manifest_digest, "audit_manifest_digest")
    payload = {
        "version": IDENTITY_VERSION,
        "project_id": project_id,
        "dag_id": dag.dag_id,
        "handoff_id": handoff_id,
        "determinism": context.to_dict(),
        "mode": mode,
        "audit_manifest_digest": audit_manifest_digest,
        "parent_run_id": parent_run_id,
    }
    return typed_canonical_hash(payload)


def derive_node_execution_id(identity: ExecutionIdentity) -> str:
    return identity.node_execution_id


def derive_partition_seed(root_seed: int, node_id: str, partition_key: str) -> int:
    digest = typed_canonical_hash({
        "version": IDENTITY_VERSION,
        "root_seed": root_seed,
        "node_id": node_id,
        "partition_key": partition_key,
    })
    return int(digest[:16], 16)


def environment_fingerprint() -> str:
    """项目算子父子进程用于核对实际环境的摘要。"""
    build_digest = hashlib.sha256(b"research-pipeline-source-runtime").hexdigest()
    return AuditEnvironmentManifest.capture(build_artifact_digest=build_digest).manifest_digest


__all__ = [
    "AUDIT_ENVIRONMENT_VERSION",
    "CACHE_COMPATIBILITY_VERSION",
    "EXECUTION_IDENTITY_VERSION",
    "IDENTITY_VERSION",
    "AuditEnvironmentManifest",
    "CacheCompatibilityMode",
    "CacheCompatibilityProfile",
    "ExecutionIdentity",
    "derive_node_execution_id",
    "derive_partition_seed",
    "derive_run_id",
    "environment_fingerprint",
]
