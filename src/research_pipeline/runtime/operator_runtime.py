"""正式算子 Runtime 的统一调用合同。"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import math
from pathlib import Path
import time
from types import MappingProxyType
from typing import Callable, Mapping

from research_pipeline.platform import canonical_json, typed_canonical_hash

from .contracts import ArtifactRef, NodeSpec, ResourceBudget
from .errors import RuntimeIntegrityError, RuntimeWorkerError
from .external_artifact import ExternalArtifactCommit, ExternalArtifactStore
from .resource_governor import ResourceGovernor, ResourceLease


RUNTIME_NODE_VALUE_VERSION = "research-runtime-node-value-v2"
RUNTIME_COMPLETION_METADATA_VERSION = "research-runtime-completion-metadata-v1"


def _digest_mapping(values: Mapping[str, str], field_name: str) -> Mapping[str, str]:
    normalized = dict(sorted(values.items()))
    if any(
        not isinstance(key, str)
        or not key
        or not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
        for key, value in normalized.items()
    ):
        raise RuntimeIntegrityError(f"Runtime completion {field_name} 无效")
    return MappingProxyType(normalized)


@dataclass(frozen=True)
class RuntimeCompletionMetadata:
    """领域 adapter 写入、通用 Runtime 合并的固定收尾元数据。"""

    artifact_hashes: Mapping[str, str] = field(default_factory=dict)
    proof_hashes: Mapping[str, str] = field(default_factory=dict)
    counts: Mapping[str, int] = field(default_factory=dict)
    backend_id: str | None = None
    fidelity: str | None = None
    limitations: tuple[str, ...] = ()
    contract_version: str = RUNTIME_COMPLETION_METADATA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_hashes",
            _digest_mapping(self.artifact_hashes, "artifact_hashes"),
        )
        object.__setattr__(
            self,
            "proof_hashes",
            _digest_mapping(self.proof_hashes, "proof_hashes"),
        )
        normalized_counts = dict(sorted(self.counts.items()))
        if any(
            not isinstance(key, str)
            or not key
            or type(value) is not int
            or value < 0
            for key, value in normalized_counts.items()
        ):
            raise RuntimeIntegrityError("Runtime completion counts 无效")
        object.__setattr__(self, "counts", MappingProxyType(normalized_counts))
        for field_name in ("backend_id", "fidelity"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise RuntimeIntegrityError(f"Runtime completion {field_name} 无效")
        normalized_limitations = tuple(sorted(set(self.limitations)))
        if any(not isinstance(value, str) or not value for value in normalized_limitations):
            raise RuntimeIntegrityError("Runtime completion limitations 无效")
        object.__setattr__(self, "limitations", normalized_limitations)
        if self.contract_version != RUNTIME_COMPLETION_METADATA_VERSION:
            raise RuntimeIntegrityError("Runtime completion metadata 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_hashes": dict(self.artifact_hashes),
            "proof_hashes": dict(self.proof_hashes),
            "counts": dict(self.counts),
            "backend_id": self.backend_id,
            "fidelity": self.fidelity,
            "limitations": list(self.limitations),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "RuntimeCompletionMetadata":
        expected = {
            "artifact_hashes",
            "proof_hashes",
            "counts",
            "backend_id",
            "fidelity",
            "limitations",
            "contract_version",
        }
        if (
            set(payload) != expected
            or not isinstance(payload["artifact_hashes"], Mapping)
            or not isinstance(payload["proof_hashes"], Mapping)
            or not isinstance(payload["counts"], Mapping)
            or not isinstance(payload["limitations"], list)
        ):
            raise RuntimeIntegrityError("Runtime completion metadata schema 无效")
        return cls(
            artifact_hashes=dict(payload["artifact_hashes"]),
            proof_hashes=dict(payload["proof_hashes"]),
            counts=dict(payload["counts"]),
            backend_id=(
                None if payload["backend_id"] is None else str(payload["backend_id"])
            ),
            fidelity=None if payload["fidelity"] is None else str(payload["fidelity"]),
            limitations=tuple(payload["limitations"]),
            contract_version=str(payload["contract_version"]),
        )

    @classmethod
    def merge(
        cls, values: Mapping[str, "RuntimeCompletionMetadata"]
    ) -> "RuntimeCompletionMetadata":
        artifact_hashes: dict[str, str] = {}
        proof_hashes: dict[str, str] = {}
        counts: dict[str, int] = {}
        backends = {item.backend_id for item in values.values() if item.backend_id is not None}
        fidelities = {item.fidelity for item in values.values() if item.fidelity is not None}
        limitations: set[str] = set()
        for node_id, metadata in sorted(values.items()):
            for target, incoming in (
                (artifact_hashes, metadata.artifact_hashes),
                (proof_hashes, metadata.proof_hashes),
                (counts, metadata.counts),
            ):
                for key, value in incoming.items():
                    target[f"{node_id}/{key}"] = value
            limitations.update(metadata.limitations)
        return cls(
            artifact_hashes=artifact_hashes,
            proof_hashes=proof_hashes,
            counts=counts,
            backend_id=next(iter(backends)) if len(backends) == 1 else None,
            fidelity=next(iter(fidelities)) if len(fidelities) == 1 else None,
            limitations=tuple(limitations),
        )


@dataclass(frozen=True)
class RuntimeNodeValue:
    """节点输出；checkpoint 只保存内联字节或已提交外部工件引用。"""

    artifact_ref: ArtifactRef
    inline_content: bytes | None = None
    external_commit: ExternalArtifactCommit | None = None

    def __post_init__(self) -> None:
        if (self.inline_content is None) == (self.external_commit is None):
            raise RuntimeIntegrityError("RuntimeNodeValue 必须恰含 inline 或 external 内容")
        if self.inline_content is not None:
            digest = hashlib.sha256(self.inline_content).hexdigest()
            if digest != self.artifact_ref.content_hash:
                raise RuntimeIntegrityError("RuntimeNodeValue inline 内容 hash 不一致")
        elif self.external_commit is not None and self.artifact_ref != self.external_commit.artifact_ref:
            raise RuntimeIntegrityError("RuntimeNodeValue external 引用不一致")

    @classmethod
    def inline(cls, *, name: str, artifact_type: str, content: bytes) -> "RuntimeNodeValue":
        digest = hashlib.sha256(content).hexdigest()
        key = typed_canonical_hash({
            "kind": "inline",
            "name": name,
            "artifact_type": artifact_type,
            "content_hash": digest,
        })
        return cls(ArtifactRef(name, artifact_type, key, digest), inline_content=content)

    @classmethod
    def external(cls, commit: ExternalArtifactCommit) -> "RuntimeNodeValue":
        return cls(commit.artifact_ref, external_commit=commit)

    def checkpoint_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "artifact_ref": self.artifact_ref.to_dict(),
            "kind": "inline" if self.inline_content is not None else "external",
        }
        if self.inline_content is not None:
            payload["content_base64"] = base64.b64encode(self.inline_content).decode("ascii")
        return payload


@dataclass(frozen=True)
class RuntimeNodeOutputs:
    """节点的完整端口输出；Runtime、checkpoint 与 Result 共用同一索引。"""

    values: Mapping[str, RuntimeNodeValue]
    completion_metadata: RuntimeCompletionMetadata | None = None

    def __post_init__(self) -> None:
        normalized = dict(sorted(self.values.items()))
        if not normalized:
            raise RuntimeIntegrityError("RuntimeNodeOutputs 不能为空")
        if any(port != value.artifact_ref.name for port, value in normalized.items()):
            raise RuntimeIntegrityError("RuntimeNodeOutputs 端口与 ArtifactRef 名称不一致")
        object.__setattr__(self, "values", MappingProxyType(normalized))

    @classmethod
    def single(cls, value: RuntimeNodeValue) -> "RuntimeNodeOutputs":
        return cls({value.artifact_ref.name: value})

    @property
    def artifact_refs(self) -> Mapping[str, ArtifactRef]:
        return MappingProxyType({
            port: value.artifact_ref for port, value in self.values.items()
        })

    def checkpoint_bytes(self) -> bytes:
        return canonical_json({
            "contract_version": RUNTIME_NODE_VALUE_VERSION,
            "completion_metadata": (
                None
                if self.completion_metadata is None
                else self.completion_metadata.to_dict()
            ),
            "outputs": {
                port: value.checkpoint_payload()
                for port, value in self.values.items()
            },
        }).encode("utf-8")


@dataclass(frozen=True)
class RuntimeNodeContext:
    node: NodeSpec
    inputs: Mapping[str, RuntimeNodeValue]
    work_dir: Path
    external_store: ExternalArtifactStore
    root_seed: int
    fixed_clock: str
    effective_resource_budget: ResourceBudget
    resource_governor: ResourceGovernor | None
    resource_lease: ResourceLease | None
    attempt_deadline_monotonic: float | None = None
    process_slots: int | None = None

    def __post_init__(self) -> None:
        if self.process_slots is not None and (
            type(self.process_slots) is not int or self.process_slots <= 0
        ):
            raise RuntimeIntegrityError("节点进程槽声明无效")

    def remaining_resource_budget(self) -> ResourceBudget:
        """返回当前节点 attempt 剩余的统一资源上限。"""

        if self.attempt_deadline_monotonic is None:
            return self.effective_resource_budget
        remaining = self.attempt_deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise RuntimeWorkerError(
                "节点执行超过 wall_seconds",
                error_code="heartbeat_timeout",
            )
        budget = self.effective_resource_budget
        return ResourceBudget(
            budget.memory_bytes,
            budget.cpu_slots,
            budget.temp_bytes,
            max(1, math.ceil(remaining)),
        )


@dataclass(frozen=True)
class OperatorRuntimeContext:
    """定义绑定 adapter 的唯一输入；共享环境由正式 CLI 一次性构造。"""

    node_context: RuntimeNodeContext
    environment: object

    @property
    def node(self) -> NodeSpec:
        return self.node_context.node

    @property
    def inputs(self) -> Mapping[str, RuntimeNodeValue]:
        return self.node_context.inputs

    @property
    def work_dir(self) -> Path:
        return self.node_context.work_dir

    @property
    def external_store(self) -> ExternalArtifactStore:
        return self.node_context.external_store

    @property
    def root_seed(self) -> int:
        return self.node_context.root_seed

    @property
    def fixed_clock(self) -> str:
        return self.node_context.fixed_clock

    @property
    def effective_resource_budget(self) -> ResourceBudget:
        """本次运行实际允许节点使用的上限，不改变计划内声明预算。"""

        return self.node_context.effective_resource_budget

    @property
    def resource_governor(self) -> ResourceGovernor | None:
        return self.node_context.resource_governor

    @property
    def resource_lease(self) -> ResourceLease | None:
        return self.node_context.resource_lease


RuntimeOperatorAdapter = Callable[[OperatorRuntimeContext], RuntimeNodeOutputs]


__all__ = [
    "OperatorRuntimeContext",
    "RUNTIME_COMPLETION_METADATA_VERSION",
    "RUNTIME_NODE_VALUE_VERSION",
    "RuntimeCompletionMetadata",
    "RuntimeNodeContext",
    "RuntimeNodeOutputs",
    "RuntimeNodeValue",
    "RuntimeOperatorAdapter",
]
