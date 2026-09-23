"""运行时 checkpoint v3：绑定节点、实现、输入、完整端口输出和缓存画像。"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import ArtifactRef, _require_fields
from .errors import RuntimeIntegrityError


CHECKPOINT_MANIFEST_VERSION = "research-runtime-checkpoint-v3"


def _require_digest(value: str, field: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RuntimeIntegrityError(f"checkpoint {field} 必须是 sha256 小写摘要")


@dataclass(frozen=True)
class CheckpointExpectation:
    node_execution_id: str
    inputs: tuple[ArtifactRef, ...]
    implementation_id: str
    implementation_digest: str
    operator_definition_digest: str
    cache_profile_digest: str

    def __post_init__(self) -> None:
        if not self.implementation_id:
            raise RuntimeIntegrityError("checkpoint implementation_id 不能为空")
        for field, value in (
            ("node_execution_id", self.node_execution_id),
            ("implementation_digest", self.implementation_digest),
            ("operator_definition_digest", self.operator_definition_digest),
            ("cache_profile_digest", self.cache_profile_digest),
        ):
            _require_digest(value, field)


@dataclass(frozen=True)
class CheckpointManifest:
    node_execution_id: str
    attempt_id: str
    inputs: tuple[ArtifactRef, ...]
    outputs: Mapping[str, ArtifactRef]
    implementation_id: str
    implementation_digest: str
    operator_definition_digest: str
    cache_profile_digest: str
    audit_environment_digest: str
    execution_identity_digest: str
    root_seed: int
    fixed_clock: str
    partition_key: str | None
    content_size: int
    content_hash: str
    content_path: str = "content.bin"
    contract_version: str = CHECKPOINT_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if not self.attempt_id or not self.implementation_id or not self.fixed_clock:
            raise RuntimeIntegrityError("checkpoint manifest 必填身份字段不完整")
        if type(self.root_seed) is not int or self.root_seed < 0 or type(self.content_size) is not int or self.content_size < 0:
            raise RuntimeIntegrityError("checkpoint seed 或 content_size 无效")
        if self.content_path != "content.bin":
            raise RuntimeIntegrityError("checkpoint content_path 不受支持")
        if self.contract_version != CHECKPOINT_MANIFEST_VERSION:
            raise RuntimeIntegrityError("checkpoint manifest 版本不受支持")
        normalized_outputs = dict(sorted(self.outputs.items()))
        if not normalized_outputs or any(
            port != artifact.name for port, artifact in normalized_outputs.items()
        ):
            raise RuntimeIntegrityError("checkpoint outputs 端口索引无效")
        object.__setattr__(self, "outputs", MappingProxyType(normalized_outputs))
        for field, value in (
            ("node_execution_id", self.node_execution_id),
            ("implementation_digest", self.implementation_digest),
            ("operator_definition_digest", self.operator_definition_digest),
            ("cache_profile_digest", self.cache_profile_digest),
            ("audit_environment_digest", self.audit_environment_digest),
            ("execution_identity_digest", self.execution_identity_digest),
            ("content_hash", self.content_hash),
        ):
            _require_digest(value, field)

    @property
    def output(self) -> ArtifactRef:
        """单输出调用点的类型化便捷访问；多输出必须按端口读取 outputs。"""

        if len(self.outputs) != 1:
            raise RuntimeIntegrityError("checkpoint 含多输出，必须按端口读取")
        return next(iter(self.outputs.values()))

    def identity_payload(self) -> dict[str, object]:
        return {
            "node_execution_id": self.node_execution_id,
            "attempt_id": self.attempt_id,
            "inputs": [item.to_dict() for item in self.inputs],
            "outputs": {
                port: artifact.to_dict() for port, artifact in self.outputs.items()
            },
            "implementation_id": self.implementation_id,
            "implementation_digest": self.implementation_digest,
            "operator_definition_digest": self.operator_definition_digest,
            "cache_profile_digest": self.cache_profile_digest,
            "audit_environment_digest": self.audit_environment_digest,
            "execution_identity_digest": self.execution_identity_digest,
            "root_seed": self.root_seed,
            "fixed_clock": self.fixed_clock,
            "partition_key": self.partition_key,
            "content_size": self.content_size,
            "content_hash": self.content_hash,
            "content_path": self.content_path,
            "contract_version": self.contract_version,
        }

    @property
    def manifest_hash(self) -> str:
        return typed_canonical_hash(self.identity_payload())

    def to_dict(self) -> dict[str, object]:
        return {**self.identity_payload(), "manifest_hash": self.manifest_hash}

    def require_expectation(self, expected: CheckpointExpectation) -> None:
        actual = (
            self.node_execution_id,
            self.inputs,
            self.implementation_id,
            self.implementation_digest,
            self.operator_definition_digest,
            self.cache_profile_digest,
        )
        wanted = (
            expected.node_execution_id,
            expected.inputs,
            expected.implementation_id,
            expected.implementation_digest,
            expected.operator_definition_digest,
            expected.cache_profile_digest,
        )
        if actual != wanted:
            raise RuntimeIntegrityError("checkpoint 节点、实现、输入或缓存画像与当前预期不一致")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CheckpointManifest":
        fields = {
            "node_execution_id",
            "attempt_id",
            "inputs",
            "outputs",
            "implementation_id",
            "implementation_digest",
            "operator_definition_digest",
            "cache_profile_digest",
            "audit_environment_digest",
            "execution_identity_digest",
            "root_seed",
            "fixed_clock",
            "partition_key",
            "content_size",
            "content_hash",
            "content_path",
            "contract_version",
            "manifest_hash",
        }
        contract_version = payload.get("contract_version")
        if contract_version is not None and contract_version != CHECKPOINT_MANIFEST_VERSION:
            raise RuntimeIntegrityError("旧 checkpoint 仅可离线审阅，不能 resume")
        _require_fields(payload, fields, "CheckpointManifest")
        if not isinstance(payload["outputs"], Mapping) or not payload["outputs"]:
            raise RuntimeIntegrityError("checkpoint outputs 端口索引无效")
        manifest = cls(
            str(payload["node_execution_id"]),
            str(payload["attempt_id"]),
            tuple(ArtifactRef.from_dict(item) for item in payload["inputs"]),
            {
                str(port): ArtifactRef.from_dict(artifact)
                for port, artifact in payload["outputs"].items()
            },
            str(payload["implementation_id"]),
            str(payload["implementation_digest"]),
            str(payload["operator_definition_digest"]),
            str(payload["cache_profile_digest"]),
            str(payload["audit_environment_digest"]),
            str(payload["execution_identity_digest"]),
            int(payload["root_seed"]),
            str(payload["fixed_clock"]),
            None if payload["partition_key"] is None else str(payload["partition_key"]),
            int(payload["content_size"]),
            str(payload["content_hash"]),
            str(payload["content_path"]),
            str(payload["contract_version"]),
        )
        if payload["manifest_hash"] != manifest.manifest_hash:
            raise RuntimeIntegrityError("checkpoint manifest hash 校验失败")
        return manifest


__all__ = ["CHECKPOINT_MANIFEST_VERSION", "CheckpointExpectation", "CheckpointManifest"]
