"""Typed DAG 使用的不可变运行时合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from typing import Any

from .errors import RuntimeContractError


RUNTIME_CONTRACT_VERSION = "research-runtime-contract-v1"
_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")


def _require_fields(payload: dict[str, Any], expected: set[str], label: str) -> None:
    unknown = set(payload) - expected
    missing = expected - set(payload)
    if unknown:
        raise RuntimeContractError(f"{label} 含未知字段: {sorted(unknown)}")
    if missing:
        raise RuntimeContractError(f"{label} 缺少字段: {sorted(missing)}")


def _require_id(value: str, label: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERN.fullmatch(value) or ":" in value:
        raise RuntimeContractError(f"{label} 不是安全稳定 ID")
    return value


def _strict_int(value: object, label: str) -> int:
    if type(value) is not int:
        raise RuntimeContractError(f"{label} 必须是整数")
    return value


def _strict_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise RuntimeContractError(f"{label} 必须是布尔值")
    return value


class CheckpointPolicy(str, Enum):
    REQUIRED = "required"
    DISABLED = "disabled"


@dataclass(frozen=True)
class ResourceBudget:
    memory_bytes: int
    cpu_slots: int
    temp_bytes: int
    wall_seconds: int

    def __post_init__(self) -> None:
        for name, value in self.to_dict().items():
            minimum = 0 if name == "temp_bytes" else 1
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                requirement = "非负整数" if name == "temp_bytes" else "正整数"
                raise RuntimeContractError(f"资源预算 {name} 必须是{requirement}")

    def to_dict(self) -> dict[str, int]:
        return {
            "memory_bytes": self.memory_bytes,
            "cpu_slots": self.cpu_slots,
            "temp_bytes": self.temp_bytes,
            "wall_seconds": self.wall_seconds,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ResourceBudget:
        _require_fields(payload, {"memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds"}, "ResourceBudget")
        return cls(*(_strict_int(payload[key], key) for key in ("memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds")))


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    retryable_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise RuntimeContractError("max_attempts 必须至少为 1")
        if len(set(self.retryable_codes)) != len(self.retryable_codes):
            raise RuntimeContractError("retryable_codes 不得重复")
        for code in self.retryable_codes:
            _require_id(code, "retryable code")

    def to_dict(self) -> dict[str, object]:
        return {"max_attempts": self.max_attempts, "retryable_codes": list(self.retryable_codes)}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RetryPolicy:
        _require_fields(payload, {"max_attempts", "retryable_codes"}, "RetryPolicy")
        return cls(_strict_int(payload["max_attempts"], "max_attempts"), tuple(str(item) for item in payload["retryable_codes"]))


@dataclass(frozen=True)
class PartitionSpec:
    enabled: bool
    keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.enabled and self.keys:
            raise RuntimeContractError("未启用分区时不能声明 partition keys")
        if len(set(self.keys)) != len(self.keys):
            raise RuntimeContractError("partition keys 不得重复")

    def to_dict(self) -> dict[str, object]:
        return {"enabled": self.enabled, "keys": list(self.keys)}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> PartitionSpec:
        _require_fields(payload, {"enabled", "keys"}, "PartitionSpec")
        return cls(_strict_bool(payload["enabled"], "partition.enabled"), tuple(str(item) for item in payload["keys"]))


@dataclass(frozen=True)
class DeterminismContext:
    root_seed: int
    fixed_clock: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.root_seed, int) or isinstance(self.root_seed, bool) or self.root_seed < 0:
            raise RuntimeContractError("root_seed 必须是非负整数")
        if self.fixed_clock.tzinfo is None or self.fixed_clock.utcoffset() is None:
            raise RuntimeContractError("fixed_clock 必须显式带时区")

    def to_dict(self) -> dict[str, object]:
        return {"root_seed": self.root_seed, "fixed_clock": self.fixed_clock.isoformat()}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> DeterminismContext:
        _require_fields(payload, {"root_seed", "fixed_clock"}, "DeterminismContext")
        return cls(_strict_int(payload["root_seed"], "root_seed"), datetime.fromisoformat(str(payload["fixed_clock"])))


@dataclass(frozen=True)
class ArtifactRef:
    name: str
    artifact_type: str
    artifact_key: str
    content_hash: str

    def __post_init__(self) -> None:
        _require_id(self.name, "artifact name")
        _require_id(self.artifact_type, "artifact type")
        if not self.artifact_key or not self.content_hash:
            raise RuntimeContractError("artifact key/content hash 不能为空")

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "artifact_type": self.artifact_type, "artifact_key": self.artifact_key, "content_hash": self.content_hash}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ArtifactRef:
        _require_fields(payload, {"name", "artifact_type", "artifact_key", "content_hash"}, "ArtifactRef")
        return cls(*(str(payload[key]) for key in ("name", "artifact_type", "artifact_key", "content_hash")))


@dataclass(frozen=True)
class NodeSpec:
    node_id: str
    implementation_id: str
    input_types: tuple[tuple[str, str], ...]
    output_types: tuple[tuple[str, str], ...]
    resource_budget: ResourceBudget
    retry_policy: RetryPolicy
    checkpoint_policy: CheckpointPolicy
    partition: PartitionSpec
    cacheable: bool = True
    pure: bool = True
    contract_version: str = RUNTIME_CONTRACT_VERSION
    configuration_hash: str | None = None

    def __post_init__(self) -> None:
        _require_id(self.node_id, "node_id")
        _require_id(self.implementation_id, "implementation_id")
        for label, ports in (("input", self.input_types), ("output", self.output_types)):
            names = [name for name, _ in ports]
            if len(names) != len(set(names)):
                raise RuntimeContractError(f"{label} port 不得重复")
            for name, artifact_type in ports:
                _require_id(name, f"{label} port")
                _require_id(artifact_type, "artifact type")
        if self.contract_version != RUNTIME_CONTRACT_VERSION:
            raise RuntimeContractError("runtime contract version 不受支持")
        if self.configuration_hash is not None and (
            len(self.configuration_hash) != 64
            or any(char not in "0123456789abcdef" for char in self.configuration_hash)
        ):
            raise RuntimeContractError("configuration_hash 必须是 sha256")

    def to_dict(self) -> dict[str, object]:
        payload = {
            "node_id": self.node_id,
            "implementation_id": self.implementation_id,
            "input_types": [{"port": p, "artifact_type": t} for p, t in self.input_types],
            "output_types": [{"port": p, "artifact_type": t} for p, t in self.output_types],
            "resource_budget": self.resource_budget.to_dict(),
            "retry_policy": self.retry_policy.to_dict(),
            "checkpoint_policy": self.checkpoint_policy.value,
            "partition": self.partition.to_dict(),
            "cacheable": self.cacheable,
            "pure": self.pure,
            "contract_version": self.contract_version,
        }
        if self.configuration_hash is not None:
            payload["configuration_hash"] = self.configuration_hash
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> NodeSpec:
        expected = {"node_id", "implementation_id", "input_types", "output_types", "resource_budget", "retry_policy", "checkpoint_policy", "partition", "cacheable", "pure", "contract_version"}
        configuration_hash = payload.get("configuration_hash")
        if configuration_hash is not None:
            expected.add("configuration_hash")
        _require_fields(payload, expected, "NodeSpec")
        def ports(key: str) -> tuple[tuple[str, str], ...]:
            result = []
            for item in payload[key]:
                _require_fields(item, {"port", "artifact_type"}, key)
                result.append((str(item["port"]), str(item["artifact_type"])))
            return tuple(result)
        return cls(
            str(payload["node_id"]), str(payload["implementation_id"]), ports("input_types"), ports("output_types"),
            ResourceBudget.from_dict(payload["resource_budget"]), RetryPolicy.from_dict(payload["retry_policy"]),
            CheckpointPolicy(payload["checkpoint_policy"]), PartitionSpec.from_dict(payload["partition"]),
            _strict_bool(payload["cacheable"], "cacheable"), _strict_bool(payload["pure"], "pure"), str(payload["contract_version"]),
            None if configuration_hash is None else str(configuration_hash),
        )


__all__ = [
    "ArtifactRef", "CheckpointPolicy", "DeterminismContext", "NodeSpec", "PartitionSpec",
    "RUNTIME_CONTRACT_VERSION", "ResourceBudget", "RetryPolicy",
]
