"""五类受控扩展的稳定描述合同。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Mapping

from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import ExtensionError


EXTENSION_CONTRACT_VERSION = "research-extension-contract-v1"
_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")


class ExtensionKind(str, Enum):
    PROVIDER = "provider"
    TRANSFORM = "transform"
    RESEARCH_ENGINE = "research_engine"
    MARKET_RULE = "market_rule"
    METRIC = "metric"


def _id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ExtensionError(f"{field} 必须是稳定 ID")
    return value


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ExtensionError(f"{field} 必须是 sha256 小写十六进制")
    return value


@dataclass(frozen=True)
class ResourceProfile:
    memory_bytes: int
    cpu_slots: int
    temp_bytes: int
    wall_seconds: int

    def __post_init__(self) -> None:
        if any(type(value) is not int or value <= 0 for value in self.to_dict().values()):
            raise ExtensionError("resource profile 必须全部是正整数")

    def to_dict(self) -> dict[str, int]:
        return {"memory_bytes": self.memory_bytes, "cpu_slots": self.cpu_slots, "temp_bytes": self.temp_bytes, "wall_seconds": self.wall_seconds}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResourceProfile":
        if set(payload) != {"memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds"} or any(type(payload[key]) is not int for key in payload):
            raise ExtensionError("resource profile schema 无效")
        return cls(payload["memory_bytes"], payload["cpu_slots"], payload["temp_bytes"], payload["wall_seconds"])


@dataclass(frozen=True)
class DeterminismPolicy:
    mode: str
    seed_policy: str
    comparable: bool

    def __post_init__(self) -> None:
        if self.mode not in {"deterministic", "seeded"} or self.seed_policy not in {"none", "fixed_root", "derived_partition"}:
            raise ExtensionError("determinism mode/seed policy 不受支持")
        if self.mode == "deterministic" and self.seed_policy != "none":
            raise ExtensionError("deterministic 扩展不需要 seed")
        if self.mode == "seeded" and self.seed_policy == "none":
            raise ExtensionError("不确定性扩展必须声明固定 seed policy")
        if type(self.comparable) is not bool:
            raise ExtensionError("comparable 必须是布尔值")

    def to_dict(self) -> dict[str, object]:
        return {"mode": self.mode, "seed_policy": self.seed_policy, "comparable": self.comparable}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DeterminismPolicy":
        if set(payload) != {"mode", "seed_policy", "comparable"} or type(payload["comparable"]) is not bool:
            raise ExtensionError("determinism policy schema 无效")
        return cls(str(payload["mode"]), str(payload["seed_policy"]), payload["comparable"])


@dataclass(frozen=True)
class ExtensionDescriptor:
    kind: ExtensionKind
    extension_id: str
    target_contract_version: str
    implementation_version: str
    code_hash: str
    dependencies: tuple[str, ...]
    input_types: tuple[str, ...]
    output_types: tuple[str, ...]
    resource_profile: ResourceProfile
    determinism: DeterminismPolicy
    capabilities: tuple[str, ...]
    descriptor_hash: str
    contract_version: str = EXTENSION_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _id(self.extension_id, "extension_id")
        _id(self.target_contract_version, "target_contract_version")
        _id(self.implementation_version, "implementation_version")
        _hash(self.code_hash, "code_hash")
        for field in ("dependencies", "input_types", "output_types", "capabilities"):
            values = getattr(self, field)
            if tuple(sorted(values)) != values or len(set(values)) != len(values):
                raise ExtensionError(f"{field} 必须唯一并规范排序")
            for value in values:
                _id(value, field)
        if not self.output_types or not self.capabilities:
            raise ExtensionError("extension 必须声明 output_types 和 capabilities")
        if self.contract_version != EXTENSION_CONTRACT_VERSION or self.descriptor_hash != typed_canonical_hash(self.payload()):
            raise ExtensionError("extension descriptor hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"kind": self.kind.value, "extension_id": self.extension_id, "target_contract_version": self.target_contract_version, "implementation_version": self.implementation_version, "code_hash": self.code_hash, "dependencies": list(self.dependencies), "input_types": list(self.input_types), "output_types": list(self.output_types), "resource_profile": self.resource_profile.to_dict(), "determinism": self.determinism.to_dict(), "capabilities": list(self.capabilities), "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "descriptor_hash": self.descriptor_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExtensionDescriptor":
        fields = {"kind", "extension_id", "target_contract_version", "implementation_version", "code_hash", "dependencies", "input_types", "output_types", "resource_profile", "determinism", "capabilities", "descriptor_hash", "contract_version"}
        if set(payload) != fields:
            raise ExtensionError("extension descriptor schema 无效")
        sequence_fields = ("dependencies", "input_types", "output_types", "capabilities")
        if any(not isinstance(payload[field], list) or any(not isinstance(item, str) for item in payload[field]) for field in sequence_fields):
            raise ExtensionError("extension descriptor 列表字段无效")
        resource, determinism = payload["resource_profile"], payload["determinism"]
        if not isinstance(resource, Mapping) or not isinstance(determinism, Mapping):
            raise ExtensionError("extension descriptor 子对象无效")
        try:
            kind = ExtensionKind(str(payload["kind"]))
        except ValueError as exc:
            raise ExtensionError("extension kind 不受支持") from exc
        return cls(
            kind, str(payload["extension_id"]), str(payload["target_contract_version"]), str(payload["implementation_version"]), str(payload["code_hash"]),
            tuple(payload["dependencies"]), tuple(payload["input_types"]), tuple(payload["output_types"]), ResourceProfile.from_dict(resource), DeterminismPolicy.from_dict(determinism), tuple(payload["capabilities"]), str(payload["descriptor_hash"]), str(payload["contract_version"]),
        )

    @classmethod
    def build(cls, *, kind: ExtensionKind, extension_id: str, target_contract_version: str, implementation_version: str, code_hash: str, dependencies: tuple[str, ...] = (), input_types: tuple[str, ...] = (), output_types: tuple[str, ...], resource_profile: ResourceProfile, determinism: DeterminismPolicy, capabilities: tuple[str, ...]) -> "ExtensionDescriptor":
        values = (kind, extension_id, target_contract_version, implementation_version, code_hash, tuple(sorted(dependencies)), tuple(sorted(input_types)), tuple(sorted(output_types)), resource_profile, determinism, tuple(sorted(capabilities)))
        payload = {"kind": kind.value, "extension_id": extension_id, "target_contract_version": target_contract_version, "implementation_version": implementation_version, "code_hash": code_hash, "dependencies": list(values[5]), "input_types": list(values[6]), "output_types": list(values[7]), "resource_profile": resource_profile.to_dict(), "determinism": determinism.to_dict(), "capabilities": list(values[10]), "contract_version": EXTENSION_CONTRACT_VERSION}
        return cls(*values, typed_canonical_hash(payload))


__all__ = ["EXTENSION_CONTRACT_VERSION", "DeterminismPolicy", "ExtensionDescriptor", "ExtensionKind", "ResourceProfile"]
