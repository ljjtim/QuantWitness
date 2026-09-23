"""可复现分发制品与依赖字节身份合同。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from importlib import metadata
import json
from pathlib import Path
import platform
import sys
from types import MappingProxyType
from typing import Mapping

from .canonical import typed_canonical_hash


BUILD_MANIFEST_VERSION = "research-build-manifest-v1"
DEPENDENCY_LOCK_VERSION = "research-dependency-distribution-lock-v1"


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"BuildManifest {field} 必须是 sha256 小写摘要")
    return value


def _hash_mapping(value: object, field: str) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"BuildManifest {field} 必须是非空映射")
    normalized = {
        str(key): _hash(item, f"{field}.{key}")
        for key, item in value.items()
        if isinstance(key, str) and key
    }
    if len(normalized) != len(value):
        raise ValueError(f"BuildManifest {field} 包含空或非字符串键")
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True)
class BuildManifest:
    source_commit: str
    source_dirty: bool
    source_tree_digest: str
    build_toolchain: Mapping[str, str]
    input_digests: Mapping[str, str]
    wheel_digest: str
    sdist_digest: str
    dependency_distribution_digests: Mapping[str, str]
    manifest_hash: str
    contract_version: str = BUILD_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if (
            len(self.source_commit) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.source_commit)
            or type(self.source_dirty) is not bool
        ):
            raise ValueError("BuildManifest source identity 无效")
        if self.contract_version != BUILD_MANIFEST_VERSION:
            raise ValueError("BuildManifest 版本不受支持")
        _hash(self.source_tree_digest, "source_tree_digest")
        _hash(self.wheel_digest, "wheel_digest")
        _hash(self.sdist_digest, "sdist_digest")
        if not isinstance(self.build_toolchain, Mapping) or not self.build_toolchain or any(
            not isinstance(key, str) or not key or not isinstance(value, str) or not value
            for key, value in self.build_toolchain.items()
        ):
            raise ValueError("BuildManifest build_toolchain 无效")
        object.__setattr__(
            self,
            "build_toolchain",
            MappingProxyType(dict(sorted(self.build_toolchain.items()))),
        )
        object.__setattr__(self, "input_digests", _hash_mapping(self.input_digests, "input_digests"))
        object.__setattr__(
            self,
            "dependency_distribution_digests",
            _hash_mapping(
                self.dependency_distribution_digests,
                "dependency_distribution_digests",
            ),
        )
        if self.manifest_hash != typed_canonical_hash(self.payload()):
            raise ValueError("BuildManifest hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "source_commit": self.source_commit,
            "source_dirty": self.source_dirty,
            "source_tree_digest": self.source_tree_digest,
            "build_toolchain": dict(self.build_toolchain),
            "input_digests": dict(self.input_digests),
            "wheel_digest": self.wheel_digest,
            "sdist_digest": self.sdist_digest,
            "dependency_distribution_digests": dict(self.dependency_distribution_digests),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "manifest_hash": self.manifest_hash}

    @classmethod
    def build(
        cls,
        *,
        source_commit: str,
        source_dirty: bool,
        source_tree_digest: str,
        build_toolchain: Mapping[str, str],
        input_digests: Mapping[str, str],
        wheel_digest: str,
        sdist_digest: str,
        dependency_distribution_digests: Mapping[str, str],
    ) -> "BuildManifest":
        values = {
            "source_commit": source_commit,
            "source_dirty": source_dirty,
            "source_tree_digest": source_tree_digest,
            "build_toolchain": dict(sorted(build_toolchain.items())),
            "input_digests": dict(sorted(input_digests.items())),
            "wheel_digest": wheel_digest,
            "sdist_digest": sdist_digest,
            "dependency_distribution_digests": dict(
                sorted(dependency_distribution_digests.items())
            ),
            "contract_version": BUILD_MANIFEST_VERSION,
        }
        return cls(
            source_commit,
            source_dirty,
            source_tree_digest,
            values["build_toolchain"],
            values["input_digests"],
            wheel_digest,
            sdist_digest,
            values["dependency_distribution_digests"],
            typed_canonical_hash(values),
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "BuildManifest":
        expected = {
            "source_commit",
            "source_dirty",
            "source_tree_digest",
            "build_toolchain",
            "input_digests",
            "wheel_digest",
            "sdist_digest",
            "dependency_distribution_digests",
            "manifest_hash",
            "contract_version",
        }
        if set(payload) != expected:
            raise ValueError("BuildManifest schema 无效")
        source_dirty = payload["source_dirty"]
        if type(source_dirty) is not bool:
            raise ValueError("BuildManifest source_dirty 必须是布尔值")
        return cls(
            str(payload["source_commit"]),
            source_dirty,
            str(payload["source_tree_digest"]),
            _string_mapping(payload["build_toolchain"], "build_toolchain"),
            _hash_mapping(payload["input_digests"], "input_digests"),
            str(payload["wheel_digest"]),
            str(payload["sdist_digest"]),
            _hash_mapping(
                payload["dependency_distribution_digests"],
                "dependency_distribution_digests",
            ),
            str(payload["manifest_hash"]),
            str(payload["contract_version"]),
        )


def _string_mapping(value: object, field: str) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"BuildManifest {field} 必须是非空映射")
    result = {str(key): str(item) for key, item in value.items() if key and item}
    if len(result) != len(value):
        raise ValueError(f"BuildManifest {field} 包含空字段")
    return result


def load_build_manifest(path: str | Path) -> BuildManifest:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("BuildManifest 无法读取") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("BuildManifest 必须是对象")
    return BuildManifest.from_dict(payload)


def installed_distribution_digest(distribution_name: str) -> tuple[str, str, str]:
    """返回规范名称、版本及已安装 distribution 的 METADATA/RECORD 摘要。"""
    try:
        distribution = metadata.distribution(distribution_name)
    except metadata.PackageNotFoundError as exc:
        raise ValueError(f"依赖 distribution 未安装: {distribution_name}") from exc
    metadata_text = distribution.read_text("METADATA") or ""
    record_text = distribution.read_text("RECORD") or ""
    if not metadata_text or not record_text:
        raise ValueError(f"依赖 distribution 缺少 METADATA/RECORD: {distribution_name}")
    digest = hashlib.sha256(
        f"{metadata_text}\n--RECORD--\n{record_text}".encode("utf-8")
    ).hexdigest()
    return distribution.metadata["Name"], distribution.version, digest


def verify_dependency_distribution_lock(path: str | Path) -> Mapping[str, str]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("依赖 distribution lock 无法读取") from exc
    expected = {"contract_version", "platform", "python_cache_tag", "distributions"}
    if not isinstance(payload, Mapping) or set(payload) != expected:
        raise ValueError("依赖 distribution lock schema 无效")
    if payload["contract_version"] != DEPENDENCY_LOCK_VERSION:
        raise ValueError("依赖 distribution lock 版本不受支持")
    if payload["platform"] != platform.system().lower() or payload["python_cache_tag"] != sys.implementation.cache_tag:
        raise ValueError("依赖 distribution lock 与当前平台/Python ABI 不一致")
    distributions = payload["distributions"]
    if not isinstance(distributions, list) or not distributions:
        raise ValueError("依赖 distribution lock 为空")
    verified = {}
    seen = set()
    for item in distributions:
        if not isinstance(item, Mapping) or set(item) != {"name", "version", "distribution_digest"}:
            raise ValueError("依赖 distribution lock 条目 schema 无效")
        locked_name = str(item["name"])
        normalized = locked_name.lower().replace("_", "-")
        if normalized in seen:
            raise ValueError(f"依赖 distribution lock 名称重复: {locked_name}")
        seen.add(normalized)
        actual_name, actual_version, actual_digest = installed_distribution_digest(locked_name)
        if actual_name.lower().replace("_", "-") != normalized or actual_version != item["version"]:
            raise ValueError(f"依赖 distribution 版本漂移: {locked_name}")
        if actual_digest != item["distribution_digest"]:
            raise ValueError(f"依赖 distribution 字节漂移: {locked_name}")
        verified[f"{actual_name}=={actual_version}"] = actual_digest
    return MappingProxyType(dict(sorted(verified.items())))


__all__ = [
    "BUILD_MANIFEST_VERSION",
    "DEPENDENCY_LOCK_VERSION",
    "BuildManifest",
    "installed_distribution_digest",
    "load_build_manifest",
    "verify_dependency_distribution_lock",
]
