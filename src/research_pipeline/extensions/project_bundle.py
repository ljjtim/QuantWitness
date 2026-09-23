"""受控项目算子 bundle 的编译与离线复验合同。"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Iterable, Mapping

import yaml

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.operator_contracts import (
    OPERATOR_CONTRACT_VERSION,
    OperatorContractError,
    OperatorSpec,
    ParameterSpec,
    PortSpec,
    StrategyRole,
)

from .errors import ExtensionError
from .governance import (
    validate_project_dependency_lock,
    validate_project_operator_artifacts,
)


PROJECT_OPERATOR_BUNDLE_VERSION = "project-operator-extension-bundle-v2"
PROJECT_OPERATOR_ABI_VERSION = "project-operator-abi-v2"
PROJECT_OPERATOR_DECLARATION_VERSION = "project-operator-declaration-v2"
PROJECT_OPERATOR_REQUIRES_PYTHON = ">=3.10"
_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_MODULE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
_FUNCTION_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_PAYLOAD_KEYS = frozenset({
    "callable", "code", "database", "database_connection", "db", "expression", "module",
    "module_path", "path", "python", "runner", "script", "shell", "sql", "url",
})
def _canonical_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _repair_payload(
    code: str,
    *,
    missing_requirements: Iterable[str],
) -> dict[str, object]:
    return {
        "contract_version": "project-operator-repair-v1",
        "code": code,
        "missing_requirements": sorted(set(missing_requirements)),
        "next_commands": [
            "python -m research_pipeline operator scaffold --help",
            "python -m research_pipeline operator validate --help",
        ],
    }


def _safe_id(value: str, field: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERN.fullmatch(value):
        raise ExtensionError(f"{field} 无效")
    return value


def _safe_relative_path(value: str, field: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ExtensionError(f"{field} 必须是 POSIX 安全相对路径")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "." in path.parts or path.as_posix() != value:
        raise ExtensionError(f"{field} 必须是 POSIX 安全相对路径")
    return value


def _freeze_payload(value: object, field: str = "parameters") -> object:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_payload(item, f"{field}[]") for item in value)
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str) or key.lower() in _FORBIDDEN_PAYLOAD_KEYS:
                raise ExtensionError(f"{field} 包含可执行或不安全字段: {key}")
            frozen[key] = _freeze_payload(item, f"{field}.{key}")
        return MappingProxyType(dict(sorted(frozen.items())))
    raise ExtensionError(f"{field} 只能包含 JSON 标量、列表和映射")


def _thaw_payload(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_payload(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_payload(item) for item in value]
    return value


def _assert_regular_file(path: Path, *, root: Path) -> None:
    if not path.is_file():
        raise ExtensionError(f"项目源码包含非普通文件: {path}")
    try:
        path.resolve(strict=True).relative_to(root)
    except ValueError as exc:
        raise ExtensionError(f"项目源码逃逸受信根: {path}") from exc


def _source_entries(source_root: str | Path) -> tuple[Path, tuple[dict[str, object], ...]]:
    root = Path(source_root).resolve(strict=True)
    if not root.is_dir():
        raise ExtensionError("项目源码根必须是已存在目录")
    entries: list[dict[str, object]] = []
    for path in _source_paths(root):
        if path.is_dir():
            continue
        _assert_regular_file(path, root=root)
        relative = path.relative_to(root).as_posix()
        if PurePosixPath(relative).is_absolute() or ".." in PurePosixPath(relative).parts:
            raise ExtensionError(f"项目源码相对路径无效: {relative}")
        if path.suffix != ".py" or "__pycache__" in path.parts:
            raise ExtensionError(f"项目 bundle 首版只允许 Python 源码: {relative}")
        data = path.read_bytes()
        try:
            data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ExtensionError(f"项目源码必须是 UTF-8: {relative}") from exc
        entries.append({"path": relative, "size": len(data), "sha256": _sha256(data)})
    if not entries:
        raise ExtensionError("项目源码根没有 Python 文件")
    return root, tuple(entries)


def _source_paths(root: Path) -> tuple[Path, ...]:
    paths = []
    for directory, directories, names in os.walk(root, followlinks=True):
        current = Path(directory)
        ancestors = {current.resolve()}
        ancestors.update(parent.resolve() for parent in current.parents
                         if parent == root or root in parent.parents)
        for name in directories:
            target = (current / name).resolve(strict=True)
            if not target.is_relative_to(root) or target in ancestors:
                raise ExtensionError("项目源码目录越界或循环")
        paths.extend(current / name for name in (*directories, *names))
    return tuple(sorted(paths, key=lambda path: path.as_posix()))


def project_source_hash(source_root: str | Path) -> str:
    """计算规范化项目源码闭包身份。"""
    _, entries = _source_entries(source_root)
    return typed_canonical_hash({"source_files": list(entries)})


@dataclass(frozen=True)
class ProjectOperatorPermissionProfile:
    artifact_write_scope: str = "output_only"

    def __post_init__(self) -> None:
        if self.artifact_write_scope != "output_only":
            raise ExtensionError("项目算子只能写 output root")

    def to_dict(self) -> dict[str, object]:
        return {"artifact_write_scope": self.artifact_write_scope}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectOperatorPermissionProfile":
        if set(payload) != {"artifact_write_scope"}:
            raise ExtensionError("项目算子权限合同无效")
        return cls(artifact_write_scope=str(payload["artifact_write_scope"]))


@dataclass(frozen=True)
class ProjectArtifactInput:
    port: str
    artifact_type: str
    relative_path: str
    content_hash: str
    schema_hash: str

    def __post_init__(self) -> None:
        _safe_id(self.port, "artifact_input.port")
        _safe_id(self.artifact_type, "artifact_input.artifact_type")
        _safe_relative_path(self.relative_path, "artifact_input.relative_path")
        if any(not _HASH_PATTERN.fullmatch(value) for value in (self.content_hash, self.schema_hash)):
            raise ExtensionError("artifact input hash 无效")

    def to_dict(self) -> dict[str, str]:
        return {
            "port": self.port,
            "artifact_type": self.artifact_type,
            "relative_path": self.relative_path,
            "content_hash": self.content_hash,
            "schema_hash": self.schema_hash,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectArtifactInput":
        expected = {"port", "artifact_type", "relative_path", "content_hash", "schema_hash"}
        if set(payload) != expected:
            raise ExtensionError("artifact input schema 无效")
        return cls(*(str(payload[key]) for key in ("port", "artifact_type", "relative_path", "content_hash", "schema_hash")))


@dataclass(frozen=True)
class ProjectOperatorContext:
    project_id: str
    run_id: str
    node_id: str
    attempt_id: str
    fixed_clock: str
    root_seed: int
    parameters: Mapping[str, object]
    effective_resource_budget: Mapping[str, int] = field(default_factory=dict)
    abi_version: str = PROJECT_OPERATOR_ABI_VERSION

    def __post_init__(self) -> None:
        for field_name in ("project_id", "run_id", "node_id", "attempt_id"):
            _safe_id(str(getattr(self, field_name)), field_name)
        try:
            parsed = datetime.fromisoformat(self.fixed_clock)
        except ValueError as exc:
            raise ExtensionError("项目算子 fixed_clock 必须是 ISO 时间") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ExtensionError("项目算子 fixed_clock 必须带时区")
        if type(self.root_seed) is not int or self.root_seed < 0:
            raise ExtensionError("项目算子 root_seed 必须是非负整数")
        if self.abi_version != PROJECT_OPERATOR_ABI_VERSION:
            raise ExtensionError("项目算子 ABI 版本不受支持")
        frozen = _freeze_payload(self.parameters)
        if not isinstance(frozen, Mapping):
            raise ExtensionError("项目算子 parameters 必须是映射")
        object.__setattr__(self, "parameters", frozen)
        expected_budget = {
            "memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds",
        }
        budget = dict(self.effective_resource_budget)
        if budget and (
            set(budget) != expected_budget
            or any(type(value) is not int for value in budget.values())
            or any(value <= 0 for key, value in budget.items() if key != "temp_bytes")
            or budget.get("temp_bytes", 0) < 0
        ):
            raise ExtensionError("项目算子有效资源预算无效")
        object.__setattr__(
            self,
            "effective_resource_budget",
            MappingProxyType(dict(sorted(budget.items()))),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "fixed_clock": self.fixed_clock,
            "root_seed": self.root_seed,
            "parameters": _thaw_payload(self.parameters),
            "effective_resource_budget": dict(self.effective_resource_budget),
            "abi_version": self.abi_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectOperatorContext":
        expected = {
            "project_id", "run_id", "node_id", "attempt_id", "fixed_clock",
            "root_seed", "parameters", "effective_resource_budget", "abi_version",
        }
        if (
            set(payload) != expected
            or not isinstance(payload["parameters"], Mapping)
            or not isinstance(payload["effective_resource_budget"], Mapping)
        ):
            raise ExtensionError("项目算子 context schema 无效")
        return cls(
            project_id=str(payload["project_id"]),
            run_id=str(payload["run_id"]),
            node_id=str(payload["node_id"]),
            attempt_id=str(payload["attempt_id"]),
            fixed_clock=str(payload["fixed_clock"]),
            root_seed=payload["root_seed"],
            parameters=dict(payload["parameters"]),
            effective_resource_budget=dict(payload["effective_resource_budget"]),
            abi_version=str(payload["abi_version"]),
        )


@dataclass(frozen=True)
class ProjectArtifactCommit:
    port: str
    artifact_type: str
    relative_path: str
    content_hash: str
    schema_hash: str
    byte_size: int

    def __post_init__(self) -> None:
        _safe_id(self.port, "artifact_commit.port")
        _safe_id(self.artifact_type, "artifact_commit.artifact_type")
        _safe_relative_path(self.relative_path, "artifact_commit.relative_path")
        if any(not _HASH_PATTERN.fullmatch(value) for value in (self.content_hash, self.schema_hash)):
            raise ExtensionError("artifact commit hash 无效")
        if type(self.byte_size) is not int or self.byte_size < 0:
            raise ExtensionError("artifact commit byte_size 无效")

    def to_dict(self) -> dict[str, object]:
        return {
            "port": self.port,
            "artifact_type": self.artifact_type,
            "relative_path": self.relative_path,
            "content_hash": self.content_hash,
            "schema_hash": self.schema_hash,
            "byte_size": self.byte_size,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectArtifactCommit":
        expected = {"port", "artifact_type", "relative_path", "content_hash", "schema_hash", "byte_size"}
        if set(payload) != expected:
            raise ExtensionError("artifact commit schema 无效")
        return cls(
            port=str(payload["port"]),
            artifact_type=str(payload["artifact_type"]),
            relative_path=str(payload["relative_path"]),
            content_hash=str(payload["content_hash"]),
            schema_hash=str(payload["schema_hash"]),
            byte_size=payload["byte_size"],
        )


@dataclass(frozen=True)
class ProjectDirectoryCommit:
    """一个输出端口提交已声明的完整目录。"""

    port: str
    artifact_type: str
    relative_path: str
    files: tuple[Mapping[str, object], ...]

    def __post_init__(self) -> None:
        _safe_id(self.port, "directory_commit.port")
        _safe_id(self.artifact_type, "directory_commit.artifact_type")
        _safe_relative_path(self.relative_path, "directory_commit.relative_path")
        paths = []
        for item in self.files:
            if set(item) != {"relative_path", "content_hash", "byte_size"}:
                raise ExtensionError("目录提交文件描述无效")
            path = str(item["relative_path"])
            _safe_relative_path(path, "directory_commit.file")
            if (
                not _HASH_PATTERN.fullmatch(str(item["content_hash"]))
                or type(item["byte_size"]) is not int
                or item["byte_size"] < 0
            ):
                raise ExtensionError("目录提交文件身份无效")
            paths.append(path)
        if not paths or paths != sorted(set(paths)):
            raise ExtensionError("目录提交文件必须非空、唯一且排序")

    def to_dict(self) -> dict[str, object]:
        return {
            "port": self.port,
            "artifact_type": self.artifact_type,
            "relative_path": self.relative_path,
            "files": [dict(item) for item in self.files],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectDirectoryCommit":
        if (
            set(payload) != {"port", "artifact_type", "relative_path", "files"}
            or not isinstance(payload["files"], (list, tuple))
            or any(not isinstance(item, Mapping) for item in payload["files"])
        ):
            raise ExtensionError("目录提交 schema 无效")
        return cls(
            port=str(payload["port"]),
            artifact_type=str(payload["artifact_type"]),
            relative_path=str(payload["relative_path"]),
            files=tuple(dict(item) for item in payload["files"]),
        )


@dataclass(frozen=True)
class ProjectOperatorBundleManifest:
    project_id: str
    bundle_id: str
    operator_spec: OperatorSpec
    entry_module: str
    entry_function: str
    source_files: tuple[Mapping[str, object], ...]
    source_tree_hash: str
    dependency_lock: Mapping[str, str]
    dependency_lock_hash: str
    permissions: ProjectOperatorPermissionProfile
    requires_python: str
    bundle_hash: str
    contract_version: str = PROJECT_OPERATOR_BUNDLE_VERSION
    abi_version: str = PROJECT_OPERATOR_ABI_VERSION

    def __post_init__(self) -> None:
        _safe_id(self.project_id, "project_id")
        _safe_id(self.bundle_id, "bundle_id")
        if not _MODULE_PATTERN.fullmatch(self.entry_module) or not _FUNCTION_PATTERN.fullmatch(self.entry_function):
            raise ExtensionError("项目算子入口描述符无效")
        hashes = (self.source_tree_hash, self.dependency_lock_hash, self.bundle_hash)
        if any(not _HASH_PATTERN.fullmatch(value) for value in hashes):
            raise ExtensionError("项目 bundle 身份字段无效")
        if self.operator_spec.code_hash != self.source_tree_hash:
            raise ExtensionError("operator code_hash 必须绑定完整项目源码闭包")
        if self.contract_version != PROJECT_OPERATOR_BUNDLE_VERSION or self.abi_version != PROJECT_OPERATOR_ABI_VERSION:
            raise ExtensionError("项目 bundle 或 ABI 版本不受支持")
        if self.requires_python != PROJECT_OPERATOR_REQUIRES_PYTHON:
            raise ExtensionError("项目 bundle requires-python 不受支持")
        if tuple(sorted(self.source_files, key=lambda item: str(item["path"]))) != self.source_files:
            raise ExtensionError("项目 bundle 源码清单未规范排序")
        for item in self.source_files:
            if set(item) != {"path", "size", "sha256"}:
                raise ExtensionError("项目 bundle 源码条目 schema 无效")
            _safe_relative_path(str(item["path"]), "source_files[].path")
            if type(item["size"]) is not int or item["size"] < 0 or not _HASH_PATTERN.fullmatch(str(item["sha256"])):
                raise ExtensionError("项目 bundle 源码条目身份无效")
        if dict(sorted(self.dependency_lock.items())) != dict(self.dependency_lock):
            raise ExtensionError("项目依赖 lock 未规范排序")
        object.__setattr__(self, "source_files", tuple(MappingProxyType(dict(item)) for item in self.source_files))
        object.__setattr__(self, "dependency_lock", MappingProxyType(dict(self.dependency_lock)))
        if self.bundle_hash != typed_canonical_hash(self.payload()):
            raise ExtensionError("项目 bundle hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "bundle_id": self.bundle_id,
            "operator_spec": self.operator_spec.to_dict(),
            "entry": {"module": self.entry_module, "function": self.entry_function},
            "source_files": [dict(item) for item in self.source_files],
            "source_tree_hash": self.source_tree_hash,
            "dependency_lock": dict(self.dependency_lock),
            "dependency_lock_hash": self.dependency_lock_hash,
            "permissions": self.permissions.to_dict(),
            "requires_python": self.requires_python,
            "contract_version": self.contract_version,
            "abi_version": self.abi_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "bundle_hash": self.bundle_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectOperatorBundleManifest":
        expected = {
            "project_id", "bundle_id", "operator_spec", "entry", "source_files", "source_tree_hash",
            "dependency_lock", "dependency_lock_hash", "permissions", "requires_python",
            "bundle_hash", "contract_version", "abi_version",
        }
        if set(payload) != expected:
            raise ExtensionError("项目 bundle manifest schema 无效")
        if not isinstance(payload["operator_spec"], Mapping) or not isinstance(payload["entry"], Mapping):
            raise ExtensionError("项目 bundle operator/entry 合同无效")
        if set(payload["entry"]) != {"module", "function"}:
            raise ExtensionError("项目 bundle entry schema 无效")
        if not isinstance(payload["source_files"], list) or any(not isinstance(item, Mapping) for item in payload["source_files"]):
            raise ExtensionError("项目 bundle source_files 无效")
        if not isinstance(payload["dependency_lock"], Mapping) or not isinstance(payload["permissions"], Mapping):
            raise ExtensionError("项目 bundle lock/permissions 无效")
        return cls(
            project_id=str(payload["project_id"]),
            bundle_id=str(payload["bundle_id"]),
            operator_spec=OperatorSpec.from_dict(payload["operator_spec"]),
            entry_module=str(payload["entry"]["module"]),
            entry_function=str(payload["entry"]["function"]),
            source_files=tuple(dict(item) for item in payload["source_files"]),
            source_tree_hash=str(payload["source_tree_hash"]),
            dependency_lock={str(key): str(value) for key, value in payload["dependency_lock"].items()},
            dependency_lock_hash=str(payload["dependency_lock_hash"]),
            permissions=ProjectOperatorPermissionProfile.from_dict(payload["permissions"]),
            requires_python=str(payload["requires_python"]),
            bundle_hash=str(payload["bundle_hash"]),
            contract_version=str(payload["contract_version"]),
            abi_version=str(payload["abi_version"]),
        )


@dataclass(frozen=True)
class ProjectOperatorDeclaration:
    """源码旁的薄声明；代码身份始终从显式 source root 派生。"""

    project_id: str
    operator_spec: OperatorSpec
    entry_module: str
    entry_function: str
    dependency_lock: Mapping[str, str]
    permissions: ProjectOperatorPermissionProfile
    project_artifact_types: tuple[str, ...] = ()
    contract_version: str = PROJECT_OPERATOR_DECLARATION_VERSION

    def __post_init__(self) -> None:
        _safe_id(self.project_id, "project_id")
        if not _MODULE_PATTERN.fullmatch(self.entry_module) or not _FUNCTION_PATTERN.fullmatch(
            self.entry_function
        ):
            raise ExtensionError("项目算子入口描述符无效")
        if self.contract_version != PROJECT_OPERATOR_DECLARATION_VERSION:
            raise ExtensionError("项目算子薄声明版本不受支持")
        normalized_lock = dict(
            sorted((str(key), str(value)) for key, value in self.dependency_lock.items())
        )
        validate_project_dependency_lock(normalized_lock)
        object.__setattr__(self, "dependency_lock", MappingProxyType(normalized_lock))
        declared_types = tuple(sorted(set(self.project_artifact_types)))
        if any(not _ID_PATTERN.fullmatch(item) for item in declared_types):
            raise ExtensionError("项目 Artifact 类型标识无效")
        used_types = {
            item.artifact_type
            for item in (*self.operator_spec.input_ports, *self.operator_spec.output_ports)
        }
        if not set(declared_types).issubset(used_types):
            raise ExtensionError("项目 Artifact 类型声明未被当前算子使用")
        object.__setattr__(self, "project_artifact_types", declared_types)

    def to_dict(self) -> dict[str, object]:
        operator = self.operator_spec.to_dict()
        operator.pop("code_hash")
        operator.pop("spec_hash")
        return {
            "contract_version": self.contract_version,
            "project_id": self.project_id,
            "operator": operator,
            "entry": {
                "module": self.entry_module,
                "function": self.entry_function,
            },
            "dependency_lock": dict(self.dependency_lock),
            "permissions": self.permissions.to_dict(),
            "project_artifact_types": list(self.project_artifact_types),
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, object],
        *,
        source_root: str | Path,
    ) -> "ProjectOperatorDeclaration":
        expected = {
            "contract_version",
            "project_id",
            "operator",
            "entry",
            "dependency_lock",
            "permissions",
        }
        allowed = expected | {"project_artifact_types"}
        if not expected.issubset(payload) or not set(payload).issubset(allowed):
            missing = expected - set(payload)
            raise ExtensionError(
                "项目算子薄声明 schema 无效",
                failure_payload=_repair_payload(
                    "declaration_schema_invalid",
                    missing_requirements=(
                        *(f"declaration.{item}" for item in missing),
                        *(f"unknown:{item}" for item in set(payload) - allowed),
                    ),
                ),
            )
        if payload.get("contract_version") != PROJECT_OPERATOR_DECLARATION_VERSION:
            raise ExtensionError("项目算子薄声明版本不受支持")
        operator = payload.get("operator")
        entry = payload.get("entry")
        dependency_lock = payload.get("dependency_lock")
        permissions = payload.get("permissions")
        if (
            not isinstance(operator, Mapping)
            or not isinstance(entry, Mapping)
            or not isinstance(dependency_lock, Mapping)
            or not isinstance(permissions, Mapping)
        ):
            raise ExtensionError("项目算子薄声明字段类型无效")
        operator_expected = {
            "operator_id",
            "operator_version",
            "input_ports",
            "output_ports",
            "parameters",
            "strategy_roles",
            "resource_profile",
            "determinism_mode",
            "seed_policy",
            "pit_capabilities",
            "contract_version",
        }
        if set(operator) != operator_expected or operator.get("contract_version") != OPERATOR_CONTRACT_VERSION:
            raise ExtensionError(
                "项目算子薄声明 operator schema 无效",
                failure_payload=_repair_payload(
                    "operator_schema_invalid",
                    missing_requirements=(
                        *(f"operator.{item}" for item in operator_expected - set(operator)),
                        *(f"operator.unknown:{item}" for item in set(operator) - operator_expected),
                    ),
                ),
            )
        if set(entry) != {"module", "function"}:
            raise ExtensionError(
                "项目算子薄声明 entry schema 无效",
                failure_payload=_repair_payload(
                    "entry_schema_invalid",
                    missing_requirements=(
                        *(
                            f"entry.{item}"
                            for item in {"module", "function"} - set(entry)
                        ),
                        *(
                            f"entry.unknown:{item}"
                            for item in set(entry) - {"module", "function"}
                        ),
                    ),
                ),
            )
        sequence_fields = (
            "input_ports",
            "output_ports",
            "parameters",
            "strategy_roles",
            "pit_capabilities",
        )
        if any(not isinstance(operator[field], (list, tuple)) for field in sequence_fields):
            raise ExtensionError("项目算子薄声明 operator 列表字段无效")
        if any(
            not isinstance(item, Mapping)
            for field in ("input_ports", "output_ports", "parameters")
            for item in operator[field]
        ) or not isinstance(operator["resource_profile"], Mapping):
            raise ExtensionError("项目算子薄声明 port/parameter/resource schema 无效")
        try:
            specification = OperatorSpec.build(
                operator_id=str(operator["operator_id"]),
                operator_version=str(operator["operator_version"]),
                input_ports=tuple(PortSpec.from_dict(item) for item in operator["input_ports"]),
                output_ports=tuple(PortSpec.from_dict(item) for item in operator["output_ports"]),
                parameters=tuple(
                    ParameterSpec.from_dict(item) for item in operator["parameters"]
                ),
                strategy_roles=tuple(StrategyRole(str(item)) for item in operator["strategy_roles"]),
                resource_profile=dict(operator["resource_profile"]),
                determinism_mode=str(operator["determinism_mode"]),
                seed_policy=str(operator["seed_policy"]),
                code_hash=project_source_hash(source_root),
                pit_capabilities=tuple(str(item) for item in operator["pit_capabilities"]),
            )
        except (OperatorContractError, ValueError, TypeError) as exc:
            raise ExtensionError(f"项目算子薄声明 operator 无效: {exc}") from exc
        return cls(
            project_id=str(payload["project_id"]),
            operator_spec=specification,
            entry_module=str(entry["module"]),
            entry_function=str(entry["function"]),
            dependency_lock={str(key): str(value) for key, value in dependency_lock.items()},
            permissions=ProjectOperatorPermissionProfile.from_dict(permissions),
            project_artifact_types=tuple(
                str(item) for item in payload.get("project_artifact_types", ())
            ),
            contract_version=str(payload["contract_version"]),
        )


def load_project_operator_declaration(
    path: str | Path,
    *,
    source_root: str | Path | None = None,
) -> object:
    """读取显式 YAML/JSON 薄声明，不扫描源码目录。"""

    declaration_path = Path(path).resolve(strict=True)
    if declaration_path.suffix.lower() not in {".json", ".yaml", ".yml"}:
        raise ExtensionError("项目算子薄声明必须是 YAML 或 JSON")
    try:
        payload = yaml.safe_load(declaration_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ExtensionError("项目算子薄声明无法读取") from exc
    if not isinstance(payload, Mapping):
        raise ExtensionError("项目算子薄声明必须是对象")
    if source_root is None:
        raise ExtensionError("Python 项目算子声明必须提供显式源码目录")
    return ProjectOperatorDeclaration.from_dict(payload, source_root=source_root)


def _validate_imports(root: Path, entries: tuple[dict[str, object], ...], dependency_lock: Mapping[str, str]) -> None:
    validate_project_dependency_lock(dependency_lock)
    internal = {str(item["path"]).split("/", 1)[0].removesuffix(".py") for item in entries}
    stdlib = set(getattr(sys, "stdlib_module_names", ()))
    declared = set(dependency_lock)
    for item in entries:
        path = root / str(item["path"])
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            module = None
            if isinstance(node, ast.Import):
                names = [alias.name.split(".", 1)[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    continue
                names = [node.module.split(".", 1)[0]] if node.module else []
            else:
                continue
            for module in names:
                if module not in internal and module not in stdlib and module not in declared:
                    raise ExtensionError(f"项目源码使用未登记依赖: {module}")


def _validate_entry_abi(tree: ast.Module, entry_function: str) -> None:
    entries = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == entry_function
    ]
    if not entries:
        raise ExtensionError(
            "项目算子入口函数不存在",
            failure_payload=_repair_payload(
                "entry_missing",
                missing_requirements=("entry.function",),
            ),
        )
    entry = entries[0]
    positional_count = len(entry.args.posonlyargs) + len(entry.args.args)
    required_positional_count = positional_count - len(entry.args.defaults)
    required_keyword_only = tuple(
        argument.arg
        for argument, default in zip(
            entry.args.kwonlyargs,
            entry.args.kw_defaults,
            strict=True,
        )
        if default is None
    )
    signature_valid = (
        isinstance(entry, ast.FunctionDef)
        and len(entries) == 1
        and required_positional_count <= 3
        and (positional_count >= 3 or entry.args.vararg is not None)
        and not required_keyword_only
    )
    if not signature_valid:
        raise ExtensionError(
            "项目算子入口 ABI 必须是可由 Worker 以三个位置参数同步调用的函数",
            failure_payload=_repair_payload(
                "entry_abi_invalid",
                missing_requirements=(
                    "entry_signature(context, inputs, output_root)",
                ),
            ),
        )


def compile_project_operator_bundle(
    *,
    source_root: str | Path,
    output_root: str | Path,
    project_id: str,
    operator_spec: OperatorSpec,
    entry_module: str,
    entry_function: str,
    dependency_lock: Mapping[str, str],
    registered_operator_specs: Iterable[OperatorSpec],
    project_artifact_types: Iterable[str] = (),
    permissions: ProjectOperatorPermissionProfile | None = None,
) -> Path:
    """从显式受信源码根生成内容寻址、不可变的项目算子 bundle。"""
    _safe_id(project_id, "project_id")
    if not _MODULE_PATTERN.fullmatch(entry_module) or not _FUNCTION_PATTERN.fullmatch(entry_function):
        raise ExtensionError("项目算子入口描述符无效")
    root, entries = _source_entries(source_root)
    source_hash = typed_canonical_hash({"source_files": list(entries)})
    if operator_spec.code_hash != source_hash:
        raise ExtensionError("operator code_hash 未绑定当前源码闭包")
    validate_project_operator_artifacts(
        operator_spec,
        registered_operator_specs,
        project_artifact_types=project_artifact_types,
    )
    normalized_lock = dict(sorted((str(key), str(value)) for key, value in dependency_lock.items()))
    if any(not _ID_PATTERN.fullmatch(key) or not value or value.strip() != value for key, value in normalized_lock.items()):
        raise ExtensionError("项目依赖 lock 必须使用稳定导入名和精确版本")
    _validate_imports(root, entries, normalized_lock)
    entry_path = root / Path(*entry_module.split(".")).with_suffix(".py")
    if not entry_path.is_file() or entry_path.relative_to(root).as_posix() not in {str(item["path"]) for item in entries}:
        raise ExtensionError("项目算子入口模块不在源码闭包")
    tree = ast.parse(entry_path.read_text(encoding="utf-8"), filename=str(entry_path))
    _validate_entry_abi(tree, entry_function)
    permission_profile = permissions or ProjectOperatorPermissionProfile()
    dependency_lock_hash = typed_canonical_hash(normalized_lock)
    bundle_id = f"project.{project_id}.{operator_spec.operator_id}.{operator_spec.operator_version}"
    payload = {
        "project_id": project_id,
        "bundle_id": bundle_id,
        "operator_spec": operator_spec.to_dict(),
        "entry": {"module": entry_module, "function": entry_function},
        "source_files": list(entries),
        "source_tree_hash": source_hash,
        "dependency_lock": normalized_lock,
        "dependency_lock_hash": dependency_lock_hash,
        "permissions": permission_profile.to_dict(),
        "requires_python": PROJECT_OPERATOR_REQUIRES_PYTHON,
        "contract_version": PROJECT_OPERATOR_BUNDLE_VERSION,
        "abi_version": PROJECT_OPERATOR_ABI_VERSION,
    }
    bundle_hash = typed_canonical_hash(payload)
    manifest = ProjectOperatorBundleManifest(
        project_id=project_id,
        bundle_id=bundle_id,
        operator_spec=operator_spec,
        entry_module=entry_module,
        entry_function=entry_function,
        source_files=entries,
        source_tree_hash=source_hash,
        dependency_lock=normalized_lock,
        dependency_lock_hash=dependency_lock_hash,
        permissions=permission_profile,
        requires_python=PROJECT_OPERATOR_REQUIRES_PYTHON,
        bundle_hash=bundle_hash,
    )
    output = Path(output_root).resolve() / bundle_hash
    output_parent = output.parent
    try:
        output_parent.relative_to(root)
    except ValueError:
        pass
    else:
        raise ExtensionError("项目 bundle 输出根不得位于源码根内")
    if output.exists():
        raise ExtensionError(f"项目 bundle 输出已存在: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle_hash}.", dir=output.parent))
    try:
        source_output = staging / "sources"
        for item in entries:
            relative = Path(str(item["path"]))
            target = source_output / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / relative, target)
        (staging / "dependency-lock.json").write_bytes(_canonical_bytes(normalized_lock) + b"\n")
        (staging / "manifest.json").write_bytes(_canonical_bytes(manifest.to_dict()) + b"\n")
        (staging / "COMMITTED").write_text(bundle_hash + "\n", encoding="utf-8")
        os.replace(staging, output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    try:
        verify_project_operator_bundle(output)
    except Exception:
        shutil.rmtree(output, ignore_errors=True)
        raise
    return output


def verify_project_operator_bundle(path: str | Path) -> object:
    """离线复验 bundle 的文件闭包、合同和全部内容身份。"""
    root = Path(path).resolve(strict=True)
    if not root.is_dir():
        raise ExtensionError("项目 bundle 必须是已存在目录")
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise ExtensionError("项目 bundle 缺少 manifest.json")
    try:
        contract_probe = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExtensionError("项目 bundle manifest 无法读取") from exc
    expected_top = {"sources", "dependency-lock.json", "manifest.json", "COMMITTED"}
    if {item.name for item in root.iterdir()} != expected_top:
        raise ExtensionError("项目 bundle 顶层文件集合漂移")
    for name in ("dependency-lock.json", "manifest.json", "COMMITTED"):
        _assert_regular_file(root / name, root=root)
    if not (root / "sources").is_dir() or not (root / "sources").resolve().is_relative_to(root):
        raise ExtensionError("项目 bundle sources 必须位于 bundle 根目录内")
    try:
        raw = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExtensionError("项目 bundle manifest 无法读取") from exc
    if not isinstance(raw, Mapping):
        raise ExtensionError("项目 bundle manifest 必须是对象")
    manifest = ProjectOperatorBundleManifest.from_dict(raw)
    if root.name != manifest.bundle_hash:
        raise ExtensionError("项目 bundle 目录名与身份不一致")
    if (root / "COMMITTED").read_text(encoding="utf-8") != manifest.bundle_hash + "\n":
        raise ExtensionError("项目 bundle COMMITTED 不一致")
    lock_bytes = _canonical_bytes(dict(manifest.dependency_lock)) + b"\n"
    if (root / "dependency-lock.json").read_bytes() != lock_bytes:
        raise ExtensionError("项目 bundle dependency lock 漂移")
    source_root = root / "sources"
    descendants = _source_paths(source_root)
    actual_paths = tuple(sorted(item.relative_to(source_root).as_posix() for item in descendants if item.is_file()))
    declared_paths = tuple(str(item["path"]) for item in manifest.source_files)
    if actual_paths != declared_paths:
        raise ExtensionError("项目 bundle 源码文件集合漂移")
    allowed_directories = {""}
    for relative in declared_paths:
        parent = PurePosixPath(relative).parent
        while parent.as_posix() != ".":
            allowed_directories.add(parent.as_posix())
            parent = parent.parent
    actual_directories = {item.relative_to(source_root).as_posix() for item in descendants if item.is_dir()}
    if actual_directories != allowed_directories - {""}:
        raise ExtensionError("项目 bundle 源码目录集合漂移")
    for item in manifest.source_files:
        file_path = source_root / str(item["path"])
        _assert_regular_file(file_path, root=source_root.resolve(strict=True))
        data = file_path.read_bytes()
        if len(data) != item["size"] or _sha256(data) != item["sha256"]:
            raise ExtensionError(f"项目 bundle 源码漂移: {item['path']}")
    if typed_canonical_hash({"source_files": [dict(item) for item in manifest.source_files]}) != manifest.source_tree_hash:
        raise ExtensionError("项目 bundle source tree hash 不一致")
    if typed_canonical_hash(dict(manifest.dependency_lock)) != manifest.dependency_lock_hash:
        raise ExtensionError("项目 bundle dependency lock hash 不一致")
    _validate_imports(source_root, tuple(dict(item) for item in manifest.source_files), manifest.dependency_lock)
    return manifest


__all__ = [
    "PROJECT_OPERATOR_ABI_VERSION",
    "PROJECT_OPERATOR_BUNDLE_VERSION",
    "PROJECT_OPERATOR_DECLARATION_VERSION",
    "PROJECT_OPERATOR_REQUIRES_PYTHON",
    "ProjectOperatorBundleManifest",
    "ProjectOperatorDeclaration",
    "ProjectArtifactCommit",
    "ProjectArtifactInput",
    "ProjectDirectoryCommit",
    "ProjectOperatorContext",
    "ProjectOperatorPermissionProfile",
    "compile_project_operator_bundle",
    "load_project_operator_declaration",
    "project_source_hash",
    "verify_project_operator_bundle",
]
