"""项目 Verifier bundle 的独立身份、准入和离线复验合同。"""

from __future__ import annotations

import ast
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.metric_contracts import MetricDefinition

from .errors import ExtensionError
from .project_bundle import (
    PROJECT_OPERATOR_REQUIRES_PYTHON,
    _FUNCTION_PATTERN,
    _HASH_PATTERN,
    _ID_PATTERN,
    _MODULE_PATTERN,
    _assert_regular_file,
    _canonical_bytes,
    _safe_id,
    _safe_relative_path,
    _source_entries,
    _source_paths,
    _validate_imports,
)


PROJECT_VERIFIER_BUNDLE_VERSION = "project-verifier-extension-bundle-v1"
PROJECT_VERIFIER_ABI_VERSION = "project-verifier-abi-v1"


@dataclass(frozen=True)
class ProjectVerifierBundleManifest:
    project_id: str
    verifier_id: str
    verifier_version: str
    entry_module: str
    entry_function: str
    authorized_schema_ids: tuple[str, ...]
    authorized_support_artifact_types: tuple[str, ...]
    metric_definitions: tuple[MetricDefinition, ...]
    source_files: tuple[Mapping[str, object], ...]
    source_tree_hash: str
    dependency_lock: Mapping[str, str]
    dependency_lock_hash: str
    requires_python: str
    bundle_hash: str
    contract_version: str = PROJECT_VERIFIER_BUNDLE_VERSION
    abi_version: str = PROJECT_VERIFIER_ABI_VERSION

    def __post_init__(self) -> None:
        for name in ("project_id", "verifier_id", "verifier_version"):
            _safe_id(str(getattr(self, name)), name)
        if (
            not _MODULE_PATTERN.fullmatch(self.entry_module)
            or not _FUNCTION_PATTERN.fullmatch(self.entry_function)
        ):
            raise ExtensionError("项目 Verifier 入口描述符无效")
        schemas = tuple(sorted(set(self.authorized_schema_ids)))
        support_types = tuple(sorted(set(self.authorized_support_artifact_types)))
        if any(not _ID_PATTERN.fullmatch(item) for item in (*schemas, *support_types)):
            raise ExtensionError("项目 Verifier 授权身份无效")
        if not schemas and not support_types:
            raise ExtensionError("项目 Verifier 必须声明至少一个授权输入")
        object.__setattr__(self, "authorized_schema_ids", schemas)
        object.__setattr__(self, "authorized_support_artifact_types", support_types)
        if len({item.metric_ref for item in self.metric_definitions}) != len(self.metric_definitions):
            raise ExtensionError("项目 Verifier Metric 定义重复")
        if any(
            item.result_schema_id not in schemas
            for item in self.metric_definitions
        ):
            raise ExtensionError("项目 Verifier Metric 必须绑定已授权 Result schema")
        object.__setattr__(self, "metric_definitions", tuple(sorted(
            self.metric_definitions, key=lambda item: item.metric_ref
        )))
        hashes = (self.source_tree_hash, self.dependency_lock_hash, self.bundle_hash)
        if any(not _HASH_PATTERN.fullmatch(value) for value in hashes):
            raise ExtensionError("项目 Verifier bundle 身份无效")
        if self.contract_version != PROJECT_VERIFIER_BUNDLE_VERSION:
            raise ExtensionError("项目 Verifier bundle 版本不受支持")
        if self.abi_version != PROJECT_VERIFIER_ABI_VERSION:
            raise ExtensionError("项目 Verifier ABI 版本不受支持")
        if self.requires_python != PROJECT_OPERATOR_REQUIRES_PYTHON:
            raise ExtensionError("项目 Verifier requires-python 不受支持")
        files = tuple(MappingProxyType(dict(item)) for item in self.source_files)
        if tuple(sorted(files, key=lambda item: str(item["path"]))) != files:
            raise ExtensionError("项目 Verifier 源码清单未规范排序")
        object.__setattr__(self, "source_files", files)
        lock = dict(sorted((str(key), str(value)) for key, value in self.dependency_lock.items()))
        object.__setattr__(self, "dependency_lock", MappingProxyType(lock))
        if self.bundle_hash != typed_canonical_hash(self.payload()):
            raise ExtensionError("项目 Verifier bundle hash 不一致")

    @property
    def implementation_hash(self) -> str:
        return typed_canonical_hash({
            "verifier_id": self.verifier_id,
            "verifier_version": self.verifier_version,
            "source_tree_hash": self.source_tree_hash,
            "dependency_lock_hash": self.dependency_lock_hash,
            "abi_version": self.abi_version,
        })

    def identity(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "verifier_id": self.verifier_id,
            "verifier_version": self.verifier_version,
            "bundle_hash": self.bundle_hash,
            "implementation_hash": self.implementation_hash,
            "authorized_schema_ids": list(self.authorized_schema_ids),
            "authorized_support_artifact_types": list(
                self.authorized_support_artifact_types
            ),
            "metric_definitions": [
                {**item.payload(), "definition_digest": item.definition_digest}
                for item in self.metric_definitions
            ],
            "abi_version": self.abi_version,
        }

    def payload(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "verifier_id": self.verifier_id,
            "verifier_version": self.verifier_version,
            "entry": {"module": self.entry_module, "function": self.entry_function},
            "authorized_schema_ids": list(self.authorized_schema_ids),
            "authorized_support_artifact_types": list(
                self.authorized_support_artifact_types
            ),
            "metric_definitions": [
                {**item.payload(), "definition_digest": item.definition_digest}
                for item in self.metric_definitions
            ],
            "source_files": [dict(item) for item in self.source_files],
            "source_tree_hash": self.source_tree_hash,
            "dependency_lock": dict(self.dependency_lock),
            "dependency_lock_hash": self.dependency_lock_hash,
            "requires_python": self.requires_python,
            "contract_version": self.contract_version,
            "abi_version": self.abi_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "bundle_hash": self.bundle_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ProjectVerifierBundleManifest":
        expected = {
            "project_id", "verifier_id", "verifier_version", "entry",
            "authorized_schema_ids", "authorized_support_artifact_types",
            "metric_definitions",
            "source_files", "source_tree_hash", "dependency_lock",
            "dependency_lock_hash", "requires_python", "bundle_hash",
            "contract_version", "abi_version",
        }
        if set(payload) != expected or not isinstance(payload["entry"], Mapping):
            raise ExtensionError("项目 Verifier manifest schema 无效")
        if set(payload["entry"]) != {"module", "function"}:
            raise ExtensionError("项目 Verifier entry schema 无效")
        for field in (
            "authorized_schema_ids",
            "authorized_support_artifact_types",
            "source_files",
        ):
            if not isinstance(payload[field], list):
                raise ExtensionError(f"项目 Verifier {field} 无效")
        if not isinstance(payload["dependency_lock"], Mapping):
            raise ExtensionError("项目 Verifier dependency lock 无效")
        if any(not isinstance(item, Mapping) for item in payload["source_files"]):
            raise ExtensionError("项目 Verifier source_files 条目无效")
        if (
            not isinstance(payload["metric_definitions"], list)
            or any(not isinstance(item, Mapping) for item in payload["metric_definitions"])
        ):
            raise ExtensionError("项目 Verifier metric_definitions 无效")
        return cls(
            project_id=str(payload["project_id"]),
            verifier_id=str(payload["verifier_id"]),
            verifier_version=str(payload["verifier_version"]),
            entry_module=str(payload["entry"]["module"]),
            entry_function=str(payload["entry"]["function"]),
            authorized_schema_ids=tuple(str(item) for item in payload["authorized_schema_ids"]),
            authorized_support_artifact_types=tuple(
                str(item) for item in payload["authorized_support_artifact_types"]
            ),
            metric_definitions=tuple(
                MetricDefinition.from_dict(item) for item in payload["metric_definitions"]
            ),
            source_files=tuple(dict(item) for item in payload["source_files"]),
            source_tree_hash=str(payload["source_tree_hash"]),
            dependency_lock={str(key): str(value) for key, value in payload["dependency_lock"].items()},
            dependency_lock_hash=str(payload["dependency_lock_hash"]),
            requires_python=str(payload["requires_python"]),
            bundle_hash=str(payload["bundle_hash"]),
            contract_version=str(payload["contract_version"]),
            abi_version=str(payload["abi_version"]),
        )


def _validate_verifier_entry(root: Path, module: str, function: str) -> None:
    path = root / Path(*module.split(".")).with_suffix(".py")
    if not path.is_file():
        raise ExtensionError("项目 Verifier 入口模块不在源码闭包")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    entries = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == function
    ]
    if len(entries) != 1:
        raise ExtensionError("项目 Verifier 入口函数不存在或重复")
    entry = entries[0]
    positional = len(entry.args.posonlyargs) + len(entry.args.args)
    required = positional - len(entry.args.defaults)
    if required > 2 or positional < 2 or entry.args.kwonlyargs:
        raise ExtensionError(
            "项目 Verifier ABI 必须是同步函数 verify(context, input_root)"
        )


def compile_project_verifier_bundle(
    *,
    source_root: str | Path,
    output_root: str | Path,
    project_id: str,
    verifier_id: str,
    verifier_version: str,
    entry_module: str,
    entry_function: str,
    authorized_schema_ids: Iterable[str],
    authorized_support_artifact_types: Iterable[str] = (),
    metric_definitions: Iterable[MetricDefinition] = (),
    dependency_lock: Mapping[str, str],
) -> Path:
    """从显式源码根生成与 Operator bundle 分离的 Verifier bundle。"""
    root, entries = _source_entries(source_root)
    source_hash = typed_canonical_hash({"source_files": list(entries)})
    normalized_lock = dict(sorted((str(key), str(value)) for key, value in dependency_lock.items()))
    _validate_imports(root, entries, normalized_lock)
    _validate_verifier_entry(root, entry_module, entry_function)
    payload = {
        "project_id": project_id,
        "verifier_id": verifier_id,
        "verifier_version": verifier_version,
        "entry": {"module": entry_module, "function": entry_function},
        "authorized_schema_ids": sorted(set(str(item) for item in authorized_schema_ids)),
        "authorized_support_artifact_types": sorted(
            set(str(item) for item in authorized_support_artifact_types)
        ),
        "metric_definitions": [
            {**item.payload(), "definition_digest": item.definition_digest}
            for item in sorted(metric_definitions, key=lambda definition: definition.metric_ref)
        ],
        "source_files": list(entries),
        "source_tree_hash": source_hash,
        "dependency_lock": normalized_lock,
        "dependency_lock_hash": typed_canonical_hash(normalized_lock),
        "requires_python": PROJECT_OPERATOR_REQUIRES_PYTHON,
        "contract_version": PROJECT_VERIFIER_BUNDLE_VERSION,
        "abi_version": PROJECT_VERIFIER_ABI_VERSION,
    }
    manifest = ProjectVerifierBundleManifest(
        project_id=project_id,
        verifier_id=verifier_id,
        verifier_version=verifier_version,
        entry_module=entry_module,
        entry_function=entry_function,
        authorized_schema_ids=tuple(payload["authorized_schema_ids"]),
        authorized_support_artifact_types=tuple(
            payload["authorized_support_artifact_types"]
        ),
        metric_definitions=tuple(
            MetricDefinition.from_dict(item) for item in payload["metric_definitions"]
        ),
        source_files=entries,
        source_tree_hash=source_hash,
        dependency_lock=normalized_lock,
        dependency_lock_hash=str(payload["dependency_lock_hash"]),
        requires_python=PROJECT_OPERATOR_REQUIRES_PYTHON,
        bundle_hash=typed_canonical_hash(payload),
    )
    output = Path(output_root).resolve() / manifest.bundle_hash
    if output.exists():
        raise ExtensionError(f"项目 Verifier bundle 输出已存在: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{manifest.bundle_hash}.", dir=output.parent))
    try:
        for item in entries:
            relative = Path(str(item["path"]))
            target = staging / "sources" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / relative, target)
        (staging / "dependency-lock.json").write_bytes(
            _canonical_bytes(normalized_lock) + b"\n"
        )
        (staging / "manifest.json").write_bytes(
            _canonical_bytes(manifest.to_dict()) + b"\n"
        )
        (staging / "COMMITTED").write_text(
            manifest.bundle_hash + "\n", encoding="utf-8"
        )
        os.replace(staging, output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    verify_project_verifier_bundle(output)
    return output


def verify_project_verifier_bundle(path: str | Path) -> ProjectVerifierBundleManifest:
    root = Path(path).resolve(strict=True)
    if {item.name for item in root.iterdir()} != {
        "sources", "dependency-lock.json", "manifest.json", "COMMITTED",
    }:
        raise ExtensionError("项目 Verifier bundle 顶层文件集合漂移")
    raw = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ExtensionError("项目 Verifier manifest 必须是对象")
    manifest = ProjectVerifierBundleManifest.from_dict(raw)
    if root.name != manifest.bundle_hash:
        raise ExtensionError("项目 Verifier bundle 目录名与身份不一致")
    if (root / "COMMITTED").read_text(encoding="utf-8") != manifest.bundle_hash + "\n":
        raise ExtensionError("项目 Verifier COMMITTED 不一致")
    if (root / "dependency-lock.json").read_bytes() != (
        _canonical_bytes(dict(manifest.dependency_lock)) + b"\n"
    ):
        raise ExtensionError("项目 Verifier dependency lock 漂移")
    source_root = root / "sources"
    descendants = _source_paths(source_root)
    actual = tuple(sorted(
        item.relative_to(source_root).as_posix()
        for item in descendants if item.is_file()
    ))
    declared = tuple(str(item["path"]) for item in manifest.source_files)
    if actual != declared:
        raise ExtensionError("项目 Verifier 源码文件集合漂移")
    for item in manifest.source_files:
        file_path = source_root / str(item["path"])
        _assert_regular_file(file_path, root=source_root.resolve(strict=True))
        data = file_path.read_bytes()
        import hashlib
        if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ExtensionError(f"项目 Verifier 源码漂移: {item['path']}")
    if typed_canonical_hash({"source_files": [dict(item) for item in manifest.source_files]}) != manifest.source_tree_hash:
        raise ExtensionError("项目 Verifier source tree hash 不一致")
    if typed_canonical_hash(dict(manifest.dependency_lock)) != manifest.dependency_lock_hash:
        raise ExtensionError("项目 Verifier dependency lock hash 不一致")
    _validate_imports(source_root, tuple(dict(item) for item in manifest.source_files), manifest.dependency_lock)
    _validate_verifier_entry(source_root, manifest.entry_module, manifest.entry_function)
    return manifest


@dataclass(frozen=True)
class AdmittedProjectVerifier:
    path: Path
    manifest: ProjectVerifierBundleManifest

    @property
    def identity(self) -> Mapping[str, object]:
        return MappingProxyType(self.manifest.identity())


def admit_project_verifier_bundle(
    path: str | Path,
    *,
    expected_project_id: str | None = None,
) -> AdmittedProjectVerifier:
    manifest = verify_project_verifier_bundle(path)
    if expected_project_id is not None and manifest.project_id != expected_project_id:
        raise ExtensionError("项目 Verifier project_id 与准入上下文不一致")
    return AdmittedProjectVerifier(Path(path).resolve(strict=True), manifest)


__all__ = [
    "PROJECT_VERIFIER_ABI_VERSION",
    "PROJECT_VERIFIER_BUNDLE_VERSION",
    "AdmittedProjectVerifier",
    "ProjectVerifierBundleManifest",
    "admit_project_verifier_bundle",
    "compile_project_verifier_bundle",
    "verify_project_verifier_bundle",
]
