"""研究包固定目录、严格 YAML 读取与原子初始化。"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path
import shutil

import yaml

from .models import LocalizationDecision, MetricContract, PackageClaimContract, PackageSource, ResearchPackage, ResearchPackageError, SourceProvenance
from .constants import OPERATOR_GRAPH_BUILDER_ID


PACKAGE_FILES = ("package.yaml", "sources/sources.yaml", "localization.yaml", "spec/research.yaml")


def load_research_package(root: str | Path) -> ResearchPackage:
    directory = Path(root).resolve()
    package = _load_yaml(directory, "package.yaml")
    source_payload = _load_yaml(directory, "sources/sources.yaml")
    localization_payload = _load_yaml(directory, "localization.yaml")
    spec_payload = _load_yaml(directory, "spec/research.yaml")
    _exact(package, _PACKAGE_KEYS, "package.yaml")
    if package.get("builder_id") == OPERATOR_GRAPH_BUILDER_ID:
        _validate_declarative_layout(directory)
    _exact(source_payload, {"sources"}, "sources/sources.yaml")
    _exact(localization_payload, {"decisions"}, "localization.yaml")
    sources = tuple(_load_source(item) for item in _typed_list(source_payload["sources"], "sources/sources.yaml.sources"))
    localizations = tuple(
        LocalizationDecision(**{**_typed_mapping(item, _LOCALIZATION_KEYS, "localization"), "evidence_source_ids": _string_sequence(item["evidence_source_ids"], "localization.evidence_source_ids")})
        for item in _typed_list(localization_payload["decisions"], "localization.yaml.decisions")
    )
    metric = _typed_mapping(package["metric_contract"], _METRIC_KEYS, "metric_contract")
    claim = _typed_mapping(package["claim_contract"], _CLAIM_KEYS, "claim_contract")
    semantics = metric["semantics"]
    if not isinstance(semantics, Mapping) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in semantics.items()):
        raise ResearchPackageError("metric semantics 必须是字符串映射")
    metric_contract = MetricContract.build(contract_id=metric["contract_id"], version=metric["version"], metrics=_string_sequence(metric["metrics"], "metric_contract.metrics"), semantics=semantics)
    claim_contract = PackageClaimContract.build(allowed_claim_levels=_string_sequence(claim["allowed_claim_levels"], "claim_contract.allowed_claim_levels"), max_claim_level=claim["max_claim_level"])
    return ResearchPackage.build(
        package_slug=package["package_slug"],
        display_name=package["display_name"],
        package_version=package["package_version"],
        builder_id=package["builder_id"],
        sources=sources,
        localizations=localizations,
        metric_contract=metric_contract,
        claim_contract=claim_contract,
        spec_payload=spec_payload,
    )


def initialize_research_package(destination: str | Path) -> Path:
    """从安装包资源原子初始化，目标存在时失败关闭。"""
    target = Path(destination).resolve()
    if target.exists():
        raise ResearchPackageError("ResearchPackage 目标目录已存在")
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        raise ResearchPackageError("ResearchPackage 临时目录已存在")
    temporary.mkdir(parents=True)
    try:
        for relative_path, content in default_research_package_template().items():
            output = (temporary / relative_path).resolve()
            output.relative_to(temporary)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(content, encoding="utf-8")
        validate_research_package_layout(temporary)
        temporary.replace(target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return target


def validate_research_package_layout(root: str | Path) -> None:
    """只校验草稿的四份声明文件及目录；正式内容由严格 loader 校验。"""
    directory = Path(root).resolve()
    _validate_declarative_layout(directory)
    for relative_path in PACKAGE_FILES:
        _load_yaml(directory, relative_path)


def default_research_package_template() -> Mapping[str, str]:
    root = files("research_pipeline.packages").joinpath("templates")
    try:
        return {relative_path: root.joinpath(*relative_path.split("/")).read_text(encoding="utf-8") for relative_path in PACKAGE_FILES}
    except (FileNotFoundError, OSError) as exc:
        raise ResearchPackageError("安装包内 ResearchPackage 模板缺失") from exc


def _load_yaml(root: Path, relative_path: str) -> dict[str, object]:
    path = (root / relative_path).resolve()
    try:
        path.relative_to(root)
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (ValueError, OSError, yaml.YAMLError) as exc:
        raise ResearchPackageError(f"无法读取 {relative_path}") from exc
    if not isinstance(payload, dict):
        raise ResearchPackageError(f"{relative_path} 顶层必须是映射")
    return payload


def _validate_declarative_layout(root: Path) -> None:
    allowed = {Path(item).as_posix() for item in PACKAGE_FILES} | {"README.md"}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ResearchPackageError("声明式 ResearchPackage 不允许符号链接或 junction")
        if path.is_file() and path.relative_to(root).as_posix() not in allowed:
            raise ResearchPackageError("声明式 ResearchPackage 只允许四个声明文件和 README")


def _typed_list(value: object, field: str) -> list[object]:
    if isinstance(value, list) and not value:
        raise ResearchPackageError(f"{field} 必须是非空列表；请填写对应声明后重新运行 package lint")
    if not isinstance(value, list):
        raise ResearchPackageError(f"{field} 必须是非空列表")
    return value


def _load_source(value: object) -> PackageSource:
    if not isinstance(value, dict):
        raise ResearchPackageError("source 必须是映射")
    # 旧研究包没有 provenance；明确降级为仅引用，不把历史 content_hash 当作正文证明。
    expected = _SOURCE_KEYS | ({"provenance"} if "provenance" in value else set())
    source = _typed_mapping(value, expected, "source")
    provenance_payload = source.pop("provenance", None)
    if provenance_payload is None:
        provenance = SourceProvenance.citation_only()
    else:
        provenance = SourceProvenance(**_typed_mapping(provenance_payload, _PROVENANCE_KEYS, "source.provenance"))
    return PackageSource(**source, provenance=provenance)


def _typed_mapping(value: object, expected: set[str], field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ResearchPackageError(f"{field} 必须是映射")
    _exact(value, expected, field)
    return value


def _string_sequence(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ResearchPackageError(f"{field} 必须是非空字符串序列")
    return tuple(value)


def _exact(value: object, expected: set[str], field: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise ResearchPackageError(f"{field} schema 字段不完整或含未知字段")


_PACKAGE_KEYS = {"package_slug", "display_name", "package_version", "builder_id", "metric_contract", "claim_contract"}
_SOURCE_KEYS = {"source_id", "source_type", "title", "url", "accessed_at", "content_hash", "license_id", "status", "limitation"}
_PROVENANCE_KEYS = {"mode", "content_digest", "media_type", "snapshot_artifact_id", "snapshot_manifest_hash", "importer_id", "imported_at", "contract_version"}
_LOCALIZATION_KEYS = {"decision_id", "original_assumption", "local_market", "local_adaptation", "evidence_source_ids", "status", "claim_effect"}
_METRIC_KEYS = {"contract_id", "version", "metrics", "semantics"}
_CLAIM_KEYS = {"allowed_claim_levels", "max_claim_level"}


__all__ = ["PACKAGE_FILES", "default_research_package_template", "initialize_research_package", "load_research_package"]
