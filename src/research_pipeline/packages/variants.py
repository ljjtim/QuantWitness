"""从一个完整 ResearchPackage 原子展开受限参数变体。"""

from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path
import re
import shutil
from typing import Mapping

import yaml

from .models import ResearchPackageError
from .store import PACKAGE_FILES, load_research_package


VARIANT_SET_VERSION = "research-package-variant-set-v1"
_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def expand_research_package_variants(
    *,
    base_root: str | Path,
    manifest_path: str | Path,
    output_root: str | Path,
) -> dict[str, object]:
    """展开完整包；任一变体无效时不发布任何输出。"""

    base_path = Path(base_root).resolve(strict=True)
    base = load_research_package(base_path)
    payload = _load_manifest(manifest_path)
    if payload["base_package_hash"] != base.package_hash:
        raise ResearchPackageError("变体清单 base_package_hash 与基包不一致")
    target = Path(output_root).resolve()
    if target.exists():
        raise ResearchPackageError("变体输出目录必须不存在")
    staging = target.with_name(f".{target.name}.tmp")
    if staging.exists():
        raise ResearchPackageError("变体临时输出目录已存在")
    variants = payload["variants"]
    errors: list[str] = []
    generated: list[dict[str, object]] = []
    staging.mkdir(parents=True)
    try:
        seen_variant_ids: set[str] = set()
        seen_slugs: set[str] = set()
        seen_graph_ids: set[str] = set()
        for index, raw_variant in enumerate(variants):
            label = f"variants[{index}]"
            try:
                variant = _validate_variant(
                    raw_variant,
                    label=label,
                    seen_variant_ids=seen_variant_ids,
                    seen_slugs=seen_slugs,
                    seen_graph_ids=seen_graph_ids,
                )
                destination = staging / variant["package_slug"]
                _materialize_variant(base_path, destination, variant, minute_scope_inputs=base.minute_scope_inputs)
                materialized = load_research_package(destination)
                generated.append(
                    {
                        "variant_id": variant["variant_id"],
                        "package_slug": materialized.package_slug,
                        "package_id": materialized.package_id,
                        "package_hash": materialized.package_hash,
                        "path": str(target / variant["package_slug"]),
                    }
                )
            except (OSError, ResearchPackageError, TypeError, ValueError) as exc:
                errors.append(f"{label}: {exc}")
        if errors:
            raise ResearchPackageError("变体展开失败：" + "；".join(errors))
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        "contract_version": VARIANT_SET_VERSION,
        "base_package_hash": base.package_hash,
        "output_root": str(target),
        "variants": generated,
        "next_action": "分别对生成的完整 ResearchPackage 运行 package lint 和 package admit。",
    }


def _load_manifest(path: str | Path) -> dict[str, object]:
    try:
        payload = yaml.safe_load(Path(path).resolve(strict=True).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ResearchPackageError("变体清单无法读取") from exc
    expected = {"contract_version", "base_package_hash", "variants"}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ResearchPackageError("变体清单 schema 无效")
    if payload["contract_version"] != VARIANT_SET_VERSION:
        raise ResearchPackageError("变体清单 contract_version 不受支持")
    if not isinstance(payload["base_package_hash"], str):
        raise ResearchPackageError("变体清单 base_package_hash 无效")
    if not isinstance(payload["variants"], list) or not payload["variants"]:
        raise ResearchPackageError("变体清单 variants 必须是非空列表")
    return payload


def _validate_variant(
    value: object,
    *,
    label: str,
    seen_variant_ids: set[str],
    seen_slugs: set[str],
    seen_graph_ids: set[str],
) -> dict[str, object]:
    expected = {
        "variant_id",
        "package_slug",
        "display_name",
        "package_version",
        "graph_id",
        "parameter_overrides",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ResearchPackageError("variant schema 无效")
    result = dict(value)
    for field in ("variant_id", "package_slug", "display_name", "package_version", "graph_id"):
        if not isinstance(result[field], str) or not result[field].strip():
            raise ResearchPackageError(f"{field} 必须是非空字符串")
    for field in ("variant_id", "package_slug", "graph_id"):
        if not _SAFE_COMPONENT.fullmatch(str(result[field])):
            raise ResearchPackageError(f"{field} 不是安全稳定标识")
    for field, seen in (
        ("variant_id", seen_variant_ids),
        ("package_slug", seen_slugs),
        ("graph_id", seen_graph_ids),
    ):
        item = str(result[field])
        if item in seen:
            raise ResearchPackageError(f"{field} 重复: {item}")
    overrides = result["parameter_overrides"]
    if not isinstance(overrides, list) or not overrides:
        raise ResearchPackageError("parameter_overrides 必须是非空列表")
    normalized = []
    keys: set[tuple[str, str]] = set()
    for index, override in enumerate(overrides):
        if not isinstance(override, Mapping) or set(override) != {
            "node_id",
            "parameter_name",
            "value",
        }:
            raise ResearchPackageError(f"parameter_overrides[{index}] schema 无效")
        node_id = override["node_id"]
        parameter_name = override["parameter_name"]
        if not isinstance(node_id, str) or not node_id or not isinstance(parameter_name, str) or not parameter_name:
            raise ResearchPackageError(f"parameter_overrides[{index}] 定位字段无效")
        key = (node_id, parameter_name)
        if key in keys:
            raise ResearchPackageError(f"参数覆盖重复: {node_id}/{parameter_name}")
        keys.add(key)
        normalized.append(dict(override))
    result["parameter_overrides"] = normalized
    seen_variant_ids.add(str(result["variant_id"]))
    seen_slugs.add(str(result["package_slug"]))
    seen_graph_ids.add(str(result["graph_id"]))
    return result


def _materialize_variant(
    base_root: Path,
    destination: Path,
    variant: Mapping[str, object],
    *, minute_scope_inputs=None,
) -> None:
    destination.mkdir()
    for relative in PACKAGE_FILES:
        source = base_root / relative
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    from research_pipeline.domain.minute_scope_input import copy_minute_scope_input

    for source in (minute_scope_inputs or {}).values():
        copy_minute_scope_input(source, destination)
    readme = base_root / "README.md"
    if readme.is_file():
        shutil.copyfile(readme, destination / "README.md")
    package_payload = _read_yaml(destination / "package.yaml")
    package_payload.update(
        {
            "package_slug": variant["package_slug"],
            "display_name": variant["display_name"],
            "package_version": variant["package_version"],
        }
    )
    spec_path = destination / "spec" / "research.yaml"
    spec_payload = _read_yaml(spec_path)
    graph = spec_payload.get("graph")
    if not isinstance(graph, dict) or not isinstance(graph.get("nodes"), list):
        raise ResearchPackageError("基包 spec.graph schema 无效")
    graph["graph_id"] = variant["graph_id"]
    nodes = {
        node.get("node_id"): node
        for node in graph["nodes"]
        if isinstance(node, dict) and isinstance(node.get("node_id"), str)
    }
    for override in variant["parameter_overrides"]:
        node_id = str(override["node_id"])
        parameter_name = str(override["parameter_name"])
        node = nodes.get(node_id)
        if node is None or not isinstance(node.get("parameters"), dict):
            raise ResearchPackageError(f"覆盖引用未知节点或无参数节点: {node_id}")
        parameters = node["parameters"]
        if parameter_name not in parameters:
            raise ResearchPackageError(f"覆盖引用未知参数: {node_id}/{parameter_name}")
        original = parameters[parameter_name]
        incoming = override["value"]
        if not _compatible_value_type(original, incoming):
            raise ResearchPackageError(f"覆盖参数类型不一致: {node_id}/{parameter_name}")
        parameters[parameter_name] = deepcopy(incoming)
    _write_yaml(destination / "package.yaml", package_payload)
    _write_yaml(spec_path, spec_payload)


def _compatible_value_type(original: object, incoming: object) -> bool:
    if isinstance(original, bool):
        return isinstance(incoming, bool)
    if isinstance(original, int):
        return isinstance(incoming, int) and not isinstance(incoming, bool)
    if isinstance(original, float):
        return isinstance(incoming, (int, float)) and not isinstance(incoming, bool)
    return type(incoming) is type(original)


def _read_yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ResearchPackageError(f"{path.name} 顶层必须是映射")
    return payload


def _write_yaml(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(
        yaml.safe_dump(dict(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


__all__ = ["VARIANT_SET_VERSION", "expand_research_package_variants"]
