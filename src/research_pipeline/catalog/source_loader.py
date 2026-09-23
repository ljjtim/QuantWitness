"""严格 YAML 目录声明读取器。"""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import yaml

from .errors import CatalogParseError
from .models import (
    ApprovalDecision,
    CatalogContract,
    CatalogCoverageBaseline,
    CatalogSourceManifest,
    DatasetContract,
    FieldContract,
    FieldFamilySource,
    PhysicalBindingContract,
    PolicyContract,
    TransformContract,
)


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise CatalogParseError(f"YAML 重复 key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)

_KINDS = {
    "field": FieldContract,
    "dataset": DatasetContract,
    "policy": PolicyContract,
    "binding": PhysicalBindingContract,
    "transform": TransformContract,
    "coverage_baseline": CatalogCoverageBaseline,
    "source_manifest": CatalogSourceManifest,
    "field_family": FieldFamilySource,
    "approval": ApprovalDecision,
}


def load_contract_payload(payload: Any) -> CatalogContract:
    if not isinstance(payload, dict):
        raise CatalogParseError("目录声明必须是 mapping")
    raw = dict(payload)
    kind = raw.pop("kind", None)
    model = _KINDS.get(kind)
    if model is None:
        raise CatalogParseError(f"未知目录声明 kind: {kind}")
    allowed = {item.name for item in fields(model)}
    unknown = set(raw) - allowed
    if unknown:
        raise CatalogParseError(f"目录声明包含未知字段: {sorted(unknown)}")
    for item in fields(model):
        if item.name in raw and item.type in {tuple[str, ...], "tuple[str, ...]"} and isinstance(raw[item.name], list):
            raw[item.name] = tuple(raw[item.name])
    try:
        return model(**raw)
    except TypeError as exc:
        raise CatalogParseError(f"目录声明字段不完整: {exc}") from exc


def load_contract_file(path: str | Path) -> CatalogContract:
    try:
        payload = yaml.load(Path(path).read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise CatalogParseError(f"YAML 解析失败: {exc}") from exc
    return load_contract_payload(payload)


__all__ = ["load_contract_file", "load_contract_payload"]
