"""目录源合同的 JSON Schema 导出。"""

from dataclasses import MISSING, fields
from collections.abc import Mapping
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

from .models import (
    ApprovalDecision,
    CatalogCoverageBaseline,
    CatalogSourceManifest,
    DatasetContract,
    FieldContract,
    FieldFamilySource,
    PhysicalBindingContract,
    PolicyContract,
    TransformContract,
)


MODELS = {
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


def _schema_for_type(annotation: Any) -> dict[str, Any]:
    origin = get_origin(annotation)
    if annotation is str:
        return {"type": "string"}
    if annotation is int:
        return {"type": "integer"}
    if annotation is bool:
        return {"type": "boolean"}
    if origin in {tuple, list}:
        arguments = get_args(annotation)
        item_type = arguments[0] if arguments else Any
        return {"type": "array", "items": _schema_for_type(item_type)}
    if origin in {dict, Mapping}:
        return {"type": "object"}
    if origin in {Union, UnionType}:
        options = [_schema_for_type(item) for item in get_args(annotation)]
        simple_types = [item["type"] for item in options if set(item) == {"type"}]
        if len(simple_types) == len(options):
            return {"type": simple_types}
        return {"anyOf": options}
    if annotation is type(None):
        return {"type": "null"}
    if annotation is Any:
        return {}
    return {}


def catalog_source_schema(kind: str) -> dict[str, Any]:
    model = MODELS[kind]
    type_hints = get_type_hints(model)
    properties = {"kind": {"const": kind}}
    required = ["kind"]
    for item in fields(model):
        properties[item.name] = _schema_for_type(type_hints[item.name])
        if item.default is MISSING and item.default_factory is MISSING:
            required.append(item.name)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": model.__name__,
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def catalog_schema_bundle() -> dict[str, Any]:
    return {kind: catalog_source_schema(kind) for kind in sorted(MODELS)}


__all__ = ["catalog_schema_bundle", "catalog_source_schema"]
