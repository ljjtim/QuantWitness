"""字段族的确定性展开。"""

from __future__ import annotations

import re
from typing import Any

from .errors import CatalogFinancialSemanticsError, CatalogParseError
from .models import FieldContract, FieldFamilySource


def expand_field_family(family: FieldFamilySource, physical_columns: tuple[str, ...]) -> tuple[FieldContract, ...]:
    if str(family.defaults.get("semantic_type", "")) == "financial":
        raise CatalogFinancialSemanticsError("财务字段族禁止用公共模板代替逐字段语义")
    pattern = re.compile(family.column_regex)
    matched: list[tuple[str, re.Match[str]]] = []
    for column in physical_columns:
        match = pattern.fullmatch(column)
        if match and column not in family.exclude:
            matched.append((column, match))
    if len(matched) != family.expected_count:
        raise CatalogParseError(f"字段族 {family.family_id} 期望 {family.expected_count} 列，实际 {len(matched)} 列")
    generated: list[FieldContract] = []
    seen_names: set[str] = set()
    for column, match in sorted(matched):
        variables = {"column": column, **match.groupdict()}
        logical_name = family.logical_name_template.format(**variables)
        values: dict[str, Any] = dict(family.defaults)
        values.update(dict(family.overrides.get(column, {})))
        field_id_template = str(values.pop("field_id_template", ""))
        if not field_id_template:
            raise CatalogParseError("字段族 defaults 必须提供 field_id_template")
        field_id = field_id_template.format(**variables)
        if logical_name in seen_names:
            raise CatalogParseError(f"字段族生成重复逻辑名: {logical_name}")
        seen_names.add(logical_name)
        generated.append(FieldContract(field_id=field_id, logical_name=logical_name, **values))
    return tuple(generated)


__all__ = ["expand_field_family"]
