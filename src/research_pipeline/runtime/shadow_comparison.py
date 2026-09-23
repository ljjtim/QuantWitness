"""旧执行路径与统一 Runtime 路径的严格 shadow 等价门禁。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from research_pipeline.platform import canonical_json, typed_canonical_hash

from .errors import RuntimeIntegrityError


SHADOW_COMPARISON_VERSION = "research-runtime-shadow-comparison-v1"
SHADOW_WHITELIST_VERSION = "research-runtime-shadow-whitelist-v1"
MONEY_ABSOLUTE_TOLERANCE = Decimal("1e-8")
RETURN_ABSOLUTE_TOLERANCE = Decimal("1e-12")


@dataclass(frozen=True)
class ShadowWhitelistEntry:
    field: str
    difference_kind: str
    reviewer: str
    reason: str
    expires_on: date

    def __post_init__(self) -> None:
        if any(not value for value in (self.field, self.difference_kind, self.reviewer, self.reason)):
            raise RuntimeIntegrityError("shadow whitelist 条目字段不完整")

    def to_dict(self) -> dict[str, str]:
        return {
            "field": self.field,
            "difference_kind": self.difference_kind,
            "reviewer": self.reviewer,
            "reason": self.reason,
            "expires_on": self.expires_on.isoformat(),
        }


@dataclass(frozen=True)
class ShadowComparison:
    old_result_hash: str
    new_result_hash: str
    differences: tuple[Mapping[str, object], ...]
    approved_differences: tuple[Mapping[str, object], ...]
    status: str
    comparison_hash: str
    contract_version: str = SHADOW_COMPARISON_VERSION

    def __post_init__(self) -> None:
        if self.status not in {"pass", "fail"} or self.contract_version != SHADOW_COMPARISON_VERSION:
            raise RuntimeIntegrityError("shadow comparison 状态或版本无效")
        expected = "pass" if len(self.differences) == len(self.approved_differences) else "fail"
        if self.status != expected or self.comparison_hash != typed_canonical_hash(self.payload()):
            raise RuntimeIntegrityError("shadow comparison 结果或 hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "old_result_hash": self.old_result_hash,
            "new_result_hash": self.new_result_hash,
            "differences": [dict(item) for item in self.differences],
            "approved_differences": [dict(item) for item in self.approved_differences],
            "status": self.status,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "comparison_hash": self.comparison_hash}

    def write(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise RuntimeIntegrityError("shadow comparison 输出必须不存在")
        target.write_text(canonical_json(self.to_dict()), encoding="utf-8")


def compare_shadow_results(
    old: Mapping[str, object],
    new: Mapping[str, object],
    *,
    whitelist: tuple[ShadowWhitelistEntry, ...] = (),
    as_of: date,
) -> ShadowComparison:
    _validate_result(old, "old")
    _validate_result(new, "new")
    differences: list[dict[str, object]] = []
    for field in ("order_count", "ledger_count", "schema_hashes", "artifact_hashes"):
        if old[field] != new[field]:
            differences.append({
                "field": field,
                "difference_kind": "exact_mismatch",
                "old": old[field],
                "new": new[field],
            })
    for family, tolerance, kind in (
        ("money_values", MONEY_ABSOLUTE_TOLERANCE, "money_tolerance_exceeded"),
        ("return_values", RETURN_ABSOLUTE_TOLERANCE, "return_tolerance_exceeded"),
    ):
        old_values = old[family]
        new_values = new[family]
        assert isinstance(old_values, Mapping) and isinstance(new_values, Mapping)
        if set(old_values) != set(new_values):
            differences.append({
                "field": family,
                "difference_kind": "key_set_mismatch",
                "old": sorted(str(item) for item in old_values),
                "new": sorted(str(item) for item in new_values),
            })
            continue
        for name in sorted(old_values):
            delta = abs(_decimal(new_values[name], f"{family}.{name}") - _decimal(old_values[name], f"{family}.{name}"))
            if delta > tolerance:
                differences.append({
                    "field": f"{family}.{name}",
                    "difference_kind": kind,
                    "old": str(old_values[name]),
                    "new": str(new_values[name]),
                    "absolute_delta": str(delta),
                    "tolerance": str(tolerance),
                })
    whitelist_map = {}
    for item in whitelist:
        key = (item.field, item.difference_kind)
        if key in whitelist_map or item.expires_on < as_of:
            raise RuntimeIntegrityError("shadow whitelist 重复或已过期")
        whitelist_map[key] = item
    approved = []
    for difference in differences:
        entry = whitelist_map.get((str(difference["field"]), str(difference["difference_kind"])))
        if entry is not None:
            approved.append({**difference, "approval": entry.to_dict()})
    old_hash = typed_canonical_hash(dict(old))
    new_hash = typed_canonical_hash(dict(new))
    status = "pass" if len(approved) == len(differences) else "fail"
    payload = {
        "old_result_hash": old_hash,
        "new_result_hash": new_hash,
        "differences": differences,
        "approved_differences": approved,
        "status": status,
        "contract_version": SHADOW_COMPARISON_VERSION,
    }
    return ShadowComparison(
        old_hash,
        new_hash,
        tuple(differences),
        tuple(approved),
        status,
        typed_canonical_hash(payload),
    )


def shadow_whitelist_schema() -> dict[str, object]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Runtime Shadow Whitelist",
        "type": "object",
        "additionalProperties": False,
        "required": ["contract_version", "entries"],
        "properties": {
            "contract_version": {"const": SHADOW_WHITELIST_VERSION},
            "entries": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["field", "difference_kind", "reviewer", "reason", "expires_on"],
                    "properties": {
                        "field": {"type": "string", "minLength": 1},
                        "difference_kind": {"type": "string", "minLength": 1},
                        "reviewer": {"type": "string", "minLength": 1},
                        "reason": {"type": "string", "minLength": 1},
                        "expires_on": {"type": "string", "format": "date"},
                    },
                },
            },
        },
    }


def _validate_result(result: Mapping[str, object], label: str) -> None:
    expected = {"order_count", "ledger_count", "schema_hashes", "artifact_hashes", "money_values", "return_values"}
    if set(result) != expected or type(result["order_count"]) is not int or type(result["ledger_count"]) is not int:
        raise RuntimeIntegrityError(f"shadow {label} result schema 无效")
    for field in ("schema_hashes", "artifact_hashes", "money_values", "return_values"):
        value = result[field]
        if not isinstance(value, Mapping) or any(not isinstance(key, str) or not key for key in value):
            raise RuntimeIntegrityError(f"shadow {label}.{field} 无效")


def _decimal(value: object, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeIntegrityError(f"shadow {field} 不是有效数值") from exc
    if not number.is_finite():
        raise RuntimeIntegrityError(f"shadow {field} 不是有限数值")
    return number


__all__ = [
    "MONEY_ABSOLUTE_TOLERANCE",
    "RETURN_ABSOLUTE_TOLERANCE",
    "SHADOW_COMPARISON_VERSION",
    "SHADOW_WHITELIST_VERSION",
    "ShadowComparison",
    "ShadowWhitelistEntry",
    "compare_shadow_results",
    "shadow_whitelist_schema",
]
