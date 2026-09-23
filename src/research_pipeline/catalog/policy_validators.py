"""Catalog policy payload 的受信静态 validator 注册表。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

from .errors import CatalogParseError


PolicyValidator = Callable[[Mapping[str, Any], Mapping[str, Any]], None]
_VALIDATORS: dict[str, PolicyValidator] = {}


def register_policy_validator(validator_id: str, validator: PolicyValidator) -> None:
    if not isinstance(validator_id, str) or not validator_id.strip():
        raise CatalogParseError("policy validator_id 必须是非空字符串")
    if not callable(validator):
        raise CatalogParseError("policy validator 必须可调用")
    existing = _VALIDATORS.get(validator_id)
    if existing is not None and existing is not validator:
        raise CatalogParseError(f"policy validator 重复注册: {validator_id}")
    _VALIDATORS[validator_id] = validator


def validate_policy_payload(
    validator_id: str | None,
    rules: Mapping[str, Any],
    applicability: Mapping[str, Any],
) -> None:
    if validator_id is None:
        return
    validator = _VALIDATORS.get(validator_id)
    if validator is None:
        raise CatalogParseError(f"未注册 policy validator: {validator_id}")
    validator(MappingProxyType(dict(rules)), MappingProxyType(dict(applicability)))


__all__ = [
    "PolicyValidator",
    "register_policy_validator",
    "validate_policy_payload",
]
