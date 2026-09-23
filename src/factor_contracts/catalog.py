"""因子配方与来源正文的唯一序列化和摘要算法。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any


FACTOR_CATALOG_SNAPSHOT_VERSION = "factor-catalog-snapshot-v1"


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _contract_dict(value: object, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        payload = dict(value)
    else:
        to_dict = getattr(value, "to_dict", None)
        if not callable(to_dict):
            raise TypeError(f"{label} 必须是 mapping 或提供 to_dict()")
        payload = to_dict()
    if not isinstance(payload, dict):
        raise TypeError(f"{label}.to_dict() 必须返回 dict")
    return payload


def factor_catalog_payload(
    recipes: Iterable[object],
    sources: Iterable[object],
) -> dict[str, object]:
    return {
        "recipes": [_contract_dict(item, "recipe") for item in recipes],
        "sources": [_contract_dict(item, "source") for item in sources],
    }


def factor_catalog_hash(
    recipes: Iterable[object],
    sources: Iterable[object],
) -> str:
    encoded = canonical_json(factor_catalog_payload(recipes, sources)).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def factor_catalog_snapshot_payload(
    recipes: Iterable[object],
    sources: Iterable[object],
) -> dict[str, object]:
    return {
        "snapshot_version": FACTOR_CATALOG_SNAPSHOT_VERSION,
        **factor_catalog_payload(recipes, sources),
    }
