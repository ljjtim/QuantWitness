"""研究计划与治理身份使用的版本化 canonical codec。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import hashlib
import json
import math

from .errors import CanonicalEncodingError


CANONICAL_JSON_V1_VERSION = "canonical-json-v1"
TYPED_CANONICAL_V1_VERSION = "canonical-codec-v2-v1"


def canonical_json(value: object) -> str:
    """保持历史计划/快照口径的稳定 JSON，不额外注入版本字段。"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def fingerprint(value: object) -> str:
    """按 canonical JSON v1 计算 sha256。"""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def typed_canonical_bytes(value: object) -> bytes:
    """按带类型标签的治理 codec 编码，拒绝歧义输入。"""
    payload = {
        "codec_version": TYPED_CANONICAL_V1_VERSION,
        "value": _typed_canonical_value(value),
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def typed_canonical_hash(value: object) -> str:
    """按带类型标签的治理 codec 计算 sha256。"""
    return hashlib.sha256(typed_canonical_bytes(value)).hexdigest()


def typed_canonical_hash_streamed(value: object) -> str:
    """按同一治理编码逐段摘要，允许列表字段用一次性迭代器交付。"""
    digest = hashlib.sha256()
    digest.update(b'{"codec_version":')
    digest.update(canonical_json(TYPED_CANONICAL_V1_VERSION).encode("utf-8"))
    digest.update(b',"value":')
    for part in _typed_canonical_parts(value):
        digest.update(part.encode("utf-8"))
    digest.update(b"}")
    return digest.hexdigest()


def _typed_canonical_parts(value: object) -> Iterator[str]:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise CanonicalEncodingError("canonical payload 的 mapping key 必须是字符串")
        yield '{"type":"object","value":{'
        for position, key in enumerate(sorted(value)):
            if position:
                yield ","
            yield canonical_json(key)
            yield ":"
            yield from _typed_canonical_parts(value[key])
        yield "}}"
    elif isinstance(value, (list, tuple, Iterator)):
        yield '{"type":"list","value":['
        for position, item in enumerate(value):
            if position:
                yield ","
            yield from _typed_canonical_parts(item)
        yield "]}"
    else:
        yield canonical_json(_typed_canonical_value(value))


def _typed_canonical_value(value: object) -> object:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalEncodingError("canonical payload 不接受 NaN 或 Inf")
        return {"type": "float", "value": value.hex()}
    if isinstance(value, str):
        return {"type": "str", "value": value}
    if isinstance(value, (list, tuple)):
        return {
            "type": "list",
            "value": [_typed_canonical_value(item) for item in value],
        }
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise CanonicalEncodingError(
                "canonical payload 的 mapping key 必须是字符串"
            )
        return {
            "type": "object",
            "value": {
                key: _typed_canonical_value(value[key]) for key in sorted(value)
            },
        }
    raise CanonicalEncodingError(
        f"canonical payload 不支持类型: {type(value).__name__}"
    )


__all__ = [
    "CANONICAL_JSON_V1_VERSION",
    "TYPED_CANONICAL_V1_VERSION",
    "canonical_json",
    "fingerprint",
    "typed_canonical_bytes",
    "typed_canonical_hash",
]
