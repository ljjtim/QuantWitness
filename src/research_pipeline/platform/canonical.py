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
    # 先保持原有拒绝顺序，再编码，避免 UTF-8 错误提前遮住非法输入。
    _validate_typed_canonical_value(value)
    return _typed_canonical_digest(value, allow_iterators=False)


def typed_canonical_hash_streamed(value: object) -> str:
    """按同一治理编码逐段摘要，允许列表字段用一次性迭代器交付。"""
    return _typed_canonical_digest(value, allow_iterators=True)


def _typed_canonical_digest(value: object, *, allow_iterators: bool) -> str:
    digest = hashlib.sha256()
    digest.update(b'{"codec_version":')
    digest.update(canonical_json(TYPED_CANONICAL_V1_VERSION).encode("utf-8"))
    digest.update(b',"value":')
    for part in _TypedCanonicalEncoder(allow_iterators=allow_iterators).parts(value):
        digest.update(part.encode("utf-8"))
    digest.update(b"}")
    return digest.hexdigest()


# 快路径和单次来源快照均有固定上限，不随行数或不同来源数增长。
_SCALAR_BATCH_MAX_ITEMS = 256
_SCALAR_BATCH_MAX_CHARACTERS = 16384
_SCALAR_MAPPING_MAX_ITEMS = 64
_SOURCE_CACHE_MAX_CHARACTERS = 65536
_SCALAR_TOKEN_CACHE_MAX_ITEMS = 2048
_SCALAR_TOKEN_MAX_CHARACTERS = 256
_SCALAR_TOKEN_MAX_INTEGER_BITS = 256


def _typed_canonical_parts(value: object) -> Iterator[str]:
    yield from _TypedCanonicalEncoder().parts(value)


class _TypedCanonicalEncoder:
    """一次编码内有界复用标量 token 与最近一个来源列表。"""

    def __init__(self, *, allow_iterators: bool = True) -> None:
        self._allow_iterators = allow_iterators
        self._source_snapshot: tuple[str, ...] | None = None
        self._source_part: str | None = None
        self._scalar_tokens: dict[tuple[type, str | int], str] = {}

    def parts(self, value: object) -> Iterator[str]:
        if isinstance(value, Mapping):
            if any(not isinstance(key, str) for key in value):
                raise CanonicalEncodingError("canonical payload 的 mapping key 必须是字符串")
            if len(value) <= _SCALAR_MAPPING_MAX_ITEMS and all(
                _scalar_or_scalar_sequence(item) for item in value.values()
            ):
                yield self._mapping_part(value)
                return
            yield '{"type":"object","value":{'
            for position, key in enumerate(sorted(value)):
                if position:
                    yield ","
                yield canonical_json(key)
                yield ":"
                yield from self.parts(value[key])
            yield "}}"
        elif isinstance(value, (list, tuple)) and _scalar_or_scalar_sequence(value):
            yield self._sequence_part(value)
        elif isinstance(value, (list, tuple)) or (self._allow_iterators and isinstance(value, Iterator)):
            yield '{"type":"list","value":['
            for position, item in enumerate(value):
                if position:
                    yield ","
                yield from self.parts(item)
            yield "]}"
        else:
            yield canonical_json(_typed_canonical_value(value))

    def _sequence_part(self, value: list | tuple) -> str:
        if not value or not all(type(item) is str for item in value):
            return '{"type":"list","value":[' + ",".join(
                self._scalar_part(item) for item in value
            ) + "]}"
        snapshot = tuple(value)
        if snapshot == self._source_snapshot:
            return self._source_part
        part = canonical_json(_typed_canonical_value(value))
        if len(part) <= _SOURCE_CACHE_MAX_CHARACTERS:
            self._source_snapshot = snapshot
            self._source_part = part
        return part

    def _scalar_part(self, value: object) -> str:
        value_type = type(value)
        cacheable = (
            value_type is str and len(value) <= _SCALAR_TOKEN_MAX_CHARACTERS
        ) or (
            value_type is int and value.bit_length() <= _SCALAR_TOKEN_MAX_INTEGER_BITS
        )
        if not cacheable:
            return canonical_json(_typed_canonical_value(value))
        # 精确类型防止 bool 与 int 合并；浮点始终保留原编码中的正负零。
        key = (value_type, value)
        token = self._scalar_tokens.get(key)
        if token is None:
            token = canonical_json(_typed_canonical_value(value))
            if len(self._scalar_tokens) >= _SCALAR_TOKEN_CACHE_MAX_ITEMS:
                self._scalar_tokens.clear()
            self._scalar_tokens[key] = token
        return token

    def _mapping_part(self, value: Mapping) -> str:
        # 连续标量字段仍一次 JSON 编码，来源列表直接拼入已核对的 typed 字节。
        groups: list[dict | tuple[str, str]] = []
        scalars: dict[str, object] = {}
        for key in sorted(value):
            item = value[key]
            if isinstance(item, (list, tuple)):
                if scalars:
                    groups.append(scalars)
                    scalars = {}
                groups.append((key, self._sequence_part(item)))
            else:
                scalars[key] = _typed_canonical_value(item)
        if not groups:
            return canonical_json({"type": "object", "value": scalars})
        if scalars:
            groups.append(scalars)
        fragments = [
            canonical_json(group)[1:-1] if isinstance(group, dict)
            else canonical_json(group[0]) + ":" + group[1]
            for group in groups
        ]
        return '{"type":"object","value":{' + ",".join(fragments) + "}}"


def _scalar_or_scalar_sequence(value: object) -> bool:
    """只把有界标量集合合为一段，不消费一次性迭代器。"""
    scalar_types = (bool, int, float, str)
    if value is None or isinstance(value, scalar_types):
        return True
    if not isinstance(value, (list, tuple)) or len(value) > _SCALAR_BATCH_MAX_ITEMS:
        return False
    characters = 0
    for item in value:
        if item is not None and not isinstance(item, scalar_types):
            return False
        if isinstance(item, str):
            characters += len(item)
            if characters > _SCALAR_BATCH_MAX_CHARACTERS:
                return False
    return True


def _validate_typed_canonical_value(value: object) -> None:
    """按普通编码的顺序验证，不构造容器的 typed 副本。"""
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _validate_typed_canonical_value(item)
    elif isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise CanonicalEncodingError("canonical payload 的 mapping key 必须是字符串")
        for key in sorted(value):
            _validate_typed_canonical_value(value[key])
    else:
        _typed_canonical_value(value)


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
