"""基础合同字段检查与共用文件操作。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
import hashlib
import os
from pathlib import Path
import re

from .canonical import canonical_json


_SHA256 = re.compile(r"[0-9a-f]{64}")


def require_text(value: object, field: str, *, error: type[Exception]) -> str:
    if not isinstance(value, str) or not value.strip():
        raise error(f"{field} 必须是非空字符串")
    return value


def require_sha256(value: object, field: str, *, error: type[Exception],
                   requirement: str = "必须是 sha256 小写摘要") -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise error(f"{field} {requirement}")
    return value


def require_mapping(value: object, field: str, *, error: type[Exception],
                    string_keys: bool = False, requirement: str = "必须是对象") -> Mapping[str, object]:
    if not isinstance(value, Mapping) or (string_keys and any(not isinstance(key, str) for key in value)):
        raise error(f"{field} {requirement}")
    return value


def require_exact_fields(value: Mapping[str, object], expected: set[str], field: str, *,
                         error: type[Exception]) -> None:
    actual = set(value)
    if actual != expected:
        raise error(f"{field} schema 不匹配；缺失={sorted(expected - actual)}，未知={sorted(actual - expected)}")


def require_integer(value: object, field: str, *, error: type[Exception]) -> int:
    if type(value) is not int:
        raise error(f"{field} 必须是整数")
    return value


def require_aware(value: object, field: str, *, error: type[Exception],
                  requirement: str = "必须是带时区时间") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise error(f"{field} {requirement}")
    return value


def parse_iso_datetime(value: object, field: str, *, error: type[Exception],
                       requirement: str = "必须是 ISO 时间") -> datetime:
    if not isinstance(value, str):
        raise error(f"{field} {requirement}")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise error(f"{field} {requirement}") from exc


def parse_iso_date(value: object, field: str, *, error: type[Exception]) -> date:
    if not isinstance(value, str):
        raise error(f"{field} 必须是 ISO 日期")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise error(f"{field} 必须是 ISO 日期") from exc


def parse_aware_seconds(value: object, field: str, *, error: type[Exception],
                        timezone_requirement: str = "必须带时区") -> datetime:
    parsed = parse_iso_datetime(value, field, error=error, requirement="必须是带时区 ISO 时间")
    require_aware(parsed, field, error=error, requirement=timezone_requirement)
    if value != parsed.isoformat(timespec="seconds"):
        raise error(f"{field} 必须使用规范秒精度 ISO 时间")
    return parsed


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_canonical_json_atomic(path: Path, payload: Mapping[str, object], *,
                                staging: Path, sync: bool = False) -> None:
    with staging.open("w", encoding="utf-8") as stream:
        stream.write(canonical_json(dict(payload)))
        if sync:
            stream.flush()
            os.fsync(stream.fileno())
    os.replace(staging, path)
