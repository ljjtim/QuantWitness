"""不接受 SQL 的类型化 Query IR。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import QueryIRInvalidError, QueryUnboundedError


QUERY_IR_VERSION = "query-ir-v1"
QUERY_IR_V2_VERSION = "query-ir-v2"
DATE_RANGE_V1_VERSION = "date-range-v1"
INSTANT_RANGE_V2_VERSION = "instant-range-v2"
MINUTE_TIMEZONE = "Asia/Shanghai"
_MINUTE_INSTANT = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$"
)


class QueryPurpose(str, Enum):
    FEATURE = "feature"
    LABEL = "label"
    UNIVERSE = "universe"
    AUDIT = "audit"


class FilterOperator(str, Enum):
    EQ = "eq"
    IN = "in"
    RANGE = "range"
    IS_NULL = "is_null"


def _scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise QueryIRInvalidError("datetime 必须带时区")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    raise QueryIRInvalidError(f"filter 值类型不受支持: {type(value).__name__}")


@dataclass(frozen=True)
class QueryBudget:
    max_rows: int
    max_bytes: int
    batch_size: int = 65_536

    def __post_init__(self) -> None:
        if self.max_rows <= 0 or self.max_bytes <= 0 or self.batch_size <= 0:
            raise QueryIRInvalidError("查询预算必须为正整数")

    def to_dict(self) -> dict[str, int]:
        return {
            "max_rows": self.max_rows,
            "max_bytes": self.max_bytes,
            "batch_size": self.batch_size,
        }


@dataclass(frozen=True)
class DateRangeV1:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise QueryIRInvalidError("时间范围 start 不能晚于 end")

    def to_dict(self) -> dict[str, str]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}


def _iana_zone(timezone_name: object, field: str = "timezone") -> ZoneInfo:
    if not isinstance(timezone_name, str) or not timezone_name.strip():
        raise QueryIRInvalidError(f"{field} 必须是非空 IANA timezone")
    try:
        return ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise QueryIRInvalidError(f"{field} 不是有效 IANA timezone") from exc


def normalize_instant(
    value: object,
    field: str,
    *,
    timezone_name: str,
) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise QueryIRInvalidError(f"{field} 必须是带时区 datetime")
    return value.astimezone(_iana_zone(timezone_name))


def normalize_minute_instant(value: object, field: str) -> datetime:
    """中国分钟兼容包装；通用 InstantRange 不在这里固定市场时区。"""

    return normalize_instant(value, field, timezone_name=MINUTE_TIMEZONE)


def parse_minute_instant(value: object, field: str) -> datetime:
    return parse_instant(value, field, timezone_name=MINUTE_TIMEZONE)


def parse_instant(
    value: object,
    field: str,
    *,
    timezone_name: str,
) -> datetime:
    if not isinstance(value, str) or _MINUTE_INSTANT.fullmatch(value) is None:
        raise QueryIRInvalidError(
            f"{field} 必须是带偏移、秒到微秒精度的 ISO 时间"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise QueryIRInvalidError(f"{field} 不是有效 ISO 时间") from exc
    return normalize_instant(parsed, field, timezone_name=timezone_name)


def parse_aware_datetime(value: object, field: str) -> datetime:
    """解析不改写时区的通用时点。"""

    if not isinstance(value, str) or not value.strip():
        raise QueryIRInvalidError(f"{field} 必须是带时区 ISO 时间")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise QueryIRInvalidError(f"{field} 不是有效 ISO 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QueryIRInvalidError(f"{field} 必须带时区")
    return parsed


def resolve_as_of_cutoff(
    value: object,
    *,
    reference_clock: datetime,
    field: str = "as_of",
) -> datetime:
    """把 QueryIR as_of 转为可与固定时钟比较的排他截止时点。

    日期表示“截至该本地日”，因此使用固定时钟的本地时区，并返回
    次日 00:00 的排他边界。带时区时间则保留原绝对时点。
    """

    if reference_clock.tzinfo is None or reference_clock.utcoffset() is None:
        raise QueryIRInvalidError("reference_clock 必须带时区")
    if not isinstance(value, str) or not value.strip():
        raise QueryIRInvalidError(f"{field} 必须是 ISO 日期或带时区时间")
    if "T" in value:
        return parse_aware_datetime(value, field)
    try:
        cutoff_date = date.fromisoformat(value)
    except ValueError as exc:
        raise QueryIRInvalidError(f"{field} 不是有效 ISO 日期") from exc
    return datetime.combine(
        cutoff_date + timedelta(days=1),
        time.min,
        tzinfo=reference_clock.tzinfo,
    )


def canonical_minute_instant(value: datetime) -> str:
    return canonical_instant(
        value,
        timezone_name=MINUTE_TIMEZONE,
        field="minute instant",
    )


def canonical_instant(
    value: datetime,
    *,
    timezone_name: str,
    field: str = "instant",
) -> str:
    return normalize_instant(value, field, timezone_name=timezone_name).isoformat(
        timespec="microseconds"
    )


def source_local_naive(value: datetime) -> datetime:
    """把规范分钟时点转换为物理 timestamp[us] 的本地墙钟参数。"""

    return normalize_minute_instant(value, "minute instant").replace(tzinfo=None)


@dataclass(frozen=True)
class InstantRangeV2:
    start_at: datetime
    end_at: datetime
    timezone: str = MINUTE_TIMEZONE
    contract_version: str = INSTANT_RANGE_V2_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != INSTANT_RANGE_V2_VERSION:
            raise QueryIRInvalidError("InstantRangeV2 contract_version 不受支持")
        _iana_zone(self.timezone)
        start = normalize_instant(
            self.start_at,
            "start_at",
            timezone_name=self.timezone,
        )
        end = normalize_instant(
            self.end_at,
            "end_at",
            timezone_name=self.timezone,
        )
        if start >= end:
            raise QueryIRInvalidError("分钟范围必须是非空半开区间")
        object.__setattr__(self, "start_at", start)
        object.__setattr__(self, "end_at", end)

    @property
    def start(self) -> datetime:
        return self.start_at

    @property
    def end(self) -> datetime:
        return self.end_at

    def to_dict(self) -> dict[str, str]:
        return {
            "contract_version": self.contract_version,
            "start_at": canonical_instant(
                self.start_at,
                timezone_name=self.timezone,
                field="start_at",
            ),
            "end_at": canonical_instant(
                self.end_at,
                timezone_name=self.timezone,
                field="end_at",
            ),
            "timezone": self.timezone,
        }

    @classmethod
    def from_dict(cls, value: object) -> "InstantRangeV2":
        if not isinstance(value, dict):
            raise QueryIRInvalidError("InstantRangeV2 必须是 mapping")
        expected = {"contract_version", "start_at", "end_at", "timezone"}
        if set(value) != expected:
            raise QueryIRInvalidError("InstantRangeV2 schema 不匹配")
        timezone_name = value["timezone"]
        _iana_zone(timezone_name)
        return cls(
            parse_instant(
                value["start_at"],
                "start_at",
                timezone_name=timezone_name,
            ),
            parse_instant(
                value["end_at"],
                "end_at",
                timezone_name=timezone_name,
            ),
            timezone_name,
            str(value["contract_version"]),
        )


@dataclass(frozen=True)
class UniverseSelection:
    instruments: tuple[str, ...] = ()
    snapshot_id: str | None = None

    def __post_init__(self) -> None:
        normalized = tuple(sorted(set(self.instruments)))
        if any(not item.strip() for item in normalized):
            raise QueryIRInvalidError("Universe 证券代码不能为空")
        if bool(normalized) == bool(self.snapshot_id):
            raise QueryIRInvalidError("Universe 必须且只能提供代码集合或 snapshot_id")
        object.__setattr__(self, "instruments", normalized)

    def to_dict(self) -> dict[str, object]:
        return {
            "instruments": list(self.instruments),
            "snapshot_id": self.snapshot_id,
        }


@dataclass(frozen=True)
class FilterPredicate:
    field_id: str
    operator: FilterOperator
    values: tuple[Any, ...]

    def __post_init__(self) -> None:
        if not self.field_id:
            raise QueryIRInvalidError("filter field_id 不能为空")
        values = tuple(_scalar(item) for item in self.values)
        if self.operator == FilterOperator.EQ and len(values) != 1:
            raise QueryIRInvalidError("eq filter 必须有一个值")
        if self.operator == FilterOperator.IN and not values:
            raise QueryIRInvalidError("in filter 不能为空")
        if self.operator == FilterOperator.RANGE and len(values) != 2:
            raise QueryIRInvalidError("range filter 必须有两个值")
        if self.operator == FilterOperator.IS_NULL and len(values) != 1:
            raise QueryIRInvalidError("is_null filter 必须有一个 boolean")
        if self.operator == FilterOperator.IS_NULL and not isinstance(values[0], bool):
            raise QueryIRInvalidError("is_null filter 的值必须是 boolean")
        if self.operator == FilterOperator.IN:
            values = tuple(
                sorted(set(values), key=lambda item: (type(item).__name__, repr(item)))
            )
        object.__setattr__(self, "values", values)

    def to_dict(self) -> dict[str, object]:
        return {
            "field_id": self.field_id,
            "operator": self.operator.value,
            "values": list(self.values),
        }


@dataclass(frozen=True)
class SortKey:
    field_id: str
    descending: bool = False

    def to_dict(self) -> dict[str, object]:
        return {"field_id": self.field_id, "descending": self.descending}


@dataclass(frozen=True)
class QueryIR:
    dataset_id: str
    dataset_version: int
    field_ids: tuple[str, ...]
    purpose: QueryPurpose
    time_range: DateRangeV1 | InstantRangeV2
    universe: UniverseSelection
    filters: tuple[FilterPredicate, ...]
    sort: tuple[SortKey, ...]
    budget: QueryBudget
    adjustment: str = "none"
    limit: int | None = None
    as_of: str | None = None
    ir_version: str = QUERY_IR_VERSION

    def __post_init__(self) -> None:
        if not self.dataset_id or self.dataset_version <= 0:
            raise QueryIRInvalidError("dataset 身份非法")
        if not self.field_ids:
            raise QueryIRInvalidError("投影字段不能为空")
        if len(set(self.field_ids)) != len(self.field_ids):
            raise QueryIRInvalidError("投影字段不能重复")
        if not self.sort:
            raise QueryIRInvalidError("必须显式声明稳定排序")
        if self.limit is not None and self.limit <= 0:
            raise QueryIRInvalidError("limit 必须为正整数")
        if self.adjustment not in {"none", "unadjusted", "raw", "pre", "post"}:
            raise QueryIRInvalidError("adjustment 不受支持")
        if self.time_range is None or self.universe is None:
            raise QueryUnboundedError("查询必须同时提供时间和 Universe 边界")
        if isinstance(self.time_range, DateRangeV1):
            if self.ir_version != QUERY_IR_VERSION:
                raise QueryIRInvalidError("DateRangeV1 只能用于 Query IR v1")
            if self.as_of is None:
                return
            try:
                as_of_date = date.fromisoformat(self.as_of)
            except (TypeError, ValueError) as exc:
                raise QueryIRInvalidError("as_of 必须是 ISO 日期") from exc
            if self.time_range.end > as_of_date:
                raise QueryIRInvalidError("查询结束日不能晚于 as_of")
            return
        if isinstance(self.time_range, InstantRangeV2):
            if self.ir_version != QUERY_IR_V2_VERSION:
                raise QueryIRInvalidError("InstantRangeV2 只能用于 Query IR v2")
            if self.as_of is None:
                raise QueryIRInvalidError("分钟 Query IR 必须声明带时区 as_of")
            as_of = parse_instant(
                self.as_of,
                "as_of",
                timezone_name=self.time_range.timezone,
            )
            object.__setattr__(
                self,
                "as_of",
                canonical_instant(
                    as_of,
                    timezone_name=self.time_range.timezone,
                    field="as_of",
                ),
            )
            return
        raise QueryIRInvalidError("time_range 合同类型不受支持")

    @property
    def as_of_instant(self) -> datetime | None:
        if not isinstance(self.time_range, InstantRangeV2) or self.as_of is None:
            return None
        return parse_instant(
            self.as_of,
            "as_of",
            timezone_name=self.time_range.timezone,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "ir_version": self.ir_version,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "field_ids": list(self.field_ids),
            "purpose": self.purpose.value,
            "time_range": self.time_range.to_dict(),
            "universe": self.universe.to_dict(),
            "filters": [item.to_dict() for item in self.filters],
            "sort": [item.to_dict() for item in self.sort],
            "budget": self.budget.to_dict(),
            "adjustment": self.adjustment,
            "limit": self.limit,
            "as_of": self.as_of,
        }


__all__ = [
    "DATE_RANGE_V1_VERSION",
    "DateRangeV1",
    "FilterOperator",
    "FilterPredicate",
    "INSTANT_RANGE_V2_VERSION",
    "InstantRangeV2",
    "MINUTE_TIMEZONE",
    "QUERY_IR_V2_VERSION",
    "QUERY_IR_VERSION",
    "QueryBudget",
    "QueryIR",
    "QueryPurpose",
    "SortKey",
    "UniverseSelection",
    "parse_aware_datetime",
    "resolve_as_of_cutoff",
]
