"""Catalog 时态规则编译后的唯一执行合同。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from itertools import groupby
import re
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import QueryIRInvalidError


TEMPORAL_SELECTION_PLAN_VERSION = "temporal-selection-plan-v4"
SESSION_CLOSE_SELECTION_VERSION = "session-close-selection-v2"
SELECTION_CLOCK_SOURCES = {
    "query_as_of",
    "consumer_observation_time",
    "consumer_decision_time",
    "event_time",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _field(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueryIRInvalidError(f"TemporalSelectionPlan {name} 必须是非空字段")
    return value


def _fields(value: object, name: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise QueryIRInvalidError(f"TemporalSelectionPlan {name} 必须是字段序列")
    result = tuple(_field(item, name) for item in value)
    if (not result and not allow_empty) or len(result) != len(set(result)):
        raise QueryIRInvalidError(f"TemporalSelectionPlan {name} 必须非空且唯一")
    return result


def _bound_time(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise QueryIRInvalidError("consumer selection time 必须带时区")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str) or not value.strip():
        raise QueryIRInvalidError("consumer selection time 必须是 ISO 日期或带时区时间")
    try:
        if "T" in value:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError
            return parsed.isoformat()
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise QueryIRInvalidError(
            "consumer selection time 必须是 ISO 日期或带时区时间"
        ) from exc


@dataclass(frozen=True)
class SelectionClock:
    source: str
    bound_time: str | None = None

    def __post_init__(self) -> None:
        if self.source not in SELECTION_CLOCK_SOURCES:
            raise QueryIRInvalidError("TemporalSelectionPlan selection clock 来源无效")
        if self.bound_time is not None:
            object.__setattr__(self, "bound_time", _bound_time(self.bound_time))
        if self.source in {"query_as_of", "event_time"} and self.bound_time is not None:
            raise QueryIRInvalidError("query/event selection clock 不接受外部绑定时点")

    @property
    def requires_consumer_binding(self) -> bool:
        return self.source in {
            "consumer_observation_time",
            "consumer_decision_time",
        }

    def bind(self, value: str | date | datetime) -> "SelectionClock":
        if not self.requires_consumer_binding:
            raise QueryIRInvalidError("当前 selection clock 不接受 consumer time")
        return replace(self, bound_time=_bound_time(value))

    def to_dict(self) -> dict[str, object]:
        return {"source": self.source, "bound_time": self.bound_time}

    @classmethod
    def from_dict(cls, payload: object) -> "SelectionClock":
        if not isinstance(payload, Mapping) or set(payload) != {"source", "bound_time"}:
            raise QueryIRInvalidError("selection_clock schema 不匹配")
        value = payload["bound_time"]
        if value is not None and not isinstance(value, str):
            raise QueryIRInvalidError("selection_clock bound_time 类型无效")
        return cls(str(payload["source"]), value)


@dataclass(frozen=True)
class VisibilityFilter:
    available_time_field: str
    inclusive: bool
    additional_time_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _field(self.available_time_field, "available_time_field")
        if type(self.inclusive) is not bool:
            raise QueryIRInvalidError("visibility inclusive 必须是 boolean")
        object.__setattr__(
            self,
            "additional_time_fields",
            _fields(
                self.additional_time_fields,
                "additional_time_fields",
                allow_empty=True,
            ),
        )
        if self.available_time_field in self.additional_time_fields:
            raise QueryIRInvalidError("visibility time fields 必须唯一")

    @property
    def time_fields(self) -> tuple[tuple[str, bool], ...]:
        return (
            (self.available_time_field, self.inclusive),
            *((field, True) for field in self.additional_time_fields),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "available_time_field": self.available_time_field,
            "inclusive": self.inclusive,
            "additional_time_fields": list(self.additional_time_fields),
        }

    @classmethod
    def from_dict(cls, payload: object) -> "VisibilityFilter":
        if not isinstance(payload, Mapping) or set(payload) != {
            "available_time_field", "inclusive", "additional_time_fields",
        }:
            raise QueryIRInvalidError("visibility_filter schema 不匹配")
        return cls(
            str(payload["available_time_field"]),
            payload["inclusive"],
            _fields(
                payload["additional_time_fields"],
                "additional_time_fields",
                allow_empty=True,
            ),
        )


@dataclass(frozen=True)
class RevisionSelector:
    entity_fields: tuple[str, ...]
    order_fields: tuple[str, ...]
    duplicate_winner: str = "reject"

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_fields", _fields(self.entity_fields, "entity_fields"))
        object.__setattr__(self, "order_fields", _fields(self.order_fields, "order_fields"))
        if self.duplicate_winner != "reject":
            raise QueryIRInvalidError("revision duplicate winner 必须拒绝")

    def to_dict(self) -> dict[str, object]:
        return {
            "entity_fields": list(self.entity_fields),
            "order_fields": list(self.order_fields),
            "duplicate_winner": self.duplicate_winner,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "RevisionSelector":
        if not isinstance(payload, Mapping) or set(payload) != {
            "entity_fields",
            "order_fields",
            "duplicate_winner",
        }:
            raise QueryIRInvalidError("revision_selector schema 不匹配")
        return cls(
            _fields(payload["entity_fields"], "entity_fields"),
            _fields(payload["order_fields"], "order_fields"),
            str(payload["duplicate_winner"]),
        )


@dataclass(frozen=True)
class EffectiveIntervalSelector:
    entity_fields: tuple[str, ...]
    effective_from_field: str
    effective_to_field: str
    left_closed: bool
    right_closed: bool
    overlap: str = "reject"

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_fields", _fields(self.entity_fields, "entity_fields"))
        _field(self.effective_from_field, "effective_from_field")
        _field(self.effective_to_field, "effective_to_field")
        if type(self.left_closed) is not bool or type(self.right_closed) is not bool:
            raise QueryIRInvalidError("interval endpoints 必须是 boolean")
        if self.overlap != "reject":
            raise QueryIRInvalidError("interval overlap 必须拒绝")

    def to_dict(self) -> dict[str, object]:
        return {
            "entity_fields": list(self.entity_fields),
            "effective_from_field": self.effective_from_field,
            "effective_to_field": self.effective_to_field,
            "left_closed": self.left_closed,
            "right_closed": self.right_closed,
            "overlap": self.overlap,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "EffectiveIntervalSelector":
        expected = {
            "entity_fields",
            "effective_from_field",
            "effective_to_field",
            "left_closed",
            "right_closed",
            "overlap",
        }
        if not isinstance(payload, Mapping) or set(payload) != expected:
            raise QueryIRInvalidError("effective_interval_selector schema 不匹配")
        return cls(
            _fields(payload["entity_fields"], "entity_fields"),
            str(payload["effective_from_field"]),
            str(payload["effective_to_field"]),
            payload["left_closed"],
            payload["right_closed"],
            str(payload["overlap"]),
        )


@dataclass(frozen=True)
class SessionCloseFact:
    instrument_id: str
    session_date: date
    completed_at: datetime

    def __post_init__(self) -> None:
        _field(self.instrument_id, "session-close instrument_id")
        if not isinstance(self.session_date, date) or isinstance(
            self.session_date, datetime
        ):
            raise QueryIRInvalidError("session-close session_date 必须是日期")
        if (
            not isinstance(self.completed_at, datetime)
            or self.completed_at.tzinfo is None
            or self.completed_at.utcoffset() is None
        ):
            raise QueryIRInvalidError("session-close completed_at 必须是带时区时点")

    def to_dict(self) -> dict[str, str]:
        return {
            "instrument_id": self.instrument_id,
            "session_date": self.session_date.isoformat(),
            "completed_at": self.completed_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionCloseFact":
        if not isinstance(value, Mapping) or set(value) != {
            "instrument_id",
            "session_date",
            "completed_at",
        }:
            raise QueryIRInvalidError("session-close fact schema 不匹配")
        try:
            session_date = date.fromisoformat(str(value["session_date"]))
            completed_at = datetime.fromisoformat(str(value["completed_at"]))
        except ValueError as exc:
            raise QueryIRInvalidError("session-close fact 时间无效") from exc
        return cls(str(value["instrument_id"]), session_date, completed_at)


@dataclass(frozen=True)
class SessionCloseInstrumentBinding:
    instrument_id: str
    market: str
    exchange: str
    product: str
    timezone: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("instrument_id", self.instrument_id),
            ("market", self.market),
            ("exchange", self.exchange),
            ("product", self.product),
            ("timezone", self.timezone),
        ):
            _field(value, f"session-close {field_name}")
        if self.market != "cn_future":
            raise QueryIRInvalidError("session-close market 只允许 cn_future")
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise QueryIRInvalidError("session-close timezone 无效") from exc

    def to_dict(self) -> dict[str, str]:
        return {
            "instrument_id": self.instrument_id,
            "market": self.market,
            "exchange": self.exchange,
            "product": self.product,
            "timezone": self.timezone,
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionCloseInstrumentBinding":
        expected = {"instrument_id", "market", "exchange", "product", "timezone"}
        if not isinstance(value, Mapping) or set(value) != expected:
            raise QueryIRInvalidError("session-close instrument binding schema 不匹配")
        return cls(
            str(value["instrument_id"]),
            str(value["market"]),
            str(value["exchange"]),
            str(value["product"]),
            str(value["timezone"]),
        )


@dataclass(frozen=True)
class SessionClosePolicyBinding:
    instrument_id: str
    session_policy_id: str
    session_policy_revision: int
    session_policy_hash: str
    coverage_start: date
    coverage_end: date
    trading_dates: tuple[date, ...]

    def __post_init__(self) -> None:
        _field(self.instrument_id, "session-close instrument_id")
        _field(self.session_policy_id, "session-close session_policy_id")
        if self.session_policy_revision < 1:
            raise QueryIRInvalidError("session-close policy revision 无效")
        if _SHA256.fullmatch(self.session_policy_hash) is None:
            raise QueryIRInvalidError("session-close session_policy_hash 必须是 sha256")
        if (
            not isinstance(self.coverage_start, date)
            or isinstance(self.coverage_start, datetime)
            or not isinstance(self.coverage_end, date)
            or isinstance(self.coverage_end, datetime)
            or self.coverage_start > self.coverage_end
        ):
            raise QueryIRInvalidError("session-close policy 覆盖区间无效")
        if (
            not self.trading_dates
            or self.trading_dates != tuple(sorted(set(self.trading_dates)))
            or self.trading_dates[0] != self.coverage_start
            or self.trading_dates[-1] != self.coverage_end
        ):
            raise QueryIRInvalidError(
                "session-close policy trading_dates 与覆盖区间不闭合"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "session_policy_id": self.session_policy_id,
            "session_policy_revision": self.session_policy_revision,
            "session_policy_hash": self.session_policy_hash,
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat(),
            "trading_dates": [item.isoformat() for item in self.trading_dates],
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionClosePolicyBinding":
        expected = {
            "instrument_id",
            "session_policy_id",
            "session_policy_revision",
            "session_policy_hash",
            "coverage_start",
            "coverage_end",
            "trading_dates",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise QueryIRInvalidError("session-close policy binding schema 不匹配")
        revision = value["session_policy_revision"]
        dates = value["trading_dates"]
        if type(revision) is not int or not isinstance(dates, list):
            raise QueryIRInvalidError("session-close policy binding 值类型无效")
        try:
            coverage_start = date.fromisoformat(str(value["coverage_start"]))
            coverage_end = date.fromisoformat(str(value["coverage_end"]))
            trading_dates = tuple(date.fromisoformat(str(item)) for item in dates)
        except ValueError as exc:
            raise QueryIRInvalidError("session-close policy binding 日期无效") from exc
        return cls(
            str(value["instrument_id"]),
            str(value["session_policy_id"]),
            revision,
            str(value["session_policy_hash"]),
            coverage_start,
            coverage_end,
            trading_dates,
        )


@dataclass(frozen=True)
class SessionCloseSelection:
    availability_policy_id: str
    availability_policy_hash: str
    bundle_id: str
    bundle_hash: str
    source_artifact_ref: str
    source_acquired_at: datetime
    instrument_field: str
    session_date_field: str
    instrument_bindings: tuple[SessionCloseInstrumentBinding, ...]
    policy_bindings: tuple[SessionClosePolicyBinding, ...]
    facts: tuple[SessionCloseFact, ...]
    contract_version: str = SESSION_CLOSE_SELECTION_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != SESSION_CLOSE_SELECTION_VERSION:
            raise QueryIRInvalidError("session-close selection 版本不受支持")
        for name, value in (
            ("availability_policy_id", self.availability_policy_id),
            ("bundle_id", self.bundle_id),
            ("source_artifact_ref", self.source_artifact_ref),
            ("instrument_field", self.instrument_field),
            ("session_date_field", self.session_date_field),
        ):
            _field(value, f"session-close {name}")
        for name, value in (
            ("availability_policy_hash", self.availability_policy_hash),
            ("bundle_hash", self.bundle_hash),
        ):
            if _SHA256.fullmatch(value) is None:
                raise QueryIRInvalidError(f"session-close {name} 必须是 sha256")
        if (
            self.source_acquired_at.tzinfo is None
            or self.source_acquired_at.utcoffset() is None
        ):
            raise QueryIRInvalidError("session-close source_acquired_at 必须带时区")
        bindings = tuple(
            sorted(self.instrument_bindings, key=lambda item: item.instrument_id)
        )
        policy_bindings = tuple(
            sorted(
                self.policy_bindings,
                key=lambda item: (
                    item.instrument_id,
                    item.coverage_start,
                    item.session_policy_id,
                    item.session_policy_revision,
                ),
            )
        )
        facts = tuple(
            sorted(self.facts, key=lambda item: (item.instrument_id, item.session_date))
        )
        if (
            not bindings
            or bindings != self.instrument_bindings
            or len({item.instrument_id for item in bindings}) != len(bindings)
        ):
            raise QueryIRInvalidError("session-close instrument bindings 必须非空、排序且唯一")
        if not policy_bindings or policy_bindings != self.policy_bindings:
            raise QueryIRInvalidError(
                "session-close policy bindings 必须非空且稳定排序"
            )
        policy_keys = {
            (
                item.session_policy_id,
                item.session_policy_revision,
            )
            for item in policy_bindings
        }
        if len(policy_keys) != len(policy_bindings):
            raise QueryIRInvalidError("session-close policy binding 身份重复")
        if not facts or facts != self.facts or len(
            {(item.instrument_id, item.session_date) for item in facts}
        ) != len(facts):
            raise QueryIRInvalidError("session-close facts 必须非空、排序且唯一")
        binding_by_instrument = {item.instrument_id: item for item in bindings}
        covered: set[tuple[str, date]] = set()
        for policy_binding in policy_bindings:
            if policy_binding.instrument_id not in binding_by_instrument:
                raise QueryIRInvalidError("session-close policy 缺少市场映射")
            for trading_date in policy_binding.trading_dates:
                key = (policy_binding.instrument_id, trading_date)
                if key in covered:
                    raise QueryIRInvalidError(
                        "session-close policy 对同一合约日期存在重复覆盖"
                    )
                covered.add(key)
        for fact in facts:
            binding = binding_by_instrument.get(fact.instrument_id)
            if binding is None:
                raise QueryIRInvalidError("session-close fact 缺少市场映射")
            if fact.completed_at.astimezone(ZoneInfo(binding.timezone)).date() != fact.session_date:
                raise QueryIRInvalidError("session-close completion 与交易日/时区不一致")
        if covered != {(item.instrument_id, item.session_date) for item in facts}:
            raise QueryIRInvalidError("session-close policy 覆盖与完成时刻不闭合")

    def completion_time(self, instrument_id: object, session_date: object) -> datetime:
        instrument = str(instrument_id)
        current = _as_source_date(session_date, None, self.session_date_field)
        matches = tuple(
            item
            for item in self.facts
            if item.instrument_id == instrument and item.session_date == current
        )
        if len(matches) != 1:
            raise QueryIRInvalidError("session-close 计划缺少日期、日历或合约映射覆盖")
        return matches[0].completed_at

    def is_visible(
        self,
        *,
        instrument_id: object,
        session_date: object,
        consumer_time: object,
    ) -> bool:
        if isinstance(consumer_time, str):
            try:
                consumer_time = datetime.fromisoformat(consumer_time)
            except ValueError as exc:
                raise QueryIRInvalidError(
                    "session-close consumer time 必须是带时区时点"
                ) from exc
        if (
            not isinstance(consumer_time, datetime)
            or consumer_time.tzinfo is None
            or consumer_time.utcoffset() is None
        ):
            raise QueryIRInvalidError("session-close consumer time 必须是带时区时点")
        return self.completion_time(instrument_id, session_date) <= consumer_time

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_policy_id": self.availability_policy_id,
            "availability_policy_hash": self.availability_policy_hash,
            "bundle_id": self.bundle_id,
            "bundle_hash": self.bundle_hash,
            "source_artifact_ref": self.source_artifact_ref,
            "source_acquired_at": self.source_acquired_at.isoformat(),
            "instrument_field": self.instrument_field,
            "session_date_field": self.session_date_field,
            "instrument_bindings": [item.to_dict() for item in self.instrument_bindings],
            "policy_bindings": [item.to_dict() for item in self.policy_bindings],
            "facts": [item.to_dict() for item in self.facts],
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> "SessionCloseSelection":
        expected = {
            "availability_policy_id",
            "availability_policy_hash",
            "bundle_id",
            "bundle_hash",
            "source_artifact_ref",
            "source_acquired_at",
            "instrument_field",
            "session_date_field",
            "instrument_bindings",
            "policy_bindings",
            "facts",
            "contract_version",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise QueryIRInvalidError("session-close selection schema 不匹配")
        try:
            source_acquired_at = datetime.fromisoformat(str(value["source_acquired_at"]))
        except ValueError as exc:
            raise QueryIRInvalidError("session-close source_acquired_at 无效") from exc
        bindings = value["instrument_bindings"]
        policy_bindings = value["policy_bindings"]
        facts = value["facts"]
        if (
            not isinstance(bindings, list)
            or not isinstance(policy_bindings, list)
            or not isinstance(facts, list)
        ):
            raise QueryIRInvalidError("session-close selection 值类型无效")
        return cls(
            str(value["availability_policy_id"]),
            str(value["availability_policy_hash"]),
            str(value["bundle_id"]),
            str(value["bundle_hash"]),
            str(value["source_artifact_ref"]),
            source_acquired_at,
            str(value["instrument_field"]),
            str(value["session_date_field"]),
            tuple(SessionCloseInstrumentBinding.from_dict(item) for item in bindings),
            tuple(SessionClosePolicyBinding.from_dict(item) for item in policy_bindings),
            tuple(SessionCloseFact.from_dict(item) for item in facts),
            str(value["contract_version"]),
        )


@dataclass(frozen=True)
class TemporalSelectionPlan:
    selection_clock: SelectionClock
    event_time_field: str
    required_scan_fields: tuple[str, ...]
    public_projection: tuple[str, ...]
    field_types: tuple[tuple[str, str], ...]
    source_timezone: str | None = None
    visibility_filter: VisibilityFilter | None = None
    revision_selector: RevisionSelector | None = None
    effective_interval_selector: EffectiveIntervalSelector | None = None
    session_close_selection: SessionCloseSelection | None = None
    contract_version: str = TEMPORAL_SELECTION_PLAN_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != TEMPORAL_SELECTION_PLAN_VERSION:
            raise QueryIRInvalidError("TemporalSelectionPlan 版本不受支持")
        _field(self.event_time_field, "event_time_field")
        scan = _fields(self.required_scan_fields, "required_scan_fields")
        public = _fields(self.public_projection, "public_projection")
        field_types = tuple((str(field), str(kind).lower()) for field, kind in self.field_types)
        object.__setattr__(self, "required_scan_fields", scan)
        object.__setattr__(self, "public_projection", public)
        object.__setattr__(self, "field_types", field_types)
        if not set(public) <= set(scan) or self.event_time_field not in scan:
            raise QueryIRInvalidError("TemporalSelectionPlan scan/public projection 不闭合")
        if (
            len(field_types) != len(scan)
            or tuple(field for field, _ in field_types) != scan
        ):
            raise QueryIRInvalidError("TemporalSelectionPlan 字段类型与 scan projection 不闭合")
        if self.source_timezone is not None:
            try:
                ZoneInfo(self.source_timezone)
            except (ZoneInfoNotFoundError, ValueError) as exc:
                raise QueryIRInvalidError(
                    "TemporalSelectionPlan source_timezone 不是有效 IANA 时区"
                ) from exc
        temporal_fields = set()
        if self.visibility_filter is not None:
            temporal_fields.update(
                field for field, _inclusive in self.visibility_filter.time_fields
            )
        if self.revision_selector is not None:
            temporal_fields.update(self.revision_selector.entity_fields)
            temporal_fields.update(self.revision_selector.order_fields)
        if self.effective_interval_selector is not None:
            temporal_fields.update(self.effective_interval_selector.entity_fields)
            temporal_fields.update(
                (
                    self.effective_interval_selector.effective_from_field,
                    self.effective_interval_selector.effective_to_field,
                )
            )
        if self.session_close_selection is not None:
            temporal_fields.update(
                (
                    self.session_close_selection.instrument_field,
                    self.session_close_selection.session_date_field,
                )
            )
        if not temporal_fields <= set(scan):
            raise QueryIRInvalidError("TemporalSelectionPlan 缺少执行所需隐藏列")
        timestamp_fields = {
            field
            for field in temporal_fields | {self.event_time_field}
            if self._field_type(field) in {"timestamp", "timestamp[us]"}
        }
        if timestamp_fields and self.source_timezone is None:
            raise QueryIRInvalidError(
                "TemporalSelectionPlan 的无时区 timestamp 必须由 Catalog 声明 source_timezone"
            )

    @property
    def requires_consumer_binding(self) -> bool:
        return self.selection_clock.requires_consumer_binding

    @property
    def is_bound(self) -> bool:
        return not self.requires_consumer_binding or self.selection_clock.bound_time is not None

    def bind_consumer_time(self, value: str | date | datetime) -> "TemporalSelectionPlan":
        return replace(self, selection_clock=self.selection_clock.bind(value))

    @property
    def source_order_fields(self) -> tuple[str, ...]:
        """内部版本事实的稳定键；正式消费者仍只能看到 public projection。"""

        fields: list[str] = []
        if self.revision_selector is not None:
            fields.extend(self.revision_selector.order_fields)
        if self.visibility_filter is not None:
            fields.extend(
                field for field, _inclusive in self.visibility_filter.time_fields
            )
        if self.effective_interval_selector is not None:
            fields.extend(
                (
                    self.effective_interval_selector.effective_from_field,
                    self.effective_interval_selector.effective_to_field,
                )
            )
        return tuple(dict.fromkeys(fields))

    def _field_type(self, field: str) -> str:
        try:
            return dict(self.field_types)[field]
        except KeyError as exc:  # pragma: no cover - __post_init__ 已闭合
            raise QueryIRInvalidError(f"TemporalSelectionPlan 缺少字段类型: {field}") from exc

    def source_cutoff_parameter(
        self,
        field: str,
        value: str | date | datetime,
        *,
        end_of_date: bool = False,
    ) -> tuple[date | datetime, bool]:
        """返回物理源可直接比较的值，以及该值是否为排他上界。"""

        field_type = self._field_type(field)
        if field_type in {"date", "date32"}:
            return _as_source_date(value, self.source_timezone, field), False
        if field_type not in {"timestamp", "timestamp[us]"}:
            raise QueryIRInvalidError(f"时态字段类型不支持比较: {field_type}")
        local = _as_source_datetime(value, self.source_timezone, field)
        if end_of_date and _is_date_value(value):
            return local + timedelta(days=1), True
        return local, False

    def to_dict(self) -> dict[str, object]:
        return {
            "selection_clock": self.selection_clock.to_dict(),
            "event_time_field": self.event_time_field,
            "required_scan_fields": list(self.required_scan_fields),
            "public_projection": list(self.public_projection),
            "field_types": [list(item) for item in self.field_types],
            "source_timezone": self.source_timezone,
            "visibility_filter": None if self.visibility_filter is None else self.visibility_filter.to_dict(),
            "revision_selector": None if self.revision_selector is None else self.revision_selector.to_dict(),
            "effective_interval_selector": (
                None
                if self.effective_interval_selector is None
                else self.effective_interval_selector.to_dict()
            ),
            "session_close_selection": (
                None
                if self.session_close_selection is None
                else self.session_close_selection.to_dict()
            ),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "TemporalSelectionPlan":
        expected = {
            "selection_clock",
            "event_time_field",
            "required_scan_fields",
            "public_projection",
            "field_types",
            "source_timezone",
            "visibility_filter",
            "revision_selector",
            "effective_interval_selector",
            "session_close_selection",
            "contract_version",
        }
        if not isinstance(payload, Mapping) or set(payload) != expected:
            raise QueryIRInvalidError("temporal_selection schema 不匹配")
        visibility = payload["visibility_filter"]
        revision = payload["revision_selector"]
        interval = payload["effective_interval_selector"]
        session_close = payload["session_close_selection"]
        return cls(
            SelectionClock.from_dict(payload["selection_clock"]),
            str(payload["event_time_field"]),
            _fields(payload["required_scan_fields"], "required_scan_fields"),
            _fields(payload["public_projection"], "public_projection"),
            tuple(tuple(item) for item in payload["field_types"]),
            None if payload["source_timezone"] is None else str(payload["source_timezone"]),
            None if visibility is None else VisibilityFilter.from_dict(visibility),
            None if revision is None else RevisionSelector.from_dict(revision),
            None if interval is None else EffectiveIntervalSelector.from_dict(interval),
            None if session_close is None else SessionCloseSelection.from_dict(session_close),
            str(payload["contract_version"]),
        )

    def select_records(
        self,
        records: Iterable[Mapping[str, object]],
        *,
        consumer_time: str | date | datetime,
    ) -> tuple[Mapping[str, object], ...]:
        """对已物化版本事实执行与 SQL provider 相同的逐消费者选择。"""

        return tuple(
            self.iter_selected_records(
                records,
                consumer_time=consumer_time,
            )
        )

    def iter_selected_records(
        self,
        records: Iterable[Mapping[str, object]],
        *,
        consumer_time: str | date | datetime,
        source_uniqueness_verified: bool = False,
    ) -> Iterator[Mapping[str, object]]:
        """流式选择可见记录；只为修订胜者或有效区间保留必要状态。"""

        seen: set[tuple[object, ...]] | None = (
            None if source_uniqueness_verified else set()
        )
        revision = self.revision_selector
        interval = self.effective_interval_selector
        revised: dict[
            tuple[object, ...],
            tuple[tuple[date | datetime, ...], Mapping[str, object], int],
        ] = {}
        active: dict[tuple[object, ...], tuple[Mapping[str, object], int]] = {}
        for raw in records:
            if not set(self.required_scan_fields) <= set(raw):
                raise QueryIRInvalidError("逐决策时态来源缺少 admitted plan 要求的字段")
            session_close = self.session_close_selection
            if session_close is not None and not session_close.is_visible(
                instrument_id=raw[session_close.instrument_field],
                session_date=raw[session_close.session_date_field],
                consumer_time=consumer_time,
            ):
                continue
            visibility = self.visibility_filter
            if visibility is not None:
                hidden_future = False
                for field, inclusive in visibility.time_fields:
                    available = _selection_value(
                        raw[field],
                        field,
                        field_type=self._field_type(field),
                        timezone_name=self.source_timezone,
                    )
                    cutoff = _selection_value(
                        consumer_time,
                        "consumer_time",
                        field_type=self._field_type(field),
                        timezone_name=self.source_timezone,
                    )
                    if available > cutoff or (available == cutoff and not inclusive):
                        hidden_future = True
                        break
                if hidden_future:
                    continue
            revision = self.revision_selector
            if revision is not None:
                revision_time = _selection_value(
                    raw[revision.order_fields[0]],
                    revision.order_fields[0],
                    field_type=self._field_type(revision.order_fields[0]),
                    timezone_name=self.source_timezone,
                )
                cutoff = _selection_value(
                    consumer_time,
                    "consumer_time",
                    field_type=self._field_type(revision.order_fields[0]),
                    timezone_name=self.source_timezone,
                )
                if revision_time > cutoff:
                    continue
            if seen is not None:
                identity = tuple(
                    _stable_identity_value(raw[field])
                    for field in self.required_scan_fields
                )
                if identity in seen:
                    raise QueryIRInvalidError("逐决策时态来源存在完全重复记录")
                seen.add(identity)

            if interval is not None:
                start = _selection_value(
                    raw[interval.effective_from_field],
                    interval.effective_from_field,
                    field_type=self._field_type(interval.effective_from_field),
                    timezone_name=self.source_timezone,
                )
                raw_end = raw[interval.effective_to_field]
                end = (
                    None
                    if raw_end is None
                    else _selection_value(
                        raw_end,
                        interval.effective_to_field,
                        field_type=self._field_type(interval.effective_to_field),
                        timezone_name=self.source_timezone,
                    )
                )
                cutoff = _selection_value(
                    consumer_time,
                    "consumer_time",
                    field_type=self._field_type(interval.effective_from_field),
                    timezone_name=self.source_timezone,
                )
                starts = start < cutoff or (interval.left_closed and start == cutoff)
                ends = end is None or end > cutoff or (
                    interval.right_closed and end == cutoff
                )
                if starts and ends:
                    pass
                else:
                    continue

            if revision is not None:
                entity = tuple(raw[field] for field in revision.entity_fields)
                order = tuple(
                        _selection_value(
                            raw[field],
                            field,
                            field_type=self._field_type(field),
                            timezone_name=self.source_timezone,
                        )
                        for field in revision.order_fields
                )
                previous = revised.get(entity)
                if previous is None or order > previous[0]:
                    revised[entity] = (order, raw, 1)
                elif order == previous[0]:
                    revised[entity] = (order, previous[1], previous[2] + 1)
                continue

            if interval is not None:
                entity = tuple(raw[field] for field in interval.entity_fields)
                previous = active.get(entity)
                active[entity] = (
                    raw if previous is None else previous[0],
                    1 if previous is None else previous[1] + 1,
                )
                continue

            yield self._public_record(raw)

        if revision is not None:
            winners: list[Mapping[str, object]] = []
            for entity in sorted(revised, key=repr):
                _order, winner, winner_count = revised[entity]
                if winner_count != 1:
                    raise QueryIRInvalidError("逐决策时态最新修订存在冲突")
                winners.append(winner)
            if interval is None:
                for winner in winners:
                    yield self._public_record(winner)
                return
            for winner in winners:
                entity = tuple(winner[field] for field in interval.entity_fields)
                previous = active.get(entity)
                active[entity] = (
                    winner if previous is None else previous[0],
                    1 if previous is None else previous[1] + 1,
                )

        if interval is not None:
            for entity in sorted(active, key=repr):
                winner, count = active[entity]
                if count != 1:
                    raise QueryIRInvalidError("逐决策时态有效区间重叠")
                yield self._public_record(winner)

    def iter_grouped_selected_records(
        self,
        records: Iterable[Mapping[str, object]],
        *,
        consumer_time: str | date | datetime,
        primary_key: tuple[str, ...],
    ) -> Iterator[Mapping[str, object]]:
        """利用已验证的主键排序逐实体选择，避免保留全体实体胜者。"""

        selectors = tuple(
            item
            for item in (
                self.revision_selector,
                self.effective_interval_selector,
            )
            if item is not None
        )
        if not selectors:
            yield from self.iter_selected_records(
                records,
                consumer_time=consumer_time,
                source_uniqueness_verified=True,
            )
            return
        key = tuple(primary_key)
        for selector in selectors:
            entity_fields = selector.entity_fields
            if key[: len(entity_fields)] != entity_fields:
                raise QueryIRInvalidError(
                    "逐决策时态实体字段必须是已验证主键排序的前缀"
                )
        group_fields = (
            self.effective_interval_selector.entity_fields
            if self.effective_interval_selector is not None
            else self.revision_selector.entity_fields
        )

        def entity_key(raw: Mapping[str, object]) -> tuple[object, ...]:
            if not set(group_fields) <= set(raw):
                raise QueryIRInvalidError("逐决策时态来源缺少实体字段")
            return tuple(
                _stable_identity_value(raw[field]) for field in group_fields
            )

        last_key: tuple[tuple[bool, object], ...] | None = None
        for raw_key, group in groupby(records, key=entity_key):
            comparable = tuple((value is None, value) for value in raw_key)
            if last_key is not None and comparable <= last_key:
                raise QueryIRInvalidError("逐决策时态来源没有按实体主键稳定排序")
            last_key = comparable
            yield from self.iter_selected_records(
                group,
                consumer_time=consumer_time,
                source_uniqueness_verified=True,
            )

    def _public_record(
        self,
        raw: Mapping[str, object],
    ) -> Mapping[str, object]:
        return MappingProxyType(
            {field: raw[field] for field in self.public_projection}
        )


def _selection_value(
    value: object,
    field: str,
    *,
    field_type: str,
    timezone_name: str | None,
) -> date | datetime:
    if field_type in {"date", "date32"}:
        return _as_source_date(value, timezone_name, field)
    if field_type not in {"timestamp", "timestamp[us]"}:
        raise QueryIRInvalidError(f"{field} 不是可比较的日期/时间字段")
    return _as_utc_datetime(value, timezone_name, field)


def _as_source_date(value: object, timezone_name: str | None, field: str) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            if timezone_name is None:
                raise QueryIRInvalidError(f"{field} 缺少来源时区")
            return value.astimezone(ZoneInfo(timezone_name)).date()
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            if "T" not in value:
                return date.fromisoformat(value)
            return _as_source_date(datetime.fromisoformat(value), timezone_name, field)
        except ValueError as exc:
            raise QueryIRInvalidError(f"{field} 不是有效时间") from exc
    raise QueryIRInvalidError(f"{field} 必须是日期或时间")


def _as_source_datetime(
    value: object,
    timezone_name: str | None,
    field: str,
) -> datetime:
    if timezone_name is None:
        raise QueryIRInvalidError(f"{field} 缺少来源时区")
    zone = ZoneInfo(timezone_name)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str):
        try:
            parsed = datetime.combine(date.fromisoformat(value), time.min) if _is_date_value(value) else datetime.fromisoformat(value)
        except ValueError as exc:
            raise QueryIRInvalidError(f"{field} 不是有效时间") from exc
    else:
        raise QueryIRInvalidError(f"{field} 必须是日期或时间")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed
    return parsed.astimezone(zone).replace(tzinfo=None)


def _as_utc_datetime(
    value: object,
    timezone_name: str | None,
    field: str,
) -> datetime:
    local = _as_source_datetime(value, timezone_name, field)
    return local.replace(tzinfo=ZoneInfo(str(timezone_name))).astimezone(timezone.utc)


def _is_date_value(value: object) -> bool:
    return isinstance(value, date) and not isinstance(value, datetime) or (
        isinstance(value, str) and "T" not in value
    )


def _stable_identity_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


__all__ = [
    "EffectiveIntervalSelector",
    "RevisionSelector",
    "SELECTION_CLOCK_SOURCES",
    "SelectionClock",
    "SESSION_CLOSE_SELECTION_VERSION",
    "SessionCloseFact",
    "SessionCloseInstrumentBinding",
    "SessionClosePolicyBinding",
    "SessionCloseSelection",
    "TEMPORAL_SELECTION_PLAN_VERSION",
    "TemporalSelectionPlan",
    "VisibilityFilter",
]
