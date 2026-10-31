"""PIT、Schema、质量与预算门禁。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from research_pipeline.platform.canonical import typed_canonical_hash

from .admission import (
    MINUTE_AVAILABILITY_RULE,
    MINUTE_TIME_NORMALIZATION_VERSION,
    AdmittedQueryPlan,
)
from .errors import QualityGateError
from .query_ir import DateRangeV1, InstantRangeV2


QUALITY_GATE_VERSION = "quality-gates-v1"


@dataclass(frozen=True)
class QueryEstimate:
    estimated_rows_upper: int
    estimated_bytes_upper: int
    method: str
    confidence: str


def gate_policy_hash(plan: AdmittedQueryPlan) -> str:
    payload = {
        "gate_version": QUALITY_GATE_VERSION,
        "availability_policy_hash": plan.availability_policy_hash,
        "revision_policy_hash": plan.revision_policy_hash,
    }
    if plan.minute_dataset_semantics_hash is not None:
        payload["minute_time_contract"] = {
            "timezone": plan.minute_timezone,
            "timestamp_storage": plan.minute_timestamp_storage,
            "timestamp_role": plan.minute_timestamp_role,
            "bar_interval": plan.minute_bar_interval,
            "availability_rule": plan.minute_availability_rule,
            "normalization_version": plan.minute_time_normalization_version,
        }
    return typed_canonical_hash(payload)


def preflight_estimate(plan: AdmittedQueryPlan, estimate: QueryEstimate) -> None:
    if estimate.estimated_rows_upper < 0 or estimate.estimated_bytes_upper < 0:
        raise QualityGateError("估算值不能为负数")
    if estimate.estimated_rows_upper > plan.query.budget.max_rows:
        raise QualityGateError(
            f"dataset={plan.query.dataset_id} 估算行数超过预算，method={estimate.method}"
        )
    if estimate.estimated_bytes_upper > plan.query.budget.max_bytes:
        raise QualityGateError(
            f"dataset={plan.query.dataset_id} 估算字节超过预算，method={estimate.method}"
        )


def _expected_arrow_type(value: str) -> str:
    return {
        "date32": "date32[day]",
        "decimal128": "decimal128(38, 18)",
        "float64": "double",
        "int8": "int8",
        "int16": "int16",
        "string": "string",
        "bool": "bool",
        "int64": "int64",
        "timestamp[us]": "timestamp[us]",
        "timestamp": "timestamp[us]",
    }.get(value, value)


class QualityGateSession:
    """按稳定主键流式检查，不保存全量主键集合。"""

    def __init__(self, plan: AdmittedQueryPlan, *, temporal_source: bool = False) -> None:
        self.plan = plan
        self.temporal_source = temporal_source
        self.rows = 0
        self.bytes = 0
        self._last_key: tuple[Any, ...] | None = None
        self._expected_names = (plan.temporal_selection.required_scan_fields
            if temporal_source else plan.query.field_ids)
        self._expected_types = {name: _expected_arrow_type(kind) for name, kind in plan.field_types}
        self._nullables = dict(plan.field_nullables)
        self._allowed = pa.array(plan.query.universe.instruments, type=pa.string())
        self._key_fields = (tuple(dict.fromkeys((*plan.primary_key, *plan.temporal_selection.source_order_fields)))
            if temporal_source else plan.primary_key)
        self._time_range = plan.query.time_range
        self._date_timestamp_bounds = None
        self._minute_contract_valid = (
            plan.minute_timezone, plan.minute_timestamp_storage, plan.minute_timestamp_role,
            plan.minute_bar_interval, plan.minute_availability_rule, plan.minute_time_normalization_version,
        ) == ("Asia/Shanghai", "naive_local_wall_clock", "completed_bar_end", "1m",
              MINUTE_AVAILABILITY_RULE, MINUTE_TIME_NORMALIZATION_VERSION)
        if isinstance(self._time_range, InstantRangeV2):
            zone = self._time_range.start_at.tzinfo
            self._minute_start = self._time_range.start_at.replace(tzinfo=None)
            self._minute_end = self._time_range.end_at.astimezone(zone).replace(tzinfo=None)
            self._minute_as_of = (None if plan.query.as_of_instant is None
                else plan.query.as_of_instant.astimezone(zone).replace(tzinfo=None))

    def validate_batch(self, batch: Any) -> None:
        names = tuple(batch.schema.names)
        if names != self._expected_names:
            raise QualityGateError(f"dataset={self.plan.query.dataset_id} Arrow 字段顺序漂移")
        for field in batch.schema:
            expected = self._expected_types[field.name]
            if str(field.type) != expected:
                raise QualityGateError(f"field={field.name} Arrow 类型漂移: {field.type} != {expected}")
            if not self._nullables[field.name] and batch.column(field.name).null_count:
                raise QualityGateError(f"field={field.name} 非空约束失败")
        self.rows += int(batch.num_rows)
        self.bytes += int(batch.nbytes)
        if self.rows > self.plan.query.budget.max_rows or self.bytes > self.plan.query.budget.max_bytes:
            raise QualityGateError(f"dataset={self.plan.query.dataset_id} 运行时预算超限")
        self._validate_temporal_and_keys(batch)

    def _validate_temporal_and_keys(self, batch: Any) -> None:
        if batch.num_rows == 0:
            return
        events = batch.column(self.plan.event_time_field)
        failures = []

        def reject_at(mask, message):
            index = pc.index(pc.or_kleene(pc.is_null(mask), mask), True).as_py()
            if index >= 0:
                failures.append((index, message))

        pit_message = f"field={self.plan.event_time_field} PIT 时间越界"
        if isinstance(self._time_range, DateRangeV1):
            if pa.types.is_timestamp(events.type):
                if self._date_timestamp_bounds is None:
                    timezone = events.type.tz
                    zone = None
                    if timezone is not None:
                        zone = (datetime.fromisoformat("2000-01-01T00:00:00" + timezone).tzinfo
                                if timezone.startswith(("+", "-")) else ZoneInfo(timezone))
                    self._date_timestamp_bounds = (
                        pa.scalar(datetime.combine(self._time_range.start, time.min, zone), type=events.type),
                        pa.scalar(datetime.combine(self._time_range.end, time.min, zone) + timedelta(days=1), type=events.type),
                    )
                start, end = self._date_timestamp_bounds
                reject_at(pc.invert(pc.and_(pc.greater_equal(events, start), pc.less(events, end))), pit_message)
                dates = None
            elif pa.types.is_date(events.type):
                dates = events
            else:
                dates = None
                failures.append((0, pit_message))
            if dates is not None:
                reject_at(pc.invert(pc.and_(pc.greater_equal(dates, self._time_range.start),
                    pc.less_equal(dates, self._time_range.end))), pit_message)
        elif isinstance(self._time_range, InstantRangeV2):
            if not self._minute_contract_valid:
                raise QualityGateError("分钟计划缺少可执行时间标准化合同")
            type_message = f"field={self.plan.event_time_field} 必须是 naive 本地墙钟 timestamp[us]"
            if not pa.types.is_timestamp(events.type) or events.type.tz is not None:
                failures.append((0, type_message))
            else:
                reject_at(pc.is_null(events), type_message)
                if self._minute_as_of is None:
                    failures.append((0, pit_message))
                else:
                    visible = pc.and_(pc.and_(pc.greater_equal(events, self._minute_start),
                        pc.less(events, self._minute_end)), pc.less_equal(events, self._minute_as_of))
                    reject_at(pc.invert(visible), pit_message)
        if len(self._allowed):
            message = f"field={self.plan.instrument_field} Universe 越界"
            if self.plan.instrument_field is None:
                failures.append((0, message))
            else:
                instruments = batch.column(self.plan.instrument_field)
                if pa.types.is_string(instruments.type) or pa.types.is_large_string(instruments.type):
                    reject_at(pc.invert(pc.is_in(instruments, value_set=self._allowed)), message)
                else:
                    failures.append((0, message))
        if failures:
            # 保持原逐行顺序：最早失败行优先，同一行先报告PIT再报告universe。
            raise QualityGateError(min(failures, key=lambda item: item[0])[1])

        columns = [batch.column(name) for name in self._key_fields]
        if not columns:
            return

        def key_at(index):
            values = (column[index].as_py() for column in columns)
            return tuple((value is None, value) for value in values)

        first = key_at(0)
        if self._last_key is not None and first <= self._last_key:
            reason = "重复" if first == self._last_key else "乱序"
            raise QualityGateError(f"dataset={self.plan.query.dataset_id} primary_key {reason}")
        if batch.num_rows > 1:
            same_prefix, decreasing = pa.scalar(True), pa.scalar(False)
            for column in columns:
                previous, current = column.slice(0, batch.num_rows - 1), column.slice(1)
                both_valid = pc.and_(pc.is_valid(current), pc.is_valid(previous))
                same = pc.or_(pc.and_kleene(both_valid, pc.equal(current, previous)),
                    pc.and_(pc.is_null(current), pc.is_null(previous)))
                less = pc.or_(pc.and_kleene(both_valid, pc.less(current, previous)),
                    pc.and_(pc.is_valid(current), pc.is_null(previous)))
                decreasing = pc.or_(decreasing, pc.and_(same_prefix, less))
                same_prefix = pc.and_(same_prefix, same)
            invalid = pc.index(pc.or_(decreasing, same_prefix), True).as_py()
            if invalid >= 0:
                self._last_key = key_at(invalid)
                reason = "重复" if same_prefix[invalid].as_py() else "乱序"
                raise QualityGateError(f"dataset={self.plan.query.dataset_id} primary_key {reason}")
        self._last_key = key_at(batch.num_rows - 1)


def validate_table(plan: AdmittedQueryPlan, table: Any) -> None:
    session = QualityGateSession(plan)
    for batch in table.to_batches(max_chunksize=plan.query.budget.batch_size):
        session.validate_batch(batch)


def validate_publish_manifest(
    plan: AdmittedQueryPlan,
    manifest: dict[str, Any],
    *,
    logical_snapshot_id: str,
) -> None:
    if manifest.get("admitted_plan_hash") != plan.plan_hash:
        raise QualityGateError("发布 manifest 的 admitted plan 已失配")
    if manifest.get("logical_snapshot_id") != logical_snapshot_id:
        raise QualityGateError("发布 manifest 的 logical snapshot 已失配")
    if manifest.get("gate_policy_hash") != gate_policy_hash(plan):
        raise QualityGateError("发布 manifest 的 gate policy 已失配")
    expected_temporal_source = plan.temporal_selection.requires_consumer_binding
    if manifest.get("temporal_source", False) is not expected_temporal_source:
        raise QualityGateError("发布 manifest 的逐决策时态来源标记失配")


__all__ = [
    "QUALITY_GATE_VERSION",
    "QualityGateSession",
    "QueryEstimate",
    "gate_policy_hash",
    "preflight_estimate",
    "validate_table",
    "validate_publish_manifest",
]
