"""PIT、Schema、质量与预算门禁。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from research_pipeline.platform.canonical import typed_canonical_hash

from .admission import (
    MINUTE_AVAILABILITY_RULE,
    MINUTE_TIME_NORMALIZATION_VERSION,
    AdmittedQueryPlan,
)
from .errors import QualityGateError
from .query_ir import DateRangeV1, InstantRangeV2, normalize_minute_instant


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

    def validate_batch(self, batch: Any) -> None:
        names = tuple(batch.schema.names)
        expected_names = (
            self.plan.temporal_selection.required_scan_fields
            if self.temporal_source
            else self.plan.query.field_ids
        )
        if names != expected_names:
            raise QualityGateError(f"dataset={self.plan.query.dataset_id} Arrow 字段顺序漂移")
        expected_types = dict(self.plan.field_types)
        nullables = dict(self.plan.field_nullables)
        for field in batch.schema:
            expected = _expected_arrow_type(expected_types[field.name])
            if str(field.type) != expected:
                raise QualityGateError(f"field={field.name} Arrow 类型漂移: {field.type} != {expected}")
            if not nullables[field.name] and batch.column(field.name).null_count:
                raise QualityGateError(f"field={field.name} 非空约束失败")
        self.rows += int(batch.num_rows)
        self.bytes += int(batch.nbytes)
        if self.rows > self.plan.query.budget.max_rows or self.bytes > self.plan.query.budget.max_bytes:
            raise QualityGateError(f"dataset={self.plan.query.dataset_id} 运行时预算超限")
        self._validate_temporal_and_keys(batch)

    def _validate_temporal_and_keys(self, batch: Any) -> None:
        event_values = batch.column(self.plan.event_time_field).to_pylist()
        instrument_values = (
            [None] * len(event_values)
            if self.plan.instrument_field is None
            else batch.column(self.plan.instrument_field).to_pylist()
        )
        allowed = set(self.plan.query.universe.instruments)
        for event, instrument in zip(event_values, instrument_values):
            if isinstance(self.plan.query.time_range, DateRangeV1):
                event_date = event.date() if isinstance(event, datetime) else event
                if not isinstance(event_date, date) or not self.plan.query.time_range.start <= event_date <= self.plan.query.time_range.end:
                    raise QualityGateError(f"field={self.plan.event_time_field} PIT 时间越界")
            elif isinstance(self.plan.query.time_range, InstantRangeV2):
                if (
                    self.plan.minute_timezone,
                    self.plan.minute_timestamp_storage,
                    self.plan.minute_timestamp_role,
                    self.plan.minute_bar_interval,
                    self.plan.minute_availability_rule,
                    self.plan.minute_time_normalization_version,
                ) != (
                    "Asia/Shanghai",
                    "naive_local_wall_clock",
                    "completed_bar_end",
                    "1m",
                    MINUTE_AVAILABILITY_RULE,
                    MINUTE_TIME_NORMALIZATION_VERSION,
                ):
                    raise QualityGateError("分钟计划缺少可执行时间标准化合同")
                if not isinstance(event, datetime) or event.tzinfo is not None:
                    raise QualityGateError(
                        f"field={self.plan.event_time_field} 必须是 naive 本地墙钟 timestamp[us]"
                    )
                event_at = normalize_minute_instant(
                    event.replace(tzinfo=self.plan.query.time_range.start_at.tzinfo),
                    self.plan.event_time_field,
                )
                as_of = self.plan.query.as_of_instant
                if (
                    as_of is None
                    or not self.plan.query.time_range.start_at <= event_at < self.plan.query.time_range.end_at
                    or event_at > as_of
                ):
                    raise QualityGateError(f"field={self.plan.event_time_field} PIT 时间越界")
            if allowed and instrument not in allowed:
                raise QualityGateError(f"field={self.plan.instrument_field} Universe 越界")
        key_fields = self.plan.primary_key
        if self.temporal_source:
            key_fields = tuple(
                dict.fromkeys(
                    (
                        *key_fields,
                        *self.plan.temporal_selection.source_order_fields,
                    )
                )
            )
        key_columns = [batch.column(field_id).to_pylist() for field_id in key_fields]
        for raw_key in zip(*key_columns):
            key = tuple((value is None, value) for value in raw_key)
            if self._last_key is not None and key <= self._last_key:
                reason = "重复" if key == self._last_key else "乱序"
                raise QualityGateError(f"dataset={self.plan.query.dataset_id} primary_key {reason}")
            self._last_key = key


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
