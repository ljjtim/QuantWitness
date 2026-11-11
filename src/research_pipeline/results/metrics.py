"""从已验证 Result 指标表读取唯一指标事实。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
from numbers import Real
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

from research_pipeline.platform.metric_contracts import (
    MetricDefinition,
    compose_metric_registry,
)

from .contracts import ResultBundle
from .errors import ResultContractError


class MetricSnapshot(Protocol):
    """指标消费需要的已验证列式快照。"""

    bundle: ResultBundle

    def table_schema(self, schema_id: str) -> pa.Schema: ...

    def iter_table_batches(
        self, schema_id: str, *, columns: tuple[str, ...] | None = None,
        batch_size: int = 8_192,
    ): ...


class MetricStore(Protocol):
    """指标读取所需的结果存储接口。"""

    def result_directory(self, bundle: ResultBundle) -> Path: ...

    def open_snapshot(self, result_directory: str | Path) -> MetricSnapshot: ...


_REQUIRED_COLUMNS = frozenset({
    "metric_ref",
    "value",
    "unit",
    "sample_start",
    "sample_end",
    "sample_size",
    "status",
})


@dataclass(frozen=True)
class ResultMetric:
    metric_ref: str
    value: float
    unit: str
    sample_start: str
    sample_end: str
    sample_size: int
    status: str
    table_id: str

    def to_dict(self) -> dict[str, object]:
        return {
            "metric_ref": self.metric_ref,
            "value": self.value,
            "unit": self.unit,
            "sample_start": self.sample_start,
            "sample_end": self.sample_end,
            "sample_size": self.sample_size,
            "status": self.status,
            "table_id": self.table_id,
        }


def load_result_metrics(
    store: MetricStore,
    bundle: ResultBundle,
) -> tuple[MetricSnapshot, tuple[ResultMetric, ...]]:
    """一次完整扫描同时验证 Result 并读取所有证明绑定的指标表。"""

    snapshot = store.open_snapshot(store.result_directory(bundle))
    if snapshot.bundle != bundle:
        raise ResultContractError("指标读取的 ResultBundle 与验证快照不一致")
    return snapshot, metrics_from_snapshot(snapshot)


def metrics_from_snapshot(snapshot: MetricSnapshot) -> tuple[ResultMetric, ...]:
    """消费操作内已验证快照，不再次打开 Result 文件。"""

    bundle = snapshot.bundle
    registry = compose_metric_registry(metric_definitions_from_snapshot(snapshot))
    rows_by_ref: dict[str, list[ResultMetric]] = {
        proof.metric_ref: [] for proof in bundle.metric_proofs
    }
    proofs_by_schema: dict[str, list[object]] = {}
    for proof in bundle.metric_proofs:
        proofs_by_schema.setdefault(proof.result_schema_id, []).append(proof)
    for schema_id, proofs in proofs_by_schema.items():
        schema = snapshot.table_schema(schema_id)
        if not _REQUIRED_COLUMNS <= set(schema.names):
            missing = sorted(_REQUIRED_COLUMNS - set(schema.names))
            raise ResultContractError(f"正式指标表缺少最小列: {missing}")
        proof_by_ref = {proof.metric_ref: proof for proof in proofs}
        for batch in snapshot.iter_table_batches(schema_id, batch_size=8_192):
            for row in batch.to_pylist():
                metric_ref = row.get("metric_ref")
                proof = proof_by_ref.get(metric_ref)
                if proof is None:
                    continue
                definition = registry.require(proof.metric_ref)
                if definition.definition_digest != proof.definition_digest:
                    raise ResultContractError("Result 指标定义与正式生产声明不一致")
                value, sample_size, text_fields = validate_metric_row(row, definition)
                rows_by_ref[proof.metric_ref].append(ResultMetric(
                    metric_ref=proof.metric_ref,
                    value=float(value),
                    unit=text_fields["unit"],
                    sample_start=text_fields["sample_start"],
                    sample_end=text_fields["sample_end"],
                    sample_size=sample_size,
                    status=text_fields["status"],
                    table_id=proof.result_table_id,
                ))
    invalid = sorted(ref for ref, rows in rows_by_ref.items() if len(rows) != 1)
    if invalid:
        raise ResultContractError(f"正式指标 metric_ref 行数无效：实际 {dict((ref, len(rows_by_ref[ref])) for ref in invalid)!r}；期望每项恰好有一行")
    return tuple(rows_by_ref[ref][0] for ref in sorted(rows_by_ref))


def validate_metric_row(row, definition):
    """发布与消费共用的正式指标行合同。"""
    ref = definition.metric_ref
    def invalid(field, expected):
        value = row.get(field)
        raise ResultContractError(
            f"正式指标 {ref} 字段 {field}: 实际 {value!r} "
            f"(类型 {type(value).__name__})；期望 {expected}"
        )
    value, sample_size = row.get("value"), row.get("sample_size")
    if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(float(value)):
        invalid("value", "有限实数，且不是 bool")
    if isinstance(sample_size, bool) or not isinstance(sample_size, int) or sample_size <= 0:
        invalid("sample_size", "正整数，且不是 bool")
    text = {field: row.get(field) for field in ("unit", "sample_start", "sample_end", "status")}
    for field, item in text.items():
        if not isinstance(item, str) or not item:
            invalid(field, "非空字符串")
    if text["unit"] != definition.unit:
        invalid("unit", repr(definition.unit))
    dates = {}
    for field in ("sample_start", "sample_end"):
        try:
            dates[field] = datetime.fromisoformat(text[field])
        except ValueError:
            invalid(field, "ISO 日期或时间字符串")
    if (dates["sample_start"].tzinfo is None) != (dates["sample_end"].tzinfo is None):
        invalid("sample_end", "与 sample_start 相同的时区口径")
    if dates["sample_start"] > dates["sample_end"]:
        invalid("sample_end", f"不早于 sample_start={text['sample_start']!r}")
    if text["status"] != "computed":
        invalid("status", "'computed'")
    return value, sample_size, text


def metric_definitions_from_snapshot(snapshot):
    """从 Result 冻结的项目定义和 LEAN 表定义读取指标合同。"""
    definitions = list(snapshot.bundle.metric_definitions)
    from research_pipeline.platform.lean_contract import LEAN_METRICS_SCHEMA_ID
    if any(table.schema_id == LEAN_METRICS_SCHEMA_ID for table in snapshot.bundle.tables):
        seen = set()
        for batch in snapshot.iter_table_batches(LEAN_METRICS_SCHEMA_ID, columns=("metric_ref", "definition_json"), batch_size=1024):
            for row in batch.to_pylist():
                definition = MetricDefinition.from_dict(json.loads(row["definition_json"]))
                if (definition.metric_ref != row["metric_ref"] or definition.metric_ref in seen
                        or not definition.metric_id.startswith("project.lean.")
                        or definition.result_schema_id != LEAN_METRICS_SCHEMA_ID):
                    raise ResultContractError("LEAN正式指标冻结定义不一致或重复")
                seen.add(definition.metric_ref)
                existing = next((item for item in definitions if item.metric_ref == definition.metric_ref), None)
                if existing is None:
                    definitions.append(definition)
                elif existing != definition:
                    raise ResultContractError("表内指标定义与 Result 声明不一致")
    return tuple(definitions)


__all__ = ["ResultMetric", "load_result_metrics", "metrics_from_snapshot", "metric_definitions_from_snapshot"]
