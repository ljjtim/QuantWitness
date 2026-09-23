"""从已验证 Result 指标表读取唯一指标事实。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from numbers import Real

from research_pipeline.platform.metric_contracts import (
    MetricDefinition,
    compose_metric_registry,
)

from .contracts import ResultBundle
from .errors import ResultContractError
from .store import ResultSnapshot, ResultStore


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
    store: ResultStore,
    bundle: ResultBundle,
) -> tuple[ResultSnapshot, tuple[ResultMetric, ...]]:
    """一次完整扫描同时验证 Result 并读取所有证明绑定的指标表。"""

    snapshot = store.open_snapshot(store.result_directory(bundle))
    if snapshot.bundle != bundle:
        raise ResultContractError("指标读取的 ResultBundle 与验证快照不一致")
    return snapshot, metrics_from_snapshot(snapshot)


def metrics_from_snapshot(snapshot: ResultSnapshot) -> tuple[ResultMetric, ...]:
    """消费操作内已验证快照，不再次打开 Result 文件。"""

    bundle = snapshot.bundle
    identity = bundle.verification.verifier_identity
    project_definitions = (
        () if identity is None else tuple(
            MetricDefinition.from_dict(item)
            for item in identity["metric_definitions"]
        )
    )
    project_metric_refs = {item.metric_ref for item in project_definitions}
    registry = compose_metric_registry(project_definitions)
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
                if (
                    proof.metric_ref in project_metric_refs
                    and definition.definition_digest != proof.definition_digest
                ):
                    raise ResultContractError("正式指标定义与 Result 可达性证明不一致")
                value = row.get("value")
                sample_size = row.get("sample_size")
                text_fields = {
                    field: row.get(field)
                    for field in ("unit", "sample_start", "sample_end", "status")
                }
                if (
                    isinstance(value, bool)
                    or not isinstance(value, Real)
                    or not math.isfinite(float(value))
                    or isinstance(sample_size, bool)
                    or not isinstance(sample_size, int)
                    or sample_size <= 0
                    or any(
                        not isinstance(item, str) or not item
                        for item in text_fields.values()
                    )
                ):
                    raise ResultContractError(
                        f"正式指标行字段无效: {proof.metric_ref}"
                    )
                if text_fields["unit"] != definition.unit:
                    raise ResultContractError(
                        f"正式指标单位与定义不一致: {proof.metric_ref}"
                    )
                try:
                    sample_start = datetime.fromisoformat(text_fields["sample_start"])
                    sample_end = datetime.fromisoformat(text_fields["sample_end"])
                except ValueError as exc:
                    raise ResultContractError(
                        f"正式指标样本期不是 ISO 日期/时间: {proof.metric_ref}"
                    ) from exc
                if (sample_start.tzinfo is None) != (sample_end.tzinfo is None):
                    raise ResultContractError(
                        f"正式指标样本期时区口径不一致: {proof.metric_ref}"
                    )
                if sample_start > sample_end:
                    raise ResultContractError(
                        f"正式指标样本期倒置: {proof.metric_ref}"
                    )
                if text_fields["status"] != "computed":
                    raise ResultContractError(
                        f"正式指标状态不受支持: {proof.metric_ref}"
                    )
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
        raise ResultContractError(f"每个已声明指标必须恰好有一行正式结果: {invalid}")
    return tuple(rows_by_ref[ref][0] for ref in sorted(rows_by_ref))


__all__ = ["ResultMetric", "load_result_metrics", "metrics_from_snapshot"]
