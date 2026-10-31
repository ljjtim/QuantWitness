"""直接读取正式 Result，生成报告、比较与可迁移交付。"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
from typing import Mapping

from .errors import ResultContractError
from .metrics import ResultMetric, metrics_from_snapshot
from .store import ResultSnapshot, ResultStore


@dataclass(frozen=True)
class ResultContext:
    snapshot: ResultSnapshot
    metrics: tuple[ResultMetric, ...]
    claim_level: str
    limitations: tuple[str, ...]
    research_cost_assumption: Mapping[str, object] | None


@dataclass(frozen=True)
class ResultReport:
    result_id: str
    run: object
    claim_level: str
    limitations: tuple[str, ...]
    research_cost_assumption: Mapping[str, object] | None
    metrics: tuple[ResultMetric, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "result_id": self.result_id, "run": self.run.to_dict(),
            "claim_level": self.claim_level, "limitations": list(self.limitations),
            "research_cost_assumption": None if self.research_cost_assumption is None else dict(self.research_cost_assumption),
            "metrics": [metric.to_dict() for metric in self.metrics],
        }


@dataclass(frozen=True)
class ResultComparison:
    comparable: bool
    reason_codes: tuple[str, ...]
    metric_comparisons: tuple[Mapping[str, object], ...]
    left_result_id: str
    right_result_id: str
    comparison_scope: str = "formal_metric_facts"

    def to_dict(self) -> dict[str, object]:
        return {
            "comparison_scope": self.comparison_scope,
            "comparable": self.comparable, "reason_codes": list(self.reason_codes),
            "left_result_id": self.left_result_id, "right_result_id": self.right_result_id,
            "metric_comparisons": [dict(row) for row in self.metric_comparisons],
        }


def load_result_context(
    result: str | Path, *, result_store: str | Path | None = None,
    additional_table_ids: tuple[str, ...] = (),
) -> ResultContext:
    """只打开指标和调用方实际选择的表，不扫描其他研究载荷。"""
    directory = Path(result).resolve()
    if directory.is_file():
        if directory.name != "result.json":
            raise ResultContractError("Result 路径必须指向结果目录或 result.json")
        directory = directory.parent
    store = ResultStore(result_store if result_store is not None else directory.parents[2], create=False)
    bundle = store.inspect_directory(directory)
    by_id = {table.table_id: table for table in bundle.tables}
    unknown = set(additional_table_ids) - set(by_id)
    if unknown:
        raise ResultContractError(f"Result 不包含请求的表: {sorted(unknown)}")
    schema_ids = tuple(sorted({
        *(proof.result_schema_id for proof in bundle.metric_proofs),
        *(by_id[name].schema_id for name in additional_table_ids),
    }))
    snapshot = store.open_snapshot(directory, verify_schema_ids=schema_ids, verify_all_files=False)
    limitations = tuple(sorted(set(bundle.run.limitations) | {
        f"input_claim_ceiling:{request_id}:{level}"
        for request_id, level in bundle.input_claim_ceilings.items()
        if level != "tradable_simulation"
    }))
    return ResultContext(snapshot, metrics_from_snapshot(snapshot), bundle.claim_level, limitations, bundle.research_cost_assumption)


def build_result_report(context: ResultContext) -> ResultReport:
    return ResultReport(
        context.snapshot.bundle.result_id, context.snapshot.bundle.run,
        context.claim_level, context.limitations, context.research_cost_assumption, context.metrics,
    )


def render_result_report(context: ResultContext) -> str:
    report = build_result_report(context)
    rows = [
        "# 研究结果报告", "", f"- Result：`{report.result_id}`",
        f"- 执行状态：{report.run.status}", f"- 声明结论级别：`{report.claim_level}`",
    ]
    if report.limitations:
        rows.extend(("", "## 数据限制", "", *(f"- {item}" for item in report.limitations)))
    if report.research_cost_assumption is not None:
        rows.extend(("", "## 研究费用假设", "", str(dict(report.research_cost_assumption))))
    rows.extend(("", "## 正式指标", ""))
    rows.extend(
        f"- `{metric.metric_ref}`：{metric.value} {metric.unit}；"
        f"样本 {metric.sample_start} 至 {metric.sample_end}，n={metric.sample_size}"
        for metric in report.metrics
    )
    return "\n".join(rows) + "\n"


def compare_results(left: ResultContext, right: ResultContext) -> ResultComparison:
    reasons = set()
    if left.claim_level != right.claim_level:
        reasons.add("comparison.claim_level_mismatch")
    if left.snapshot.bundle.effective_input_claim_ceiling != right.snapshot.bundle.effective_input_claim_ceiling:
        reasons.add("comparison.claim_ceiling_mismatch")
    left_metrics = {row.metric_ref: row for row in left.metrics}
    right_metrics = {row.metric_ref: row for row in right.metrics}
    if set(left_metrics) != set(right_metrics):
        reasons.add("comparison.metric_refs_mismatch")
    left_proofs = {proof.metric_ref: proof for proof in left.snapshot.bundle.metric_proofs}
    right_proofs = {proof.metric_ref: proof for proof in right.snapshot.bundle.metric_proofs}
    comparisons = []
    for ref in sorted(set(left_metrics) & set(right_metrics)):
        a, b = left_metrics[ref], right_metrics[ref]
        if left_proofs[ref].definition_digest != right_proofs[ref].definition_digest:
            reasons.add("comparison.metric_definition_mismatch")
        for field in ("unit", "sample_start", "sample_end", "sample_size", "status"):
            if getattr(a, field) != getattr(b, field):
                reasons.add(f"comparison.metric_{field}_mismatch")
        comparisons.append({
            "metric_ref": ref, "unit": a.unit, "sample_start": a.sample_start,
            "sample_end": a.sample_end, "sample_size": a.sample_size, "status": a.status,
            "left_value": a.value, "right_value": b.value, "delta": b.value - a.value,
        })
    return ResultComparison(not reasons, tuple(sorted(reasons)), tuple(comparisons) if not reasons else (),
                            left.snapshot.bundle.result_id, right.snapshot.bundle.result_id)


def export_result(context: ResultContext, destination: str | Path) -> Path:
    """复制正式结果，保留 namespace；目标必须为新目录。"""
    target = Path(destination).resolve()
    if target.exists():
        raise ResultContractError("export-result 目标必须不存在")
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        raise ResultContractError("export-result 临时目录已存在")
    bundle = context.snapshot.bundle
    relative = Path(bundle.project_id) / bundle.run_id / bundle.result_id
    copied = temporary / relative
    try:
        copied.parent.mkdir(parents=True)
        shutil.copytree(context.snapshot.directory, copied)
        if ResultStore(temporary, create=False).verify(copied) != bundle:
            raise ResultContractError("export-result 复制内容与原结果不一致")
        os.replace(temporary, target)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    return target / relative


__all__ = ["ResultContext", "ResultReport", "ResultComparison", "load_result_context",
           "build_result_report", "render_result_report", "compare_results", "export_result"]
