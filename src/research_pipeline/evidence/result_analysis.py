"""从正式 Result 流式生成通用时间序列分析。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
import math
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import yaml

from research_pipeline.platform import canonical_json
from research_pipeline.platform.exclusive_output import write_text_exclusive_atomic

from .analysis_contracts import (
    AnalysisComparison,
    AnalysisRequest,
    AnalysisResult,
    AnalysisSource,
    AnalysisStatistics,
    AnalysisTableBinding,
    AnalysisYear,
)
from .errors import EvidenceContractError
from research_pipeline.results import ResultReference, load_result_context


DEFAULT_ANALYSIS_MEMORY_BYTES = 8 * 1024 * 1024
MAX_ANALYSIS_BATCH_ROWS = 8_192


def load_analysis_request(source: str | Path) -> AnalysisRequest:
    """读取 YAML/JSON 请求并生成规范身份。"""

    try:
        payload = yaml.safe_load(Path(source).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise EvidenceContractError("AnalysisRequest 无法读取") from exc
    if not isinstance(payload, Mapping):
        raise EvidenceContractError("AnalysisRequest 必须是对象")
    return AnalysisRequest.from_dict(payload)


def load_analysis_result(source: str | Path) -> AnalysisResult:
    """读取并复核规范 AnalysisResult JSON。"""

    raw, payload = _load_canonical_mapping(source, "AnalysisResult")
    result = AnalysisResult.from_dict(payload)
    if raw != canonical_json(result.to_dict()):
        raise EvidenceContractError("AnalysisResult 不是规范 JSON")
    return result


def load_analysis_comparison(source: str | Path) -> AnalysisComparison:
    """读取并复核规范 AnalysisComparison JSON。"""

    raw, payload = _load_canonical_mapping(source, "AnalysisComparison")
    comparison = AnalysisComparison.from_dict(payload)
    if raw != canonical_json(comparison.to_dict()):
        raise EvidenceContractError("AnalysisComparison 不是规范 JSON")
    return comparison


def analyze_result(
    result: str | Path,
    *,
    result_store: str | Path | None = None,
    request: AnalysisRequest,
    memory_bytes: int = DEFAULT_ANALYSIS_MEMORY_BYTES,
) -> AnalysisResult:
    """只读取请求的两个列，生成不提高来源 claim 的分析结果。"""

    if type(memory_bytes) is not int or memory_bytes <= 0:
        raise EvidenceContractError("analysis memory budget 必须是正整数字节")
    context = load_result_context(
        result,
        result_store=result_store,
        additional_table_ids=(request.selection.table_id,),
    )
    bundle = context.snapshot.bundle
    if request.result_id != bundle.result_id:
        raise EvidenceContractError("AnalysisRequest result_id 与 Result 不一致")

    manifests = tuple(
        table for table in bundle.tables
        if table.table_id == request.selection.table_id
    )
    if len(manifests) != 1:
        raise EvidenceContractError("AnalysisRequest table_id 未唯一绑定 Result 表")
    manifest = manifests[0]
    schema = context.snapshot.table_schema(manifest.schema_id)
    try:
        date_field = schema.field(request.selection.date_column)
        value_field = schema.field(request.selection.value_column)
    except KeyError as exc:
        raise EvidenceContractError("AnalysisRequest 选择的列不存在") from exc
    if not (
        pa.types.is_date32(date_field.type)
        or pa.types.is_date64(date_field.type)
        or pa.types.is_timestamp(date_field.type)
    ):
        raise EvidenceContractError("analysis 日期列必须是 Arrow date 或 timestamp")
    if not (
        pa.types.is_integer(value_field.type)
        or pa.types.is_floating(value_field.type)
    ) or pa.types.is_boolean(value_field.type):
        raise EvidenceContractError("analysis 数值列必须是 Arrow integer 或 floating")

    batch_size = _batch_size(
        memory_bytes,
        date_field.type.bit_width,
        value_field.type.bit_width,
    )
    start = date.fromisoformat(request.window.start)
    end = date.fromisoformat(request.window.end)
    overall = _Accumulator(request.semantics.value_semantics)
    yearly: dict[int, _Accumulator] = {}
    input_rows = 0
    selected_rows = 0
    missing_rows = 0
    outside_window_rows = 0
    previous_order_key: date | datetime | None = None
    actual_start: str | None = None
    actual_end: str | None = None

    for batch in context.snapshot.iter_table_batches(
        manifest.schema_id,
        columns=(request.selection.date_column, request.selection.value_column),
        batch_size=batch_size,
    ):
        dates = batch.column(0).to_pylist()
        values = batch.column(1).to_pylist()
        for raw_date, raw_value in zip(dates, values, strict=True):
            input_rows += 1
            order_key, calendar_date, display_date = _normalize_temporal(raw_date)
            if previous_order_key is not None and order_key <= previous_order_key:
                if order_key == previous_order_key:
                    raise EvidenceContractError("analysis 日期列存在重复值")
                raise EvidenceContractError("analysis 日期列必须严格递增")
            previous_order_key = order_key
            if calendar_date < start or calendar_date > end:
                outside_window_rows += 1
                continue
            if raw_value is None:
                missing_rows += 1
                if request.policies.missing == "reject":
                    raise EvidenceContractError("analysis 窗口内数值列存在空值")
                continue
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                raise EvidenceContractError("analysis 数值无法转换为浮点数")
            value = float(raw_value)
            if not math.isfinite(value):
                raise EvidenceContractError("analysis 数值列包含 NaN 或 Inf")
            if request.semantics.value_semantics == "simple_return" and value < -1.0:
                raise EvidenceContractError("simple_return 不能小于 -1")
            overall.add(value, display_date)
            yearly.setdefault(
                calendar_date.year,
                _Accumulator(request.semantics.value_semantics),
            ).add(value, display_date)
            selected_rows += 1
            if actual_start is None:
                actual_start = display_date
            actual_end = display_date

    expected_rows = context.snapshot.table_row_count(manifest.schema_id)
    if input_rows != expected_rows:
        raise EvidenceContractError("analysis 实际读取行数与 Result manifest 不一致")
    if selected_rows == 0 or actual_start is None or actual_end is None:
        raise EvidenceContractError("analysis 窗口没有有效观察")

    aggregations = request.aggregations
    year_results = _build_year_results(request, yearly)
    source = AnalysisSource(
        result_reference=ResultReference.build(bundle),
        package_hash=bundle.package_hash, plan_hash=bundle.plan_hash,
        runtime_hash=bundle.runtime_hash,
        implementation_manifest_hash=bundle.implementation_manifest_hash,
        claim_level=context.claim_level,
        claim_ceiling=bundle.effective_input_claim_ceiling, limitations=context.limitations,
    )
    table = AnalysisTableBinding(
        table_id=manifest.table_id,
        schema_id=manifest.schema_id,
        table_manifest_hash=manifest.table_manifest_hash,
        date_column=request.selection.date_column,
        value_column=request.selection.value_column,
        date_arrow_type=str(date_field.type),
        value_arrow_type=str(value_field.type),
    )
    return AnalysisResult.build(
        source=source,
        request_hash=request.request_hash,
        analysis_spec_hash=request.analysis_spec_hash,
        table=table,
        requested_window=request.window,
        actual_start=actual_start,
        actual_end=actual_end,
        semantics=request.semantics,
        policies=request.policies,
        aggregations=aggregations,
        input_rows=input_rows,
        selected_rows=selected_rows,
        missing_rows=missing_rows,
        outside_window_rows=outside_window_rows,
        overall=overall.statistics(
            aggregations,
            periods_per_year=request.semantics.periods_per_year,
        ),
        yearly=year_results,
    )


def compare_analysis_results(
    results: tuple[AnalysisResult, ...],
    *,
    measure: str,
    direction: str,
) -> AnalysisComparison:
    """对同口径 AnalysisResult 排名；不一致时整体不给部分排名。"""

    if len(results) < 2:
        raise EvidenceContractError("analysis compare 至少需要两份 AnalysisResult")
    if len({item.analysis_hash for item in results}) != len(results):
        raise EvidenceContractError("analysis compare 输入不能重复")
    ordered = tuple(sorted(results, key=lambda item: item.analysis_hash))
    # 构造一次可验证的空比较，以复用合同对 measure/direction 的枚举校验。
    reasons = _comparison_reasons(ordered, measure)
    sources = tuple(_comparison_source(item) for item in ordered)
    rankings: tuple[Mapping[str, object], ...] = ()
    if not reasons:
        values = tuple((item, _measure_value(item, measure)) for item in ordered)
        rankings = _rankings(values, direction)
    return AnalysisComparison.build(
        measure=measure,
        direction=direction,
        comparable=not reasons,
        reason_codes=tuple(sorted(reasons)),
        sources=sources,
        rankings=rankings,
    )


def write_analysis_result(
    result: AnalysisResult,
    destination: str | Path,
) -> Path:
    return _write_canonical_document(destination, result.to_dict())


def write_analysis_comparison(
    comparison: AnalysisComparison,
    destination: str | Path,
) -> Path:
    return _write_canonical_document(destination, comparison.to_dict())


@dataclass
class _Accumulator:
    value_semantics: str
    count: int = 0
    total: float = 0.0
    compensation: float = 0.0
    positive_count: int = 0
    negative_count: int = 0
    zero_count: int = 0
    best_value: float | None = None
    best_date: str | None = None
    worst_value: float | None = None
    worst_date: str | None = None
    first_value: float | None = None
    last_value: float | None = None
    net_value: float = 1.0
    running_peak: float = 1.0
    max_drawdown: float = 0.0
    cumulative_log_return: float = 0.0

    def add(self, value: float, display_date: str) -> None:
        adjusted = value - self.compensation
        updated = self.total + adjusted
        self.compensation = (updated - self.total) - adjusted
        self.total = updated
        if not math.isfinite(self.total) or not math.isfinite(self.compensation):
            raise EvidenceContractError("analysis 累计统计溢出")
        self.count += 1
        self.first_value = value if self.first_value is None else self.first_value
        self.last_value = value
        if value > 0:
            self.positive_count += 1
        elif value < 0:
            self.negative_count += 1
        else:
            self.zero_count += 1
        if self.best_value is None or value > self.best_value:
            self.best_value = value
            self.best_date = display_date
        if self.worst_value is None or value < self.worst_value:
            self.worst_value = value
            self.worst_date = display_date
        if self.value_semantics == "simple_return":
            self.net_value *= 1.0 + value
            if not math.isfinite(self.net_value):
                raise EvidenceContractError("simple_return 净值路径溢出")
            self._update_drawdown()
        elif self.value_semantics == "log_return":
            self.cumulative_log_return += value
            try:
                self.net_value = math.exp(self.cumulative_log_return)
            except OverflowError as exc:
                raise EvidenceContractError("log_return 净值路径溢出") from exc
            if not math.isfinite(self.net_value):
                raise EvidenceContractError("log_return 净值路径不是有限数值")
            self._update_drawdown()

    def _update_drawdown(self) -> None:
        self.running_peak = max(self.running_peak, self.net_value)
        drawdown = self.net_value / self.running_peak - 1.0
        self.max_drawdown = min(self.max_drawdown, drawdown)

    def statistics(
        self,
        aggregations: tuple[str, ...],
        *,
        periods_per_year: int | None,
    ) -> AnalysisStatistics:
        selected = set(aggregations)
        annualized = None
        if "geometric_annualized_return" in selected and self.count:
            if periods_per_year is None:
                raise EvidenceContractError("几何年化缺少 periods_per_year")
            if self.net_value == 0.0:
                annualized = -1.0
            else:
                try:
                    annualized = (
                        self.net_value ** (periods_per_year / self.count) - 1.0
                    )
                except OverflowError as exc:
                    raise EvidenceContractError("几何年化计算溢出") from exc
                if not math.isfinite(annualized):
                    raise EvidenceContractError("几何年化不是有限数值")
        is_return = self.value_semantics in {"simple_return", "log_return"}
        return AnalysisStatistics(
            count=self.count if "count" in selected else None,
            mean=(self.total / self.count) if "mean" in selected and self.count else None,
            positive_count=(
                self.positive_count if "sign_counts" in selected else None
            ),
            negative_count=(
                self.negative_count if "sign_counts" in selected else None
            ),
            zero_count=self.zero_count if "sign_counts" in selected else None,
            best_value=self.best_value if "best_period" in selected else None,
            best_date=self.best_date if "best_period" in selected else None,
            worst_value=self.worst_value if "worst_period" in selected else None,
            worst_date=self.worst_date if "worst_period" in selected else None,
            first_value=self.first_value if "first_value" in selected else None,
            last_value=self.last_value if "last_value" in selected else None,
            absolute_change=(
                self.last_value - self.first_value
                if "absolute_change" in selected
                and self.first_value is not None
                and self.last_value is not None
                else None
            ),
            compounded_return=(
                self.net_value - 1.0
                if "compounded_return" in selected and is_return and self.count
                else None
            ),
            geometric_annualized_return=annualized,
            max_drawdown=(
                self.max_drawdown
                if "max_drawdown" in selected and is_return and self.count
                else None
            ),
        )


def _normalize_temporal(
    value: object,
) -> tuple[date | datetime, date, str]:
    if isinstance(value, datetime):
        return value, value.date(), value.isoformat()
    if isinstance(value, date):
        return value, value, value.isoformat()
    raise EvidenceContractError("analysis 日期列包含空值或无法识别的日期")


def _batch_size(memory_bytes: int, date_bits: int, value_bits: int) -> int:
    projected_row_bytes = max(1, (date_bits + value_bits + 7) // 8)
    if memory_bytes < projected_row_bytes:
        raise EvidenceContractError("analysis memory budget 不足以容纳一个投影行")
    return min(MAX_ANALYSIS_BATCH_ROWS, memory_bytes // projected_row_bytes)


def _build_year_results(
    request: AnalysisRequest,
    yearly: Mapping[int, _Accumulator],
) -> tuple[AnalysisYear, ...]:
    if "calendar_year" not in request.aggregations:
        return ()
    expected = request.policies.expected_observations_per_year
    values = []
    for year in range(
        date.fromisoformat(request.window.start).year,
        date.fromisoformat(request.window.end).year + 1,
    ):
        accumulator = yearly.get(year, _Accumulator(request.semantics.value_semantics))
        if expected is None:
            completeness = "not_assessed"
        else:
            completeness = "complete" if accumulator.count == expected else "incomplete"
        if completeness == "incomplete" and request.policies.incomplete_year == "reject":
            raise EvidenceContractError(f"日历年 {year} 的观察数不完整")
        included = not (
            completeness == "incomplete"
            and request.policies.incomplete_year == "exclude"
        )
        values.append(AnalysisYear(
            year=year,
            completeness=completeness,
            included=included,
            observation_count=accumulator.count,
            statistics=(
                accumulator.statistics(
                    request.aggregations,
                    periods_per_year=request.semantics.periods_per_year,
                )
                if included
                else None
            ),
        ))
    return tuple(values)


def _comparison_reasons(
    results: tuple[AnalysisResult, ...],
    measure: str,
) -> set[str]:
    reasons: set[str] = set()
    first = results[0]
    if any(item.analysis_spec_hash != first.analysis_spec_hash for item in results[1:]):
        reasons.add("analysis_spec_mismatch")
    if any(
        (item.actual_start, item.actual_end) != (first.actual_start, first.actual_end)
        for item in results[1:]
    ):
        reasons.add("actual_window_mismatch")
    if any(item.table.date_arrow_type != first.table.date_arrow_type for item in results[1:]):
        reasons.add("date_type_mismatch")
    if any(item.table.value_arrow_type != first.table.value_arrow_type for item in results[1:]):
        reasons.add("value_type_mismatch")
    if any(item.source.claim_level != first.source.claim_level for item in results[1:]):
        reasons.add("claim_level_mismatch")
    if any(item.source.claim_ceiling != first.source.claim_ceiling for item in results[1:]):
        reasons.add("claim_ceiling_mismatch")
    try:
        values = tuple(_measure_value(item, measure) for item in results)
    except EvidenceContractError:
        reasons.add("measure_unavailable")
    else:
        if any(value is None or not math.isfinite(value) for value in values):
            reasons.add("measure_unavailable")
    return reasons


def _measure_value(result: AnalysisResult, measure: str) -> float:
    if not measure.startswith("overall."):
        raise EvidenceContractError("analysis comparison measure 不受支持")
    field = measure.removeprefix("overall.")
    if field not in {
        "mean", "absolute_change", "compounded_return",
        "geometric_annualized_return", "max_drawdown",
    }:
        raise EvidenceContractError("analysis comparison measure 不受支持")
    value = getattr(result.overall, field)
    if value is None:
        raise EvidenceContractError("AnalysisResult 不包含所选比较指标")
    return float(value)


def _comparison_source(result: AnalysisResult) -> dict[str, object]:
    return {
        "analysis_hash": result.analysis_hash,
        "result_reference": result.source.result_reference.to_dict(),
        "package_hash": result.source.package_hash,
        "plan_hash": result.source.plan_hash,
        "runtime_hash": result.source.runtime_hash,
        "implementation_manifest_hash": result.source.implementation_manifest_hash,
        "claim_level": result.source.claim_level,
        "claim_ceiling": result.source.claim_ceiling,
        "table_id": result.table.table_id,
        "schema_id": result.table.schema_id,
        "table_manifest_hash": result.table.table_manifest_hash,
    }


def _rankings(
    values: tuple[tuple[AnalysisResult, float], ...],
    direction: str,
) -> tuple[Mapping[str, object], ...]:
    if direction not in {"higher_is_better", "lower_is_better"}:
        raise EvidenceContractError("analysis comparison direction 不受支持")
    reverse = direction == "higher_is_better"
    ordered = sorted(
        values,
        key=lambda item: (
            -item[1] if reverse else item[1],
            item[0].analysis_hash,
        ),
    )
    best_value = ordered[0][1]
    rankings = []
    previous_value: float | None = None
    rank = 0
    for position, (result, value) in enumerate(ordered, start=1):
        if previous_value is None or value != previous_value:
            rank = position
        rankings.append({
            "analysis_hash": result.analysis_hash,
            "result_reference": result.source.result_reference.to_dict(),
            "value": value,
            "difference_from_best": value - best_value,
            "rank": rank,
        })
        previous_value = value
    return tuple(rankings)


def _load_canonical_mapping(
    source: str | Path,
    label: str,
) -> tuple[str, Mapping[str, object]]:
    try:
        raw = Path(source).read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError(f"{label} 无法读取") from exc
    if not isinstance(payload, Mapping):
        raise EvidenceContractError(f"{label} 必须是对象")
    return raw, payload


def _write_canonical_document(
    destination: str | Path,
    payload: Mapping[str, object],
) -> Path:
    """同目录原子发布规范 JSON，且绝不覆盖既有文件。"""

    try:
        return write_text_exclusive_atomic(
            destination,
            canonical_json(dict(payload)),
        )
    except FileExistsError as exc:
        raise EvidenceContractError(
            f"分析输出已存在: {Path(destination).resolve()}"
        ) from exc


__all__ = [
    "DEFAULT_ANALYSIS_MEMORY_BYTES", "analyze_result",
    "compare_analysis_results", "load_analysis_comparison", "load_analysis_request",
    "load_analysis_result", "write_analysis_comparison", "write_analysis_result",
]
