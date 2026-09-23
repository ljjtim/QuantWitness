"""正式准入阶段的有限查询执行规模估算。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from research_pipeline.catalog.discovery import ObjectExecutionEvidence
from research_pipeline.platform.canonical import typed_canonical_hash

from .admission import AdmittedQueryPlan
from .errors import ProviderExecutionError
from .execution_budget import (
    DataPlaneExecutionBudget,
    build_provider_execution_plan,
)


EXECUTION_ESTIMATE_VERSION = "data-plane-execution-estimate-v3"
_EXPANSION_BOUND_METHODS = frozenset(
    {
        "catalog_hard_upper_v1",
        "database_scope_stat_v1",
        "database_scope_json_array_length_v1",
    }
)
_PARTITION_BOUND_METHODS = frozenset({"parquet_footer_month_scope_v1"})


@dataclass(frozen=True)
class ExecutionEstimate:
    """与一个 request、数据库 revision 和节点预算绑定的执行上界。"""

    object_name: str
    object_kind: str
    dependency_chain: tuple[str, ...]
    query_scope_hash: str
    database_revision: str
    object_evidence_hash: str
    source_rows_upper: int
    expanded_rows_upper: int | None
    intermediate_rows_upper: int
    projected_row_width_upper: int
    intermediate_bytes_upper: int
    sort_working_set_bytes: int
    window_working_set_bytes: int
    required_temp_bytes: int
    provider_duckdb_memory_bytes: int
    provider_batch_writer_memory_bytes: int
    provider_process_reserve_bytes: int
    provider_batch_rows: int
    output_max_rows: int
    output_max_bytes: int
    execution_memory_bytes: int
    execution_temp_bytes: int
    execution_cpu_slots: int
    variable_width_upper: tuple[tuple[str, int], ...]
    has_json_expansion: bool
    has_window: bool
    has_order_by: bool
    may_spill: bool
    expansion_bound_method: str | None
    dependency_edges: tuple[tuple[str, str], ...] = ()
    partition_count: int = 0
    partition_rows_upper: int | None = None
    partition_uncompressed_bytes_upper: int | None = None
    partition_key: str | None = None
    partition_bound_method: str | None = None
    method: str = "provider_allocation_envelope_v1"
    contract_version: str = EXECUTION_ESTIMATE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != EXECUTION_ESTIMATE_VERSION:
            raise ProviderExecutionError("ExecutionEstimate 版本不受支持")
        integer_fields = (
            "source_rows_upper",
            "intermediate_rows_upper",
            "projected_row_width_upper",
            "intermediate_bytes_upper",
            "sort_working_set_bytes",
            "window_working_set_bytes",
            "required_temp_bytes",
            "provider_duckdb_memory_bytes",
            "provider_batch_writer_memory_bytes",
            "provider_process_reserve_bytes",
            "provider_batch_rows",
            "output_max_rows",
            "output_max_bytes",
            "execution_memory_bytes",
            "execution_temp_bytes",
            "execution_cpu_slots",
        )
        if any(type(getattr(self, field)) is not int for field in integer_fields):
            raise ProviderExecutionError("ExecutionEstimate 数值字段必须为整数")
        if any(getattr(self, field) < 0 for field in integer_fields):
            raise ProviderExecutionError("ExecutionEstimate 数值字段不能为负")
        if self.execution_cpu_slots <= 0 or self.execution_memory_bytes <= 0:
            raise ProviderExecutionError("ExecutionEstimate 执行资源无效")
        provider_parts = (
            self.provider_duckdb_memory_bytes,
            self.provider_batch_writer_memory_bytes,
            self.provider_process_reserve_bytes,
            self.provider_batch_rows,
        )
        if any(value <= 0 for value in provider_parts):
            raise ProviderExecutionError("ExecutionEstimate provider 分配必须为正整数")
        if (
            self.provider_duckdb_memory_bytes
            + self.provider_batch_writer_memory_bytes
            + self.provider_process_reserve_bytes
            != self.execution_memory_bytes
        ):
            raise ProviderExecutionError(
                "ExecutionEstimate provider 内存分配与批准包络不闭合"
            )
        if self.method != "provider_allocation_envelope_v1":
            raise ProviderExecutionError("ExecutionEstimate 分配方法不受支持")
        if self.expanded_rows_upper is not None and (
            type(self.expanded_rows_upper) is not int
            or self.expanded_rows_upper < 0
        ):
            raise ProviderExecutionError("ExecutionEstimate 展开行数上界无效")
        if self.has_json_expansion and (
            self.expanded_rows_upper is None
            or self.expansion_bound_method not in _EXPANSION_BOUND_METHODS
        ):
            raise ProviderExecutionError("JSON 展开缺少生产数值上界")
        if not self.has_json_expansion and self.expansion_bound_method is not None:
            raise ProviderExecutionError("非展开对象不得声明展开倍率依据")
        if self.variable_width_upper != tuple(sorted(self.variable_width_upper)):
            raise ProviderExecutionError("变长字段上界必须稳定排序")
        if self.dependency_edges != tuple(sorted(set(self.dependency_edges))):
            raise ProviderExecutionError("ExecutionEstimate 依赖边必须唯一并排序")
        if self.partition_bound_method is None:
            if any(
                value not in {0, None}
                for value in (
                    self.partition_count,
                    self.partition_rows_upper,
                    self.partition_uncompressed_bytes_upper,
                    self.partition_key,
                )
            ):
                raise ProviderExecutionError("分区估算字段不能脱离分区方法")
        else:
            if self.partition_bound_method not in _PARTITION_BOUND_METHODS:
                raise ProviderExecutionError("ExecutionEstimate 分区方法不受支持")
            if type(self.partition_count) is not int or self.partition_count < 0:
                raise ProviderExecutionError("ExecutionEstimate 分区数量无效")
            for field in (
                "partition_rows_upper",
                "partition_uncompressed_bytes_upper",
            ):
                value = getattr(self, field)
                if type(value) is not int or value < 0:
                    raise ProviderExecutionError(
                        f"ExecutionEstimate {field} 无效"
                    )
            if self.partition_count == 0:
                if (
                    self.partition_rows_upper != 0
                    or self.partition_uncompressed_bytes_upper != 0
                    or self.partition_key is not None
                ):
                    raise ProviderExecutionError("ExecutionEstimate 空分区证据无效")
            elif (
                self.partition_rows_upper == 0
                or self.partition_uncompressed_bytes_upper == 0
                or not self.partition_key
            ):
                raise ProviderExecutionError("ExecutionEstimate 分区证据不完整")
        for field in (
            "query_scope_hash",
            "database_revision",
            "object_evidence_hash",
        ):
            value = getattr(self, field)
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ProviderExecutionError(f"ExecutionEstimate {field} 无效")

    @property
    def estimate_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @property
    def execution_budget(self) -> dict[str, int]:
        return {
            "memory_bytes": self.execution_memory_bytes,
            "temp_bytes": self.execution_temp_bytes,
            "cpu_slots": self.execution_cpu_slots,
        }

    def accepts_runtime_budget(self, budget: DataPlaneExecutionBudget) -> bool:
        """准入预算是最低执行证明；同次运行可以从总容量借用更高上限。"""

        return (
            budget.memory_bytes >= self.execution_memory_bytes
            and budget.temp_bytes >= self.execution_temp_bytes
            and budget.cpu_slots >= self.execution_cpu_slots
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "object_name": self.object_name,
            "object_kind": self.object_kind,
            "dependency_chain": list(self.dependency_chain),
            "query_scope_hash": self.query_scope_hash,
            "database_revision": self.database_revision,
            "object_evidence_hash": self.object_evidence_hash,
            "source_rows_upper": self.source_rows_upper,
            "expanded_rows_upper": self.expanded_rows_upper,
            "intermediate_rows_upper": self.intermediate_rows_upper,
            "projected_row_width_upper": self.projected_row_width_upper,
            "intermediate_bytes_upper": self.intermediate_bytes_upper,
            "sort_working_set_bytes": self.sort_working_set_bytes,
            "window_working_set_bytes": self.window_working_set_bytes,
            "required_temp_bytes": self.required_temp_bytes,
            "provider_allocation": {
                "duckdb_memory_bytes": self.provider_duckdb_memory_bytes,
                "batch_writer_memory_bytes": self.provider_batch_writer_memory_bytes,
                "process_reserve_bytes": self.provider_process_reserve_bytes,
                "batch_rows": self.provider_batch_rows,
            },
            "output_max_rows": self.output_max_rows,
            "output_max_bytes": self.output_max_bytes,
            "execution_budget": self.execution_budget,
            "variable_width_upper": dict(self.variable_width_upper),
            "has_json_expansion": self.has_json_expansion,
            "has_window": self.has_window,
            "has_order_by": self.has_order_by,
            "may_spill": self.may_spill,
            "expansion_bound_method": self.expansion_bound_method,
            "dependency_edges": [list(edge) for edge in self.dependency_edges],
            "partition_count": self.partition_count,
            "partition_rows_upper": self.partition_rows_upper,
            "partition_uncompressed_bytes_upper": self.partition_uncompressed_bytes_upper,
            "partition_key": self.partition_key,
            "partition_bound_method": self.partition_bound_method,
            "method": self.method,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "ExecutionEstimate":
        expected = {
            "contract_version",
            "object_name",
            "object_kind",
            "dependency_chain",
            "query_scope_hash",
            "database_revision",
            "object_evidence_hash",
            "source_rows_upper",
            "expanded_rows_upper",
            "intermediate_rows_upper",
            "projected_row_width_upper",
            "intermediate_bytes_upper",
            "sort_working_set_bytes",
            "window_working_set_bytes",
            "required_temp_bytes",
            "provider_allocation",
            "output_max_rows",
            "output_max_bytes",
            "execution_budget",
            "variable_width_upper",
            "has_json_expansion",
            "has_window",
            "has_order_by",
            "may_spill",
            "expansion_bound_method",
            "dependency_edges",
            "partition_count",
            "partition_rows_upper",
            "partition_uncompressed_bytes_upper",
            "partition_key",
            "partition_bound_method",
            "method",
        }
        if set(value) != expected:
            raise ProviderExecutionError("ExecutionEstimate schema 无效")
        raw_budget = value["execution_budget"]
        raw_allocation = value["provider_allocation"]
        raw_widths = value["variable_width_upper"]
        raw_chain = value["dependency_chain"]
        raw_edges = value["dependency_edges"]
        if (
            not isinstance(raw_budget, Mapping)
            or set(raw_budget) != {"memory_bytes", "temp_bytes", "cpu_slots"}
            or not isinstance(raw_allocation, Mapping)
            or set(raw_allocation)
            != {
                "duckdb_memory_bytes",
                "batch_writer_memory_bytes",
                "process_reserve_bytes",
                "batch_rows",
            }
            or not isinstance(raw_widths, Mapping)
            or not isinstance(raw_chain, (list, tuple))
            or not isinstance(raw_edges, (list, tuple))
            or any(
                not isinstance(edge, (list, tuple)) or len(edge) != 2
                for edge in raw_edges
            )
        ):
            raise ProviderExecutionError("ExecutionEstimate 复合字段无效")
        return cls(
            object_name=str(value["object_name"]),
            object_kind=str(value["object_kind"]),
            dependency_chain=tuple(str(item) for item in raw_chain),
            query_scope_hash=str(value["query_scope_hash"]),
            database_revision=str(value["database_revision"]),
            object_evidence_hash=str(value["object_evidence_hash"]),
            source_rows_upper=int(value["source_rows_upper"]),
            expanded_rows_upper=(
                None
                if value["expanded_rows_upper"] is None
                else int(value["expanded_rows_upper"])
            ),
            intermediate_rows_upper=int(value["intermediate_rows_upper"]),
            projected_row_width_upper=int(value["projected_row_width_upper"]),
            intermediate_bytes_upper=int(value["intermediate_bytes_upper"]),
            sort_working_set_bytes=int(value["sort_working_set_bytes"]),
            window_working_set_bytes=int(value["window_working_set_bytes"]),
            required_temp_bytes=int(value["required_temp_bytes"]),
            provider_duckdb_memory_bytes=int(raw_allocation["duckdb_memory_bytes"]),
            provider_batch_writer_memory_bytes=int(
                raw_allocation["batch_writer_memory_bytes"]
            ),
            provider_process_reserve_bytes=int(
                raw_allocation["process_reserve_bytes"]
            ),
            provider_batch_rows=int(raw_allocation["batch_rows"]),
            output_max_rows=int(value["output_max_rows"]),
            output_max_bytes=int(value["output_max_bytes"]),
            execution_memory_bytes=int(raw_budget["memory_bytes"]),
            execution_temp_bytes=int(raw_budget["temp_bytes"]),
            execution_cpu_slots=int(raw_budget["cpu_slots"]),
            variable_width_upper=tuple(
                sorted((str(field), int(width)) for field, width in raw_widths.items())
            ),
            has_json_expansion=value["has_json_expansion"] is True,
            has_window=value["has_window"] is True,
            has_order_by=value["has_order_by"] is True,
            may_spill=value["may_spill"] is True,
            expansion_bound_method=(
                None
                if value["expansion_bound_method"] is None
                else str(value["expansion_bound_method"])
            ),
            dependency_edges=tuple(
                sorted((str(edge[0]), str(edge[1])) for edge in raw_edges)
            ),
            partition_count=int(value["partition_count"]),
            partition_rows_upper=(
                None
                if value["partition_rows_upper"] is None
                else int(value["partition_rows_upper"])
            ),
            partition_uncompressed_bytes_upper=(
                None
                if value["partition_uncompressed_bytes_upper"] is None
                else int(value["partition_uncompressed_bytes_upper"])
            ),
            partition_key=(
                None if value["partition_key"] is None else str(value["partition_key"])
            ),
            partition_bound_method=(
                None
                if value["partition_bound_method"] is None
                else str(value["partition_bound_method"])
            ),
            method=str(value["method"]),
            contract_version=str(value["contract_version"]),
        )


def build_execution_estimate(
    plan: AdmittedQueryPlan,
    *,
    evidence: ObjectExecutionEvidence,
    execution_budget: DataPlaneExecutionBudget,
) -> ExecutionEstimate:
    """只接受当前范围证据；未知展开或超出 memory/temp 时准入失败。"""

    query_scope_hash = typed_canonical_hash(plan.query.to_dict())
    if evidence.query_scope_hash != query_scope_hash:
        raise ProviderExecutionError("对象执行证据与当前 QueryIR 查询范围不一致")
    if evidence.object_name != plan.object_name:
        raise ProviderExecutionError("对象执行证据与 admitted binding 不一致")
    if evidence.has_json_expansion and (
        evidence.expanded_rows_upper is None
        or evidence.expansion_bound_method not in _EXPANSION_BOUND_METHODS
    ):
        raise ProviderExecutionError(
            f"object={plan.object_name} 的 JSON 展开缺少 Catalog 硬上界或"
            "本次数据库范围统计上界"
        )
    if evidence.source_rows_upper is None:
        raise ProviderExecutionError(
            f"object={plan.object_name} 缺少与本次查询范围绑定的来源行数上界"
        )
    variable_widths = dict(evidence.variable_width_upper)
    allocation = build_provider_execution_plan(
        plan,
        execution_budget,
        variable_width_upper=variable_widths,
    )
    intermediate_rows = (
        evidence.partition_rows_upper
        if evidence.partition_bound_method is not None
        else (
            evidence.expanded_rows_upper
            if evidence.has_json_expansion
            else evidence.source_rows_upper
        )
    )
    if intermediate_rows is None:  # pragma: no cover - 前置条件已关闭
        raise ProviderExecutionError("执行中间行数无法有界")
    row_width = allocation.projected_row_width_upper
    row_bytes = intermediate_rows * row_width
    intermediate_bytes = max(
        row_bytes,
        evidence.partition_uncompressed_bytes_upper or 0,
    )
    output_rows = min(
        plan.query.budget.max_rows,
        plan.query.limit
        if plan.query.limit is not None
        else plan.query.budget.max_rows,
    )
    # Provider 使用 max_rows + 1 的 Top-N 观察预算超限；工作集因此不需要
    # 随完整来源行数增长。窗口选择仍必须看到完整范围，不能按输出上限缩小。
    sort_rows = min(intermediate_rows, output_rows + 1)
    sort_bytes = (
        intermediate_bytes
        if evidence.partition_bound_method is not None
        else sort_rows * row_width
    )
    temporal = plan.temporal_selection
    needs_window = evidence.has_window or any(
        item is not None
        for item in (
            temporal.revision_selector,
            temporal.effective_interval_selector,
        )
    )
    window_bytes = intermediate_bytes if needs_window else 0
    dependency_children: dict[str, int] = {}
    for parent, _child in evidence.dependency_edges:
        dependency_children[parent] = dependency_children.get(parent, 0) + 1
    has_relational_branch = any(
        child_count > 1 for child_count in dependency_children.values()
    )
    duckdb_working_set = max(
        sort_bytes,
        window_bytes,
        intermediate_bytes if evidence.has_json_expansion else 0,
        intermediate_bytes if has_relational_branch else 0,
    )
    if needs_window and duckdb_working_set > allocation.duckdb_memory_bytes:
        raise ProviderExecutionError(
            f"object={plan.object_name} 的窗口工作集超过 execution memory；"
            "请缩小查询范围或提高节点 memory"
        )
    required_temp = max(0, duckdb_working_set - allocation.duckdb_memory_bytes)
    if required_temp > allocation.temp_bytes:
        raise ProviderExecutionError(
            f"object={plan.object_name} 的排序/展开工作集超过 execution memory/temp；"
            "请缩小查询范围、补对象上界或调整节点资源"
        )
    return ExecutionEstimate(
        object_name=evidence.object_name,
        object_kind=evidence.object_kind,
        dependency_chain=evidence.dependency_chain,
        query_scope_hash=query_scope_hash,
        database_revision=evidence.database_revision,
        object_evidence_hash=evidence.evidence_hash,
        source_rows_upper=evidence.source_rows_upper,
        expanded_rows_upper=evidence.expanded_rows_upper,
        intermediate_rows_upper=intermediate_rows,
        projected_row_width_upper=row_width,
        intermediate_bytes_upper=intermediate_bytes,
        sort_working_set_bytes=sort_bytes,
        window_working_set_bytes=window_bytes,
        required_temp_bytes=required_temp,
        provider_duckdb_memory_bytes=allocation.duckdb_memory_bytes,
        provider_batch_writer_memory_bytes=allocation.batch_writer_memory_bytes,
        provider_process_reserve_bytes=allocation.process_reserve_bytes,
        provider_batch_rows=allocation.batch_rows,
        output_max_rows=plan.query.budget.max_rows,
        output_max_bytes=plan.query.budget.max_bytes,
        execution_memory_bytes=execution_budget.memory_bytes,
        execution_temp_bytes=execution_budget.temp_bytes,
        execution_cpu_slots=execution_budget.cpu_slots,
        variable_width_upper=tuple(sorted(variable_widths.items())),
        has_json_expansion=evidence.has_json_expansion,
        has_window=needs_window,
        has_order_by=True,
        may_spill=required_temp > 0,
        expansion_bound_method=evidence.expansion_bound_method,
        dependency_edges=evidence.dependency_edges,
        partition_count=evidence.partition_count,
        partition_rows_upper=evidence.partition_rows_upper,
        partition_uncompressed_bytes_upper=evidence.partition_uncompressed_bytes_upper,
        partition_key=evidence.partition_key,
        partition_bound_method=evidence.partition_bound_method,
    )


def load_execution_estimates(
    value: object,
    *,
    request_ids: tuple[str, ...],
) -> dict[str, ExecutionEstimate]:
    if not isinstance(value, Mapping) or set(value) != set(request_ids):
        raise ProviderExecutionError("正式计划 execution estimates 集合不闭合")
    estimates = {}
    for request_id in sorted(request_ids):
        raw = value[request_id]
        if not isinstance(raw, Mapping):
            raise ProviderExecutionError(
                f"request={request_id} ExecutionEstimate 不是对象"
            )
        estimates[request_id] = ExecutionEstimate.from_dict(raw)
    return estimates


__all__ = [
    "EXECUTION_ESTIMATE_VERSION",
    "ExecutionEstimate",
    "build_execution_estimate",
    "load_execution_estimates",
]
