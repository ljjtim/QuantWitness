"""列式数据平面的节点执行资源合同。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from .admission import AdmittedQueryPlan
from .errors import ProviderExecutionError
from .query_ir import FilterOperator, InstantRangeV2, QueryIR


_FIXED_WIDTHS = {
    "bool": 1,
    "int8": 1,
    "int16": 2,
    "int32": 4,
    "date32": 4,
    "float64": 8,
    "int64": 8,
    "timestamp": 8,
    "timestamp[us]": 8,
    "decimal128": 16,
}
_BATCH_LIVE_COPIES = 3  # 当前 batch、分区 take 副本、单 writer 缓冲。
_DUCKDB_MIN_BATCH_COPIES = 1  # DuckDB 至少保留一个同规模执行批次。
_CANONICAL_INSTRUMENT_UTF8_BYTES = 64
_MIB = 1024**2
_MIN_PROVIDER_PROCESS_MEMORY_BYTES = 256 * _MIB
_PROVIDER_PROCESS_RESERVE_FLOOR_BYTES = 96 * _MIB
_PROVIDER_PROCESS_RESERVE_RATIO_DENOMINATOR = 8


@dataclass(frozen=True)
class DataPlaneExecutionBudget:
    """Runtime 批准给单次 provider 的进程树资源包络。"""

    memory_bytes: int
    temp_bytes: int
    cpu_slots: int

    def __post_init__(self) -> None:
        if type(self.memory_bytes) is not int or self.memory_bytes <= 0:
            raise ProviderExecutionError("data-plane execution memory_bytes 必须为正整数")
        if type(self.temp_bytes) is not int or self.temp_bytes < 0:
            raise ProviderExecutionError("data-plane execution temp_bytes 不能为负数")
        if type(self.cpu_slots) is not int or self.cpu_slots <= 0:
            raise ProviderExecutionError("data-plane execution cpu_slots 必须为正整数")

    @classmethod
    def from_resource_budget(cls, budget: object) -> "DataPlaneExecutionBudget":
        return cls(
            memory_bytes=getattr(budget, "memory_bytes", None),
            temp_bytes=getattr(budget, "temp_bytes", None),
            cpu_slots=getattr(budget, "cpu_slots", None),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "memory_bytes": self.memory_bytes,
            "temp_bytes": self.temp_bytes,
            "cpu_slots": self.cpu_slots,
        }


@dataclass(frozen=True)
class ProviderExecutionPlan:
    """一次 provider 调用的资源分配；不声称是任意 allocator 的数学上界。"""

    approved_process_memory_bytes: int
    duckdb_memory_bytes: int
    temp_bytes: int
    cpu_slots: int
    batch_rows: int
    projected_row_width_upper: int
    max_batch_bytes: int
    batch_writer_memory_bytes: int
    process_reserve_bytes: int

    def __post_init__(self) -> None:
        values = self.to_dict()
        if any(type(value) is not int or value <= 0 for key, value in values.items() if key != "temp_bytes"):
            raise ProviderExecutionError("provider execution plan 必须为正整数")
        if type(self.temp_bytes) is not int or self.temp_bytes < 0:
            raise ProviderExecutionError("provider execution temp_bytes 不能为负数")

    def to_dict(self) -> dict[str, int]:
        return {
            "approved_process_memory_bytes": self.approved_process_memory_bytes,
            "duckdb_memory_bytes": self.duckdb_memory_bytes,
            "temp_bytes": self.temp_bytes,
            "cpu_slots": self.cpu_slots,
            "batch_rows": self.batch_rows,
            "projected_row_width_upper": self.projected_row_width_upper,
            "max_batch_bytes": self.max_batch_bytes,
            "batch_writer_memory_bytes": self.batch_writer_memory_bytes,
            "process_reserve_bytes": self.process_reserve_bytes,
        }


def validate_provider_execution_budget(
    execution_budget: DataPlaneExecutionBudget,
) -> None:
    """在打开对象统计或数据扫描前拒绝不在已校准支持包络内的预算。"""

    if execution_budget.memory_bytes < _MIN_PROVIDER_PROCESS_MEMORY_BYTES:
        raise ProviderExecutionError(
            "data-plane provider memory_bytes 低于已校准的 256 MiB 进程包络；"
            "请提高数据节点 memory，不能靠缩样本继续"
        )


def _provider_process_reserve_bytes(memory_bytes: int) -> int:
    """给 Python、Arrow、连接和 allocator 留出固定/比例校准余量。"""

    return max(
        _PROVIDER_PROCESS_RESERVE_FLOOR_BYTES,
        memory_bytes // _PROVIDER_PROCESS_RESERVE_RATIO_DENOMINATOR,
    )


def bind_data_plane_request_budgets(
    *,
    request_queries: Mapping[str, QueryIR],
    recipe_nodes: Iterable[object],
    dag: object,
) -> dict[str, DataPlaneExecutionBudget]:
    """把每个 QueryIR 绑定到实际读取它的数据节点预算。"""

    if not request_queries or any(
        not isinstance(request_id, str) or not request_id
        for request_id in request_queries
    ):
        raise ProviderExecutionError("data-plane request 集合无效")
    dag_nodes = tuple(getattr(dag, "nodes", ()))
    dag_by_id = {
        getattr(node, "node_id", None): node
        for node in dag_nodes
    }
    if len(dag_by_id) != len(dag_nodes) or None in dag_by_id:
        raise ProviderExecutionError("DAG 节点身份无效")

    materialize_nodes: list[tuple[str, DataPlaneExecutionBudget]] = []
    minute_owners: dict[str, list[tuple[str, DataPlaneExecutionBudget]]] = {}
    for recipe_node in recipe_nodes:
        operator_id = getattr(recipe_node, "operator_id", None)
        if operator_id not in {
            "data.columnar.materialize",
            "data.minute.scan",
        }:
            continue
        node_id = getattr(recipe_node, "node_id", None)
        dag_node = dag_by_id.get(node_id)
        if not isinstance(node_id, str) or dag_node is None:
            raise ProviderExecutionError(
                f"data-plane 节点没有对应 DAG ResourceBudget: {node_id}"
            )
        budget = DataPlaneExecutionBudget.from_resource_budget(
            getattr(dag_node, "resource_budget", None)
        )
        if operator_id == "data.columnar.materialize":
            materialize_nodes.append((node_id, budget))
            continue

        parameters = getattr(recipe_node, "parameters", None)
        raw_request_ids = (
            parameters.get("request_ids")
            if isinstance(parameters, Mapping)
            else None
        )
        if (
            not isinstance(raw_request_ids, (list, tuple))
            or len(raw_request_ids) != 1
            or not isinstance(raw_request_ids[0], str)
            or not raw_request_ids[0]
        ):
            raise ProviderExecutionError(
                f"分钟节点={node_id} 必须恰好绑定一个 request_id"
            )
        request_id = raw_request_ids[0]
        query = request_queries.get(request_id)
        if query is None:
            raise ProviderExecutionError(
                f"分钟节点={node_id} 绑定了未知 request={request_id}"
            )
        if not isinstance(query.time_range, InstantRangeV2):
            raise ProviderExecutionError(
                f"非分钟 request={request_id} 不能绑定 data.minute.scan"
            )
        minute_owners.setdefault(request_id, []).append((node_id, budget))

    if len(materialize_nodes) > 1:
        raise ProviderExecutionError(
            "非分钟 request 必须共用唯一 data.columnar.materialize 节点"
        )
    result: dict[str, DataPlaneExecutionBudget] = {}
    for request_id, query in sorted(request_queries.items()):
        if isinstance(query.time_range, InstantRangeV2):
            owners = minute_owners.get(request_id, [])
            if len(owners) != 1:
                raise ProviderExecutionError(
                    f"分钟 request={request_id} 必须恰好绑定一个 data.minute.scan 节点"
                )
            result[request_id] = owners[0][1]
            continue
        if request_id in minute_owners:
            raise ProviderExecutionError(
                f"非分钟 request={request_id} 不能绑定 data.minute.scan"
            )
        if len(materialize_nodes) != 1:
            raise ProviderExecutionError(
                f"非分钟 request={request_id} 缺少唯一 data.columnar.materialize 节点"
            )
        result[request_id] = materialize_nodes[0][1]
    return result


def _bounded_string_width(plan: AdmittedQueryPlan, field_id: str) -> int | None:
    if field_id == plan.instrument_field and plan.query.universe.instruments:
        return max(len(value.encode("utf-8")) for value in plan.query.universe.instruments)
    bounds: list[int] = []
    for predicate in plan.query.filters:
        if predicate.field_id != field_id or predicate.operator not in {
            FilterOperator.EQ,
            FilterOperator.IN,
        }:
            continue
        if all(isinstance(value, str) for value in predicate.values):
            bounds.append(max(len(value.encode("utf-8")) for value in predicate.values))
    if bounds:
        return min(bounds)
    if field_id == plan.instrument_field:
        # 当前平台的证券标识使用规范化市场代码；64 字节是执行合同硬上限，
        # 避免为分钟 Parquet 的代码列扫描数十亿行来推导宽度。
        return _CANONICAL_INSTRUMENT_UTF8_BYTES
    return None


def projected_row_width_upper(
    plan: AdmittedQueryPlan,
    *,
    variable_width_upper: dict[str, int] | None = None,
) -> int:
    """按本次真实扫描列计算单行 Arrow 字节上界。"""

    widths = dict(plan.field_types)
    fields = (
        plan.temporal_selection.required_scan_fields
        if plan.temporal_selection.requires_consumer_binding
        else plan.query.field_ids
    )
    observed_widths = variable_width_upper or {}
    total = 0
    for field_id in fields:
        logical_type = widths[field_id].lower()
        fixed = _FIXED_WIDTHS.get(logical_type)
        if fixed is not None:
            total += fixed
            continue
        if logical_type == "string":
            value_bytes = _bounded_string_width(plan, field_id)
            if value_bytes is None:
                value_bytes = observed_widths.get(field_id)
            if value_bytes is None:
                raise ProviderExecutionError(
                    f"field={field_id} 是变长列但没有单行 UTF-8 字节上界；"
                    "请在 Catalog 或本次只读对象证据中提供硬上界"
                )
            total += 4 + value_bytes  # Arrow UTF-8 offset 与值字节。
            continue
        raise ProviderExecutionError(f"field={field_id} 的字节宽度无法有界: {logical_type}")
    # 每列 validity bitmap 最多占一 bit；向上取整到字节。
    return total + (len(fields) + 7) // 8


def build_provider_execution_plan(
    plan: AdmittedQueryPlan,
    execution_budget: DataPlaneExecutionBudget,
    *,
    variable_width_upper: dict[str, int] | None = None,
) -> ProviderExecutionPlan:
    """把批准包络分给 DuckDB、batch/writer 和未逐项计数的进程余量。"""

    validate_provider_execution_budget(execution_budget)

    row_width = projected_row_width_upper(
        plan,
        variable_width_upper=variable_width_upper,
    )
    process_reserve = _provider_process_reserve_bytes(execution_budget.memory_bytes)
    available_for_batches = execution_budget.memory_bytes - process_reserve
    required_copies = _BATCH_LIVE_COPIES + _DUCKDB_MIN_BATCH_COPIES
    if available_for_batches < row_width * required_copies:
        raise ProviderExecutionError(
            "data-plane 节点内存不足以同时容纳一行的 DuckDB 执行批次、"
            "输出 batch、take 副本和 writer 缓冲"
        )
    max_rows_by_memory = available_for_batches // (row_width * required_copies)
    batch_rows = min(plan.query.budget.batch_size, max_rows_by_memory)
    max_batch_bytes = batch_rows * row_width
    batch_and_writer = max_batch_bytes * _BATCH_LIVE_COPIES
    duckdb_memory = execution_budget.memory_bytes - process_reserve - batch_and_writer
    if duckdb_memory <= 0:
        raise ProviderExecutionError(
            "data-plane provider 预算扣除 batch/writer 与进程余量后没有 DuckDB 内存"
        )
    return ProviderExecutionPlan(
        approved_process_memory_bytes=execution_budget.memory_bytes,
        duckdb_memory_bytes=duckdb_memory,
        temp_bytes=execution_budget.temp_bytes,
        cpu_slots=execution_budget.cpu_slots,
        batch_rows=batch_rows,
        projected_row_width_upper=row_width,
        max_batch_bytes=max_batch_bytes,
        batch_writer_memory_bytes=batch_and_writer,
        process_reserve_bytes=process_reserve,
    )


def require_scratch_root(path: str | Path) -> Path:
    root = Path(path).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir():
        raise ProviderExecutionError("data-plane scratch_root 不是目录")
    return root


__all__ = [
    "DataPlaneExecutionBudget",
    "ProviderExecutionPlan",
    "bind_data_plane_request_budgets",
    "build_provider_execution_plan",
    "projected_row_width_upper",
    "require_scratch_root",
    "validate_provider_execution_budget",
]
