"""只读 DuckDB Arrow Provider。"""

from __future__ import annotations

from pathlib import Path
import shutil
import uuid

from research_pipeline.platform.canonical import typed_canonical_hash

from ..admission import AdmittedQueryPlan
from ..errors import DataPlaneError, ProviderExecutionError
from ..execution_budget import (
    DataPlaneExecutionBudget,
    build_provider_execution_plan,
    require_scratch_root,
)
from ..execution_estimate import ExecutionEstimate
from ..stream import ColumnarStream
from .sql import compile_duckdb_query


class DuckDBColumnarProvider:
    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def open_stream(
        self,
        plan: AdmittedQueryPlan,
        *,
        execution_budget: DataPlaneExecutionBudget,
        execution_estimate: ExecutionEstimate,
        scratch_root: str | Path,
    ) -> ColumnarStream:
        return self._open_stream(
            plan,
            execution_budget=execution_budget,
            execution_estimate=execution_estimate,
            scratch_root=scratch_root,
            preserve_temporal_facts=False,
        )

    def open_temporal_source_stream(
        self,
        plan: AdmittedQueryPlan,
        *,
        execution_budget: DataPlaneExecutionBudget,
        execution_estimate: ExecutionEstimate,
        scratch_root: str | Path,
    ) -> ColumnarStream:
        if not plan.temporal_selection.requires_consumer_binding:
            raise ProviderExecutionError("当前计划不需要保留逐决策时点的版本事实")
        return self._open_stream(
            plan,
            execution_budget=execution_budget,
            execution_estimate=execution_estimate,
            scratch_root=scratch_root,
            preserve_temporal_facts=True,
        )

    def _open_stream(
        self,
        plan: AdmittedQueryPlan,
        *,
        execution_budget: DataPlaneExecutionBudget,
        execution_estimate: ExecutionEstimate,
        scratch_root: str | Path,
        preserve_temporal_facts: bool,
    ) -> ColumnarStream:
        import duckdb

        if not self.database.is_file():
            raise ProviderExecutionError(f"只读数据库不存在: {self.database.name}")
        allocation = build_provider_execution_plan(
            plan,
            execution_budget,
            variable_width_upper=dict(execution_estimate.variable_width_upper),
        )
        if (
            execution_estimate.query_scope_hash
            != typed_canonical_hash(plan.query.to_dict())
            or execution_estimate.object_name != plan.object_name
            or not execution_estimate.accepts_runtime_budget(execution_budget)
        ):
            raise ProviderExecutionError(
                "ExecutionEstimate 与当前 request 或节点预算不一致"
            )
        scratch = require_scratch_root(scratch_root)
        temp_directory: Path | None = None
        if allocation.temp_bytes:
            temp_directory = scratch / f"duckdb-temp-{uuid.uuid4().hex}"
            temp_directory.mkdir()
        connection = None
        try:
            connection = duckdb.connect(str(self.database), read_only=True)
            connection.execute(f"SET threads = {allocation.cpu_slots:d}")
            connection.execute(
                f"SET memory_limit = '{allocation.duckdb_memory_bytes:d}B'"
            )
            if temp_directory is None:
                connection.execute("SET temp_directory = ''")
            else:
                escaped = str(temp_directory).replace("'", "''")
                connection.execute(f"SET temp_directory = '{escaped}'")
                connection.execute(
                    "SET max_temp_directory_size = "
                    f"'{allocation.temp_bytes:d}B'"
                )
            sql, params = compile_duckdb_query(
                plan,
                preserve_temporal_facts=preserve_temporal_facts,
            )
            reader = connection.execute(sql, params).to_arrow_reader(
                allocation.batch_rows
            )
            expected_fields = (
                plan.temporal_selection.required_scan_fields
                if preserve_temporal_facts
                else plan.query.field_ids
            )
            if tuple(reader.schema.names) != expected_fields:
                raise ProviderExecutionError("Provider 输出字段顺序与 QueryPlan 不一致")
            return ColumnarStream(
                reader,
                connection,
                max_rows=plan.query.budget.max_rows,
                max_bytes=plan.query.budget.max_bytes,
                on_close=(
                    None
                    if temp_directory is None
                    else lambda: shutil.rmtree(temp_directory, ignore_errors=True)
                ),
            )
        except DataPlaneError:
            if connection is not None:
                connection.close()
            if temp_directory is not None:
                shutil.rmtree(temp_directory, ignore_errors=True)
            raise
        except Exception as exc:
            if connection is not None:
                connection.close()
            if temp_directory is not None:
                shutil.rmtree(temp_directory, ignore_errors=True)
            raise ProviderExecutionError(
                f"DuckDB 只读查询失败: {type(exc).__name__}"
            ) from exc
        except BaseException:
            if connection is not None:
                connection.close()
            if temp_directory is not None:
                shutil.rmtree(temp_directory, ignore_errors=True)
            raise


__all__ = ["DuckDBColumnarProvider"]
