"""受安全根和提交标记约束的 Parquet Arrow Provider。"""

from __future__ import annotations

from pathlib import Path
import shutil
import uuid

from ..admission import AdmittedQueryPlan
from ..errors import DataPlaneError, ProviderExecutionError
from ..execution_budget import (
    DataPlaneExecutionBudget,
    build_provider_execution_plan,
    require_scratch_root,
)
from ..execution_estimate import ExecutionEstimate
from research_pipeline.platform.canonical import typed_canonical_hash
from ..path_policy import PathRolePolicy
from ..snapshots import verify_parquet_snapshot
from ..stream import ColumnarStream
from .sql import compile_duckdb_query


class ParquetColumnarProvider:
    def __init__(self, source: str | Path, *, allowed_root: str | Path) -> None:
        self.path_policy = PathRolePolicy()
        try:
            self.allowed_root = self.path_policy.resolve_root(
                allowed_root,
                role="parquet_allowed_root",
            )
            source_path = self.path_policy.resolve_contained_path(
                allowed_root=self.allowed_root,
                candidate=source,
                root_role="parquet_allowed_root",
                path_role="parquet_source",
                expected_kind="any",
            )
        except DataPlaneError as exc:
            if "越出" in str(exc):
                raise ProviderExecutionError("Parquet source 超出允许根目录") from exc
            raise ProviderExecutionError(str(exc)) from exc
        if source_path.is_dir():
            self.source_kind = "snapshot"
        elif source_path.is_file() and source_path.suffix.lower() == ".parquet":
            self.source_kind = "file"
        else:
            raise ProviderExecutionError("没有可读取的 Parquet 文件")
        self.source = source_path
        self.files = self._resolve_files()

    def _resolve_files(self) -> tuple[Path, ...]:
        try:
            source = self.path_policy.resolve_contained_path(
                allowed_root=self.allowed_root,
                candidate=self.source,
                root_role="parquet_allowed_root",
                path_role="parquet_source",
                expected_kind="directory" if self.source_kind == "snapshot" else "file",
            )
            if self.source_kind == "file":
                return (source,)
            manifest = verify_parquet_snapshot(source)
            raw_files = manifest.get("files", [])
            if not isinstance(raw_files, list):
                raise ProviderExecutionError("Parquet manifest files 无效")
            relative_paths = tuple(str(item["relative_path"]) for item in raw_files)
            return self.path_policy.resolve_manifest_files(
                allowed_root=source,
                relative_paths=relative_paths,
                root_role="parquet_snapshot_root",
                file_role="parquet_partition",
            )
        except ProviderExecutionError:
            raise
        except DataPlaneError as exc:
            message = str(exc)
            if "COMMITTED" in message or "提交标记" in message:
                raise ProviderExecutionError("Parquet snapshot 尚未提交") from exc
            raise ProviderExecutionError(message) from exc

    def open_stream(
        self,
        plan: AdmittedQueryPlan,
        *,
        execution_budget: DataPlaneExecutionBudget,
        execution_estimate: ExecutionEstimate,
        scratch_root: str | Path,
    ) -> ColumnarStream:
        import duckdb

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
        self.files = self._resolve_files()
        source_columns = None
        if self.source_kind == "snapshot":
            manifest = verify_parquet_snapshot(self.source)
            if manifest.get("admitted_plan_hash") != plan.plan_hash:
                raise ProviderExecutionError("Parquet snapshot 与当前 admitted plan 不一致")
            source_columns = {field_id: field_id for field_id in plan.column_map}
        if plan.minute_dataset_semantics_hash is not None:
            self._validate_minute_timestamp_type(plan, source_columns=source_columns)
        connection = None
        try:
            connection = duckdb.connect(":memory:")
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
            placeholders = ",".join("?" for _ in self.files)
            source = f"read_parquet([{placeholders}], hive_partitioning=false)"
            sql, params = compile_duckdb_query(
                plan,
                source_expression=source,
                source_columns=source_columns,
                materialized_snapshot=self.source_kind == "snapshot",
            )
            reader = connection.execute(
                sql,
                [*(str(path) for path in self.files), *params],
            ).to_arrow_reader(allocation.batch_rows)
            if tuple(reader.schema.names) != plan.query.field_ids:
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
                f"Parquet 只读查询失败: {type(exc).__name__}"
            ) from exc
        except BaseException:
            if connection is not None:
                connection.close()
            if temp_directory is not None:
                shutil.rmtree(temp_directory, ignore_errors=True)
            raise

    def _validate_minute_timestamp_type(
        self,
        plan: AdmittedQueryPlan,
        *,
        source_columns: dict[str, str] | None,
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = plan.column_map if source_columns is None else source_columns
        physical_name = columns[plan.event_time_field]
        expected = pa.timestamp("us")
        for path in self.files:
            try:
                field = pq.read_schema(path).field(physical_name)
            except (KeyError, OSError) as exc:
                raise ProviderExecutionError(
                    "分钟 Parquet 缺少物理 event_time 字段"
                ) from exc
            if field.type != expected:
                raise ProviderExecutionError(
                    "分钟 Parquet event_time 必须是无时区 timestamp[us]"
                )


__all__ = ["ParquetColumnarProvider"]
