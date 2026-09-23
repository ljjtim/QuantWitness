"""独立 evidence oracle 共用的有界 DuckDB/Arrow 工作区。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Callable, Iterable, Iterator, Mapping, Sequence

import duckdb
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.results import ResultContractError, ResultSnapshot

from .errors import EvidenceContractError


DEFAULT_FINANCIAL_ORACLE_MEMORY_BYTES = 1024 * 1024 * 1024
DEFAULT_FINANCIAL_ORACLE_TEMP_BYTES = 8 * 1024 * 1024 * 1024
FINANCIAL_ORACLE_MIN_MEMORY_BYTES = 64 * 1024 * 1024
FINANCIAL_ORACLE_NON_ENGINE_RESERVE_BYTES = 384 * 1024 * 1024
FINANCIAL_ORACLE_BATCH_SIZE = 8_192


@dataclass(frozen=True)
class FinancialOracleBudget:
    """独立金融复核可使用的进程内存与临时磁盘配额。"""

    memory_bytes: int = DEFAULT_FINANCIAL_ORACLE_MEMORY_BYTES
    temp_bytes: int = DEFAULT_FINANCIAL_ORACLE_TEMP_BYTES
    scratch_root: Path | None = None

    def __post_init__(self) -> None:
        if type(self.memory_bytes) is not int or self.memory_bytes <= 0:
            raise EvidenceContractError("金融独立复核资源不足: memory 预算必须是正整数")
        if type(self.temp_bytes) is not int or self.temp_bytes <= 0:
            raise EvidenceContractError("金融独立复核资源不足: temp 预算必须是正整数")
        if self.scratch_root is not None:
            root = Path(self.scratch_root).resolve()
            if not root.is_dir():
                raise EvidenceContractError("金融独立复核 scratch root 不是目录")
            object.__setattr__(self, "scratch_root", root)


@dataclass(frozen=True)
class ResultTableSource:
    schema: pa.Schema
    row_count: int
    uncompressed_bytes: int
    iter_batches: Callable[[], Iterable[pa.RecordBatch]]
    parquet_paths: tuple[Path, ...] = ()


class OracleTable(Sequence[dict[str, object]]):
    """DuckDB 外部表的有界批迭代视图；不会把整表保留为 Python 行。"""

    def __init__(
        self,
        workspace: OracleWorkspace,
        name: str,
        row_count: int,
        schema: pa.Schema | None = None,
    ) -> None:
        self.workspace = workspace
        self.name = name
        self.row_count = row_count
        self.schema = schema

    def __len__(self) -> int:
        return self.row_count

    def __iter__(self) -> Iterator[dict[str, object]]:
        return self.iter_rows()

    def __getitem__(self, index: int | slice):
        if isinstance(index, slice):
            start, stop, step = index.indices(self.row_count)
            if step != 1:
                return [self[position] for position in range(start, stop, step)]
            return list(
                self.workspace.iter_rows(
                    self.name,
                    limit=max(0, stop - start),
                    offset=start,
                )
            )
        normalized = index if index >= 0 else self.row_count + index
        if normalized < 0 or normalized >= self.row_count:
            raise IndexError(index)
        return next(self.workspace.iter_rows(self.name, limit=1, offset=normalized))

    def iter_rows(
        self,
        *,
        order_by: Sequence[str] = (),
    ) -> Iterator[dict[str, object]]:
        yield from self.workspace.iter_rows(self.name, order_by=order_by)


class OracleWorkspace:
    """把已验证 Parquet 暴露给一个受配额约束的只读复核数据库。"""

    def __init__(
        self,
        sources: Mapping[str, ResultTableSource],
        *,
        budget: FinancialOracleBudget,
    ) -> None:
        self.sources = dict(sources)
        self.budget = budget
        self._temporary: tempfile.TemporaryDirectory | None = None
        self.root: Path | None = None
        self.connection: duckdb.DuckDBPyConnection | None = None
        self.tables: dict[str, OracleTable] = {}
        self.minute_tables: dict[str, OracleTable] = {}
        self.minute_indexes: set[tuple[str, tuple[str, ...]]] = set()

    def __enter__(self) -> OracleWorkspace:
        try:
            current_rss = int(psutil.Process().memory_info().rss)
            engine_memory_bytes = (
                self.budget.memory_bytes
                - current_rss
                - FINANCIAL_ORACLE_NON_ENGINE_RESERVE_BYTES
            )
            if engine_memory_bytes < FINANCIAL_ORACLE_MIN_MEMORY_BYTES:
                raise EvidenceContractError(
                    "金融独立复核资源不足: 扣除当前进程和 Arrow 批处理后，"
                    "DuckDB memory 低于 64 MiB 支持下限"
                )
            self._temporary = tempfile.TemporaryDirectory(
                prefix="research-pipeline-financial-oracle-",
                dir=(
                    None
                    if self.budget.scratch_root is None
                    else self.budget.scratch_root
                ),
            )
            self.root = Path(self._temporary.name)
            temp_directory = self.root / "spill"
            temp_directory.mkdir()
            self.connection = duckdb.connect(":memory:")
            escaped_temp = str(temp_directory).replace("'", "''")
            self.connection.execute(f"SET memory_limit = '{engine_memory_bytes:d}B'")
            self.connection.execute(f"SET temp_directory = '{escaped_temp}'")
            self.connection.execute(
                f"SET max_temp_directory_size = '{self.budget.temp_bytes:d}B'"
            )
            self.connection.execute("SET threads = 1")
            self.connection.execute("SET preserve_insertion_order = false")
            self.connection.execute("SET TimeZone = 'Asia/Shanghai'")
            for index, (schema_id, source) in enumerate(self.sources.items()):
                name = f"source_{index}"
                paths = source.parquet_paths
                if not paths:
                    staged = self.root / f"{name}.parquet"
                    self._stage_source(source, staged)
                    paths = (staged,)
                path_list = ", ".join(
                    "'" + str(path).replace("'", "''") + "'" for path in paths
                )
                self.connection.execute(
                    f"CREATE VIEW {name} AS "
                    f"SELECT * FROM read_parquet("
                    f"[{path_list}], hive_partitioning = false)"
                )
                self.tables[schema_id] = OracleTable(
                    self, name, source.row_count, source.schema
                )
            staged_bytes = sum(
                path.stat().st_size for path in self.root.glob("source_*.parquet")
            )
            remaining_temp_bytes = self.budget.temp_bytes - staged_bytes
            if remaining_temp_bytes <= 0:
                raise EvidenceContractError(
                    "金融独立复核资源不足: staged Parquet 已用尽 temp 预算"
                )
            self.connection.execute(
                f"SET max_temp_directory_size = '{remaining_temp_bytes:d}B'"
            )
            return self
        except Exception:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None

    def _stage_source(self, source: ResultTableSource, target: Path) -> None:
        writer = pq.ParquetWriter(target, source.schema)
        rows = 0
        try:
            for batch in source.iter_batches():
                writer.write_batch(batch)
                rows += batch.num_rows
                if target.stat().st_size > self.budget.temp_bytes:
                    raise EvidenceContractError(
                        "金融独立复核资源不足: staged Parquet 超出 temp 预算"
                    )
        finally:
            writer.close()
        if rows != source.row_count:
            raise EvidenceContractError("金融复核暂存表行数与 Result manifest 不一致")

    def execute(self, sql: str, parameters: Sequence[object] = ()):
        if self.connection is None:
            raise RuntimeError("金融复核 workspace 尚未打开")
        return self.connection.execute(sql, list(parameters))

    def iter_rows(
        self,
        table: str,
        *,
        order_by: Sequence[str] = (),
        limit: int | None = None,
        offset: int = 0,
    ) -> Iterator[dict[str, object]]:
        order = ""
        if order_by:
            order = " ORDER BY " + ", ".join(
                quoted_identifier(item) for item in order_by
            )
        pagination = ""
        if limit is not None:
            pagination = f" LIMIT {limit:d} OFFSET {offset:d}"
        yield from self.iter_query(f"SELECT * FROM {table}{order}{pagination}")

    def iter_query(
        self,
        sql: str,
        parameters: Sequence[object] = (),
    ) -> Iterator[dict[str, object]]:
        if self.connection is None:
            raise RuntimeError("金融复核 workspace 尚未打开")
        # 分钟逐行复核会查询该行关联事实；独立 cursor 避免覆盖外层批迭代。
        cursor = self.connection.cursor()
        try:
            cursor.execute("SET TimeZone = 'Asia/Shanghai'")
            cursor.execute(sql, list(parameters))
            names = [item[0] for item in cursor.description]
            reader = cursor.to_arrow_reader(batch_size=FINANCIAL_ORACLE_BATCH_SIZE)
            for batch in reader:
                for values in zip(*(column.to_pylist() for column in batch.columns)):
                    yield dict(zip(names, values, strict=True))
        finally:
            cursor.close()

    def table(self, schema_id: str) -> OracleTable:
        return self.tables[schema_id]

    def minute_table(
        self,
        source: OracleTable,
        keys: tuple[str, ...],
    ) -> OracleTable:
        """分钟关联表和查询索引由当前 DuckDB 配额管理，不转成 Python 历史字典。"""

        table = self.minute_tables.get(source.name)
        if table is None:
            name = f"minute_rows_{len(self.minute_tables)}"
            self.execute(f"CREATE TABLE {name} AS SELECT * FROM {source.name}")
            table = OracleTable(self, name, len(source), source.schema)
            self.minute_tables[source.name] = table
            self.minute_tables[name] = table
        index_key = table.name, keys
        if keys and index_key not in self.minute_indexes:
            fields = ", ".join(quoted_identifier(key) for key in keys)
            index_name = f"minute_index_{len(self.minute_indexes)}"
            self.execute(f"CREATE INDEX {index_name} ON {table.name} ({fields})")
            self.minute_indexes.add(index_key)
        return table


def quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def result_table_source(
    snapshot: ResultSnapshot,
    schema_id: str,
) -> ResultTableSource:
    try:
        directory = getattr(snapshot, "directory", None)
        selected = tuple(
            table for table in snapshot.bundle.tables if table.schema_id == schema_id
        )
        parquet_paths = ()
        if directory is not None and len(selected) == 1:
            parquet_paths = tuple(
                Path(directory) / relative_path for relative_path in selected[0].files
            )
        return ResultTableSource(
            schema=snapshot.table_schema(schema_id),
            row_count=snapshot.table_row_count(schema_id),
            uncompressed_bytes=snapshot.table_uncompressed_bytes(schema_id),
            iter_batches=lambda: snapshot.iter_table_batches(
                schema_id,
                batch_size=FINANCIAL_ORACLE_BATCH_SIZE,
            ),
            parquet_paths=parquet_paths,
        )
    except (AttributeError, ResultContractError) as exc:
        raise EvidenceContractError(
            f"金融复核无法读取 Result 表引用: {schema_id}"
        ) from exc


def arrow_table_source(table: pa.Table | ResultTableSource) -> ResultTableSource:
    if isinstance(table, ResultTableSource):
        return table
    return ResultTableSource(
        schema=table.schema,
        row_count=table.num_rows,
        uncompressed_bytes=table.nbytes,
        iter_batches=lambda: table.to_batches(
            max_chunksize=FINANCIAL_ORACLE_BATCH_SIZE
        ),
    )


__all__ = [
    "DEFAULT_FINANCIAL_ORACLE_MEMORY_BYTES",
    "DEFAULT_FINANCIAL_ORACLE_TEMP_BYTES",
    "FINANCIAL_ORACLE_BATCH_SIZE",
    "FINANCIAL_ORACLE_MIN_MEMORY_BYTES",
    "FINANCIAL_ORACLE_NON_ENGINE_RESERVE_BYTES",
    "FinancialOracleBudget",
    "OracleTable",
    "OracleWorkspace",
    "ResultTableSource",
    "arrow_table_source",
    "quoted_identifier",
    "result_table_source",
]
