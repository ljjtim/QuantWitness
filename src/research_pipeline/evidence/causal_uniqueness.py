"""对正式因果表执行受预算约束的全局行键排序复核。"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import tempfile

import duckdb
import psutil
import pyarrow as pa
import pyarrow.compute as pc

from .errors import EvidenceContractError
from .oracle_workspace import (
    FINANCIAL_ORACLE_MIN_MEMORY_BYTES,
    FINANCIAL_ORACLE_NON_ENGINE_RESERVE_BYTES,
    FinancialOracleBudget,
)


def verify_causal_key_uniqueness(
    *,
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    budget: FinancialOracleBudget,
    parquet_paths: tuple[Path, ...] = (),
) -> int:
    """只消费完整行键投影；排序由本次 verifier 执行，不信任源文件顺序。"""

    engine_memory = (
        budget.memory_bytes
        - int(psutil.Process().memory_info().rss)
        - FINANCIAL_ORACLE_NON_ENGINE_RESERVE_BYTES
    )
    if engine_memory < FINANCIAL_ORACLE_MIN_MEMORY_BYTES:
        raise EvidenceContractError("因果唯一性复核资源不足: DuckDB memory 低于 64 MiB")
    keys = ", ".join('"' + name.replace('"', '""') + '"' for name in schema.names)
    try:
        with (
            tempfile.TemporaryDirectory(
                prefix="research-pipeline-causal-uniqueness-", dir=budget.scratch_root
            ) as scratch,
            duckdb.connect(
                ":memory:",
                config={
                    "memory_limit": f"{engine_memory}B",
                    "temp_directory": scratch,
                    "threads": 1,
                    "preserve_insertion_order": False,
                },
            ) as connection,
        ):
            # DuckDB 初始化 temp_directory 后再设置配额，否则目录初始化会重置上限。
            connection.execute(f"SET max_temp_directory_size = '{budget.temp_bytes}B'")
            if parquet_paths:
                relation = connection.from_parquet(
                    [str(path) for path in parquet_paths], hive_partitioning=False
                )
            else:
                relation = connection.from_arrow(
                    pa.RecordBatchReader.from_batches(schema, batches)
                )
            reader = (
                relation.project(keys).order(keys).to_arrow_reader(batch_size=8_192)
            )
            previous = None
            row_count = 0
            for batch in reader:
                if not batch.num_rows:
                    continue
                if any(
                    pc.any(pc.is_null(column, nan_is_null=True)).as_py()
                    for column in batch.columns
                ):
                    raise EvidenceContractError(
                        "Result 正式 Feature/Label 行键包含空值"
                    )
                first = tuple(column[0].as_py() for column in batch.columns)
                duplicate = first if first == previous else None
                if batch.num_rows > 1:
                    equal = None
                    for column in batch.columns:
                        same = pc.equal(
                            column.slice(0, len(column) - 1), column.slice(1)
                        )
                        equal = same if equal is None else pc.and_(equal, same)
                    if pc.any(equal).as_py():
                        offset = pc.index(equal, True).as_py()
                        duplicate = tuple(
                            column[offset].as_py() for column in batch.columns
                        )
                if duplicate is not None:
                    raise EvidenceContractError(
                        f"Result 正式 Feature/Label 行键不唯一: {dict(zip(schema.names, duplicate))}"
                    )
                previous = tuple(column[-1].as_py() for column in batch.columns)
                row_count += batch.num_rows
            return row_count
    except (duckdb.Error, pa.ArrowException, MemoryError, OSError) as exc:
        raise EvidenceContractError(f"因果唯一性复核资源或读取失败: {exc}") from exc
