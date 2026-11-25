"""Arrow 批流及其资源生命周期。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any, Callable

from .errors import ProviderExecutionError


@dataclass
class DatabaseReadTiming:
    query_ns: int = 0
    arrow_ns: int = 0
    source_verify_ns: int = 0
    visibility_ns: int = 0
    column_convert_ns: int = 0
    row_build_ns: int = 0
    field_project_ns: int = 0
    delivery_copy_ns: int = 0
    index_decode_ns: int = 0

    @property
    def arrange_ns(self):
        return (self.column_convert_ns + self.row_build_ns + self.field_project_ns
                + self.delivery_copy_ns + self.index_decode_ns)


_READ_TIMING: ContextVar[DatabaseReadTiming | None] = ContextVar("database_read_timing", default=None)


def current_read_timing():
    return _READ_TIMING.get()


@contextmanager
def measure_database_reads() -> Iterator[DatabaseReadTiming]:
    """只为当前同步消费范围记录直接来源读取各阶段的墙钟时间。"""
    timing = DatabaseReadTiming()
    token = _READ_TIMING.set(timing)
    try:
        yield timing
    finally:
        _READ_TIMING.reset(token)


def open_duckdb_arrow_reader(connection, sql, params, batch_rows):
    timing = _READ_TIMING.get()
    if timing is None:
        return connection.execute(sql, params).to_arrow_reader(batch_rows)
    started = perf_counter_ns()
    try:
        return connection.execute(sql, params).to_arrow_reader(batch_rows)
    finally:
        timing.query_ns += perf_counter_ns() - started


def time_source_verification(verify, *args):
    timing = _READ_TIMING.get()
    if timing is None:
        return verify(*args)
    started = perf_counter_ns()
    try:
        return verify(*args)
    finally:
        timing.source_verify_ns += perf_counter_ns() - started


class ColumnarStream(Iterator[Any]):
    """同时拥有 RecordBatchReader 与底层连接。"""

    def __init__(
        self,
        reader: Any,
        connection: Any,
        *,
        max_rows: int,
        max_bytes: int,
        on_close: Callable[[], None] | None = None,
        measure_arrow_reads: bool = False,
    ) -> None:
        self._reader = reader
        self._iterator = iter(reader)
        self._connection = connection
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self.rows_read = 0
        self.bytes_read = 0
        self.closed = False
        self._on_close = on_close
        self._measure_arrow_reads = measure_arrow_reads

    @property
    def schema(self) -> Any:
        return self._reader.schema

    def __iter__(self) -> "ColumnarStream":
        return self

    def __next__(self) -> Any:
        if self.closed:
            raise StopIteration
        timing = _READ_TIMING.get() if self._measure_arrow_reads else None
        try:
            if timing is None:
                batch = next(self._iterator)
            else:
                started = perf_counter_ns()
                try:
                    batch = next(self._iterator)
                finally:
                    timing.arrow_ns += perf_counter_ns() - started
        except BaseException:
            self.close()
            raise
        self.rows_read += int(batch.num_rows)
        self.bytes_read += int(batch.nbytes)
        if self.rows_read > self._max_rows or self.bytes_read > self._max_bytes:
            self.close()
            raise ProviderExecutionError(
                f"Arrow 批流超过预算: rows={self.rows_read}, bytes={self.bytes_read}"
            )
        return batch

    def read_table(self) -> Any:
        import pyarrow as pa

        batches = list(self)
        if not batches:
            return pa.Table.from_batches([], schema=self.schema)
        return pa.Table.from_batches(batches, schema=batches[0].schema)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            close = getattr(self._reader, "close", None)
            if callable(close):
                close()
        finally:
            try:
                if self._connection is not None:
                    self._connection.close()
            finally:
                if self._on_close is not None:
                    self._on_close()

    def __enter__(self) -> "ColumnarStream":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


__all__ = ["ColumnarStream", "DatabaseReadTiming", "measure_database_reads"]
