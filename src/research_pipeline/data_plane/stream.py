"""Arrow 批流及其资源生命周期。"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Callable

from .errors import ProviderExecutionError


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

    @property
    def schema(self) -> Any:
        return self._reader.schema

    def __iter__(self) -> "ColumnarStream":
        return self

    def __next__(self) -> Any:
        if self.closed:
            raise StopIteration
        try:
            batch = next(self._iterator)
        except StopIteration:
            self.close()
            raise
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


__all__ = ["ColumnarStream"]
