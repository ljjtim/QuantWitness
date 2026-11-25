"""为 LEAN 正式执行表选择可无损保存数据的 Parquet 小数类型。"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq


def _decimal_paths(fields, prefix=()):
    for field in fields:
        path = (*prefix, field.name)
        dtype = field.type
        if pa.types.is_decimal256(dtype):
            yield path, dtype
        elif pa.types.is_struct(dtype):
            yield from _decimal_paths(dtype, path)
        elif pa.types.is_list(dtype):
            yield from _decimal_paths((dtype.value_field,), path)


def _decimal_values(record, path):
    if record is None:
        return
    if not path:
        yield record
    elif isinstance(record, list):
        for value in record:
            yield from _decimal_values(value, path[1:])
    else:
        yield from _decimal_values(record.get(path[0]), path[1:])


def _physical_field(field, decimal_types, prefix=()):
    path = (*prefix, field.name)
    dtype = field.type
    if pa.types.is_decimal256(dtype):
        return field.with_type(decimal_types[path])
    if pa.types.is_struct(dtype):
        return field.with_type(pa.struct([_physical_field(child, decimal_types, path) for child in dtype]))
    if pa.types.is_list(dtype):
        return field.with_type(pa.list_(_physical_field(dtype.value_field, decimal_types, path)))
    return field


class LeanExecutionParquetWriter:
    """一张表维持同一物理 schema，超界时只重写已写出的 Parquet 批次。"""

    def __init__(self, destination, schema):
        self.destination = destination
        self.schema = schema
        self._decimals = dict(_decimal_paths(schema))
        self._digits = dict.fromkeys(self._decimals, (0, 0))
        self._writer = None
        self._physical_schema = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self._writer is not None:
            self._writer.close()

    def _select_schema(self, rows):
        types = {}
        for path, dtype in self._decimals.items():
            integer, scale = self._digits[path]
            if integer + scale <= 38:
                for row in rows:
                    for value in _decimal_values(row, path):
                        if not value.is_finite():
                            raise ValueError(f"LEAN执行表小数必须有限：{'.'.join(path)}")
                        if value:
                            _, digits, exponent = value.as_tuple()
                            trailing_zeros = next(index for index, digit in enumerate(reversed(digits)) if digit)
                            integer = max(integer, len(digits) + exponent)
                            scale = max(scale, -exponent - trailing_zeros)
                self._digits[path] = (integer, scale)
            # 保持常见列的默认小数位。大整数挤占默认小数位时，仅缩减可无损省略的尾数。
            types[path] = (dtype if integer + scale > 38
                           else pa.decimal128(38, max(scale, min(dtype.scale, 38 - integer))))
        return pa.schema([_physical_field(field, types) for field in self.schema], metadata=self.schema.metadata)

    def write_rows(self, rows):
        schema = self._select_schema(rows)
        table = pa.Table.from_pylist(rows, schema=schema)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.destination, schema, compression="zstd")
        elif schema != self._physical_schema:
            self._writer.close()
            self._writer = None
            previous = self.destination.with_suffix(".previous.parquet")
            self.destination.replace(previous)
            self._writer = pq.ParquetWriter(self.destination, schema, compression="zstd")
            with pq.ParquetFile(previous) as existing:
                for batch in existing.iter_batches(batch_size=4096):
                    self._writer.write_batch(batch.cast(schema))
            previous.unlink()
        self._physical_schema = schema
        self._writer.write_table(table)
