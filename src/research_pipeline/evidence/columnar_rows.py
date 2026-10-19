"""模型独立验证的只读列式行视图，不展开整表Python字典。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
import operator

import pyarrow as pa


def _value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_value(item) for item in value]
    return value


class _ColumnarRow(Mapping):
    __slots__ = ('_owner', '_position')

    def __init__(self, owner, position):
        self._owner = owner
        self._position = position

    def __getitem__(self, name):
        return _value(self._owner._columns[name][self._position].as_py())

    def __iter__(self):
        return iter(self._owner._columns)

    def __len__(self):
        return len(self._owner._columns)


class ColumnarRows(Sequence):
    """保存Arrow列，按访问位置生成轻量行；金融验证仍逐值执行。"""
    __slots__ = ('_table', '_columns')

    def __init__(self, batches, *, schema):
        self._table = pa.Table.from_batches(batches, schema=schema)
        self._columns = {name: self._table.column(name) for name in self._table.column_names}

    def to_pandas(self):
        """直接转换列缓冲，不生成整表行字典。"""
        return self._table.to_pandas()

    @property
    def nbytes(self):
        return self._table.nbytes

    def __len__(self):
        return len(self._table)

    def __getitem__(self, position):
        if isinstance(position, slice):
            return [self[index] for index in range(*position.indices(len(self)))]
        position = operator.index(position)
        if position < 0:
            position += len(self)
        if not 0 <= position < len(self):
            raise IndexError(position)
        return _ColumnarRow(self, position)

    def __iter__(self):
        for position in range(len(self)):
            yield _ColumnarRow(self, position)
