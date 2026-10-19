"""有界因果事实的列式传递；来源清单在描述中共享，时间字符串保持原样。"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import RuntimeIntegrityError


_VERSION = "causal-facts-v1"
_PATH = "causal-facts/records.parquet"
_SOURCE = "source_partition_ids"
_REFERENCE = "__source_partition_ids_ref"
_SCALAR_METADATA = {b"causal_scalar": b"mixed-v1"}
_SCALAR_TYPES = {str: ("str", pa.string()), int: ("int", pa.int64()),
                 float: ("float", pa.float64()), bool: ("bool", pa.bool_())}
_MIXED_TYPE = pa.struct([pa.field(name, kind) for name, kind in _SCALAR_TYPES.values()])


def _key_columns(value: object) -> list[str]:
    if (not isinstance(value, list) or not value
            or any(not isinstance(name, str) or not name for name in value)
            or len(set(value)) != len(value)
            or _SOURCE in value or _REFERENCE in value):
        raise RuntimeIntegrityError("因果事实必须声明非空、唯一的标量键列")
    return list(value)


def _source_lists(value: object) -> list[list[str]]:
    if (not isinstance(value, list) or any(
        not isinstance(row, list) or any(not isinstance(item, str) for item in row)
        for row in value
    )):
        raise RuntimeIntegrityError("因果事实共享来源必须是字符串列表的列表")
    return [list(row) for row in value]


def _path(root: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RuntimeIntegrityError("因果事实路径必须是相对路径")
    base = root.resolve()
    path = (base / relative).resolve()
    if not path.is_relative_to(base / "causal-facts"):
        raise RuntimeIntegrityError("因果事实路径超出 attempt 的 causal-facts 目录")
    return path


def _scalar_column(name: str, values: list) -> tuple[pa.Field, pa.Array]:
    kinds = {type(value) for value in values if value is not None}
    if not kinds <= _SCALAR_TYPES.keys():
        raise RuntimeIntegrityError(f"因果事实列 {name} 只能保存 JSON 标量")
    if len(kinds) <= 1:
        kind = _SCALAR_TYPES[next(iter(kinds))][1] if kinds else pa.null()
        return pa.field(name, kind), pa.array(values, type=kind)
    # 只有混合标量列需要类型分支；避免 Arrow 将 int 自动转为 float。
    encoded = [None if value is None else {_SCALAR_TYPES[type(value)][0]: value}
               for value in values]
    return (pa.field(name, _MIXED_TYPE, metadata=_SCALAR_METADATA),
            pa.array(encoded, type=_MIXED_TYPE))


def write_causal_facts(facts: Mapping, root: Path) -> dict:
    """在 attempt_root/causal-facts 写入事实，返回可直接放入 JSON 的描述。"""
    keys = _key_columns(facts.get("key_columns"))
    records = facts.get("records")
    lineage = facts.get("lineage")
    if not isinstance(records, list) or not isinstance(lineage, list):
        raise RuntimeIntegrityError("因果事实 records 和 lineage 必须是列表")
    columns = list(records[0]) if records and isinstance(records[0], Mapping) else keys
    if (_REFERENCE in columns or any(not isinstance(name, str) for name in columns)
            or not set(keys) <= set(columns)):
        raise RuntimeIntegrityError("因果事实记录缺少键列或包含保留引用列")
    column_set = set(columns)
    values = {name: [] for name in columns}
    shared = []
    indices = {}
    for row in records:
        if not isinstance(row, Mapping) or set(row) != column_set:
            raise RuntimeIntegrityError("因果事实记录的列不一致")
        for name in columns:
            value = row[name]
            if name == _SOURCE:
                if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                    raise RuntimeIntegrityError("因果事实来源必须是字符串列表")
                identity = tuple(value)
                if identity not in indices:
                    indices[identity] = len(shared)
                    shared.append(list(value))
                value = indices[identity]
            values[name].append(value)
    try:
        fields, arrays = [], []
        for name in columns:
            if name == _SOURCE:
                field, array = pa.field(_REFERENCE, pa.int32()), pa.array(values[name], type=pa.int32())
            else:
                field, array = _scalar_column(name, values[name])
            fields.append(field)
            arrays.append(array)
        table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        path = _path(root, _PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path, compression="zstd", row_group_size=8192)
    except (OSError, pa.ArrowException, OverflowError) as exc:
        raise RuntimeIntegrityError(f"因果事实 Parquet 写入失败: {exc}") from exc
    return {"version": _VERSION, "path": _PATH, "row_count": len(records),
            "key_columns": keys, "lineage": lineage, "source_partition_lists": shared}


def read_causal_facts(descriptor: Mapping, root: Path) -> dict:
    """读取当前描述协议，恢复有序的原始标量记录及独立来源列表。"""
    if descriptor.get("version") != _VERSION:
        raise RuntimeIntegrityError("因果事实描述版本不匹配")
    keys = _key_columns(descriptor.get("key_columns"))
    count = descriptor.get("row_count")
    lineage = descriptor.get("lineage")
    shared = _source_lists(descriptor.get("source_partition_lists"))
    if type(count) is not int or count < 0 or not isinstance(lineage, list):
        raise RuntimeIntegrityError("因果事实行数或 lineage 无效")
    path = _path(root, descriptor.get("path"))
    if not path.is_file():
        raise RuntimeIntegrityError("因果事实 Parquet 文件不存在")
    try:
        # ParquetFile 读取单文件，避免目录名被解释为分区列。
        with pq.ParquetFile(path) as parquet:
            if parquet.metadata.num_rows != count:
                raise RuntimeIntegrityError("因果事实 Parquet 行数与描述不一致")
            table = parquet.read(use_threads=False)
    except (OSError, pa.ArrowException) as exc:
        raise RuntimeIntegrityError(f"因果事实 Parquet 读取失败: {exc}") from exc
    names = table.column_names
    if (len(set(names)) != len(names) or not set(keys) <= set(names) or _SOURCE in names
            or (shared and _REFERENCE not in names)):
        raise RuntimeIntegrityError("因果事实 Parquet 缺少键列或来源引用列")
    mixed = []
    for field in table.schema:
        if field.name == _REFERENCE:
            if not pa.types.is_integer(field.type):
                raise RuntimeIntegrityError("因果事实来源引用列必须为整数")
        elif field.metadata == _SCALAR_METADATA and field.type == _MIXED_TYPE:
            mixed.append(field.name)
        elif not (pa.types.is_string(field.type) or pa.types.is_integer(field.type)
                  or pa.types.is_floating(field.type) or pa.types.is_boolean(field.type)
                  or pa.types.is_null(field.type)):
            raise RuntimeIntegrityError("因果事实 Parquet 存在非标量列")
    records = table.to_pylist()
    for row in records:
        for name in mixed:
            value = row[name]
            if value is not None:
                branches = [item for item in value.values() if item is not None]
                if len(branches) != 1:
                    raise RuntimeIntegrityError("因果事实混合标量必须只有一个类型分支")
                row[name] = branches[0]
        if _REFERENCE in row:
            reference = row[_REFERENCE]
            if type(reference) is not int or not 0 <= reference < len(shared):
                raise RuntimeIntegrityError("因果事实来源引用越界或为空")
            # 在原来的列位置替换引用，保持记录字段顺序。
            restored = {(_SOURCE if name == _REFERENCE else name):
                        (list(shared[reference]) if name == _REFERENCE else value)
                        for name, value in row.items()}
            row.clear()
            row.update(restored)
    return {"key_columns": keys, "lineage": lineage, "records": records}
