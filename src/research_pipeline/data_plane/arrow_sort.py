"""对已准入Arrow批次做有界稳定排序，大分区由DuckDB落盘。"""
from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from threading import Lock
import shutil


@contextmanager
def sorted_record_batches(batches, root, *, sort_columns, memory_bytes, temp_bytes, cpu_slots):
    """保留完整列类型及同键输入顺序，临时落盘共享调用方额度。"""
    import pyarrow as pa
    import pyarrow.compute as pc

    root = Path(root).resolve()
    root.mkdir()
    connection = reader = output = None
    source = iter(batches)
    source_lock = Lock()
    source_closed = False
    try:
        first = next(source, None)
        if first is None:
            yield iter(())
            return
        schema = first.schema
        ordinal = "__qw_input_sequence"
        if ordinal in schema.names or not set(sort_columns) <= set(schema.names):
            raise ValueError("列式排序字段缺失或与输入序号重名")
        # 同时为输入、排序索引、重排结果和下游消费保留余量；超界后沿用落盘排序。
        buffered = deque()
        retained_bytes = 0
        for batch in chain((first,), source):
            if batch.schema != schema:
                raise ValueError("列式排序输入批次schema不一致")
            if batch.num_rows:
                buffered.append(batch)
                retained_bytes += batch.get_total_buffer_size() + 8 * batch.num_rows
            if retained_bytes > memory_bytes // 8:
                break
        else:
            if not buffered:
                yield iter(())
                return
            table = pa.Table.from_batches(list(buffered), schema=schema)
            indices = pc.sort_indices(table,
                sort_keys=[(name, "ascending") for name in sort_columns],
                null_placement="at_end")
            # 整列相同的变长字段不受行序影响，复用原缓冲，避免重排来源标识时复制字符串。
            constants = {}
            for index, field in enumerate(schema):
                if not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
                        or pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type)):
                    continue
                values = table.column(index)
                if values.null_count == table.num_rows:
                    constants[index] = values
                elif values.null_count == 0:
                    first_value = values[0]
                    if all(pc.all(pc.equal(chunk, first_value)).as_py() for chunk in values.chunks
                           if len(chunk)):
                        constants[index] = values
            varying = [index for index in range(len(schema)) if index not in constants]
            positions = {index: position for position, index in enumerate(varying)}
            del buffered, first, batch

            def ordered_batches():
                for offset in range(0, table.num_rows, 65_536):
                    selected = indices.slice(offset, 65_536)
                    taken = pc.take(table.select(varying), selected)
                    yield from pa.Table.from_arrays([
                        constants[index].slice(offset, len(selected)) if index in constants
                        else taken.column(positions[index])
                        for index in range(len(schema))
                    ], schema=schema).to_batches(max_chunksize=65_536)

            # 下游分钟窗口只引用当前输出块，不会通过一个切片保留整分区结果。
            output = ordered_batches()
            yield output
            return

        del first, batch

        import duckdb
        augmented = schema.append(pa.field(ordinal, pa.int64()))

        def sequenced():
            position = 0
            while True:
                with source_lock:
                    if source_closed:
                        return
                    batch = buffered.popleft() if buffered else next(source, None)
                if batch is None:
                    return
                if batch.schema != schema:
                    raise ValueError("列式排序输入批次schema不一致")
                sequence = pa.array(range(position, position + batch.num_rows), type=pa.int64())
                position += batch.num_rows
                yield pa.RecordBatch.from_arrays([*batch.columns, sequence], schema=augmented)

        reader = pa.RecordBatchReader.from_batches(augmented, sequenced())
        connection = duckdb.connect(":memory:", config={
            "memory_limit": f"{memory_bytes // 2}B",
            "temp_directory": str(root),
            "threads": str(cpu_slots),
            "preserve_insertion_order": "false",
        })
        # 在连接创建后将额度设置到实际暂存文件管理器。
        connection.execute("SET max_temp_directory_size = ?", [f"{temp_bytes}B"])
        connection.register("input_batches", reader)
        def quoted(name):
            return '"' + name.replace('"', '""') + '"'
        projection = ", ".join(quoted(name) for name in schema.names)
        ordering = ", ".join(quoted(name) for name in (*sort_columns, ordinal))
        output = connection.execute(
            f"SELECT {projection} FROM input_batches ORDER BY {ordering}"
        ).fetch_record_batch(rows_per_batch=4096)

        def restored():
            for batch in output:
                # DuckDB时区展示遵循连接设置；按原Arrow类型恢复时区和整数宽度。
                yield batch.cast(schema)

        yield restored()
    finally:
        # DuckDB可能仍在预取最后一个输入批；关闭源前等待该次读取返回。
        with source_lock:
            source_closed = True
            close = getattr(source, "close", None)
            if close is not None:
                close()
        if output is not None:
            output.close()
        if connection is not None:
            connection.close()
        if reader is not None:
            reader.close()
        shutil.rmtree(root)
