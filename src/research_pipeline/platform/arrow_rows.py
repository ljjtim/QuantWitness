"""有界列式批次转行，重复标量只解码一次。"""
from __future__ import annotations


def record_batch_rows(batch):
    """保留Arrow的时区、精度及空值；缓存仅存在于当前批次。"""
    import pyarrow as pa
    import pyarrow.compute as pc

    columns = []
    for column in batch.columns:
        if (pa.types.is_timestamp(column.type) or pa.types.is_date(column.type)
                or pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
            encoded = pc.dictionary_encode(column)
            values = encoded.dictionary.to_pylist()
            if encoded.indices.null_count:
                indices = encoded.indices.to_pylist()
                columns.append([None if index is None else values[index] for index in indices])
            else:
                indices = encoded.indices.to_numpy(zero_copy_only=False).tolist()
                columns.append([values[index] for index in indices])
        elif (not column.null_count and (pa.types.is_integer(column.type)
                or pa.types.is_floating(column.type) or pa.types.is_boolean(column.type))):
            columns.append(column.to_numpy(zero_copy_only=False).tolist())
        else:
            columns.append(column.to_pylist())
    names = batch.schema.names
    for values in zip(*columns):
        yield dict(zip(names, values))
