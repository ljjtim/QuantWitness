"""把完整校验的Feature批次转换为有界宽上下文。"""
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.research.modeling.inputs import (
    ModelMainlineError,
    assemble_daily_feature_context,
)


def compact_feature_context(
    scan,
    budget,
    *,
    assemble=assemble_daily_feature_context,
    scratch_root=None,
    value_names=None,
):
    """按证券暂存完整键组，逐组转换后只保留宽表。"""
    owners = {}
    writers = {}
    buffers = {}
    buffer_bytes = {}
    frames = []
    columns = set()
    if scratch_root is not None:
        Path(scratch_root).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='model-feature-context-', dir=scratch_root) as directory:
        directory = Path(directory)

        def consume(frame):
            entities = frame.entity_id.astype(str)
            for entity in entities.unique():
                if entity not in owners:
                    owners[entity] = len(owners) % 256
            slots = entities.map(owners)
            batch = pa.Table.from_pandas(frame, preserve_index=False)
            budget.require_additional(batch.nbytes, label='Feature整批Arrow投影')
            slot_values = slots.to_numpy()
            for slot in slots.unique():
                table = batch.take(pa.array(np.flatnonzero(slot_values == slot)))
                if slot not in writers:
                    writers[slot] = pq.ParquetWriter(
                        directory / f'{slot}.parquet', table.schema, compression='zstd',
                    )
                buffers.setdefault(slot, []).append(table)
                buffer_bytes[slot] = buffer_bytes.get(slot, 0) + table.nbytes
                budget.require_additional(
                    batch.nbytes + 2 * sum(buffer_bytes.values()), label='Feature归组Arrow缓冲',
                )
                if buffer_bytes[slot] >= 1024**2:
                    writers[slot].write_table(pa.concat_tables(buffers.pop(slot)))
                    buffer_bytes[slot] = 0

        try:
            scan(consume)
        finally:
            for slot, writer in writers.items():
                if slot in buffers:
                    writer.write_table(pa.concat_tables(buffers[slot]))
                writer.close()
            buffers.clear()
        for path in sorted(directory.glob('*.parquet')):
            budget.require_parquet_materialization((path,), label='Feature有界证券分组')
            long = budget.collect_arrow_batches(
                pq.ParquetFile(path).iter_batches(batch_size=65536), label='Feature证券分组',
            )
            try:
                if not long.status.astype(str).eq('ok').any():
                    continue
                budget.require_additional(
                    3 * int(long.memory_usage(deep=True).sum()), label='Feature分组透视工作集',
                )
                if value_names is not None:
                    long.attrs['model_feature_value_names'] = value_names
                wide, names = assemble(long)
                columns.update(names)
                budget.reserve_frame(wide, label='Feature紧凑上下文分组')
                frames.append(wide)
            finally:
                budget.release_frame(long)
        if not frames:
            raise ModelMainlineError('模型样本缺少可用Feature')
        context = budget.concat_reserved_frames(frames, label='Feature紧凑上下文')
        return context, tuple(sorted(columns))
