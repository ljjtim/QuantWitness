"""在原工件身份内按fold读取模型表与实际引用窗口。"""
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from research_pipeline.research.modeling.sequence import SequenceWindows


class FoldInputReader:
    def __init__(self, root, metadata, budget, paths, verify):
        self.root, self.metadata, self.budget = root, metadata, budget
        self.paths, self.verify = paths, verify
        self.verified = set()

    def table(self, name, *, predicate=None):
        if name not in self.verified:
            self.verify(self.root, name, self.metadata, self.budget)
            self.verified.add(name)
        paths = self.paths(self.root, name)
        def batches():
            schema = None
            emitted = False
            for path in paths:
                parquet = pq.ParquetFile(path)
                try:
                    if schema is None:
                        schema = parquet.schema_arrow
                    # 先解码有界批次再过滤，避免Dataset按整行组筛选时的原生工作区累积。
                    for batch in parquet.iter_batches(batch_size=8192, use_threads=False):
                        table = pa.Table.from_batches([batch])
                        selected = table if predicate is None else table.filter(predicate)
                        if len(selected):
                            emitted = True
                            yield from selected.to_batches()
                finally:
                    parquet.close()
            if not emitted and schema is not None:
                yield pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in schema], schema=schema)
        return self.budget.collect_arrow_batches(batches(), label='fold/'+name)

    def samples(self, ids, *, prediction=False):
        name = 'prediction_samples' if prediction and 'prediction_samples' in self.metadata.get('row_counts', {}) else 'samples'
        return self.table(name, predicate=ds.field('sample_id').isin(list(ids)))

    def windows(self, ids):
        spec = self.metadata.get('sequence')
        if spec is None:
            return None
        targets = self.table('sequence_targets', predicate=ds.field('sample_id').isin(list(ids)))
        members = self.table('sequence_members', predicate=ds.field('sample_id').isin(list(ids)))
        referenced = np.unique(members.context_index.to_numpy())
        context = self.table('sequence_context', predicate=ds.field('context_index').isin(referenced))
        exclusions = self.table('sequence_exclusions', predicate=ds.field('sample_id').isin(list(ids)))
        calendar = tuple(pd.Timestamp(day).date() for day in spec['calendar_sessions'])
        return SequenceWindows(
            context, targets, members, exclusions, tuple(spec['feature_columns']),
            calendar, spec['step_len'],
        )

    def release_windows(self, windows):
        if windows is not None:
            for name in ('context', 'targets', 'members', 'exclusions'):
                self.budget.release_frame(getattr(windows, name))
