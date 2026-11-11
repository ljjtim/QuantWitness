"""合成观察均值的项目算子与标准指标表。"""
import pyarrow as pa
import pyarrow.parquet as pq

ARTIFACT = 'project.custom_metric.sample.v1'
SCHEMA = 'project.custom_metric.metrics.v1'
METRIC_REF = 'project.custom_metric.mean_return@1.0.0'
METRIC_SCHEMA = pa.schema([
    ('metric_ref', pa.string()), ('value', pa.float64()), ('unit', pa.string()),
    ('sample_start', pa.string()), ('sample_end', pa.string()),
    ('sample_size', pa.int64()), ('status', pa.string()),
])


def calculate_metric(observations: pa.Table) -> pa.Table:
    """观察收益的算术均值；不是策略信号或可交易收益。"""
    rows = observations.to_pylist()
    if not rows:
        raise ValueError('合成观察样本不能为空')
    sessions = [row['session'] for row in rows]
    return pa.Table.from_pylist([{
        'metric_ref': METRIC_REF, 'value': sum(row['return'] for row in rows) / len(rows),
        'unit': 'decimal_return', 'sample_start': min(sessions), 'sample_end': max(sessions),
        'sample_size': len(rows), 'status': 'computed',
    }], schema=METRIC_SCHEMA)


def run(context, inputs, output_root):
    source = next(item for item in inputs if item.port == 'data')
    batches = list(source.request(context.parameters['data_request_id']).iter_batches(
        columns=('fld_metric_session', 'fld_metric_return'), batch_size=16))
    raw = pa.Table.from_batches(batches)
    observations = pa.table({'session': [day.isoformat() for day in raw['fld_metric_session'].to_pylist()],
                             'return': raw['fld_metric_return']})
    root = output_root / 'result'
    files = []
    for name, table in [('observations', observations), ('metrics', calculate_metric(observations))]:
        path = root / name / 'part-00000.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        files.append(name + '/part-00000.parquet')
    return output_root.commit_directory(port='result', artifact_type=ARTIFACT,
        relative_path='result', files=tuple(files))
