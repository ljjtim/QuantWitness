"""过去窗口分组与独立未来标签的教学算子。"""
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

ARTIFACT = 'project.turnover_factor.sample.v1'
SCHEMA = 'project.turnover_factor.metrics.v1'
METRIC_REF = 'project.turnover_factor.low_minus_high_return@1.0.0'
METRIC_SCHEMA = pa.schema([
    ('metric_ref', pa.string()), ('value', pa.float64()), ('unit', pa.string()),
    ('sample_start', pa.string()), ('sample_end', pa.string()),
    ('sample_size', pa.int64()), ('status', pa.string()),
])


def build_groups(rows, window=3):
    """每个观察日只用观察日前的窗口，代码排序用于固定同值次序。"""
    sessions = sorted({row['fld_metric_session'] for row in rows})
    indexed = {(row['fld_metric_session'], row['fld_metric_code']): row for row in rows}
    securities = sorted({row['fld_metric_code'] for row in rows})
    output = []
    for index in range(window, len(sessions)):
        day = sessions[index]
        history = sessions[index-window:index]
        scores = []
        for code in securities:
            samples = [indexed.get((session, code)) for session in history]
            if any(row is None or row['fld_metric_turnover'] is None for row in samples):
                continue
            scores.append((sum(row['fld_metric_turnover'] for row in samples) / window, code))
        scores.sort()
        if len(scores) < 5:
            continue
        for rank, (score, code) in enumerate(scores):
            output.append({'session': day.isoformat(), 'code': code,
                           'mean_turnover': score, 'group': rank * 5 // len(scores) + 1})
    return output


def build_labels(rows):
    """次日收盘收益只供事后评价，不参与窗口、资格和分组。"""
    by_code = defaultdict(list)
    for row in rows:
        by_code[row['fld_metric_code']].append(row)
    labels = {}
    for code, records in by_code.items():
        records.sort(key=lambda row: row['fld_metric_session'])
        for current, following in zip(records, records[1:]):
            labels[(current['fld_metric_session'].isoformat(), code)] = (
                following['fld_metric_close'] / current['fld_metric_close'] - 1.0)
    return labels


def evaluate(rows):
    groups = build_groups(rows)
    labels = build_labels(rows)
    observations = [{**row, 'forward_return': labels.get((row['session'], row['code']))}
                    for row in groups]
    daily = defaultdict(lambda: defaultdict(list))
    for row in observations:
        if row['forward_return'] is not None:
            daily[row['session']][row['group']].append(row['forward_return'])
    spreads = [(day, sum(groups[1]) / len(groups[1]) - sum(groups[5]) / len(groups[5]))
               for day, groups in sorted(daily.items()) if groups[1] and groups[5]]
    if not spreads:
        raise ValueError('没有具有完整低组和高组标签的观察日')
    metrics = pa.Table.from_pylist([{
        'metric_ref': METRIC_REF, 'value': sum(value for _, value in spreads) / len(spreads),
        'unit': 'decimal_return', 'sample_start': spreads[0][0], 'sample_end': spreads[-1][0],
        'sample_size': len(spreads), 'status': 'computed',
    }], schema=METRIC_SCHEMA)
    return pa.Table.from_pylist(observations), metrics


def run(context, inputs, output_root):
    source = next(item for item in inputs if item.port == 'data')
    rows = [row for batch in source.request(context.parameters['data_request_id']).iter_batches(
        columns=('fld_metric_session', 'fld_metric_code', 'fld_metric_turnover', 'fld_metric_close'),
        batch_size=1000) for row in batch.to_pylist()]
    observations, metrics = evaluate(rows)
    files = []
    for name, table in [('observations', observations), ('metrics', metrics)]:
        path = output_root / 'result' / name / 'part-00000.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        files.append(name + '/part-00000.parquet')
    return output_root.commit_directory(port='result', artifact_type=ARTIFACT,
        relative_path='result', files=tuple(files))
