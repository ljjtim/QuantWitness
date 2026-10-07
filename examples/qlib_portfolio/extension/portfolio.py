"""从当时可见的 test 预测生成固定组合，并独立提供成交行情。"""
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
import json
import math

import pyarrow as pa
import pyarrow.parquet as pq



PREDICTION_COLUMNS = (
    'fold_id', 'sample_id', 'entity_id', 'observation_session', 'candidate_id',
    'prediction', 'decision_time', 'feature_available_time', 'stage',
)
PRICE_COLUMNS = ('fld_equity_daily_date', 'fld_equity_daily_code', 'fld_equity_daily_close')
MARKET_COLUMNS = (*PRICE_COLUMNS, 'fld_demo_open', 'fld_demo_high_limit', 'fld_demo_low_limit', 'fld_demo_paused')


def read_table(value, prefix, columns=None):
    rows = []
    for name in value.file_paths:
        if name.startswith(prefix + '/') and name.endswith('.parquet'):
            rows.extend(pq.ParquetFile(pa.BufferReader(value.read_bytes(name))).read(columns=columns).to_pylist())
    if not rows:
        raise ValueError('组合输入表为空：' + prefix)
    return rows


def at(day, clock):
    return datetime.fromisoformat(str(day)[:10] + 'T' + clock + '+08:00')


def moment(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('预测可见时点缺少时区')
    return result


def build_targets(predictions, prices, design, source_identity):
    """固定持有分数最高三只，各30%；剩余10%现金作为成交费用余量。"""
    days = list(design['calendar_sessions'])
    entities = sorted(design['entities'])
    grouped = defaultdict(dict)
    for row in predictions:
        session, code = str(row['observation_session'])[:10], row['entity_id']
        if row['stage'] != 'test' or not math.isfinite(row['prediction']):
            raise ValueError('组合只接受有限的开发test预测')
        if code not in entities or code in grouped[session]:
            raise ValueError('test预测证券未知或会话内重复')
        decision = at(session, '09:31:00')
        if (moment(row['decision_time']) > decision
                or moment(row['feature_available_time']) > moment(row['decision_time'])
                or decision >= moment(design['holdout_start'])):
            raise ValueError('组合预测尚不可见或进入最终holdout')
        grouped[session][code] = row
    price_columns = tuple(design.get('price_fields', PRICE_COLUMNS))
    price_index = {(str(row[price_columns[0]])[:10], row[price_columns[1]]): row[price_columns[2]] for row in prices}
    targets, benchmarks = [], []
    for session, prediction_by_code in sorted(grouped.items()):
        if set(prediction_by_code) != set(entities):
            raise ValueError('固定组合证券池缺少test预测')
        index = days.index(session)
        if index == 0 or index + 1 >= len(days):
            raise ValueError('组合缺少前收盘或下一执行会话')
        decision, order_time = at(session, '09:31:00'), at(days[index + 1], '09:30:00')
        ranked = sorted(entities, key=lambda code: (-prediction_by_code[code]['prediction'], code))
        selected = sorted(ranked[:3])
        target = {'decision_time': decision.isoformat(), 'target_type': 'weight',
            'entries': [{'instrument': {'instrument_id': code, 'asset_class': 'cn_etf', 'venue': code.rsplit('.', 1)[-1],
                'currency': 'CNY', 'contract_kind': 'etf', 'contract_version': 'research-instrument-key-v1'},
                'target_type': 'weight', 'value': 0.3} for code in selected],
            'base_currency': 'CNY', 'cash_weight': 0.1, 'short_allowed': False, 'leverage_limit': 1.,
            'source_hashes': [source_identity], 'contract_version': 'research-portfolio-target-v1'}
        targets.append({'decision_time': decision, 'order_time': order_time,
            'target_json': json.dumps(target, ensure_ascii=False, sort_keys=True, separators=(',', ':')),
            'intent_plan_hash': source_identity})
        for code in entities:
            price = price_index[(days[index - 1], code)]
            if not math.isfinite(price) or price <= 0:
                raise ValueError('决策前收盘价必须为正且有限')
            units = Decimal(str(price)) * 1000
            if units != units.to_integral_value():
                raise ValueError('决策基准价格超过三位小数')
            benchmarks.append({'code': code, 'decision_time': decision,
                'price_units': int(units), 'price_scale': 3,
                'available_at': at(session, '09:30:00')})
    if not targets:
        raise ValueError('组合缺少可执行的test预测')
    return targets, benchmarks


def commit_tables(output_root, port, artifact_type, tables):
    files = []
    for name, rows in tables.items():
        relative = name + '/part-00000.parquet'
        path = output_root / port / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), path)
        files.append(relative)
    return output_root.commit_directory(port=port, artifact_type=artifact_type,
        relative_path=port, files=tuple(files), publish_at_artifact_root=True)


def targets(context, inputs, output_root):
    values = {value.port: value for value in inputs}
    predictions = read_table(values['selection'], 'test_predictions', list(PREDICTION_COLUMNS))
    prices = [row for batch in values['data'].request('daily_feature').iter_batches(columns=context.parameters['design'].get('price_fields', PRICE_COLUMNS), batch_size=8192) for row in batch.to_pylist()]
    target_rows, benchmark_rows = build_targets(predictions, prices, context.parameters['design'], values['selection'].source_identity)
    return commit_tables(output_root, 'targets', 'research.portfolio-targets.v1', {'targets': target_rows, 'benchmarks': benchmark_rows})


def market(context, inputs, output_root):
    values = {value.port: value for value in inputs}
    target_rows = read_table(values['targets'], 'targets', ['order_time'])
    first = min(row['order_time'].date() for row in target_rows)
    last = max(row['order_time'].date() for row in target_rows)
    rows = []
    market_columns = tuple(context.parameters['design'].get('market_fields', MARKET_COLUMNS))
    for batch in values['data'].request('daily_feature').iter_batches(columns=market_columns, batch_size=8192):
        for row in batch.to_pylist():
            if first <= row[market_columns[0]] <= last:
                rows.append(dict(zip(('date', 'code', 'close', 'open', 'high_limit', 'low_limit', 'paused'), (row[key] for key in market_columns))))
    return commit_tables(output_root, 'market', 'data.daily-market.v1', {'market': rows})
