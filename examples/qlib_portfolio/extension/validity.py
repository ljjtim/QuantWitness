"""从已封存的日频模型端口收集可独立复核的预测诊断事实。"""
from datetime import date, datetime
import json
from statistics import fmean
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq


TABLE_PORTS = {
    'features': 'features', 'labels': 'labels',
    'samples': 'splits', 'split_audit': 'splits', 'holdout_index': 'splits',
    'fit_audit': 'fits', 'validation_predictions': 'predictions',
    'selection': 'selection', 'test_predictions': 'selection', 'fold_selections': 'selection',
    'holdout_predictions': 'holdout', 'holdout_receipt': 'holdout',
    'raw_prices': 'summary', 'study_design': 'summary', 'metrics': 'summary', 'models': 'summary',
}


def _table(value, prefix, *, allow_empty=False):
    rows = []
    found = False
    for name in value.file_paths:
        if name.startswith(prefix + '/') and name.endswith('.parquet'):
            found = True
            parquet = pq.ParquetFile(pa.BufferReader(value.read_bytes(name)))
            for batch in parquet.iter_batches(batch_size=8192):
                rows.extend(batch.to_pylist())
    if not found or (not rows and not allow_empty):
        raise ValueError('模型有效性缺少正式输入表：' + prefix)
    return rows


def _json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _ns(value):
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError('模型数据准入时间必须包含时区')
    return int(moment.timestamp()) * 1_000_000_000 + moment.microsecond * 1000


def collect_facts(context, inputs):
    values = {value.port: value for value in inputs}
    bindings = context.parameters['table_bindings']
    development = 'holdout' not in values
    table_ports = {name: port for name, port in TABLE_PORTS.items() if not development or port not in ('holdout', 'selection')}
    sequence_ports = {"sequence_" + name: "splits" for name in ("context", "targets", "members", "exclusions")}
    if set(bindings) not in (set(table_ports), set(table_ports) | set(sequence_ports)):
        raise ValueError('模型有效性绑定表集合与正式端口不一致')
    design_rows = _table(values['summary'], 'study_design')
    if len(design_rows) != 1:
        raise ValueError('冻结研究设计必须唯一')
    design = json.loads(design_rows[0]['design_json'])
    if design.get('sequence') is not None:
        if not set(sequence_ports) <= set(bindings) or design['sequence'].get('schema') != 'research.model-sequence-context.v2':
            raise ValueError('序列研究必须绑定v2窗口表')
    elif set(sequence_ports) & set(bindings):
        raise ValueError('未声明序列研究却绑定窗口表')
    observations, ceilings = [], {}
    for request_id in sorted(context.parameters['data_request_ids']):
        admission = values['data'].admission(request_id)
        ceilings[request_id] = admission['input_claim_ceiling']
        observations.append({
            'available_at_ns': _ns(admission['as_of_cutoff']),
            'decision_at_ns': _ns(context.fixed_clock),
            'source_revision_hash': admission['source_revision_hash'],
            'availability_policy_hash': admission['availability_policy_hash'],
        })
    levels = ('research_observation', 'portfolio_simulation_candidate', 'tradable_simulation')
    if not ceilings or any(value not in levels for value in ceilings.values()):
        raise ValueError('模型输入结论上限缺失或非法')
    prediction_name = 'validation_predictions' if development else 'holdout_predictions'
    prediction_port = 'predictions' if development else 'holdout'
    count, start, end = 0, None, None
    def squared_errors():
        nonlocal count, start, end
        for name in values[prediction_port].file_paths:
            if name.startswith(prediction_name + '/') and name.endswith('.parquet'):
                parquet = pq.ParquetFile(pa.BufferReader(values[prediction_port].read_bytes(name)))
                for batch in parquet.iter_batches(batch_size=8192, columns=['prediction', 'actual', 'observation_session']):
                    for row in batch.to_pylist():
                        day = str(row['observation_session'])[:10]
                        start = day if start is None else min(start, day)
                        end = day if end is None else max(end, day)
                        count += 1
                        yield (row['prediction'] - row['actual']) ** 2
    mse = fmean(squared_errors())
    metric_rows = _table(values['summary'], 'metrics')
    if len(metric_rows) != 1 or metric_rows[0]['metric_ref'] != context.parameters['metric_ref']:
        raise ValueError('模型诊断指标必须唯一并绑定冻结定义')
    facts = {
        'contract_version': 'research-validity-facts-v1',
        'model_diagnostics': {
            'mode': 'walk_forward_development_v1' if development else 'walk_forward_prediction_v1',
            'table_storage': 'result_tables_v2', 'table_bindings': dict(bindings),
        },
        'data_pit': {
            'observations': observations, 'consumed_request_ids': sorted(ceilings),
            'input_claim_ceilings': ceilings,
            'effective_claim_ceiling': min(ceilings.values(), key=levels.index),
        },
        'label_split': {'mode': 'walk_forward_development_v1' if development else 'walk_forward_prediction_v1'},
        'search_holdout': {'mode': 'walk_forward_development_v1' if development else 'walk_forward_prediction_v1'},
        'statistics': {'method': 'validation_mse' if development else 'prediction_mse', 'sample_count': count,
                       'mse': mse, 'metric_ref': context.parameters['metric_ref'],
                       'sample_start': start, 'sample_end': end},
        'financial_tradability': {'applicability': 'not_applicable',
                                 'reason': 'prediction_diagnostics_has_no_trading_simulation'},
    }
    if 'simulation' in values:
        result = values['simulation'].read_json('result.json')
        facts['financial_tradability'] = {
            'applicability': 'applicable', 'mode': 'daily_cash_simulation_v1',
            'simulation_result_hash': result['simulation_result_hash'],
            'source_ledger_hash': result['source_ledger_hash'],
            'bar_tca': {
                **{key: value for key, value in result.items() if key.startswith('tca_') and key not in {'tca_claim_ceiling', 'tca_reconciliation_delta_units', 'tca_liquidity_attribution_status', 'tca_table_rows'}},
                'claim_ceiling': result['tca_claim_ceiling'],
                'reconciliation_delta_units': result['tca_reconciliation_delta_units'],
                'liquidity_attribution_status': result['tca_liquidity_attribution_status'],
            },
        }
    return _json_value(facts)


def run(context, inputs, output_root):
    facts = collect_facts(context, inputs)
    directory = output_root / 'validity'
    directory.mkdir()
    (directory / 'result.json').write_text(
        json.dumps(facts, ensure_ascii=False, allow_nan=False, separators=(',', ':')),
        encoding='utf-8',
    )
    validity = output_root.commit_directory(
        port='validity', artifact_type='research.validity-facts.v1',
        relative_path='validity', files=('result.json',), publish_at_artifact_root=True,
    )
    values = {value.port: value for value in inputs}
    statistics = facts['statistics']
    metric = _table(values['summary'], 'metrics')[0]
    metric_table = pa.Table.from_pylist([{**metric, 'unit': 'squared_decimal_price_change',
        'sample_start': statistics['sample_start'], 'sample_end': statistics['sample_end'],
        'sample_size': statistics['sample_count'], 'status': 'computed'}])
    metrics = output_root.write_batches(
        port='metrics', artifact_type='project.qlib_demo.prediction-metrics.v1',
        relative_path='metrics/part-00000.parquet', schema=metric_table.schema,
        batches=metric_table.to_batches(),
    )
    return [validity, metrics]
