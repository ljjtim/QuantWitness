"""日频价格变化研究的受限计算与结果汇总。"""
from datetime import date, datetime
import json
import math
import statistics
import re
import pyarrow as pa
import pyarrow.parquet as pq


def _expression_values(design, prices, keys, key_columns):
    """每个09:30决定只把前一会话及更早收盘交给Qlib。"""
    import pandas as pd
    from expressions import evaluate_expressions, validate_expression

    expressions = dict(design['feature_expressions'])
    expression = expressions.get('historical_return')
    compact = re.sub(r"\s+", "", expression) if isinstance(expression, str) else ''
    accepted = (
        r"\$close/(?:Ref|Mean)\(\$close,[1-5]\)-1(?:\.0)?",
        r"Std\(\$close,([2-5])\)/Mean\(\$close,\1\)",
        r"\(\$close-Min\(\$close,([2-5])\)\)/\(Max\(\$close,\1\)-Min\(\$close,\1\)\)",
    )
    if set(expressions) != {'historical_return'} or not any(re.fullmatch(pattern, compact) for pattern in accepted):
        raise ValueError('本例仅复核收盘动量、均价偏离、价格离散度和区间位置的有界窗口公式')
    spec = validate_expression(expressions['historical_return'], ['close'], max_window=5)
    sessions = design['calendar_sessions']
    grouped = {}
    for key in keys:
        row = dict(zip(key_columns, key))
        grouped.setdefault(row['observation_session'], set()).add(row['entity_id'])
    result = {}
    for day, entities in grouped.items():
        index = sessions.index(day)
        # 只送入表达式所需窗口，避免更早价格参与滚动方差的浮点累积。
        history_days = sessions[max(0, index - spec.lookback - 1):index]
        if not history_days:
            raise ValueError('表达式缺少决定时点前的收盘历史')
        records = [{'datetime': pd.Timestamp(session), 'instrument': entity,
                    'close': prices.get((entity, session))}
                   for entity in sorted(entities) for session in history_days]
        frame = pd.DataFrame(records).set_index(['datetime', 'instrument'])
        frame['close'] = frame['close'].where(frame['close'].gt(0) & frame['close'].map(math.isfinite))
        calculated = evaluate_expressions(frame, expressions, fields=['close'], max_window=5,
            min_periods='full', sessions=pd.DatetimeIndex(history_days),
            output_start=history_days[-1], output_end=history_days[-1])
        for entity in entities:
            needed = [prices.get((entity, session)) for session in history_days[-spec.lookback-1:]]
            complete = len(needed) == spec.lookback+1 and all(
                value is not None and math.isfinite(value) and value > 0 for value in needed)
            value = calculated.loc[(pd.Timestamp(history_days[-1]), entity), 'historical_return']
            result[(entity, day)] = float(value) if complete and math.isfinite(value) else None
    return result


def _factor_values(design, bars, keys, key_columns):
    """按决定日裁出前一会话及更早日线，不向求值器交付未来行。"""
    import pandas as pd
    from factor_baselines import evaluate_factor_suite

    suite = design['feature_suite']
    sessions = design['calendar_sessions']
    grouped = {}
    for key in keys:
        row = dict(zip(key_columns, key))
        grouped.setdefault(row['observation_session'], set()).add(row['entity_id'])
    result = {}
    for day, entities in grouped.items():
        index = sessions.index(day)
        history_days = sessions[max(0, index - suite['lookback'] - 1):index]
        records = [{'datetime': pd.Timestamp(session), 'instrument': entity,
                    **{role: bars.get((entity, session), {}).get(field) for role, field in design['factor_fields'].items()}}
                   for entity in sorted(entities) for session in history_days]
        frame = pd.DataFrame(records).set_index(['datetime', 'instrument'])
        calculated = evaluate_factor_suite(frame, suite, sessions=pd.DatetimeIndex(history_days),
            output_start=history_days[-1], output_end=history_days[-1])
        for entity in entities:
            for feature in suite['features']:
                value = calculated.loc[(pd.Timestamp(history_days[-1]), entity), feature]
                result[(entity, day, feature)] = float(value) if math.isfinite(value) else None
    return result


def run(context, inputs, output_root):
    plan = context.parameters['causal_plan']
    item = plan['work_items'][0]
    design = context.parameters['design']
    sessions = design['calendar_sessions']
    columns = tuple(design['price_fields'])
    source_columns = tuple(dict.fromkeys([*columns, *design.get('factor_fields', {}).values()])) if plan['kind'] == 'feature' else columns
    bars = {(str(row[columns[1]]), row[columns[0]].isoformat()): row
            for batch in inputs[0].iter_batches(columns=source_columns, batch_size=8192)
            for row in batch.to_pylist()}
    prices = {key: row[columns[2]] for key, row in bars.items()}
    factor_values = (_factor_values(design, bars, item['key_rows'], plan['key_columns'])
                     if plan['kind'] == 'feature' and 'feature_suite' in design else None)
    expression_values = (_expression_values(design, prices, item['key_rows'], plan['key_columns'])
                         if plan['kind'] == 'feature' and 'feature_expressions' in design else None)
    rows = []
    for key in item['key_rows']:
        row = dict(zip(plan['key_columns'], key))
        entity, day = row['entity_id'], row['observation_session']
        index = sessions.index(day)
        row['observation_session'] = date.fromisoformat(day)
        row['lineage_hash'] = context.parameters['lineage_ref']
        if plan['kind'] == 'feature':
            window = row['window_sessions']
            history = [prices.get((entity, d)) for d in sessions[index-window-1:index]]
            valid = len(history) == window+1 and all(x is not None and math.isfinite(x) and x > 0 for x in history)
            value = None
            if valid:
                returns = [b/a-1 for a,b in zip(history,history[1:])]
                value = history[-1]/history[0]-1 if row['feature_id']=='historical_return' else statistics.pstdev(returns)
            if expression_values is not None and row['feature_id'] == 'historical_return':
                value = expression_values[(entity, day)]
                valid = value is not None
            if factor_values is not None:
                value = factor_values[(entity, day, row['feature_id'])]
                valid = value is not None
            row.update(value=value, status='ok' if valid else 'missing',
                       observation_time=datetime.fromisoformat((sessions[index-1]+'T15:00:00+08:00') if factor_values is not None else day+'T09:30:00+08:00'),
                       available_time=datetime.fromisoformat(item['decision_time']))
        else:
            horizon = design.get('horizon_sessions', 1)
            if row['horizon_sessions'] != horizon:
                raise ValueError('标签期限与冻结研究声明不一致')
            first, last = prices.get((entity, day)), prices.get((entity, sessions[index+horizon]))
            if first is None or last is None or first <= 0 or last <= 0:
                raise ValueError('冻结标签窗口缺少合法价格，必须保留缺失诊断并停止本次模型验收')
            row.update(forward_return=last/first-1,
                       label_start_time=datetime.fromisoformat(day+'T15:00:00+08:00'),
                       label_end_time=datetime.fromisoformat(sessions[index+horizon]+'T15:00:00+08:00'))
        rows.append(row)
    table = pa.Table.from_pylist(rows)
    return output_root.write_batches(port=plan['output_port'], artifact_type='research.feature-set.v1' if plan['kind']=='feature' else 'research.label.v1',
                                     relative_path='rows.parquet',schema=table.schema,batches=table.to_batches())


def _table(value, prefix):
    rows = []
    for name in value.file_paths:
        if name.startswith(prefix+'/') and name.endswith('.parquet'):
            rows.extend(pq.ParquetFile(pa.BufferReader(value.read_bytes(name))).read().to_pylist())
    if not rows:
        raise ValueError('缺少输入表: '+prefix)
    return rows


def summarize(context, inputs, output_root):
    values = {v.port:v for v in inputs}
    data = values['data'].request('daily_feature')
    columns = tuple(context.parameters['design']['price_fields'])
    factor_fields = context.parameters['design'].get('factor_fields', {})
    raw_columns = tuple(dict.fromkeys([*columns, *factor_fields.values()]))
    raw = [dict(session=r[columns[0]],entity_id=r[columns[1]],close=r[columns[2]],
                **{role: r[field] for role, field in factor_fields.items() if role != 'close'})
           for b in data.iter_batches(columns=raw_columns,batch_size=8192) for r in b.to_pylist()]
    development = context.parameters['design'].get('mode') == 'development'
    predictions = _table(values['predictions'],'validation_predictions') if development else _table(values['holdout'],'holdout_predictions')
    mse = sum((r['prediction']-r['actual'])**2 for r in predictions)/len(predictions)
    tables = {'raw_prices':raw,'study_design':[{'design_json':json.dumps(dict(context.parameters['design']),ensure_ascii=False,default=dict)}],
              'metrics':[{'metric_ref':context.parameters['design']['metric_ref'],'value':mse}]}
    if development:
        tables['metrics'][0].update(
            session=max(str(row['observation_session']) for row in predictions),
            available_at=max(row['label_available_time'] for row in predictions),
            stage='validation')
    models=[]
    for port in (('fit',) if development else ('fit','holdout')):
        value=values[port]
        for row in _table(value,'models'):
            if row['status']!='fitted':
                raise ValueError('本次验收必须所有固定候选拟合成功')
            config=value.read_json(row['config_path'])
            prefix=port+'/'
            files=[config['model_path'],*[x for phase in config['processor_files'].values() for x in phase]]
            if config.get('schema') == 'research.qlib-sequence-model-bundle.v1':
                files.extend([config['weights_path'], *config['sequence']['files'].values()])
            if 'generated' in config:
                files.extend([config['generated']['source_path'], config['generated']['weights_path']])
            for path in files:
                target=output_root/'result'/prefix/path
                target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(value.read_bytes(path))
            if config.get('schema') == 'research.qlib-sequence-model-bundle.v1':
                config['weights_path'] = prefix + config['weights_path']
                config['effective_fit_kwargs']['save_path'] = config['weights_path']
                config['sequence']['files'] = {key: prefix + value for key, value in config['sequence']['files'].items()}
            if 'generated' in config:
                for field in ('source_path', 'weights_path'):
                    config['generated'][field] = prefix + config['generated'][field]
            config['model_path']=prefix+config['model_path']
            config['processor_files']={k:[prefix+x for x in v] for k,v in config['processor_files'].items()}
            target=output_root/'result'/prefix/row['config_path']
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(json.dumps(config,ensure_ascii=False),encoding='utf-8')
            models.append({**row,'config_path':prefix+row['config_path'],'model_path':prefix+row['model_path']})
    tables['models']=models
    for name,rows in tables.items():
        path=output_root/'result'/name/'part-00000.parquet'
        path.parent.mkdir(parents=True,exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows),path)
    root=output_root/'result'
    return output_root.commit_directory(port='result',artifact_type='project.qlib_demo.summary.v1',relative_path='result',
                                       files=tuple(sorted(p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file())),publish_at_artifact_root=True)
