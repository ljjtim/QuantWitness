"""日频价格变化研究的受限计算与结果汇总。"""
from datetime import date, datetime
import json
import math
import statistics
import pyarrow as pa
import pyarrow.parquet as pq


def run(context, inputs, output_root):
    plan = context.parameters['causal_plan']
    item = plan['work_items'][0]
    design = context.parameters['design']
    sessions = design['calendar_sessions']
    columns = tuple(design['price_fields'])
    prices = {(str(row[columns[1]]), row[columns[0]].isoformat()): row[columns[2]]
              for batch in inputs[0].iter_batches(columns=columns, batch_size=8192)
              for row in batch.to_pylist()}
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
            row.update(value=value, status='ok' if valid else 'missing',
                       observation_time=datetime.fromisoformat(day+'T09:30:00+08:00'),
                       available_time=datetime.fromisoformat(item['decision_time']))
        else:
            first, last = prices.get((entity, day)), prices.get((entity, sessions[index+1]))
            if first is None or last is None or first <= 0 or last <= 0:
                raise ValueError('冻结标签窗口缺少合法价格，必须保留缺失诊断并停止本次模型验收')
            row.update(forward_return=last/first-1,
                       label_start_time=datetime.fromisoformat(day+'T15:00:00+08:00'),
                       label_end_time=datetime.fromisoformat(sessions[index+1]+'T15:00:00+08:00'))
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
    raw = [dict(session=r[columns[0]],entity_id=r[columns[1]],close=r[columns[2]])
           for b in data.iter_batches(columns=columns,batch_size=8192) for r in b.to_pylist()]
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
            for path in files:
                target=output_root/'result'/prefix/path
                target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(value.read_bytes(path))
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
