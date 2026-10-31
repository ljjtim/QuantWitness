"""执行正式应用主链并保留分阶段状态、日志和完整端到端时间。"""
import argparse
from contextlib import redirect_stdout, redirect_stderr
import importlib.util
import threading
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
import pyarrow.parquet as pq
import psutil


def sample_process_tree(pid):
    """采样当前存活进程树；退出中的进程不计入本次样本。"""
    try:
        parent=psutil.Process(pid)
        processes=[parent,*parent.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0,0
    rss=count=0
    for process in {item.pid:item for item in processes}.values():
        try:
            rss+=process.memory_info().rss
            count+=1
        except psutil.NoSuchProcess:
            continue
    return rss,count


def finalize_observations(events_path):
    """由同一finalize attempt的pending和succeeded计算实际Result组装时间。"""
    pending={}
    attempts=[]
    with Path(events_path).open(encoding='utf-8') as stream:
        for line in stream:
            event=json.loads(line)
            payload=event.get('payload',{})
            if event.get('kind')!='diagnostic' or payload.get('diagnostic_type')!='result_finalize_attempt':
                continue
            at=datetime.fromisoformat(event['occurred_at'].replace('Z','+00:00'))
            attempt=event['attempt_id']
            status=payload['finalize']['status']
            if status=='pending':
                pending[attempt]=at
            elif status=='succeeded':
                if attempt not in pending:
                    raise ValueError('Result finalize成功事件缺少同attempt的pending')
                seconds=(at-pending.pop(attempt)).total_seconds()
                if seconds<0:
                    raise ValueError('Result finalize事件时间倒退')
                attempts.append(dict(attempt_id=attempt,wall_seconds=seconds,
                    result_id=payload['finalize']['result_id']))
    return attempts


def result_observations(result, run_root):
    """按正式表绑定定位唯一执行工件，并读取已封存日耗时。"""
    result = Path(result)
    manifest=json.loads((result/'result.json').read_text(encoding='utf-8'))
    daily=[table for table in manifest['tables'] if table['schema_id']=='research.lean.daily.v1']
    if len(daily)!=1:
        raise ValueError('正式Result必须有唯一LEAN daily表')
    table=daily[0]
    artifact=run_root/'external-artifacts/objects'/table['artifact_key']
    payload=json.loads((artifact/'result.json').read_text(encoding='utf-8'))
    if payload.get('contract_version')!='research-lean-simulation-v1':
        raise ValueError('正式LEAN工件类型不一致')
    observation=dict(artifact_key=table['artifact_key'],stage_seconds=payload['preparation'].get('stage_seconds',{}))
    days=[]
    for relative in sorted(table['files']):
        with pq.ParquetFile(result/relative) as reader:
            for batch in reader.iter_batches(columns=['payload_json'],batch_size=256):
                for raw in batch.column(0).to_pylist():
                    row=json.loads(raw)
                    days.append(dict(date=row['date'],wall_seconds=row['day_wall_seconds'],rows=row['rows']))
    days.sort(key=lambda row:row['date'])
    middle=len(days)//2
    observation['daily']=days
    observation['daily_halves']={name:dict(sessions=len(rows),wall_seconds=sum(row['wall_seconds'] for row in rows),
        mean_seconds=sum(row['wall_seconds'] for row in rows)/len(rows) if rows else None)
        for name,rows in [('first',days[:middle]),('second',days[middle:])]}
    execution=Path(payload['preparation']['run']).resolve()
    if execution.is_relative_to(run_root.resolve()) and (execution/'resource-usage.json').is_file():
        observation['runtime_resources']=json.loads((execution/'resource-usage.json').read_text(encoding='utf-8'))
        observation['resource_source']='runtime_diagnostic'
    observation['result_finalize_attempts']=finalize_observations(run_root/'events.jsonl')
    return observation

def execute(args):
    data_db = getattr(args, 'data_db', None)
    snapshot_manifest = getattr(args, 'input_snapshot_manifest', None)
    if bool(data_db) == bool(snapshot_manifest):
        raise ValueError('必须且只能提供data-db或input-snapshot-manifest')
    source_argv = (['--data-db', str(data_db)] if data_db else
                   ['--input-snapshot-manifest', str(snapshot_manifest)])
    root = args.output.resolve()
    if root.exists():
        raise ValueError('运行输出目录必须不存在')
    root.mkdir(parents=True)
    environment=os.environ.copy()
    environment.pop('PYTHONPATH',None)
    environment.pop('PYTHONHOME',None)
    environment.update(PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1',PYTHONUTF8='1',TEMP=str(root/'temp'),TMP=str(root/'temp'))
    (root/'temp').mkdir()
    started=time.perf_counter()
    record=dict(status='running',started_at=datetime.now(timezone.utc).isoformat(),
                python=sys.executable,source_package=str(args.from_package),stage_seconds={},stage_resources={},commands={},
                budgets=dict(run_memory_bytes=12*2**30))
    def save():
        (root/'acceptance-timing.json').write_text(json.dumps(record,ensure_ascii=False,indent=2),encoding='utf-8')
    def stage(name, action, *, command):
        record['commands'][name]=command
        record['current_stage']=name
        save()
        at=time.perf_counter()
        resources=dict(peak_tree_rss_bytes=0,peak_processes=0,samples=0,
            sampling_interval_seconds=0.2,scope='stage_process_tree')
        finished=threading.Event()
        def sample():
            while True:
                rss,count=sample_process_tree(os.getpid())
                resources['peak_tree_rss_bytes']=max(resources['peak_tree_rss_bytes'],rss)
                resources['peak_processes']=max(resources['peak_processes'],count)
                resources['samples']+=1
                if finished.wait(0.2):
                    return
        sampler=threading.Thread(target=sample,daemon=True)
        sampler.start()
        try:
            with (root/(name+'.log')).open('w',encoding='utf-8') as log:
                with redirect_stdout(log),redirect_stderr(log):
                    value=action()
        finally:
            finished.set()
            sampler.join()
            record['stage_seconds'][name]=time.perf_counter()-at
            limit=12*2**30 if name=='run' else None
            resources.update(memory_budget_bytes=limit,
                sampled_memory_within_budget=resources['peak_tree_rss_bytes']<=limit if limit else None)
            record['stage_resources'][name]=resources
            save()
        if limit and not resources['sampled_memory_within_budget']:
            raise RuntimeError(name+'采样进程树内存超过声明额度')
        return value
    try:
        from research_pipeline.cli.parser import build_parser
        from research_pipeline.application.research_plan import prepare_package
        from research_pipeline.application.research_run import _execute as run_research
        from research_pipeline.application.result_delivery import _execute as deliver_result
        parser=build_parser()
        spec=importlib.util.spec_from_file_location('lean_package_preparation',Path(__file__).with_name('prepare.py'))
        preparation=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(preparation)
        os.environ.update(environment)
        conversion,prepared_package=stage('prepare',lambda: preparation.prepare_package(
            args.from_package,root/'conversion',args.lean_home),command=['prepare',str(args.from_package)])
        if conversion['run_budget']['memory_bytes'] != 12*2**30:
            raise ValueError('原包run内存必须已经声明12GiB，入口不扩大原包预算')
        bundles=[value for bundle in conversion['bundles'] for value in ('--extension-bundle',bundle)]
        package=conversion['package']
        declaration=prepared_package.spec_payload
        common=['--package',package,*bundles,'--catalog-lock',str(args.catalog_lock)]
        admission_argv=['package','admit',*common,*source_argv,'--output',str(root/'plan')]
        _,prepared_plan=stage('admit',lambda: prepare_package(
            parser.parse_args(admission_argv),package=prepared_package),command=admission_argv)
        budget=conversion['run_budget']
        run_argv=['run','--plan',str(root/'plan'),*source_argv,
            '--artifact-root',str(root/'artifacts'),
            '--run-root',str(root/'run'),'--result-store',str(root/'results'),
            '--clock',str(declaration['fixed_clock']),'--root-seed',str(declaration['root_seed']),
            '--resource-state-dir',str(root/'resource-state'),'--resource-cpu-slots',str(budget['cpu_slots']),
            '--resource-memory-bytes',str(12*2**30),'--resource-scratch-bytes',str(budget['temp_bytes']),
            '--resource-process-slots',str(budget['process_slots']),'--resource-timeout-seconds','120']
        if getattr(args, 'minute_root', None):
            run_argv.extend(['--minute-data-root', str(args.minute_root)])
        if getattr(args, 'persist_inputs', False):
            run_argv.append('--persist-inputs')
        run=stage('run',lambda: run_research(parser.parse_args(run_argv),prepared_plan=prepared_plan),command=run_argv)
        if run.get('status') != 'result_finalized':
            raise RuntimeError('run未成功，不能生成研究报告')
        result=run['result_directory']
        record['result_directory']=result
        manifest=json.loads((Path(result)/'result.json').read_text(encoding='utf-8'))
        table_ids={role:next(table['table_id'] for table in manifest['tables']
            if table['schema_id']==f'research.lean.{role}.v1') for role in ('daily','orders','metrics')}
        request=dict(contract_version='lean-research-report-v1',result_id=manifest['result_id'],
            tables=table_ids,budget=dict(max_rows=100000,memory_bytes=268435456))
        (root/'html-report-request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2),encoding='utf-8')
        report_argv=[
            ['report','--result',result,'--result-store',str(root/'results'),
             '--output',str(root/'report.md'),'--format','markdown'],
            ['report','--result',result,'--result-store',str(root/'results'),
             '--output',str(root/'report.html'),'--format','html',
             '--request',str(root/'html-report-request.json')]]
        stage('report',lambda: [deliver_result(parser.parse_args(argv)) for argv in report_argv],command=report_argv)
        record['end_to_end_seconds']=time.perf_counter()-started
        record['run_report_seconds']=sum(record['stage_seconds'][name] for name in ('run','report'))
        record['status']='pass'
        record['current_stage']='completed'
        # 节点阶段只用于归因；完整端到端时间不扣除上游节点或封存成本。
        observation=result_observations(result,root/'run')
        record['node_stage_observations']=observation
        record['run_seconds_outside_recorded_node_stages']=record['stage_seconds']['run']-sum(observation['stage_seconds'].values())
        record['peak_scratch_bytes']=observation.get('runtime_resources',{}).get('peak_scratch_bytes')
        files=logical_bytes=0
        for directory,_,names in os.walk(root):
            for name in names:
                path=Path(directory)/name
                try:
                    logical_bytes+=path.stat().st_size
                    files+=1
                except FileNotFoundError:
                    continue
        record['output_storage']=dict(file_count=files,logical_bytes=logical_bytes)

    except BaseException as error:
        record.update(status='failed',error=str(error),end_to_end_seconds=time.perf_counter()-started)
        save()
        raise
    save()
    print(json.dumps(record,ensure_ascii=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('from-package','lean-home','catalog-lock','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--data-db', type=Path)
    source.add_argument('--input-snapshot-manifest', type=Path)
    parser.add_argument('--minute-root', type=Path)
    parser.add_argument('--persist-inputs', action='store_true')
    execute(parser.parse_args())


