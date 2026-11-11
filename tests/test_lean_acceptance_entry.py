"""正式验收入口的进程树峰值与Result组装计时。"""
import importlib.util
from importlib.machinery import ModuleSpec
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


PATH=Path(__file__).resolve().parents[1]/'integrations/lean/accept.py'
SPEC=importlib.util.spec_from_file_location('lean_acceptance_entry',PATH)
entry=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(entry)


@pytest.mark.parametrize('data_db,snapshot', [(None, None), ('source.duckdb', 'inputs.json')])
def test_input_source_conflict_is_rejected_before_output_creation(tmp_path, data_db, snapshot):
    output = tmp_path / 'run'
    with pytest.raises(ValueError, match='必须且只能'):
        entry.execute(SimpleNamespace(data_db=data_db, input_snapshot_manifest=snapshot, output=output))
    assert not output.exists()


def test_process_tree_ignores_exited_child_and_counts_each_pid_once(monkeypatch):
    class Process:
        def __init__(self,pid,rss):
            self.pid,self.rss=pid,rss
        def memory_info(self):
            if self.rss is None:
                raise entry.psutil.NoSuchProcess(self.pid)
            return SimpleNamespace(rss=self.rss)
    root=Process(1,100)
    child=Process(2,200)
    root.children=lambda recursive:[child,child,Process(3,None)]
    monkeypatch.setattr(entry.psutil,'Process',lambda pid:root)
    assert entry.sample_process_tree(1)==(300,2)


def test_exited_parent_returns_empty_sample(monkeypatch):
    def gone(pid):
        raise entry.psutil.NoSuchProcess(pid)
    monkeypatch.setattr(entry.psutil,'Process',gone)
    assert entry.sample_process_tree(1)==(0,0)


def finalize(attempt,status,second):
    return dict(kind='diagnostic',attempt_id=attempt,occurred_at=f'2026-10-29T10:00:{second:02d}+00:00',
                payload=dict(diagnostic_type='result_finalize_attempt',
                             finalize=dict(status=status,result_id='result' if status=='succeeded' else None)))


def test_finalize_duration_pairs_actual_attempt_and_ignores_failed_attempt(tmp_path):
    events=[finalize('a','pending',0),finalize('a','failed',2),
            finalize('b','pending',10),finalize('b','succeeded',17)]
    path=tmp_path/'events.jsonl'
    path.write_text(''.join(json.dumps(row)+'\n' for row in events),encoding='utf-8')
    assert entry.finalize_observations(path)==[dict(attempt_id='b',wall_seconds=7,result_id='result')]


@pytest.mark.parametrize('events',[[finalize('a','succeeded',3)],
                                 [finalize('a','pending',8),finalize('a','succeeded',3)]])
def test_finalize_missing_start_or_reverse_time_fails(tmp_path,events):
    path=tmp_path/'events.jsonl'
    path.write_text(''.join(json.dumps(row)+'\n' for row in events),encoding='utf-8')
    with pytest.raises(ValueError):
        entry.finalize_observations(path)


def test_report_request_binds_all_current_tables_by_schema():
    from research_pipeline.evidence.lean_report import REPORT_SCHEMAS, LEAN_REPORT_VERSION
    manifest = dict(result_id='formal-result', tables=[dict(schema_id=schema, table_id='actual-'+name)
        for name, schema in REPORT_SCHEMAS.items()])
    request = entry.build_report_request(manifest)
    assert request['contract_version'] == LEAN_REPORT_VERSION
    assert len(request['tables']) == 9
    assert request['tables']['account_events'] == 'actual-account_events'
    assert request['tables']['metrics'] == 'actual-metrics'
    manifest['tables'].pop()
    with pytest.raises(ValueError, match='唯一绑定'):
        entry.build_report_request(manifest)


def test_result_observation_reads_current_daily_and_artifact(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from research_pipeline.platform.lean_contract import LEAN_SCHEMA_IDS, LEAN_RESULT_VERSION
    result, run = tmp_path/'result', tmp_path/'run'
    result.mkdir()
    artifact = run/'external-artifacts/objects/artifact'
    artifact.mkdir(parents=True)
    (artifact/'result.json').write_text(json.dumps(dict(contract_version=LEAN_RESULT_VERSION,
        preparation=dict(run=str(run/'execution'), stage_seconds={'execute':2}))), encoding='utf-8')
    pq.write_table(pa.table({'payload_json':[json.dumps(dict(date='2024-01-03', day_wall_seconds=1.5, rows=240))]}), result/'daily.parquet')
    (result/'result.json').write_text(json.dumps(dict(tables=[dict(schema_id=LEAN_SCHEMA_IDS['daily'],
        artifact_key='artifact', files=['daily.parquet'])])), encoding='utf-8')
    (run/'events.jsonl').write_text('', encoding='utf-8')
    observed = entry.result_observations(result, run)
    assert observed['daily'] == [dict(date='2024-01-03', wall_seconds=1.5, rows=240)]
    assert observed['stage_seconds'] == {'execute':2}


@pytest.mark.parametrize('memory_gib', [4, 8, 12, 16])
@pytest.mark.parametrize('over_budget', [False, True])
def test_run_uses_declared_memory_for_dispatch_sampling_and_record(tmp_path, monkeypatch, memory_gib, over_budget):
    from research_pipeline.application import research_plan, research_run, result_delivery
    from research_pipeline.evidence.lean_report import REPORT_SCHEMAS

    memory_bytes = memory_gib * 2**30
    budget = dict(memory_bytes=memory_bytes, cpu_slots=2, temp_bytes=32 * 2**30, process_slots=6)
    output = tmp_path / 'execution'
    result = tmp_path / 'result'
    result.mkdir()
    manifest = dict(result_id='result', tables=[dict(schema_id=schema, table_id=name)
        for name, schema in REPORT_SCHEMAS.items()])
    (result / 'result.json').write_text(json.dumps(manifest), encoding='utf-8')
    prepared = SimpleNamespace(spec_payload=dict(fixed_clock='2024-01-03T16:00:00+08:00', root_seed=1))
    plan = object()
    calls = []

    class PreparationLoader:
        def create_module(self, spec):
            return None

        def exec_module(self, module):
            module.prepare_package = lambda *args: (
                dict(run_budget=budget, bundles=[], package=str(tmp_path / 'package')), prepared)

    original_spec = entry.importlib.util.spec_from_file_location
    monkeypatch.setattr(entry.importlib.util, 'spec_from_file_location',
        lambda name, path: ModuleSpec(name, PreparationLoader()) if name == 'lean_package_preparation'
        else original_spec(name, path))
    monkeypatch.setattr(research_plan, 'prepare_package', lambda args, package: (None, plan))

    def run(args, *, prepared_plan):
        assert prepared_plan is plan
        calls.append(args)
        return dict(status='result_finalized', result_directory=str(result))

    reports = []
    monkeypatch.setattr(research_run, '_execute', run)
    monkeypatch.setattr(result_delivery, '_execute', lambda args: reports.append(args))
    monkeypatch.setattr(entry, 'result_observations', lambda *args: dict(stage_seconds={}))
    monkeypatch.setattr(entry, 'sample_process_tree', lambda pid: (memory_bytes + int(over_budget), 1))
    # 入口更新进程环境；测试结束由 monkeypatch 恢复所有相关键。
    for key in ('PYTHONDONTWRITEBYTECODE', 'PYTHONNOUSERSITE', 'PYTHONUTF8', 'TEMP', 'TMP'):
        monkeypatch.setenv(key, entry.os.environ.get(key, ''))
    args = SimpleNamespace(data_db=None, input_snapshot_manifest=tmp_path / 'inputs.json',
        output=output, from_package=tmp_path / 'package', lean_home=tmp_path / 'lean',
        catalog_lock=tmp_path / 'catalog')
    if over_budget:
        with pytest.raises(RuntimeError, match='内存超过声明额度'):
            entry.execute(args)
    else:
        entry.execute(args)
    assert len(calls) == 1
    assert calls[0].resource_memory_bytes == memory_bytes
    assert calls[0].resource_cpu_slots == budget['cpu_slots']
    assert calls[0].resource_scratch_bytes == budget['temp_bytes']
    assert calls[0].resource_process_slots == budget['process_slots']
    record = json.loads((output / 'acceptance-timing.json').read_text(encoding='utf-8'))
    assert record['budgets']['run_memory_bytes'] == memory_bytes
    sampled = record['stage_resources']['run']
    assert sampled['memory_budget_bytes'] == memory_bytes
    assert sampled['peak_tree_rss_bytes'] == memory_bytes + int(over_budget)
    assert sampled['sampled_memory_within_budget'] is not over_budget
    assert record['status'] == ('failed' if over_budget else 'pass')
    assert len(reports) == (0 if over_budget else 2)
