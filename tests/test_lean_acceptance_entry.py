"""正式验收入口的进程树峰值与Result组装计时。"""
import importlib.util
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
