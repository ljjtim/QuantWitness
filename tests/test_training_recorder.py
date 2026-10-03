"""训练记录限定节点范围，启动和结束失败保留真实诊断。"""
from __future__ import annotations

from copy import deepcopy
import os
import sqlite3

import mlflow
import pytest
from qlib.config import C
from qlib.workflow import R
from qlib.workflow.recorder import MLflowRecorder

from research_pipeline.research.modeling.qlib import _training_recorder


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    import duckdb

    def reject(*args, **kwargs):
        raise AssertionError("训练记录测试不得连接数据库")

    monkeypatch.setattr(sqlite3, "connect", reject)
    monkeypatch.setattr(duckdb, "connect", reject)


def _state():
    return (R._provider, deepcopy(C.exp_manager), mlflow.get_tracking_uri(),
            os.environ.get("MLFLOW_ALLOW_FILE_STORE"), mlflow.active_run())


def test_recorder_skips_repository_diff_and_preserves_metrics(tmp_path, monkeypatch):
    import qlib.workflow.recorder as upstream

    before = _state()
    hook = MLflowRecorder._log_uncommitted_code
    calls = []
    original_check_output = upstream.subprocess.check_output

    def repository_output(command, *args, **kwargs):
        if isinstance(command, str) and command.startswith("git "):
            calls.append(command)
            return b"\xff"
        return original_check_output(command, *args, **kwargs)

    monkeypatch.setattr(upstream.subprocess, "check_output", repository_output)
    output = tmp_path / "records"
    with _training_recorder(output):
        run_id = mlflow.active_run().info.run_id
        R.log_metrics(training_loss=0.125, step=2)
    assert calls == []
    assert _state() == before
    assert MLflowRecorder._log_uncommitted_code is hook
    assert not list(output.rglob("code_diff.txt"))
    assert not list(output.rglob("code_cached.txt"))
    client = mlflow.tracking.MlflowClient(tracking_uri=output.resolve().as_uri())
    run = client.get_run(run_id)
    assert run.info.status == "FINISHED"
    assert run.data.metrics["training_loss"] == 0.125


@pytest.mark.parametrize("opened", [False, True])
def test_start_failure_releases_owned_state(tmp_path, monkeypatch, opened):
    before = _state()
    original_start = MLflowRecorder.start_run
    problem = ValueError("训练记录启动失败")

    def fail_start(self):
        if opened:
            original_start(self)
        raise problem

    with monkeypatch.context() as patch:
        patch.setattr(MLflowRecorder, "start_run", fail_start)
        with pytest.raises(ValueError) as raised:
            with _training_recorder(tmp_path / "failed"):
                pytest.fail("启动失败后不得开始训练")
    assert raised.value is problem
    assert _state() == before
    with _training_recorder(tmp_path / "next"):
        R.log_metrics(training_loss=0.25, step=1)
    assert _state() == before


@pytest.mark.parametrize("primary_failure", [False, True])
def test_end_failure_preserves_primary_error_and_clears_active_run(
    tmp_path, monkeypatch, caplog, primary_failure,
):
    before = _state()
    problem = ValueError("模型训练失败")
    cleanup = OSError("训练记录结束失败")
    original_end = MLflowRecorder.end_run

    def fail_end(self, status):
        # 模拟异步日志已排空、MLflow run 尚未结束时的文件存储失败。
        if self.async_log is not None:
            self.async_log.wait()
            self.async_log = None
        raise cleanup

    with monkeypatch.context() as patch:
        patch.setattr(MLflowRecorder, "end_run", fail_end)
        with pytest.raises(ValueError if primary_failure else OSError) as raised:
            with _training_recorder(tmp_path / "failed"):
                R.log_metrics(training_loss=0.5, step=0)
                if primary_failure:
                    raise problem
    assert raised.value is (problem if primary_failure else cleanup)
    assert _state() == before
    if primary_failure:
        assert "训练记录清理失败" in caplog.text
    assert MLflowRecorder.end_run is original_end


def test_start_and_cleanup_failures_keep_start_error(tmp_path, monkeypatch):
    before = _state()
    original_start = MLflowRecorder.start_run
    original_end = MLflowRecorder.end_run
    problem = ValueError("训练记录已打开后启动失败")

    def fail_start(self):
        original_start(self)
        raise problem

    def fail_end(self, status):
        original_end(self, status)
        raise OSError("记录清理失败")

    monkeypatch.setattr(MLflowRecorder, "start_run", fail_start)
    monkeypatch.setattr(MLflowRecorder, "end_run", fail_end)
    with pytest.raises(ValueError) as raised:
        with _training_recorder(tmp_path / "failed"):
            pytest.fail("启动失败后不得开始训练")
    assert raised.value is problem
    assert _state() == before
