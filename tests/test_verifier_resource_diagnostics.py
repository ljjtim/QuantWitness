"""从真实资源检查分支到 CLI 摘要的无数据库诊断验收。"""

from __future__ import annotations

import json
import time

import pytest

from research_pipeline.cli import main
from research_pipeline.cli.commands import evidence_lifecycle
from research_pipeline.evidence import project_verifier_runtime as runtime
from research_pipeline.platform import error_code_for_exception
from research_pipeline.platform.resource_budget import ResourceBudget
from research_pipeline.results.errors import ResultContractError


@pytest.mark.parametrize("dimension,code", [
    ("memory_bytes", "project_verifier_memory_exceeded"),
    ("process_slots", "project_verifier_process_slots_exceeded"),
    ("temp_bytes", "project_verifier_temp_exceeded"),
])
def test_real_guard_preserves_dimensions_and_error_code(monkeypatch, tmp_path, dimension, code):
    values = {"memory_bytes": 1, "process_slots": 1, "temp_bytes": 1}
    values[dimension] = 11
    monkeypatch.setattr(runtime, "_project_process_usage", lambda *args: (values["memory_bytes"], values["process_slots"]))
    monkeypatch.setattr(runtime, "_measure_attempt_tree_bytes", lambda root: values["temp_bytes"])
    guard = runtime._VerifierResources(tmp_path, ResourceBudget(10, 1, 10, 30), 10, time.monotonic())
    with pytest.raises(ResultContractError) as raised:
        guard.check()
    assert error_code_for_exception(raised.value) == code
    assert raised.value.failure_payload["exceeded"] == {dimension: {"actual": 11, "limit": 10}}


def test_guard_measurement_failure_is_not_budget_shortage(monkeypatch, tmp_path):
    def unavailable(*args):
        raise OSError("不可测量")
    monkeypatch.setattr(runtime, "_project_process_usage", unavailable)
    guard = runtime._VerifierResources(tmp_path, ResourceBudget(10, 1, 10, 30), 2, time.monotonic())
    with pytest.raises(runtime.ProjectVerifierResourceError) as raised:
        guard.check()
    assert raised.value.error_code == "project_verifier_measurement_unavailable"
    assert raised.value.failure_payload == {"resource_measurement_status": "measurement_unavailable"}


def test_verifier_failure_reaches_cli_summary(monkeypatch, tmp_path, capsys):
    import duckdb

    def forbidden(*args, **kwargs):
        raise AssertionError("诊断测试不得连接数据库")
    monkeypatch.setattr(duckdb, "connect", forbidden)
    monkeypatch.setattr(runtime, "_project_process_usage", lambda *args: (1, 4))
    monkeypatch.setattr(runtime, "_measure_attempt_tree_bytes", lambda root: 0)
    def verify(*args, **kwargs):
        runtime._VerifierResources(tmp_path, ResourceBudget(10, 1, 10, 30), 3, time.monotonic()).check()
    monkeypatch.setattr(evidence_lifecycle, "verify_result", verify)
    (tmp_path / "result").mkdir()
    (tmp_path / "store").mkdir()
    code = main(["verify", "--result", str(tmp_path / "result"), "--result-store", str(tmp_path / "store"), "--output", str(tmp_path / "out.json"), "--summary"])
    data = json.loads(capsys.readouterr().out)
    assert code == 1
    assert data["error_code"] == "project_verifier_process_slots_exceeded"
    assert data["summary"]["verification_status"] is None
    advice = data["summary"]["resource_advice"][0]
    assert advice["status"] == "measured_exceeded"
    assert advice["exceeded"]["process_slots"] == {"actual": 4, "limit": 3, "suggested_limit": 4}
