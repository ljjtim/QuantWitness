"""正式运行诊断决定是否可修复代码，资源与输出阶段失败保持原候选。"""
import json
from pathlib import Path

import pytest

from quantwitness_rdagent.contracts import write_json
from quantwitness_rdagent.feedback import candidate_needs_repair
from quantwitness_rdagent.worker import failure_diagnostics
from research_pipeline.runtime.errors import RuntimeWorkerError


@pytest.mark.parametrize("diagnostic,expected", [
    ({"stage": "code_validation", "error_type": "SyntaxError"}, True),
    ({"stage": "code_validation", "error_type": "ValueError", "message": "code.unsupported_import"}, True),
    ({"stage": "run", "error_code": "TypeError", "candidate_operator": True}, True),
    ({"stage": "run", "error_code": "TypeError", "candidate_operator": False}, False),
    ({"stage": "run", "error_code": "resource_measurement_unavailable", "candidate_operator": True}, False),
    ({"stage": "run", "error_code": "TypeError", "candidate_operator": True, "recommended_action": "wait"}, False),
    ({"stage": "report", "error_type": "OSError"}, False),
    ({"stage": "verify", "error_type": "RuntimeWorkerError"}, False),
    ({"stage": "run", "error_type": "RuntimeWorkerError"}, False),
])
def test_only_explicit_candidate_failures_are_repairable(diagnostic, expected):
    assert candidate_needs_repair({"command_status": "failed", "diagnostics": [diagnostic]}) is expected


def test_formula_fail_repairs_only_after_command_complete():
    assert candidate_needs_repair({"command_status": "succeeded", "formula_status": "fail", "diagnostics": ["formula.value_mismatch"]})
    assert not candidate_needs_repair({"command_status": "failed", "formula_status": "fail", "diagnostics": [{"stage": "report"}]})


def test_runtime_error_code_preserved_with_formal_node_inspection(tmp_path, monkeypatch):
    from quantwitness_rdagent import worker
    write_json(tmp_path / "execution/run/operator-dag-invocation.json", {})
    write_json(tmp_path / "execution/plan/admitted/operator-graph-plan.json", {"recipe": {"nodes": [
        {"node_id": "formula", "operator_id": "candidate", "operator_version": "2.0.0"},
        {"node_id": "analysis", "operator_id": "analysis", "operator_version": "1.0.0"}]}})
    write_json(tmp_path / "bundle/manifest.json", {"operator_spec": {"operator_id": "candidate", "operator_version": "2.0.0"}})
    def inspect(argv):
        assert argv[:2] == ["inspect", "--run-root"]
        return {"recommended_action": "retry-node", "nodes": {"formula": {"status": "exhausted", "last_error": {
            "error_code": "resource_measurement_unavailable", "exception_type": "RuntimeWorkerError", "message": "measurement unavailable"}}}}
    monkeypatch.setattr(worker, "_command", inspect)
    error = RuntimeWorkerError("operator 节点执行失败", error_code="resource_measurement_unavailable", failure_payload={"code": "resource_measurement_unavailable"})
    diagnostics = failure_diagnostics(error, stage="run", execution=tmp_path / "execution", bundle=tmp_path / "bundle")
    assert diagnostics[0]["error_code"] == "resource_measurement_unavailable"
    assert diagnostics[1]["candidate_operator"] is True and diagnostics[1]["node_id"] == "formula"
    assert not candidate_needs_repair({"command_status": "failed", "diagnostics": diagnostics})


def test_unknown_inspect_failure_stops_code_repair(tmp_path, monkeypatch):
    from quantwitness_rdagent import worker
    write_json(tmp_path / "execution/run/operator-dag-invocation.json", {})
    def inspect(argv):
        raise OSError("cannot inspect")
    monkeypatch.setattr(worker, "_command", inspect)
    diagnostics = failure_diagnostics(RuntimeWorkerError("unknown"), stage="run", execution=tmp_path / "execution")
    assert diagnostics[1]["stage"] == "inspect"
    assert not candidate_needs_repair({"command_status": "failed", "diagnostics": diagnostics})
