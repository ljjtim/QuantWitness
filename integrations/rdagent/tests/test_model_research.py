"""生成模型研究的开发隔离、预算修复和调用恢复。"""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from quantwitness_rdagent.model_research import ModelResearch
from quantwitness_rdagent.native_model_research import _context
from quantwitness_rdagent.contracts import write_json

BASELINE = {"nodes": [{"inputs": [-1], "width": 1, "activation": "identity"}]}
GENERATED = {"nodes": [{"inputs": [-1], "width": 3, "activation": "tanh"},
                       {"inputs": [-1, 0], "width": 1, "activation": "identity"}]}


def session(root):
    value = ModelResearch.__new__(ModelResearch)
    value.root, value.model_env_file = root, None
    value.metric_description = {"unit": "squared_ratio"}
    value.materials = {"text": "固定研究定义", "responses": [json.dumps({"definition": GENERATED})]}
    value.payload = {"budget": {"rounds": 2, "repairs": 1, "model_calls": 1, "output_tokens": 512, "max_output_tokens_per_call": 512},
                     "proposer": {"mode": "fixed_responses"}, "baseline": {"definition": BASELINE, "hypothesis": "基准", "reason": "依据"},
                     "package_template": {"objective": {"direction": "minimize"}, "development": {"end": "2025-01-01"}}}
    value.knowledge = None
    return value


def test_compile_repair_and_completed_proposal_resume_do_not_repeat_call(tmp_path, monkeypatch):
    from quantwitness_rdagent import native_model_research
    value = session(tmp_path)
    calls = []
    def propose(context, complete):
        calls.append(context)
        return {"hypothesis": "跳连网络", "reason": "前轮反思", "response": json.dumps({"definition": {"nodes": [{"inputs": [0], "width": 1, "activation": "identity"}]}})}
    monkeypatch.setattr(native_model_research, "propose_model", propose)
    actual = value.propose(1)
    assert actual["definition"] == GENERATED
    receipts = list((tmp_path / "calls").glob("*.json"))
    assert len(receipts) == 1
    assert "此前节点" in json.loads(receipts[0].read_text(encoding="utf-8"))["request"]["prompt"]
    snapshot = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob("*") if p.is_file()}
    assert value.propose(1) == actual and len(calls) == 1
    assert snapshot == {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob("*") if p.is_file()}


def test_model_context_only_contains_verified_development_metrics(tmp_path):
    value = session(tmp_path)
    for index, status in enumerate(("failed", "evaluated")):
        write_json(tmp_path / "rounds" / str(index) / "record.json", {"candidate_id": str(index), "status": status,
            "definition": BASELINE, "hypothesis": "h", "reason": "r", "metrics": {"value": 1.0, "rows": 2, "test": 999, "result_ref": "PRIVATE"}})
    context = value.context()
    assert "metrics" not in context["history"][0]
    assert context["history"][1]["metrics"] == {"value": 1.0, "rows": 2}
    assert "PRIVATE" not in json.dumps(context) and "test" not in json.dumps(context)
    _context(context)
    bad = deepcopy(context)
    bad["history"][0]["metrics"] = {"value": 0, "rows": 1}
    with pytest.raises(ValueError, match="技术失败"):
        _context(bad)


def test_structure_repair_budget_stops_without_evaluation(tmp_path, monkeypatch):
    from quantwitness_rdagent import native_model_research
    value = session(tmp_path)
    value.payload["budget"]["repairs"] = 0
    monkeypatch.setattr(native_model_research, "propose_model", lambda *args: {"hypothesis": "h", "reason": "r", "response": "{}"})
    actual = value.propose(1)
    assert actual["action"] == "reject" and not (tmp_path / "calls").exists()


def test_model_execution_recovers_completed_child_without_restarting(tmp_path, monkeypatch):
    from quantwitness_rdagent import model_execution
    payload = {'session_root': str(tmp_path / 'experiment'), 'candidate': 'one'}
    calls = []
    def execute(argv, **kwargs):
        from types import SimpleNamespace
        calls.append(argv)
        write_json(Path(payload['session_root']) / 'model-execution-metrics.json', {'value': 0.1, 'verification_status': 'pass'})
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(model_execution.subprocess, 'run', execute)
    first = model_execution.evaluate_model_package(payload)
    assert first == model_execution.evaluate_model_package(payload) and len(calls) == 1
    assert 'quantwitness_rdagent.model_execution' in calls[0]
    with pytest.raises(ValueError, match='冻结'):
        model_execution.evaluate_model_package({**payload, 'candidate': 'changed'})


def test_model_execution_failure_keeps_request_without_financial_result(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from quantwitness_rdagent import model_execution
    monkeypatch.setattr(model_execution.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=1))
    with pytest.raises(RuntimeError, match='尚未完成'):
        model_execution.evaluate_model_package({'session_root': str(tmp_path / 'experiment')})
    assert (tmp_path / 'experiment/model-execution-request.json').exists()
    assert not (tmp_path / 'experiment/model-execution-metrics.json').exists()
