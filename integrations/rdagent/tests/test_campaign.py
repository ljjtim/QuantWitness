"""研究反馈、预约预算与恢复，不调用真实模型或数据库。"""
import copy
import importlib.util
import json
from pathlib import Path

import pytest
from quantwitness_rdagent import campaign


def prepare(tmp_path):
    path = Path(__file__).resolve().parents[1] / "examples/prediction_campaign/prepare.py"
    spec = importlib.util.spec_from_file_location("prediction_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.prepare(tmp_path / "case")


def step(session, index, proposal=None):
    proposal = session.propose(index) if proposal is None else proposal
    result = session.evaluate(index, proposal)
    session.record(index, result)
    return result


def live_payload(payload):
    payload["proposer"] = {"mode": "live", "model": "fixture", "base_url": "https://example.invalid/v1"}
    payload["budget"].update(model_calls=1, output_tokens=100, max_output_tokens_per_call=100)
    return payload


def test_two_rounds_new_hypothesis_and_completed_recovery(tmp_path, monkeypatch):
    payload = prepare(tmp_path)
    session = campaign.Campaign(payload)
    first = step(session, 0)
    proposal = session.propose(1)
    assert proposal["parent_id"] == "baseline" and proposal["candidate_id"] == "half"
    second = step(session, 1)
    assert first["metrics"]["mse"] == pytest.approx(0.0000255)
    assert second["metrics"]["mse"] == 0
    outcome = campaign._read(session.root / "outcome.json")
    assert outcome["selected_candidate_id"] == "half"
    assert outcome["stop_reason"] == "target_reached"
    before = {p.relative_to(session.root): (p.read_bytes(), p.stat().st_mtime_ns) for p in session.root.rglob("*") if p.is_file()}
    recovered = campaign.Campaign(payload)
    monkeypatch.setattr(campaign, "evaluate_candidate", lambda *a: pytest.fail("已完成评价不可重算"))
    assert recovered.evaluate(1, proposal) == second
    recovered.record(1, second)
    recovered.finish("target_reached")
    after = {p.relative_to(session.root): (p.read_bytes(), p.stat().st_mtime_ns) for p in session.root.rglob("*") if p.is_file()}
    assert before == after


def test_fixed_next_hypothesis_changes_with_development_feedback():
    candidates = [{"id": "a", "shrinkage": 1}, {"id": "b", "shrinkage": 0.5}, {"id": "c", "shrinkage": 0.25}, {"id": "d", "shrinkage": 0.75}]
    history = [{"candidate_id": "a", "status": "evaluated", "metrics": {"mse": 1}, "improvement": 0},
               {"candidate_id": "b", "status": "evaluated", "metrics": {"mse": 0.5}, "improvement": 0.5}]
    assert campaign.fixed_proposal(candidates, history, "a")["candidate_id"] == "c"
    history[-1].update(metrics={"mse": 2}, improvement=-1)
    assert campaign.fixed_proposal(candidates, history, "a")["candidate_id"] == "d"


@pytest.mark.parametrize("proposal", [[], {"action": "evaluate", "candidate_id": "outside", "parent_id": "baseline", "reason": "研究"},
    {"action": "evaluate", "candidate_id": "baseline", "parent_id": "baseline", "reason": "重复"},
    {"action": "evaluate", "candidate_id": "half", "parent_id": "unknown", "reason": "未知父节点"},
    {"action": "evaluate", "candidate_id": "half", "parent_id": "baseline", "reason": "改变范围", "table_id": "holdout_predictions"}])
def test_rejected_proposals_retained_without_evaluation(tmp_path, monkeypatch, proposal):
    session = campaign.Campaign(prepare(tmp_path))
    step(session, 0)
    monkeypatch.setattr(campaign, "evaluate_candidate", lambda *a: pytest.fail("非法候选不可评价"))
    result = step(session, 1, proposal)
    assert result["status"] == "rejected"
    assert session.history()[1] == result
    assert campaign._read(session.root / "outcome.json")["selected_candidate_id"] == "baseline"


def test_live_prompt_and_completed_call_reuse(tmp_path, monkeypatch):
    from quantwitness_rdagent import model_client
    payload = live_payload(prepare(tmp_path))
    session = campaign.Campaign(payload)
    step(session, 0)
    calls = []
    monkeypatch.setattr(model_client, "public_config", lambda p: {"model": "fixture", "base_url": "https://example.invalid/v1"})
    def generate(env, prompt, maximum, **kwargs):
        calls.append(json.loads(prompt))
        assert maximum == 100
        return {"model": "fixture", "text": json.dumps({"action": "evaluate", "candidate_id": "half", "parent_id": "baseline", "reason": "检验收缩"}), "usage": {"output_tokens": 30}}
    monkeypatch.setattr(model_client, "request_text", generate)
    proposal = session.propose(1, "fixture.env")
    assert session.propose(1, "fixture.env") == proposal
    assert len(calls) == 1
    assert set(calls[0]) == {"question", "objective", "candidates", "baseline_id", "history"}
    assert "actual" not in json.dumps(calls) and "result_id" not in json.dumps(calls)
    step(session, 1, proposal)
    assert campaign._read(session.root / "model-calls/0001.json")["reserved_output_tokens"] == 100


def test_reserved_call_cannot_repeat_and_failed_evaluation_retained(tmp_path, monkeypatch):
    from quantwitness_rdagent import model_client
    session = campaign.Campaign(live_payload(prepare(tmp_path)))
    step(session, 0)
    monkeypatch.setattr(model_client, "public_config", lambda p: {"model": "fixture", "base_url": "https://example.invalid/v1"})
    campaign.write_json(session.root / "model-calls/0001.json", {"status": "reserved", "reserved_output_tokens": 100})
    monkeypatch.setattr(model_client, "request_text", lambda *a, **k: pytest.fail("已预约不可重复调用"))
    with pytest.raises(RuntimeError, match="预约"):
        session.propose(1, "fixture.env")
    def failed(*args):
        raise ValueError("样本不可评价")
    monkeypatch.setattr(campaign, "evaluate_candidate", failed)
    record = step(session, 1, {"action": "evaluate", "candidate_id": "half", "parent_id": "baseline", "reason": "研究"})
    assert record["status"] == "failed"
    assert session.history()[1]["status"] == "failed"


@pytest.mark.parametrize("change", ["budget", "data", "labels"])
def test_resume_freezes_budget_and_input(tmp_path, change):
    payload = prepare(tmp_path)
    campaign.Campaign(payload)
    if change == "budget":
        payload["budget"]["rounds"] += 1
    else:
        fixture = campaign._read(payload["source"]["path"])
        fixture["rows"][0]["prediction" if change == "data" else "actual"] += 0.1
        campaign.write_json(payload["source"]["path"], fixture)
    with pytest.raises(ValueError, match="冻结"):
        campaign.Campaign(payload)


def test_fold_equal_weight_and_sample_alignment(tmp_path):
    rows = [{"candidate_id": "a", "fold_id": "one", "prediction": 1, "actual": 0}] * 3 + [{"candidate_id": "a", "fold_id": "two", "prediction": 3, "actual": 0}]
    metric = campaign.evaluate_candidate(rows, {"model_candidate_id": "a", "shrinkage": 1}, ["one", "two"])
    assert metric["mse"] == 5
    payload = prepare(tmp_path)
    payload["candidates"].append({"id": "missing", "model_candidate_id": "absent", "shrinkage": 1})
    with pytest.raises(ValueError, match="样本"):
        campaign.Campaign(payload)


@pytest.mark.parametrize("condition,expected", [("rounds", "round_budget"), ("evaluations", "evaluation_budget"), ("patience", "no_improvement"), ("stop", "proposer_stop")])
def test_explicit_stop_conditions(tmp_path, condition, expected):
    payload = prepare(tmp_path)
    payload["stop"]["target_mse"] = None
    payload["budget"].update(rounds=4, evaluations=4)
    payload["candidates"].append({"id": "third", "model_candidate_id": "synthetic_model", "shrinkage": 0.75})
    if condition in ("rounds", "evaluations"):
        payload["budget"][condition] = 1
    if condition == "patience":
        payload["stop"].update(patience=1, min_improvement=1)
    session = campaign.Campaign(payload)
    step(session, 0)
    if condition == "patience":
        step(session, 1)
    if condition == "stop":
        step(session, 1, {"action": "stop", "reason": "信息不足"})
    assert campaign._read(session.root / "outcome.json")["stop_reason"] == expected


def test_rejection_does_not_spend_evaluation_budget(tmp_path):
    payload = prepare(tmp_path)
    payload["budget"]["rounds"] = 3
    session = campaign.Campaign(payload)
    step(session, 0)
    step(session, 1, {"action": "invalid"})
    assert session.stop_reason() is None
    assert step(session, 2)["status"] == "evaluated"


def test_failed_baseline_allows_next_candidate(tmp_path, monkeypatch):
    session = campaign.Campaign(prepare(tmp_path))
    original = campaign.evaluate_candidate
    def fail(*args):
        raise OverflowError("平方溢出")
    monkeypatch.setattr(campaign, "evaluate_candidate", fail)
    assert step(session, 0)["status"] == "failed"
    monkeypatch.setattr(campaign, "evaluate_candidate", original)
    assert step(session, 1)["status"] == "evaluated"


def test_prompt_does_not_recycle_proposer_reason(tmp_path):
    session = campaign.Campaign(prepare(tmp_path))
    step(session, 0, {"action": "evaluate", "candidate_id": "baseline", "parent_id": None, "reason": "unverified_model_claim"})
    assert "unverified_model_claim" not in session.prompt()


def test_model_budget_stop_has_explicit_reason(tmp_path, monkeypatch):
    from quantwitness_rdagent import model_client
    session = campaign.Campaign(live_payload(prepare(tmp_path)))
    step(session, 0)
    campaign.write_json(session.root / "model-calls/0000.json", {"status": "completed", "reserved_output_tokens": 100})
    monkeypatch.setattr(model_client, "public_config", lambda p: {"model": "fixture", "base_url": "https://example.invalid/v1"})
    proposal = session.propose(1, "fixture.env")
    step(session, 1, proposal)
    assert campaign._read(session.root / "outcome.json")["stop_reason"] == "model_budget"
