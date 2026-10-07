"""联合调度预算、方向引用及跨分支开发反馈合同。"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from quantwitness_rdagent.factor_research import ResearchBudgetExhausted, ResearchCalls
from quantwitness_rdagent.joint_feedback import validate_joint_feedback


def record():
    return {"record_id": "joint:0000", "kind": "factor", "candidate_id": "factor_0000", "status": "evaluated",
            "expression": "$close / Ref($close, 5) - 1", "hypothesis": "动量", "reason": "已验证开发基线",
            "metrics": {"value": 0.5, "rows": 1}}


def test_shared_call_budget_and_resume(tmp_path):
    owner = SimpleNamespace(root=tmp_path, payload={"budget": {"model_calls": 2, "output_tokens": 1024, "max_output_tokens_per_call": 512},
        "proposer": {"mode": "fixed_responses"}}, materials={"responses": ["factor-response", "model-response"]})
    factor = SimpleNamespace(call_owner=owner, call_prefix="factor-")
    model = SimpleNamespace(call_owner=owner, call_prefix="model-")
    assert ResearchCalls(factor, "0001-proposal")("factor", instructions="f") == "factor-response"
    assert ResearchCalls(model, "0002-proposal")("model", instructions="m") == "model-response"
    assert ResearchCalls(factor, "0001-proposal")("factor", instructions="f") == "factor-response"
    with pytest.raises(ResearchBudgetExhausted):
        ResearchCalls(model, "0002-reflection")("new", instructions="m")
    assert len(list((tmp_path / "calls").glob("*.json"))) == 2


def test_reserved_call_is_not_repeated(tmp_path):
    owner = SimpleNamespace(root=tmp_path, payload={"budget": {"model_calls": 1, "output_tokens": 512, "max_output_tokens_per_call": 512},
        "proposer": {"mode": "fixed_responses"}}, materials={"responses": ["unused"]})
    path = tmp_path / "calls" / "direction-00.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"status": "reserved", "request": {"prompt": "next", "instructions": "direction", "max_output_tokens": 512}}))
    with pytest.raises(RuntimeError, match="outcome_unknown"):
        ResearchCalls(owner, "direction")("next", instructions="direction")


@pytest.mark.parametrize("change", [{"status": "failed"}, {"holdout": 1}, {"metrics": {"value": float("inf"), "rows": 1}}, {"metrics": {"value": 0.2, "rows": 0}}, {"metrics": {"value": 0.2, "rows": 1, "test": 0.1}}])
def test_joint_feedback_rejects_unverified_or_future_fields(change):
    row = record()
    row.update(change)
    with pytest.raises(ValueError):
        validate_joint_feedback([row])


def test_direction_uses_upstream_and_real_refs():
    from quantwitness_rdagent.joint_research import select_joint_direction
    calls = []
    def complete(prompt, **kwargs):
        calls.append({"prompt": prompt, **kwargs})
        return json.dumps({"action": "model", "direction": "generated_model", "knowledge_refs": ["joint:0000"], "reason": "开发因子误差支持检验非线性"})
    result = select_joint_direction({"knowledge": [record()], "max_output_tokens": 512}, complete)
    assert result["action"] == "model"
    assert len(calls) == 1 and "joint:0000" in calls[0]["prompt"] and "0.5" in calls[0]["prompt"]
    def invalid(prompt, **kwargs):
        return json.dumps({"action": "model", "direction": "generated_model", "knowledge_refs": ["unknown"], "reason": "无来源"})
    with pytest.raises(ValueError, match="knowledge_refs"):
        select_joint_direction({"knowledge": [record()], "max_output_tokens": 512}, invalid)


def test_model_prompt_consumes_factor_knowledge():
    from quantwitness_rdagent.native_model_research import propose_model
    replies = iter([json.dumps({"hypothesis": "非线性", "reason": "消费因子开发反馈"}), json.dumps({"definition": {"nodes": [{"inputs": [-1], "width": 1, "activation": "identity"}]}})])
    prompts = []
    def complete(prompt, **kwargs):
        prompts.append(prompt)
        return next(replies)
    context = {"confirmed_spec": "固定开发研究", "development": {}, "objective": {}, "history": [], "max_output_tokens": 512,
               "joint_knowledge": [record()], "joint_direction": {"action": "model", "direction": "generated_model"}}
    propose_model(context, complete)
    assert len(prompts) == 2 and all("joint:0000" in prompt for prompt in prompts)


def test_failed_evaluation_consumes_budget_and_resume_reuses(tmp_path):
    from quantwitness_rdagent.joint_research import JointResearch
    calls = []
    class FailedBranch:
        def evaluate(self, index, proposal):
            calls.append(index)
            return {**proposal, "status": "failed", "reason": "verification_failed"}
    session = object.__new__(JointResearch)
    session.root = tmp_path
    session.payload = {"budget": {"evaluations": 1}}
    session.branches = {"factor": FailedBranch()}
    proposal = {"kind": "factor", "candidate_id": "f0", "round": 0}
    first = session.evaluate(0, proposal)
    assert first["status"] == "failed"
    assert session.evaluate(0, proposal) == first and calls == [0]
    second = session.evaluate(1, {"kind": "factor", "candidate_id": "f1", "round": 1})
    assert second["status"] == "stopped" and second["reason"] == "evaluation_budget" and calls == [0]
    assert len(list((tmp_path / "rounds").glob("*/evaluation-reservation.json"))) == 1


def test_population_requires_common_validation_and_complete_factor():
    from copy import deepcopy
    from quantwitness_rdagent.joint_feedback import development_population
    model = {"mode": "walk_forward_development_v1", "tables": {
        "samples": [{"sample_id": "s1", "target": 0.2, "label_start_time": "t1", "label_end_time": "t2", "label_available_time": "t3", "historical_return__w5": 0.1, "volatility__w5": 0.2}],
        "validation_predictions": [{"fold_id": "f1", "sample_id": "s1", "actual": 0.2, "label_end_time": "t2", "label_available_time": "t3", "candidate_id": "a"}]}}
    baseline = development_population(model)
    candidate = deepcopy(model)
    candidate["tables"]["validation_predictions"].append({**candidate["tables"]["validation_predictions"][0], "candidate_id": "b"})
    assert development_population(candidate) == baseline
    candidate["tables"]["samples"][0]["historical_return__w5"] = None
    with pytest.raises(ValueError, match="完整值"):
        development_population(candidate)
    candidate = deepcopy(model)
    candidate["tables"]["validation_predictions"][0]["actual"] = 0.3
    assert development_population(candidate) != baseline


def test_failed_model_metric_is_recorded_with_reserved_budget(tmp_path):
    from quantwitness_rdagent.joint_research import JointResearch
    class FailedModel:
        def evaluate(self, index, proposal):
            raise ValueError("未通过独立验证")
    session = object.__new__(JointResearch)
    session.root = tmp_path
    session.payload = {"budget": {"evaluations": 1}}
    session.branches = {"model": FailedModel()}
    result = session.evaluate(1, {"kind": "model", "candidate_id": "m1", "round": 1})
    assert result["status"] == "failed" and "metrics" not in result
    assert (tmp_path / "rounds/0001/evaluation-reservation.json").exists()


def test_failed_record_stays_in_branch_history_without_financial_feedback(tmp_path):
    from quantwitness_rdagent.joint_research import JointResearch
    session = object.__new__(JointResearch)
    session.root = tmp_path
    session.payload = {"campaign_id": "joint", "budget": {"rounds": 5}}
    branch = SimpleNamespace(root=tmp_path / "branches/factor")
    session.branches = {"factor": branch}
    value = {"kind": "factor", "round": 1, "candidate_id": "factor_0001", "status": "failed", "reason": "verification_failed", "expression": "$close / Ref($close, 3) - 1"}
    session.record(1, value)
    assert json.loads((branch.root / "rounds/0001/record.json").read_text(encoding="utf-8")) == value
    assert session.knowledge_view() == []
