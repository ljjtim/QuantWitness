"""知识引用必须影响实际表达式，且方向调用受原生预算和恢复约束。"""
import copy
import json

import pytest

from quantwitness_rdagent.direction_selection import knowledge_view, select_direction
from quantwitness_rdagent.native_research import propose_factor
from test_native_research import context, FixedRequest, hypothesis, factor, native


def knowledge_context():
    value = context()
    value.update(objective={"direction": "minimize"}, development={"end": "2020-01-10"})
    value["knowledge"] = [{"record_id": "prior/mean3/result", "kind": "negative", "status": "evaluated",
        "expression": "$close / Mean($close, 3) - 1", "hypothesis": "三期平滑", "reason": "开发误差未改善",
        "metrics": {"value": 0.3, "rows": 1}}]
    return value


def direction():
    return {"action": "factor", "direction": "mean_deviation", "knowledge_refs": ["prior/mean3/result"],
            "reason": "保留三期负结果，检验较长平滑窗口"}


@native
def test_direction_is_cited_and_constrains_experiment():
    value = knowledge_context()
    complete = FixedRequest([direction()])
    value["direction"] = select_direction(value, complete)
    assert "prior/mean3/result" in complete.calls[0]["prompt"]
    experiment = factor("$close / Mean($close, 4) - 1")
    next(iter(experiment.values()))["knowledge_refs"] = ["prior/mean3/result"]
    complete = FixedRequest([hypothesis(), experiment])
    result = propose_factor(value, complete)
    assert result["knowledge_refs"] == ["prior/mean3/result"]
    assert result["expression"] == "$close / Mean($close, 4) - 1"
    assert all("prior/mean3/result" in item["prompt"] for item in complete.calls)


@native
@pytest.mark.parametrize("fault", ["reference", "branch", "family", "repeat"])
def test_invalid_reference_branch_or_repeated_experiment_is_rejected(fault):
    value = knowledge_context()
    decision = direction()
    if fault == "reference":
        decision["knowledge_refs"] = ["unretrieved"]
    if fault == "branch":
        decision.update(action="model", direction="model")
    if fault in {"reference", "branch"}:
        with pytest.raises(ValueError):
            select_direction(value, FixedRequest([decision]))
        return
    value["direction"] = decision
    expression = "$close / Ref($close, 4) - 1" if fault == "family" else "$close / Mean($close, 3) - 1"
    experiment = factor(expression)
    next(iter(experiment.values()))["knowledge_refs"] = decision["knowledge_refs"]
    with pytest.raises(ValueError):
        propose_factor(value, FixedRequest([hypothesis(), experiment]))


def test_no_knowledge_stops_without_model_call():
    complete = FixedRequest([])
    value = knowledge_context()
    value["knowledge"] = []
    assert select_direction(value, complete)["reason"] == "no_eligible_knowledge"
    assert complete.calls == []


def test_knowledge_projection_keeps_negative_evidence_without_paths():
    records = [{"record_id": "a", "usable_for_proposal": True, "status": "no_improvement",
        "kind": "negative", "metrics": {"value": 1, "rows": 1}, "source": {"result_ref": "PRIVATE"}},
        {"record_id": "b", "usable_for_proposal": True, "status": "technical_failure",
         "kind": "negative", "reflection": {}, "expression": None}]
    actual = knowledge_view(records)
    assert actual[0]["status"] == "evaluated" and actual[0]["kind"] == "negative"
    assert actual[1] == {"record_id": "b", "kind": "negative", "status": "failed"}
    assert "PRIVATE" not in json.dumps(actual)


def test_factor_proposal_preserves_direction_and_reference_on_resume(tmp_path, monkeypatch):
    from test_factor_research import session
    from quantwitness_rdagent import direction_selection, native_research
    value = session(tmp_path)
    value.payload["knowledge"] = {"index": "unused", "output": "unused", "max_records": 8}
    value.knowledge_records = [dict(knowledge_context()["knowledge"][0], usable_for_proposal=True)]
    calls = []
    def choose(*args):
        calls.append("direction")
        return direction()
    def propose(*args):
        calls.append("proposal")
        return {"expression": "$close / Mean($close, 4) - 1", "hypothesis": "h", "reason": "r",
                "knowledge_refs": direction()["knowledge_refs"]}
    monkeypatch.setattr(direction_selection, "select_direction", choose)
    monkeypatch.setattr(native_research, "propose_factor", propose)
    first = value.propose(1)
    assert first["direction"] == direction() and "action" not in first
    assert value.propose(1) == first and calls == ["direction", "proposal"]


def test_normalized_knowledge_expression_is_rejected_before_execution(tmp_path, monkeypatch):
    from test_factor_research import session
    from quantwitness_rdagent import direction_selection, native_research
    value = session(tmp_path)
    value.payload["knowledge"] = {"index": "unused", "output": "unused", "max_records": 8}
    value.knowledge_records = [dict(knowledge_context()["knowledge"][0], usable_for_proposal=True)]
    monkeypatch.setattr(direction_selection, "select_direction", lambda *args: direction())
    monkeypatch.setattr(native_research, "propose_factor", lambda *args: {
        "expression": "$close / Mean($close, 3) - 1.0", "hypothesis": "h", "reason": "r",
        "knowledge_refs": direction()["knowledge_refs"]})
    proposal = value.propose(1)
    assert proposal["action"] == "reject"
    assert proposal["reason"] == "unsupported_or_repeated_expression"
    class Template:
        data = {}
        def prepare_data(self): return {}
    value.template = Template()
    assert value.evaluate(1, proposal)["status"] == "rejected"
    assert not (tmp_path / "experiments").exists()


@native
@pytest.mark.parametrize("name,expression", [
    ("relative_volatility", "Std($close, 3) / Mean($close, 3)"),
    ("range_position", "($close - Min($close, 3)) / (Max($close, 3) - Min($close, 3))"),
])
def test_new_price_direction_enters_native_experiment(name, expression):
    value = knowledge_context()
    decision = dict(direction(), direction=name)
    value["direction"] = select_direction(value, FixedRequest([decision]))
    experiment = factor(expression)
    next(iter(experiment.values()))["knowledge_refs"] = decision["knowledge_refs"]
    complete = FixedRequest([hypothesis(), experiment])
    actual = propose_factor(value, complete)
    assert actual["expression"] == expression
    assert actual["knowledge_refs"] == decision["knowledge_refs"]
    assert len(complete.calls) == 2
