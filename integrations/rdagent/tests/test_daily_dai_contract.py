"""已确认日级因子的字段、时点资格、方向、模型绑定与恢复合同。"""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from quantwitness_rdagent.contracts import write_json
from quantwitness_rdagent.factor_research import (
    DAI_FACTOR_CONTRACT, DAI_EXPRESSION_CONTRACT, FactorResearch, ResearchBudgetExhausted,
    ResearchCalls, canonical_expression, expression_direction, factor_contract,
    validate_factor_research,
)
from quantwitness_rdagent.joint_feedback import development_population


def daily_record():
    return {"record_id": "daily:0000", "kind": "factor", "candidate_id": "factor_0000",
            "status": "evaluated", "expression": "$dai", "hypothesis": "已确认日级值",
            "reason": "基线独立验证", "metrics": {"value": 0.5, "rows": 1}}


def daily_session(tmp_path):
    session = object.__new__(FactorResearch)
    session.root = tmp_path
    session.design = {"factor_contract": DAI_FACTOR_CONTRACT,
                      "feature_expressions": {"dai_following": "$dai"}}
    session.materials = {"text": "已确认日级定义"}
    session.metric_description = {"metric_id": "validation_mse", "unit": "squared_decimal_price_change"}
    session.payload = {"factor_contract": DAI_FACTOR_CONTRACT, "campaign_id": "daily",
        "budget": {"rounds": 3}, "package_template": {"development": {}, "objective": {"direction": "minimize"}}}
    return session


@pytest.mark.parametrize("expression,direction", [("$dai", "baseline")]
    + [(f"Ref($dai, {n})", "lag") for n in range(1, 6)]
    + [(f"Mean($dai, {n})", "mean") for n in range(2, 6)])
def test_supported_daily_definitions(expression, direction):
    compact = " " + expression.replace(" ", "") + " "
    assert canonical_expression(compact, contract=DAI_FACTOR_CONTRACT) == expression
    assert expression_direction(expression, contract=DAI_FACTOR_CONTRACT) == direction


@pytest.mark.parametrize("expression", ["Ref($dai, -1)", "Ref($dai, 0)", "Ref($dai, 6)",
    "Mean($dai, 1)", "Mean($dai, 6)", "Std($dai, 2)", "$dai + 1", "Ref($close, 2)",
    "$close / Ref($close, 2) - 1", "Mean(Ref($dai, 1), 2)"])
def test_daily_contract_rejects_future_or_unapproved_formula(expression):
    with pytest.raises(ValueError):
        canonical_expression(expression, contract=DAI_FACTOR_CONTRACT)


def test_daily_contract_requires_explicit_request_and_original_baseline(monkeypatch):
    import quantwitness_rdagent.factor_research as module
    monkeypatch.setattr(module, "validate_package_campaign", lambda value: deepcopy(value))
    request = {"contract_version": "rd-factor-research-v1", "research_kind": "factor_research",
        "campaign_id": "daily", "session_root": "session", "factor_contract": DAI_FACTOR_CONTRACT,
        "package_template": {"proposer": {"mode": "fixed_policy"}, "candidates": [{"id": "baseline"}]},
        "confirmed_spec": {"kind": "synthetic", "path": "definition.json"},
        "baseline": {"expression": "$dai", "hypothesis": "日级基线", "reason": "已确认"},
        "budget": {"rounds": 3, "model_calls": 12, "output_tokens": 98304, "max_output_tokens_per_call": 8192},
        "proposer": {"mode": "fixed_responses", "responses": ["response.txt"]}}
    assert validate_factor_research(request)["baseline"]["expression"] == "$dai"
    without_contract = deepcopy(request)
    without_contract.pop("factor_contract")
    with pytest.raises(ValueError):
        validate_factor_research(without_contract)
    derived_baseline = deepcopy(request)
    derived_baseline["baseline"]["expression"] = "Ref($dai, 1)"
    with pytest.raises(ValueError, match="基线"):
        validate_factor_research(derived_baseline)
    with pytest.raises(ValueError):
        factor_contract({"factor_contract": "unconfirmed"})
    with pytest.raises(ValueError):
        canonical_expression("$dai")


def test_daily_context_projects_only_development_and_declares_contract(tmp_path):
    session = daily_session(tmp_path)
    write_json(tmp_path / "rounds/0000/record.json", {"candidate_id": "f0", "status": "failed",
        "expression": "$dai", "hypothesis": "基线", "reason": "失败", "metrics": {"holdout": 999}})
    write_json(tmp_path / "rounds/0001/record.json", {"candidate_id": "f1", "status": "evaluated",
        "expression": "Ref($dai, 2)", "hypothesis": "滞后", "reason": "开发",
        "metrics": {"value": 0.2, "rows": 1, "result_ref": "PRIVATE_RESULT", "test": 999}})
    context = session.context()
    assert context["fields"] == ["dai"] and context["factor_contract"] == DAI_FACTOR_CONTRACT
    assert context["expression_contract"] == DAI_EXPRESSION_CONTRACT
    assert "metrics" not in context["history"][0]
    assert context["history"][1]["metrics"] == {"value": 0.2, "rows": 1}
    assert "PRIVATE" not in json.dumps(context) and "holdout" not in json.dumps(context)


def test_daily_factor_and_model_bind_the_same_slot(tmp_path, monkeypatch):
    from quantwitness_rdagent.joint_research import JointModelResearch
    from quantwitness_rdagent.model_research import ModelResearch
    session = daily_session(tmp_path)
    factor_payload = session._candidate_payload({"candidate_id": "f1", "expression": " Ref($dai,2) "})
    overrides = factor_payload["candidates"][0]["parameter_overrides"]
    designs = [row["value"] for row in overrides if row["parameter_name"] == "design"]
    assert all(row["feature_expressions"] == {"dai_following": "Ref($dai, 2)"} for row in designs)
    model_base = {"candidates": [{"parameter_overrides": [
        {"node_id": "feature", "parameter_name": "design", "value": deepcopy(session.design)},
        {"node_id": "label", "parameter_name": "design", "value": deepcopy(session.design)},
        {"node_id": "feature", "parameter_name": "lineage_ref", "value": "old"},
        {"node_id": "model_fit", "parameter_name": "research_identity_hash", "value": "old"}]}]}
    monkeypatch.setattr(ModelResearch, "_candidate_payload", lambda self, proposal: deepcopy(model_base))
    model = object.__new__(JointModelResearch)
    payload = model._candidate_payload({"factor_expression": "Mean($dai, 3)"})
    model_overrides = payload["candidates"][0]["parameter_overrides"]
    assert all(row["value"]["feature_expressions"] == {"dai_following": "Mean($dai, 3)"}
               for row in model_overrides if row["parameter_name"] == "design")
    assert len({row["value"] for row in model_overrides if row["parameter_name"] in {"lineage_ref", "research_identity_hash"}}) == 1


def daily_population():
    return {"mode": "walk_forward_development_v1", "design": {"factor_contract": DAI_FACTOR_CONTRACT},
        "tables": {"samples": [{"sample_id": "s1", "target": 0.1, "dai_following__w25": 0.0,
            "label_start_time": "d", "label_end_time": "d1", "label_available_time": "d2"}],
        "validation_predictions": [{"fold_id": "f1", "sample_id": "s1", "actual": 0.1,
            "label_end_time": "d1", "label_available_time": "d2"}]}}


def test_daily_zero_is_valid_and_missing_or_changed_maturity_is_not_common():
    model = daily_population()
    baseline = development_population(model)
    missing = deepcopy(model)
    missing["tables"]["samples"][0]["dai_following__w25"] = None
    with pytest.raises(ValueError, match="完整值"):
        development_population(missing)
    changed = deepcopy(model)
    changed["tables"]["validation_predictions"][0]["label_available_time"] = "future"
    assert development_population(changed) != baseline
    holdout = deepcopy(model)
    holdout["tables"]["holdout_predictions"] = []
    with pytest.raises(ValueError, match="留出"):
        development_population(holdout)
    wrong_slot = deepcopy(model)
    wrong_slot["tables"]["samples"][0].pop("dai_following__w25")
    wrong_slot["tables"]["samples"][0]["historical_return__w5"] = 0.0
    with pytest.raises(ValueError, match="完整值"):
        development_population(wrong_slot)


@pytest.mark.parametrize("direction", ["lag", "mean"])
def test_daily_direction_is_scoped_and_uses_verified_refs(direction):
    from quantwitness_rdagent.joint_research import select_joint_direction
    from quantwitness_rdagent.direction_selection import select_direction
    calls = []
    def complete(prompt, **kwargs):
        calls.append({"prompt": prompt, **kwargs})
        return json.dumps({"action": "factor", "direction": direction, "knowledge_refs": ["daily:0000"], "reason": "开发依据"})
    context = {"factor_contract": DAI_FACTOR_CONTRACT, "knowledge": [daily_record()],
               "history": [], "objective": {}, "development": {}, "max_output_tokens": 512}
    assert select_joint_direction(context, complete)["direction"] == direction
    assert select_direction(context, complete)["direction"] == direction
    assert all("$dai" in call["instructions"] for call in calls)
    price_context = {key: value for key, value in context.items() if key != "factor_contract"}
    with pytest.raises(ValueError):
        select_joint_direction(price_context, complete)


def test_daily_native_factor_and_model_prompts_keep_contract_and_parent():
    from quantwitness_rdagent.native_research import propose_factor
    from quantwitness_rdagent.native_model_research import propose_model
    factor_replies = iter([json.dumps({"hypothesis": "历史滞后", "reason": "已验证日级值"}), json.dumps({"Lag": {
        "description": "日级滞后", "formulation": "Ref($dai, 2)", "variables": {"$dai": "已确认日级值"},
        "expression": "Ref($dai, 2)", "knowledge_refs": ["daily:0000"]}})])
    prompts = []
    def factor_complete(prompt, **kwargs):
        prompts.append({"prompt": prompt, **kwargs})
        return next(factor_replies)
    context = {"confirmed_spec": "日级定义", "fields": ["dai"], "factor_contract": DAI_FACTOR_CONTRACT,
        "max_window": 5, "expression_contract": DAI_EXPRESSION_CONTRACT, "history": [],
        "max_output_tokens": 512, "knowledge": [daily_record()],
        "direction": {"direction": "lag", "knowledge_refs": ["daily:0000"]}}
    assert propose_factor(context, factor_complete)["expression"] == "Ref($dai, 2)"
    assert all("dai-daily-v1" in call["prompt"] + call["instructions"] for call in prompts)
    model_replies = iter([json.dumps({"hypothesis": "非线性", "reason": "日级因子开发反馈"}),
        json.dumps({"definition": {"nodes": [{"inputs": [-1], "width": 1, "activation": "identity"}]}})])
    model_prompts = []
    def model_complete(prompt, **kwargs):
        model_prompts.append({"prompt": prompt, **kwargs})
        return next(model_replies)
    propose_model({"confirmed_spec": "日级模型", "development": {}, "objective": {}, "history": [],
        "max_output_tokens": 512, "factor_contract": DAI_FACTOR_CONTRACT, "factor_feature_slot": "dai_following",
        "joint_knowledge": [daily_record()], "joint_direction": {"action": "model", "direction": "generated_model"}}, model_complete)
    assert all("dai_following" in call["prompt"] + call["instructions"]
               and "daily:0000" in call["prompt"] + call["instructions"] for call in model_prompts)


def test_daily_model_selects_best_factor_and_proposal_resume_reuses(tmp_path, monkeypatch):
    import quantwitness_rdagent.joint_research as module
    session = object.__new__(module.JointResearch)
    session.root = tmp_path
    session.payload = {"factor_request": {"factor_contract": DAI_FACTOR_CONTRACT,
        "package_template": {"development": {}, "objective": {"direction": "minimize"}}},
        "budget": {"evaluations": 3, "max_output_tokens_per_call": 512}}
    for index, expression, loss in [(0, "$dai", 0.5), (1, "Ref($dai, 2)", 0.2)]:
        row = daily_record()
        row.update(round=index, record_id=f"daily:{index:04d}", candidate_id=f"factor_{index:04d}", expression=expression)
        row["metrics"] = {"value": loss, "rows": 1, "verification_status": "pass"}
        write_json(tmp_path / "rounds" / f"{index:04d}" / "record.json", row)
    calls = []
    session.branches = {"factor": SimpleNamespace(metric_description={}), "model": SimpleNamespace(
        propose=lambda index: calls.append(index) or {"candidate_id": "model_0002", "definition": {"nodes": []}})}
    monkeypatch.setattr(module, "select_joint_direction", lambda context, complete: {
        "action": "model", "direction": "generated_model", "knowledge_refs": ["daily:0001"], "reason": "开发依据"})
    proposal = session.propose(2)
    path = tmp_path / "rounds/0002/proposal.json"
    before = path.read_bytes(), path.stat().st_mtime_ns
    assert proposal["factor_record_id"] == "daily:0001" and proposal["factor_expression"] == "Ref($dai, 2)"
    assert session.propose(2) == proposal and calls == [2]
    assert before == (path.read_bytes(), path.stat().st_mtime_ns)


def test_daily_calls_share_budget_and_resume_does_not_spend_again(tmp_path):
    owner = SimpleNamespace(root=tmp_path, payload={"budget": {"model_calls": 2, "output_tokens": 1024,
        "max_output_tokens_per_call": 512}, "proposer": {"mode": "fixed_responses"}}, materials={"responses": ["factor", "model"]})
    factor = SimpleNamespace(call_owner=owner, call_prefix="factor-")
    model = SimpleNamespace(call_owner=owner, call_prefix="model-")
    assert ResearchCalls(factor, "daily-proposal")("$dai", instructions=DAI_EXPRESSION_CONTRACT) == "factor"
    assert ResearchCalls(model, "daily-proposal")("dai_following", instructions=DAI_EXPRESSION_CONTRACT) == "model"
    before = [(path.read_bytes(), path.stat().st_mtime_ns) for path in sorted((tmp_path / "calls").glob("*.json"))]
    assert ResearchCalls(factor, "daily-proposal")("$dai", instructions=DAI_EXPRESSION_CONTRACT) == "factor"
    assert before == [(path.read_bytes(), path.stat().st_mtime_ns) for path in sorted((tmp_path / "calls").glob("*.json"))]
    with pytest.raises(ResearchBudgetExhausted):
        ResearchCalls(model, "daily-reflection")("new", instructions=DAI_EXPRESSION_CONTRACT)


def test_daily_knowledge_does_not_mix_price_contract():
    from quantwitness_rdagent.research_knowledge import _compatible, _scope
    development = {"start": "2020-01-01", "end": "2020-01-10", "as_of": "2020-01-10T16:00:00+08:00"}
    base = {"mode": "development", "calendar_id": "calendar", "snapshot_scope": "synthetic", "price_basis": "raw",
            "entities": ["S"], "research_sessions": ["2020-01-02"]}
    daily = {**base, "factor_contract": DAI_FACTOR_CONTRACT}
    daily_scope = _scope(daily, development)
    price_scope = _scope(base, development)
    row = {"usable_for_proposal": True, "objective": {}, "metric_description": {},
        "development_scope": development, "as_of": development["as_of"], "design_scope": daily_scope}
    assert _compatible(row, development, {}, {}, daily_scope)
    assert not _compatible(row, development, {}, {}, price_scope)
    row["design_scope"] = price_scope
    assert not _compatible(row, development, {}, {}, daily_scope)


def test_fixed_schedule_is_explicit_and_validated(monkeypatch):
    import quantwitness_rdagent.joint_research as module
    proposer = {"mode": "fixed_responses", "responses": ["response.txt"]}
    branch = {"package_template": {"source": {}, "development": {}, "objective": {}}, "proposer": proposer}
    monkeypatch.setattr(module, "validate_factor_research", lambda value: value)
    monkeypatch.setattr(module, "validate_model_research", lambda value: value)
    request = {"contract_version": "rd-joint-research-v1", "research_kind": "joint_research", "campaign_id": "daily",
        "session_root": "session", "factor_request": deepcopy(branch), "model_request": deepcopy(branch), "proposer": proposer,
        "budget": {"rounds": 3, "evaluations": 3, "model_calls": 12, "output_tokens": 98304, "max_output_tokens_per_call": 8192}}
    assert "research_schedule" not in module.validate_joint_research(request)
    request["research_schedule"] = ["baseline", "factor", "model"]
    assert module.validate_joint_research(request)["research_schedule"] == request["research_schedule"]
    wrong = deepcopy(request)
    wrong["research_schedule"] = ["baseline", "model", "factor"]
    with pytest.raises(ValueError, match="计划"):
        module.validate_joint_research(wrong)


@pytest.mark.parametrize("scheduled,returned,direction", [("factor", "model", "generated_model"), ("model", "factor", "lag")])
def test_scheduled_direction_rejects_another_branch_without_rewriting(scheduled, returned, direction):
    from quantwitness_rdagent.joint_research import select_joint_direction
    response = {"action": returned, "direction": direction, "knowledge_refs": ["daily:0000"], "reason": "开发依据"}
    def complete(prompt, **kwargs):
        assert f"action只能为{scheduled}或stop" in kwargs["instructions"]
        return json.dumps(response)
    with pytest.raises(ValueError, match="方向"):
        select_joint_direction({"factor_contract": DAI_FACTOR_CONTRACT, "scheduled_action": scheduled,
            "knowledge": [daily_record()], "max_output_tokens": 512}, complete)
    assert response["action"] == returned


def test_fixed_schedule_resume_and_directions_share_original_budget(tmp_path):
    from quantwitness_rdagent.factor_research import _freeze
    from quantwitness_rdagent.joint_research import JointResearch
    session = object.__new__(JointResearch)
    session.root = tmp_path
    session.payload = {"campaign_id": "daily", "factor_request": {"factor_contract": DAI_FACTOR_CONTRACT,
        "package_template": {"development": {}, "objective": {"direction": "minimize"}}},
        "research_schedule": ["baseline", "factor", "model"],
        "proposer": {"mode": "fixed_responses"},
        "budget": {"rounds": 3, "evaluations": 3, "model_calls": 2, "output_tokens": 1024, "max_output_tokens_per_call": 512}}
    session.materials = {"responses": [json.dumps({"action": "factor", "direction": "lag", "knowledge_refs": ["daily:0000"], "reason": "开发依据"}),
        json.dumps({"action": "model", "direction": "generated_model", "knowledge_refs": ["daily:0001"], "reason": "开发依据"})]}
    proposed = []
    session.branches = {"factor": SimpleNamespace(metric_description={}, propose=lambda index: proposed.append(index) or {
        "candidate_id": "factor_0001", "expression": "Ref($dai, 2)"}),
        "model": SimpleNamespace(propose=lambda index: proposed.append(index) or {"candidate_id": "model_0002", "definition": {"nodes": []}})}
    baseline = daily_record()
    baseline["metrics"]["verification_status"] = "pass"
    write_json(tmp_path / "rounds/0000/record.json", baseline)
    _freeze(tmp_path / "request.json", session.payload)
    factor = session.propose(1)
    assert factor["kind"] == "factor" and factor["direction_choice"]["direction"] == "lag"
    assert session.propose(1) == factor and proposed == [1]
    factor_record = {**baseline, **factor, "record_id": "daily:0001", "status": "evaluated",
                     "metrics": {"value": 0.2, "rows": 1, "verification_status": "pass"}}
    write_json(tmp_path / "rounds/0001/record.json", factor_record)
    model = session.propose(2)
    assert model["kind"] == "model" and model["factor_record_id"] == "daily:0001"
    files = sorted((tmp_path / "calls").glob("*.json"))
    before = [(path.read_bytes(), path.stat().st_mtime_ns) for path in files]
    assert session.propose(2) == model and proposed == [1, 2]
    assert before == [(path.read_bytes(), path.stat().st_mtime_ns) for path in files]
    assert len(files) == 2
    with pytest.raises(ResearchBudgetExhausted):
        ResearchCalls(session, "repair")("new", instructions="日级计划")
    changed = deepcopy(session.payload)
    changed["research_schedule"] = ["baseline", "model", "factor"]
    with pytest.raises(ValueError, match="冻结"):
        _freeze(tmp_path / "request.json", changed)
