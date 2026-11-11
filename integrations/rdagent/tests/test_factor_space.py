"""不同项目声明共用因子、联合、知识与恢复入口。"""
from copy import deepcopy
import pytest

from quantwitness_rdagent.factor_research import (
    FactorResearch, canonical_expression, expression_direction, factor_directions,
    validate_factor_space, _freeze,
)
from quantwitness_rdagent.joint_feedback import development_population
from quantwitness_rdagent.joint_research import JointModelResearch
from quantwitness_rdagent.native_model_research import _context as model_context
from quantwitness_rdagent.native_research import _context as factor_context
from quantwitness_rdagent import research_knowledge as knowledge
from quantwitness_rdagent.session_inspection import check_frozen_request


@pytest.fixture(params=[("signal_a", "feature_a", 3), ("signal_b", "feature_b", 7)])
def space(request):
    field, slot, window = request.param
    return {"space_id": field + "-study", "fields": [field], "max_window": window,
        "expression_contract": "使用前一完整会话及更早输入，公式由项目算子解释。",
        "feature_slot": slot, "population_feature_columns": [slot + "__value"],
        "candidates": [{"expression": "$" + field, "direction": "baseline"},
            {"expression": f"Ref(${field}, {window})", "direction": "lag"}]}


def test_explicit_candidate_context_and_direction(space):
    assert validate_factor_space(space) == space
    expression = space["candidates"][1]["expression"]
    assert canonical_expression(expression.replace(" ", ""), space=space) == expression
    assert expression_direction(expression, space=space) == "lag"
    assert factor_directions(space) == ("baseline", "lag")
    context = {"factor_space": space, "confirmed_spec": "确认的项目定义", "history": [],
        **{key: space[key] for key in ("fields", "max_window", "expression_contract")}}
    assert factor_context(context)["factor_space"] == space
    with pytest.raises(ValueError, match="冻结研究空间"):
        canonical_expression("Ref($other, 1)", space=space)
    context["fields"] = ["unbound"]
    with pytest.raises(ValueError, match="上下文"):
        factor_context(context)


def test_candidate_changes_only_declared_slot_and_bound_identity(space, tmp_path):
    session = object.__new__(FactorResearch)
    session.design = {"factor_space": space, "feature_expressions": {
        space["feature_slot"]: space["candidates"][0]["expression"], "fixed_feature": "$fixed"}}
    session.payload = {"factor_space": space, "campaign_id": "study", "package_template": {}}
    session.root = tmp_path
    expression = space["candidates"][1]["expression"]
    payload = session._candidate_payload({"candidate_id": "candidate", "expression": expression})
    overrides = payload["candidates"][0]["parameter_overrides"]
    designs = [item["value"] for item in overrides if item["parameter_name"] == "design"]
    assert len(designs) == 3 and designs[0] == designs[1] == designs[2]
    assert designs[0]["feature_expressions"] == {space["feature_slot"]: expression, "fixed_feature": "$fixed"}
    identities = [item["value"] for item in overrides if item["parameter_name"] in {"lineage_ref", "research_identity_hash"}]
    assert len(set(identities)) == 1
    assert session.design["feature_expressions"][space["feature_slot"]] != expression


def test_joint_model_context_binds_project_slot(space, monkeypatch):
    from quantwitness_rdagent.model_research import ModelResearch
    monkeypatch.setattr(ModelResearch, "context", lambda self: {
        "confirmed_spec": "项目模型定义", "history": [], "max_output_tokens": 512})
    model = object.__new__(JointModelResearch)
    model.design = {"factor_space": space}
    context = model.context()
    assert model_context(context)["factor_feature_slot"] == space["feature_slot"]
    context["factor_feature_slot"] = "other"
    with pytest.raises(ValueError, match="因子槽"):
        model_context(context)


def test_population_uses_explicit_columns_and_keeps_label_facts(space):
    design = {"mode": "development", "factor_space": space}
    column = space["population_feature_columns"][0]
    tables = {"samples": [{"sample_id": "one", "target": 0.1, "label_start_time": "t1",
        "label_end_time": "t2", "label_available_time": "t3", column: 0.0, "unrelated": None}],
        "validation_predictions": [{"fold_id": "fold", "sample_id": "one", "actual": 0.1,
            "label_end_time": "t2", "label_available_time": "t3"}]}
    population = development_population(design, tables)
    assert population["samples"][0]["label_available_time"] == "t3"
    changed = deepcopy(tables)
    changed["samples"][0][column] = None
    with pytest.raises(ValueError, match="完整值"):
        development_population(design, changed)
    changed = deepcopy(tables)
    changed["validation_predictions"][0]["label_available_time"] = "t4"
    assert development_population(design, changed) != population


def test_request_recovery_and_knowledge_match_complete_space(space, tmp_path):
    payload = {"session_root": str(tmp_path), "factor_space": space}
    _freeze(tmp_path / "request.json", payload)
    check_frozen_request(payload)
    changed = deepcopy(payload)
    changed["factor_space"]["max_window"] += 1
    with pytest.raises(ValueError, match="factor_space.max_window"):
        check_frozen_request(changed)
    development = {"start": "2025-01-01", "end": "2025-01-02", "as_of": "2025-01-03T00:00:00+08:00"}
    design = {"factor_space": space, "mode": "development", "calendar_id": "calendar",
        "snapshot_scope": "synthetic", "price_basis": "raw", "entities": ["asset"], "research_sessions": ["2025-01-02"]}
    scope = knowledge._scope(design, development)
    row = {"usable_for_proposal": True, "objective": {}, "metric_description": {},
        "development_scope": development, "as_of": development["as_of"], "design_scope": scope}
    assert knowledge._compatible(row, development, {}, {}, scope)
    other = {**scope, "factor_space": changed["factor_space"]}
    assert not knowledge._compatible(row, development, {}, {}, other)


@pytest.mark.parametrize("change", [
    {"max_window": True}, {"fields": ["a", "a"]}, {"population_feature_columns": []},
    {"candidates": [{"expression": "$a", "direction": "stop"}]},
    {"candidates": [{"expression": "$a", "direction": "x"}, {"expression": " $a ", "direction": "y"}]},
])
def test_invalid_space_is_rejected(space, change):
    with pytest.raises(ValueError):
        validate_factor_space({**space, **change})


def test_joint_schedule_is_explicit_and_not_limited_to_three_rounds(space, monkeypatch):
    from quantwitness_rdagent import joint_research as joint
    template = {"source": {}, "development": {}, "objective": {}}
    branch = {"factor_space": space, "package_template": template, "proposer": {"mode": "fixed_responses"}}
    monkeypatch.setattr(joint, "validate_factor_research", lambda value: value)
    monkeypatch.setattr(joint, "validate_model_research", lambda value: value)
    payload = {"contract_version": "rd-joint-research-v2", "research_kind": "joint_research",
        "campaign_id": "study", "session_root": "new-session", "factor_request": branch, "model_request": branch,
        "proposer": branch["proposer"], "research_schedule": ["baseline", "factor", "factor", "model"],
        "budget": {"rounds": 4, "evaluations": 4, "model_calls": 12, "output_tokens": 8192, "max_output_tokens_per_call": 512}}
    assert joint.validate_joint_research(payload)["research_schedule"] == payload["research_schedule"]
    for schedule in (["baseline", "factor", "model"], ["model", "factor", "factor", "model"], ["baseline", {}, "factor", "model"]):
        with pytest.raises(ValueError, match="研究计划"):
            joint.validate_joint_research({**payload, "research_schedule": schedule})
    with pytest.raises(ValueError, match="联合研究合同"):
        joint.validate_joint_research({**payload, "contract_version": "rd-joint-research-v1"})


def test_request_baseline_must_belong_to_frozen_space(space, monkeypatch):
    from quantwitness_rdagent import factor_research as factor
    monkeypatch.setattr(factor, "validate_package_campaign", lambda value: value)
    payload = {"contract_version": "rd-factor-research-v2", "research_kind": "factor_research", "campaign_id": "study",
        "session_root": "new-session", "factor_space": space,
        "package_template": {"proposer": {"mode": "fixed_policy"}, "candidates": [{}]},
        "confirmed_spec": {"kind": "synthetic", "path": "definition.json"},
        "baseline": {"expression": space["candidates"][0]["expression"], "hypothesis": "基线", "reason": "确认定义"},
        "budget": {"rounds": 2, "model_calls": 3, "output_tokens": 1536, "max_output_tokens_per_call": 512},
        "proposer": {"mode": "fixed_responses", "responses": ["response.json"]}}
    assert factor.validate_factor_research(payload)["factor_space"] == space
    with pytest.raises(ValueError, match="冻结研究空间"):
        factor.validate_factor_research({**payload, "baseline": {**payload["baseline"], "expression": "$other"}})
    with pytest.raises(ValueError, match="不支持的因子研究合同"):
        factor.validate_factor_research({**payload, "contract_version": "rd-factor-research-v1"})
