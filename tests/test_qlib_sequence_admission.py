"""公开研究包序列参数在执行前的准入。"""
from __future__ import annotations

import json
from dataclasses import replace

import pytest

from research_pipeline.packages import ResearchPackageError
from research_pipeline.packages.operator_graph import _validate_operator_graph_special_contracts
from research_pipeline.platform.operator_contracts import InputBinding, OperatorGraphRecipe, OperatorNodeRecipe
from research_pipeline.runtime.operator_families.daily_model import build_daily_model_operator_definitions
from test_qlib_gru_contract import gru_candidate


def _table_candidate():
    return {"candidate_id": "ridge", "model": {"class": "LinearModel",
        "module_path": "qlib.contrib.model.linear", "kwargs": {"estimator": "ridge", "alpha": 0.1}},
        "processors": {"infer": [], "learn": [{"class": "DropnaLabel", "kwargs": {}}]}, "fit": {}}


def _recipe(*, scope="final", step=3, fit=None, holdout=None):
    split_parameters = {"evaluation_scope": scope, "holdout_start": "2024-01-05",
        "calendar_sessions": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]}
    if step is not None:
        split_parameters["sequence_step_len"] = step
    nodes = [OperatorNodeRecipe("split", "research.model.split-manifest", "2.0.0", (), split_parameters, ())]
    declarations = [("fit", "research.model.fit", fit)]
    if scope == "final":
        declarations.append(("holdout", "research.model.locked-holdout", holdout))
    for name, operator, candidates in declarations:
        if candidates is None:
            candidates = [gru_candidate(), _table_candidate()]
        nodes.append(OperatorNodeRecipe(name, operator, "2.0.0", (InputBinding("splits", "split", "splits"),),
            {"thread_count": 1, "candidate_jsons": [json.dumps(item) for item in candidates]}, ()))
    return OperatorGraphRecipe.build(graph_id="sequence-admission", nodes=tuple(nodes))


def _check(recipe):
    _validate_operator_graph_special_contracts(recipe)


@pytest.mark.parametrize("scope", ["development", "final"])
def test_sequence_package_accepts_gru_and_table_candidates(scope):
    _check(_recipe(scope=scope))


@pytest.mark.parametrize("scope", ["development", "final"])
def test_table_package_keeps_default_no_sequence(scope):
    _check(_recipe(scope=scope, step=None, fit=[_table_candidate()], holdout=[_table_candidate()]))


@pytest.mark.parametrize("step", [True, False, 0, 1, -1, 3.0, "3"])
def test_split_rejects_invalid_sequence_length(step):
    with pytest.raises(ResearchPackageError, match="sequence_step_len"):
        _check(_recipe(step=step))


@pytest.mark.parametrize("scope", ["development", "final"])
def test_gru_requires_explicit_split_window(scope):
    with pytest.raises(ResearchPackageError, match="匹配split冻结"):
        _check(_recipe(scope=scope, step=None))


@pytest.mark.parametrize("node", ["fit", "holdout"])
def test_all_gru_candidates_must_match_split(node):
    candidates = [gru_candidate(), gru_candidate(**{"candidate_id": "other-gru", "dataset.step_len": 4})]
    with pytest.raises(ResearchPackageError, match="匹配split冻结"):
        _check(_recipe(**{node: candidates}))


@pytest.mark.parametrize("node", ["fit", "holdout"])
def test_sequence_node_must_include_gru(node):
    with pytest.raises(ResearchPackageError, match="至少需要一个"):
        _check(_recipe(**{node: [_table_candidate()]}))


@pytest.mark.parametrize("node", ["fit", "holdout"])
def test_sequence_evaluation_rejects_rank_labels(node):
    candidates = [gru_candidate(), _table_candidate()]
    for candidate in candidates:
        candidate["processors"]["learn"].append({"class": "CSRankNorm", "kwargs": {"fields_group": "label"}})
    with pytest.raises(ResearchPackageError, match="raw标签"):
        _check(_recipe(**{node: candidates}))


def test_table_rank_label_contract_is_unchanged():
    table = _table_candidate()
    table["processors"]["learn"].append({"class": "CSRankNorm", "kwargs": {"fields_group": "label"}})
    _check(_recipe(step=None, fit=[table], holdout=[table]))


@pytest.mark.parametrize("binding", [None, InputBinding("splits", "unknown", "splits"), InputBinding("splits", "split", "other")])
def test_gru_must_reference_the_actual_split_output(binding):
    recipe = _recipe(scope="development")
    nodes = tuple(replace(node, inputs=() if binding is None else (binding,)) if node.node_id == "fit" else node for node in recipe.nodes)
    with pytest.raises(ResearchPackageError, match="匹配split冻结"):
        _check(OperatorGraphRecipe.build(graph_id=recipe.graph_id, nodes=nodes))


def test_sequence_split_without_any_model_is_rejected():
    recipe = _recipe(scope="development")
    nodes = tuple(node for node in recipe.nodes if node.node_id == "split")
    with pytest.raises(ResearchPackageError, match="至少一个序列模型"):
        _check(OperatorGraphRecipe.build(graph_id=recipe.graph_id, nodes=nodes))


def test_second_split_does_not_satisfy_gru_window_requirement():
    recipe = _recipe(scope="development", step=None)
    other = OperatorNodeRecipe("other-split", "research.model.split-manifest", "2.0.0", (), {"sequence_step_len": 3}, ())
    with pytest.raises(ResearchPackageError, match="匹配split冻结"):
        _check(OperatorGraphRecipe.build(graph_id=recipe.graph_id, nodes=(*recipe.nodes, other)))


def test_split_parameter_is_registered_as_optional_integer():
    definitions = build_daily_model_operator_definitions()
    split = next(item.operator_spec for item in definitions if item.operator_spec.operator_id == "research.model.split-manifest")
    parameter = next(item for item in split.parameters if item.name == "sequence_step_len")
    assert parameter.required is False
    assert parameter.validate(3) == 3


def test_candidate_thread_contract_remains_in_force():
    recipe = _recipe(scope="development")
    nodes = tuple(replace(node, parameters={**node.parameters, "thread_count": 2}) if node.node_id == "fit" else node for node in recipe.nodes)
    with pytest.raises(ResearchPackageError, match="thread_count"):
        _check(OperatorGraphRecipe.build(graph_id=recipe.graph_id, nodes=nodes))


@pytest.mark.parametrize("edit, message", [("missing_dataset", "dataset"), ("old_dataset", "TSDatasetH"), ("missing_policy", "complete_window")])
def test_gru_rejects_missing_or_obsolete_dataset_contract(edit, message):
    candidate = gru_candidate()
    if edit == "missing_dataset":
        del candidate["dataset"]
    elif edit == "old_dataset":
        candidate["dataset"]["class"] = "DatasetH"
    else:
        candidate["dataset"]["missing_policy"] = "fill"
    with pytest.raises(ResearchPackageError, match=message):
        _check(_recipe(scope="development", fit=[candidate]))
