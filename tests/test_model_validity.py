"""模型有效性使用内存事实验证，覆盖时间泄漏、选模和一次性 holdout。"""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pyarrow as pa
import pytest

from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.model_validity import (
    _holdout, _selection, _split, _statistics, recompute_model_validity_issues,
    verify_model_result_binding,
)
from research_pipeline.platform import typed_canonical_hash as digest


def stamp(day, hour=9):
    return datetime(2020, 1, 1, hour, 30, tzinfo=timezone.utc).replace(day=day).isoformat()


def sample(day):
    return dict(sample_id=f"s{day}", entity_id="A", observation_session=f"2020-01-{day:02d}",
                observation_time=stamp(day, 8), feature_available_time=stamp(day, 8), decision_time=stamp(day),
                label_start_time=stamp(day, 10), label_end_time=stamp(day, 15),
                label_available_time=stamp(day + 1, 8), target=0.0)


def prediction(row, fold, candidate, stage, value):
    return {**{key: value for key, value in row.items() if key != "target"},
            "fold_id": fold, "candidate_id": candidate, "stage": stage,
            "horizon_sessions": 1, "actual": row["target"], "raw_label": row["target"],
            "evaluation_label": row["target"], "prediction": value,
            "score_semantics": "raw_return_prediction"}


def model_fixture():
    candidates = {"a": {}, "b": {}}
    rows = [sample(day) for day in range(1, 13)]
    design = dict(holdout_start=stamp(20), split_calendar_sessions=[row["observation_session"] for row in rows],
                  train_sessions=3, validation_sessions=2, test_sessions=2, step_sessions=2,
                  embargo_sessions=1, expanding=True, candidate_ids=list(candidates))
    tables = dict(samples=rows, split_audit=[], fit_audit=[], validation_predictions=[],
                  test_predictions=[], fold_selections=[], selection=[])
    groups = [([1, 2, 3], [4, 5], [6], [7, 8]),
              ([1, 2, 3, 4, 5], [6, 7], [8], [9, 10]),
              ([1, 2, 3, 4, 5, 6, 7], [8, 9], [10], [11, 12])]
    for number, (train, valid, embargo, test) in enumerate(groups, 1):
        fid = f"walk_forward_{number:03d}"
        for role, days in (("train", train), ("validation", valid), ("embargoed", embargo), ("test", test)):
            for day in days:
                tables["split_audit"].append(dict(fold_id=fid, sample_id=f"s{day}", role=role,
                    exclusion_reason=role if role == "embargoed" else None,
                    label_start=stamp(day, 10), label_end=stamp(day, 15)))
        for cid in candidates:
            tables["fit_audit"].append(dict(fold_id=fid, candidate_id=cid,
                train_ids_json=json.dumps([f"s{day}" for day in train]),
                validation_ids_json=json.dumps([f"s{day}" for day in valid]),
                evidence_scope="processor_train_sample_scope"))
            for day in valid:
                tables["validation_predictions"].append(prediction(rows[day-1], fid, cid, "validation", 0.0 if cid == "a" else 1.0))
        for day in test:
            tables["test_predictions"].append(prediction(rows[day-1], fid, "a", "test", 0.0))
        tables["fold_selections"].append(dict(fold_id=fid, selected_candidate_id="a", validation_metric=0.0,
            selection_time=stamp(test[0]), validation_fold_count=number))
    tables["selection"] = [dict(selected_candidate_id="a", validation_metric=0.0, test_metric=0.0,
        objective="neg_mean_squared_error", direction="maximize", selection_scope="subsequent_locked_holdout",
        selection_time=stamp(10, 8))]
    return dict(mode="walk_forward_prediction_v1", design=design, tables=tables)


def holdout_fixture():
    candidate = {"model": {"class": "LinearModel"}}
    cid = "candidate_" + digest(candidate)[:16]
    design = dict(holdout_start=stamp(20), root_seed=17)
    selection = dict(selected_candidate_id=cid, selected_parameters_json=json.dumps(candidate), selection_hash="1" * 64)
    row = prediction(sample(20), "locked_holdout", cid, "holdout", 0.5)
    receipt = dict(status="committed", objective="neg_mean_squared_error", metric=-0.25)
    freeze = dict(parent_research_purpose=digest(design), mode="single_candidate_confirmation",
        candidates=[digest(candidate)], failure_policy="opened_then_failure_is_consumed",
        random_protocol={"seed": 17}, validation={"rule": {"selection_hash": selection["selection_hash"], "objective": receipt["objective"]}},
        data_snapshot="2" * 64, holdout_split={"sample_ids": ["s20"], "start": "2020-01-20", "end": "2020-01-20"})
    identity = digest(dict(parent_research_purpose=digest(design), data_snapshot=freeze["data_snapshot"],
        holdout_start="2020-01-20", holdout_end="2020-01-20", sample_ids=["s20"]))
    plan = dict(freeze_payload=freeze, actor="tester", reason="confirmation", unlock_at=stamp(21), holdout_identity_hash=identity)
    plan["plan_hash"] = digest(plan)
    plan.update(contract_version="research-persistent-holdout-plan-v2", state="frozen", token_hash=digest({"domain": "locked-holdout-token-v2", "plan_hash": plan["plan_hash"]}))
    prepared = dict(contract_version="research-persistent-holdout-prepared-v2", state="prepared", plan_hash=plan["plan_hash"], preflight={"count": 1}, prepared_at=stamp(21))
    prepared["prepared_hash"] = digest(prepared)
    opened = dict(contract_version="research-persistent-holdout-opened-v2", state="opened", plan_hash=plan["plan_hash"], prepared_hash=prepared["prepared_hash"],
        actor="tester", reason="confirmation", token_hash=plan["token_hash"], sample_ids_hash=digest(["s20"]), opened_at=stamp(21), opening_id="holdout:" + identity[:16])
    opened["opened_hash"] = digest(opened)
    result_hash = digest({"predictions": [row], "objective": receipt["objective"], "metric": receipt["metric"]})
    terminal = dict(contract_version="research-persistent-holdout-terminal-v2", opened_hash=opened["opened_hash"], status="committed", reason=None, result_hash=result_hash)
    terminal["terminal_hash"] = digest(terminal)
    receipt.update(holdout_identity_hash=identity, plan_hash=plan["plan_hash"], prepared_hash=prepared["prepared_hash"], opened_hash=opened["opened_hash"], terminal_hash=terminal["terminal_hash"], result_hash=result_hash)
    model = dict(mode="walk_forward_prediction_v1", design=design,
        tables={"holdout_predictions": [row], "holdout_index": [dict(sample_id="s20", label_available_time=stamp(21, 8))], "holdout_receipt": [receipt]},
        holdout_ledger=dict(plan=plan, prepared=prepared, opened=opened, terminal=terminal))
    return model, selection


def test_independent_split_and_visible_selection():
    model = model_fixture()
    samples, folds, candidates = _split(model)
    assert len(folds) == 3
    assert _selection(model, samples, folds, candidates)["selected_candidate_id"] == "a"


@pytest.mark.parametrize("mutation", ["late_label", "embargo", "fit_scope"])
def test_split_rejects_actual_temporal_errors(mutation):
    model = model_fixture()
    if mutation == "late_label":
        model["tables"]["samples"][2]["label_available_time"] = stamp(5)
    elif mutation == "embargo":
        next(row for row in model["tables"]["split_audit"] if row["role"] == "embargoed")["role"] = "train"
    else:
        model["tables"]["fit_audit"][0]["train_ids_json"] = '["s1","s2","s3","s4"]'
    with pytest.raises(EvidenceContractError):
        _split(model)


@pytest.mark.parametrize("mutation", ["future_selection", "wrong_test_candidate", "validation_label"])
def test_selection_rejects_future_or_unbound_inputs(mutation):
    model = model_fixture()
    samples, folds, candidates = _split(model)
    if mutation == "future_selection":
        model["tables"]["fold_selections"][0]["validation_fold_count"] = 3
    elif mutation == "wrong_test_candidate":
        model["tables"]["test_predictions"][0]["candidate_id"] = "b"
    else:
        model["tables"]["validation_predictions"][0]["actual"] = 0.1
    with pytest.raises(EvidenceContractError):
        _selection(model, samples, folds, candidates)


def test_holdout_receipt_content_and_mse():
    model, selection = holdout_fixture()
    assert _holdout(model, selection) == (0.25, 1)


@pytest.mark.parametrize("mutation", ["predictions", "opened", "terminal", "candidate"])
def test_holdout_rejects_changed_content(mutation):
    model, selection = holdout_fixture()
    if mutation == "predictions":
        model["tables"]["holdout_predictions"][0]["prediction"] = -0.5
    elif mutation == "candidate":
        model["tables"]["holdout_predictions"][0]["candidate_id"] = "another"
    else:
        model["holdout_ledger"][mutation]["reason"] = "changed"
    with pytest.raises(EvidenceContractError):
        _holdout(model, selection)


def test_result_table_facts_cannot_replace_snapshot_rows():
    model = dict(mode="walk_forward_prediction_v1", tables={"samples": [{"sample_id": "s2"}]}, table_bindings={"samples": "test.samples.v1"})
    snapshot = SimpleNamespace(read_table=lambda schema: pa.table({"sample_id": ["s1"]}))
    with pytest.raises(EvidenceContractError, match="Result"):
        verify_model_result_binding(snapshot, {"model_diagnostics": model})


def metric_fixture():
    model, _ = holdout_fixture()
    model["tables"]["metrics"] = [dict(metric_ref="test.mse@1.0.0", value=0.25,
        unit="squared_decimal_price_change", sample_start="2020-01-20", sample_end="2020-01-20",
        sample_size=1, status="computed")]
    statistics = dict(method="prediction_mse", sample_count=1, mse=0.25, metric_ref="test.mse@1.0.0")
    return model, statistics


def test_metric_metadata_matches_holdout_rows():
    model, statistics = metric_fixture()
    _statistics(model, statistics, 0.25, 1)


@pytest.mark.parametrize(("field", "value"), [
    ("unit", "decimal_return"), ("sample_start", "2020-01-19"),
    ("sample_end", "2020-01-21"), ("sample_size", 2), ("status", "not_computed"),
])
def test_metric_metadata_rejects_wrong_scope(field, value):
    model, statistics = metric_fixture()
    model["tables"]["metrics"][0][field] = value
    with pytest.raises(EvidenceContractError):
        _statistics(model, statistics, 0.25, 1)


def test_metric_metadata_is_required():
    model, statistics = metric_fixture()
    del model["tables"]["metrics"][0]["sample_size"]
    with pytest.raises(KeyError):
        _statistics(model, statistics, 0.25, 1)


@pytest.mark.parametrize("field,value", [("sample", 5), ("design", 5), ("design", True), ("design", 0)])
def test_split_rejects_sample_horizon_different_from_frozen_design(field, value):
    model = model_fixture()
    if field == "sample":
        model["tables"]["samples"][0]["horizon_sessions"] = value
    else:
        model["design"]["horizon_sessions"] = value
    with pytest.raises(EvidenceContractError, match="期限"):
        _split(model)


def test_holdout_rejects_prediction_horizon_different_from_design():
    model, selection = holdout_fixture()
    model["tables"]["holdout_predictions"][0]["horizon_sessions"] = 5
    with pytest.raises(EvidenceContractError, match="期限"):
        _holdout(model, selection)
