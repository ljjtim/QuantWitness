"""开发预测读取的阶段、时间、来源和预算边界，无数据库。"""
import copy
from datetime import date
import json
from types import SimpleNamespace

import pyarrow as pa
import pytest

from quantwitness_rdagent import campaign_data


def prediction(**changes):
    row = {
        "candidate_id": "ridge", "fold_id": "fold1", "sample_id": "sample1", "entity_id": "synthetic1",
        "observation_session": "2025-01-02", "prediction": 0.02, "actual": 0.01,
        "feature_available_time": "2025-01-02T15:00:00+08:00",
        "decision_time": "2025-01-02T15:00:00+08:00",
        "label_start_time": "2025-01-02T15:00:00+08:00",
        "label_end_time": "2025-01-03T15:00:00+08:00",
        "label_available_time": "2025-01-06T09:30:00+08:00",
        "stage": "validation", "horizon_sessions": 1, "score_semantics": "raw_return_prediction",
    }
    return {**row, **changes}


@pytest.fixture
def setup(tmp_path):
    fixture = {"fixture": "synthetic", "design": {"holdout_start": "2025-02-01T00:00:00+08:00"},
               "rows": [prediction()]}
    path = tmp_path / "fixture.json"
    payload = {
        "source": {"kind": "synthetic", "path": str(path)},
        "development": {"start": "2025-01-02", "end": "2025-01-03", "as_of": "2025-01-07T15:00:00+08:00",
                        "horizon_sessions": 1, "fold_ids": ["fold1"]},
        "budget": {"max_rows": 20, "memory_bytes": 1024 * 1024},
    }
    def save():
        path.write_text(json.dumps(fixture, ensure_ascii=False), encoding="utf-8")
    save()
    return payload, fixture, save


def test_synthetic_rows_are_projected_without_final_feedback(setup):
    payload, fixture, save = setup
    fixture["rows"][0]["holdout_metric"] = 999
    fixture["selection"] = {"final_winner": "forbidden"}
    save()
    result = campaign_data.load_development(payload)
    assert result["rows"] == [prediction()]
    assert result["provenance"]["kind"] == "synthetic"
    assert "result_id" not in result["provenance"]
    assert "999" not in json.dumps(result)
    assert result["holdout_start"] == fixture["design"]["holdout_start"]


@pytest.mark.parametrize("fault", [
    "fixture_marker", "test", "holdout", "future_label", "future_feature", "backward_label",
    "rank_score", "nan", "infinite", "bool", "naive_asof", "asof_holdout", "window_holdout",
    "duplicate", "empty_fold", "duplicate_fold", "missing_column", "max_rows", "memory",
])
def test_rejects_unusable_development(setup, fault):
    payload, fixture, save = setup
    row = fixture["rows"][0]
    if fault == "fixture_marker":
        fixture["fixture"] = "real"
    elif fault in {"test", "holdout"}:
        row["stage"] = fault
    elif fault == "future_label":
        row["label_available_time"] = "2025-01-08T09:30:00+08:00"
    elif fault == "future_feature":
        row["feature_available_time"] = "2025-01-03T15:00:00+08:00"
    elif fault == "backward_label":
        row["label_end_time"] = "2025-01-01T15:00:00+08:00"
    elif fault == "rank_score":
        row["score_semantics"] = "ranking_score"
    elif fault in {"nan", "infinite", "bool"}:
        row["prediction"] = {"nan": float("nan"), "infinite": float("inf"), "bool": True}[fault]
    elif fault == "naive_asof":
        payload["development"]["as_of"] = "2025-01-07T15:00:00"
    elif fault == "asof_holdout":
        payload["development"]["as_of"] = fixture["design"]["holdout_start"]
    elif fault == "window_holdout":
        payload["development"]["end"] = "2025-02-01"
    elif fault == "duplicate":
        fixture["rows"].append(copy.deepcopy(row))
    elif fault == "empty_fold":
        payload["development"]["fold_ids"] = ["other"]
    elif fault == "duplicate_fold":
        payload["development"]["fold_ids"] = ["fold1", "fold1"]
    elif fault == "missing_column":
        del row["label_available_time"]
    elif fault == "max_rows":
        fixture["rows"].append(prediction(sample_id="sample2"))
        payload["budget"]["max_rows"] = 1
    else:
        payload["budget"]["memory_bytes"] = 1
    save()
    with pytest.raises(ValueError, match="campaign\\."):
        campaign_data.load_development(payload)


def test_rejects_test_rows_even_outside_selected_window(setup):
    payload, fixture, save = setup
    fixture["rows"].append(prediction(stage="test", observation_session="2024-01-01"))
    save()
    with pytest.raises(ValueError, match="non_validation_row"):
        campaign_data.load_development(payload)


class Snapshot:
    def __init__(self, rows, design):
        normalized = [{**row, "observation_session": date.fromisoformat(row["observation_session"])} for row in rows]
        self.tables = {
            "validation.v1": pa.Table.from_pylist(normalized),
            "design.v1": pa.Table.from_pylist([{"design_json": json.dumps(design)}]),
        }
        self.calls = []
        self.bundle = SimpleNamespace(result_id="result1", tables=[
            SimpleNamespace(table_id="validation_predictions", schema_id="validation.v1"),
            SimpleNamespace(table_id="study_design", schema_id="design.v1"),
        ])

    def table_schema(self, schema_id):
        return self.tables[schema_id].schema

    def iter_table_batches(self, schema_id, *, columns, batch_size):
        self.calls.append((schema_id, tuple(columns)))
        return iter(self.tables[schema_id].select(columns).to_batches(max_chunksize=batch_size))


def formal(setup, monkeypatch):
    payload, fixture, _ = setup
    payload["source"] = {"kind": "verified_result", "verification_result": "verified.json",
                         "result_store": "result-store", "result_id": "result1",
                         "table_id": "validation_predictions", "design_table_id": "study_design"}
    snapshot = Snapshot(fixture["rows"], fixture["design"])
    reference = SimpleNamespace(result_id="result1", to_dict=lambda: {"project_id": "p", "run_id": "r", "result_id": "result1"})
    verification = SimpleNamespace(status="pass", result_reference=reference, verification_hash="verified1")
    context = SimpleNamespace(snapshot=snapshot, verification=verification,
                              metrics={"holdout_mse": 888, "winner": "never-forward"})
    monkeypatch.setattr(campaign_data, "_load_verified", lambda source: context)
    return payload, context


def test_formal_input_preserves_only_development_source_references(setup, monkeypatch):
    payload, context = formal(setup, monkeypatch)
    result = campaign_data.load_development(payload)
    assert result["rows"] == [prediction()]
    assert result["provenance"] == {
        "kind": "verified_result", "result_reference": {"project_id": "p", "run_id": "r", "result_id": "result1"},
        "result_id": "result1", "verification_hash": "verified1",
        "table_id": "validation_predictions", "schema_id": "validation.v1",
    }
    assert "never-forward" not in json.dumps(result)
    assert context.snapshot.calls == [("design.v1", ("design_json",)), ("validation.v1", campaign_data.COLUMNS)]


@pytest.mark.parametrize("fault", ["unverified", "result_id", "table_id", "design_table_id", "duplicate_table"])
def test_formal_input_rejects_wrong_bindings(setup, monkeypatch, fault):
    payload, context = formal(setup, monkeypatch)
    if fault == "unverified":
        context.verification.status = "fail"
    elif fault == "result_id":
        payload["source"]["result_id"] = "other"
    elif fault in {"table_id", "design_table_id"}:
        payload["source"][fault] = "holdout_predictions"
    else:
        context.snapshot.bundle.tables.append(context.snapshot.bundle.tables[0])
    with pytest.raises(ValueError, match="campaign\\."):
        campaign_data.load_development(payload)
