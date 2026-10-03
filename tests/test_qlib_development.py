"""开发研究不读取最终样本，不接受test/holdout评价。"""
import pandas as pd
import pyarrow.parquet as pq
import pytest

from research_pipeline.evidence import model_validity as validity
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.runtime import walk_forward_model_execution as execution
from test_walk_forward_model_mainline import _write_input_artifact
from test_model_validity import model_fixture


def _inputs(tmp_path):
    days = pd.bdate_range("2024-01-02", periods=72, tz="UTC")
    features, labels = [], []
    for index, day in enumerate(days[:70]):
        features.append(dict(entity_id="SYN", observation_session=day.date(),
            observation_time=day + pd.Timedelta(hours=9), available_time=day + pd.Timedelta(hours=9),
            feature_id="return", window_sessions=5, value=index / 100, status="ok", lineage_hash="a" * 64))
        labels.append(dict(entity_id="SYN", observation_session=day.date(),
            decision_time=day + pd.Timedelta(hours=9), label_start_time=day + pd.Timedelta(hours=15),
            label_end_time=days[index+1] + pd.Timedelta(hours=15), available_time=days[index+2] + pd.Timedelta(hours=9),
            horizon_sessions=1, forward_return=index / 1000, lineage_hash="b" * 64))
    for name, rows in (("features", features), ("labels", labels)):
        _write_input_artifact(tmp_path / name, name, pd.DataFrame(rows), "semantics")
    path = next((tmp_path / "labels" / "labels").glob("*.parquet"))
    table = pq.read_table(path)
    pq.write_table(table, path, row_group_size=1)
    return dict(holdout_start=days[60].isoformat(), horizon_sessions=1, target_field="forward_return",
        calendar_sessions=[day.date().isoformat() for day in days], train_sessions=20,
        validation_sessions=8, test_sessions=8, step_sessions=8, embargo_sessions=1, expanding=True)


def test_development_split_does_not_scan_holdout_index(tmp_path, monkeypatch):
    parameters = _inputs(tmp_path)
    calls = []
    original = execution._load_label_slice
    def load(*args, **kwargs):
        calls.append(kwargs["label"])
        if "holdout" in kwargs["label"]:
            pytest.fail("开发分支不得读取holdout索引")
        return original(*args, **kwargs)
    monkeypatch.setattr(execution, "_load_label_slice", load)
    result = execution.execute_model_split_artifact(feature_root=tmp_path/"features", label_root=tmp_path/"labels",
        parameters={**parameters, "evaluation_scope": "development"}, output_root=tmp_path/"split",
        fixed_clock="2025-01-01T00:00:00Z", max_memory_bytes=64*1024**2)
    assert calls == ["Walk-forward development labels"]
    assert result["holdout_end"] is None
    assert result["row_counts"]["holdout_index"] == 0
    assert pq.read_table(tmp_path/"split"/"holdout_index").schema.field("sample_id").type is not None


def test_final_split_retains_holdout_index_by_default(tmp_path):
    parameters = _inputs(tmp_path)
    result = execution.execute_model_split_artifact(feature_root=tmp_path/"features", label_root=tmp_path/"labels",
        parameters=parameters, output_root=tmp_path/"split", fixed_clock="2025-01-01T00:00:00Z", max_memory_bytes=64*1024**2)
    assert result["row_counts"]["holdout_index"] == 10
    assert result["holdout_end"] is not None


def _development(monkeypatch):
    model = model_fixture()
    if isinstance(model, tuple):
        model = model[0]
    for name in ("test_predictions", "selection", "fold_selections", "holdout_predictions", "holdout_receipt"):
        model["tables"].pop(name, None)
    model["holdout_ledger"] = {}
    model["tables"]["holdout_index"] = []
    monkeypatch.setattr(validity, "_fit_configs", lambda *args: None)
    samples, folds, candidates = validity._split(model)
    return model, samples, folds, candidates


def test_development_validates_validation_predictions(monkeypatch):
    args = _development(monkeypatch)
    mse, count = validity._development(*args)
    assert mse >= 0 and count > 0


@pytest.mark.parametrize("attack", ["test_predictions", "selection", "holdout_predictions", "holdout_receipt", "holdout_index", "ledger", "future_feature", "wrong_actual"])
def test_development_rejects_final_evaluation_and_leakage(monkeypatch, attack):
    model, samples, folds, candidates = _development(monkeypatch)
    if attack == "ledger":
        model["holdout_ledger"] = {"opened": {}}
    elif attack == "future_feature":
        model["tables"]["validation_predictions"][0]["feature_available_time"] = "2030-01-01T00:00:00+00:00"
    elif attack == "wrong_actual":
        model["tables"]["validation_predictions"][0]["actual"] += 1
    else:
        model["tables"][attack] = [{}]
    with pytest.raises(EvidenceContractError):
        validity._development(model, samples, folds, candidates)
