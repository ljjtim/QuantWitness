"""公开序列示例的生产窗口资格与冻结候选。"""
from datetime import date, datetime, timedelta
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from research_pipeline.research.modeling.inputs import ModelMainlineError
from research_pipeline.research.modeling.sequence import build_sequence_windows
from research_pipeline.research.modeling import assemble_daily_model_samples

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"


def at(day, hour=9, minute=30):
    return datetime.fromisoformat(f"{day}T{hour:02d}:{minute:02d}:00+08:00")


def case():
    days = [date(2024, 1, 2) + timedelta(days=i) for i in range(4)]
    columns = ["historical_return__w10", "historical_return__w5", "volatility__w10", "volatility__w5"]
    features = pd.DataFrame([
        {"entity_id": "SYN", "observation_session": day, "status": "ok", "value": index / 100,
         "feature_id": feature, "window_sessions": window, "observation_time": at(day, 9, 0),
         "available_time": at(day), "lineage_hash": "feature"}
        for index, day in enumerate(days) for feature in ("historical_return", "volatility") for window in (5, 10)])
    targets = pd.DataFrame([
        {"sample_id": str(day), "entity_id": "SYN", "observation_session": day, "decision_time": at(day)}
        for day in days])
    parameters = {"calendar_sessions": days, "feature_columns": columns, "step_len": 3}
    return features, targets, parameters, days


def test_sequence_example_excludes_incomplete_history():
    features, targets, parameters, days = case()
    windows = build_sequence_windows(features, targets, **parameters)
    assert set(windows.members.sample_id) == {str(day) for day in days[2:]}
    assert windows.exclusions.reason_code.tolist() == ["insufficient_history", "insufficient_history"]
    assert set(windows.members.groupby("sample_id").size()) == {3}


@pytest.mark.parametrize("kind", ["missing", "late"])
def test_sequence_example_uses_visible_complete_feature_set(kind):
    features, targets, parameters, days = case()
    if kind == "missing":
        features.loc[0, "status"] = "missing"
    else:
        features.loc[0, "available_time"] = at(days[-1], 10, 0)
    windows = build_sequence_windows(features, targets, **parameters)
    assert set(windows.members.sample_id) == {str(days[-1])}
    reason = "missing_feature" if kind == "missing" else "feature_not_visible"
    assert windows.exclusions.set_index("sample_id").loc[str(days[2]), "reason_code"] == reason


@pytest.mark.parametrize("kind", ["calendar", "columns", "step"])
def test_sequence_example_rejects_inconsistent_design(kind):
    features, targets, parameters, _ = case()
    parameters[{"calendar": "calendar_sessions", "columns": "feature_columns", "step": "step_len"}[kind]] = True if kind == "step" else []
    with pytest.raises(ModelMainlineError):
        build_sequence_windows(features, targets, **parameters)


def test_table_example_keeps_short_history_endpoints():
    features, targets, _, days = case()
    labels = targets.drop(columns="sample_id").copy()
    labels["horizon_sessions"] = 1
    labels["forward_return"] = 0.01
    labels["lineage_hash"] = "label"
    labels["label_start_time"] = pd.to_datetime(labels.decision_time) + pd.Timedelta(hours=5, minutes=30)
    labels["label_end_time"] = labels.label_start_time + pd.Timedelta(days=1)
    labels["available_time"] = labels.label_end_time + pd.Timedelta(hours=18, minutes=30)
    samples, _ = assemble_daily_model_samples(features, labels, horizon_sessions=1)
    assert set(samples.sample_id) == {f"SYN:{day}:h1" for day in days}


@pytest.mark.parametrize("model_class", ["GRU", "LSTM"])
def test_public_prepare_freezes_explicit_gru_candidates(tmp_path, monkeypatch, model_class):
    import json
    import yaml
    example = EXAMPLE
    monkeypatch.syspath_prepend(str(example))
    module_spec = importlib.util.spec_from_file_location("sequence_candidate_prepare", example / "prepare.py")
    prepare = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(prepare)
    make = prepare.gru_candidate if model_class == "GRU" else prepare.lstm_candidate
    small, large = make(3), make(3)
    small["candidate_id"], large["candidate_id"] = "small", "large"
    large["model"]["kwargs"]["hidden_size"] = 8
    prepare.prepare(tmp_path / "research", "model", sequence_step_len=3, model_candidates=[small, large],
        research_series={"series_id": "sequence-model", "root": str(tmp_path / "series"),
            "purpose": "序列候选最终评价", "legacy_ledger_roots": []})
    request = json.loads((tmp_path / "research/request.json").read_text(encoding="utf-8"))
    design = request["design"]
    candidates = json.loads(design["candidate_parameters_json"])
    assert {item["model"]["class"] for item in candidates.values()} == {model_class}
    assert {item["model"]["kwargs"]["hidden_size"] for item in candidates.values()} == {4, 8}
    graph = yaml.safe_load((tmp_path / "research/package/spec/research.yaml").read_text(encoding="utf-8"))["graph"]
    models = [node for node in graph["nodes"] if node["node_id"] in {"model_fit", "model_holdout"}]
    assert len(models) == 2
    for node in models:
        assert node["parameters"]["candidate_jsons"] == request["model_parameters"]["candidate_jsons"]
        assert {json.loads(value)["model"]["class"] for value in node["parameters"]["candidate_jsons"]} == {model_class}
    assert small["candidate_id"] == "small" and large["candidate_id"] == "large"
