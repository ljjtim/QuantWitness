"""公开序列示例独立筛选完整历史窗口。"""
from datetime import date, timedelta
import importlib.util
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio/verifier/check.py"
spec = importlib.util.spec_from_file_location("sequence_example_verifier", SOURCE)
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


def case():
    days = [date(2024, 1, 2) + timedelta(days=i) for i in range(4)]
    columns = ["historical_return__w10", "historical_return__w5", "volatility__w10", "volatility__w5"]
    design = {"split_calendar_sessions": [str(day) for day in days], "sequence": {
        "calendar_sessions": [str(day) for day in days], "step_len": 3,
        "feature_columns": columns, "missing_policy": "complete_window",
        "candidate_sample_policy": "shared_complete_endpoints"}}
    features = [{"entity_id": "SYN", "observation_session": day, "status": "ok",
        "feature_id": feature, "window_sessions": window, "available_time": check._at(day, 9, 30)}
        for day in days for feature in ("historical_return", "volatility") for window in (5, 10)]
    tables = {"features": features, **{"sequence_" + name: [] for name in ("context", "targets", "members", "exclusions")}}
    samples = {str(day): {"entity_id": "SYN", "observation_session": day,
        "decision_time": check._at(day, 9, 30)} for day in days}
    return tables, design, samples, days


def test_sequence_example_excludes_incomplete_history():
    tables, design, samples, days = case()
    assert set(check._sequence_samples(tables, design, samples)) == {str(day) for day in days[2:]}


@pytest.mark.parametrize("kind", ["missing", "late"])
def test_sequence_example_uses_visible_complete_feature_set(kind):
    tables, design, samples, days = case()
    if kind == "missing":
        tables["features"][0]["status"] = "missing"
    else:
        tables["features"][0]["available_time"] = check._at(days[-1], 10)
    assert set(check._sequence_samples(tables, design, samples)) == {str(days[-1])}


@pytest.mark.parametrize("kind", ["calendar", "columns", "step", "evidence"])
def test_sequence_example_rejects_inconsistent_design(kind):
    tables, design, samples, days = case()
    if kind == "calendar":
        design["sequence"]["calendar_sessions"] = []
    elif kind == "columns":
        design["sequence"]["feature_columns"] = []
    elif kind == "step":
        design["sequence"]["step_len"] = True
    else:
        tables.pop("sequence_members")
    with pytest.raises(ValueError):
        check._sequence_samples(tables, design, samples)


def test_table_example_keeps_existing_sample_set():
    tables, design, samples, _ = case()
    design.pop("sequence")
    assert check._sequence_samples(tables, design, samples) == samples

@pytest.mark.parametrize("model_class", ["GRU", "LSTM"])
def test_public_prepare_freezes_explicit_gru_candidates(tmp_path, monkeypatch, model_class):
    import json
    import yaml
    example = SOURCE.parents[1]
    monkeypatch.syspath_prepend(str(example))
    module_spec = importlib.util.spec_from_file_location("sequence_candidate_prepare", example / "prepare.py")
    prepare = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(prepare)
    make = prepare.gru_candidate if model_class == "GRU" else prepare.lstm_candidate
    small, large = make(3), make(3)
    small["candidate_id"], large["candidate_id"] = "small", "large"
    large["model"]["kwargs"]["hidden_size"] = 8
    prepare.prepare(tmp_path / "research", "model", sequence_step_len=3, model_candidates=[small, large])
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
