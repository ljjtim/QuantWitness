from __future__ import annotations

import pytest

from research_pipeline.research.modeling.qlib import QlibModelError, normalize_candidates
from research_pipeline.research.modeling.walk_forward import ModelDependencyError, model_dependency_preflight


def gru_candidate(**changes):
    item = {
        "candidate_id": "gru-contract",
        "model": {"class": "GRU", "module_path": "qlib.contrib.model.pytorch_gru_ts",
                  "kwargs": {"d_feat": 2, "hidden_size": 4, "num_layers": 1,
                              "batch_size": 1, "n_epochs": 2, "early_stop": 1,
                              "GPU": -1, "n_jobs": 0, "loss": "mse", "metric": "",
                              "optimizer": "adam"}},
        "dataset": {"class": "TSDatasetH", "step_len": 3, "missing_policy": "complete_window"},
        "processors": {"infer": [], "learn": [{"class": "DropnaLabel", "kwargs": {}}]},
        "fit": {},
    }
    for path, value in changes.items():
        target = item
        parts = path.split(".")
        for part in parts[:-1]: target = target[part]
        target[parts[-1]] = value
    return item


def test_gru_candidate_contract_is_explicit_and_does_not_change_table_candidates():
    normalized = normalize_candidates([gru_candidate()])
    assert normalized[0]["dataset"] == {"class": "TSDatasetH", "step_len": 3, "missing_policy": "complete_window"}
    with pytest.raises(QlibModelError, match="dataset"):
        normalize_candidates([dict(gru_candidate(), dataset={"class": "DatasetH"})])
    table = {"candidate_id": "linear", "model": {"class": "LinearModel", "module_path": "qlib.contrib.model.linear", "kwargs": {}},
             "processors": {"infer": [], "learn": [{"class": "DropnaLabel", "kwargs": {}}]}, "fit": {}}
    assert "dataset" not in normalize_candidates([table])[0]


def test_gru_rejects_table_dataset_and_unsupported_settings():
    bad = gru_candidate()
    bad["model"]["kwargs"]["GPU"] = 0
    with pytest.raises(QlibModelError, match="CPU"):
        normalize_candidates([bad])
    bad = gru_candidate()
    bad["model"]["kwargs"]["metric"] = "rmse"
    with pytest.raises(QlibModelError, match="mse"):
        normalize_candidates([bad])
    bad = gru_candidate()
    bad["dataset"]["missing_policy"] = "fill"
    with pytest.raises(QlibModelError, match="complete_window"):
        normalize_candidates([bad])
    bad = gru_candidate()
    bad["dataset"]["step_len"] = 1
    with pytest.raises(QlibModelError, match="step_len"):
        normalize_candidates([bad])


def test_gru_dependency_preflight_stops_before_runtime_when_torch_is_missing(monkeypatch):
    import research_pipeline.research.modeling.walk_forward as modeling
    original = modeling._distribution_version
    monkeypatch.setattr(modeling, "_distribution_version", lambda name: "missing" if name == "torch" else original(name))
    with pytest.raises(ModelDependencyError, match="torch"):
        model_dependency_preflight([gru_candidate()], thread_count=1)


def test_model_candidate_normalization_preserves_gru_window_contract():
    from research_pipeline.research.modeling.walk_forward import normalize_model_candidates
    result = normalize_model_candidates([gru_candidate()])
    assert result[0]["dataset"]["step_len"] == 3


def test_gru_dependency_preflight_rejects_unsupported_torch_version(monkeypatch):
    import research_pipeline.research.modeling.walk_forward as modeling
    original = modeling._distribution_version
    monkeypatch.setattr(modeling, "_distribution_version", lambda name: "2.6.0" if name == "torch" else original(name))
    with pytest.raises(ModelDependencyError, match="torch==2.5.1"):
        model_dependency_preflight([gru_candidate()], thread_count=1)
