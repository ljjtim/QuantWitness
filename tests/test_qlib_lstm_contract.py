from __future__ import annotations

import pytest

from research_pipeline.research.modeling.qlib import QlibModelError, normalize_candidates
from test_qlib_gru_contract import gru_candidate


def lstm_candidate(**changes):
    item = gru_candidate()
    item["candidate_id"] = "lstm-contract"
    item["model"]["class"] = "LSTM"
    item["model"]["module_path"] = "qlib.contrib.model.pytorch_lstm_ts"
    for path, value in changes.items():
        target = item
        parts = path.split(".")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
    return item


def test_lstm_candidate_uses_the_same_explicit_window_contract():
    normalized = normalize_candidates([lstm_candidate()])
    assert normalized[0]["dataset"] == {
        "class": "TSDatasetH", "step_len": 3, "missing_policy": "complete_window",
    }


@pytest.mark.parametrize("field", ["GPU", "n_jobs", "batch_size"])
def test_lstm_rejects_non_deterministic_resource_settings(field):
    bad = lstm_candidate()
    bad["model"]["kwargs"][field] = {"GPU": 0, "n_jobs": 2, "batch_size": 2}[field]
    with pytest.raises(QlibModelError):
        normalize_candidates([bad])


@pytest.mark.parametrize("scope", ["development", "final"])
def test_lstm_public_admission_accepts_mixed_candidates_and_rejects_wrong_windows(scope):
    from test_qlib_sequence_admission import _recipe, _check, _table_candidate
    from research_pipeline.packages import ResearchPackageError
    candidates = [lstm_candidate(), gru_candidate(), _table_candidate()]
    _check(_recipe(scope=scope, fit=candidates, holdout=candidates))
    candidates[0]["dataset"]["step_len"] = 4
    with pytest.raises(ResearchPackageError, match="匹配split冻结"):
        _check(_recipe(scope=scope, fit=candidates, holdout=candidates))


def test_lstm_preflight_rejects_missing_torch(monkeypatch):
    from research_pipeline.research.modeling import walk_forward
    original = walk_forward._distribution_version
    monkeypatch.setattr(walk_forward, "_distribution_version",
                        lambda name: "missing" if name == "torch" else original(name))
    with pytest.raises(walk_forward.ModelDependencyError, match="torch"):
        walk_forward.model_dependency_preflight([lstm_candidate()], thread_count=1)
