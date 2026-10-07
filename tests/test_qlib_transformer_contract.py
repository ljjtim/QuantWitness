"""Transformer 候选、完整窗口和公开准入合同。"""
from __future__ import annotations

import pytest

from research_pipeline.research.modeling.qlib import QlibModelError, normalize_candidates
from test_qlib_gru_contract import gru_candidate
from test_qlib_lstm_contract import lstm_candidate


def transformer_candidate(**changes):
    item = gru_candidate()
    item["candidate_id"] = "transformer-contract"
    item["model"] = {
        "class": "TransformerModel",
        "module_path": "qlib.contrib.model.pytorch_transformer_ts",
        "kwargs": {
            "d_feat": 2, "d_model": 4, "nhead": 2, "num_layers": 1,
            "dropout": 0.0, "n_epochs": 2, "lr": 0.001, "reg": 0.0,
            "batch_size": 1, "early_stop": 1, "GPU": -1, "n_jobs": 0,
            "loss": "mse", "metric": "", "optimizer": "adam",
        },
    }
    for path, value in changes.items():
        target = item
        parts = path.split(".")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
    return item


def test_transformer_candidate_preserves_table_gru_and_lstm_contracts():
    from test_qlib_sequence_admission import _table_candidate

    candidates = [_table_candidate(), gru_candidate(), lstm_candidate(), transformer_candidate()]
    normalized = normalize_candidates(candidates)
    assert normalized == candidates
    assert "dataset" not in normalized[0]
    assert normalized[-1]["dataset"] == {
        "class": "TSDatasetH", "step_len": 3, "missing_policy": "complete_window",
    }


@pytest.mark.parametrize("step_len", [2, 1000])
def test_transformer_accepts_supported_position_encoding_lengths(step_len):
    candidate = transformer_candidate(**{"dataset.step_len": step_len})
    assert normalize_candidates([candidate])[0]["dataset"]["step_len"] == step_len


@pytest.mark.parametrize(
    "path,value,message",
    [
        ("model.kwargs.d_model", 1, "d_model"),
        ("model.kwargs.d_model", 3, "d_model"),
        ("model.kwargs.d_model", True, "d_model"),
        ("model.kwargs.d_model", 4.0, "d_model"),
        ("model.kwargs.nhead", 0, "nhead"),
        ("model.kwargs.nhead", 3, "nhead"),
        ("model.kwargs.nhead", True, "nhead"),
        ("model.kwargs.nhead", 2.0, "nhead"),
        ("model.kwargs.reg", -0.1, "reg"),
        ("model.kwargs.reg", float("inf"), "reg"),
        ("model.kwargs.reg", float("nan"), "reg"),
        ("model.kwargs.reg", True, "reg"),
        ("model.kwargs.num_layers", 0, "num_layers"),
        ("model.kwargs.n_epochs", 0, "n_epochs"),
        ("model.kwargs.dropout", 1.0, "dropout"),
        ("model.kwargs.lr", 0.0, "lr"),
        ("model.kwargs.GPU", 0, "CPU"),
        ("model.kwargs.n_jobs", 1, "n_jobs"),
        ("model.kwargs.batch_size", 2, "batch_size"),
        ("model.kwargs.loss", "mae", "mse"),
        ("model.kwargs.metric", "rmse", "mse"),
        ("model.kwargs.optimizer", "adamw", "optimizer"),
        ("model.kwargs.hidden_size", 4, "固定构造参数"),
        ("dataset.step_len", 1, "step_len"),
        ("dataset.step_len", 1001, "1000"),
        ("dataset.step_len", True, "step_len"),
        ("dataset.class", "DatasetH", "TSDatasetH"),
        ("dataset.missing_policy", "fill", "complete_window"),
        ("model.module_path", "qlib.contrib.model.pytorch_transformer", "固定路径"),
        ("fit", {"save_path": "weights.pt"}, "fit 必须为空"),
    ],
)
def test_transformer_rejects_unsupported_parameters_before_runtime(path, value, message):
    with pytest.raises(QlibModelError, match=message):
        normalize_candidates([transformer_candidate(**{path: value})])


@pytest.mark.parametrize("scope", ["development", "final"])
def test_transformer_public_admission_accepts_all_supported_candidate_families(scope):
    from research_pipeline.packages import ResearchPackageError
    from test_qlib_sequence_admission import _check, _recipe, _table_candidate

    candidates = [transformer_candidate(), lstm_candidate(), gru_candidate(), _table_candidate()]
    _check(_recipe(scope=scope, fit=candidates, holdout=candidates))
    candidates[0]["dataset"]["step_len"] = 4
    with pytest.raises(ResearchPackageError, match="匹配split冻结"):
        _check(_recipe(scope=scope, fit=candidates, holdout=candidates))


@pytest.mark.parametrize("scope", ["development", "final"])
def test_transformer_public_admission_rejects_rank_labels(scope):
    from research_pipeline.packages import ResearchPackageError
    from test_qlib_sequence_admission import _check, _recipe

    candidate = transformer_candidate()
    candidate["processors"]["learn"].append({"class": "CSRankNorm", "kwargs": {}})
    with pytest.raises(ResearchPackageError, match="raw标签"):
        _check(_recipe(scope=scope, fit=[candidate], holdout=[candidate]))


@pytest.mark.parametrize("version", ["missing", "2.6.0"])
def test_transformer_preflight_rejects_missing_or_unfrozen_torch(monkeypatch, version):
    from research_pipeline.research.modeling import walk_forward

    original = walk_forward._distribution_version
    monkeypatch.setattr(
        walk_forward, "_distribution_version",
        lambda name: version if name == "torch" else original(name),
    )
    with pytest.raises(walk_forward.ModelDependencyError, match="torch"):
        walk_forward.model_dependency_preflight([transformer_candidate()], thread_count=1)


def test_transformer_walk_forward_normalization_preserves_model_and_window():
    from research_pipeline.research.modeling.walk_forward import normalize_model_candidates

    candidate = normalize_model_candidates([transformer_candidate()])[0]
    assert candidate["model"]["class"] == "TransformerModel"
    assert candidate["dataset"]["step_len"] == 3
