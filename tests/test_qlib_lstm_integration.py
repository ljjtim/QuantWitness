"""真实 CPU LSTM 的训练、无标签预测和序列文件恢复验收。"""
from __future__ import annotations

import importlib.util
import json

import numpy as np
import pytest

from research_pipeline.research.modeling.qlib import fit_bundle, predict_bundle
from test_qlib_gru_integration import inputs
from test_qlib_lstm_contract import lstm_candidate

pytestmark = pytest.mark.skipif(importlib.util.find_spec("torch") is None, reason="需要 ml-sequence 可选依赖")


def test_lstm_round_trip_does_not_read_prediction_labels(tmp_path, monkeypatch):
    import numpy as np
    from qlib.data.dataset import TSDatasetH

    train, valid, test, windows, original = inputs()
    spec = lstm_candidate()
    spec["processors"] = original["processors"]
    prepared = []
    original_prepare = TSDatasetH.prepare

    def tracked(self, segment, **kwargs):
        prepared.append(segment)
        data = original_prepare(self, segment, **kwargs)
        if segment == "test":
            assert np.isnan(data[0][:, -1]).all()
        return data

    monkeypatch.setattr(TSDatasetH, "prepare", tracked)
    row = fit_bundle(
        train, valid, candidate=spec, feature_columns=windows.feature_columns,
        output_root=tmp_path, bundle_path="bundles/lstm", root_seed=7,
        fit_scope_ref="train-only", sequence_context=windows,
    )
    prediction = predict_bundle(
        tmp_path, row,
        test.drop(columns=["target", "label_available_time", "label_start_time", "label_end_time"]),
        sequence_context=windows,
    )
    assert prepared == ["train", "valid", "test"]
    assert prediction.shape == (len(test),) and np.isfinite(prediction).all()
    config = json.loads((tmp_path / row["config_path"]).read_text(encoding="utf-8"))
    assert config["candidate"]["model"]["class"] == "LSTM"
    assert config["schema"] == "research.qlib-sequence-model-bundle.v1"
    assert config["training_curve"]
    assert {record["metric"] for record in config["training_curve"]} == {"negative_mse"}


def test_lstm_prediction_is_independent_of_future_target_values(tmp_path):
    train, valid, test, windows, original = inputs()
    spec = lstm_candidate()
    spec["processors"] = original["processors"]
    row = fit_bundle(
        train, valid, candidate=spec, feature_columns=windows.feature_columns,
        output_root=tmp_path, bundle_path="bundles/lstm", root_seed=7,
        fit_scope_ref="train-only", sequence_context=windows,
    )
    infer = test.drop(columns=["target", "label_available_time", "label_start_time", "label_end_time"])
    base = predict_bundle(tmp_path, row, infer, sequence_context=windows)
    changed = infer.copy()
    changed["target"] = 1e15
    np.testing.assert_array_equal(base, predict_bundle(tmp_path, row, changed, sequence_context=windows))
