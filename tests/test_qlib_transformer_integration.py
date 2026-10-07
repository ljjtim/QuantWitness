"""真实 CPU Transformer 的训练、预测和可迁移封存验收。"""
from __future__ import annotations

from dataclasses import replace
import importlib.util
import json
import shutil

import numpy as np
import pandas as pd
import pytest

from research_pipeline.research.modeling.qlib import QlibModelError, fit_bundle, predict_bundle
from test_qlib_gru_integration import inputs as sequence_inputs
from test_qlib_transformer_contract import transformer_candidate

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("torch") is None, reason="需要 ml-sequence 可选依赖",
)


def inputs(model_class="TransformerModel"):
    train, valid, test, windows, original = sequence_inputs()
    candidate = transformer_candidate()
    candidate["processors"] = original["processors"]
    return train, valid, test, windows, candidate


def fit(root, train, valid, windows, candidate):
    return fit_bundle(
        train, valid, candidate=candidate, feature_columns=windows.feature_columns,
        output_root=root, bundle_path="bundles/transformer", root_seed=7,
        fit_scope_ref="train-only", sequence_context=windows,
    )


@pytest.mark.parametrize("parameter_scope", ["explicit", "upstream_defaults"])
def test_transformer_round_trip_single_sample_and_no_prediction_labels(tmp_path, monkeypatch, parameter_scope):
    import torch
    from qlib.data.dataset import TSDatasetH
    from qlib.utils.serial import Serializable

    original_prepare = TSDatasetH.prepare
    prepared = []

    def tracked(self, segment, **kwargs):
        prepared.append(segment)
        sampled = original_prepare(self, segment, **kwargs)
        if segment == "test":
            assert np.isnan(sampled[0][:, -1]).all()
        return sampled

    monkeypatch.setattr(TSDatasetH, "prepare", tracked)
    train, valid, test, windows, candidate = inputs()
    if parameter_scope == "upstream_defaults":
        for name in ("d_model", "nhead", "lr", "reg", "early_stop"):
            del candidate["model"]["kwargs"][name]
        candidate["model"]["kwargs"]["n_epochs"] = 1
    numpy_state = np.random.get_state()
    torch_state, threads = torch.get_rng_state(), torch.get_num_threads()
    row = fit(tmp_path, train, valid, windows, candidate)
    assert prepared == ["train", "valid"]
    assert torch.get_num_threads() == threads
    assert torch.equal(torch.get_rng_state(), torch_state)
    after = np.random.get_state()
    assert after[0] == numpy_state[0]
    assert np.array_equal(after[1], numpy_state[1]) and after[2:] == numpy_state[2:]

    infer = test.drop(columns=["target", "label_available_time", "label_start_time", "label_end_time"])
    prediction = predict_bundle(tmp_path, row, infer, sequence_context=windows)
    assert prediction.shape == (len(test),) and np.isfinite(prediction).all()
    single = predict_bundle(tmp_path, row, infer.iloc[:1], sequence_context=windows)
    assert single.shape == (1,) and single[0] == prediction[0]

    config = json.loads((tmp_path / row["config_path"]).read_text(encoding="utf-8"))
    assert config["schema"] == "research.qlib-sequence-model-bundle.v1"
    assert config["candidate"]["model"]["class"] == "TransformerModel"
    assert config["versions"]["torch"].split("+")[0] == "2.5.1"
    assert config["effective_model_kwargs"]["GPU"] == -1
    assert config["effective_model_kwargs"]["n_jobs"] == 0
    assert config["effective_model_kwargs"]["batch_size"] == 1
    assert config["effective_model_kwargs"]["d_feat"] == len(windows.feature_columns)
    assert config["effective_fit_kwargs"] == {"save_path": "bundles/transformer/weights.pt"}
    assert config["training_curve"]
    assert {record["metric"] for record in config["training_curve"]} == {"negative_mse"}
    assert all(record["value"] <= 0 for record in config["training_curve"])

    saved = Serializable.load(tmp_path / row["model_path"])
    weights = torch.load(tmp_path / config["weights_path"], weights_only=True)
    assert saved.model.feature_layer.in_features == len(windows.feature_columns)
    if parameter_scope == "upstream_defaults":
        assert saved.d_model == 64
        assert saved.model.transformer_encoder.layers[0].self_attn.num_heads == 2
        assert saved.lr == 0.0001 and saved.reg == 0.001 and saved.early_stop == 5
        assert saved.train_optimizer.param_groups[0]["lr"] == saved.lr
        assert saved.train_optimizer.param_groups[0]["weight_decay"] == saved.reg
    assert set(weights) == set(saved.model.state_dict())
    for name, value in weights.items():
        torch.testing.assert_close(value, saved.model.state_dict()[name], rtol=0, atol=0)
    stored_context = pd.read_parquet(tmp_path / config["sequence"]["files"]["context"])
    stored_targets = pd.read_parquet(tmp_path / config["sequence"]["files"]["targets"])
    assert stored_context.observation_session.max() == valid.observation_session.max()
    assert "target" not in stored_context and "target" not in stored_targets
    assert set(stored_targets.sample_id) == set(train.sample_id) | set(valid.sample_id)

    relocated = tmp_path / "relocated"
    shutil.copytree(tmp_path / "bundles", relocated / "bundles")
    np.testing.assert_array_equal(prediction, predict_bundle(relocated, row, infer, sequence_context=windows))
    assert set(prepared) == {"train", "valid", "test"}


def test_direct_upstream_transformer_matches_weights_and_single_batch_predictions(tmp_path):
    import torch
    from qlib.contrib.model.pytorch_transformer_ts import TransformerModel
    from qlib.data.dataset import TSDatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    from qlib.utils.serial import Serializable
    from research_pipeline.research.modeling.gru import cpu_execution
    from research_pipeline.research.modeling.qlib import _matrix

    train, valid, test, windows, candidate = inputs()
    candidate["processors"]["infer"] = []
    for frame in (train, valid, test):
        frame["x2__w1"] = frame["x2__w1"].fillna(0)
    windows.context["x2__w1"] = windows.context["x2__w1"].fillna(0)
    row = fit(tmp_path / "managed", train, valid, windows, candidate)
    config = json.loads((tmp_path / "managed" / row["config_path"]).read_text(encoding="utf-8"))

    # 对照独立构造上游数据集，并手取完整历史窗口送入网络。
    context = windows.context
    index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(context.observation_session), context.entity_id],
        names=["datetime", "instrument"],
    )
    data = pd.DataFrame(
        context[list(windows.feature_columns)].to_numpy(float), index=index,
        columns=pd.MultiIndex.from_product([["feature"], windows.feature_columns]),
    )
    data[("label", "target")] = np.nan
    data[("filter", "endpoint")] = False
    endpoint = _matrix(pd.concat([train, valid]), windows.feature_columns, label=True)
    data.loc[endpoint.index, ("label", "target")] = endpoint[("label", "target")]
    data.loc[endpoint.index, ("filter", "endpoint")] = True
    handler = DataHandlerLP(
        data_loader=StaticDataLoader(data.sort_index()), infer_processors=[], learn_processors=[],
    )
    dataset = TSDatasetH(
        handler=handler, step_len=3, flt_col="filter", segments={
            "train": (pd.Timestamp(train.observation_session.min()), pd.Timestamp(train.observation_session.max())),
            "valid": (pd.Timestamp(valid.observation_session.min()), pd.Timestamp(valid.observation_session.max())),
        },
    )

    def preserve_batch_axis(module, args, output):
        return output.reshape(-1)

    with cpu_execution():
        direct = TransformerModel(**config["effective_model_kwargs"])
        direct.model.register_forward_hook(preserve_batch_axis)
        direct.fit(dataset, save_path=str(tmp_path / "direct.pt"), evals_result={})
        managed = Serializable.load(tmp_path / "managed" / row["model_path"])
        for name, value in direct.model.state_dict().items():
            torch.testing.assert_close(value, managed.model.state_dict()[name], rtol=0, atol=0)
        expected = []
        direct.model.eval()
        for record in test.to_dict("records"):
            history = context.loc[
                (context.entity_id == record["entity_id"])
                & (context.observation_session <= record["observation_session"])
            ].sort_values("observation_session").tail(3)
            with torch.no_grad():
                tensor = torch.tensor(
                    history[list(windows.feature_columns)].to_numpy(float)[None, :, :],
                    dtype=torch.float32,
                )
                expected.append(direct.model(tensor).item())
    np.testing.assert_array_equal(
        expected, predict_bundle(tmp_path / "managed", row, test, sequence_context=windows),
    )


def test_transformer_fit_and_prediction_ignore_future_context_and_labels(tmp_path):
    import torch

    train, valid, test, windows, candidate = inputs()
    row = fit(tmp_path / "base", train, valid, windows, candidate)
    infer = test.drop(columns=["target", "label_available_time", "label_start_time", "label_end_time"])
    base = predict_bundle(tmp_path / "base", row, infer, sequence_context=windows)
    changed = replace(windows, context=windows.context.copy(), targets=windows.targets.copy())
    changed.context.loc[
        changed.context.observation_session > test.observation_session.max(), "x1__w1",
    ] = 1e12
    other = fit(tmp_path / "changed", train, valid, changed, candidate)
    left = torch.load(tmp_path / "base/bundles/transformer/weights.pt", weights_only=True)
    right = torch.load(tmp_path / "changed/bundles/transformer/weights.pt", weights_only=True)
    assert all(torch.equal(left[name], right[name]) for name in left)
    changed_infer = infer.copy()
    changed_infer["target"] = -1e12
    np.testing.assert_array_equal(
        base, predict_bundle(tmp_path / "changed", other, changed_infer, sequence_context=changed),
    )


def test_transformer_rejects_weight_and_model_mismatch(tmp_path):
    import torch

    train, valid, test, windows, candidate = inputs()
    row = fit(tmp_path, train, valid, windows, candidate)
    config = json.loads((tmp_path / row["config_path"]).read_text(encoding="utf-8"))
    path = tmp_path / config["weights_path"]
    weights = torch.load(path, weights_only=True)
    weights[next(iter(weights))].add_(1)
    torch.save(weights, path)
    with pytest.raises(QlibModelError, match="权重"):
        predict_bundle(tmp_path, row, test, sequence_context=windows)


@pytest.mark.parametrize("fail_loading", [False, True])
def test_transformer_locked_holdout_trains_before_open_and_consumes_once(tmp_path, monkeypatch, fail_loading):
    import test_qlib_gru_integration as sequence_tests

    monkeypatch.setattr(sequence_tests, "inputs", inputs)
    sequence_tests.test_gru_locked_holdout_trains_before_open_and_consumes_only_once(
        tmp_path, monkeypatch, fail_loading, model_class="TransformerModel",
    )
