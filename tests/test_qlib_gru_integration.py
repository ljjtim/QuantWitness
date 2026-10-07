"""真实 CPU GRU、冻结处理器、无标签预测和文件恢复验收。"""
from __future__ import annotations

from dataclasses import replace
import json
import importlib.util
import shutil

import numpy as np
import pandas as pd
import pytest

from research_pipeline.research.modeling.qlib import fit_bundle, predict_bundle, normalize_candidates, QlibModelError
from research_pipeline.research.modeling.sequence import build_sequence_windows
from test_qlib_gru_contract import gru_candidate
from test_qlib_model_integration import synthetic_samples

pytestmark = pytest.mark.skipif(importlib.util.find_spec("torch") is None, reason="需要 ml-sequence 可选依赖")


def inputs(model_class="GRU"):
    samples, calendar = synthetic_samples()
    samples = samples.loc[samples.entity_id.isin(["SYN_00", "SYN_01", "SYN_02"])].copy()
    samples = samples.rename(columns={"x1":"x1__w1", "x2":"x2__w1"})
    raw = []
    for row in samples.to_dict("records"):
        for name in ("x1", "x2"):
            raw.append(dict(entity_id=row["entity_id"], observation_session=row["observation_session"],
                observation_time=row["observation_time"], available_time=row["feature_available_time"],
                window_sessions=1, feature_id=name, value=row[f"{name}__w1"], status="ok", lineage_hash=f'{row["sample_id"]}:{name}'))
    windows = build_sequence_windows(pd.DataFrame(raw), samples, calendar_sessions=calendar,
                                    feature_columns=("x1__w1", "x2__w1"), step_len=3)
    spec = gru_candidate()
    if model_class == "LSTM":
        spec["model"].update({"class": "LSTM", "module_path": "qlib.contrib.model.pytorch_lstm_ts"})
    spec["processors"]["infer"] = [{"class":"RobustZScoreNorm", "kwargs":{"fields_group":"feature"}},
                                      {"class":"Fillna", "kwargs":{"fields_group":"feature", "fill_value":0}}]
    train = samples.iloc[6:36].copy()
    valid = samples.iloc[39:48].copy()
    test = samples.iloc[51:56].copy()
    return train, valid, test, windows, spec


def fit(root, train, valid, windows, spec):
    return fit_bundle(train, valid, candidate=spec, feature_columns=("x1__w1", "x2__w1"),
                      output_root=root, bundle_path="bundles/gru", root_seed=7,
                      fit_scope_ref="train-only", sequence_context=windows)


def test_cpu_gru_round_trip_one_sample_prediction_and_no_target_access(tmp_path, monkeypatch):
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
    train, valid, test, windows, spec = inputs()
    numpy_state, torch_state, threads = np.random.get_state(), torch.get_rng_state(), torch.get_num_threads()
    row = fit(tmp_path, train, valid, windows, spec)
    assert prepared == ["train", "valid"]
    assert torch.get_num_threads() == threads and torch.equal(torch.get_rng_state(), torch_state)
    after = np.random.get_state()
    assert after[0] == numpy_state[0] and np.array_equal(after[1], numpy_state[1]) and after[2:] == numpy_state[2:]
    prediction = predict_bundle(tmp_path, row, test.drop(columns=["target", "label_available_time", "label_start_time", "label_end_time"]), sequence_context=windows)
    assert len(prediction) == 5 and np.isfinite(prediction).all()
    single = predict_bundle(tmp_path, row, test.iloc[:1], sequence_context=windows)
    assert single.shape == (1,) and single[0] == prediction[0]
    config = json.loads((tmp_path / row["config_path"]).read_text())
    assert config["schema"] == "research.qlib-sequence-model-bundle.v1"
    assert config["versions"]["torch"].split("+")[0] == "2.5.1"
    assert config["effective_model_kwargs"]["GPU"] == -1
    assert config["effective_model_kwargs"]["d_feat"] == 2
    assert config["effective_fit_kwargs"] == {"save_path":"bundles/gru/weights.pt"}
    assert config["training_curve"] and {r["metric"] for r in config["training_curve"]} == {"negative_mse"}
    assert all(r["value"] <= 0 for r in config["training_curve"])
    stored_context = pd.read_parquet(tmp_path / config["sequence"]["files"]["context"])
    stored_targets = pd.read_parquet(tmp_path / config["sequence"]["files"]["targets"])
    assert stored_context.observation_session.max() == valid.observation_session.max()
    assert "target" not in stored_context and "target" not in stored_targets
    assert set(stored_targets.sample_id) == set(train.sample_id) | set(valid.sample_id)
    # 处理器保存的拟合统计仅来自 train，而非窗口预热行或 valid。
    from qlib.data.dataset.processor import RobustZScoreNorm
    from research_pipeline.research.modeling.qlib import _matrix
    tr = _matrix(train, windows.feature_columns, label=True)
    direct = RobustZScoreNorm(fields_group="feature", fit_start_time=tr.index.get_level_values(0).min(), fit_end_time=tr.index.get_level_values(0).max())
    direct.fit(tr)
    saved = Serializable.load(tmp_path / config["processor_files"]["infer"][0])
    pd.testing.assert_frame_equal(saved(tr.copy()), direct(tr.copy()))
    relocated = tmp_path / "relocated"
    shutil.copytree(tmp_path / "bundles", relocated / "bundles")
    changed = test.copy()
    changed["target"] = 1e15
    np.testing.assert_array_equal(prediction, predict_bundle(relocated, row, changed, sequence_context=windows))
    assert set(prepared) == {"train", "valid", "test"}


@pytest.mark.parametrize("model_class", ["GRU", "LSTM"])
def test_direct_qlib_same_weights_and_predictions_including_single_batch(tmp_path, model_class):
    import torch
    from qlib.contrib.model.pytorch_gru_ts import GRU
    from qlib.contrib.model.pytorch_lstm_ts import LSTM
    from qlib.data.dataset import TSDatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    from qlib.utils.serial import Serializable
    from research_pipeline.research.modeling.gru import cpu_execution
    from research_pipeline.research.modeling.qlib import _matrix
    train, valid, test, windows, spec = inputs(model_class)
    spec["processors"]["infer"] = []
    # 对照使用无缺失第二特征，直接构造上游DataHandler，不复用生产数据集构建器。
    for frame in (train, valid, test): frame["x2__w1"] = frame["x2__w1"].fillna(0)
    windows.context["x2__w1"] = windows.context["x2__w1"].fillna(0)
    row = fit(tmp_path / "managed", train, valid, windows, spec)
    config = json.loads((tmp_path / "managed" / row["config_path"]).read_text())
    context = windows.context
    index = pd.MultiIndex.from_arrays([pd.to_datetime(context.observation_session), context.entity_id], names=["datetime", "instrument"])
    data = pd.DataFrame(context[list(windows.feature_columns)].to_numpy(float), index=index,
                        columns=pd.MultiIndex.from_product([["feature"], windows.feature_columns]))
    data[("label", "target")] = np.nan
    data[("filter", "endpoint")] = False
    endpoint = _matrix(pd.concat([train, valid]), windows.feature_columns, label=True)
    data.loc[endpoint.index, ("label", "target")] = endpoint[("label", "target")]
    data.loc[endpoint.index, ("filter", "endpoint")] = True
    handler = DataHandlerLP(data_loader=StaticDataLoader(data.sort_index()), infer_processors=[], learn_processors=[])
    dataset = TSDatasetH(handler=handler, step_len=3, flt_col="filter", segments={
        "train":(pd.Timestamp(train.observation_session.min()), pd.Timestamp(train.observation_session.max())),
        "valid":(pd.Timestamp(valid.observation_session.min()), pd.Timestamp(valid.observation_session.max()))})
    # 只调整单条输出形状，训练仍执行上游 TS 模型的 fit。
    def shape_hook(module, args, output): return torch.atleast_1d(output)
    with cpu_execution():
        direct = {"GRU": GRU, "LSTM": LSTM}[model_class](**config["effective_model_kwargs"])
        direct_network = getattr(direct, model_class + "_model")
        direct_network.register_forward_hook(shape_hook)
        direct.fit(dataset, save_path=str(tmp_path / "direct.pt"), evals_result={})
        managed = Serializable.load(tmp_path / "managed" / row["model_path"])
        for name, values in direct_network.state_dict().items():
            torch.testing.assert_close(values, getattr(managed, model_class + "_model").state_dict()[name], rtol=0, atol=0)
        # 独立手取每个证券的三个历史会话送入直接 Qlib 网络。
        expected = []
        for record in test.to_dict("records"):
            part = context.loc[(context.entity_id == record["entity_id"]) & (context.observation_session <= record["observation_session"])].sort_values("observation_session").tail(3)
            with torch.no_grad():
                value = direct_network(torch.tensor(part[list(windows.feature_columns)].to_numpy(float)[None, :, :], dtype=torch.float32))
            expected.append(value.item())
    np.testing.assert_array_equal(expected, predict_bundle(tmp_path / "managed", row, test, sequence_context=windows))


@pytest.mark.parametrize("change", ["step", "calendar", "decision", "entity", "available", "value"])
def test_gru_rejects_changed_prediction_context(tmp_path, change):
    train, valid, test, windows, spec = inputs()
    row = fit(tmp_path, train, valid, windows, spec)
    sid = test.iloc[0].sample_id
    if change == "step": windows = replace(windows, step_len=4)
    elif change == "calendar": windows = replace(windows, calendar_sessions=windows.calendar_sessions[:-1])
    elif change == "decision": windows.targets.loc[windows.targets.sample_id == sid, "decision_time"] += pd.Timedelta(minutes=1)
    elif change == "entity": windows.members.loc[windows.members.sample_id == sid, "entity_id"] = "OTHER"
    elif change == "available": windows.members.loc[windows.members.sample_id == sid, "feature_available_time"] += pd.Timedelta(minutes=1)
    else: test.loc[test.sample_id == sid, "x1__w1"] += 1
    with pytest.raises(QlibModelError): predict_bundle(tmp_path, row, test, sequence_context=windows)


def test_gru_rejects_weight_and_model_mismatch(tmp_path):
    import torch
    train, valid, test, windows, spec = inputs()
    row = fit(tmp_path, train, valid, windows, spec)
    path = tmp_path / "bundles/gru/weights.pt"
    weights = torch.load(path, weights_only=True)
    weights[next(iter(weights))].add_(1)
    torch.save(weights, path)
    with pytest.raises(QlibModelError, match="权重"):
        predict_bundle(tmp_path, row, test, sequence_context=windows)


def test_gru_fails_before_bundle_write_if_context_or_d_feat_invalid(tmp_path):
    train, valid, test, windows, spec = inputs()
    with pytest.raises(QlibModelError, match="sequence_context"):
        fit(tmp_path / "no-context", train, valid, None, spec)
    assert not (tmp_path / "no-context").exists()
    spec["model"]["kwargs"]["d_feat"] = 6
    with pytest.raises(QlibModelError, match="d_feat"):
        fit(tmp_path / "wrong-features", train, valid, windows, spec)
    assert not (tmp_path / "wrong-features").exists()


def test_gru_cpu_context_restores_state_on_training_exception(tmp_path, monkeypatch):
    import torch
    from qlib.contrib.model.pytorch_gru_ts import GRU
    train, valid, test, windows, spec = inputs()
    state, threads = torch.get_rng_state(), torch.get_num_threads()
    def failed_fit(*args, **kwargs): raise RuntimeError("training failed")
    monkeypatch.setattr(GRU, "fit", failed_fit)
    with pytest.raises(RuntimeError, match="training failed"): fit(tmp_path, train, valid, windows, spec)
    assert torch.equal(state, torch.get_rng_state()) and torch.get_num_threads() == threads
    assert not (tmp_path / "bundles/gru/config.json").exists()


@pytest.mark.parametrize("mutation", ["batch_size", "cross_sectional"])
def test_gru_rejects_unsupported_tail_batches_or_feature_universe(mutation):
    spec = gru_candidate()
    if mutation == "batch_size": spec["model"]["kwargs"]["batch_size"] = 2
    else: spec["processors"]["infer"] = [{"class":"CSZScoreNorm", "kwargs":{"fields_group":"feature"}}]
    with pytest.raises(QlibModelError): normalize_candidates([spec])

@pytest.mark.parametrize("model_class", ["GRU", "LSTM"])
@pytest.mark.parametrize("fail_loading", [False, True])
def test_gru_locked_holdout_trains_before_open_and_consumes_only_once(tmp_path, monkeypatch, fail_loading, model_class):
    from datetime import datetime, timezone
    import research_pipeline.research.modeling.qlib as models
    from research_pipeline.research.modeling.walk_forward import evaluate_locked_holdout
    from research_pipeline.research.validation import ValidationError
    train, valid, holdout, windows, spec = inputs(model_class)
    development = pd.concat([train, valid])
    ledger = tmp_path / "ledger"
    output = tmp_path / "final"
    fit_calls, load_calls = [], []
    original_fit = models.fit_bundle
    def tracked_fit(*args, **kwargs):
        assert not list(ledger.glob("*/opened.json"))
        fit_calls.append(1)
        return original_fit(*args, **kwargs)
    monkeypatch.setattr(models, "fit_bundle", tracked_fit)
    def load_labels():
        assert len(fit_calls) == 1 and list(ledger.glob("*/opened.json"))
        load_calls.append("labels")
        return holdout.copy()
    def load_windows():
        assert load_calls == ["labels"] and list(ledger.glob("*/opened.json"))
        load_calls.append("windows")
        if fail_loading: raise RuntimeError("window unavailable")
        return windows
    arguments = dict(development_samples=development, holdout_preflight=lambda:{"format":"dataframe"},
        holdout_loader=load_labels, development_ids=development.sample_id.tolist(), holdout_ids=holdout.sample_id.tolist(),
        holdout_start=str(holdout.observation_session.min()), holdout_end=str(holdout.observation_session.max()),
        feature_columns=windows.feature_columns, selected_candidate=spec, target_kind="regression", objective="neg_mean_squared_error",
        validation_sessions=3, output_root=output, research_identity_hash="2"*64, data_snapshot_hash="3"*64,
        selection_hash="4"*64, package_hash="5"*64, implementation_hash="6"*64, actor="framework", reason="locked_holdout_primary",
        unlock_at=datetime(2024,1,1,tzinfo=timezone.utc), fixed_clock=datetime(2024,6,1,tzinfo=timezone.utc),
        ledger_root=ledger, root_seed=7, development_sequence_context=windows, holdout_sequence_loader=load_windows)
    if fail_loading:
        with pytest.raises(RuntimeError, match="window unavailable"): evaluate_locked_holdout(**arguments)
    else:
        result = evaluate_locked_holdout(**arguments)
        assert result["status"] == "committed" and len(result["predictions"]) == len(holdout)
    terminal = json.loads(next(ledger.glob("*/terminal.json")).read_text())
    assert terminal["status"] == ("consumed_failed" if fail_loading else "committed")
    config = json.loads((output / "bundles/holdout/0/config.json").read_text())
    assert not set(holdout.sample_id) & set(config["train_ids"] + config["valid_ids"])
    with pytest.raises(ValidationError): evaluate_locked_holdout(**arguments)
    assert fit_calls == [1] and load_calls == ["labels", "windows"]


@pytest.mark.parametrize("model_class", ["GRU", "LSTM"])
def test_future_context_and_labels_do_not_change_fit_or_development_prediction(tmp_path, model_class):
    import torch
    train, valid, test, windows, spec = inputs(model_class)
    row = fit(tmp_path / "base", train, valid, windows, spec)
    base = predict_bundle(tmp_path / "base", row, test, sequence_context=windows)
    changed = replace(windows, context=windows.context.copy(), targets=windows.targets.copy())
    # 测试末端之后的任意特征不能影响拟合、处理器统计或该末端预测。
    changed.context.loc[changed.context.observation_session > test.observation_session.max(), "x1__w1"] = 1e12
    other = fit(tmp_path / "changed", train, valid, changed, spec)
    left = torch.load(tmp_path / "base/bundles/gru/weights.pt", weights_only=True)
    right = torch.load(tmp_path / "changed/bundles/gru/weights.pt", weights_only=True)
    assert all(torch.equal(left[name], right[name]) for name in left)
    test["target"] = -1e12
    np.testing.assert_array_equal(base, predict_bundle(tmp_path / "changed", other, test, sequence_context=changed))


def test_gru_bundle_works_without_explicit_d_feat_and_preserves_rank_label_scope(tmp_path):
    from research_pipeline.research.modeling.qlib import evaluation_labels
    train, valid, test, windows, spec = inputs()
    del spec["model"]["kwargs"]["d_feat"]
    spec["processors"]["learn"].append({"class":"CSRankNorm", "kwargs":{}})
    row = fit(tmp_path, train, valid, windows, spec)
    config = json.loads((tmp_path / row["config_path"]).read_text())
    assert config["effective_model_kwargs"]["d_feat"] == len(windows.feature_columns)
    assert config["training_label"] == "cross_sectional_rank"
    before = test.target.copy()
    rank = evaluation_labels(tmp_path, row, test)
    assert np.isfinite(rank).all()
    pd.testing.assert_series_equal(test.target, before)
    assert np.isfinite(predict_bundle(tmp_path, row, test, sequence_context=windows)).all()
