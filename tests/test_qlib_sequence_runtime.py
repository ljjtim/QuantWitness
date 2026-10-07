"""序列模型六阶段Runtime及完整末端切分，无数据库访问。"""
from __future__ import annotations

import importlib.util
import json

import numpy as np
import pandas as pd
import pytest

from research_pipeline.platform import canonical_json
from research_pipeline.research.modeling.inputs import ModelMainlineError
from research_pipeline.research.dataframe_budget import PandasFrameBudget, PandasFrameBudgetError
from research_pipeline.runtime.model_sequence import build_runtime_windows
import research_pipeline.runtime.walk_forward_model_execution as runtime
from test_qlib_model_integration import synthetic_samples, candidate, _parameters, _read
from test_qlib_gru_contract import gru_candidate

BUDGET = 256 * 1024**2


def source(root, *, missing=False, late=False):
    samples, sessions = synthetic_samples()
    samples = samples.loc[samples.entity_id.isin(["SYN_00", "SYN_01", "SYN_02"])].copy()
    features = []
    for row in samples.to_dict("records"):
        for name in ("x1", "x2"):
            if missing and row["entity_id"] == "SYN_01" and row["observation_session"] == sessions[20]:
                continue
            features.append({"entity_id": row["entity_id"], "observation_session": row["observation_session"],
                "observation_time": row["observation_time"], "available_time": row["feature_available_time"],
                "window_sessions": 1, "feature_id": name, "value": row[name], "status": "ok", "lineage_hash": "1" * 64})
    features = pd.DataFrame(features)
    if late:
        delayed = (features.entity_id == "SYN_01") & features.observation_session.isin([sessions[20], sessions[60]])
        features.loc[delayed, "available_time"] += pd.Timedelta(days=3)
    labels = samples.loc[:, ["entity_id", "observation_session", "decision_time", "label_start_time", "label_end_time", "label_available_time", "horizon_sessions", "target"]].rename(columns={"label_available_time":"available_time", "target":"forward_return"})
    labels["lineage_hash"] = "2" * 64
    for name, frame in (("features", features), ("labels", labels)):
        runtime._write_artifact(root / name, {name:frame}, status="ok", extra={"semantics_hash":"3"*64})
        if name == "labels":
            frame.to_parquet(root/name/name/"part-00000.parquet", index=False, row_group_size=1)
    parameters = {"holdout_start":str(sessions[60])+"T00:00:00Z", "horizon_sessions":1,
        "target_field":"forward_return", "calendar_sessions":[str(day) for day in sessions],
        "train_sessions":30, "validation_sessions":10, "test_sessions":8, "step_sessions":8,
        "embargo_sessions":0, "expanding":True, "sequence_step_len":3}
    return features, labels, parameters


def split(root, parameters):
    return runtime.execute_model_split_artifact(feature_root=root/"features", label_root=root/"labels",
        output_root=root/"split", parameters=parameters, fixed_clock="2024-06-01T00:00:00Z", max_memory_bytes=BUDGET)


def parameters():
    p = _parameters()
    spec = gru_candidate()
    spec["processors"]["infer"] = candidate()["processors"]["infer"]
    p["candidate_jsons"] = [canonical_json(spec), canonical_json(candidate())]
    return p


def test_sequence_split_filters_endpoints_but_retains_purge_history(tmp_path, monkeypatch):
    features, labels, p = source(tmp_path, missing=True)
    seen = []
    original = runtime._load_label_slice
    def track(*args, **kwargs):
        seen.append(kwargs["columns"])
        return original(*args, **kwargs)
    monkeypatch.setattr(runtime, "_load_label_slice", track)
    meta = split(tmp_path, p)
    excluded = _read(tmp_path/"split", "sequence_exclusions")
    samples = _read(tmp_path/"split", "samples")
    assert {"insufficient_history", "missing_feature"} <= set(excluded.reason_code)
    assert not set(samples.sample_id) & set(excluded.sample_id)
    missing_id = "SYN_01:" + str(pd.bdate_range("2024-01-02", periods=21)[20].date()) + ":h1"
    assert missing_id in set(excluded.sample_id)
    context = _read(tmp_path/"split", "sequence_context")
    assert "target" not in context and "forward_return" not in context
    assert context.observation_session.min() < samples.observation_session.min()
    assert "forward_return" not in seen[1]
    assert meta["sequence"]["step_len"] == 3
    assert set(meta["sequence"]["calendar_sessions"]) == set(p["calendar_sessions"])


def test_sequence_window_budget_refuses_before_build(monkeypatch):
    import research_pipeline.runtime.model_sequence as module
    monkeypatch.setattr(module, "build_sequence_windows", lambda *a, **k: pytest.fail("预算不足不得构建"))
    with pytest.raises(PandasFrameBudgetError, match="工作集"):
        build_runtime_windows(pd.DataFrame({"x":[1]}), pd.DataFrame({"sample_id":["a"]}),
            calendar=[], columns=[], step_len=10, budget=PandasFrameBudget(1))


@pytest.mark.parametrize("step", [None, 4])
def test_sequence_candidate_requires_matching_split_before_fit(tmp_path, monkeypatch, step):
    _, _, p = source(tmp_path)
    if step is None: del p["sequence_step_len"]
    else: p["sequence_step_len"] = step
    split(tmp_path, p)
    monkeypatch.setattr(runtime, "fit_bundle", lambda *a, **k: pytest.fail("声明不匹配不得训练"))
    with pytest.raises(ModelMainlineError, match="sequence_step_len"):
        runtime.execute_model_fit_artifact(split_root=tmp_path/"split", parameters=parameters(),
            output_root=tmp_path/"fit", root_seed=7, max_memory_bytes=BUDGET)


@pytest.mark.skipif(importlib.util.find_spec("torch") is None, reason="需要ml-sequence可选依赖")
@pytest.mark.parametrize("final_model", ["GRU", "LinearModel"])
def test_gru_runtime_six_stages_and_holdout_consumption(tmp_path, monkeypatch, final_model):
    _, _, p = source(tmp_path, late=final_model == "LinearModel")
    split(tmp_path, p)
    params = parameters()
    runtime.execute_model_fit_artifact(split_root=tmp_path/"split", parameters=params,
        output_root=tmp_path/"fit", root_seed=7, max_memory_bytes=BUDGET)
    models = _read(tmp_path/"fit", "models")
    assert len(set(models.candidate_id)) == 2 and set(models.status) == {"fitted"}
    for fold_id, rows in models.groupby("fold_id"):
        configs = [json.loads((tmp_path/"fit"/row.config_path).read_text()) for row in rows.itertuples()]
        assert configs[0]["train_ids"] == configs[1]["train_ids"]
        assert configs[0]["valid_ids"] == configs[1]["valid_ids"]
    runtime.execute_model_predict_artifact(split_root=tmp_path/"split", model_root=tmp_path/"fit",
        output_root=tmp_path/"predict", max_memory_bytes=BUDGET)
    runtime.execute_model_fold_metrics_artifact(prediction_root=tmp_path/"predict", model_root=tmp_path/"fit",
        parameters=params, output_root=tmp_path/"metrics", max_memory_bytes=BUDGET)
    runtime.execute_model_selection_artifact(metrics_root=tmp_path/"metrics", split_root=tmp_path/"split",
        model_root=tmp_path/"fit", parameters=params, output_root=tmp_path/"selection", max_memory_bytes=BUDGET)
    assert np.isfinite(_read(tmp_path/"selection", "test_predictions").prediction).all()
    selection_root = tmp_path/"selection"
    if final_model == "GRU":
        # 只含GRU的搜索覆盖最终神经网络重训；混合搜索另验收表格赢家。
        gru_root = tmp_path/"gru_fit"
        params["candidate_jsons"] = params["candidate_jsons"][:1]
        runtime.execute_model_fit_artifact(split_root=tmp_path/"split", parameters=params,
            output_root=gru_root, root_seed=7, max_memory_bytes=BUDGET)
        runtime.execute_model_predict_artifact(split_root=tmp_path/"split", model_root=gru_root,
            output_root=tmp_path/"gru_predict", max_memory_bytes=BUDGET)
        runtime.execute_model_fold_metrics_artifact(prediction_root=tmp_path/"gru_predict", model_root=gru_root,
            parameters=params, output_root=tmp_path/"gru_metrics", max_memory_bytes=BUDGET)
        selection_root = tmp_path/"gru_selection"
        runtime.execute_model_selection_artifact(metrics_root=tmp_path/"gru_metrics", split_root=tmp_path/"split",
            model_root=gru_root, parameters=params, output_root=selection_root, max_memory_bytes=BUDGET)
    selected = json.loads(_read(selection_root, "selection").iloc[0].selected_parameters_json)
    assert selected["model"]["class"] == final_model
    params.update(package_hash="4"*64, implementation_hash="5"*64, holdout_actor="test",
        holdout_reason="序列验收", holdout_unlock_at="2024-06-01T00:00:00Z", fixed_clock="2024-06-02T00:00:00Z")
    original = runtime._load_table
    def track(root, name, **kwargs):
        if name in {"features", "labels"}:
            assert list((tmp_path/"ledger").glob("*/opened.json"))
        return original(root, name, **kwargs)
    monkeypatch.setattr(runtime, "_load_table", track)
    kwargs = dict(split_root=tmp_path/"split", selection_root=selection_root,
        feature_root=tmp_path/"features", label_root=tmp_path/"labels", parameters=params,
        output_root=tmp_path/"holdout", ledger_root=tmp_path/"ledger", root_seed=7, max_memory_bytes=BUDGET)
    result = runtime.execute_model_locked_holdout_artifact(**kwargs)
    assert result["status"] == "model_locked_holdout_succeeded"
    assert set(_read(tmp_path/"holdout", "holdout_predictions").sample_id) == set(_read(tmp_path/"split", "holdout_index").sample_id)
    if final_model == "LinearModel":
        assert len(_read(tmp_path/"holdout", "holdout_predictions")) < 30
    from research_pipeline.research.validation import ValidationError
    with pytest.raises(ValidationError): runtime.execute_model_locked_holdout_artifact(**kwargs)


@pytest.mark.parametrize("scope", ["development", "final"])
def test_late_endpoint_is_excluded_and_recorded_before_sample_assembly(tmp_path, scope):
    features, _, p = source(tmp_path)
    day = sorted(features.observation_session.unique())[20]
    chosen = (features.entity_id == "SYN_01") & (features.observation_session == day)
    features.loc[chosen, "available_time"] += pd.Timedelta(days=3)
    runtime._write_artifact(tmp_path/"features", {"features":features}, status="ok", extra={"semantics_hash":"3"*64})
    p["evaluation_scope"] = scope
    split(tmp_path, p)
    excluded = _read(tmp_path/"split", "sequence_exclusions")
    sid = "SYN_01:" + str(day) + ":h1"
    assert excluded.set_index("sample_id").loc[sid, "reason_code"] == "feature_not_visible"
    assert sid not in set(_read(tmp_path/"split", "samples").sample_id)
    if scope == "development":
        assert _read(tmp_path/"split", "holdout_index").empty
        context = _read(tmp_path/"split", "sequence_context")
        assert context.observation_session.max() < pd.Timestamp(p["holdout_start"]).date()


def test_sequence_artifact_drift_is_rejected_before_model_construction(tmp_path, monkeypatch):
    _, _, p = source(tmp_path)
    split(tmp_path, p)
    file = tmp_path/"split/sequence_context/part-00000.parquet"
    frame = pd.read_parquet(file)
    frame.loc[0, "x1__w1"] += 1
    frame.to_parquet(file, index=False)
    monkeypatch.setattr(runtime, "fit_bundle", lambda *a, **k: pytest.fail("工件漂移不得训练"))
    with pytest.raises(ModelMainlineError, match="漂移"):
        runtime.execute_model_fit_artifact(split_root=tmp_path/"split", parameters=parameters(),
            output_root=tmp_path/"fit", root_seed=7, max_memory_bytes=BUDGET)


def test_gru_sampler_budget_accounts_for_dense_dates_and_securities(tmp_path):
    from research_pipeline.runtime.model_sequence import load_runtime_windows, sequence_for_candidate
    _, _, p = source(tmp_path)
    metadata = split(tmp_path, p)
    windows = load_runtime_windows(tmp_path/"split", metadata, PandasFrameBudget(BUDGET), runtime._load_table)
    with pytest.raises(PandasFrameBudgetError, match="GRU训练工作集"):
        sequence_for_candidate(windows, gru_candidate(), PandasFrameBudget(1))
