"""Qlib 表格模型、冻结处理器及六节点无数据库合成验收。"""
from __future__ import annotations
import json
from pathlib import Path
import shutil
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import pytest

from research_pipeline.research.modeling.qlib import fit_bundle, predict_bundle, normalize_candidates, QlibModelError
from research_pipeline.platform import canonical_json
from research_pipeline.research.validation import build_walk_forward
import research_pipeline.runtime.walk_forward_model_execution as runtime

BUDGET = 128 * 1024**2


def synthetic_samples():
    sessions = pd.bdate_range("2024-01-02", periods=71, tz="UTC")
    rows=[]
    for i, session in enumerate(sessions[:70]):
        for j in range(10):
            x1 = -1.5 + 3*i/69 + 0.1*j
            decision = session + pd.Timedelta(hours=15)
            end = sessions[i+1] + pd.Timedelta(hours=16)
            rows.append(dict(sample_id=f"s{i:02d}_{j:02d}", entity_id=f"SYN_{j:02d}",
                observation_session=session.date(), observation_time=decision, feature_available_time=decision,
                decision_time=decision, label_start_time=decision, label_end_time=end, label_available_time=end,
                horizon_sessions=1, target=0.6*x1 - 0.2*np.sin(i+j), x1=x1,
                x2=np.nan if (i,j) in {(2,0),(19,1),(33,2)} else np.cos(i/4+j)))
    return pd.DataFrame(rows), tuple(s.date() for s in sessions[:70])


def candidate(name="LinearModel", *, alpha=0.1, rank=False):
    spec={"candidate_id":f"{name}-{alpha}", "model":{"class":name,"module_path":{
        "LinearModel":"qlib.contrib.model.linear", "LGBModel":"qlib.contrib.model.gbdt",
        "XGBModel":"qlib.contrib.model.xgboost", "DEnsembleModel":"qlib.contrib.model.double_ensemble"}[name],"kwargs":{}},
        "processors":{"infer":[],"learn":[{"class":"DropnaLabel","kwargs":{}}]}, "fit":{}}
    if name == "LinearModel":
        spec["model"]["kwargs"]={"estimator":"ridge","alpha":alpha,"fit_intercept":True,"include_valid":False}
        spec["processors"]["infer"]=[{"class":"RobustZScoreNorm","kwargs":{"fields_group":"feature"}},
            {"class":"Fillna","kwargs":{"fields_group":"feature","fill_value":0}}]
    elif name == "LGBModel":
        spec["model"]["kwargs"]={"num_leaves":5,"min_data_in_leaf":5,"learning_rate":0.1}
        spec["fit"]={"num_boost_round":8,"early_stopping_rounds":3,"verbose_eval":0}
    elif name == "DEnsembleModel":
        spec["model"]["kwargs"]={"num_models":2,"enable_sr":True,"enable_fs":False,
            "decay":0.9,"epochs":4,"bins_sr":3,"num_leaves":5,"min_data_in_leaf":5,"verbosity":-1}
    else:
        spec["model"]["kwargs"]={"max_depth":2,"eta":0.1}
        spec["fit"]={"num_boost_round":8,"early_stopping_rounds":None,"verbose_eval":False}
    if rank:
        spec["processors"]["learn"].append({"class":"CSRankNorm","kwargs":{}})
    return spec


@pytest.mark.parametrize("name", ["LinearModel","LGBModel","XGBModel","DEnsembleModel"])
def test_three_models_frozen_bundle_round_trip_and_future_labels(tmp_path, name, monkeypatch):
    import sqlite3
    def reject_database(*args, **kwargs):
        raise AssertionError("合成模型测试不得连接数据库")
    monkeypatch.setattr(sqlite3, "connect", reject_database)
    samples,_=synthetic_samples()
    train=samples.iloc[:390].copy()
    valid=samples.iloc[400:490].copy()
    test=samples.iloc[500:600].copy()
    root=tmp_path/name
    row=fit_bundle(train,valid,candidate=candidate(name), feature_columns=("x1","x2"),
        output_root=root,bundle_path="bundles/0/0",root_seed=7,fit_scope_ref="train-ids")
    prediction=predict_bundle(root,row,test)
    relocated=tmp_path/(name+"-restored")
    shutil.copytree(root,relocated)
    changed=test.copy()
    changed["target"]=1e12
    np.testing.assert_array_equal(prediction,predict_bundle(relocated,row,changed))
    assert len(prediction)==100
    config=json.loads((root/row["config_path"]).read_text())
    assert pd.Timestamp(config["fit_time"]) >= valid.label_available_time.max()
    assert config["train_ids"]==train.sample_id.tolist()
    assert not list(root.rglob("*.db")) and not list(root.rglob("*.sqlite"))
    from qlib.utils.serial import Serializable
    restored=Serializable.load(root/row["model_path"])
    if name=="XGBModel":
        assert restored._params["nthread"]==1


def test_processors_fit_only_train_and_rank_preserves_raw_labels(tmp_path):
    samples,_=synthetic_samples()
    train=samples.iloc[:390].copy(); valid=samples.iloc[400:490].copy()
    original=train.target.copy()
    row=fit_bundle(train,valid,candidate=candidate(rank=True),feature_columns=("x1","x2"),
        output_root=tmp_path,bundle_path="bundles/0/0",root_seed=7,fit_scope_ref="train-only")
    pd.testing.assert_series_equal(original,train.target)
    config=json.loads((tmp_path/row["config_path"]).read_text())
    from qlib.utils.serial import Serializable
    processor=Serializable.load(tmp_path/config["processor_files"]["infer"][0])
    np.testing.assert_allclose(processor.mean_train,np.nanmedian(train[["x1","x2"]].to_numpy(),axis=0))
    assert config["training_label"]=="cross_sectional_rank"
    ranker=Serializable.load(tmp_path/config["processor_files"]["learn"][1])
    assert ranker.fields_group=="label"
    from research_pipeline.research.modeling.qlib import evaluation_labels
    labels=evaluation_labels(tmp_path,row,valid)
    assert not np.allclose(labels,valid.target)
    assert np.isfinite(labels).all()
    with pytest.raises(QlibModelError,match="口径必须一致"):
        normalize_candidates([candidate(rank=True),candidate(alpha=10.0)])
    changed=valid.copy(); changed[["x1","x2"]]=1e9
    other=fit_bundle(train,changed,candidate=candidate(rank=True),feature_columns=("x1","x2"),
        output_root=tmp_path,bundle_path="bundles/1/0",root_seed=7,fit_scope_ref="train-only")
    changed_config=json.loads((tmp_path/other["config_path"]).read_text())
    changed_proc=Serializable.load(tmp_path/changed_config["processor_files"]["infer"][0])
    np.testing.assert_array_equal(processor.mean_train,changed_proc.mean_train)


def test_rejects_future_training_labels_and_unsupported_configuration(tmp_path):
    samples,_=synthetic_samples(); train=samples.iloc[:390].copy();valid=samples.iloc[400:490].copy()
    train.loc[0,"label_available_time"]=pd.Timestamp("2025-01-01",tz="UTC")
    with pytest.raises(QlibModelError,match="尚不可见"):
        fit_bundle(train,valid,candidate=candidate(),feature_columns=("x1","x2"),output_root=tmp_path,
            bundle_path="bundles/0/0",root_seed=1,fit_scope_ref="x")
    spec=candidate();spec["model"]["kwargs"]["include_valid"]=True
    with pytest.raises(QlibModelError,match="include_valid"):
        normalize_candidates([spec])


def _parameters():
    return {"candidate_jsons":[canonical_json(candidate(alpha=a)) for a in (0.1,10.0)],
        "research_identity_hash":"1"*64,"target_kind":"regression","objective":"neg_mean_squared_error",
        "direction":"maximize","search_id":"qlib-contract","search_frozen_at":"2023-12-01T00:00:00Z",
        "thread_count":1}


def _read(root,name):
    return pd.concat([pd.read_parquet(p) for p in sorted((root/name).glob("*.parquet"))],ignore_index=True)


@pytest.mark.parametrize("model_name", ["LinearModel", "DEnsembleModel"])
def test_split_fit_predict_metrics_selection_and_locked_holdout(tmp_path, model_name):
    samples,sessions=synthetic_samples()
    development=samples.iloc[:590].copy()
    split=build_walk_forward(development.rename(columns={"label_start_time":"label_start","label_end_time":"label_end"}),
        calendar=sessions[:59],train_sessions=30,validation_sessions=10,test_sessions=8,step_sessions=8,
        embargo_sessions=0,expanding=True)
    runtime._write_artifact(tmp_path/"split",{"samples":development,"split_audit":split.audit},
        status="model_split_succeeded",extra={"split_manifest":runtime._split_payload(split),
        "feature_columns":["x1","x2"],"holdout_start":str(sessions[60])+"T00:00:00Z","semantics_hash":"2"*64,
        "validation_sessions":10,"horizon_sessions":1})
    p=_parameters()
    if model_name == "DEnsembleModel":
        p["candidate_jsons"] = [canonical_json(candidate(model_name))]
    runtime.execute_model_fit_artifact(split_root=tmp_path/"split",parameters=p,output_root=tmp_path/"fit",root_seed=7,max_memory_bytes=BUDGET)
    runtime.execute_model_predict_artifact(split_root=tmp_path/"split",model_root=tmp_path/"fit",output_root=tmp_path/"predict",max_memory_bytes=BUDGET)
    runtime.execute_model_fold_metrics_artifact(prediction_root=tmp_path/"predict",model_root=tmp_path/"fit",parameters=p,output_root=tmp_path/"metrics",max_memory_bytes=BUDGET)
    runtime.execute_model_selection_artifact(metrics_root=tmp_path/"metrics",split_root=tmp_path/"split",model_root=tmp_path/"fit",parameters=p,output_root=tmp_path/"selection",max_memory_bytes=BUDGET)
    predictions=_read(tmp_path/"selection","test_predictions")
    assert {"entity_id","observation_session","raw_label","model_ref","processor_ref"} <= set(predictions)
    assert np.isfinite(predictions.prediction).all()
    selected=json.loads((tmp_path/"selection"/"artifact-metadata.json").read_text())["selection"]["selected_parameters"]
    from research_pipeline.research.modeling import evaluate_locked_holdout
    holdout=samples.iloc[600:].copy()
    opened=[]
    def load():
        opened.append(True)
        return holdout
    kwargs=dict(development_samples=development,holdout_preflight=lambda:{"rows":len(holdout)},
        holdout_loader=load,development_ids=development.sample_id.tolist(),holdout_ids=holdout.sample_id.tolist(),
        holdout_start=str(sessions[60]),holdout_end=str(sessions[69]),feature_columns=("x1","x2"),
        selected_candidate=selected,target_kind="regression",objective=p["objective"],validation_sessions=10,
        output_root=tmp_path/"holdout",research_identity_hash="1"*64,data_snapshot_hash="2"*64,selection_hash="3"*64,
        package_hash="4"*64,implementation_hash="5"*64,actor="synthetic-test",reason="组件验收",
        unlock_at=datetime(2024,6,1,tzinfo=timezone.utc),fixed_clock=datetime(2024,6,2,tzinfo=timezone.utc),
        ledger_root=tmp_path/"ledger",root_seed=7)
    result=evaluate_locked_holdout(**kwargs)
    assert len(opened)==1 and len(result["predictions"])==100
    assert all(row["raw_label"]==row["actual"] for row in result["predictions"])
    with pytest.raises(Exception,match="prepared/opened"):
        evaluate_locked_holdout(**kwargs)
    assert len(opened)==1

def _run_daily_chain(root, samples, *, monkeypatch=None, failure=False):
    sessions=tuple(sorted(pd.to_datetime(samples.observation_session).dt.date.unique()))
    development=samples.iloc[:590].copy()
    split=build_walk_forward(development.rename(columns={"label_start_time":"label_start","label_end_time":"label_end"}),
        calendar=sessions[:59],train_sessions=30,validation_sessions=10,test_sessions=8,step_sessions=8,embargo_sessions=0,expanding=True)
    runtime._write_artifact(root/"split",{"samples":development,"split_audit":split.audit},status="model_split_succeeded",
        extra={"split_manifest":runtime._split_payload(split),"feature_columns":["x1","x2"],"holdout_start":str(sessions[60])+"T00:00:00Z",
            "semantics_hash":"2"*64,"validation_sessions":10,"horizon_sessions":1})
    parameters=_parameters()
    if failure:
        original=runtime.fit_bundle
        def fail_future(train,valid,**kwargs):
            if kwargs["candidate"]["model"]["kwargs"]["alpha"]==0.1 and kwargs["bundle_path"].split("/")[1]=="1":
                raise runtime.CandidateFitRejected("该 fold 合格样本不足",reason_code="insufficient_training_samples")
            return original(train,valid,**kwargs)
        monkeypatch.setattr(runtime,"fit_bundle",fail_future)
    runtime.execute_model_fit_artifact(split_root=root/"split",parameters=parameters,output_root=root/"fit",root_seed=7,max_memory_bytes=BUDGET)
    runtime.execute_model_predict_artifact(split_root=root/"split",model_root=root/"fit",output_root=root/"predict",max_memory_bytes=BUDGET)
    runtime.execute_model_fold_metrics_artifact(prediction_root=root/"predict",model_root=root/"fit",parameters=parameters,output_root=root/"metrics",max_memory_bytes=BUDGET)
    runtime.execute_model_selection_artifact(metrics_root=root/"metrics",split_root=root/"split",model_root=root/"fit",parameters=parameters,output_root=root/"selection",max_memory_bytes=BUDGET)
    return split


def test_future_labels_do_not_change_previous_selected_prediction(tmp_path):
    samples,_=synthetic_samples()
    baseline=tmp_path/"baseline";changed=tmp_path/"changed"
    split=_run_daily_chain(baseline,samples)
    future=samples.copy()
    future.loc[future.index>=480,"target"]=future.loc[future.index>=480,"target"]*1000
    _run_daily_chain(changed,future)
    fold=split.folds[0].fold_id
    for table,cols in (("fold_selections",["selected_candidate_id","selection_time","validation_metric"]),
                       ("test_predictions",["sample_id","candidate_id","prediction","model_ref"])):
        before=_read(baseline/"selection",table);after=_read(changed/"selection",table)
        pd.testing.assert_frame_equal(before.loc[before.fold_id==fold,cols].reset_index(drop=True),
            after.loc[after.fold_id==fold,cols].reset_index(drop=True))


def test_future_candidate_failure_is_excluded_only_after_its_fit_time(tmp_path,monkeypatch):
    samples,_=synthetic_samples()
    baseline=tmp_path/"baseline";changed=tmp_path/"changed"
    split=_run_daily_chain(baseline,samples)
    _run_daily_chain(changed,samples,monkeypatch=monkeypatch,failure=True)
    before=_read(baseline/"selection","fold_selections");after=_read(changed/"selection","fold_selections")
    fold=split.folds[0].fold_id
    assert before.loc[before.fold_id==fold,"selected_candidate_id"].iloc[0]==after.loc[after.fold_id==fold,"selected_candidate_id"].iloc[0]
    models=_read(changed/"fit","models")
    failed=set(models.loc[models.status=="failed","candidate_id"])
    assert failed
    aggregate=_read(changed/"metrics","candidate_metrics")
    assert not failed & set(aggregate.candidate_id)
    assert not failed & set(_read(changed/"selection","selection").selected_candidate_id)


def test_unknown_fit_error_propagates_without_success_metadata(tmp_path,monkeypatch):
    samples,_=synthetic_samples()
    def broken(*args,**kwargs):
        raise RuntimeError("模型接口异常")
    monkeypatch.setattr(runtime,"fit_bundle",broken)
    with pytest.raises(RuntimeError,match="模型接口异常"):
        _run_daily_chain(tmp_path,samples)
    assert not (tmp_path/"fit"/"artifact-metadata.json").exists()

def test_real_prediction_result_generates_qlib_html(tmp_path):
    """正式预测表经 Result 封存后驱动报告，不使用手工替代预测表。"""
    import pyarrow.parquet as pq
    from research_pipeline.results import ResultAssembler, ResultSpec, ResultTableSpec
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from research_pipeline.evidence.verification_result import verify_result
    from research_pipeline.evidence.qlib_report import render_qlib_report
    from test_result_bundle import (_runtime_fixture,_spec,_proof,_data_reference,HASH_A,HASH_B,HASH_C,HASH_D,
        VALIDITY_FACTS_PRODUCER_HASH,policy_id_for_claim)
    from test_qlib_report import _request
    samples,_=synthetic_samples()
    producer=tmp_path/"producer"
    _run_daily_chain(producer,samples)
    run_root,result_root,_=_runtime_fixture(tmp_path/"sealed")
    external=ExternalArtifactStore(run_root/"external-artifacts")
    staging=external.prepare()
    shutil.copytree(producer/"predict",staging,dirs_exist_ok=True)
    commit=external.commit(staging,artifact_name="predictions",artifact_type="research.model-validation-predictions.v2")
    path=run_root/"operator-dag-run.json";record=json.loads(path.read_text())
    record["outputs"]["statistics"]["predictions"]=commit.artifact_ref.to_dict()
    path.write_text(canonical_json(record),encoding="utf-8")
    spec=ResultSpec.build((*_spec().tables,ResultTableSpec("model_predictions","diagnostic","statistics","predictions",
        "research.model-validation-predictions.v2","research.qlib-predictions.v2","validation_predictions")))
    bundle,directory,_=ResultAssembler(result_root).finalize(run_root=run_root,project_id="research_package_test",package_hash=HASH_B,
        plan_hash=HASH_C,result_spec=spec,catalog_hashes={"prices":HASH_D},data_references={"prices":_data_reference()},
        metric_proofs=(_proof(),),implementation_manifest_hash=HASH_A,verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH)
    context=verify_result(directory,result_store=result_root)
    predictions=_read(producer/"predict","validation_predictions")
    first=predictions.iloc[0]
    request=_request();request.update(result_id=bundle.result_id,table_id="model_predictions")
    request["selection"].update(candidate_id=first.candidate_id,fold_id=first.fold_id)
    request["window"]={"start":"2024-01-01","end":"2024-06-01"}
    html=render_qlib_report(context,request,verification_summary="合成模型接口验证；不代表真实策略结论")
    assert "Qlib 模型研究报告" in html and "plotly" in html.lower()
    assert bundle.result_id in html
    assert '"rows": 90' in __import__("html").unescape(html)
    assert str(context.verification.status) in html


@pytest.mark.parametrize("name", ["LinearModel", "LGBModel", "XGBModel"])
def test_managed_predictions_match_direct_qlib(name, tmp_path):
    """独立构建 Qlib Dataset，验证数据投影、处理器和模型参数的结果。"""
    import importlib
    from contextlib import nullcontext
    from qlib.data.dataset import DatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    from research_pipeline.research.modeling.qlib import _training_recorder

    samples, _ = synthetic_samples()
    train, valid, infer = samples.iloc[:390], samples.iloc[400:490], samples.iloc[500:600]
    spec = candidate(name)
    managed = fit_bundle(train, valid, candidate=spec, feature_columns=("x1", "x2"),
                         output_root=tmp_path, bundle_path="managed", root_seed=7,
                         fit_scope_ref="train-only")
    raw = pd.concat([train, valid, infer]).copy()
    raw["datetime"] = pd.to_datetime(raw.observation_session)
    raw = raw.set_index(["datetime", "entity_id"]).sort_index()
    raw.index.names = ["datetime", "instrument"]
    matrix = raw[["x1", "x2", "target"]].copy()
    matrix.columns = pd.MultiIndex.from_tuples([
        ("feature", "x1"), ("feature", "x2"), ("label", "target")])
    processors = []
    for declaration in spec["processors"]["infer"]:
        kwargs = dict(declaration["kwargs"])
        if declaration["class"] == "RobustZScoreNorm":
            kwargs.update(fit_start_time=str(train.observation_session.min()),
                          fit_end_time=str(train.observation_session.max()))
        processors.append({"class": declaration["class"], "kwargs": kwargs})
    handler = DataHandlerLP(data_loader=StaticDataLoader(matrix),
                            infer_processors=processors,
                            learn_processors=spec["processors"]["learn"])
    dataset = DatasetH(handler, segments={
        "train": (str(train.observation_session.min()), str(train.observation_session.max())),
        "valid": (str(valid.observation_session.min()), str(valid.observation_session.max())),
        "test": (str(infer.observation_session.min()), str(infer.observation_session.max())),
    })
    kwargs, fit = dict(spec["model"]["kwargs"]), dict(spec["fit"])
    recorder = nullcontext()
    if name == "LGBModel":
        kwargs.update(num_threads=1, seed=7, device_type="cpu")
        recorder = _training_recorder(tmp_path / "direct-records")
    elif name == "XGBModel":
        kwargs.update(nthread=1, seed=7, device="cpu", objective="reg:squarederror")
    if name != "LinearModel":
        fit["evals_result"] = {}
    model = getattr(importlib.import_module(spec["model"]["module_path"]), name)(**kwargs)
    with recorder:
        model.fit(dataset, **fit)
    expected = model.predict(dataset, segment="test")
    wanted = pd.MultiIndex.from_arrays([pd.to_datetime(infer.observation_session), infer.entity_id],
                                       names=["datetime", "instrument"])
    np.testing.assert_allclose(predict_bundle(tmp_path, managed, infer),
                               expected.reindex(wanted).to_numpy(), rtol=0, atol=0)
    config = json.loads((tmp_path / managed["config_path"]).read_text())
    assert bool(config["training_curve"]) == (name != "LinearModel")
    curves = runtime._learning_curves(tmp_path, [{**managed, "candidate_id": name,
                                                "fold_id": "fold-1", "status": "fitted"}])
    assert len(curves) == len(config["training_curve"])
    if name != "LinearModel":
        assert set(curves.segment) == {"train", "valid"}
        assert np.isfinite(curves.value).all()


@pytest.mark.parametrize("change", [
    {"enable_fs": True}, {"decay": None}, {"decay": 0}, {"num_models": 1},
    {"epochs": 0}, {"sub_weights": [0, 1]}, {"sub_weights": [1]},
    {"enable_sr": False}, {"bins_sr": 0}, {"device": "gpu"},
])
def test_double_ensemble_rejects_unsupported_or_invalid_parameters(change):
    spec = candidate("DEnsembleModel")
    spec["model"]["kwargs"].update(change)
    with pytest.raises(QlibModelError):
        normalize_candidates([spec])


def test_double_ensemble_matches_upstream_and_seals_submodels(tmp_path):
    from qlib.contrib.model.double_ensemble import DEnsembleModel
    from research_pipeline.research.modeling.qlib import _matrix, _dataset
    from qlib.utils.serial import Serializable
    samples, _ = synthetic_samples()
    train, valid, test = samples.iloc[:390], samples.iloc[400:490], samples.iloc[500:600]
    spec = candidate("DEnsembleModel")
    row = fit_bundle(train, valid, candidate=spec, feature_columns=("x1", "x2"),
        output_root=tmp_path, bundle_path="bundles/de", root_seed=7, fit_scope_ref="train-only")
    tr, va = _matrix(train, ("x1", "x2"), label=True), _matrix(valid, ("x1", "x2"), label=True)
    dataset = _dataset(pd.concat([tr, va]), [], [], {
        "train": (tr.index.get_level_values(0).min(), tr.index.get_level_values(0).max()),
        "valid": (va.index.get_level_values(0).min(), va.index.get_level_values(0).max())})
    direct = DEnsembleModel(**dict(spec["model"]["kwargs"], num_threads=1, seed=7, device_type="cpu"))
    direct.fit(dataset)
    matrix = _matrix(test, ("x1", "x2"), label=False)
    predictions = direct.predict(_dataset(matrix, [], [], {"test": (matrix.index.get_level_values(0).min(),
        matrix.index.get_level_values(0).max())}))
    np.testing.assert_array_equal(predictions.to_numpy(), predict_bundle(tmp_path, row, test))
    config_path = tmp_path / row["config_path"]
    config = json.loads(config_path.read_text())
    assert config["ensemble_state"]["num_models"] == 2 and config["training_curve"] == []
    model = Serializable.load(tmp_path / row["model_path"])
    assert len(model.ensemble) == 2 and model.params["num_threads"] == 1
    from research_pipeline.evidence.model_validity import _fit_configs
    cid = spec["candidate_id"]
    sealed = dict(row, fold_id="fold", candidate_id=cid, status="fitted")
    evidence = {"design": {"candidate_parameters_json": json.dumps({cid: spec}),
        "candidate_ids": [cid], "validation_sessions": 9, "root_seed": 7,
        "holdout_start": "2024-12-31T00:00:00+00:00"},
        "tables": {"models": [sealed], "fit_audit": [{"fold_id": "fold", "candidate_id": cid,
            "fit_scope_ref": "train-only"}]}, "model_configs": {row["config_path"]: config}}
    sample_map = {value["sample_id"]: value for value in pd.concat([train, valid]).to_dict("records")}
    folds = {"fold": {"train": set(train.sample_id), "validation": set(valid.sample_id)}}
    _fit_configs(evidence, sample_map, folds)
    config["ensemble_state"]["sub_weights"][1] = 99
    with pytest.raises(ValueError, match="子模型状态"):
        _fit_configs(evidence, sample_map, folds)
    config["ensemble_state"]["sub_weights"][1] = 1
    config["ensemble_state"]["sub_features"][1].reverse()
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(QlibModelError, match="状态"):
        predict_bundle(tmp_path, row, test)
