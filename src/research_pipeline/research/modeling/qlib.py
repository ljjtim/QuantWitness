"""Qlib 单标签回归、训练期处理器和可恢复文件工件。"""
from __future__ import annotations

import importlib
import importlib.metadata
import json
import logging
import os
import platform
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from research_pipeline.platform.canonical import canonical_json
from research_pipeline.platform.errors import MainlineError


class QlibModelError(MainlineError):
    error_code = "research_qlib_model_invalid"


MODEL_CLASSES = {
    "LinearModel": "qlib.contrib.model.linear",
    "LGBModel": "qlib.contrib.model.gbdt",
    "XGBModel": "qlib.contrib.model.xgboost",
}
PROCESSORS = {"RobustZScoreNorm", "Fillna", "DropnaLabel", "CSRankNorm"}


def normalize_candidates(candidates: Sequence[Mapping[str, object]]) -> list[dict]:
    if not isinstance(candidates, (list, tuple)) or not candidates:
        raise QlibModelError("Qlib 候选必须是非空列表")
    result = []
    ids = set()
    for raw in candidates:
        item = json.loads(canonical_json(dict(raw)))
        if set(item) != {"candidate_id", "model", "processors", "fit"}:
            raise QlibModelError("候选必须声明 candidate_id/model/processors/fit")
        cid = item["candidate_id"]
        if not isinstance(cid, str) or not cid or cid in ids:
            raise QlibModelError("candidate_id 必须非空且唯一")
        ids.add(cid)
        model = item["model"]
        if set(model) != {"class", "module_path", "kwargs"} or MODEL_CLASSES.get(model["class"]) != model["module_path"]:
            raise QlibModelError("首批只支持 Qlib LinearModel/LGBModel/XGBModel 固定路径")
        kwargs = model["kwargs"]
        fit = item["fit"]
        if not isinstance(kwargs, dict) or not isinstance(fit, dict):
            raise QlibModelError("Qlib kwargs 与 fit 必须是对象")
        if model["class"] == "LinearModel":
            if set(kwargs) - {"estimator", "alpha", "fit_intercept", "include_valid"}:
                raise QlibModelError("LinearModel 参数不受支持")
            if kwargs.get("estimator", "ols") not in {"ols", "ridge"} or kwargs.get("include_valid", False):
                raise QlibModelError("LinearModel 只支持 OLS/Ridge，include_valid 必须为 false")
            if fit:
                raise QlibModelError("LinearModel 首批 fit 不接受参数")
        elif model["class"] == "LGBModel":
            if kwargs.get("loss", "mse") != "mse":
                raise QlibModelError("LGBModel 首批只支持 mse 回归")
            if set(fit) - {"num_boost_round", "early_stopping_rounds", "verbose_eval"}:
                raise QlibModelError("LGBModel fit 只支持轮数、早停与日志间隔")
            if any(kwargs.get(k, 1) != 1 for k in ("num_threads", "n_jobs", "nthread")):
                raise QlibModelError("LGBModel 必须 CPU 单线程")
            if kwargs.get("device_type", "cpu") != "cpu" or kwargs.get("device", "cpu") != "cpu":
                raise QlibModelError("首批模型只支持 CPU")
        else:
            if kwargs.get("objective", "reg:squarederror") != "reg:squarederror":
                raise QlibModelError("XGBModel 首批只支持平方误差回归")
            if set(fit) - {"num_boost_round", "early_stopping_rounds", "verbose_eval"}:
                raise QlibModelError("XGBModel fit 只支持轮数与日志间隔")
            if fit.get("early_stopping_rounds") is not None:
                raise QlibModelError("XGBModel 首批 early_stopping_rounds 必须为 null")
            if any(kwargs.get(k, 1) != 1 for k in ("nthread", "n_jobs")) or kwargs.get("device", "cpu") != "cpu":
                raise QlibModelError("XGBModel 必须 CPU 单线程")
        chains = item["processors"]
        if set(chains) != {"infer", "learn"}:
            raise QlibModelError("processors 必须声明 infer/learn")
        for phase, specs in chains.items():
            if not isinstance(specs, list):
                raise QlibModelError("Processor 链必须是列表")
            for spec in specs:
                if set(spec) - {"class", "module_path", "kwargs"} or spec.get("class") not in PROCESSORS:
                    raise QlibModelError("不支持的 Qlib Processor")
                if spec.get("module_path", "qlib.data.dataset.processor") != "qlib.data.dataset.processor":
                    raise QlibModelError("Processor 必须来自 qlib.data.dataset.processor")
                p = spec.get("kwargs", {})
                if "fit_start_time" in p or "fit_end_time" in p:
                    raise QlibModelError("Processor 拟合区间由实际 train 样本确定")
                allowed = {"RobustZScoreNorm": {"fields_group", "clip_outlier"},
                    "Fillna": {"fields_group", "fill_value"}, "DropnaLabel": set(),
                    "CSRankNorm": {"fields_group"}}[spec["class"]]
                if set(p) - allowed:
                    raise QlibModelError(f"{spec['class']} 参数不受支持: {sorted(set(p) - allowed)}")
                if spec["class"] in {"RobustZScoreNorm", "Fillna"}:
                    if phase != "infer" or p.get("fields_group") != "feature":
                        raise QlibModelError("数值标准化和填充仅允许 infer feature")
                elif phase != "learn" or (spec["class"] == "CSRankNorm" and p.get("fields_group", "label") != "label"):
                    raise QlibModelError("标签处理只允许 learn label")
        result.append(item)
    label_methods = {any(p["class"] == "CSRankNorm" for p in item["processors"]["learn"]) for item in result}
    if len(label_methods) != 1:
        raise QlibModelError("同一次候选搜索的标签学习与评价口径必须一致")
    return result


def _matrix(frame: pd.DataFrame, features: Sequence[str], *, label: bool) -> pd.DataFrame:
    required = {"sample_id", "entity_id", "observation_session", *features}
    if not required <= set(frame):
        raise QlibModelError(f"Qlib 输入缺少列: {sorted(required - set(frame))}")
    if "feature_available_time" in frame and "decision_time" in frame:
        if (pd.to_datetime(frame["feature_available_time"], utc=True) > pd.to_datetime(frame["decision_time"], utc=True)).any():
            raise QlibModelError("Feature 在决策时点尚不可见")
    index = pd.MultiIndex.from_arrays([
        pd.to_datetime(frame["observation_session"], utc=True).dt.tz_localize(None),
        frame["entity_id"].astype(str),
    ], names=["datetime", "instrument"])
    if index.has_duplicates or frame["sample_id"].duplicated().any():
        raise QlibModelError("Qlib 输入要求 sample_id 和证券日期一一对应")
    values = frame.loc[:, list(features)].to_numpy(dtype=float)
    result = pd.DataFrame(values, index=index, columns=pd.MultiIndex.from_product([["feature"], features]))
    if np.isinf(values).any():
        raise QlibModelError("Qlib feature 不允许无穷值")
    if label:
        target = frame["target"].to_numpy(dtype=float)
        if not np.isfinite(target).all():
            raise QlibModelError("Qlib label 必须有限")
        result[("label", "target")] = target
    return result.sort_index()


def _dataset(data, infer, learn, segments):
    from qlib.data.dataset import DatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    handler = DataHandlerLP(data_loader=StaticDataLoader(data), infer_processors=infer,
                            learn_processors=learn, init_data=False)
    handler.setup_data(init_type=DataHandlerLP.IT_LS)
    return DatasetH(handler=handler, segments=segments)


@contextmanager
def _training_recorder(directory: Path):
    """训练指标只写本节点；研究来源由 RP 工件封存。"""
    import mlflow
    from qlib.config import C
    from qlib.workflow import R, QlibRecorder
    from qlib.workflow.expm import MLflowExpManager
    from qlib.workflow.recorder import Recorder

    uri = directory.resolve().as_uri()
    previous = R._provider
    previous_config = deepcopy(C.exp_manager)
    previous_uri = mlflow.get_tracking_uri()
    previous_allow = os.environ.get("MLFLOW_ALLOW_FILE_STORE")
    manager = recorder = provider = None
    registered = False
    failure = cleanup_failure = None
    try:
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"
        manager = MLflowExpManager(uri=uri, default_exp_name="rp-model")
        provider = QlibRecorder(manager)
        R.register(provider)
        registered = True
        experiment = manager.create_exp(experiment_name="rp-model")
        recorder = experiment.create_recorder()
        # 只关闭当前 recorder 的整仓 Git 抓取，不修改上游类或其他实验。
        recorder._log_uncommitted_code = lambda: None
        experiment.active_recorder = recorder
        manager.active_experiment = experiment
        recorder.start_run()
        yield
    except BaseException as exc:
        failure = exc
        raise
    finally:
        status = Recorder.STATUS_FA if failure is not None else Recorder.STATUS_FI
        try:
            if recorder is not None and recorder.id is not None:
                try:
                    recorder.end_run(status)
                except BaseException as exc:
                    cleanup_failure = exc
                # end_run 若在异步日志清理中失败，仍结束本次已打开的 MLflow run。
                active = mlflow.active_run()
                if active is not None and active.info.run_id == recorder.id:
                    try:
                        mlflow.end_run(Recorder.STATUS_FA)
                    except BaseException as exc:
                        cleanup_failure = cleanup_failure or exc
        finally:
            if registered:
                manager.active_experiment = None
                R.register(previous)
            C.exp_manager = previous_config
            mlflow.set_tracking_uri(previous_uri)
            if previous_allow is None:
                os.environ.pop("MLFLOW_ALLOW_FILE_STORE", None)
            else:
                os.environ["MLFLOW_ALLOW_FILE_STORE"] = previous_allow
        if cleanup_failure is not None:
            if failure is None:
                raise cleanup_failure
            logging.getLogger(__name__).warning("训练记录清理失败：%s", cleanup_failure)


def fit_bundle(train, valid, *, candidate, feature_columns, output_root, bundle_path, root_seed, fit_scope_ref):
    candidate = normalize_candidates([candidate])[0]
    features = tuple(feature_columns)
    if train.empty or valid.empty:
        raise QlibModelError("Qlib train/valid 必须非空")
    if set(train["sample_id"]) & set(valid["sample_id"]):
        raise QlibModelError("Qlib train 与 valid 不得重叠")
    fit_time = pd.to_datetime(valid["decision_time"], utc=True).min()
    if (pd.to_datetime(train["label_available_time"], utc=True) > fit_time).any():
        raise QlibModelError("train 标签在拟合边界尚不可见")
    tr = _matrix(train, features, label=True)
    va = _matrix(valid, features, label=True)
    if tr.index.get_level_values("datetime").max() >= va.index.get_level_values("datetime").min():
        raise QlibModelError("Qlib train 必须严格早于 valid")
    root = Path(output_root)
    bundle = root / bundle_path
    bundle.mkdir(parents=True, exist_ok=False)
    processor_module = importlib.import_module("qlib.data.dataset.processor")
    processors = {"infer": [], "learn": []}
    processor_files = {"infer": [], "learn": []}
    fitted_data = tr.copy()
    for phase in ("infer", "learn"):
        for i, spec in enumerate(candidate["processors"][phase]):
            kwargs = dict(spec.get("kwargs", {}))
            if spec["class"] == "CSRankNorm":
                kwargs.setdefault("fields_group", "label")
            if spec["class"] == "RobustZScoreNorm":
                kwargs.update(fit_start_time=tr.index.get_level_values("datetime").min(),
                              fit_end_time=tr.index.get_level_values("datetime").max())
            proc = getattr(processor_module, spec["class"])(**kwargs)
            proc.fit(fitted_data)
            fitted_data = proc(fitted_data.copy())
            processors[phase].append(proc)
            relative = f"{bundle_path}/processors/{phase}-{i:03d}.pkl"
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            proc.to_pickle(path, dump_all=True)
            processor_files[phase].append(relative)
    data = pd.concat([tr, va]).sort_index()
    dataset = _dataset(data, processors["infer"], processors["learn"], {
        "train": (tr.index.get_level_values("datetime").min(), tr.index.get_level_values("datetime").max()),
        "valid": (va.index.get_level_values("datetime").min(), va.index.get_level_values("datetime").max()),
    })
    spec = candidate["model"]
    kwargs = dict(spec["kwargs"])
    fit = dict(candidate["fit"])
    if spec["class"] == "LGBModel":
        kwargs.update(num_threads=1, seed=root_seed, device_type="cpu")
        fit["evals_result"] = {}
        fit.setdefault("verbose_eval", 0)
    elif spec["class"] == "XGBModel":
        kwargs.update(nthread=1, seed=root_seed, device="cpu", objective="reg:squarederror")
        fit.update(early_stopping_rounds=None, evals_result={})
        fit.setdefault("verbose_eval", False)
    model = getattr(importlib.import_module(spec["module_path"]), spec["class"])(**kwargs)
    # 只有 LGBModel 向全局 Qlib Recorder 记录训练曲线。
    if spec["class"] == "LGBModel":
        with _training_recorder(bundle / "training-records"):
            model.fit(dataset, **fit)
    else:
        model.fit(dataset, **fit)
    model.to_pickle(bundle / "model.pkl", dump_all=True)
    fit_time = max(fit_time, pd.to_datetime(valid["label_available_time"], utc=True).max(), pd.to_datetime(valid["label_end_time"], utc=True).max())
    versions = {"python": platform.python_version()}
    for name in ("pyqlib", "numpy", "pandas", "scikit-learn", "lightgbm", "xgboost"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            pass
    training_curve = []
    for segment, metrics in fit.get("evals_result", {}).items():
        if isinstance(metrics, list):
            metric_name = kwargs.get("eval_metric", "rmse")
            if isinstance(metric_name, list):
                metric_name = metric_name[0]
            metrics = {metric_name: metrics}
        for metric_name, values in metrics.items():
            training_curve.extend({"segment": segment, "metric": metric_name,
                                   "iteration": step, "value": float(value)}
                                  for step, value in enumerate(values))
    config = {
        "schema": "research.qlib-model-bundle.v1", "candidate": candidate,
        "feature_columns": list(features), "processor_files": processor_files,
        "model_path": f"{bundle_path}/model.pkl", "versions": versions,
        "root_seed": root_seed, "thread_count": 1, "fit_scope_ref": fit_scope_ref,
        "effective_model_kwargs": kwargs,
        "training_curve": training_curve,
        "effective_fit_kwargs": {key: value for key, value in fit.items() if key != "evals_result"},
        "train_ids": train["sample_id"].astype(str).tolist(),
        "valid_ids": valid["sample_id"].astype(str).tolist(),
        "fit_time": fit_time.isoformat(),
        "training_label": "cross_sectional_rank" if any(p["class"] == "CSRankNorm" for p in candidate["processors"]["learn"]) else "raw",
        "index": ["datetime", "instrument"],
    }
    config_path = f"{bundle_path}/config.json"
    (root / config_path).write_text(canonical_json(config), encoding="utf-8")
    return {"bundle_path": bundle_path, "model_path": config["model_path"],
            "config_path": config_path, "model_class": f"{spec['module_path']}.{spec['class']}",
            "feature_columns_json": canonical_json(list(features)), "target_kind": "regression",
            "fit_scope_ref": fit_scope_ref, "fit_time": fit_time.isoformat()}


def predict_bundle(root, model_row, frame):
    root = Path(root)
    config = json.loads((root / model_row["config_path"]).read_text(encoding="utf-8"))
    if config.get("schema") != "research.qlib-model-bundle.v1":
        raise QlibModelError("旧模型工件不能用于 Qlib v2 恢复；请使用原环境或新建 v2 运行")
    from qlib.utils.serial import Serializable
    features = config["feature_columns"]
    data = _matrix(frame, features, label=False)
    processors = [Serializable.load(root / path) for path in config["processor_files"]["infer"]]
    dataset = _dataset(data, processors, [], {"test": (data.index.get_level_values("datetime").min(), data.index.get_level_values("datetime").max())})
    model = Serializable.load(root / config["model_path"])
    result = model.predict(dataset, segment="test")
    wanted = pd.MultiIndex.from_arrays([pd.to_datetime(frame["observation_session"], utc=True).dt.tz_localize(None), frame["entity_id"].astype(str)], names=["datetime", "instrument"])
    if result.index.has_duplicates or set(result.index) != set(wanted):
        raise QlibModelError("Qlib 预测必须覆盖全部输入证券日期且不得重复")
    values = result.reindex(wanted).to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise QlibModelError("Qlib 预测包含非有限值")
    return values



def evaluation_labels(root, model_row, frame):
    """评价使用冻结 learn 标签方法，原始收益由调用方另外保留。"""
    from qlib.utils.serial import Serializable
    root = Path(root)
    config = json.loads((root / model_row["config_path"]).read_text(encoding="utf-8"))
    data = _matrix(frame, config["feature_columns"], label=True)
    for path in config["processor_files"]["learn"]:
        data = Serializable.load(root / path)(data)
    wanted = pd.MultiIndex.from_arrays([pd.to_datetime(frame["observation_session"], utc=True).dt.tz_localize(None),
                                       frame["entity_id"].astype(str)], names=["datetime", "instrument"])
    return data[("label", "target")].reindex(wanted).to_numpy(dtype=float)
