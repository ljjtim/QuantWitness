"""Qlib 单标签回归、训练期处理器和可恢复文件工件。"""
from __future__ import annotations

import importlib
import importlib.metadata
import json
import logging
import math
import os
import platform
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from research_pipeline.platform.canonical import canonical_json
from .inputs import QlibModelError, qlib_matrix as _matrix


MODEL_CLASSES = {
    "GeneratedModel": "research_pipeline.research.modeling.generated",
    "LinearModel": "qlib.contrib.model.linear",
    "LGBModel": "qlib.contrib.model.gbdt",
    "XGBModel": "qlib.contrib.model.xgboost",
    "DEnsembleModel": "qlib.contrib.model.double_ensemble",
    "GRU": "qlib.contrib.model.pytorch_gru_ts",
    "LSTM": "qlib.contrib.model.pytorch_lstm_ts",
    "TransformerModel": "qlib.contrib.model.pytorch_transformer_ts",
}
SEQUENCE_MODEL_CLASSES = {"GRU", "LSTM", "TransformerModel"}
PROCESSORS = {"RobustZScoreNorm", "ZScoreNorm", "CSZScoreNorm", "Fillna", "DropnaLabel", "CSRankNorm"}
TRAIN_FITTED_PROCESSORS = {"RobustZScoreNorm", "ZScoreNorm"}


def normalize_candidates(candidates: Sequence[Mapping[str, object]]) -> list[dict]:
    if not isinstance(candidates, (list, tuple)) or not candidates:
        raise QlibModelError("Qlib 候选必须是非空列表")
    result = []
    ids = set()
    for raw in candidates:
        item = json.loads(canonical_json(dict(raw)))
        if set(item) not in ({"candidate_id", "model", "processors", "fit"},
                              {"candidate_id", "model", "processors", "fit", "dataset"}):
            raise QlibModelError("候选必须声明 candidate_id/model/processors/fit；序列模型还需 dataset")
        cid = item["candidate_id"]
        if not isinstance(cid, str) or not cid or cid in ids:
            raise QlibModelError("candidate_id 必须非空且唯一")
        ids.add(cid)
        model = item["model"]
        if set(model) != {"class", "module_path", "kwargs"} or MODEL_CLASSES.get(model["class"]) != model["module_path"]:
            raise QlibModelError("只支持 Qlib LinearModel/LGBModel/XGBModel/DEnsembleModel/GRU/LSTM/TransformerModel及受控GeneratedModel固定路径")
        if model["class"] not in SEQUENCE_MODEL_CLASSES and "dataset" in item:
            raise QlibModelError("dataset 只允许序列候选")
        kwargs = model["kwargs"]
        fit = item["fit"]
        if not isinstance(kwargs, dict) or not isinstance(fit, dict):
            raise QlibModelError("Qlib kwargs 与 fit 必须是对象")
        if model["class"] == "GeneratedModel":
            from .generated_definition import validate_kwargs
            validate_kwargs(kwargs)
            if fit:
                raise QlibModelError("生成模型fit必须为空")
        elif model["class"] in SEQUENCE_MODEL_CLASSES:
            if set(item) != {"candidate_id", "model", "processors", "fit", "dataset"}:
                raise QlibModelError("序列模型 候选必须声明 dataset")
            dataset = item["dataset"]
            if not isinstance(dataset, dict) or set(dataset) != {"class", "step_len", "missing_policy"}:
                raise QlibModelError("序列模型 dataset 必须声明 class/step_len/missing_policy")
            if dataset["class"] != "TSDatasetH" or type(dataset["step_len"]) is not int or dataset["step_len"] < 2:
                raise QlibModelError("序列模型 dataset 必须使用 step_len>=2 的 TSDatasetH")
            if dataset["missing_policy"] != "complete_window":
                raise QlibModelError("序列模型 只允许 complete_window 缺失策略")
            allowed_sequence = {"d_feat", "hidden_size", "num_layers", "dropout", "n_epochs", "lr",
                           "metric", "batch_size", "early_stop", "loss", "optimizer", "n_jobs", "GPU", "seed"}
            is_transformer = model["class"] == "TransformerModel"
            if is_transformer:
                allowed_sequence = (allowed_sequence - {"hidden_size"}) | {"d_model", "nhead", "reg"}
                if dataset["step_len"] > 1000:
                    raise QlibModelError("TransformerModel step_len 不能超过位置编码上限1000")
                width, heads = kwargs.get("d_model", 64), kwargs.get("nhead", 2)
                if type(width) is not int or width < 2 or width % 2:
                    raise QlibModelError("TransformerModel d_model 必须为不小于2的偶数")
                if type(heads) is not int or heads < 1 or width % heads:
                    raise QlibModelError("TransformerModel nhead 必须为正整数且整除d_model")
                reg = kwargs.get("reg", 1e-3)
                if type(reg) not in (int, float) or not math.isfinite(reg) or reg < 0:
                    raise QlibModelError("TransformerModel reg 必须为有限非负数")
            if set(kwargs) - allowed_sequence or fit:
                raise QlibModelError("序列模型 只支持固定构造参数，fit 必须为空")
            if kwargs.get("loss", "mse") != "mse" or kwargs.get("metric", "") not in {"", "loss"}:
                raise QlibModelError("序列模型 只支持 mse 与 negative_mse 评价")
            optimizer = kwargs.get("optimizer", "adam")
            if not isinstance(optimizer, str) or optimizer.lower() not in {"adam", "gd"}:
                raise QlibModelError("序列模型 optimizer 只支持 adam 或 gd")
            if (type(kwargs.get("GPU", -1)) is not int or kwargs.get("GPU", -1) != -1
                    or type(kwargs.get("n_jobs", 0)) is not int or kwargs.get("n_jobs", 0) != 0):
                raise QlibModelError("序列模型 首批只支持 CPU 与 n_jobs=0")
            if type(kwargs.get("batch_size", 1)) is not int or kwargs.get("batch_size", 1) != 1:
                raise QlibModelError("序列模型 首批 batch_size 必须为 1，保证验证尾批完整消费")
            defaults = {"d_feat": 1, "hidden_size": 64, "num_layers": 2,
                        "n_epochs": 200, "batch_size": 1, "early_stop": 20}
            for name, minimum in (("d_feat", 1), ("hidden_size", 1), ("num_layers", 1),
                                  ("n_epochs", 1), ("batch_size", 1), ("early_stop", 1)):
                value = kwargs.get(name, defaults[name])
                if type(value) is not int or value < minimum:
                    raise QlibModelError(f"序列模型 {name} 必须为正整数")
            dropout = kwargs.get("dropout", 0.0)
            lr = kwargs.get("lr", 0.001)
            if type(dropout) not in (int, float) or not math.isfinite(dropout) or not 0 <= dropout < 1:
                raise QlibModelError("序列模型 dropout 必须在 [0,1) 内")
            if type(lr) not in (int, float) or not math.isfinite(lr) or lr <= 0:
                raise QlibModelError("序列模型 lr 必须为正数")
        elif model["class"] == "LinearModel":
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
        elif model["class"] == "DEnsembleModel":
            if fit:
                raise QlibModelError("DEnsembleModel 的轮数和早停在 model.kwargs 声明，fit 必须为空")
            if kwargs.get("base_model", "gbm") != "gbm" or kwargs.get("loss", "mse") != "mse":
                raise QlibModelError("DEnsembleModel 只支持 gbm/mse")
            if kwargs.get("enable_sr", True) is not True or kwargs.get("enable_fs") is not False:
                raise QlibModelError("DEnsembleModel 要求启用样本重加权并显式关闭特征选择")
            for name, default, minimum in (("num_models", 6, 2), ("epochs", 100, 1), ("bins_sr", 10, 1)):
                value = kwargs.get(name, default)
                if type(value) is not int or value < minimum:
                    raise QlibModelError(f"DEnsembleModel {name} 必须为不小于 {minimum} 的整数")
            for name, default in (("decay", None), ("alpha1", 1.0), ("alpha2", 1.0)):
                value = kwargs.get(name, default)
                if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                    raise QlibModelError(f"DEnsembleModel {name} 必须为有限正数")
            early = kwargs.get("early_stopping_rounds")
            if early is not None and (type(early) is not int or early < 1):
                raise QlibModelError("DEnsembleModel early_stopping_rounds 必须为正整数或 null")
            weights = kwargs.get("sub_weights")
            if weights is not None and (not isinstance(weights, list)
                    or len(weights) != kwargs.get("num_models", 6)
                    or any(type(w) not in (int, float) or not math.isfinite(w) or w < 0 for w in weights)
                    or weights[0] <= 0 or not math.isfinite(sum(weights))):
                raise QlibModelError("DEnsembleModel sub_weights 必须匹配子模型数、非负且首项为正")
            if any(name in kwargs for name in ("bins_fs", "sample_ratios")):
                raise QlibModelError("DEnsembleModel 当前不接受特征选择参数")
            if kwargs.get("objective", "mse") != "mse":
                raise QlibModelError("DEnsembleModel objective 必须为 mse")
            if any(kwargs.get(k, 1) != 1 for k in ("num_threads", "n_jobs", "nthread")):
                raise QlibModelError("DEnsembleModel 必须 CPU 单线程")
            if kwargs.get("device_type", "cpu") != "cpu" or kwargs.get("device", "cpu") != "cpu":
                raise QlibModelError("DEnsembleModel 必须使用 CPU")
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
                    "ZScoreNorm": {"fields_group"}, "CSZScoreNorm": {"fields_group", "method"},
                    "Fillna": {"fields_group", "fill_value"}, "DropnaLabel": set(),
                    "CSRankNorm": {"fields_group"}}[spec["class"]]
                if set(p) - allowed:
                    raise QlibModelError(f"{spec['class']} 参数不受支持: {sorted(set(p) - allowed)}")
                if spec["class"] in TRAIN_FITTED_PROCESSORS | {"CSZScoreNorm", "Fillna"}:
                    if phase != "infer" or p.get("fields_group") != "feature":
                        raise QlibModelError("数值标准化和填充仅允许 infer feature")
                    if spec["class"] == "CSZScoreNorm" and p.get("method", "zscore") not in {"zscore", "robust"}:
                        raise QlibModelError("CSZScoreNorm method 只支持 zscore/robust")
                elif phase != "learn" or (spec["class"] == "CSRankNorm" and p.get("fields_group", "label") != "label"):
                    raise QlibModelError("标签处理只允许 learn label")
        if model["class"] == "GeneratedModel" and any(p["class"] == "CSRankNorm" for p in chains["learn"]):
            raise QlibModelError("生成模型只支持原始收益标签")
        if model["class"] in SEQUENCE_MODEL_CLASSES and any(p["class"] == "CSZScoreNorm" for p in chains["infer"]):
            raise QlibModelError("序列模型不支持横截面特征标准化；需先定义逐末端可见证券集合")
        result.append(item)
    label_methods = {any(p["class"] == "CSRankNorm" for p in item["processors"]["learn"]) for item in result}
    if len(label_methods) != 1:
        raise QlibModelError("同一次候选搜索的标签学习与评价口径必须一致")
    return result


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


def fit_bundle(train, valid, *, candidate, feature_columns, output_root, bundle_path, root_seed, fit_scope_ref, sequence_context=None):
    candidate = normalize_candidates([candidate])[0]
    is_sequence = candidate["model"]["class"] in SEQUENCE_MODEL_CLASSES
    if not is_sequence and sequence_context is not None:
        raise QlibModelError("表格模型不接受 sequence_context")
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
    sequence_windows = None
    if is_sequence:
        from .gru import bind_windows, effective_kwargs, require_sequence_environment
        require_sequence_environment()
        effective_kwargs(candidate, features, root_seed)
        sequence_windows = bind_windows(sequence_context, pd.concat([train, valid]), features, candidate["dataset"]["step_len"])
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
            if spec["class"] in TRAIN_FITTED_PROCESSORS:
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
    if is_sequence:
        from .gru import processed_dataset
        dataset = processed_dataset(sequence_windows, processors=processors,
            segments={"train": train.sample_id.astype(str).tolist(), "valid": valid.sample_id.astype(str).tolist()},
            label_frame=pd.concat([train, valid]))
    else:
        data = pd.concat([tr, va]).sort_index()
        dataset = _dataset(data, processors["infer"], processors["learn"], {
            "train": (tr.index.get_level_values("datetime").min(), tr.index.get_level_values("datetime").max()),
            "valid": (va.index.get_level_values("datetime").min(), va.index.get_level_values("datetime").max()),
        })
    spec = candidate["model"]
    kwargs = dict(spec["kwargs"])
    fit = dict(candidate["fit"])
    if spec["class"] == "GeneratedModel":
        from .gru import require_sequence_environment
        require_sequence_environment()
        kwargs.update(d_feat=len(features), seed=root_seed)
    elif spec["class"] == "LGBModel":
        kwargs.update(num_threads=1, seed=root_seed, device_type="cpu")
        fit["evals_result"] = {}
        fit.setdefault("verbose_eval", 0)
    elif spec["class"] == "DEnsembleModel":
        kwargs.update(num_threads=1, seed=root_seed, device_type="cpu")
    elif spec["class"] == "XGBModel":
        kwargs.update(nthread=1, seed=root_seed, device="cpu", objective="reg:squarederror")
        fit.update(early_stopping_rounds=None, evals_result={})
        fit.setdefault("verbose_eval", False)
    execution_scope = nullcontext()
    if is_sequence:
        from .gru import cpu_execution, effective_kwargs, preserve_batch_axis
        kwargs = effective_kwargs(candidate, features, root_seed)
        fit.update(evals_result={}, save_path=str(bundle / "weights.pt"))
        execution_scope = cpu_execution()
    with execution_scope:
        model = getattr(importlib.import_module(spec["module_path"]), spec["class"])(**kwargs)
        if is_sequence:
            from .gru import model_network
            model_network(model, spec["class"]).register_forward_hook(preserve_batch_axis)
        # 只有 LGBModel 向全局 Qlib Recorder 记录训练曲线。
        if spec["class"] == "LGBModel":
            with _training_recorder(bundle / "training-records"):
                model.fit(dataset, **fit)
        else:
            model.fit(dataset, **fit)
    model.to_pickle(bundle / "model.pkl", dump_all=True)
    fit_time = max(fit_time, pd.to_datetime(valid["label_available_time"], utc=True).max(), pd.to_datetime(valid["label_end_time"], utc=True).max())
    versions = {"python": platform.python_version()}
    distributions = ("pyqlib", "numpy", "pandas", "scikit-learn", "lightgbm", "xgboost")
    if is_sequence or spec["class"] == "GeneratedModel":
        distributions += ("torch",)
    for name in distributions:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            pass
    training_curve = []
    for segment, metrics in fit.get("evals_result", {}).items():
        if isinstance(metrics, list):
            metric_name = "negative_mse" if is_sequence else kwargs.get("eval_metric", "rmse")
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
    if is_sequence:
        from .gru import SEQUENCE_BUNDLE_SCHEMA, save_window_facts, verify_loaded_model
        if not training_curve or any(not math.isfinite(row["value"]) or row["value"] > 0 for row in training_curve):
            raise QlibModelError("序列模型学习曲线必须包含有限 negative_mse")
        config["schema"] = SEQUENCE_BUNDLE_SCHEMA
        config["weights_path"] = f"{bundle_path}/weights.pt"
        config["effective_fit_kwargs"] = {"save_path": config["weights_path"]}
        config["sequence"] = save_window_facts(root, bundle_path, sequence_windows)
        verify_loaded_model(root, config, model)
    if spec["class"] == "DEnsembleModel":
        config["ensemble_state"] = _ensemble_state(model, features)
    if spec["class"] == "GeneratedModel":
        from .generated import save_generated, verify_loaded
        config["generated"] = save_generated(root, bundle_path, model)
        config["training_curve"] = model.curve
        verify_loaded(root, config, model)
    config_path = f"{bundle_path}/config.json"
    (root / config_path).write_text(canonical_json(config), encoding="utf-8")
    return {"bundle_path": bundle_path, "model_path": config["model_path"],
            "config_path": config_path, "model_class": f"{spec['module_path']}.{spec['class']}",
            "feature_columns_json": canonical_json(list(features)), "target_kind": "regression",
            "fit_scope_ref": fit_scope_ref, "fit_time": fit_time.isoformat()}


def _ensemble_state(model, features):
    sub_features = [list(columns) for columns in model.sub_features]
    if len(model.ensemble) != model.num_models or sub_features != [list(features)] * model.num_models:
        raise QlibModelError("DEnsembleModel 子模型数量或特征顺序不符合冻结声明")
    iterations = [part.current_iteration() for part in model.ensemble]
    if any(value < 1 or value > model.epochs for value in iterations):
        raise QlibModelError("DEnsembleModel 子模型没有有效训练轮数")
    return {"num_models": model.num_models, "sub_features": sub_features,
            "sub_weights": list(model.sub_weights), "iterations": iterations}


def predict_bundle(root, model_row, frame, *, sequence_context=None):
    root = Path(root)
    config = json.loads((root / model_row["config_path"]).read_text(encoding="utf-8"))
    from qlib.utils.serial import Serializable
    features = config["feature_columns"]
    is_sequence = config["candidate"]["model"]["class"] in SEQUENCE_MODEL_CLASSES
    if is_sequence:
        from .gru import SEQUENCE_BUNDLE_SCHEMA, bind_windows, processed_dataset, cpu_execution, verify_loaded_model, require_sequence_environment
        require_sequence_environment()
        if config.get("schema") != SEQUENCE_BUNDLE_SCHEMA:
            raise QlibModelError("序列模型必须使用序列模型文件合同")
        candidate = normalize_candidates([config["candidate"]])[0]
        windows = bind_windows(sequence_context, frame, features, candidate["dataset"]["step_len"])
        if (config["sequence"]["step_len"] != windows.step_len
                or config["sequence"]["feature_columns"] != list(windows.feature_columns)
                or config["sequence"]["calendar_sessions"] != [str(day) for day in windows.calendar_sessions]
                or config["sequence"]["missing_policy"] != "complete_window"):
            raise QlibModelError("序列模型预测窗口、日历或特征顺序与拟合封存不一致")
        processors = {"infer": [Serializable.load(root / path) for path in config["processor_files"]["infer"]], "learn": []}
        dataset = processed_dataset(windows, processors=processors, segments={"test": frame.sample_id.astype(str).tolist()})
        with cpu_execution():
            model = Serializable.load(root / config["model_path"])
            verify_loaded_model(root, config, model)
            result = model.predict(dataset)
    else:
        if sequence_context is not None:
            raise QlibModelError("表格模型不接受 sequence_context")
        if config.get("schema") != "research.qlib-model-bundle.v1":
            raise QlibModelError("旧模型工件不能用于 Qlib v2 恢复；请使用原环境或新建 v2 运行")
        data = _matrix(frame, features, label=False)
        processors = [Serializable.load(root / path) for path in config["processor_files"]["infer"]]
        dataset = _dataset(data, processors, [], {"test": (data.index.get_level_values("datetime").min(), data.index.get_level_values("datetime").max())})
        model = Serializable.load(root / config["model_path"])
        if config["candidate"]["model"]["class"] == "GeneratedModel":
            from .generated import verify_loaded
            verify_loaded(root, config, model)
        if config["candidate"]["model"]["class"] == "DEnsembleModel":
            if config.get("ensemble_state") != _ensemble_state(model, features):
                raise QlibModelError("恢复的 DEnsembleModel 状态与封存配置不一致")
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
