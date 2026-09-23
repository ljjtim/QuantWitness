"""有限静态模型的拟合、预处理与一次性 holdout；编排由七阶段 Runtime 负责。"""

from __future__ import annotations

from datetime import date, datetime
import importlib
import importlib.metadata
import math
from pathlib import Path
from typing import Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError
from research_pipeline.research.validation import (
    HoldoutAccessPlan,
    PersistentHoldoutLedger,
    ValidationError,
    bind_fit_artifact,
    build_seed_manifest,
    issue_fit_scope,
)


WALK_FORWARD_MODEL_VERSION = "research-walk-forward-model-v2"
SUPPORTED_MODELS = frozenset({"ridge", "elastic_net", "huber", "logistic", "lightgbm"})
REGRESSION_MODELS = frozenset({"ridge", "elastic_net", "huber", "lightgbm"})
CLASSIFICATION_MODELS = frozenset({"logistic", "lightgbm"})
MODEL_PARAMETER_FIELDS = {
    "ridge": frozenset({"model_id", "alpha"}),
    "elastic_net": frozenset({"model_id", "alpha", "l1_ratio"}),
    "huber": frozenset({"model_id", "alpha", "epsilon"}),
    "logistic": frozenset(
        {"model_id", "C", "minimum_class_count", "minimum_class_fraction"}
    ),
    "lightgbm": frozenset(
        {
            "model_id",
            "n_estimators",
            "num_leaves",
            "learning_rate",
            "max_depth",
            "minimum_class_count",
            "minimum_class_fraction",
        }
    ),
}


class ModelMainlineError(MainlineError):
    error_code = "research_model_mainline_invalid"


class ModelDependencyError(ModelMainlineError):
    error_code = "research_model_dependency_missing"


class CandidateFitRejected(ModelMainlineError):
    """当前冻结样本不满足候选拟合条件，但不表示程序执行出错。"""

    error_code = "research_model_candidate_fit_rejected"

    def __init__(self, message: str, *, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def assemble_daily_model_samples(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    horizon_sessions: int,
    target_field: str = "forward_return",
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """把日频长表变成资产无关的样本矩阵，不改变任何特征值。"""
    feature_required = {
        "entity_id",
        "observation_session",
        "observation_time",
        "available_time",
        "window_sessions",
        "feature_id",
        "value",
        "status",
        "lineage_hash",
    }
    label_required = {
        "entity_id",
        "observation_session",
        "decision_time",
        "label_start_time",
        "label_end_time",
        "available_time",
        "horizon_sessions",
        target_field,
        "lineage_hash",
    }
    _require_columns(features, feature_required, "features")
    _require_columns(labels, label_required, "labels")
    if type(horizon_sessions) is not int or horizon_sessions < 1:
        raise ModelMainlineError("horizon_sessions 必须是正整数")
    valid_features = features.loc[features["status"].astype(str) == "ok"].copy()
    selected_labels = labels.loc[labels["horizon_sessions"] == horizon_sessions].copy()
    if valid_features.empty or selected_labels.empty:
        raise ModelMainlineError("模型样本缺少可用 Feature 或目标 horizon Label")
    for frame, columns in (
        (valid_features, ("observation_time", "available_time")),
        (
            selected_labels,
            ("decision_time", "label_start_time", "label_end_time", "available_time"),
        ),
    ):
        for column in columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if (valid_features["observation_time"] > valid_features["available_time"]).any():
        raise ModelMainlineError("Feature 在观察时点尚不可见")
    # 收盘到下一收盘标签使用开区间 (decision_time, label_end_time]：
    # 起点价格在决定时点已可见，因此边界相等合法，只有更早的起点才是前视。
    if (selected_labels["decision_time"] > selected_labels["label_start_time"]).any():
        raise ModelMainlineError("Label 不得从决策之前开始")
    valid_features["feature_column"] = (
        valid_features["feature_id"].astype(str)
        + "__w"
        + valid_features["window_sessions"].astype(int).astype(str)
    )
    keys = ["entity_id", "observation_session"]
    if valid_features.duplicated([*keys, "feature_column"]).any():
        raise ModelMainlineError("同一样本的 Feature 列不唯一")
    wide = valid_features.pivot(
        index=keys, columns="feature_column", values="value"
    ).reset_index()
    feature_columns = tuple(
        sorted(str(column) for column in wide.columns if column not in keys)
    )
    if not feature_columns:
        raise ModelMainlineError("模型样本没有数值 Feature")
    feature_meta = (
        valid_features.groupby(keys, sort=True)
        .agg(
            observation_time=("observation_time", "max"),
            feature_available_time=("available_time", "max"),
            feature_lineage_hash=(
                "lineage_hash",
                lambda values: typed_canonical_hash(sorted(map(str, values))),
            ),
        )
        .reset_index()
    )
    label_columns = [
        *keys,
        "decision_time",
        "label_start_time",
        "label_end_time",
        "available_time",
        target_field,
        "lineage_hash",
    ]
    if selected_labels.duplicated(keys).any():
        raise ModelMainlineError("同一样本的目标 horizon Label 不唯一")
    result = wide.merge(
        feature_meta, on=keys, how="inner", validate="one_to_one"
    ).merge(
        selected_labels.loc[:, label_columns],
        on=keys,
        how="inner",
        validate="one_to_one",
    )
    result = result.rename(
        columns={
            "available_time": "label_available_time",
            "lineage_hash": "label_lineage_hash",
            target_field: "target",
        }
    )
    if (result["feature_available_time"] > result["decision_time"]).any():
        raise ModelMainlineError("模型 Feature 晚于决策时点，存在前视偏差")
    result["sample_id"] = (
        result["entity_id"].astype(str)
        + ":"
        + pd.to_datetime(result["observation_session"]).dt.strftime("%Y-%m-%d")
        + f":h{horizon_sessions}"
    )
    result["target"] = pd.to_numeric(result["target"], errors="raise")
    if (
        result["sample_id"].duplicated().any()
        or not np.isfinite(result["target"].to_numpy(float)).all()
    ):
        raise ModelMainlineError("模型 sample_id 必须唯一且 target 必须有限")
    ordered = [
        "sample_id",
        "entity_id",
        "observation_session",
        "observation_time",
        "feature_available_time",
        "decision_time",
        "label_start_time",
        "label_end_time",
        "label_available_time",
        "target",
        "feature_lineage_hash",
        "label_lineage_hash",
        *feature_columns,
    ]
    return result.loc[:, ordered].sort_values(
        ["observation_time", "sample_id"], kind="stable"
    ).reset_index(drop=True), feature_columns


def model_dependency_preflight(
    candidates: Sequence[Mapping[str, object]],
    *,
    simple_model_gate_passed: bool,
    thread_count: int,
) -> dict[str, object]:
    """运行前冻结依赖和确定性边界；不得在运行中静默换模型。"""
    if thread_count != 1:
        raise ModelMainlineError("正式模型只允许 thread_count=1，以保证确定性")
    normalized = _normalize_candidates(candidates)
    missing: list[str] = []
    needs_sklearn = any(item["model_id"] != "lightgbm" for item in normalized)
    required: dict[str, str] = {}
    if needs_sklearn:
        required["scikit-learn"] = _distribution_version("scikit-learn")
    wants_lightgbm = any(item["model_id"] == "lightgbm" for item in normalized)
    if wants_lightgbm and simple_model_gate_passed:
        version = _distribution_version("lightgbm")
        if version == "missing":
            missing.append("lightgbm")
        required["lightgbm"] = version
    if required.get("scikit-learn") == "missing":
        missing.append("scikit-learn")
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION,
        "dependencies": required,
        "missing": sorted(set(missing)),
        "thread_count": thread_count,
        "simple_model_gate_passed": bool(simple_model_gate_passed),
        "lightgbm_state": "execute"
        if wants_lightgbm and simple_model_gate_passed
        else "NOT_RUN",
    }
    payload["preflight_hash"] = typed_canonical_hash(payload)
    if missing:
        raise ModelDependencyError(f"模型依赖缺失: {sorted(set(missing))}")
    return payload


def fit_fold_preprocessor(
    train: pd.DataFrame,
    evaluation: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    feature_selection_k: int | None,
    preprocessing: str,
    split_manifest: object,
    fold_id: str,
    root_seed: int,
    research_identity_hash: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    """按 fit scope 证书只在指定 fold 的训练样本拟合预处理器。"""
    return _fit_transform_fold(
        train,
        evaluation,
        feature_columns=tuple(feature_columns),
        feature_selection_k=feature_selection_k,
        preprocessing=preprocessing,
        split_manifest=split_manifest,
        fold_id=fold_id,
        root_seed=root_seed,
        research_identity_hash=research_identity_hash,
    )


def fit_model_candidate(
    train: pd.DataFrame,
    *,
    candidate: Mapping[str, object],
    target_kind: str,
    feature_columns: Sequence[str],
    root_seed: int,
    thread_count: int,
    preflight_hash: str,
) -> dict[str, object]:
    """拟合静态白名单模型并返回可内容寻址的数值参数。"""
    return _fit_model(
        train,
        candidate=candidate,
        target_kind=target_kind,
        feature_columns=tuple(feature_columns),
        root_seed=root_seed,
        thread_count=thread_count,
        preflight_hash=preflight_hash,
    )


def predict_model(model: Mapping[str, object], frame: pd.DataFrame) -> np.ndarray:
    """从已封存线性模型参数重算预测。"""
    return _predict(model, frame)


def score_model(
    actual: np.ndarray, predicted: np.ndarray, *, objective: str, target_kind: str
) -> float:
    """计算有限白名单目标指标。"""
    return _metric(actual, predicted, objective, target_kind)


def normalize_model_candidates(
    candidates: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """规范化并校验静态候选全集。"""
    return _normalize_candidates(candidates)


def evaluate_locked_holdout(
    development_samples: pd.DataFrame,
    *,
    holdout_preflight: Callable[[], Mapping[str, object]],
    holdout_loader: Callable[[], pd.DataFrame],
    development_ids: Sequence[str],
    holdout_ids: Sequence[str],
    holdout_start: str,
    holdout_end: str,
    feature_columns: Sequence[str],
    selected_candidate: Mapping[str, object],
    target_kind: str,
    objective: str,
    preprocessing: str,
    feature_selection_k: int | None,
    research_identity_hash: str,
    data_snapshot_hash: str,
    selection_hash: str,
    package_hash: str,
    implementation_hash: str,
    actor: str,
    reason: str,
    unlock_at: datetime,
    fixed_clock: datetime,
    ledger_root: str | Path,
    root_seed: int,
    thread_count: int = 1,
) -> dict[str, object]:
    """开发拟合完成后原子记录 opened，再首次读取 holdout 值。"""
    if fixed_clock.tzinfo is None or fixed_clock.utcoffset() is None:
        raise ModelMainlineError("locked holdout fixed_clock 必须包含时区")
    development_ids = tuple(sorted(map(str, development_ids)))
    holdout_ids = tuple(sorted(map(str, holdout_ids)))
    if (
        not development_ids
        or not holdout_ids
        or set(development_ids) & set(holdout_ids)
    ):
        raise ModelMainlineError("development/holdout 样本必须非空且互斥")
    if not isinstance(development_samples, pd.DataFrame):
        raise ModelMainlineError("locked holdout 必须显式提供 development 样本")
    if "sample_id" not in development_samples.columns:
        raise ModelMainlineError("locked holdout development 样本缺少 sample_id")
    if set(development_samples["sample_id"].astype(str)) != set(development_ids):
        raise ModelMainlineError("development 样本必须与冻结样本全集完全一致")
    candidate_id = typed_canonical_hash(dict(selected_candidate))
    for value, label in (
        (data_snapshot_hash, "data_snapshot_hash"),
        (package_hash, "package_hash"),
        (implementation_hash, "implementation_hash"),
    ):
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ModelMainlineError(f"{label} 必须是 sha256")
    try:
        holdout_start_date = date.fromisoformat(holdout_start)
        holdout_end_date = date.fromisoformat(holdout_end)
    except ValueError as exc:
        raise ModelMainlineError("holdout start/end 必须是 ISO 日期") from exc
    if holdout_end_date < holdout_start_date:
        raise ModelMainlineError("holdout end 不能早于 start")
    freeze_payload = {
        "parent_research_purpose": research_identity_hash,
        "data_snapshot": data_snapshot_hash,
        "holdout_split": {
            "split_id": typed_canonical_hash(list(holdout_ids)),
            "start": holdout_start_date.isoformat(),
            "end": holdout_end_date.isoformat(),
            "sample_ids": list(holdout_ids),
        },
        "mode": "single_candidate_confirmation",
        "candidates": [candidate_id],
        "selection_rule": {
            "method": "frozen_selection_hash_v1",
            "uses_validation": True,
        },
        "validation": {
            "purpose": "selection",
            "rule": {"selection_hash": selection_hash, "objective": objective},
        },
        "primary_estimand": objective,
        "direction": "greater" if objective in {"accuracy"} else "less",
        "alpha": 0.05,
        "multiple_testing": None,
        "random_protocol": {"combinations": [], "repetitions": 0, "seed": root_seed},
        "failure_policy": "opened_then_failure_is_consumed",
        "exposed_intervals": [],
        "package_plan_identity": package_hash,
        "implementation_identity": implementation_hash,
        "aliases": {
            "run": research_identity_hash,
            "package": package_hash,
            "family": candidate_id,
        },
    }
    plan = HoldoutAccessPlan.build(
        freeze_payload=freeze_payload,
        actor=actor,
        reason=reason,
        unlock_at=unlock_at,
    )
    ledger_path = Path(ledger_root).resolve() / plan.holdout_identity_hash
    ledger = PersistentHoldoutLedger(ledger_path, plan)
    prior = ledger.verify()
    if prior.get("status") != "frozen":
        raise ValidationError(
            "同一研究目的和 holdout 已经 prepared/opened，不能重复读取"
        )
    frame = _normalize_model_samples(
        development_samples, feature_columns, target_kind
    ).set_index("sample_id", drop=False)
    train = frame.loc[list(development_ids)].copy()
    preflight = model_dependency_preflight(
        (selected_candidate,),
        simple_model_gate_passed=True,
        thread_count=thread_count,
    )
    transformed_train, preprocessor = _fit_transform_unscoped(
        train,
        train.iloc[:0].copy(),
        tuple(feature_columns),
        feature_selection_k,
        preprocessing,
    )
    model = _fit_model(
        transformed_train["train"],
        candidate=dict(selected_candidate),
        target_kind=target_kind,
        feature_columns=tuple(preprocessor["selected_columns"]),
        root_seed=root_seed,
        thread_count=thread_count,
        preflight_hash=str(preflight["preflight_hash"]),
    )
    prepared = ledger.prepare(prepared_at=fixed_clock, preflight=holdout_preflight)
    opened = ledger.open(
        opening_id=f"holdout:{plan.holdout_identity_hash[:16]}",
        token_hash=plan.token_hash,
        actor=actor,
        reason=reason,
        fixed_clock=fixed_clock,
        prepared_hash=str(prepared["prepared_hash"]),
    )
    try:
        raw_holdout = holdout_loader()
        holdout_frame = _normalize_model_samples(
            raw_holdout, feature_columns, target_kind
        ).set_index("sample_id", drop=False)
        if set(holdout_frame.index) != set(holdout_ids):
            raise ModelMainlineError("holdout loader 必须只返回冻结样本全集")
        if "label_available_time" in holdout_frame.columns:
            available = pd.to_datetime(
                holdout_frame.loc[list(holdout_ids), "label_available_time"],
                utc=True,
                errors="raise",
            )
            if (available > fixed_clock).any():
                raise ModelMainlineError(
                    "locked holdout Label 在 fixed_clock 时尚不可见"
                )
        holdout = holdout_frame.loc[list(holdout_ids)].copy()
        transformed_holdout, _ = _apply_frozen_preprocessor(
            holdout, tuple(feature_columns), preprocessor
        )
        predictions = _predict(model, transformed_holdout)
        metric = _metric(
            holdout["target"].to_numpy(float), predictions, objective, target_kind
        )
        rows = _prediction_records(
            transformed_holdout,
            predictions,
            "selected",
            "locked_holdout",
            "holdout",
            preprocessor["preprocessor_hash"],
            model["model_hash"],
        )
        result_hash = typed_canonical_hash(
            {"predictions": rows, "objective": objective, "metric": metric}
        )
        terminal = ledger.finish(
            opened_hash=str(opened["opened_hash"]),
            status="committed",
            result_hash=result_hash,
        )
    except Exception as exc:
        terminal = ledger.finish(
            opened_hash=str(opened["opened_hash"]),
            status="consumed_failed",
            result_hash=typed_canonical_hash({"error": type(exc).__name__}),
            reason=type(exc).__name__,
        )
        raise
    return {
        "contract_version": WALK_FORWARD_MODEL_VERSION,
        "status": "committed",
        "predictions": rows,
        "objective": objective,
        "metric": metric,
        "holdout_identity_hash": plan.holdout_identity_hash,
        "plan_hash": plan.plan_hash,
        "prepared_hash": prepared["prepared_hash"],
        "opened_hash": opened["opened_hash"],
        "terminal_hash": terminal["terminal_hash"],
        "result_hash": result_hash,
    }


def _fit_transform_fold(
    train: pd.DataFrame,
    evaluation: pd.DataFrame,
    *,
    feature_columns: tuple[str, ...],
    feature_selection_k: int | None,
    preprocessing: str,
    split_manifest: object,
    fold_id: str,
    root_seed: int,
    research_identity_hash: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    component_id = f"preprocessor:{fold_id}"
    seed_manifest = build_seed_manifest(
        root_seed=root_seed,
        research_identity_hash=research_identity_hash,
        component_ids=(component_id,),
    )
    certificate = issue_fit_scope(
        component_id=component_id,
        component_kind="transformer",
        split_manifest=split_manifest,
        fold_id=fold_id,
        input_columns=feature_columns,
        parameters={
            "preprocessing": preprocessing,
            "feature_selection_k": feature_selection_k,
        },
        code_hash=typed_canonical_hash(
            {"implementation": "walk-forward-preprocessor-v1"}
        ),
        environment_hash=typed_canonical_hash(
            {"numpy": np.__version__, "pandas": pd.__version__}
        ),
        seed_manifest=seed_manifest,
    )
    transformed, artifact = _fit_transform_unscoped(
        train,
        evaluation,
        feature_columns,
        feature_selection_k,
        preprocessing,
    )
    binding = bind_fit_artifact(
        certificate,
        actual_fit_ids=tuple(train["sample_id"]),
        output_artifact_hash=str(artifact["preprocessor_hash"]),
    )
    artifact["fit_scope_certificate_hash"] = certificate.certificate_hash
    artifact["fit_binding_hash"] = binding.binding_hash
    return transformed, artifact


def _fit_transform_unscoped(
    train: pd.DataFrame,
    evaluation: pd.DataFrame,
    feature_columns: tuple[str, ...],
    feature_selection_k: int | None,
    preprocessing: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    if preprocessing not in {"median_standardize_v1", "median_only_v1"}:
        raise ModelMainlineError("预处理只允许训练 fold 内中位数填充及可选标准化")
    if feature_selection_k is not None and (
        type(feature_selection_k) is not int or feature_selection_k < 1
    ):
        raise ModelMainlineError("feature_selection_k 必须为正整数或空")
    x_train = train.loc[:, feature_columns].astype(float)
    medians = x_train.median(axis=0).fillna(0.0)
    filled = x_train.fillna(medians)
    means = filled.mean(axis=0)
    scales = filled.std(axis=0, ddof=0).replace(0.0, 1.0)
    selected = list(feature_columns)
    if feature_selection_k is not None and feature_selection_k < len(selected):
        target = train["target"].astype(float)
        scores = []
        for column in selected:
            correlation = filled[column].corr(target)
            scores.append(
                (
                    0.0
                    if not math.isfinite(float(correlation))
                    else abs(float(correlation)),
                    column,
                )
            )
        selected = [
            column
            for _, column in sorted(scores, key=lambda item: (-item[0], item[1]))[
                :feature_selection_k
            ]
        ]

    def transform(frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        values = result.loc[:, feature_columns].astype(float).fillna(medians)
        if preprocessing == "median_standardize_v1":
            values = (values - means) / scales
        result.loc[:, feature_columns] = values
        return result.loc[
            :, [*result.columns.difference(feature_columns, sort=False), *selected]
        ]

    artifact = {
        "preprocessing": preprocessing,
        "fit_sample_ids_hash": typed_canonical_hash(
            sorted(map(str, train["sample_id"]))
        ),
        "input_columns": list(feature_columns),
        "selected_columns": selected,
        "medians": {key: float(medians[key]) for key in feature_columns},
        "means": {key: float(means[key]) for key in feature_columns},
        "scales": {key: float(scales[key]) for key in feature_columns},
    }
    artifact["preprocessor_hash"] = typed_canonical_hash(artifact)
    return {"train": transform(train), "evaluation": transform(evaluation)}, artifact


def _apply_frozen_preprocessor(
    frame: pd.DataFrame,
    feature_columns: tuple[str, ...],
    artifact: Mapping[str, object],
) -> tuple[pd.DataFrame, str]:
    """只应用 development 已冻结的预处理参数，不重新拟合 holdout。"""

    medians = pd.Series(artifact["medians"], dtype=float)
    means = pd.Series(artifact["means"], dtype=float)
    scales = pd.Series(artifact["scales"], dtype=float)
    selected = tuple(str(item) for item in artifact["selected_columns"])
    values = frame.loc[:, feature_columns].astype(float).fillna(medians)
    if artifact["preprocessing"] == "median_standardize_v1":
        values = (values - means) / scales
    result = frame.copy()
    result.loc[:, feature_columns] = values
    transformed = result.loc[
        :, [*result.columns.difference(feature_columns, sort=False), *selected]
    ]
    return transformed, str(artifact["preprocessor_hash"])




def _fit_model(
    train: pd.DataFrame,
    *,
    candidate: Mapping[str, object],
    target_kind: str,
    feature_columns: tuple[str, ...],
    root_seed: int,
    thread_count: int,
    preflight_hash: str,
) -> dict[str, object]:
    if thread_count != 1:
        raise ModelMainlineError("正式模型拟合不允许多线程非确定性")
    model_id = str(candidate["model_id"])
    allowed = (
        REGRESSION_MODELS if target_kind == "regression" else CLASSIFICATION_MODELS
    )
    if model_id not in allowed:
        raise ModelMainlineError(f"{target_kind} 不支持模型 {model_id}")
    x = train.loc[:, feature_columns].to_numpy(float)
    y = train["target"].to_numpy(float)
    if not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ModelMainlineError("模型拟合矩阵必须全部有限")
    if target_kind == "classification":
        classes, counts = np.unique(y, return_counts=True)
        minimum_count = int(candidate.get("minimum_class_count", 2))
        minimum_fraction = float(candidate.get("minimum_class_fraction", 0.1))
        if (
            len(classes) != 2
            or counts.min() < minimum_count
            or counts.min() / len(y) < minimum_fraction
        ):
            raise CandidateFitRejected(
                "分类样本占比 Gate 未通过",
                reason_code="insufficient_class_support",
            )
    if model_id == "ridge":
        linear_model = importlib.import_module("sklearn.linear_model")
        estimator = linear_model.Ridge(
            alpha=float(candidate.get("alpha", 1.0)), solver="svd"
        )
    elif model_id == "elastic_net":
        linear_model = importlib.import_module("sklearn.linear_model")
        estimator = linear_model.ElasticNet(
            alpha=float(candidate.get("alpha", 0.01)),
            l1_ratio=float(candidate.get("l1_ratio", 0.5)),
            fit_intercept=True,
            max_iter=10_000,
            selection="cyclic",
        )
    elif model_id == "huber":
        linear_model = importlib.import_module("sklearn.linear_model")
        estimator = linear_model.HuberRegressor(
            epsilon=float(candidate.get("epsilon", 1.35)),
            alpha=float(candidate.get("alpha", 0.0001)),
            max_iter=1_000,
        )
    elif model_id == "logistic":
        linear_model = importlib.import_module("sklearn.linear_model")
        estimator = linear_model.LogisticRegression(
            C=float(candidate.get("C", 1.0)),
            solver="liblinear",
            random_state=root_seed,
            max_iter=1_000,
            n_jobs=1,
        )
    else:
        lightgbm = importlib.import_module("lightgbm")
        common = {
            "n_estimators": int(candidate.get("n_estimators", 100)),
            "num_leaves": int(candidate.get("num_leaves", 15)),
            "learning_rate": float(candidate.get("learning_rate", 0.05)),
            "max_depth": int(candidate.get("max_depth", -1)),
            "random_state": root_seed,
            "n_jobs": 1,
            "deterministic": True,
            "force_col_wise": True,
            "verbosity": -1,
        }
        estimator = (
            lightgbm.LGBMRegressor(**common)
            if target_kind == "regression"
            else lightgbm.LGBMClassifier(**common)
        )
    estimator.fit(x, y)
    if model_id == "lightgbm":
        payload = {
            "contract_version": WALK_FORWARD_MODEL_VERSION,
            "model_id": model_id,
            "target_kind": target_kind,
            "feature_columns": list(feature_columns),
            "booster_model": estimator.booster_.model_to_string(),
            "parameters": dict(sorted(candidate.items())),
            "fit_sample_ids_hash": typed_canonical_hash(
                sorted(map(str, train["sample_id"]))
            ),
            "dependency_versions": {"lightgbm": _distribution_version("lightgbm")},
            "preflight_hash": preflight_hash,
            "root_seed": root_seed,
        }
        payload["model_hash"] = typed_canonical_hash(payload)
        return payload
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION,
        "model_id": model_id,
        "target_kind": target_kind,
        "feature_columns": list(feature_columns),
        "coef": np.asarray(estimator.coef_, dtype=float).reshape(-1).tolist(),
        "intercept": float(
            np.asarray(estimator.intercept_, dtype=float).reshape(-1)[0]
        ),
        "parameters": dict(sorted(candidate.items())),
        "fit_sample_ids_hash": typed_canonical_hash(
            sorted(map(str, train["sample_id"]))
        ),
        "dependency_versions": {"scikit-learn": _distribution_version("scikit-learn")},
        "preflight_hash": preflight_hash,
        "root_seed": root_seed,
    }
    payload["model_hash"] = typed_canonical_hash(payload)
    return payload


def _predict(model: Mapping[str, object], frame: pd.DataFrame) -> np.ndarray:
    columns = tuple(str(item) for item in model["feature_columns"])
    x = frame.loc[:, columns].to_numpy(float)
    if model["model_id"] == "lightgbm":
        lightgbm = importlib.import_module("lightgbm")
        booster = lightgbm.Booster(model_str=str(model["booster_model"]))
        prediction = np.asarray(booster.predict(x), dtype=float)
        return prediction
    coef = np.asarray(model["coef"], dtype=float)
    linear = x @ coef + float(model["intercept"])
    if model["target_kind"] == "classification":
        return 1.0 / (1.0 + np.exp(-np.clip(linear, -40.0, 40.0)))
    return linear


def _metric(
    actual: np.ndarray, predicted: np.ndarray, objective: str, target_kind: str
) -> float:
    if target_kind == "regression" and objective == "neg_mean_squared_error":
        return -float(np.mean((actual - predicted) ** 2))
    if target_kind == "regression" and objective == "neg_mean_absolute_error":
        return -float(np.mean(np.abs(actual - predicted)))
    if target_kind == "classification" and objective == "accuracy":
        return float(np.mean((predicted >= 0.5) == actual.astype(bool)))
    raise ModelMainlineError("objective 与 target_kind 不匹配")


def _prediction_records(
    frame: pd.DataFrame,
    predictions: np.ndarray,
    candidate_id: str,
    fold_id: str,
    stage: str,
    preprocessor_hash: str,
    model_hash: str,
) -> list[dict[str, object]]:
    return [
        {
            "candidate_id": candidate_id,
            "fold_id": fold_id,
            "stage": stage,
            "sample_id": str(row.sample_id),
            "observation_time": pd.Timestamp(row.observation_time).isoformat(),
            "label_start_time": pd.Timestamp(row.label_start_time).isoformat(),
            "label_end_time": pd.Timestamp(row.label_end_time).isoformat(),
            "prediction": float(prediction),
            "actual": float(row.target),
            "preprocessor_hash": preprocessor_hash,
            "model_hash": model_hash,
        }
        for row, prediction in zip(
            frame.itertuples(index=False), predictions, strict=True
        )
    ]


def _normalize_model_samples(
    samples: pd.DataFrame, feature_columns: Sequence[str], target_kind: str
) -> pd.DataFrame:
    required = {
        "sample_id",
        "observation_time",
        "label_start_time",
        "label_end_time",
        "target",
        *feature_columns,
    }
    _require_columns(samples, required, "model samples")
    if target_kind not in {"regression", "classification"}:
        raise ModelMainlineError("target_kind 只支持 regression/classification")
    if not feature_columns or len(set(feature_columns)) != len(feature_columns):
        raise ModelMainlineError("feature_columns 必须非空且唯一")
    frame = samples.copy()
    frame["sample_id"] = frame["sample_id"].astype(str)
    if frame["sample_id"].eq("").any() or frame["sample_id"].duplicated().any():
        raise ModelMainlineError("模型 sample_id 必须非空且唯一")
    for column in ("observation_time", "label_start_time", "label_end_time"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if (
        (frame["observation_time"] > frame["label_start_time"])
        | (frame["label_start_time"] >= frame["label_end_time"])
    ).any():
        raise ModelMainlineError(
            "模型样本时钟必须满足 observation <= label_start < label_end"
        )
    frame["target"] = pd.to_numeric(frame["target"], errors="raise")
    if not np.isfinite(frame["target"].to_numpy(float)).all():
        raise ModelMainlineError("target 必须有限")
    if target_kind == "classification" and not set(frame["target"].unique()) <= {
        0,
        1,
        0.0,
        1.0,
    }:
        raise ModelMainlineError("分类 target 只允许 0/1")
    for column in feature_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        values = frame[column].dropna().to_numpy(float)
        if not np.isfinite(values).all():
            raise ModelMainlineError("Feature 只允许有限数或缺失值")
    return frame.sort_values(
        ["observation_time", "sample_id"], kind="stable"
    ).reset_index(drop=True)


def _normalize_candidates(
    candidates: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    if not isinstance(candidates, (list, tuple)) or not candidates:
        raise ModelMainlineError("模型候选不能为空")
    normalized: list[dict[str, object]] = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise ModelMainlineError("每个模型候选必须是参数对象")
        item = dict(candidate)
        model_id = str(item.get("model_id", ""))
        if model_id not in SUPPORTED_MODELS:
            raise ModelMainlineError(f"模型不在静态白名单: {model_id}")
        item["model_id"] = model_id
        unknown = set(item) - MODEL_PARAMETER_FIELDS[model_id]
        if unknown:
            raise ModelMainlineError(f"{model_id} 包含未登记参数: {sorted(unknown)}")
        _validate_candidate_parameters(item)
        normalized.append(item)
    if len({typed_canonical_hash(item) for item in normalized}) != len(normalized):
        raise ModelMainlineError("模型候选不能重复")
    return normalized


def _validate_candidate_parameters(candidate: Mapping[str, object]) -> None:
    model_id = str(candidate["model_id"])

    def finite_float(field: str, default: float) -> float:
        try:
            value = float(candidate.get(field, default))
        except (TypeError, ValueError) as exc:
            raise ModelMainlineError(f"{model_id}.{field} 必须是有限数") from exc
        if not math.isfinite(value):
            raise ModelMainlineError(f"{model_id}.{field} 必须是有限数")
        return value

    def strict_int(field: str, default: int) -> int:
        value = candidate.get(field, default)
        if type(value) is not int:
            raise ModelMainlineError(f"{model_id}.{field} 必须是整数")
        return value

    if model_id == "ridge" and finite_float("alpha", 1.0) < 0:
        raise ModelMainlineError("ridge.alpha 不能为负")
    if model_id == "elastic_net":
        if finite_float("alpha", 0.01) <= 0:
            raise ModelMainlineError("elastic_net.alpha 必须大于零")
        l1_ratio = finite_float("l1_ratio", 0.5)
        if not 0 <= l1_ratio <= 1:
            raise ModelMainlineError("elastic_net.l1_ratio 必须在 [0, 1]")
    if model_id == "huber":
        if finite_float("alpha", 0.0001) < 0 or finite_float("epsilon", 1.35) <= 1:
            raise ModelMainlineError("huber.alpha 不能为负且 epsilon 必须大于 1")
    if model_id == "logistic" and finite_float("C", 1.0) <= 0:
        raise ModelMainlineError("logistic.C 必须大于零")
    if model_id == "lightgbm":
        if strict_int("n_estimators", 100) < 1 or strict_int("num_leaves", 15) < 2:
            raise ModelMainlineError(
                "lightgbm.n_estimators 必须为正且 num_leaves 至少为 2"
            )
        max_depth = strict_int("max_depth", -1)
        if max_depth == 0 or max_depth < -1 or finite_float("learning_rate", 0.05) <= 0:
            raise ModelMainlineError(
                "lightgbm.max_depth 只允许 -1 或正整数且 learning_rate 必须大于零"
            )
    if model_id in CLASSIFICATION_MODELS:
        if strict_int("minimum_class_count", 2) < 1:
            raise ModelMainlineError(f"{model_id}.minimum_class_count 必须为正整数")
        fraction = finite_float("minimum_class_fraction", 0.1)
        if not 0 < fraction <= 0.5:
            raise ModelMainlineError(
                f"{model_id}.minimum_class_fraction 必须在 (0, 0.5]"
            )


def _distribution_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "missing"


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ModelMainlineError(f"{label} 缺少字段: {sorted(missing)}")


__all__ = [
    "CandidateFitRejected",
    "ModelDependencyError",
    "ModelMainlineError",
    "SUPPORTED_MODELS",
    "WALK_FORWARD_MODEL_VERSION",
    "assemble_daily_model_samples",
    "evaluate_locked_holdout",
    "fit_fold_preprocessor",
    "fit_model_candidate",
    "model_dependency_preflight",
    "normalize_model_candidates",
    "predict_model",
    "score_model",
]
