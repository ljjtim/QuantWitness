"""Qlib 六阶段模型研究的时间事实、评价与一次性 holdout。"""

from __future__ import annotations

from datetime import date, datetime
import importlib.metadata
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
)


WALK_FORWARD_MODEL_VERSION = "research-qlib-model-v1"


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
    result["horizon_sessions"] = horizon_sessions
    ordered = [
        "horizon_sessions",
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


def model_dependency_preflight(candidates, *, thread_count: int) -> dict[str, object]:
    """冻结本次 Qlib 候选实际依赖及 CPU 单线程约束。"""
    if thread_count != 1:
        raise ModelMainlineError("Qlib 首批只支持 thread_count=1")
    normalized = normalize_model_candidates(candidates)
    distributions = {"pyqlib", "numpy", "pandas"}
    for candidate in normalized:
        distributions.add({"LinearModel": "scikit-learn", "LGBModel": "lightgbm", "XGBModel": "xgboost"}[candidate["model"]["class"]])
    versions = {name: _distribution_version(name) for name in sorted(distributions)}
    missing = [name for name, version in versions.items() if version == "missing"]
    if missing:
        raise ModelDependencyError(f"Qlib 模型依赖缺失: {missing}")
    payload = {"contract_version": WALK_FORWARD_MODEL_VERSION, "dependencies": versions, "thread_count": 1}
    payload["preflight_hash"] = typed_canonical_hash(payload)
    return payload


def score_model(
    actual: np.ndarray, predicted: np.ndarray, *, objective: str, target_kind: str
) -> float:
    """计算有限白名单目标指标。"""
    return _metric(actual, predicted, objective, target_kind)


def normalize_model_candidates(
    candidates: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """规范化并校验静态候选全集。"""
    from .qlib import normalize_candidates
    return normalize_candidates(candidates)


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
    validation_sessions: int,
    output_root: str | Path,
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
        holdout_boundary = pd.Timestamp(holdout_start)
        if holdout_boundary.tzinfo is None:
            holdout_boundary = holdout_boundary.tz_localize("UTC")
        holdout_start_date = holdout_boundary.date()
        holdout_end_date = date.fromisoformat(holdout_end)
    except ValueError as exc:
        raise ModelMainlineError("holdout start 必须是 ISO 日期或时点，end 必须是 ISO 日期") from exc
    if holdout_end_date < holdout_start_date:
        raise ModelMainlineError("holdout end 不能早于 start")
    _metric(np.array([0.0]), np.array([0.0]), objective, target_kind)
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
        "direction": "greater",
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
    if "label_available_time" in train:
        available = pd.to_datetime(train["label_available_time"], utc=True, errors="raise")
        if available.isna().any() or (available > holdout_boundary).any():
            raise ModelMainlineError("development 标签在 locked holdout 拟合时尚不可见")
    from .qlib import fit_bundle, predict_bundle, evaluation_labels
    model_dependency_preflight((selected_candidate,), thread_count=thread_count)
    sessions = sorted(pd.to_datetime(train["observation_session"], utc=True).unique())
    if validation_sessions < 1 or len(sessions) <= validation_sessions:
        raise ModelMainlineError("最终开发区必须容纳 train 和声明的 validation_sessions")
    valid_boundary = sessions[-validation_sessions]
    observations = pd.to_datetime(train["observation_session"], utc=True)
    valid = train.loc[observations >= valid_boundary].copy()
    fit_cutoff = pd.to_datetime(valid["decision_time"], utc=True).min()
    fit_train = train.loc[(observations < valid_boundary)
                         & (pd.to_datetime(train["label_end_time"], utc=True) < fit_cutoff)
                         & (pd.to_datetime(train["label_available_time"], utc=True) <= fit_cutoff)].copy()
    model = fit_bundle(fit_train, valid, candidate=selected_candidate,
                       feature_columns=feature_columns, output_root=output_root,
                       bundle_path="bundles/holdout/0", root_seed=root_seed,
                       fit_scope_ref="holdout-final-development")
    model.update(candidate_id=f"candidate_{candidate_id[:16]}", fold_id="locked_holdout", status="fitted")
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
        predictions = predict_bundle(output_root, model, holdout)
        evaluation_target = evaluation_labels(output_root, model, holdout)
        metric = _metric(evaluation_target, predictions, objective, target_kind)
        rows = _prediction_records(
            holdout,
            predictions,
            str(model["candidate_id"]),
            "locked_holdout",
            "holdout",
            model["config_path"],
            model["model_path"],
        )
        for row, evaluation_label in zip(rows, evaluation_target, strict=True):
            row["evaluation_label"] = float(evaluation_label)
            row["score_semantics"] = "ranking_score" if any(p["class"] == "CSRankNorm" for p in selected_candidate["processors"]["learn"]) else "raw_return_prediction"
        result_hash = typed_canonical_hash(
            {"predictions": [{**row, "observation_session": row["observation_session"].isoformat()} for row in rows], "objective": objective, "metric": metric}
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
        "model_row": model,
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
            "entity_id": str(row.entity_id),
            "observation_session": pd.Timestamp(row.observation_session).date(),
            "horizon_sessions": int(row.horizon_sessions),
            "observation_time": pd.Timestamp(row.observation_time).isoformat(),
            "feature_available_time": pd.Timestamp(row.feature_available_time).isoformat(),
            "decision_time": pd.Timestamp(row.decision_time).isoformat(),
            "label_available_time": pd.Timestamp(row.label_available_time).isoformat(),
            "raw_label": float(row.target),
            "label_start_time": pd.Timestamp(row.label_start_time).isoformat(),
            "label_end_time": pd.Timestamp(row.label_end_time).isoformat(),
            "prediction": float(prediction),
            "actual": float(row.target),
            "processor_ref": preprocessor_hash,
            "model_ref": model_hash,
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
    if target_kind != "regression":
        raise ModelMainlineError("Qlib 首批只支持 regression")
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
    "WALK_FORWARD_MODEL_VERSION",
    "assemble_daily_model_samples",
    "evaluate_locked_holdout",
    "model_dependency_preflight",
    "normalize_model_candidates",
    "score_model",
]
