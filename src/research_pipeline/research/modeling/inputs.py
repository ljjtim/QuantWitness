"""表格与序列模型共用的日频输入事实、矩阵和异常。"""
from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError


class ModelMainlineError(MainlineError):
    error_code = "research_model_mainline_invalid"


class QlibModelError(MainlineError):
    error_code = "research_qlib_model_invalid"


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ModelMainlineError(f"{label} 缺少字段: {sorted(missing)}")


def assemble_daily_feature_context(features: pd.DataFrame) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """独立于标签资格保留日频历史特征，特征值不做填补或标准化。"""
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
    _require_columns(features, feature_required, "features")
    valid_features = features.loc[features["status"].astype(str) == "ok"].copy()
    if valid_features.empty:
        raise ModelMainlineError("模型样本缺少可用 Feature")
    for column in ("observation_time", "available_time"):
        valid_features[column] = pd.to_datetime(valid_features[column], utc=True, errors="raise")
    if valid_features[["observation_time", "available_time"]].isna().any().any():
        raise ModelMainlineError("Feature 观察和可见时间不得缺失")
    if (valid_features["observation_time"] > valid_features["available_time"]).any():
        raise ModelMainlineError("Feature 在观察时点尚不可见")
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
            feature_count=("feature_column", "count"),
            feature_lineage_hash=(
                "lineage_hash",
                lambda values: typed_canonical_hash(sorted(map(str, values))),
            ),
        )
        .reset_index()
    )
    return wide.merge(feature_meta, on=keys, validate="one_to_one"), feature_columns


def qlib_matrix(frame: pd.DataFrame, features: Sequence[str], *, label: bool) -> pd.DataFrame:
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
