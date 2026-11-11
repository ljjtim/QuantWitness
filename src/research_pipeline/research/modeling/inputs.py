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
    if features.attrs.get("model_feature_value_names") is not None:
        return _assemble_vector_feature_context(features)
    valid_features = features.loc[
        features["status"].astype(str).eq("ok"),
        [column for column in features.columns if column in feature_required],
    ].copy()
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


def _assemble_vector_feature_context(features):
    """固定命名向量直接成为宽表，缺数值留给冻结infer处理器。"""
    names = features.attrs["model_feature_value_names"]
    if (not isinstance(names, (list, tuple)) or not names or len(set(names)) != len(names)
            or any(not isinstance(name, str) or not name for name in names)):
        raise ModelMainlineError("向量Feature必须声明固定非空且唯一的名称")
    frame = features.loc[features.status.astype(str).eq("ok")].copy()
    if frame.empty:
        raise ModelMainlineError("模型样本缺少可用Feature")
    keys = ["entity_id", "observation_session"]
    if frame.duplicated(keys).any():
        raise ModelMainlineError("同一样本的Feature向量不唯一")
    windows = frame.window_sessions.unique()
    if len(windows) != 1 or int(windows[0]) < 1:
        raise ModelMainlineError("向量Feature窗口必须固定且为正")
    for field in ("observation_time", "available_time"):
        frame[field] = pd.to_datetime(frame[field], utc=True, errors="raise")
    if frame[["observation_time", "available_time"]].isna().any().any():
        raise ModelMainlineError("Feature观察和可见时间不得缺失")
    if (frame.observation_time > frame.available_time).any():
        raise ModelMainlineError("Feature在观察时点尚不可见")
    try:
        values = np.stack(frame.value.to_numpy()).astype(float, copy=False)
    except (ValueError, TypeError) as exc:
        raise ModelMainlineError("Feature向量长度或数值类型不一致") from exc
    if values.shape != (len(frame), len(names)) or np.isinf(values).any():
        raise ModelMainlineError("Feature向量长度与名称不一致或含无穷值")
    columns = tuple(name + "__w" + str(int(windows[0])) for name in names)
    result = frame.loc[:, keys + ["observation_time"]].reset_index(drop=True)
    result["feature_available_time"] = frame.available_time.to_numpy()
    result["feature_count"] = len(names)
    result["feature_lineage_hash"] = frame.lineage_hash.map(lambda value: typed_canonical_hash([str(value)])).to_numpy()
    result = pd.concat([result, pd.DataFrame(values, columns=columns)], axis=1)
    return result, tuple(sorted(columns))


def _qlib_input_index(frame: pd.DataFrame, features: Sequence[str]) -> pd.MultiIndex:
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
    return index


def _qlib_target(frame: pd.DataFrame) -> np.ndarray:
    target = frame["target"].to_numpy(dtype=float)
    if not np.isfinite(target).all():
        raise QlibModelError("Qlib label 必须有限")
    return target


def qlib_label_matrix(frame: pd.DataFrame, features: Sequence[str]) -> pd.DataFrame:
    """评价只物化标签，逐列保留数值转换及无穷值拒绝。"""
    index = _qlib_input_index(frame, features)
    for name in features:
        if np.isinf(frame[name].to_numpy(dtype=float)).any():
            raise QlibModelError("Qlib feature 不允许无穷值")
    return pd.DataFrame(_qlib_target(frame), index=index,
        columns=pd.MultiIndex.from_tuples([("label", "target")])).sort_index()


def qlib_matrix(frame: pd.DataFrame, features: Sequence[str], *, label: bool) -> pd.DataFrame:
    index = _qlib_input_index(frame, features)
    values = frame.loc[:, list(features)].to_numpy(dtype=float)
    result = pd.DataFrame(values, index=index, columns=pd.MultiIndex.from_product([["feature"], features]))
    if np.isinf(values).any():
        raise QlibModelError("Qlib feature 不允许无穷值")
    if label:
        result[("label", "target")] = _qlib_target(frame)
    return result.sort_index()
