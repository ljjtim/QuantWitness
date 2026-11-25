"""显式模型预测资格：历史上下文不能代替当时可用候选。"""
import numpy as np
import pandas as pd
from .inputs import ModelMainlineError

INDEX_COLUMNS = ("entity_id", "observation_session", "observation_time", "decision_time",
                 "horizon_sessions", "eligibility_available_time", "lineage_hash")


def validate_prediction_index(frame, *, horizon_sessions):
    if not set(INDEX_COLUMNS) <= set(frame):
        raise ModelMainlineError("预测资格索引缺少身份、时点或来源")
    result = frame.loc[frame.horizon_sessions.eq(horizon_sessions), list(INDEX_COLUMNS)].copy()
    if result.empty or result.isna().any().any():
        raise ModelMainlineError("预测资格索引必须非空且字段完整")
    for field in ("observation_time", "decision_time", "eligibility_available_time"):
        if any(pd.Timestamp(value).tzinfo is None for value in result[field]):
            raise ModelMainlineError("预测资格时间必须带时区")
        result[field] = pd.to_datetime(result[field], utc=True, errors="raise")
    if ((result.observation_time > result.decision_time) |
        (result.eligibility_available_time > result.decision_time)).any():
        raise ModelMainlineError("预测资格在决策时点尚不可见")
    if result.duplicated(["entity_id", "observation_session", "horizon_sessions"]).any():
        raise ModelMainlineError("预测资格键必须唯一")
    if any(not isinstance(x,str) or not x.strip() for field in ("entity_id", "lineage_hash") for x in result[field]):
        raise ModelMainlineError("预测资格身份与来源不能为空")
    return result



def _iso_times(values: pd.Series) -> np.ndarray:
    # 混合偏移量保留各行原时区，不能用 utc=True 统一改写。
    try:
        index = pd.DatetimeIndex(values)
    except (ValueError, TypeError):
        return values.map(lambda value: None if pd.isna(value) else pd.Timestamp(value).isoformat()).to_numpy()
    # 截面的证券共享时点。每个唯一时点仅格式化一次，并按原位置还原。
    positions, unique = pd.factorize(index, sort=False)
    if len(unique) == 0:
        return np.full(len(index), None, dtype=object)
    local = unique.tz_localize(None) if unique.tz is not None else unique
    output = pd.Series(np.datetime_as_string(local.to_numpy(dtype="datetime64[ns]"), unit="ns"))
    output = output.str.replace(r"\.000000000$", "", regex=True).str.replace(
        r"(\.\d{6})000$", r"\1", regex=True,
    )
    if unique.tz is not None:
        # UTC 是正式模型表的通常时区。其他时区逐列计算当地偏移（含夏令时）。
        if str(unique.tz) == "UTC":
            output += "+00:00"
        else:
            offsets = pd.Series(unique.strftime("%z")).str.replace(
                r"^([+-][0-9]{2})([0-9]{2})([0-9]{2})?$",
                lambda match: match[1] + ":" + match[2] + (":" + match[3] if match[3] else ""), regex=True,
            )
            output += offsets
    formatted = output.to_numpy(dtype=object)
    result = formatted[positions]
    result[positions < 0] = None
    return result


def build_prediction_frame(
    frame: pd.DataFrame,
    predictions: np.ndarray,
    candidate_id: str,
    fold_id: str,
    stage: str,
    model_ref: str,
    processor_ref: str,
    score_semantics: str = "raw_return_prediction",
    *,
    include_observation_time: bool = False,
) -> pd.DataFrame:
    if len(frame) != len(predictions):
        raise ModelMainlineError("模型预测行数与样本不一致")
    target = frame["target"].astype(float).to_numpy()
    time_fields = ("decision_time", "feature_available_time", "label_start_time",
                   "label_end_time", "label_available_time")
    # 独立训练记录含观察时刻，正式 DAG 表固定输出分数语义。
    if include_observation_time:
        time_fields = (*time_fields, "observation_time")
    result = pd.DataFrame({
        "candidate_id": candidate_id,
        "fold_id": fold_id,
        "stage": stage,
        "sample_id": frame["sample_id"].astype(str).to_numpy(),
        "actual": target,
        "prediction": np.asarray(predictions, dtype=float),
        "model_ref": model_ref,
        "processor_ref": processor_ref,
        "raw_label": target,
        "entity_id": frame["entity_id"].astype(str).to_numpy(),
        "observation_session": pd.to_datetime(frame["observation_session"]).dt.date.to_numpy(),
        "horizon_sessions": frame["horizon_sessions"].astype(int).to_numpy(),
        **({} if include_observation_time else {"score_semantics": score_semantics}),
        **{name: _iso_times(frame[name]) for name in time_fields},
    })
    for name in ("label_start_time", "label_end_time", "label_available_time"):
        result[name] = result[name].astype("string")
    return result
