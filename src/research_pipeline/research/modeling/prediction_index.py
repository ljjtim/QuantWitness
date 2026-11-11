"""显式模型预测资格：历史上下文不能代替当时可用候选。"""
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
