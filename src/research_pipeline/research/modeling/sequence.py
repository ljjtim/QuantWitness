"""按冻结交易日历构建无标签历史窗口，并交给 Qlib 序列采样。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from .inputs import ModelMainlineError, assemble_daily_feature_context


TARGET_COLUMNS = ("sample_id", "entity_id", "observation_session", "decision_time")
MEMBER_COLUMNS = ("sample_id", "step", "entity_id", "observation_session",
                  "observation_time", "feature_available_time", "feature_lineage_hash")


@dataclass(frozen=True)
class SequenceWindows:
    """上下文无标签；members 只记录完整且当时可见的窗口。"""

    context: pd.DataFrame
    targets: pd.DataFrame
    members: pd.DataFrame
    exclusions: pd.DataFrame
    feature_columns: tuple[str, ...]
    calendar_sessions: tuple
    step_len: int


def build_sequence_windows(
    features: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    calendar_sessions: Sequence,
    feature_columns: Sequence[str],
    step_len: int,
) -> SequenceWindows:
    """窗口资格先于标签切分；purge 只限制末端，不删除历史特征。"""
    if type(step_len) is not int or step_len < 1:
        raise ModelMainlineError("序列 step_len 必须为正整数")
    calendar = tuple(pd.Timestamp(day).date() for day in calendar_sessions)
    if not calendar or any(pd.isna(day) for day in calendar) or tuple(sorted(set(calendar))) != calendar:
        raise ModelMainlineError("序列日历必须非空、升序且无重复")
    columns = tuple(feature_columns)
    context, actual_columns = assemble_daily_feature_context(features)
    if not columns or len(set(columns)) != len(columns) or set(columns) != set(actual_columns):
        raise ModelMainlineError("序列特征列必须完整覆盖冻结 Feature 列且不得重复")
    if not set(TARGET_COLUMNS) <= set(targets):
        raise ModelMainlineError("序列末端缺少身份、证券、会话或决策时间")
    endpoints = targets.loc[:, TARGET_COLUMNS].copy()
    if endpoints.empty or endpoints.isna().any().any():
        raise ModelMainlineError("序列末端必须非空且身份、时间不得缺失")
    endpoints["entity_id"] = endpoints["entity_id"].astype(str)
    endpoints["sample_id"] = endpoints["sample_id"].astype(str)
    endpoints["observation_session"] = endpoints["observation_session"].map(lambda day: pd.Timestamp(day).date())
    endpoints["decision_time"] = pd.to_datetime(endpoints["decision_time"], utc=True)
    if endpoints["sample_id"].duplicated().any() or endpoints.duplicated(["entity_id", "observation_session"]).any():
        raise ModelMainlineError("序列末端身份及证券会话必须唯一")
    context["entity_id"] = context["entity_id"].astype(str)
    context["observation_session"] = context["observation_session"].map(lambda day: pd.Timestamp(day).date())
    if context.duplicated(["entity_id", "observation_session"]).any():
        raise ModelMainlineError("历史特征证券会话必须唯一")
    context = context.loc[context["observation_session"].isin(calendar)].reset_index(drop=True)
    records = {(row["entity_id"], row["observation_session"]): row for row in context.to_dict("records")}
    positions = {day: index for index, day in enumerate(calendar)}
    members, exclusions = [], []
    for target in endpoints.to_dict("records"):
        day = target["observation_session"]
        if day not in positions:
            raise ModelMainlineError("序列末端会话不在冻结交易日历内")
        end = positions[day]
        reason = "insufficient_history" if end + 1 < step_len else None
        history = []
        if reason is None:
            for member_day in calendar[end - step_len + 1:end + 1]:
                row = records.get((target["entity_id"], member_day))
                if row is None or row["feature_count"] != len(columns):
                    reason = "missing_feature"
                    break
                if row["feature_available_time"] > target["decision_time"]:
                    reason = "feature_not_visible"
                    break
                history.append(row)
        if reason:
            exclusions.append({"sample_id": target["sample_id"], "reason_code": reason})
            continue
        for step, row in enumerate(history):
            members.append({"sample_id": target["sample_id"], "step": step,
                            **{key: row[key] for key in MEMBER_COLUMNS[2:]}})
    return SequenceWindows(context=context, targets=endpoints,
        members=pd.DataFrame(members, columns=MEMBER_COLUMNS),
        exclusions=pd.DataFrame(exclusions, columns=("sample_id", "reason_code")),
        feature_columns=columns, calendar_sessions=calendar, step_len=step_len)


def qlib_sequence_dataset(
    windows: SequenceWindows,
    *,
    segments: Mapping[str, Sequence[str]],
    labels: pd.Series | None = None,
    transformed_features: pd.DataFrame | None = None,
):
    """只向指定末端提供标签；预测始终使用 NaN 占位，不读取 targets 的标签。"""
    from qlib.data.dataset import TSDatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader

    if set(segments) not in ({"train", "valid"}, {"test"}):
        raise ModelMainlineError("序列数据集只接受 train/valid 或 test")
    if (set(segments) == {"test"}) != (labels is None):
        raise ModelMainlineError("训练需要显式标签，预测不得提供标签")
    selected = [str(sid) for ids in segments.values() for sid in ids]
    admitted = set(windows.members["sample_id"])
    if len(set(selected)) != len(selected) or not set(selected) <= admitted or any(not len(ids) for ids in segments.values()):
        raise ModelMainlineError("序列分段末端必须非空、不重叠且具有合格窗口")
    endpoints = windows.targets.set_index("sample_id").loc[selected]
    bounds = {}
    for name, ids in segments.items():
        dates = endpoints.loc[list(ids), "observation_session"]
        bounds[name] = (pd.Timestamp(dates.min()), pd.Timestamp(dates.max()))
    if "train" in bounds and bounds["train"][1] >= bounds["valid"][0]:
        raise ModelMainlineError("序列 train 必须严格早于 valid")
    source = windows.context.set_index(["observation_session", "entity_id"])
    values = source.loc[:, windows.feature_columns].copy()
    if transformed_features is not None:
        if (not transformed_features.index.equals(source.index)
                or list(transformed_features.columns) != list(windows.feature_columns)):
            raise ModelMainlineError("处理后的序列特征必须保持上下文索引和特征顺序")
        values = transformed_features.copy()
    members = windows.members.loc[windows.members["sample_id"].isin(selected)]
    keys = pd.MultiIndex.from_frame(members[["observation_session", "entity_id"]].drop_duplicates())
    values = values.reindex(keys).sort_index()
    if not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ModelMainlineError("序列完整窗口经处理后仍有非有限特征")
    values.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(values.index.get_level_values(0)), values.index.get_level_values(1)],
        names=["datetime", "instrument"])
    values.columns = pd.MultiIndex.from_product([["feature"], windows.feature_columns])
    values[("label", "target")] = np.nan
    values[("filter", "endpoint")] = False
    if labels is not None:
        if labels.index.has_duplicates or set(labels.index) != set(selected) or not np.isfinite(labels.to_numpy(dtype=float)).all():
            raise ModelMainlineError("序列标签必须恰好覆盖 train/valid 末端且为有限数")
    for sid, row in endpoints.iterrows():
        index = (pd.Timestamp(row["observation_session"]), row["entity_id"])
        values.loc[index, ("filter", "endpoint")] = True
        if labels is not None:
            values.loc[index, ("label", "target")] = float(labels.loc[sid])
    # 上下文已应用冻结处理器；标签清洗不能在此删除没有标签的历史行。
    handler = DataHandlerLP(data_loader=StaticDataLoader(values), infer_processors=[],
                            learn_processors=[], init_data=False)
    handler.setup_data(init_type=DataHandlerLP.IT_LS)
    return TSDatasetH(handler=handler, segments=bounds, step_len=windows.step_len, flt_col="filter")
