"""按冻结交易日历构建无标签历史窗口，并交给 Qlib 序列采样。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from .inputs import ModelMainlineError, assemble_daily_feature_context


TARGET_COLUMNS = ("sample_id", "entity_id", "observation_session", "decision_time")
MEMBER_COLUMNS = ("sample_id", "step", "context_index")


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
    _feature_context: tuple[pd.DataFrame, tuple[str, ...]] | None = None,
) -> SequenceWindows:
    """窗口资格先于标签切分；purge 只限制末端，不删除历史特征。"""
    if type(step_len) is not int or step_len < 1:
        raise ModelMainlineError("序列 step_len 必须为正整数")
    calendar = tuple(pd.Timestamp(day).date() for day in calendar_sessions)
    if not calendar or any(pd.isna(day) for day in calendar) or tuple(sorted(set(calendar))) != calendar:
        raise ModelMainlineError("序列日历必须非空、升序且无重复")
    columns = tuple(feature_columns)
    if _feature_context is None:
        context, actual_columns = assemble_daily_feature_context(features)
    else:
        context, actual_columns = _feature_context
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
    metadata_columns = [column for column in context.columns if column not in columns]
    metadata = context.loc[:, metadata_columns].copy()
    metadata["entity_id"] = metadata["entity_id"].astype(str)
    metadata["observation_session"] = metadata["observation_session"].map(lambda day: pd.Timestamp(day).date())
    if metadata.duplicated(["entity_id", "observation_session"]).any():
        raise ModelMainlineError("历史特征证券会话必须唯一")
    selected = metadata["observation_session"].isin(calendar).to_numpy()
    positions_in_source = np.flatnonzero(selected)
    metadata = metadata.loc[selected].reset_index(drop=True)
    keep_all = bool(selected.all())
    # 新表拥有独立列结构；全量保留时共享只读数值，筛选时每列只take一次。
    context = pd.DataFrame({"context_index": np.arange(len(metadata), dtype=np.int64),
        **{column: (context[column].array if keep_all
            else context[column].array.take(positions_in_source)) if column in columns
            else metadata[column].array for column in context.columns}}, copy=False)
    positions = pd.Index(calendar)
    endpoint_positions = positions.get_indexer(endpoints.observation_session)
    if (endpoint_positions < 0).any():
        raise ModelMainlineError("序列末端会话不在冻结交易日历内")
    source_index = pd.MultiIndex.from_frame(context[["entity_id", "observation_session"]])
    counts = context.feature_count.to_numpy() if len(context) else np.zeros(1, dtype=np.int64)
    available = pd.to_datetime(context.feature_available_time, utc=True).astype("int64").to_numpy() if len(context) else np.zeros(1, dtype=np.int64)
    decision = pd.to_datetime(endpoints.decision_time, utc=True).astype("int64").to_numpy()
    calendar_array = np.asarray(calendar, dtype=object)
    identifiers = endpoints.sample_id.to_numpy()
    entities = endpoints.entity_id.to_numpy()
    member_ids, member_steps, member_indices, excluded_ids, excluded_reasons = [], [], [], [], []
    for start in range(0, len(endpoints), 8192):
        end = min(start + 8192, len(endpoints))
        ends = endpoint_positions[start:end]
        history_positions = ends[:, None] - np.arange(step_len - 1, -1, -1)
        insufficient = history_positions[:, 0] < 0
        safe_positions = np.maximum(history_positions, 0)
        keys = pd.MultiIndex.from_arrays([np.repeat(entities[start:end], step_len),
            calendar_array[safe_positions.ravel()]])
        indices = source_index.get_indexer(keys).reshape(-1, step_len)
        safe_indices = np.maximum(indices, 0)
        missing = (indices < 0) | (counts[safe_indices] != len(columns))
        late = available[safe_indices] > decision[start:end, None]
        invalid = missing | late
        any_invalid = invalid.any(axis=1)
        first_invalid = invalid.argmax(axis=1)
        first_missing = missing[np.arange(len(ends)), first_invalid]
        reasons = np.where(insufficient, "insufficient_history",
            np.where(any_invalid, np.where(first_missing, "missing_feature", "feature_not_visible"), ""))
        good = reasons == ""
        member_ids.append(np.repeat(identifiers[start:end][good], step_len))
        member_steps.append(np.tile(np.arange(step_len, dtype=np.int64), int(good.sum())))
        member_indices.append(indices[good].ravel())
        excluded_ids.extend(identifiers[start:end][~good])
        excluded_reasons.extend(reasons[~good])
    members = pd.DataFrame({"sample_id":np.concatenate(member_ids),
        "step":np.concatenate(member_steps),"context_index":np.concatenate(member_indices)})
    return SequenceWindows(context=context, targets=endpoints, members=members,
        exclusions=pd.DataFrame({"sample_id":excluded_ids,"reason_code":excluded_reasons}),
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
    source_index = pd.MultiIndex.from_frame(windows.context[["observation_session", "entity_id"]])
    if transformed_features is not None:
        if (not transformed_features.index.equals(source_index)
                or list(transformed_features.columns) != list(windows.feature_columns)):
            raise ModelMainlineError("处理后的序列特征必须保持上下文索引和特征顺序")
    members = windows.members.loc[windows.members["sample_id"].isin(selected), "context_index"]
    used = windows.context.loc[windows.context.context_index.isin(members),
                               ["observation_session", "entity_id"]]
    keys = pd.MultiIndex.from_frame(used.drop_duplicates()).sort_values()
    positions = source_index.get_indexer(keys)
    if (positions < 0).any():
        raise ModelMainlineError("序列窗口引用不存在的上下文")
    output_index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(keys.get_level_values(0)), keys.get_level_values(1)],
        names=["datetime", "instrument"])
    feature_source = windows.context if transformed_features is None else transformed_features
    output = {}
    for column in windows.feature_columns:
        array = feature_source[column].array.take(positions)
        if not np.isfinite(np.asarray(array, dtype=float)).all():
            raise ModelMainlineError("序列完整窗口经处理后仍有非有限特征")
        output[("feature", column)] = array
    output[("label", "target")] = np.full(len(keys), np.nan)
    output[("filter", "endpoint")] = np.zeros(len(keys), dtype=bool)
    values = pd.DataFrame(output, index=output_index, copy=False)
    del output, feature_source, members, used
    if labels is not None:
        if labels.index.has_duplicates or set(labels.index) != set(selected) or not np.isfinite(labels.to_numpy(dtype=float)).all():
            raise ModelMainlineError("序列标签必须恰好覆盖 train/valid 末端且为有限数")
    endpoint_index = pd.MultiIndex.from_arrays([pd.to_datetime(endpoints.observation_session), endpoints.entity_id], names=["datetime", "instrument"])
    endpoint_positions = values.index.get_indexer(endpoint_index)
    if (endpoint_positions < 0).any():
        raise ModelMainlineError("序列末端不在所消费上下文中")
    filter_values = np.zeros(len(values), dtype=bool)
    filter_values[endpoint_positions] = True
    values[("filter", "endpoint")] = filter_values
    if labels is not None:
        label_values = np.full(len(values), np.nan)
        label_values[endpoint_positions] = labels.reindex(endpoints.index).to_numpy(dtype=float)
        values[("label", "target")] = label_values
    # 上下文已应用冻结处理器；标签清洗不能在此删除没有标签的历史行。
    handler = DataHandlerLP(data_loader=StaticDataLoader(values), infer_processors=[],
                            learn_processors=[], init_data=False)
    handler.setup_data(init_type=DataHandlerLP.IT_LS)
    return TSDatasetH(handler=handler, segments=bounds, step_len=windows.step_len, flt_col="filter")
