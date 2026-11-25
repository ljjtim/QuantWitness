"""Feature 与 Label 的逐行因果时间合同。"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd



class CausalTimeContractError(ValueError):
    """逐行因果时间合同不闭合。"""


CORE_FEATURE_TIME_COLUMNS = (
    "decision_time",
    "max_source_observation_time",
    "max_source_available_time",
    "source_partition_ids",
)
CORE_LABEL_TIME_COLUMNS = (
    "decision_time",
    "first_actual_observation_time",
    "last_actual_observation_time",
    "available_time",
)


def causal_time_key_columns(column_names, *, is_feature: bool) -> tuple[str, ...]:
    """正式 Feature/Label 生产表与独立复核共用的完整稳定键。"""
    columns = set(column_names)
    candidates = (("date", "code"),) if is_feature else (("observation_at", "code"),)
    common = (
        ("instrument", "bar_end"),
        ("entity_id", "observation_session",
         *(("window_sessions", "feature_id") if is_feature else ("horizon_sessions",))),
        ("event_id",) if is_feature else ("event_id", "relative_session"),
    )
    for candidate in (*candidates, *common):
        if set(candidate) <= columns:
            return candidate
    raise CausalTimeContractError("Result 正式 Feature/Label 无法识别稳定行键")


def _require_frame(frame: object, field: str) -> pd.DataFrame:
    import pandas as pd

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise CausalTimeContractError(f"{field} 必须是非空 DataFrame")
    return frame


def _require_columns(
    frame: pd.DataFrame,
    columns: Sequence[str],
    field: str,
) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise CausalTimeContractError(f"{field} 缺少列: {missing}")



def _aware_utc(value: object, field: str) -> pd.Timestamp:
    import pandas as pd

    if value is None or (not isinstance(value, (datetime, pd.Timestamp, str))):
        raise CausalTimeContractError(f"{field} 必须是带时区时间")
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CausalTimeContractError(f"{field} 必须是带时区时间") from exc
    if pd.isna(parsed) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CausalTimeContractError(f"{field} 必须是带时区时间")
    return parsed.tz_convert("UTC")


def _normalized_time_column(frame: pd.DataFrame, column: str) -> pd.Series:
    import pandas as pd

    return pd.Series(
        (_aware_utc(value, column) for value in frame[column].tolist()),
        index=frame.index,
        dtype="datetime64[ns, UTC]",
    )


def _normalized_feature_facts(frame: pd.DataFrame) -> pd.DataFrame:
    _require_columns(frame, CORE_FEATURE_TIME_COLUMNS, "Feature 核心时间事实")
    normalized = frame.copy()
    for column in CORE_FEATURE_TIME_COLUMNS[:3]:
        normalized[column] = _normalized_time_column(normalized, column)
    partitions: list[list[str]] = []
    for value in normalized["source_partition_ids"]:
        if isinstance(value, str) or not isinstance(value, Iterable):
            raise CausalTimeContractError("source_partition_ids 必须是非空唯一字符串序列")
        raw_items = list(value)
        items = [item for item in raw_items if isinstance(item, str) and item]
        if not items or len(items) != len(raw_items) or items != sorted(set(items)):
            raise CausalTimeContractError("source_partition_ids 必须是非空唯一字符串序列")
        partitions.append(items)
    normalized["source_partition_ids"] = partitions
    if (
        normalized["max_source_observation_time"]
        > normalized["decision_time"]
    ).any():
        raise CausalTimeContractError("Feature 源观测时间晚于决策时间")
    if (
        normalized["max_source_available_time"]
        > normalized["decision_time"]
    ).any():
        raise CausalTimeContractError("Feature 源可见时间晚于决策时间")
    return normalized


def validate_feature_time_facts(frame: pd.DataFrame) -> None:
    """验证核心已经生成的 Feature 逐行时间事实。"""

    _normalized_feature_facts(_require_frame(frame, "Feature 核心时间事实"))



def validate_label_time_facts(frame: pd.DataFrame) -> None:
    """验证 Label 的实际未来观测区间，而非只信整体时间声明。"""

    labels = _require_frame(frame, "Label 核心时间事实")
    _require_columns(labels, CORE_LABEL_TIME_COLUMNS, "Label 核心时间事实")
    normalized = labels.copy()
    for column in CORE_LABEL_TIME_COLUMNS:
        normalized[column] = _normalized_time_column(normalized, column)
    if (
        normalized["first_actual_observation_time"]
        <= normalized["decision_time"]
    ).any():
        raise CausalTimeContractError("Label 实际首观测必须严格晚于决策")
    if (
        normalized["last_actual_observation_time"]
        < normalized["first_actual_observation_time"]
    ).any():
        raise CausalTimeContractError("Label 实际末观测不能早于实际首观测")
    if (
        normalized["available_time"]
        < normalized["last_actual_observation_time"]
    ).any():
        raise CausalTimeContractError("Label 可见时间不能早于实际末观测")


def attach_core_time_facts_arrow(values, facts, *, key_columns, is_feature: bool):
    """按稳定键附加核心时间列，保留项目值、列类型与原始行序。"""
    import pyarrow as pa
    import pyarrow.compute as pc

    keys = tuple(key_columns)
    columns = CORE_FEATURE_TIME_COLUMNS if is_feature else CORE_LABEL_TIME_COLUMNS
    if not keys or len(set(keys)) != len(keys):
        raise CausalTimeContractError("项目因果行键必须是非空唯一列名")
    if not values.num_rows or values.num_rows != facts.num_rows:
        raise CausalTimeContractError("项目输出与核心时间事实行数不一致")

    left_keys = []
    right_keys = []
    for key in keys:
        left = values[key]
        right = facts[key]
        value_type = left.type.value_type if pa.types.is_dictionary(left.type) else left.type
        fact_type = right.type.value_type if pa.types.is_dictionary(right.type) else right.type
        value_string = pa.types.is_string(value_type) or pa.types.is_large_string(value_type)
        fact_string = pa.types.is_string(fact_type) or pa.types.is_large_string(fact_type)
        if pa.types.is_floating(value_type) and pa.types.is_floating(fact_type):
            value_type = pa.float64()
        elif value_type != fact_type and not (
            value_string and fact_string
            or pa.types.is_integer(value_type) and pa.types.is_integer(fact_type)
            or pa.types.is_date(value_type) and (pa.types.is_date(fact_type) or fact_string)
            or pa.types.is_timestamp(value_type) and pa.types.is_timestamp(fact_type)
            and (value_type.tz is None) == (fact_type.tz is None)
        ):
            raise CausalTimeContractError(f"项目因果行键类型不匹配: {key}")
        left = pc.cast(left, value_type)
        right = pc.cast(right, value_type)
        if left.null_count or right.null_count or (
            pa.types.is_floating(value_type) and (
                pc.any(pc.is_nan(left)).as_py() or pc.any(pc.is_nan(right)).as_py()
            )
        ):
            raise CausalTimeContractError("项目因果行键不能包含空值")
        left_keys.append(left)
        right_keys.append(right)

    left_table = pa.Table.from_arrays(left_keys, names=keys)
    right_table = pa.Table.from_arrays(right_keys, names=keys)
    sort_keys = [(key, "ascending") for key in keys]
    left_order = pc.sort_indices(left_table, sort_keys=sort_keys)
    right_order = pc.sort_indices(right_table, sort_keys=sort_keys)
    ordered = left_table.take(left_order)
    if not ordered.equals(right_table.take(right_order)):
        raise CausalTimeContractError("项目输出与核心时间事实行键不闭合")
    adjacent_equal = None
    for key in keys:
        column = ordered[key]
        same = pc.equal(column.slice(0, len(column) - 1), column.slice(1))
        adjacent_equal = same if adjacent_equal is None else pc.and_(adjacent_equal, same)
    if pc.any(adjacent_equal).as_py():
        raise CausalTimeContractError("项目因果行键必须唯一")

    time_columns = columns[:3] if is_feature else columns
    times = {}
    for column in time_columns:
        data = facts[column]
        aware_timestamp = pa.types.is_timestamp(data.type) and data.type.tz is not None
        if data.null_count or not (
            aware_timestamp or pa.types.is_string(data.type) or pa.types.is_large_string(data.type)
        ):
            raise CausalTimeContractError(f"{column} 必须是带时区时间")
        times[column] = pc.cast(data, pa.timestamp("ns", tz="UTC"))

    decision = times["decision_time"]
    if is_feature:
        if (
            pc.any(pc.greater(times["max_source_observation_time"], decision)).as_py()
            or pc.any(pc.greater(times["max_source_available_time"], decision)).as_py()
        ):
            raise CausalTimeContractError("Feature 源观测或可见时间晚于决策时间")
        partitions = facts["source_partition_ids"]
        lengths = pc.list_value_length(partitions)
        flattened = pc.list_flatten(partitions)
        if (
            lengths.null_count
            or pc.any(pc.equal(lengths, 0)).as_py()
            or flattened.null_count
            or not (pa.types.is_string(flattened.type) or pa.types.is_large_string(flattened.type))
            or pc.any(pc.equal(flattened, "")).as_py()
        ):
            raise CausalTimeContractError("source_partition_ids 必须是非空唯一字符串序列")
        parents = pc.list_parent_indices(partitions.combine_chunks())
        preceding = len(flattened) - 1
        same_parent = pc.equal(parents.slice(0, preceding), parents.slice(1))
        unordered = pc.greater_equal(flattened.slice(0, preceding), flattened.slice(1))
        if pc.any(pc.and_(same_parent, unordered)).as_py():
            raise CausalTimeContractError("source_partition_ids 必须有序且唯一")
        times["source_partition_ids"] = partitions
    else:
        first = times["first_actual_observation_time"]
        last = times["last_actual_observation_time"]
        if (
            pc.any(pc.less_equal(first, decision)).as_py()
            or pc.any(pc.less(last, first)).as_py()
            or pc.any(pc.less(times["available_time"], last)).as_py()
        ):
            raise CausalTimeContractError("Label 实际观测与可见时间未按合同排序")

    fact_order = pc.take(right_order, pc.sort_indices(left_order))
    for column in columns:
        values = values.append_column(column, pc.take(times[column], fact_order))
    return values


__all__ = [
    "CausalTimeContractError",
    "CORE_FEATURE_TIME_COLUMNS",
    "CORE_LABEL_TIME_COLUMNS",
    "validate_feature_time_facts",
    "validate_label_time_facts",
]
