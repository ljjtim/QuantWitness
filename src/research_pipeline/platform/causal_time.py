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


def _require_key_columns(
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    field: str,
) -> tuple[str, ...]:
    keys = tuple(key_columns)
    if (
        not keys
        or len(keys) != len(set(keys))
        or any(not isinstance(column, str) or not column for column in keys)
    ):
        raise CausalTimeContractError(f"{field} 行键必须是非空唯一列名")
    _require_columns(frame, keys, field)
    if frame.loc[:, list(keys)].isna().any(axis=None):
        raise CausalTimeContractError(f"{field} 行键不能包含空值")
    return keys


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


def build_core_feature_time_facts(
    source: pd.DataFrame,
    *,
    key_columns: Sequence[str],
    decision_time_column: str,
    observation_time_column: str,
    available_time_column: str,
    source_partition_column: str,
) -> pd.DataFrame:
    """从核心实际输入生成每个 Feature 行的最大源时间。"""

    frame = _require_frame(source, "Feature 实际输入")
    keys = _require_key_columns(frame, key_columns, "Feature 实际输入")
    source_columns = (
        decision_time_column,
        observation_time_column,
        available_time_column,
        source_partition_column,
    )
    if any(not isinstance(column, str) or not column for column in source_columns):
        raise CausalTimeContractError("Feature 实际输入时间列名无效")
    _require_columns(frame, source_columns, "Feature 实际输入")

    working = frame.loc[:, [*keys, *source_columns]].copy()
    working["_decision_time"] = _normalized_time_column(
        working, decision_time_column
    )
    working["_observation_time"] = _normalized_time_column(
        working, observation_time_column
    )
    working["_available_time"] = _normalized_time_column(
        working, available_time_column
    )
    raw_partitions = working[source_partition_column]
    normalized_partitions: list[tuple[str, ...]] = []
    for value in raw_partitions:
        if isinstance(value, str):
            items = (value,)
        elif isinstance(value, Iterable):
            items = tuple(value)
        else:
            items = ()
        if (
            not items
            or any(not isinstance(item, str) or not item for item in items)
            or tuple(sorted(set(items))) != items
        ):
            raise CausalTimeContractError(
                "Feature source partition 必须是非空唯一字符串序列"
            )
        normalized_partitions.append(items)
    working["_source_partition_ids"] = normalized_partitions
    if (working["_observation_time"] > working["_decision_time"]).any():
        raise CausalTimeContractError("Feature 源观测时间晚于决策时间")
    if (working["_available_time"] > working["_decision_time"]).any():
        raise CausalTimeContractError("Feature 源可见时间晚于决策时间")

    rows: list[dict[str, object]] = []
    grouped = working.groupby(list(keys), sort=False, dropna=False, observed=True)
    for raw_key, group in grouped:
        if not group["_observation_time"].is_monotonic_increasing:
            raise CausalTimeContractError("Feature 实际输入未按观测时间有序")
        decisions = group["_decision_time"].drop_duplicates()
        if len(decisions) != 1:
            raise CausalTimeContractError("同一 Feature 行键存在多个决策时间")
        key_values = raw_key if isinstance(raw_key, tuple) else (raw_key,)
        row = dict(zip(keys, key_values))
        row.update(
            {
                "decision_time": decisions.iloc[0],
                "max_source_observation_time": group["_observation_time"].max(),
                "max_source_available_time": group["_available_time"].max(),
                "source_partition_ids": sorted(
                    {
                        partition_id
                        for partition_ids in group["_source_partition_ids"]
                        for partition_id in partition_ids
                    }
                ),
            }
        )
        rows.append(row)
    import pandas as pd

    result = pd.DataFrame(rows, columns=[*keys, *CORE_FEATURE_TIME_COLUMNS])
    validate_feature_time_facts(result)
    return result


def attach_core_feature_time_facts(
    project_values: pd.DataFrame,
    core_time_facts: pd.DataFrame,
    *,
    key_columns: Sequence[str],
) -> pd.DataFrame:
    """把核心事实按一一对应行键附到项目特征值。"""

    values = _require_frame(project_values, "项目 Feature 输出")
    facts = _require_frame(core_time_facts, "Feature 核心时间事实")
    keys = _require_key_columns(values, key_columns, "项目 Feature 输出")
    _require_key_columns(facts, keys, "Feature 核心时间事实")
    protected = sorted(set(values.columns).intersection(CORE_FEATURE_TIME_COLUMNS))
    if protected:
        raise CausalTimeContractError(
            f"项目扩展不得填写核心时间事实: {protected}"
        )
    if values.duplicated(list(keys)).any() or facts.duplicated(list(keys)).any():
        raise CausalTimeContractError("Feature 行键必须一一对应且唯一")
    normalized_facts = _normalized_feature_facts(facts)
    merged = values.merge(
        normalized_facts.loc[:, [*keys, *CORE_FEATURE_TIME_COLUMNS]],
        on=list(keys),
        how="outer",
        validate="one_to_one",
        indicator=True,
        sort=False,
    )
    if not (merged["_merge"] == "both").all():
        raise CausalTimeContractError("项目 Feature 输出与核心时间事实行键不闭合")
    return merged.drop(columns="_merge")


def attach_core_label_time_facts(
    project_values: pd.DataFrame,
    core_time_facts: pd.DataFrame,
    *,
    key_columns: Sequence[str],
) -> pd.DataFrame:
    """把核心生成的未来观测事实按一一对应行键附到项目 Label 值。"""

    values = _require_frame(project_values, "项目 Label 输出")
    facts = _require_frame(core_time_facts, "Label 核心时间事实")
    keys = _require_key_columns(values, key_columns, "项目 Label 输出")
    _require_key_columns(facts, keys, "Label 核心时间事实")
    protected = sorted(set(values.columns).intersection(CORE_LABEL_TIME_COLUMNS))
    if protected:
        raise CausalTimeContractError(
            f"项目扩展不得填写核心时间事实: {protected}"
        )
    if values.duplicated(list(keys)).any() or facts.duplicated(list(keys)).any():
        raise CausalTimeContractError("Label 行键必须一一对应且唯一")
    validate_label_time_facts(facts)
    merged = values.merge(
        facts.loc[:, [*keys, *CORE_LABEL_TIME_COLUMNS]],
        on=list(keys),
        how="outer",
        validate="one_to_one",
        indicator=True,
        sort=False,
    )
    if not (merged["_merge"] == "both").all():
        raise CausalTimeContractError("项目 Label 输出与核心时间事实行键不闭合")
    result = merged.drop(columns="_merge")
    validate_label_time_facts(result)
    return result


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


__all__ = [
    "CausalTimeContractError",
    "CORE_FEATURE_TIME_COLUMNS",
    "CORE_LABEL_TIME_COLUMNS",
    "attach_core_feature_time_facts",
    "attach_core_label_time_facts",
    "build_core_feature_time_facts",
    "validate_feature_time_facts",
    "validate_label_time_facts",
]
