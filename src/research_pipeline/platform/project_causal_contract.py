"""项目扩展的冻结读取窗口、实际交付轨迹与核心时间事实。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from itertools import product
from math import isfinite, prod
from operator import index as integer_index
from sys import maxsize
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from research_pipeline.platform.causal_time import (
    CORE_FEATURE_TIME_COLUMNS,
    CORE_LABEL_TIME_COLUMNS,
    CausalTimeContractError,
)


def _time(value: object) -> pd.Timestamp:
    import pandas as pd

    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CausalTimeContractError("causal 时间必须带时区") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise CausalTimeContractError("causal 时间必须带时区")
    return stamp.tz_convert("UTC")


def _names(raw: object, field: str) -> tuple[str, ...]:
    if not isinstance(raw, (list, tuple)) or not raw:
        raise CausalTimeContractError(f"{field} 必须是非空列名或身份序列")
    if any(not isinstance(value, str) or not value for value in raw):
        raise CausalTimeContractError(f"{field} 包含无效名称")
    if len(set(raw)) != len(raw):
        raise CausalTimeContractError(f"{field} 包含重复名称")
    return tuple(raw)


def _fields(raw: object, expected: set[str], field: str) -> Mapping:
    if not isinstance(raw, Mapping) or set(raw) != expected:
        raise CausalTimeContractError(f"{field} 字段必须完整且不含额外字段")
    return raw


@dataclass(frozen=True)
class CausalSource:
    port: str
    request_id: str
    columns: tuple[str, ...]
    observation_column: str
    available_column: str
    daily_time: Mapping | None = None

    def to_dict(self) -> dict:
        result = dict(port=self.port, request_id=self.request_id, columns=list(self.columns),
                      observation_column=self.observation_column, available_column=self.available_column)
        if self.daily_time is not None:
            result["daily_time"] = {**self.daily_time, "sessions": list(self.daily_time["sessions"])}
        return result


def _key_scalar(value: object) -> bool:
    return type(value) in (str, int, float, bool) and not (
        type(value) is float and not isfinite(value)
    )


@dataclass(frozen=True)
class CausalKeyProduct(Sequence):
    """按冻结循环顺序生成笛卡尔积键，只常驻各列的唯一值轴。"""

    axes: tuple[tuple[object, ...], ...]
    order: tuple[int, ...]
    _members: tuple[frozenset, ...] = field(init=False, repr=False, compare=False)
    _length: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.axes, (tuple, list)) or not self.axes:
            raise CausalTimeContractError("causal key_axes 必须是非空值轴序列")
        axes = []
        members = []
        for axis in self.axes:
            if not isinstance(axis, (tuple, list)) or not axis:
                raise CausalTimeContractError("causal key_axes 每个值轴必须非空")
            if any(not _key_scalar(value) for value in axis):
                raise CausalTimeContractError("causal key_axes 只能包含非空 JSON 标量")
            values = frozenset(axis)
            if len(values) != len(axis):
                raise CausalTimeContractError("causal key_axes 值轴必须唯一")
            axes.append(tuple(axis))
            members.append(values)
        if (not isinstance(self.order, (tuple, list))
                or any(type(column) is not int for column in self.order)
                or len(self.order) != len(axes)
                or set(self.order) != set(range(len(axes)))):
            raise CausalTimeContractError("causal key_order 必须是所有列索引的排列")
        length = prod(len(axis) for axis in axes)
        if length > maxsize:
            raise CausalTimeContractError("causal key_axes 键数超出 Sequence 支持范围")
        object.__setattr__(self, "axes", tuple(axes))
        object.__setattr__(self, "order", tuple(self.order))
        object.__setattr__(self, "_members", tuple(members))
        object.__setattr__(self, "_length", length)

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, index):
        if isinstance(index, slice):
            return tuple(self[position] for position in range(*index.indices(len(self))))
        position = integer_index(index)
        if position < 0:
            position += len(self)
        if not 0 <= position < len(self):
            raise IndexError("causal key index out of range")
        key = [None] * len(self.axes)
        for column in reversed(self.order):
            position, offset = divmod(position, len(self.axes[column]))
            key[column] = self.axes[column][offset]
        return tuple(key)

    def __iter__(self) -> Iterator[tuple[object, ...]]:
        for values in product(*(self.axes[column] for column in self.order)):
            key = [None] * len(self.axes)
            for column, value in zip(self.order, values):
                key[column] = value
            yield tuple(key)

    def __contains__(self, key) -> bool:
        return (isinstance(key, (tuple, list)) and len(key) == len(self.axes)
                and all(_key_scalar(value) and value in values
                        for value, values in zip(key, self._members)))

    def overlaps(self, other: CausalKeyProduct) -> bool:
        """两笛卡尔积相交当且仅当每列值轴均相交。"""
        return len(self.axes) == len(other.axes) and all(
            not left.isdisjoint(right) for left, right in zip(self._members, other._members)
        )

    def to_dict(self) -> dict:
        return {"key_axes": [list(axis) for axis in self.axes], "key_order": list(self.order)}


@dataclass(frozen=True)
class CausalWorkItem:
    key_rows: Sequence[tuple[object, ...]]
    decision_time: pd.Timestamp
    window_start: pd.Timestamp
    window_end: pd.Timestamp
    source_partitions: Mapping[str, tuple[str, ...]]

    def to_dict(self) -> dict:
        keys = (self.key_rows.to_dict() if isinstance(self.key_rows, CausalKeyProduct)
                else {"key_rows": [list(row) for row in self.key_rows]})
        return {**keys, "decision_time": self.decision_time.isoformat(),
                "window_start": self.window_start.isoformat(), "window_end": self.window_end.isoformat(),
                "source_partitions": {port: list(ids) for port, ids in self.source_partitions.items()}}


@dataclass(frozen=True)
class CausalPlan:
    kind: str
    output_port: str
    key_columns: tuple[str, ...]
    sources: tuple[CausalSource, ...]
    work_items: tuple[CausalWorkItem, ...]
    state_scope: str

    def to_dict(self) -> dict:
        return {
            "kind": self.kind, "output_port": self.output_port,
            "key_columns": list(self.key_columns), "state_scope": self.state_scope,
            "sources": [source.to_dict() for source in self.sources],
            "work_items": [item.to_dict() for item in self.work_items],
        }

    def iter_work_items(self, max_keys: int = 8192) -> Iterator[CausalWorkItem]:
        """同窗键可合批；批大小不参与冻结的计划身份。"""
        if type(max_keys) is not int or max_keys <= 0:
            raise CausalTimeContractError("causal key batch 预算必须为正整数")
        pending: list[tuple[object, ...]] = []
        current: CausalWorkItem | None = None
        for item in self.work_items:
            if current is not None and (
                item.decision_time != current.decision_time
                or item.window_start != current.window_start
                or item.window_end != current.window_end
                or item.source_partitions != current.source_partitions
            ):
                if pending:
                    yield replace(current, key_rows=tuple(pending))
                    pending = []
            current = item
            offset = 0
            while offset < len(item.key_rows):
                end = min(len(item.key_rows), offset + max_keys - len(pending))
                pending.extend(item.key_rows[offset:end])
                offset = end
                if len(pending) == max_keys:
                    yield replace(item, key_rows=tuple(pending))
                    pending = []
        if pending and current is not None:
            yield replace(current, key_rows=tuple(pending))


def parse_causal_plan(raw: object) -> CausalPlan:
    """解析固定 JSON 计划；不接受表达式、回调或扩展自报的时间事实。"""
    data = _fields(raw, {
        "kind", "output_port", "key_columns", "sources", "work_items", "state_scope",
    }, "causal_plan")
    if data["kind"] not in ("feature", "label"):
        raise CausalTimeContractError("causal kind 必须为 feature 或 label")
    if data["state_scope"] not in ("independent", "carry"):
        raise CausalTimeContractError("causal state_scope 必须为 independent 或 carry")
    output_port = _names([data["output_port"]], "output_port")[0]
    keys = _names(data["key_columns"], "key_columns")
    if set(keys).intersection((*CORE_FEATURE_TIME_COLUMNS, *CORE_LABEL_TIME_COLUMNS)):
        raise CausalTimeContractError("causal 行键不得占用核心时间事实列")
    if not isinstance(data["sources"], (list, tuple)) or not data["sources"]:
        raise CausalTimeContractError("causal sources 必须非空")
    sources = []
    for value in data["sources"]:
        fields = {
            "port", "request_id", "columns", "observation_column", "available_column",
        }
        if isinstance(value, Mapping) and "daily_time" in value:
            fields.add("daily_time")
        source = _fields(value, fields, "causal source")
        columns = _names(source["columns"], "source columns")
        for name in ("port", "request_id", "observation_column", "available_column"):
            _names([source[name]], name)
        if not {source["observation_column"], source["available_column"]} <= set(columns):
            raise CausalTimeContractError("causal source columns 必须包含冻结时间列")
        daily_time = None
        if "daily_time" in source:
            daily_time = _parse_daily_time(source["daily_time"])
            if source["observation_column"] != source["available_column"]:
                raise CausalTimeContractError("日频因果来源必须绑定同一个原始日期列")
        sources.append(CausalSource(**{**source, "columns": columns, "daily_time": daily_time}))
    ports = {source.port for source in sources}
    if len(ports) != len(sources):
        raise CausalTimeContractError("causal source port 不能重复")
    if not isinstance(data["work_items"], (list, tuple)) or not data["work_items"]:
        raise CausalTimeContractError("causal work_items 必须非空")
    work_items = []
    seen_keys: set[tuple[object, ...]] = set()
    products: list[CausalKeyProduct] = []
    for value in data["work_items"]:
        key_fields = {"key_axes", "key_order"} if isinstance(value, Mapping) and "key_axes" in value else {"key_rows"}
        item = _fields(value, {
            *key_fields, "decision_time", "window_start", "window_end", "source_partitions",
        }, "causal work item")
        if not isinstance(item["source_partitions"], Mapping) or set(item["source_partitions"]) != ports:
            raise CausalTimeContractError("causal work item 必须绑定全部 source port 分区")
        partitions = {
            port: _names(ids, "source_partitions")
            for port, ids in item["source_partitions"].items()
        }
        decision, start, end = (_time(item[name]) for name in (
            "decision_time", "window_start", "window_end",
        ))
        if start > end or (data["kind"] == "feature" and end > decision):
            raise CausalTimeContractError("Feature 历史窗口不能晚于决策")
        if data["kind"] == "label" and (start < decision or end <= decision):
            raise CausalTimeContractError("Label 窗口必须位于决策之后")
        if "key_axes" in item:
            rows = CausalKeyProduct(item["key_axes"], item["key_order"])
            if len(rows.axes) != len(keys):
                raise CausalTimeContractError("causal 行键宽度不匹配")
            if any(rows.overlaps(previous) for previous in products) or any(key in rows for key in seen_keys):
                raise CausalTimeContractError("causal 输出行键必须全局唯一")
            products.append(rows)
        else:
            if not isinstance(item["key_rows"], (list, tuple)) or not item["key_rows"]:
                raise CausalTimeContractError("causal key_rows 必须非空")
            plain_rows = []
            for row in item["key_rows"]:
                if not isinstance(row, (list, tuple)) or len(row) != len(keys):
                    raise CausalTimeContractError("causal 行键宽度不匹配")
                if any(not _key_scalar(cell) for cell in row):
                    raise CausalTimeContractError("causal 行键只能包含非空 JSON 标量")
                key = tuple(row)
                if key in seen_keys or any(key in previous for previous in products):
                    raise CausalTimeContractError("causal 输出行键必须全局唯一")
                seen_keys.add(key)
                plain_rows.append(key)
            rows = tuple(plain_rows)
        work_items.append(CausalWorkItem(rows, decision, start, end, partitions))
    work_items.sort(key=lambda item: (
        item.decision_time, item.window_start, item.window_end,
        tuple(sorted(item.source_partitions.items())),
    ))
    return CausalPlan(data["kind"], output_port, keys, tuple(sources), tuple(work_items), data["state_scope"])


def _parse_daily_time(raw: object) -> Mapping:
    from datetime import date

    data = _fields(raw, {"rule", "timezone", "calendar_id", "calendar_source", "sessions"}, "daily_time")
    if data["rule"] != "next_session_open" or data["timezone"] != "Asia/Shanghai":
        raise CausalTimeContractError("日频因果来源只支持中国股票 next_session_open")
    _names([data["calendar_id"]], "calendar_id")
    _names([data["calendar_source"]], "calendar_source")
    sessions = _names(data["sessions"], "sessions")
    try:
        dates = tuple(date.fromisoformat(value) for value in sessions)
    except ValueError as exc:
        raise CausalTimeContractError("日频交易日历必须是 ISO 日期") from exc
    if sessions != tuple(value.isoformat() for value in dates) or dates != tuple(sorted(set(dates))) or len(dates) < 2:
        raise CausalTimeContractError("日频交易日历必须规范、严格递增且包含后续交易日")
    return {**data, "sessions": sessions}
