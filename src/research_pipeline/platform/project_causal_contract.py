"""项目扩展的冻结读取窗口、实际交付轨迹与核心时间事实。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from math import isfinite
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


@dataclass(frozen=True)
class CausalWorkItem:
    key_rows: tuple[tuple[object, ...], ...]
    decision_time: pd.Timestamp
    window_start: pd.Timestamp
    window_end: pd.Timestamp
    source_partitions: Mapping[str, tuple[str, ...]]


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
            "sources": [dict(port=source.port, request_id=source.request_id,
                columns=list(source.columns), observation_column=source.observation_column,
                available_column=source.available_column) for source in self.sources],
            "work_items": [dict(key_rows=[list(row) for row in item.key_rows],
                decision_time=item.decision_time.isoformat(), window_start=item.window_start.isoformat(),
                window_end=item.window_end.isoformat(), source_partitions={port: list(ids)
                    for port, ids in item.source_partitions.items()}) for item in self.work_items],
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
            for key in item.key_rows:
                pending.append(key)
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
        source = _fields(value, {
            "port", "request_id", "columns", "observation_column", "available_column",
        }, "causal source")
        columns = _names(source["columns"], "source columns")
        for name in ("port", "request_id", "observation_column", "available_column"):
            _names([source[name]], name)
        if not {source["observation_column"], source["available_column"]} <= set(columns):
            raise CausalTimeContractError("causal source columns 必须包含冻结时间列")
        sources.append(CausalSource(**{**source, "columns": columns}))
    ports = {source.port for source in sources}
    if len(ports) != len(sources):
        raise CausalTimeContractError("causal source port 不能重复")
    if not isinstance(data["work_items"], (list, tuple)) or not data["work_items"]:
        raise CausalTimeContractError("causal work_items 必须非空")
    work_items = []
    seen_keys: set[tuple[object, ...]] = set()
    for value in data["work_items"]:
        item = _fields(value, {
            "key_rows", "decision_time", "window_start", "window_end", "source_partitions",
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
        if not isinstance(item["key_rows"], (list, tuple)) or not item["key_rows"]:
            raise CausalTimeContractError("causal key_rows 必须非空")
        rows = []
        for row in item["key_rows"]:
            if not isinstance(row, (list, tuple)) or len(row) != len(keys):
                raise CausalTimeContractError("causal 行键宽度不匹配")
            if any(type(cell) not in (str, int, float, bool) or (
                isinstance(cell, float) and not isfinite(cell)
            ) for cell in row):
                raise CausalTimeContractError("causal 行键只能包含非空 JSON 标量")
            key = tuple(row)
            if key in seen_keys:
                raise CausalTimeContractError("causal 输出行键必须全局唯一")
            seen_keys.add(key)
            rows.append(key)
        work_items.append(CausalWorkItem(tuple(rows), decision, start, end, partitions))
    work_items.sort(key=lambda item: (
        item.decision_time, item.window_start, item.window_end,
        tuple(sorted(item.source_partitions.items())),
    ))
    return CausalPlan(data["kind"], output_port, keys, tuple(sources), tuple(work_items), data["state_scope"])
