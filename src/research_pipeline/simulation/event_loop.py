"""确定性市场事件排序、去重和序号诊断。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, TypeVar

from research_pipeline.domain import MarketEvent
from research_pipeline.domain.time import require_aware_datetime

from .orders import SimulationContractError


T = TypeVar("T")


@dataclass(frozen=True)
class SequenceGap:
    source_id: str
    previous_sequence: int
    next_sequence: int


@dataclass(frozen=True)
class EventReplay:
    events: tuple[MarketEvent, ...]
    gaps: tuple[SequenceGap, ...]
    duplicate_count: int


def normalize_market_events(events: tuple[MarketEvent, ...], *, reject_gaps: bool = False) -> EventReplay:
    by_id: dict[str, MarketEvent] = {}
    by_sequence: dict[tuple[str, int], MarketEvent] = {}
    duplicates = 0
    for event in events:
        old = by_id.get(event.event_id)
        if old is not None:
            if old.event_hash != event.event_hash:
                raise SimulationContractError("重复 event_id 内容冲突")
            duplicates += 1
            continue
        sequence_key = (event.source_id, event.source_sequence)
        collision = by_sequence.get(sequence_key)
        if collision is not None and collision.event_hash != event.event_hash:
            raise SimulationContractError("同一来源序号对应不同事件")
        by_id[event.event_id] = event
        by_sequence[sequence_key] = event
    ordered = tuple(sorted(by_id.values(), key=lambda item: item.sort_key))
    gaps: list[SequenceGap] = []
    last_by_source: dict[str, int] = {}
    for event in sorted(ordered, key=lambda item: (item.source_id, item.source_sequence, item.event_id)):
        previous = last_by_source.get(event.source_id)
        if previous is not None and event.source_sequence > previous + 1:
            gaps.append(SequenceGap(event.source_id, previous, event.source_sequence))
        last_by_source[event.source_id] = event.source_sequence
    if gaps and reject_gaps:
        raise SimulationContractError("市场事件 source sequence 存在 gap")
    return EventReplay(ordered, tuple(gaps), duplicates)


def run_market_event_loop(
    events: tuple[MarketEvent, ...],
    *,
    decision_time: datetime,
    handler: Callable[[MarketEvent], T],
    reject_gaps: bool = False,
) -> tuple[EventReplay, tuple[T, ...]]:
    require_aware_datetime(decision_time, "decision_time")
    replay = normalize_market_events(events, reject_gaps=reject_gaps)
    outputs: list[T] = []
    for event in replay.events:
        if event.available_time > decision_time:
            continue
        if event.kind == "bar_completed" and not event.completed:
            raise SimulationContractError("未完成 Bar 不能驱动决策")
        outputs.append(handler(event))
    return replay, tuple(outputs)


__all__ = ["EventReplay", "SequenceGap", "normalize_market_events", "run_market_event_loop"]
