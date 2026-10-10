"""完成 Bar 的稳定执行时钟，不改变交易会话所属日期。"""
from __future__ import annotations

from .execution_market import MinuteExecutionBar
from .orders import SimulationContractError


class ExecutionClock:
    def __init__(self) -> None:
        self._last_key = None
        self._last_identity = None
        self.asset_classes: dict[str, str] = {}
        self.intervals: dict[str, int] = {}

    def consume(self, bar: MinuteExecutionBar) -> None:
        key = (bar.available_time, bar.instrument_id, bar.bar_end, bar.source_sequence)
        if self._last_key is not None and key < self._last_key:
            raise SimulationContractError("分钟流必须按 available_time 稳定排序")
        identity = (bar.instrument_id, bar.bar_end)
        if identity == self._last_identity:
            raise SimulationContractError("分钟执行 bar 主键重复")
        self._last_key, self._last_identity = key, identity
        if self.asset_classes.setdefault(bar.instrument_id, bar.asset_class) != bar.asset_class:
            raise SimulationContractError("同一标的的分钟 bar 资产类别漂移")
        if self.intervals.setdefault(bar.instrument_id, bar.interval_minutes) != bar.interval_minutes:
            raise SimulationContractError("同一标的分钟执行周期漂移")
