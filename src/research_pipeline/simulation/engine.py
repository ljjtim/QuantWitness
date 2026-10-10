"""公共执行协调：提交订单、调用成交执行、完成订单和关闭会话。"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from datetime import date, datetime
from typing import Protocol, TypeVar

from research_pipeline.domain import InstrumentKey

from .target_execution import TargetExecution, _ActiveTarget
from .broker import Broker
from .events import ExecutionOutcome
from .execution_clock import ExecutionClock
from .execution_market import MinuteExecutionBar
from .orders import Order, SimulationContractError

T = TypeVar("T")


T_co = TypeVar("T_co", covariant=True)


class MinuteExecutionContext(Protocol[T_co]):
    """分钟推进所需的会话接口；金融状态与输出缓存由会话持有。"""
    targets: TargetExecution
    explicit_execution: object | None

    def execute_explicit_orders(self, bar: MinuteExecutionBar) -> None: ...

    @property
    def instruments(self) -> dict[str, InstrumentKey]: ...
    @property
    def current_session(self) -> date | None: ...
    def record_input(self, bar: MinuteExecutionBar) -> None: ...
    def collect_bar(self, bar: MinuteExecutionBar) -> None: ...
    def execute_target(self, active: _ActiveTarget, bar: MinuteExecutionBar,
                       execute_order: Callable) -> None: ...
    def start_session(self, session: date, event_time: datetime) -> None: ...
    def close_current_session(self) -> T_co | None: ...


class ExecutionEngine:
    def __init__(self) -> None:
        self.broker = Broker()
        self.clock = ExecutionClock()
        self.finished = False

    def consume_bar(
        self, session: MinuteExecutionContext[T], bar: MinuteExecutionBar,
    ) -> tuple[T, ...]:
        if self.finished:
            raise SimulationContractError("分钟仿真已经结束，不能继续输入 bar")
        self.clock.consume(bar)
        session.record_input(bar)
        session.targets.activate(bar)
        instrument = session.instruments.get(bar.instrument_id)
        if instrument is None:
            session.targets.observe_bar(bar)
            return ()
        if bar.asset_class != instrument.asset_class:
            raise SimulationContractError("目标标的的分钟 bar 资产类别漂移")
        if not bar.completed or bar.quality_status != "pass":
            session.targets.observe_bar(bar)
            return ()
        completed = self._advance_session(session, bar)
        session.collect_bar(bar)
        active = session.targets.active_for(bar.instrument_id)
        if session.explicit_execution is not None:
            session.execute_explicit_orders(bar)
        elif (
            active is not None
            and active.prepared.target.decision_time <= bar.bar_start
            and active.prepared.eligible_after < bar.bar_end
        ):
            session.execute_target(active, bar, self.execute_order)
        session.targets.observe_bar(bar)
        return completed

    def _advance_session(
        self, session: MinuteExecutionContext[T], bar: MinuteExecutionBar,
    ) -> tuple[T, ...]:
        if session.current_session is None:
            session.start_session(bar.trading_date, bar.bar_start)
            return ()
        if bar.trading_date == session.current_session:
            return ()
        if bar.trading_date < session.current_session:
            raise SimulationContractError("分钟流的 trading_date 发生倒退")
        output = session.close_current_session()
        session.start_session(bar.trading_date, bar.bar_start)
        return () if output is None else (output,)

    def finish(self, session: MinuteExecutionContext[T]) -> tuple[T, ...]:
        if self.finished:
            raise SimulationContractError("分钟仿真 finish 只能调用一次")
        self.finished = True
        if session.targets.has_pending_target:
            raise SimulationContractError("分钟目标没有下一 eligible completed bar")
        output = session.close_current_session()
        return () if output is None else (output,)

    def execute_order(self, order: Order, *, trading_session: date,
                      event_time: datetime,
                      execute: Callable[[], ExecutionOutcome[T]]) -> T:
        if event_time < order.submitted_at:
            raise SimulationContractError("成交观察时间不能早于订单提交")
        self.broker.submit(order, trading_session)
        return self.execute_active_order(order.order_id, event_time=event_time,
                                         execute=execute, reject_unfilled=True)

    def execute_active_order(self, order_id: str, *, event_time: datetime,
                             execute: Callable[[], ExecutionOutcome[T]],
                             reject_unfilled: bool = False,
                             after_fill: Callable[[T], None] | None = None) -> T:
        """新订单和活动 DAY 订单共用成交确认与 IOC 终结路径。"""
        order = self.broker.orders[order_id]
        if event_time < order.submitted_at:
            raise SimulationContractError("成交观察时间不能早于订单提交")
        if order.status not in {"submitted", "accepted", "partially_filled"}:
            raise SimulationContractError("只能执行活动订单")
        outcome = execute()
        if outcome.filled_quantity == 0:
            if reject_unfilled:
                self.broker.advance(order_id, "reject", event_time,
                                    reason=outcome.reason or "formal_simulation_unfilled")
            elif order.time_in_force == "IOC":
                self.broker.advance(order_id, "cancel", event_time,
                                    reason=outcome.reason or "unfilled")
        else:
            if order.status == "submitted":
                self.broker.advance(order_id, "accept", order.submitted_at)
            current = self.broker.advance(order_id, "fill", event_time,
                                          quantity=outcome.filled_quantity)
            if after_fill is not None:
                after_fill(outcome.value)
            current = self.broker.orders[order_id]
            if current.status == "partially_filled" and order.time_in_force == "IOC":
                self.broker.advance(order_id, "cancel", event_time,
                                    reason="ioc_remainder_cancelled")
        return outcome.value

    def close_session(self, session: date, event_time: datetime) -> None:
        self.broker.close_session(session, event_time)

    def drain_lifecycle(self) -> tuple[dict[str, object], ...]:
        return self.broker.drain_lifecycle()

    def run_daily_sessions(
        self, sessions: Iterable[date], *,
        before_open: Callable[[date], T],
        reconcile: Callable[[date, T], None],
        after_close: Callable[[date, T], datetime],
    ) -> Iterator[tuple[dict[str, object], ...]]:
        """按盘前结算、目标执行、收盘估值、订单到期顺序推进日频会话。"""
        if self.finished:
            raise SimulationContractError("执行已经结束，不能再次输入会话")
        for session in sessions:
            context = before_open(session)
            reconcile(session, context)
            closing_time = after_close(session, context)
            self.close_session(session, closing_time)
            yield self.drain_lifecycle()
        self.finished = True
