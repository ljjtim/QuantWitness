"""持有目标流、可见决策 Bar 和目标替换状态；不持有账户或订单。"""
from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
import hashlib

from research_pipeline.domain import (
    InstrumentKey, PortfolioTarget, Price, OrderIntent, TradingRuleBinding,
)
from research_pipeline.domain.time import require_aware_datetime
from .cash_market import cash_rebalance_deltas, cash_target_quantity
from .execution_clock import ExecutionClock
from .execution_market import MinuteExecutionBar, _require_minute_asset
from .market_rules import CashMarketPolicy
from .minute_rules import _ORDER_REQUIRED_RULES
from .orders import SimulationContractError

@dataclass(frozen=True)
class PreparedMinuteTarget:
    """已经由上游目标工件绑定决策 bar 的单条分钟数量目标。"""

    target: PortfolioTarget
    instrument: InstrumentKey
    desired_quantity: int
    eligible_after: datetime
    decision_bar: MinuteExecutionBar | None = None

    def __post_init__(self) -> None:
        require_aware_datetime(self.eligible_after, "eligible_after")
        if self.target.target_type != "quantity" or len(self.target.entries) != 1:
            raise SimulationContractError("分钟仿真只接受单标的 quantity PortfolioTarget")
        entry = self.target.entries[0]
        if entry.instrument != self.instrument or int(entry.value) != self.desired_quantity:
            raise SimulationContractError("分钟已准备目标与 PortfolioTarget 内容不一致")
        _require_minute_asset(self.instrument.asset_class, tradable=True)
        if self.instrument.asset_class not in _ORDER_REQUIRED_RULES:
            raise SimulationContractError("分钟目标资产类别不受正式仿真支持")
        if (
            self.instrument.asset_class in {"cn_stock", "cn_etf"}
            and self.desired_quantity < 0
        ):
            raise SimulationContractError("股票和 ETF 分钟目标不能为负")
        if self.eligible_after > self.target.decision_time:
            raise SimulationContractError("分钟目标的决策 bar 晚于 decision_time")
        if self.decision_bar is not None and (
            self.decision_bar.instrument_id != self.instrument.instrument_id
            or self.decision_bar.asset_class != self.instrument.asset_class
            or self.decision_bar.bar_end != self.eligible_after
        ):
            raise SimulationContractError("分钟目标绑定的决策 bar 身份或时点不一致")


@dataclass
class _ActiveTarget:
    prepared: PreparedMinuteTarget
    decision_bar: MinuteExecutionBar
    t1_rejection_key: tuple[date, int, int] | None = None


@dataclass
class _PendingTarget:
    prepared: PreparedMinuteTarget
    decision_bar: MinuteExecutionBar | None


class TargetExecution:
    def __init__(
        self, prepared_targets: Iterable[PreparedMinuteTarget], clock: ExecutionClock, *,
        decision_bar_lookup: Callable[[PreparedMinuteTarget], MinuteExecutionBar | None] | None = None,
        allow_empty: bool = False,
    ) -> None:
        self.clock = clock
        self._decision_bar_lookup = decision_bar_lookup
        self._decision_session: date | None = None
        self._session_decision_bars: dict[tuple[str, datetime], MinuteExecutionBar] = {}
        self._target_iterator = iter(prepared_targets)
        self._next_target = None
        self._pending: deque[_PendingTarget] = deque()
        self._next_decision_bar: MinuteExecutionBar | None = None
        self._last_target_key = None
        self._target_digest = hashlib.sha256(b"minute-prepared-target-stream-v1\0")
        self._target_count = 0
        self.asset_class = None
        self.instruments: dict[str, InstrumentKey] = {}
        self._last_bar_by_instrument: dict[str, MinuteExecutionBar] = {}
        self._active: dict[str, _ActiveTarget] = {}
        self._read_next_target()
        if self._next_target is None and not allow_empty:
            raise SimulationContractError("分钟仿真必须提供至少一个 PortfolioTarget")

    def _read_next_target(self) -> None:
        try:
            candidate = next(self._target_iterator)
        except StopIteration:
            self._next_target = None
            self._next_decision_bar = None
            return
        key = (
            candidate.target.decision_time,
            candidate.instrument.instrument_id,
            candidate.target.target_hash,
        )
        if self._last_target_key is not None and key[:2] == self._last_target_key[:2]:
            raise SimulationContractError("同一标的同一决策时点的分钟目标重复")
        if self._last_target_key is not None and key <= self._last_target_key:
            raise SimulationContractError("分钟已准备目标必须唯一并稳定排序")
        self._last_target_key = key
        if self.asset_class is None:
            self.asset_class = candidate.instrument.asset_class
        elif self.asset_class != candidate.instrument.asset_class:
            raise SimulationContractError("一次分钟仿真不能混合资产类别")
        existing = self.instruments.get(candidate.instrument.instrument_id)
        if existing is not None and existing != candidate.instrument:
            raise SimulationContractError("同一标的的 InstrumentKey 身份漂移")
        bar_asset = self.clock.asset_classes.get(candidate.instrument.instrument_id)
        if bar_asset is not None and bar_asset != candidate.instrument.asset_class:
            raise SimulationContractError("目标标的的分钟 bar 资产类别漂移")
        self.instruments[candidate.instrument.instrument_id] = candidate.instrument
        self._target_count += 1
        self._target_digest.update(candidate.target.target_hash.encode("ascii"))
        self._target_digest.update(b"\n")
        self._next_target = candidate
        self._next_decision_bar = candidate.decision_bar
        self._capture_decision_bar()

    def _capture_decision_bar(self, *, allow_lookup: bool = False) -> None:
        if self._next_target is None or self._next_decision_bar is not None:
            return
        instrument_id = self._next_target.instrument.instrument_id
        decision_bar = self._session_decision_bars.get(
            (instrument_id, self._next_target.eligible_after)
        )
        if decision_bar is None:
            decision_bar = self._last_bar_by_instrument.get(instrument_id)
        if decision_bar is None or decision_bar.bar_end != self._next_target.eligible_after:
            active = self._active.get(instrument_id)
            decision_bar = None if active is None else active.decision_bar
        if decision_bar is not None and decision_bar.bar_end == self._next_target.eligible_after:
            self._next_decision_bar = decision_bar
        elif allow_lookup and self._decision_bar_lookup is not None:
            self._next_decision_bar = self._decision_bar_lookup(self._next_target)

    def _prepare_visible_targets(self, as_of: datetime) -> None:
        while self._next_target is not None and self._next_target.target.decision_time <= as_of:
            self._capture_decision_bar(allow_lookup=True)
            self._pending.append(_PendingTarget(self._next_target, self._next_decision_bar))
            self._read_next_target()

    @property
    def has_pending_target(self) -> bool:
        return self._next_target is not None or bool(self._pending)

    @property
    def decision_session(self) -> date | None:
        return self._decision_session

    def session_decision_bars(self) -> Iterable[MinuteExecutionBar]:
        """当前输入交易会话的已观察事实，可在切换会话前流式封存。"""
        return self._session_decision_bars.values()

    def observe_bar(self, bar: MinuteExecutionBar) -> None:
        """保留当前已可见、尚待合格执行时点的目标绑定，不保存全历史 Bar。"""
        if self._decision_session != bar.trading_date:
            self._session_decision_bars.clear()
            self._decision_session = bar.trading_date
        self._session_decision_bars[(bar.instrument_id, bar.bar_end)] = bar
        self._prepare_visible_targets(bar.available_time)
        self._last_bar_by_instrument[bar.instrument_id] = bar
        for pending in self._pending:
            if (pending.decision_bar is None
                    and pending.prepared.instrument.instrument_id == bar.instrument_id
                    and pending.prepared.eligible_after == bar.bar_end):
                pending.decision_bar = bar
        self._capture_decision_bar()

    def activate(self, bar: MinuteExecutionBar) -> None:
        """仅在同标的合格执行 Bar 上激活目标，未获执行机会的目标保持待处理。"""
        self._prepare_visible_targets(bar.bar_start)
        if not bar.completed or bar.quality_status != "pass":
            return
        remaining: deque[_PendingTarget] = deque()
        while self._pending:
            pending = self._pending.popleft()
            if (
                pending.prepared.instrument.instrument_id != bar.instrument_id
                or pending.prepared.target.decision_time > bar.bar_start
                or pending.prepared.eligible_after >= bar.bar_end
            ):
                remaining.append(pending)
                continue
            candidate = pending.prepared
            instrument_id = candidate.instrument.instrument_id
            previous = self._active.get(instrument_id)
            rejection_key = (
                previous.t1_rejection_key
                if previous is not None
                and previous.prepared.desired_quantity == candidate.desired_quantity
                else None
            )
            decision_bar = pending.decision_bar
            if decision_bar is None:
                decision_bar = candidate.decision_bar or self._session_decision_bars.get(
                    (instrument_id, candidate.eligible_after)
                )
                if decision_bar is None and self._decision_bar_lookup is not None:
                    decision_bar = self._decision_bar_lookup(candidate)
            if decision_bar is None or decision_bar.bar_end != candidate.eligible_after:
                raise SimulationContractError("分钟目标缺少已绑定的决策 bar")
            if (
                decision_bar.instrument_id != instrument_id
                or decision_bar.asset_class != candidate.instrument.asset_class
            ):
                raise SimulationContractError("分钟目标绑定的决策 bar 身份不一致")
            if (
                not decision_bar.completed
                or decision_bar.quality_status != "pass"
                or decision_bar.available_time > candidate.target.decision_time
            ):
                raise SimulationContractError("未完成、质量未通过或尚不可见的 bar 不能驱动目标")
            self._active[instrument_id] = _ActiveTarget(candidate, decision_bar, rejection_key)

        self._pending = remaining

    def active_for(self, instrument_id: str) -> _ActiveTarget | None:
        return self._active.get(instrument_id)

def daily_target_deltas(
    target_weights: Mapping[str, object],
    *,
    code_to_instrument: Mapping[str, InstrumentKey],
    open_prices: Mapping[str, Price],
    nav_units: int,
    policies: Mapping[str, CashMarketPolicy],
    current_quantities: Mapping[str, int],
) -> tuple[tuple[str, str, int], ...]:
    """按可见开盘估值和历史申报格点调和目标，保持先卖后买。"""
    # 权重目标按申报增量取整；最低申报量约束差量，不约束总持仓倍数。
    desired = {
        code: cash_target_quantity(
            nav_units=nav_units,
            target_weight=target_weights.get(code, 0.0),
            price=open_prices[code],
            lot_size=policies[code].quantity_grid("buy").step,
        )
        for code in code_to_instrument
    }
    deltas = []
    for code, side, quantity in cash_rebalance_deltas(current_quantities, desired):
        adjusted = policies[code].quantity_grid(side).floor(
            quantity, sellable=current_quantities.get(code, 0),
        )
        if adjusted:
            deltas.append((code, side, adjusted))
    return tuple(deltas)

def minute_target_intents(
    active: _ActiveTarget, bar: MinuteExecutionBar, *,
    rule_binding: TradingRuleBinding, current_quantity: int,
    sellable_quantity: int = 0,
    cash_policy: CashMarketPolicy | None = None,
) -> tuple[OrderIntent, ...]:
    """按确认持仓生成差量意图，反向期货目标保持先平后开。"""
    prepared = active.prepared
    desired = prepared.desired_quantity
    if current_quantity == desired:
        active.t1_rejection_key = None
        return ()
    if prepared.instrument.asset_class == "cn_future":
        operations = _futures_reconciliation(current_quantity, desired)
    else:
        side = "buy" if desired > current_quantity else "sell"
        quantity = abs(desired - current_quantity)
        if side == "sell":
            if sellable_quantity > 0:
                quantity = min(quantity, sellable_quantity)
            elif active.t1_rejection_key == (bar.trading_date, 0, desired):
                return ()
        if cash_policy is not None:
            quantity = cash_policy.quantity_grid(side).floor(
                quantity, sellable=sellable_quantity or current_quantity,
            )
            if quantity == 0:
                return ()
        operations = ((side, quantity, "auto"),)
    return tuple(
        OrderIntent(
            instrument=prepared.instrument, side=side, quantity=quantity,
            position_effect=effect, decision_time=prepared.target.decision_time,
            order_time=bar.bar_start, portfolio_target_hash=prepared.target.target_hash,
            market_data_artifact_hash=bar.bar_hash, rule_binding=rule_binding,
            source_hashes=tuple(sorted({*prepared.target.source_hashes, bar.source_snapshot_hash})),
        )
        for side, quantity, effect in operations
    )


def _futures_reconciliation(
    current: int,
    desired: int,
) -> tuple[tuple[str, int, str], ...]:
    if current == desired:
        return ()
    operations: list[tuple[str, int, str]] = []
    if current and desired and (current > 0) != (desired > 0):
        operations.append(("sell" if current > 0 else "buy", abs(current), "close"))
        operations.append(("buy" if desired > 0 else "sell", abs(desired), "open"))
        return tuple(operations)
    delta = desired - current
    increasing = abs(desired) > abs(current)
    side = "buy" if delta > 0 else "sell"
    operations.append((side, abs(delta), "open" if increasing else "close"))
    return tuple(operations)


