"""由数量目标、完成分钟 bar 与 PIT 规则快照驱动的正式仿真。"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from zoneinfo import ZoneInfo

from research_pipeline.domain import (
    InstrumentKey,
    MarketRuleSnapshot,
    MinuteRuleBinding,
    MinuteRuleResolver,
    MinuteRuleSnapshotBundle,
    MinuteRuleSnapshotError,
    OrderIntent,
    PortfolioTarget,
    Price,
    SessionCalendarResolver,
    TradingRuleBinding,
    load_session_policy_bundle,
)
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    require_canonical_asset_class,
    require_frequency,
    require_simulation_entry,
)

from .cash_market import (
    CashMarketPolicy,
    OpeningSnapshot,
    execute_cash_order,
    settle_cash_daily_open,
)
from .events import FinancialEvent
from .intent_port import (
    CASH_DAILY_BACKEND,
    CN_FUTURES_DAILY_BACKEND,
    IntentToOrderPort,
)
from .ledger import (
    ExecutionGroup,
    FuturesLedgerState,
    FuturesPosition,
    SpotLedgerState,
    reduce_futures,
)
from .orders import Order, SimulationContractError
from .result_contract import (
    SimulationResultContract,
    SimulationResultSemantics,
    build_simulation_result_contract,
    canonical_simulation_table,
)


INTRADAY_EXECUTION_POLICY_VERSION = "intraday-execution-policy-v1"
_ZONE = ZoneInfo("Asia/Shanghai")
_SUPPORTED_INTERVALS = (1, 5, 15, 30, 60, 120)
_EMPTY_EVENT_HASH = typed_canonical_hash([])
_ORDER_REQUIRED_RULES = {
    "cn_stock": (
        "rule.cn_stock.instrument_lifecycle.v1",
        "rule.cn_stock.lot_size.v1",
        "rule.cn_stock.price_limit.v1",
        "rule.cn_stock.session.v1",
        "rule.cn_stock.settlement.v1",
        "rule.cn_stock.suspension.v1",
        "rule.cn_stock.trading_fee.v1",
    ),
    "cn_etf": (
        "rule.cn_fund.instrument_lifecycle.v1",
        "rule.cn_fund.lot_size.v1",
        "rule.cn_fund.price_limit.v1",
        "rule.cn_fund.session.v1",
        "rule.cn_fund.settlement.v1",
        "rule.cn_fund.trading_fee.v1",
    ),
    "cn_future": (
        "rule.cn_futures.actual_contract_mapping.v1",
        "rule.cn_futures.contract_lifecycle.v1",
        "rule.cn_futures.contract_multiplier.v1",
        "rule.cn_futures.delivery_expiry.v1",
        "rule.cn_futures.fee_schedule.v1",
        "rule.cn_futures.margin.v1",
        "rule.cn_futures.price_limit.v1",
        "rule.cn_futures.price_tick.v1",
        "rule.cn_futures.session.v1",
    ),
}
_FUTURES_SETTLEMENT_RULES = (
    "rule.cn_futures.contract_multiplier.v1",
    "rule.cn_futures.margin.v1",
    "rule.cn_futures.price_tick.v1",
    "rule.cn_futures.session.v1",
    "rule.cn_futures.settlement.v1",
)


def _require_minute_asset(asset_class: str, *, tradable: bool) -> None:
    try:
        require_canonical_asset_class(asset_class)
        require_frequency(asset_class, "minute")
        if tradable:
            require_simulation_entry(asset_class, "minute_event")
    except AssetTaxonomyError as exc:
        raise SimulationContractError(str(exc)) from exc


@dataclass(frozen=True)
class IntradayExecutionPolicy:
    model_id: str = "next_bar_participation_v1"
    model_version: str = "1.0.0"
    participation_ppm: int = 100_000
    claim_ceiling: str = "bar_level_research_only"
    contract_version: str = INTRADAY_EXECUTION_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != INTRADAY_EXECUTION_POLICY_VERSION:
            raise SimulationContractError("分钟执行 policy version 不受支持")
        if self.model_id != "next_bar_participation_v1" or self.model_version != "1.0.0":
            raise SimulationContractError("首版只支持下一完成 bar 参与模型")
        if (
            type(self.participation_ppm) is not int
            or not 1 <= self.participation_ppm <= 1_000_000
        ):
            raise SimulationContractError("分钟参与率必须在 (0,100%] 内")
        if self.claim_ceiling != "bar_level_research_only":
            raise SimulationContractError("bar-level 仿真不能声明 Tick/LOB 能力")

    @property
    def policy_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "participation_ppm": self.participation_ppm,
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class MinuteExecutionBar:
    instrument_id: str
    asset_class: str
    trading_date: date
    session_id: str
    bar_start: datetime
    bar_end: datetime
    available_time: datetime
    receipt_time: datetime
    interval_minutes: int
    open_units: int
    high_units: int
    low_units: int
    close_units: int
    avg_units: int | None
    volume: int
    open_interest: int | None
    completed: bool
    quality_status: str
    source_snapshot_hash: str
    source_sequence: int

    def __post_init__(self) -> None:
        _require_minute_asset(self.asset_class, tradable=False)
        if not self.instrument_id.strip() or not self.session_id.strip():
            raise SimulationContractError("分钟执行 bar 身份无效")
        for field in ("bar_start", "bar_end", "available_time", "receipt_time"):
            value = getattr(self, field)
            require_aware_datetime(value, field)
            if value.utcoffset() != _ZONE.utcoffset(value):
                raise SimulationContractError("分钟执行 bar 必须使用 Asia/Shanghai 时区")
        if not self.bar_start < self.bar_end <= self.available_time <= self.receipt_time:
            raise SimulationContractError("分钟执行 bar 时间顺序无效")
        if self.interval_minutes not in _SUPPORTED_INTERVALS:
            raise SimulationContractError("分钟执行 bar 周期不受支持")
        prices = (self.open_units, self.high_units, self.low_units, self.close_units)
        if any(type(item) is not int or item <= 0 for item in prices):
            raise SimulationContractError("分钟执行价格必须是正整数定点值")
        if self.low_units > min(prices) or self.high_units < max(prices):
            raise SimulationContractError("分钟执行 OHLC 上下界不一致")
        if self.avg_units is not None and (
            type(self.avg_units) is not int or self.avg_units <= 0
        ):
            raise SimulationContractError("分钟执行 avg 必须是正整数定点值")
        if type(self.volume) is not int or self.volume < 0 or self.source_sequence < 0:
            raise SimulationContractError("分钟执行 volume/source sequence 无效")
        if self.asset_class == "cn_future":
            if type(self.open_interest) is not int or self.open_interest < 0:
                raise SimulationContractError("期货分钟执行必须提供非负 open_interest")
        elif self.open_interest is not None:
            raise SimulationContractError("非期货分钟执行不得提供 open_interest")
        _hash(self.source_snapshot_hash, "source_snapshot_hash")

    @property
    def bar_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "trading_date": self.trading_date.isoformat(),
            "session_id": self.session_id,
            "bar_start": self.bar_start.isoformat(),
            "bar_end": self.bar_end.isoformat(),
            "available_time": self.available_time.isoformat(),
            "receipt_time": self.receipt_time.isoformat(),
            "interval_minutes": self.interval_minutes,
            "open_units": self.open_units,
            "high_units": self.high_units,
            "low_units": self.low_units,
            "close_units": self.close_units,
            "avg_units": self.avg_units,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "completed": self.completed,
            "quality_status": self.quality_status,
            "source_snapshot_hash": self.source_snapshot_hash,
            "source_sequence": self.source_sequence,
        }


@dataclass(frozen=True)
class PreparedMinuteTarget:
    """已经由上游目标工件绑定决策 bar 的单条分钟数量目标。"""

    target: PortfolioTarget
    instrument: InstrumentKey
    desired_quantity: int
    eligible_after: datetime

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


@dataclass
class _ActiveTarget:
    prepared: PreparedMinuteTarget
    decision_bar: MinuteExecutionBar
    t1_rejection_key: tuple[date, int, int] | None = None


@dataclass(frozen=True)
class _ResolvedRules:
    bindings: tuple[MinuteRuleBinding, ...]
    parameters: Mapping[str, object]
    identity_hash: str
    available_at: datetime


@dataclass(frozen=True)
class MinuteSimulationSessionOutput:
    """一个交易日已经闭合、可立即写入列式分区的六表行。"""

    session: date
    orders: tuple[dict[str, object], ...]
    fills: tuple[dict[str, object], ...]
    positions: tuple[dict[str, object], ...]
    cash: tuple[dict[str, object], ...]
    costs: tuple[dict[str, object], ...]
    valuations: tuple[dict[str, object], ...]
    execution_bars: tuple[MinuteExecutionBar, ...]
    decision_benchmarks: tuple[dict[str, object], ...]
    execution_observations: tuple[dict[str, object], ...]
    settlement_events: tuple[dict[str, object], ...]

    @property
    def rows(self) -> dict[str, tuple[dict[str, object], ...]]:
        return {
            "orders": self.orders,
            "fills": self.fills,
            "positions": self.positions,
            "cash": self.cash,
            "costs": self.costs,
            "valuations": self.valuations,
        }


class MinuteEventSimulationStateMachine:
    """按已排序分钟 bar 推进，只在内存中保留当前交易日和账户状态。"""

    def __init__(
        self,
        prepared_targets: Iterable[PreparedMinuteTarget],
        *,
        rule_bundle: MinuteRuleSnapshotBundle,
        policy: IntradayExecutionPolicy | None = None,
        initial_cash_units: int = 100_000_000,
    ) -> None:
        self.policy = policy or IntradayExecutionPolicy()
        if type(initial_cash_units) is not int or initial_cash_units <= 0:
            raise SimulationContractError("分钟仿真初始资金必须是正整数")
        self.rule_bundle = rule_bundle
        self.initial_cash_units = initial_cash_units
        self.resolver = MinuteRuleResolver(rule_bundle)
        self._target_iterator: Iterator[PreparedMinuteTarget] = iter(prepared_targets)
        self._next_target: PreparedMinuteTarget | None = None
        self._last_target_key: tuple[datetime, str, str] | None = None
        self._last_bar_key: tuple[datetime, str, datetime, int] | None = None
        self._last_bar_identity: tuple[str, datetime] | None = None
        self._target_digest = hashlib.sha256(b"minute-prepared-target-stream-v1\0")
        self._bar_digest = hashlib.sha256(b"minute-execution-bar-stream-v1\0")
        self._target_count = 0
        self._bar_count = 0
        self.asset_class: str | None = None
        self.instruments: dict[str, InstrumentKey] = {}
        self._bar_asset_classes: dict[str, str] = {}
        self._bar_intervals: dict[str, int] = {}
        self._last_bar_by_instrument: dict[str, MinuteExecutionBar] = {}
        self._active: dict[str, _ActiveTarget] = {}
        self._spot_state: SpotLedgerState | None = None
        self._futures_state: FuturesLedgerState | None = None
        self._current_session: date | None = None
        self._session_bars: list[MinuteExecutionBar] = []
        self._session_orders: list[dict[str, object]] = []
        self._session_fills: list[dict[str, object]] = []
        self._session_costs: list[dict[str, object]] = []
        self._session_prices: dict[str, int] = {}
        self._session_valuation_time: datetime | None = None
        self._session_non_trade_events: list[str] = []
        self._session_decision_benchmarks: list[dict[str, object]] = []
        self._session_execution_observations: list[dict[str, object]] = []
        self._session_settlement_events: list[dict[str, object]] = []
        self._previous_cash = initial_cash_units
        self._previous_quantities: dict[str, int] = {}
        self._finished = False
        self._read_next_target()
        if self._next_target is None:
            raise SimulationContractError("分钟仿真必须提供至少一个 PortfolioTarget")

    def consume_bar(
        self,
        bar: MinuteExecutionBar,
    ) -> tuple[MinuteSimulationSessionOutput, ...]:
        if self._finished:
            raise SimulationContractError("分钟仿真已经结束，不能继续输入 bar")
        order_key = (
            bar.available_time,
            bar.instrument_id,
            bar.bar_end,
            bar.source_sequence,
        )
        if self._last_bar_key is not None and order_key < self._last_bar_key:
            raise SimulationContractError("分钟流必须按 available_time 稳定排序")
        bar_identity = (bar.instrument_id, bar.bar_end)
        if bar_identity == self._last_bar_identity:
            raise SimulationContractError("分钟执行 bar 主键重复")
        self._last_bar_key = order_key
        self._last_bar_identity = bar_identity
        self._bar_count += 1
        self._bar_digest.update(bar.bar_hash.encode("ascii"))
        self._bar_digest.update(b"\n")
        previous_asset = self._bar_asset_classes.setdefault(
            bar.instrument_id, bar.asset_class
        )
        if previous_asset != bar.asset_class:
            raise SimulationContractError("同一标的的分钟 bar 资产类别漂移")
        previous_interval = self._bar_intervals.setdefault(
            bar.instrument_id, bar.interval_minutes
        )
        if previous_interval != bar.interval_minutes:
            raise SimulationContractError("同一标的分钟执行周期漂移")

        self._activate_targets(bar.bar_start)
        instrument = self.instruments.get(bar.instrument_id)
        if instrument is None:
            self._last_bar_by_instrument[bar.instrument_id] = bar
            return ()
        if bar.asset_class != instrument.asset_class:
            raise SimulationContractError("目标标的的分钟 bar 资产类别漂移")
        if not bar.completed or bar.quality_status != "pass":
            self._last_bar_by_instrument[bar.instrument_id] = bar
            return ()

        completed = list(self._advance_session(bar))
        self._session_bars.append(bar)
        self._session_prices[instrument.instrument_hash] = bar.close_units
        if (
            self._session_valuation_time is None
            or bar.available_time > self._session_valuation_time
        ):
            self._session_valuation_time = bar.available_time

        current_target = self._active.get(bar.instrument_id)
        if (
            current_target is not None
            and current_target.prepared.target.decision_time <= bar.bar_start
            and current_target.prepared.eligible_after < bar.bar_end
        ):
            used_capacity: dict[str, int] = {}
            order_start = len(self._session_orders)
            fill_start = len(self._session_fills)
            if self.asset_class in {"cn_stock", "cn_etf"}:
                self._spot_state = _reconcile_spot_target(
                    current_target,
                    bar,
                    resolver=self.resolver,
                    bundle=self.rule_bundle,
                    policy=self.policy,
                    state=self._spot_state,
                    initial_cash_units=self.initial_cash_units,
                    used_capacity=used_capacity,
                    order_rows=self._session_orders,
                    fill_rows=self._session_fills,
                    cost_rows=self._session_costs,
                )
            else:
                self._futures_state = _reconcile_futures_target(
                    current_target,
                    bar,
                    resolver=self.resolver,
                    bundle=self.rule_bundle,
                    policy=self.policy,
                    state=self._futures_state,
                    initial_cash_units=self.initial_cash_units,
                    used_capacity=used_capacity,
                    order_rows=self._session_orders,
                    fill_rows=self._session_fills,
                    cost_rows=self._session_costs,
                )
            for order in self._session_orders[order_start:]:
                decision_bar = current_target.decision_bar
                self._session_decision_benchmarks.append({
                    "portfolio_id": str(order["portfolio_id"]),
                    "order_id": str(order["order_id"]),
                    "decision_price_units": decision_bar.close_units,
                    "available_at": decision_bar.available_time,
                    "source_hash": decision_bar.bar_hash,
                })
            for fill in self._session_fills[fill_start:]:
                if bar.volume <= 0:
                    raise SimulationContractError(
                        "分钟正式 fill 对应执行 bar 缺少正的可见容量"
                    )
                self._session_execution_observations.append({
                    "source_fill_id": str(fill["fill_id"]),
                    "arrival_price_units": bar.open_units,
                    "arrival_price_available_at": bar.available_time,
                    "visible_capacity": bar.volume,
                    "capacity_available_at": bar.available_time,
                })
        self._last_bar_by_instrument[bar.instrument_id] = bar
        return tuple(completed)

    def finish(self) -> tuple[MinuteSimulationSessionOutput, ...]:
        if self._finished:
            raise SimulationContractError("分钟仿真 finish 只能调用一次")
        self._finished = True
        if self._next_target is not None:
            raise SimulationContractError("分钟目标没有下一 eligible completed bar")
        output = self._close_current_session()
        return () if output is None else (output,)

    @property
    def source_simulation_hash(self) -> str:
        if not self._finished:
            raise SimulationContractError("分钟仿真未结束，不能生成正式输入身份")
        return typed_canonical_hash({
            "stream_contract": "minute-event-simulation-input-v1",
            "target_count": self._target_count,
            "target_stream_sha256": self._target_digest.hexdigest(),
            "bar_count": self._bar_count,
            "bar_stream_sha256": self._bar_digest.hexdigest(),
            "rule_bundle_hash": self.rule_bundle.bundle_hash,
            "policy": self.policy.to_dict(),
            "initial_cash_units": self.initial_cash_units,
        })

    @property
    def semantics(self) -> SimulationResultSemantics:
        if not self._finished or self.asset_class is None:
            raise SimulationContractError("分钟仿真未结束，不能生成正式语义")
        return SimulationResultSemantics(
            asset_class=self.asset_class,
            frequency="minute",
            decision_time_convention="completed_bar_portfolio_target",
            execution_time_convention="next_eligible_completed_bar",
            valuation_time_convention="last_completed_bar_per_trading_session",
            price_convention=(
                "raw_integer_cny_rule_scale_times_contract_multiplier"
                if self.asset_class == "cn_future"
                else "raw_integer_cny_rule_scale"
            ),
            fee_model_version="minute_rule_snapshot_fee_v1",
            calendar_id="minute_rule_snapshot_session",
            settlement_policy_id=(
                "futures_mark_to_market"
                if self.asset_class == "cn_future"
                else "cash_market_rule"
            ),
            missing_data_policy="fail_closed",
            negative_cash_allowed=False,
            timeline_semantics_hash=typed_canonical_hash({
                "stream_contract": "minute-target-timeline-v1",
                "rule_bundle_hash": self.rule_bundle.bundle_hash,
                "execution_policy_hash": self.policy.policy_hash,
                "target_count": self._target_count,
                "target_stream_sha256": self._target_digest.hexdigest(),
            }),
        )

    def _read_next_target(self) -> None:
        try:
            candidate = next(self._target_iterator)
        except StopIteration:
            self._next_target = None
            return
        key = (
            candidate.target.decision_time,
            candidate.instrument.instrument_id,
            candidate.target.target_hash,
        )
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
        bar_asset = self._bar_asset_classes.get(candidate.instrument.instrument_id)
        if bar_asset is not None and bar_asset != candidate.instrument.asset_class:
            raise SimulationContractError("目标标的的分钟 bar 资产类别漂移")
        self.instruments[candidate.instrument.instrument_id] = candidate.instrument
        self._target_count += 1
        self._target_digest.update(candidate.target.target_hash.encode("ascii"))
        self._target_digest.update(b"\n")
        self._next_target = candidate

    def _activate_targets(self, bar_start: datetime) -> None:
        while (
            self._next_target is not None
            and self._next_target.target.decision_time <= bar_start
        ):
            candidate = self._next_target
            instrument_id = candidate.instrument.instrument_id
            previous = self._active.get(instrument_id)
            rejection_key = (
                previous.t1_rejection_key
                if previous is not None
                and previous.prepared.desired_quantity == candidate.desired_quantity
                else None
            )
            decision_bar = self._last_bar_by_instrument.get(instrument_id)
            if decision_bar is None or decision_bar.bar_end != candidate.eligible_after:
                raise SimulationContractError("分钟目标缺少已绑定的决策 bar")
            if (
                not decision_bar.completed
                or decision_bar.quality_status != "pass"
                or decision_bar.available_time > candidate.target.decision_time
            ):
                raise SimulationContractError(
                    "未完成、质量未通过或尚不可见的 bar 不能驱动目标"
                )
            self._active[instrument_id] = _ActiveTarget(
                candidate, decision_bar, rejection_key
            )
            self._read_next_target()

    def _advance_session(
        self,
        bar: MinuteExecutionBar,
    ) -> tuple[MinuteSimulationSessionOutput, ...]:
        if self._current_session is None:
            self._current_session = bar.trading_date
            return ()
        if bar.trading_date == self._current_session:
            return ()
        if bar.trading_date < self._current_session:
            raise SimulationContractError("分钟流的 trading_date 发生倒退")
        output = self._close_current_session()
        self._reset_session(bar.trading_date)
        if self.asset_class in {"cn_stock", "cn_etf"} and self._spot_state is not None:
            self._spot_state, settlement_events = settle_cash_daily_open(
                self._spot_state,
                effective_time=bar.bar_start,
                rule_hash=self.rule_bundle.bundle_hash,
            )
            self._session_non_trade_events.extend(
                item.event_hash for item in settlement_events
            )
        return () if output is None else (output,)

    def _close_current_session(self) -> MinuteSimulationSessionOutput | None:
        if self._current_session is None or self.asset_class is None:
            return None
        state: SpotLedgerState | FuturesLedgerState | None
        if self.asset_class == "cn_future":
            if self._futures_state is not None:
                (
                    self._futures_state,
                    settlement_time,
                    settlement_hashes,
                    settlement_events,
                ) = _settle_futures_session(
                    state=self._futures_state,
                    instruments=self.instruments,
                    bars=tuple(self._session_bars),
                    trading_date=self._current_session,
                    resolver=self.resolver,
                    bundle=self.rule_bundle,
                )
                if settlement_time is not None:
                    self._session_valuation_time = settlement_time
                self._session_non_trade_events.extend(settlement_hashes)
                self._session_settlement_events.extend(settlement_events)
            state = self._futures_state
        else:
            state = self._spot_state
        if state is None:
            return MinuteSimulationSessionOutput(
                session=self._current_session,
                orders=(),
                fills=(),
                positions=(),
                cash=(),
                costs=(),
                valuations=(),
                execution_bars=tuple(self._session_bars),
                decision_benchmarks=tuple(self._session_decision_benchmarks),
                execution_observations=tuple(self._session_execution_observations),
                settlement_events=(),
            )
        if self._session_valuation_time is None:
            raise SimulationContractError("分钟日终快照缺少估值时点")
        rows, self._previous_cash, self._previous_quantities = _build_session_rows(
            asset_class=self.asset_class,
            instruments=self.instruments,
            state=state,
            session=self._current_session,
            valuation_time=self._session_valuation_time,
            prices=self._session_prices,
            non_trade_events=self._session_non_trade_events,
            order_rows=self._session_orders,
            fill_rows=self._session_fills,
            cost_rows=self._session_costs,
            initial_cash_units=self.initial_cash_units,
            previous_cash=self._previous_cash,
            previous_quantities=self._previous_quantities,
        )
        return MinuteSimulationSessionOutput(
            session=self._current_session,
            orders=tuple(rows["orders"]),
            fills=tuple(rows["fills"]),
            positions=tuple(rows["positions"]),
            cash=tuple(rows["cash"]),
            costs=tuple(rows["costs"]),
            valuations=tuple(rows["valuations"]),
            execution_bars=tuple(self._session_bars),
            decision_benchmarks=tuple(self._session_decision_benchmarks),
            execution_observations=tuple(self._session_execution_observations),
            settlement_events=tuple(self._session_settlement_events),
        )

    def _reset_session(self, session: date) -> None:
        self._current_session = session
        self._session_bars = []
        self._session_orders = []
        self._session_fills = []
        self._session_costs = []
        self._session_prices = {}
        self._session_valuation_time = None
        self._session_non_trade_events = []
        self._session_decision_benchmarks = []
        self._session_execution_observations = []
        self._session_settlement_events = []


def run_minute_event_simulation(
    targets: tuple[PortfolioTarget, ...],
    bars: tuple[MinuteExecutionBar, ...],
    *,
    rule_bundle: MinuteRuleSnapshotBundle,
    policy: IntradayExecutionPolicy | None = None,
    initial_cash_units: int = 100_000_000,
) -> SimulationResultContract:
    """小样本兼容入口；正式 Runtime 使用同一状态机逐交易日写出。"""

    ordered_bars = _validate_and_sort_bars(bars)
    prepared, _asset_class, _instruments = _prepare_targets(targets, ordered_bars)
    machine = MinuteEventSimulationStateMachine(
        prepared,
        rule_bundle=rule_bundle,
        policy=policy,
        initial_cash_units=initial_cash_units,
    )
    rows = {name: [] for name in (
        "orders", "fills", "positions", "cash", "costs", "valuations"
    )}
    for bar in ordered_bars:
        for output in machine.consume_bar(bar):
            for name, values in output.rows.items():
                rows[name].extend(values)
    for output in machine.finish():
        for name, values in output.rows.items():
            rows[name].extend(values)
    tables = {
        name: canonical_simulation_table(name, values)
        for name, values in rows.items()
    }
    return build_simulation_result_contract(
        tables=tables,
        semantics=machine.semantics,
        source_simulation_hash=machine.source_simulation_hash,
    )


def _validate_and_sort_bars(
    bars: tuple[MinuteExecutionBar, ...],
) -> tuple[MinuteExecutionBar, ...]:
    ordered = tuple(sorted(
        bars,
        key=lambda item: (
            item.available_time,
            item.instrument_id,
            item.bar_end,
            item.source_sequence,
        ),
    ))
    keys = {(item.instrument_id, item.bar_end) for item in ordered}
    if len(keys) != len(ordered):
        raise SimulationContractError("分钟执行 bar 主键重复")
    intervals: dict[str, int] = {}
    for item in ordered:
        previous = intervals.setdefault(item.instrument_id, item.interval_minutes)
        if previous != item.interval_minutes:
            raise SimulationContractError("同一标的分钟执行周期漂移")
    return ordered


def _prepare_targets(
    targets: tuple[PortfolioTarget, ...],
    bars: tuple[MinuteExecutionBar, ...],
) -> tuple[tuple[PreparedMinuteTarget, ...], str, dict[str, InstrumentKey]]:
    if not targets:
        raise SimulationContractError("分钟仿真必须提供至少一个 PortfolioTarget")
    bars_by_instrument: dict[str, list[MinuteExecutionBar]] = {}
    for bar in bars:
        bars_by_instrument.setdefault(bar.instrument_id, []).append(bar)
    prepared = []
    identities = set()
    instruments: dict[str, InstrumentKey] = {}
    asset_classes = set()
    for target in targets:
        if target.target_type != "quantity" or len(target.entries) != 1:
            raise SimulationContractError("分钟仿真只接受单标的 quantity PortfolioTarget")
        entry = target.entries[0]
        instrument = entry.instrument
        _require_minute_asset(instrument.asset_class, tradable=True)
        if instrument.asset_class not in _ORDER_REQUIRED_RULES:
            raise SimulationContractError("分钟目标资产类别不受正式仿真支持")
        desired = int(entry.value)
        if instrument.asset_class in {"cn_stock", "cn_etf"} and desired < 0:
            raise SimulationContractError("股票和 ETF 分钟目标不能为负")
        identity = (instrument.instrument_id, target.decision_time)
        if identity in identities:
            raise SimulationContractError("同一标的同一决策时点的分钟目标重复")
        identities.add(identity)
        existing = instruments.get(instrument.instrument_id)
        if existing is not None and existing != instrument:
            raise SimulationContractError("同一标的的 InstrumentKey 身份漂移")
        instruments[instrument.instrument_id] = instrument
        asset_classes.add(instrument.asset_class)
        instrument_bars = bars_by_instrument.get(instrument.instrument_id, [])
        if any(item.asset_class != instrument.asset_class for item in instrument_bars):
            raise SimulationContractError("目标标的的分钟 bar 资产类别漂移")
        candidates = [
            item
            for item in instrument_bars
            if item.bar_end <= target.decision_time
        ]
        if not candidates:
            raise SimulationContractError("分钟目标缺少决策 bar")
        decision_bar = max(candidates, key=lambda item: (item.bar_end, item.source_sequence))
        if not decision_bar.completed or decision_bar.quality_status != "pass":
            raise SimulationContractError("未完成或质量未通过的 bar 不能驱动目标")
        if decision_bar.available_time > target.decision_time:
            raise SimulationContractError("decision_time 早于输入 bar available_time")
        if decision_bar.asset_class != instrument.asset_class:
            raise SimulationContractError("目标与决策 bar 资产类别不一致")
        eligible = any(
            item.bar_end > decision_bar.bar_end
            and item.bar_start >= target.decision_time
            and item.completed
            and item.quality_status == "pass"
            for item in instrument_bars
        )
        if not eligible:
            raise SimulationContractError("分钟目标没有下一 eligible completed bar")
        prepared.append(
            PreparedMinuteTarget(target, instrument, desired, decision_bar.bar_end)
        )
    if len(asset_classes) != 1:
        raise SimulationContractError("一次分钟仿真不能混合资产类别")
    result = tuple(sorted(
        prepared,
        key=lambda item: (
            item.target.decision_time,
            item.instrument.instrument_id,
            item.target.target_hash,
        ),
    ))
    return result, next(iter(asset_classes)), instruments


def _reconcile_spot_target(
    active: _ActiveTarget,
    bar: MinuteExecutionBar,
    *,
    resolver: MinuteRuleResolver,
    bundle: MinuteRuleSnapshotBundle,
    policy: IntradayExecutionPolicy,
    state: SpotLedgerState | None,
    initial_cash_units: int,
    used_capacity: dict[str, int],
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> SpotLedgerState | None:
    instrument = active.prepared.instrument
    current, sellable = _spot_quantities(state, instrument.instrument_hash)
    desired = active.prepared.desired_quantity
    if current == desired:
        active.t1_rejection_key = None
        return state
    rules = _resolve_rules(
        resolver,
        bundle=bundle,
        asset_class=instrument.asset_class,
        instrument_id=instrument.instrument_id,
        effective_on=bar.trading_date,
        as_of=bar.bar_start,
    )
    lot_size = _positive_integer(
        rules.parameters,
        "buy_lot_shares" if instrument.asset_class == "cn_stock" else "buy_lot_units",
    )
    if desired % lot_size:
        raise SimulationContractError("分钟目标数量不符合交易单位")
    _require_cash_lifecycle(
        rules.parameters,
        asset_class=instrument.asset_class,
        trading_date=bar.trading_date,
    )
    _require_visible_price_limits(rules.parameters, bar=bar)
    if (
        instrument.asset_class == "cn_etf"
        and rules.parameters.get("cost_model_scope") != "research_assumption"
    ):
        raise SimulationContractError("ETF 分钟费用必须明确声明为研究成本假设")
    side = "buy" if desired > current else "sell"
    quantity = abs(desired - current)
    if side == "sell" and sellable > 0:
        quantity = min(quantity, sellable)
    if side == "sell" and sellable == 0:
        rejection_key = (bar.trading_date, sellable, desired)
        if active.t1_rejection_key == rejection_key:
            return state
    capacity = _remaining_capacity(bar, policy, used_capacity, lot_size=lot_size)
    result_state, reason, filled = _execute_spot_order(
        target=active.prepared.target,
        instrument=instrument,
        side=side,
        quantity=quantity,
        bar=bar,
        rules=rules,
        state=state,
        initial_cash_units=initial_cash_units,
        visible_capacity=capacity,
        order_rows=order_rows,
        fill_rows=fill_rows,
        cost_rows=cost_rows,
    )
    if filled:
        used_capacity[bar.bar_hash] = used_capacity.get(bar.bar_hash, 0) + filled
    if reason == "t1_sell_blocked":
        active.t1_rejection_key = (bar.trading_date, sellable, desired)
    elif filled or reason != "t1_sell_blocked":
        active.t1_rejection_key = None
    return result_state


def _execute_spot_order(
    *,
    target: PortfolioTarget,
    instrument: InstrumentKey,
    side: str,
    quantity: int,
    bar: MinuteExecutionBar,
    rules: _ResolvedRules,
    state: SpotLedgerState | None,
    initial_cash_units: int,
    visible_capacity: int,
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> tuple[SpotLedgerState, str | None, int]:
    market = instrument.asset_class
    lot_key = "buy_lot_shares" if market == "cn_stock" else "buy_lot_units"
    lot_size = _positive_integer(rules.parameters, lot_key)
    settlement_days = _nonnegative_integer(rules.parameters, "settlement_days")
    if settlement_days not in {0, 1}:
        raise SimulationContractError("分钟现货 settlement_days 只支持 0/1")
    combined_rule = _combined_cash_rule(
        instrument=instrument,
        bar=bar,
        rules=rules,
        lot_size=lot_size,
        settlement_days=settlement_days,
    )
    cash_policy = CashMarketPolicy(
        market,
        combined_rule,
        lot_size,
        settlement_days,
        _nonnegative_integer(rules.parameters, "commission_ppm"),
        _nonnegative_integer(rules.parameters, "min_commission_units"),
        _nonnegative_integer(rules.parameters, "sell_tax_ppm"),
        _nonnegative_integer(rules.parameters, "transfer_fee_ppm"),
    )
    current = state or SpotLedgerState(
        ExecutionGroup(
            f"minute-default-{market}", market, "CNY", f"t{settlement_days}"
        ),
        initial_cash_units,
    )
    rule_binding = TradingRuleBinding(
        instrument_hash=instrument.instrument_hash,
        rule_snapshot_hash=rules.identity_hash,
        available_at=rules.available_at,
        corporate_action_snapshot_hash=typed_canonical_hash({
            "scope": "minute-corporate-action-binding-v1",
            "instrument_hash": instrument.instrument_hash,
            "target_hash": target.target_hash,
        }),
    )
    formal_intent = OrderIntent(
        instrument=instrument,
        side=side,
        quantity=quantity,
        position_effect="auto",
        decision_time=target.decision_time,
        order_time=bar.bar_start,
        portfolio_target_hash=target.target_hash,
        market_data_artifact_hash=bar.bar_hash,
        rule_binding=rule_binding,
        source_hashes=tuple(sorted({*target.source_hashes, bar.source_snapshot_hash})),
    )
    order = IntentToOrderPort(CASH_DAILY_BACKEND).to_order(
        formal_intent,
        ordinal=0,
        time_in_force="IOC",
    )
    price_units = bar.avg_units if bar.avg_units is not None else bar.close_units
    price_scale = _price_scale(rules.parameters)
    snapshot = OpeningSnapshot(
        instrument.instrument_hash,
        Price(price_units, price_scale, "CNY"),
        Price(
            _positive_integer(rules.parameters, "high_limit_units"),
            price_scale,
            "CNY",
        ),
        Price(
            _positive_integer(rules.parameters, "low_limit_units"),
            price_scale,
            "CNY",
        ),
        bool(rules.parameters.get("paused", False)),
        visible_capacity,
        bar.available_time,
    )
    execution = execute_cash_order(
        order,
        policy=cash_policy,
        snapshot=snapshot,
        state=current,
        execution_at=bar.available_time,
    )
    reason = execution.reason_code
    if execution.filled_quantity < quantity and reason in {None, "partially_filled"}:
        reason = "participation_cap"
    _append_formal_result(
        order=order,
        target=target,
        bar=bar,
        requested_quantity=quantity,
        filled_quantity=execution.filled_quantity,
        reason=reason,
        execution_price_units=price_units,
        price_scale=price_scale,
        contract_multiplier=1,
        fee_units=_cash_execution_fee(execution.events),
        realized_pnl_units=0,
        position_effect="auto",
        event_hashes=tuple(item.event_hash for item in execution.events),
        order_rows=order_rows,
        fill_rows=fill_rows,
        cost_rows=cost_rows,
    )
    return execution.state, reason, execution.filled_quantity


def _reconcile_futures_target(
    active: _ActiveTarget,
    bar: MinuteExecutionBar,
    *,
    resolver: MinuteRuleResolver,
    bundle: MinuteRuleSnapshotBundle,
    policy: IntradayExecutionPolicy,
    state: FuturesLedgerState | None,
    initial_cash_units: int,
    used_capacity: dict[str, int],
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> FuturesLedgerState | None:
    instrument = active.prepared.instrument
    current_quantity = _futures_quantity(state, instrument.instrument_hash)
    desired = active.prepared.desired_quantity
    if current_quantity == desired:
        return state
    rules = _resolve_rules(
        resolver,
        bundle=bundle,
        asset_class="cn_future",
        instrument_id=instrument.instrument_id,
        effective_on=bar.trading_date,
        as_of=bar.bar_start,
    )
    actual = rules.parameters.get("actual_contract_id")
    if actual != instrument.instrument_id:
        raise SimulationContractError("连续期货合约不能进入正式成交")
    _require_futures_lifecycle(rules.parameters, bar.trading_date)
    _require_visible_price_limits(
        rules.parameters,
        bar=bar,
        reference_key="reference_previous_settlement_units",
        rounding="floor_to_price_tick",
    )
    tick = _positive_integer(rules.parameters, "price_tick_units")
    price_units = bar.avg_units if bar.avg_units is not None else bar.close_units
    if price_units % tick:
        raise SimulationContractError("期货成交价不符合最小变动价位")
    operations = _futures_reconciliation(current_quantity, desired)
    current_state = state
    for side, quantity, position_effect in operations:
        capacity = _remaining_capacity(bar, policy, used_capacity, lot_size=1)
        if bar.open_interest is not None:
            capacity = min(capacity, max(0, bar.open_interest - used_capacity.get(bar.bar_hash, 0)))
        result_state, filled = _execute_futures_order(
            target=active.prepared.target,
            instrument=instrument,
            side=side,
            quantity=quantity,
            position_effect=position_effect,
            bar=bar,
            rules=rules,
            state=current_state,
            initial_cash_units=initial_cash_units,
            visible_capacity=capacity,
            order_rows=order_rows,
            fill_rows=fill_rows,
            cost_rows=cost_rows,
        )
        current_state = result_state
        if filled:
            used_capacity[bar.bar_hash] = used_capacity.get(bar.bar_hash, 0) + filled
        if filled < quantity:
            break
    return current_state


def _execute_futures_order(
    *,
    target: PortfolioTarget,
    instrument: InstrumentKey,
    side: str,
    quantity: int,
    position_effect: str,
    bar: MinuteExecutionBar,
    rules: _ResolvedRules,
    state: FuturesLedgerState | None,
    initial_cash_units: int,
    visible_capacity: int,
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> tuple[FuturesLedgerState, int]:
    multiplier = _positive_integer(rules.parameters, "contract_unit_kg")
    price_scale = _price_scale(rules.parameters)
    margin_policy_id = str(rules.parameters.get("margin_policy_id", "")).strip()
    if not margin_policy_id:
        raise SimulationContractError("期货分钟规则缺少 margin_policy_id")
    margin_ppm = _futures_margin_ppm(rules.parameters)
    current = state or FuturesLedgerState(
        ExecutionGroup(
            "minute-default-cn-futures",
            "cn_future",
            "CNY",
            "daily-settlement",
            margin_policy_id,
        ),
        initial_cash_units,
    )
    binding_by_id = {item.rule.rule_id: item.identity_hash for item in rules.bindings}
    rule_binding = TradingRuleBinding(
        instrument_hash=instrument.instrument_hash,
        rule_snapshot_hash=rules.identity_hash,
        available_at=rules.available_at,
        multiplier_rule_hash=binding_by_id["rule.cn_futures.contract_multiplier.v1"],
        fee_rule_hash=binding_by_id["rule.cn_futures.fee_schedule.v1"],
        margin_rule_hash=binding_by_id["rule.cn_futures.margin.v1"],
        settlement_rule_hash=binding_by_id["rule.cn_futures.session.v1"],
    )
    formal_intent = OrderIntent(
        instrument=instrument,
        side=side,
        quantity=quantity,
        position_effect=position_effect,
        decision_time=target.decision_time,
        order_time=bar.bar_start,
        portfolio_target_hash=target.target_hash,
        market_data_artifact_hash=bar.bar_hash,
        rule_binding=rule_binding,
        source_hashes=tuple(sorted({*target.source_hashes, bar.source_snapshot_hash})),
    )
    order = IntentToOrderPort(CN_FUTURES_DAILY_BACKEND).to_order(
        formal_intent,
        ordinal=0,
        time_in_force="IOC",
    )
    filled = min(quantity, visible_capacity)
    price_units = bar.avg_units if bar.avg_units is not None else bar.close_units
    reason: str | None = None
    events: list[FinancialEvent] = []
    realized_pnl = 0
    if filled <= 0:
        filled = 0
        reason = "capacity_exceeded"
    else:
        old_position = next((
            item for item in current.positions
            if item.instrument_hash == instrument.instrument_hash
        ), FuturesPosition(instrument.instrument_hash, 0, price_units))
        if position_effect == "close":
            expected_sign = 1 if side == "sell" else -1
            if old_position.contracts * expected_sign <= 0 or filled > abs(old_position.contracts):
                raise SimulationContractError("期货平仓意图与当前持仓不一致")
            realized_pnl = (
                (price_units - old_position.settlement_price_units)
                * filled
                * multiplier
                * expected_sign
            )
        direction = 1 if side == "buy" else -1
        new_contracts = old_position.contracts + direction * filled
        old_margin = _required_futures_margin(
            price_units=old_position.settlement_price_units,
            multiplier=multiplier,
            contracts=abs(old_position.contracts),
            margin_ppm=margin_ppm,
        )
        new_margin = _required_futures_margin(
            price_units=price_units,
            multiplier=multiplier,
            contracts=abs(new_contracts),
            margin_ppm=margin_ppm,
        )
        required_margin = max(0, current.margin_units - old_margin + new_margin)
        fee = _futures_fee_units(
            rules.parameters,
            position_effect=position_effect,
            notional_units=price_units * filled * multiplier,
        )
        if required_margin > current.equity_units + realized_pnl - fee:
            filled = 0
            realized_pnl = 0
            reason = "insufficient_margin"
        else:
            if required_margin < current.margin_units:
                release = FinancialEvent(
                    f"{order.order_id}:margin-release",
                    "mark_to_market",
                    bar.available_time,
                    bar.trading_date.isoformat(),
                    current.group.group_id,
                    rules.identity_hash,
                    (("pnl_units", 0), ("required_margin_units", required_margin)),
                    order.order_id,
                )
                current = reduce_futures(current, release)
                events.append(release)
            fill_event = FinancialEvent(
                f"{order.order_id}:fill",
                "fill",
                bar.available_time,
                bar.trading_date.isoformat(),
                current.group.group_id,
                rules.identity_hash,
                tuple(sorted({
                    "contracts_delta": direction * filled,
                    "fee_units": fee,
                    "instrument_hash": instrument.instrument_hash,
                    "settlement_price_units": price_units,
                }.items())),
                order.order_id,
            )
            current = reduce_futures(current, fill_event)
            events.append(fill_event)
            margin_event = FinancialEvent(
                f"{order.order_id}:valuation",
                "mark_to_market",
                bar.available_time,
                bar.trading_date.isoformat(),
                current.group.group_id,
                rules.identity_hash,
                tuple(sorted({
                    "pnl_units": realized_pnl,
                    "required_margin_units": required_margin,
                }.items())),
                fill_event.event_id,
            )
            current = reduce_futures(current, margin_event)
            events.append(margin_event)
            if filled < quantity:
                reason = "participation_cap"
    fee_units = (
        0
        if filled == 0
        else _futures_fee_units(
            rules.parameters,
            position_effect=position_effect,
            notional_units=price_units * filled * multiplier,
        )
    )
    _append_formal_result(
        order=order,
        target=target,
        bar=bar,
        requested_quantity=quantity,
        filled_quantity=filled,
        reason=reason,
        execution_price_units=price_units,
        price_scale=price_scale,
        contract_multiplier=multiplier,
        fee_units=fee_units,
        realized_pnl_units=realized_pnl,
        position_effect=position_effect,
        event_hashes=tuple(item.event_hash for item in events),
        order_rows=order_rows,
        fill_rows=fill_rows,
        cost_rows=cost_rows,
    )
    return current, filled


def _append_formal_result(
    *,
    order: Order,
    target: PortfolioTarget,
    bar: MinuteExecutionBar,
    requested_quantity: int,
    filled_quantity: int,
    reason: str | None,
    execution_price_units: int,
    price_scale: int,
    contract_multiplier: int,
    fee_units: int,
    realized_pnl_units: int,
    position_effect: str,
    event_hashes: tuple[str, ...],
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
) -> None:
    status = (
        "filled"
        if filled_quantity == requested_quantity
        else "rejected" if filled_quantity == 0 else "partially_filled"
    )
    terminal_reason = None if status == "filled" else (reason or "formal_simulation_unfilled")
    terminal_order = replace(
        order,
        status=("rejected" if status == "rejected" else status),
        filled_quantity=filled_quantity,
        rejection_code=terminal_reason if status == "rejected" else None,
    )
    order_rows.append({
        "portfolio_id": "default",
        "order_id": order.order_id,
        "session": bar.trading_date,
        "instrument_id": order.instrument.instrument_id,
        "instrument_hash": order.instrument.instrument_hash,
        "asset_class": order.instrument.asset_class,
        "side": order.side,
        "requested_quantity": requested_quantity,
        "filled_quantity": filled_quantity,
        "status": status,
        "terminal_reason": terminal_reason,
        "decision_time": target.decision_time,
        "submitted_at": order.submitted_at,
        "source_order_hash": terminal_order.order_hash,
    })
    if filled_quantity == 0:
        return
    fill_id = typed_canonical_hash({
        "order_id": order.order_id,
        "bar_hash": bar.bar_hash,
        "event_hashes": list(event_hashes),
    })
    source_fill_hash = typed_canonical_hash({
        "fill_id": fill_id,
        "order_hash": terminal_order.order_hash,
        "event_hashes": list(event_hashes),
    })
    notional_units = (
        execution_price_units * filled_quantity * contract_multiplier
    )
    fill_rows.append({
        "portfolio_id": "default",
        "fill_id": fill_id,
        "order_id": order.order_id,
        "session": bar.trading_date,
        "instrument_id": order.instrument.instrument_id,
        "instrument_hash": order.instrument.instrument_hash,
        "asset_class": order.instrument.asset_class,
        "side": order.side,
        "quantity": filled_quantity,
        "fill_time": bar.available_time,
        "execution_price_units": execution_price_units,
        "price_scale": price_scale,
        "contract_multiplier": contract_multiplier,
        "notional_units": notional_units,
        "fee_units": fee_units,
        "realized_pnl_units": realized_pnl_units,
        "position_effect": position_effect,
        "source_fill_hash": source_fill_hash,
    })
    cost_rows.append({
        "portfolio_id": "default",
        "cost_id": typed_canonical_hash({"fill_id": fill_id, "cost": "transaction"}),
        "fill_id": fill_id,
        "session": bar.trading_date,
        "cost_type": "transaction_fee",
        "amount_units": fee_units,
        "currency": "CNY",
        "source_cost_hash": source_fill_hash,
    })


def _build_session_rows(
    *,
    asset_class: str,
    instruments: Mapping[str, InstrumentKey],
    state: SpotLedgerState | FuturesLedgerState,
    session: date,
    valuation_time: datetime,
    prices: Mapping[str, int],
    non_trade_events: Iterable[str],
    order_rows: list[dict[str, object]],
    fill_rows: list[dict[str, object]],
    cost_rows: list[dict[str, object]],
    initial_cash_units: int,
    previous_cash: int,
    previous_quantities: Mapping[str, int],
) -> tuple[dict[str, list[dict[str, object]]], int, dict[str, int]]:
    """把一个已结束交易日转换为六表行，并返回下一日的比较基线。"""

    instrument_by_hash = {item.instrument_hash: item for item in instruments.values()}
    fill_changes: dict[str, int] = {}
    fill_presence: set[str] = set()
    trade_cash_change = 0
    for row in fill_rows:
        if row["session"] != session:
            raise SimulationContractError("分钟 session 输出混入其他交易日 fill")
        instrument_hash = str(row["instrument_hash"])
        fill_presence.add(instrument_hash)
        signed = int(row["quantity"]) if row["side"] == "buy" else -int(row["quantity"])
        fill_changes[instrument_hash] = fill_changes.get(instrument_hash, 0) + signed
        if asset_class == "cn_future":
            change = int(row["realized_pnl_units"]) - int(row["fee_units"])
        else:
            notional = int(row["notional_units"])
            change = (-notional if row["side"] == "buy" else notional) - int(row["fee_units"])
        trade_cash_change += change

    position_rows: list[dict[str, object]] = []
    state_hash = (
        _futures_state_hash(state)
        if isinstance(state, FuturesLedgerState)
        else state.state_hash
    )
    snapshot_id = typed_canonical_hash({
        "portfolio_id": "default",
        "session": session.isoformat(),
        "valuation_time": valuation_time.isoformat(),
        "state_hash": state_hash,
    })
    non_trade_hash = typed_canonical_hash(sorted(non_trade_events))
    current_quantities: dict[str, int] = {}
    market_value = 0
    if isinstance(state, SpotLedgerState):
        if state.frozen_cash_units != 0:
            raise SimulationContractError("分钟终态仍有冻结现金，不能形成日终快照")
        for lot in state.positions:
            quantity = lot.sellable + lot.unsettled + lot.frozen
            current_quantities[lot.instrument_hash] = quantity
            trade_change = fill_changes.get(lot.instrument_hash, 0)
            non_trade_change = (
                quantity
                - previous_quantities.get(lot.instrument_hash, 0)
                - trade_change
            )
            if (
                quantity != 0
                or trade_change != 0
                or non_trade_change != 0
                or lot.instrument_hash in fill_presence
            ):
                price_units = prices.get(lot.instrument_hash)
                if price_units is None:
                    raise SimulationContractError("分钟估值缺少持仓标的当日完成 bar")
                value = quantity * price_units
                market_value += value
                instrument = instrument_by_hash[lot.instrument_hash]
                position_rows.append({
                    "portfolio_id": "default", "snapshot_id": snapshot_id,
                    "session": session, "valuation_time": valuation_time,
                    "instrument_id": instrument.instrument_id,
                    "instrument_hash": lot.instrument_hash,
                    "asset_class": asset_class, "quantity": quantity,
                    "sellable_quantity": lot.sellable,
                    "unsettled_quantity": lot.unsettled,
                    "frozen_quantity": lot.frozen,
                    "market_value_units": value,
                    "trade_quantity_change": trade_change,
                    "non_trade_quantity_change": non_trade_change,
                    "source_state_hash": state_hash,
                    "non_trade_source_hash": non_trade_hash,
                })
        total_cash = state.total_cash_units
        available_cash = state.available_cash_units
        receivable_cash = (
            state.unsettled_cash_units
            + sum(item.cash_units for item in state.cash_receivables)
        )
        margin_units = 0
        valuation_model = "cash_plus_position_market_value"
        nav_units = total_cash + market_value
    else:
        for position in state.positions:
            quantity = position.contracts
            current_quantities[position.instrument_hash] = quantity
            trade_change = fill_changes.get(position.instrument_hash, 0)
            non_trade_change = (
                quantity
                - previous_quantities.get(position.instrument_hash, 0)
                - trade_change
            )
            if (
                quantity != 0
                or trade_change != 0
                or non_trade_change != 0
                or position.instrument_hash in fill_presence
            ):
                instrument = instrument_by_hash[position.instrument_hash]
                position_rows.append({
                    "portfolio_id": "default", "snapshot_id": snapshot_id,
                    "session": session, "valuation_time": valuation_time,
                    "instrument_id": instrument.instrument_id,
                    "instrument_hash": position.instrument_hash,
                    "asset_class": asset_class, "quantity": quantity,
                    "sellable_quantity": 0, "unsettled_quantity": 0,
                    "frozen_quantity": 0, "market_value_units": 0,
                    "trade_quantity_change": trade_change,
                    "non_trade_quantity_change": non_trade_change,
                    "source_state_hash": state_hash,
                    "non_trade_source_hash": non_trade_hash,
                })
        total_cash = state.equity_units
        available_cash = state.free_equity_units
        receivable_cash = 0
        margin_units = state.margin_units
        valuation_model = "futures_settlement_equity"
        nav_units = total_cash
    non_trade_cash_change = total_cash - previous_cash - trade_cash_change
    if non_trade_cash_change != 0 and non_trade_hash == _EMPTY_EVENT_HASH:
        # 期货平仓盈亏已经属于 fill 的 realized_pnl，不应落到非交易残差。
        raise SimulationContractError("分钟现金变化缺少正式事件来源")
    cash_rows = [{
        "portfolio_id": "default", "snapshot_id": snapshot_id,
        "session": session, "valuation_time": valuation_time, "currency": "CNY",
        "total_cash_units": total_cash,
        "available_cash_units": available_cash,
        "receivable_cash_units": receivable_cash,
        "margin_units": margin_units,
        "trade_cash_change_units": trade_cash_change,
        "non_trade_cash_change_units": non_trade_cash_change,
        "opening_cash_units": initial_cash_units,
        "source_state_hash": state_hash,
        "non_trade_source_hash": non_trade_hash,
    }]
    valuation_rows = [{
        "portfolio_id": "default", "snapshot_id": snapshot_id,
        "session": session, "valuation_time": valuation_time,
        "nav_units": nav_units, "currency": "CNY",
        "valuation_model": valuation_model,
        "source_state_hash": state_hash,
    }]

    # 稳定排序保留同一 bar 内“先平后开”的正式执行顺序。
    ordered_orders = sorted(order_rows, key=lambda row: row["submitted_at"])
    ordered_fills = sorted(fill_rows, key=lambda row: row["fill_time"])
    ordered_costs = sorted(cost_rows, key=lambda row: row["session"])
    position_rows.sort(key=lambda row: (row["valuation_time"], row["instrument_hash"]))
    cash_rows.sort(key=lambda row: (row["valuation_time"], row["snapshot_id"]))
    valuation_rows.sort(key=lambda row: (row["valuation_time"], row["snapshot_id"]))
    return ({
        "orders": ordered_orders,
        "fills": ordered_fills,
        "positions": position_rows,
        "cash": cash_rows,
        "costs": ordered_costs,
        "valuations": valuation_rows,
    }, total_cash, current_quantities)


def _resolve_rules(
    resolver: MinuteRuleResolver,
    *,
    bundle: MinuteRuleSnapshotBundle,
    asset_class: str,
    instrument_id: str,
    effective_on: date,
    as_of: datetime,
    required_rule_ids: tuple[str, ...] | None = None,
) -> _ResolvedRules:
    bindings = []
    selected_rule_ids = (
        _ORDER_REQUIRED_RULES[asset_class]
        if required_rule_ids is None
        else required_rule_ids
    )
    for rule_id in selected_rule_ids:
        try:
            bindings.append(resolver.resolve(
                rule_id=rule_id,
                instrument_id=instrument_id,
                effective_on=effective_on,
                as_of=as_of,
            ))
        except MinuteRuleSnapshotError as exc:
            raise SimulationContractError(
                f"分钟正式仿真规则不完整: {rule_id}: {exc}"
            ) from exc
    result = tuple(bindings)
    identity_hash = typed_canonical_hash({
        "bundle_hash": bundle.bundle_hash,
        "bindings": [item.identity_hash for item in result],
    })
    available_at = max(
        item.rule.available_at for item in result if item.rule.available_at is not None
    )
    return _ResolvedRules(result, _parameters(result), identity_hash, available_at)


def _combined_cash_rule(
    *,
    instrument: InstrumentKey,
    bar: MinuteExecutionBar,
    rules: _ResolvedRules,
    lot_size: int,
    settlement_days: int,
) -> MarketRuleSnapshot:
    return MarketRuleSnapshot(
        f"minute-{instrument.asset_class}-derived-v1",
        1,
        instrument.asset_class,
        "stock" if instrument.asset_class == "cn_stock" else "etf",
        bar.trading_date,
        bar.trading_date,
        rules.available_at,
        "minute-rule-bundle",
        "research_pipeline/docs/minute_simulation.md",
        tuple(sorted({
            "commission_ppm": _nonnegative_integer(rules.parameters, "commission_ppm"),
            "cost_model_scope": str(rules.parameters.get("cost_model_scope", "market_rule")),
            "lot_size": lot_size,
            "min_commission_units": _nonnegative_integer(
                rules.parameters, "min_commission_units"
            ),
            "sell_tax_ppm": _nonnegative_integer(rules.parameters, "sell_tax_ppm"),
            "settlement_days": settlement_days,
            "price_scale": _price_scale(rules.parameters),
            "transfer_fee_ppm": _nonnegative_integer(
                rules.parameters, "transfer_fee_ppm"
            ),
            "source_binding_hash": rules.identity_hash,
            "source_instrument_id": instrument.instrument_id,
        }.items())),
    )


def _parameters(bindings: tuple[MinuteRuleBinding, ...]) -> dict[str, object]:
    result: dict[str, object] = {}
    for binding in bindings:
        for key, value in binding.rule.parameters:
            if key in result and result[key] != value:
                raise SimulationContractError(f"分钟规则参数冲突: {key}")
            result[key] = value
    return result


def _remaining_capacity(
    bar: MinuteExecutionBar,
    policy: IntradayExecutionPolicy,
    used_capacity: Mapping[str, int],
    *,
    lot_size: int,
) -> int:
    total = bar.volume * policy.participation_ppm // 1_000_000
    remaining = max(0, total - used_capacity.get(bar.bar_hash, 0))
    return remaining // lot_size * lot_size


def _spot_quantities(
    state: SpotLedgerState | None,
    instrument_hash: str,
) -> tuple[int, int]:
    if state is None:
        return 0, 0
    lot = next((
        item for item in state.positions if item.instrument_hash == instrument_hash
    ), None)
    if lot is None:
        return 0, 0
    return lot.sellable + lot.unsettled + lot.frozen, lot.sellable


def _futures_quantity(
    state: FuturesLedgerState | None,
    instrument_hash: str,
) -> int:
    if state is None:
        return 0
    return next((
        item.contracts
        for item in state.positions
        if item.instrument_hash == instrument_hash
    ), 0)


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


def _cash_execution_fee(events: tuple[FinancialEvent, ...]) -> int:
    fill = next((item for item in events if item.kind == "fill"), None)
    return 0 if fill is None else int(fill.values()["fee_units"])


def _require_cash_lifecycle(
    parameters: Mapping[str, object],
    *,
    asset_class: str,
    trading_date: date,
) -> None:
    """ETF 的产品分类和上市区间属于成交资格，不只用于留档。"""

    if asset_class != "cn_etf":
        return
    if parameters.get("product_class") != "equity_etf":
        raise SimulationContractError("ETF 分钟规则未确认股票型 ETF 品类")
    listed = _date_parameter(parameters, "listed_date")
    delisted = _date_parameter(parameters, "delisted_date")
    if delisted < listed:
        raise SimulationContractError("ETF 生命周期结束日早于上市日")
    if not listed <= trading_date <= delisted:
        raise SimulationContractError("ETF 在分钟成交日不处于可交易生命周期")


def _require_visible_price_limits(
    parameters: Mapping[str, object],
    *,
    bar: MinuteExecutionBar,
    reference_key: str = "reference_previous_close_units",
    rounding: str = "half_up_to_quote_unit",
) -> None:
    reference = _positive_integer(parameters, reference_key)
    ratio_ppm = _positive_integer(parameters, "price_limit_ratio_ppm")
    if ratio_ppm > 1_000_000:
        raise SimulationContractError("分钟涨跌停比例超出支持范围")
    price_scale = _price_scale(parameters)
    raw_available_at = parameters.get("reference_price_available_at")
    if not isinstance(raw_available_at, str):
        raise SimulationContractError("分钟前收参考值缺少可见时间")
    try:
        available_at = datetime.fromisoformat(raw_available_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SimulationContractError("分钟前收参考值可见时间无效") from exc
    require_aware_datetime(available_at, "reference_price_available_at")
    if available_at > bar.bar_start:
        raise SimulationContractError("分钟前收参考值在订单提交时尚不可见")
    ratio = Decimal(ratio_ppm) / Decimal(1_000_000)
    if rounding == "half_up_to_quote_unit":
        expected_high = int(
            (Decimal(reference) * (Decimal(1) + ratio)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
        expected_low = int(
            (Decimal(reference) * (Decimal(1) - ratio)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
    elif rounding == "floor_to_price_tick":
        tick = _positive_integer(parameters, "price_tick_units")
        expected_high = int(Decimal(reference) * (Decimal(1) + ratio)) // tick * tick
        expected_low = int(Decimal(reference) * (Decimal(1) - ratio)) // tick * tick
    else:
        raise SimulationContractError("分钟涨跌停舍位规则不受支持")
    declared_high = _positive_integer(parameters, "high_limit_units")
    declared_low = _positive_integer(parameters, "low_limit_units")
    if (declared_high, declared_low) != (expected_high, expected_low):
        raise SimulationContractError("分钟涨跌停值与前收、比例和报价精度不一致")
    execution_price = bar.avg_units if bar.avg_units is not None else bar.close_units
    if not declared_low <= execution_price <= declared_high:
        raise SimulationContractError("分钟执行价格超出当时有效的涨跌停范围")
    if not 0 <= price_scale <= 9:
        raise SimulationContractError("分钟价格精度超出支持范围")


def _settle_futures_session(
    *,
    state: FuturesLedgerState,
    instruments: Mapping[str, InstrumentKey],
    bars: tuple[MinuteExecutionBar, ...],
    trading_date: date,
    resolver: MinuteRuleResolver,
    bundle: MinuteRuleSnapshotBundle,
) -> tuple[
    FuturesLedgerState,
    datetime | None,
    tuple[str, ...],
    tuple[dict[str, object], ...],
]:
    """收盘后逐合约盯市，并让每个事件携带全账户聚合保证金。"""

    instrument_by_hash = {
        item.instrument_hash: item for item in instruments.values()
    }
    selected_ids = set()
    for position in state.positions:
        if position.contracts == 0:
            continue
        instrument = instrument_by_hash.get(position.instrument_hash)
        if instrument is None:
            raise SimulationContractError("期货持仓缺少 InstrumentKey，不能完成结算")
        selected_ids.add(instrument.instrument_id)
    if not selected_ids:
        return state, None, (), ()

    facts = []
    for instrument_id in sorted(selected_ids):
        instrument = instruments.get(instrument_id)
        if instrument is None:
            raise SimulationContractError("期货结算标的缺少 InstrumentKey")
        settlement_time = _futures_settlement_event_time(
            bundle,
            instrument_id=instrument_id,
            trading_date=trading_date,
        )
        rules = _resolve_rules(
            resolver,
            bundle=bundle,
            asset_class="cn_future",
            instrument_id=instrument_id,
            effective_on=trading_date,
            as_of=settlement_time,
            required_rule_ids=_FUTURES_SETTLEMENT_RULES,
        )
        _require_futures_session_close_trigger(
            bundle=bundle,
            rules=rules,
            instrument=instrument,
            trading_date=trading_date,
            settlement_time=settlement_time,
            bars=bars,
        )
        settlement_price = _positive_integer(
            rules.parameters, "settlement_price_units"
        )
        if rules.parameters.get("settlement_availability_semantics") != (
            "session_close_event_not_supplier_timestamp"
        ):
            raise SimulationContractError("期货结算价必须声明收盘事件可见语义")
        multiplier = _positive_integer(rules.parameters, "contract_unit_kg")
        margin_ppm = _futures_margin_ppm(rules.parameters)
        position = next((
            item for item in state.positions
            if item.instrument_hash == instrument.instrument_hash
        ), FuturesPosition(instrument.instrument_hash, 0, settlement_price))
        pnl_units = (
            (settlement_price - position.settlement_price_units)
            * position.contracts
            * multiplier
        )
        required_margin = _required_futures_margin(
            price_units=settlement_price,
            multiplier=multiplier,
            contracts=abs(position.contracts),
            margin_ppm=margin_ppm,
        )
        facts.append((
            settlement_time,
            instrument,
            rules,
            settlement_price,
            pnl_units,
            required_margin,
        ))

    settlement_times = {item[0] for item in facts}
    if len(settlement_times) != 1:
        raise SimulationContractError(
            "同一账户多合约分钟结算必须使用同一收盘事件时点"
        )
    aggregate_margin = sum(item[5] for item in facts)
    settled = state
    event_hashes = []
    event_rows = []
    for (
        settlement_time,
        instrument,
        rules,
        settlement_price,
        pnl_units,
        _required_margin,
    ) in sorted(facts, key=lambda item: (item[0], item[1].instrument_id)):
        event = FinancialEvent(
            f"minute-settlement:{instrument.instrument_id}:{trading_date.isoformat()}",
            "mark_to_market",
            settlement_time,
            trading_date.isoformat(),
            settled.group.group_id,
            rules.identity_hash,
            tuple(sorted({
                "pnl_units": pnl_units,
                "required_margin_units": aggregate_margin,
            }.items())),
        )
        settled = reduce_futures(settled, event)
        settled = replace(
            settled,
            positions=tuple(
                FuturesPosition(item.instrument_hash, item.contracts, settlement_price)
                if item.instrument_hash == instrument.instrument_hash
                else item
                for item in settled.positions
            ),
        )
        event_hashes.append(event.event_hash)
        event_rows.append({
            "instrument_id": instrument.instrument_id,
            "instrument_hash": instrument.instrument_hash,
            "settlement_time": settlement_time.isoformat(),
            "settlement_price_units": settlement_price,
            "price_scale": _positive_integer(rules.parameters, "price_scale"),
            "position_contracts_before": next(
                item.contracts
                for item in state.positions
                if item.instrument_hash == instrument.instrument_hash
            ),
            "previous_settlement_price_units": next(
                item.settlement_price_units
                for item in state.positions
                if item.instrument_hash == instrument.instrument_hash
            ),
            "contract_multiplier": _positive_integer(
                rules.parameters, "contract_unit_kg"
            ),
            "speculative_margin_ppm": _futures_margin_ppm(rules.parameters),
            "pnl_units": pnl_units,
            "required_margin_units": _required_margin,
            "rule_hash": rules.identity_hash,
            "rule_snapshot_hashes": [
                item.rule.snapshot_hash for item in rules.bindings
            ],
            "aggregate_required_margin_units": aggregate_margin,
            "event": event.to_dict(),
            "event_hash": event.event_hash,
        })
    return (
        settled,
        max(item[0] for item in facts),
        tuple(event_hashes),
        tuple(event_rows),
    )


def _require_futures_session_close_trigger(
    *,
    bundle: MinuteRuleSnapshotBundle,
    rules: _ResolvedRules,
    instrument: InstrumentKey,
    trading_date: date,
    settlement_time: datetime,
    bars: tuple[MinuteExecutionBar, ...],
) -> None:
    """只有完整收盘 bar 才能触发当日结算，缺尾段时失败关闭。"""

    metadata = next((
        item for item in bundle.instruments
        if item.instrument_id == instrument.instrument_id
    ), None)
    if metadata is None:
        raise SimulationContractError("期货结算标的缺少 session classification")
    policy_id = str(rules.parameters.get("session_policy_id", "")).strip()
    policy_revision = _positive_integer(rules.parameters, "session_policy_revision")
    session = SessionCalendarResolver(
        load_session_policy_bundle()
    ).resolve_trading_date(
        metadata,
        trading_date,
        policy_revision=policy_revision,
    )
    if session.calendar_policy_id != policy_id:
        raise SimulationContractError("期货结算 session policy 身份漂移")
    close_time = max(
        item.ends_at for item in session.segments if item.bar_eligible is True
    )
    if settlement_time < close_time:
        raise SimulationContractError("期货结算事件早于批准 session 收盘")
    close_bars = tuple(
        item for item in bars
        if item.instrument_id == instrument.instrument_id
        and item.trading_date == trading_date
        and item.session_id == session.session_id
        and item.bar_end == close_time
        and item.completed
        and item.quality_status == "pass"
    )
    if len(close_bars) != 1:
        raise SimulationContractError("期货结算缺少 completed/pass 的 session 收盘 bar")


def _futures_settlement_event_time(
    bundle: MinuteRuleSnapshotBundle,
    *,
    instrument_id: str,
    trading_date: date,
) -> datetime:
    candidates = tuple(
        item for item in bundle.rules
        if item.instrument_id == instrument_id
        and item.rule_id == "rule.cn_futures.settlement.v1"
        and item.effective_from <= trading_date <= item.effective_to
    )
    if (
        len(candidates) != 1
        or candidates[0].status != "supported"
        or candidates[0].available_at is None
    ):
        raise SimulationContractError("期货收盘结算规则缺失、重叠或不支持")
    return candidates[0].available_at


def _futures_margin_ppm(parameters: Mapping[str, object]) -> int:
    if parameters.get("margin_account_role") != "speculative":
        raise SimulationContractError("期货分钟仿真只消费明确的投机保证金率")
    rate = _positive_integer(parameters, "speculative_initial_margin_ppm")
    if rate > 1_000_000:
        raise SimulationContractError("期货保证金率超出支持范围")
    _positive_integer(parameters, "hedge_margin_ppm")
    return rate


def _required_futures_margin(
    *,
    price_units: int,
    multiplier: int,
    contracts: int,
    margin_ppm: int,
) -> int:
    return _ceil_ratio(price_units * multiplier * contracts * margin_ppm, 1_000_000)


def _futures_fee_units(
    parameters: Mapping[str, object],
    *,
    position_effect: str,
    notional_units: int,
) -> int:
    if parameters.get("fee_unit") != "notional_permyriad":
        raise SimulationContractError("期货分钟手续费只支持成交额万分比")
    if parameters.get("opening_charge_null_semantics") != (
        "use_common_clearance_charge"
    ):
        raise SimulationContractError("期货开仓空费率缺少已批准解释")
    close_rate = _nonnegative_integer(parameters, "close_fee_ppm")
    close_today_rate = _nonnegative_integer(parameters, "close_today_fee_ppm")
    if close_rate != close_today_rate:
        raise SimulationContractError(
            "分钟期货首版不能区分平今持仓，close_today_fee_ppm 必须等于 close_fee_ppm"
        )
    key = "open_fee_ppm" if position_effect == "open" else "close_fee_ppm"
    rate = _nonnegative_integer(parameters, key)
    return _ceil_ratio(notional_units * rate, 1_000_000)


def _ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def _require_futures_lifecycle(parameters: Mapping[str, object], trading_date: date) -> None:
    listed_value = parameters.get("listed_date")
    if listed_value is not None:
        if not isinstance(listed_value, str):
            raise SimulationContractError("期货 listed_date 无效")
        try:
            listed_date = date.fromisoformat(listed_value)
        except ValueError as exc:
            raise SimulationContractError("期货 listed_date 无效") from exc
        if trading_date < listed_date:
            raise SimulationContractError("期货合约尚未上市")
    value = parameters.get("last_trade_date")
    if not isinstance(value, str):
        raise SimulationContractError("期货分钟规则缺少明确 last_trade_date")
    try:
        last_trade_date = date.fromisoformat(value)
    except ValueError as exc:
        raise SimulationContractError("期货 last_trade_date 无效") from exc
    if trading_date > last_trade_date:
        raise SimulationContractError("期货合约已过最后交易日")


def _positive_integer(
    parameters: Mapping[str, object],
    key: str,
    *,
    default: int | None = None,
) -> int:
    value = parameters.get(key, default)
    if type(value) is not int or value <= 0:
        raise SimulationContractError(f"分钟规则缺少正整数参数: {key}")
    return value




def _price_scale(parameters: Mapping[str, object]) -> int:
    value = parameters.get("price_scale")
    if type(value) is not int or not 0 <= value <= 9:
        raise SimulationContractError("分钟规则缺少有效 price_scale")
    return value


def _date_parameter(parameters: Mapping[str, object], key: str) -> date:
    value = parameters.get(key)
    if not isinstance(value, str):
        raise SimulationContractError(f"分钟规则缺少日期参数: {key}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SimulationContractError(f"分钟规则日期参数无效: {key}") from exc


def _nonnegative_integer(parameters: Mapping[str, object], key: str) -> int:
    value = parameters.get(key)
    if type(value) is not int or value < 0:
        raise SimulationContractError(f"分钟规则缺少非负整数参数: {key}")
    return value


def _futures_state_hash(state: FuturesLedgerState) -> str:
    return typed_canonical_hash({
        "group": state.group.__dict__,
        "equity_units": state.equity_units,
        "margin_units": state.margin_units,
        "realized_pnl_units": state.realized_pnl_units,
        "positions": [item.__dict__ for item in state.positions],
        "applied_event_ids": list(state.applied_event_ids),
    })


def _hash(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise SimulationContractError(f"{field} 必须是 sha256")
    return value


__all__ = [
    "INTRADAY_EXECUTION_POLICY_VERSION",
    "IntradayExecutionPolicy",
    "MinuteEventSimulationStateMachine",
    "MinuteExecutionBar",
    "MinuteSimulationSessionOutput",
    "PreparedMinuteTarget",
    "run_minute_event_simulation",
]
