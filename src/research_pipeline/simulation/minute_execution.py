"""由数量目标、完成分钟 bar 与 PIT 规则快照驱动的正式仿真。"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
import hashlib

from research_pipeline.domain import (
    CorporateAction,
    Price,
    load_session_policy_bundle,
    InstrumentKey,
    MinuteRuleResolver,
    MinuteRuleSnapshotBundle,
    PortfolioTarget,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.domain.corporate_actions import resolve_corporate_actions

from .minute_rules import (
    _ORDER_REQUIRED_RULES,
)
from .settlement import _settle_futures_session
from .result_collector import _build_session_rows, collect_execution_support
from .cash_market import apply_cash_corporate_actions, settle_cash_daily_open
from .corporate_actions import (
    CorporateActionRecordPosition,
    capture_corporate_action_record_positions,
    compile_corporate_action_share_arrival,
    corporate_action_snapshot_hash,
)
from .events import FinancialEvent
from .minute_orders import _reconcile_spot_target, _reconcile_futures_target

from .execution_market import (
    INTRADAY_EXECUTION_POLICY_VERSION, IntradayExecutionPolicy, MinuteExecutionBar,
    _require_minute_asset,
)
from .ledger import (
    ExecutionGroup,
    FuturesLedgerState,
    SpotLedgerState,
    reduce_spot,
)
from .orders import SimulationContractError
from .engine import ExecutionEngine
from .explicit_orders import ExplicitOrderExecution
from .minute_rules import (
    resolve_minute_cash_policy, resolve_minute_execution_rules, minute_cash_bar_suspended,
)
from .target_execution import PreparedMinuteTarget, TargetExecution, _ActiveTarget
from .result_contract import (
    SimulationResultContract,
    SimulationResultSemantics,
    SIMULATION_RESULT_LIFECYCLE_SEMANTICS_VERSION,
    build_simulation_result_contract,
    canonical_simulation_table,
)


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
    order_lifecycle: tuple[dict[str, object], ...] = ()

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
        decision_bar_lookup: Callable[[PreparedMinuteTarget], MinuteExecutionBar | None] | None = None,
        corporate_actions: tuple[CorporateAction, ...] = (),
        order_commands: tuple = (),
    ) -> None:
        self.policy = policy or IntradayExecutionPolicy()
        if type(initial_cash_units) is not int or initial_cash_units <= 0:
            raise SimulationContractError("分钟仿真初始资金必须是正整数")
        self.rule_bundle = rule_bundle
        self.initial_cash_units = initial_cash_units
        self.resolver = MinuteRuleResolver(rule_bundle)
        self.engine = ExecutionEngine()
        self.clock = self.engine.clock
        self.explicit_execution = (ExplicitOrderExecution(order_commands, self.engine.broker, initial_cash_units)
                                   if order_commands else None)
        self.targets = TargetExecution(
            prepared_targets, self.clock, decision_bar_lookup=decision_bar_lookup,
            allow_empty=self.explicit_execution is not None,
        )
        if self.explicit_execution is not None and self.targets._target_count:
            raise SimulationContractError("目标与显式订单不能同时控制同一仿真账户")
        self._bar_digest = hashlib.sha256(b"minute-execution-bar-stream-v1\0")
        self._bar_count = 0
        self._spot_state: SpotLedgerState | None = None
        self._futures_state: FuturesLedgerState | None = None
        self._current_session: date | None = None
        self._session_bars: list[MinuteExecutionBar] = []
        self._session_orders: list[dict[str, object]] = []
        self._session_fills: list[dict[str, object]] = []
        self._session_costs: list[dict[str, object]] = []
        self._session_prices: dict[str, int] = {}
        self._session_price_keys: dict[str, tuple[datetime, int]] = {}
        self._session_valuation_time: datetime | None = None
        self._session_non_trade_events: list[str] = []
        self._session_decision_benchmarks: list[dict[str, object]] = []
        self._session_execution_observations: list[dict[str, object]] = []
        self._session_settlement_events: list[dict[str, object]] = []
        self._previous_cash = initial_cash_units
        self._previous_quantities: dict[str, int] = {}
        action_revisions: dict[tuple[str, int], CorporateAction] = {}
        for action in corporate_actions:
            if not isinstance(action, CorporateAction) or action.contract_version != 2:
                raise SimulationContractError("分钟公司行动必须使用完整 v2 合同")
            if action.kind in {"rights", "code_change"}:
                raise SimulationContractError("分钟基础公司行动不支持配股或换股账户")
            key = (action.action_id, action.revision)
            previous = action_revisions.get(key)
            if previous is not None and previous != action:
                raise SimulationContractError("同一公司行动修订内容冲突")
            action_revisions[key] = action
        self._corporate_actions = tuple(action_revisions[key] for key in sorted(action_revisions))
        self._corporate_action_groups: dict[str, list[CorporateAction]] = {}
        for action in self._corporate_actions:
            self._corporate_action_groups.setdefault(action.action_id, []).append(action)
        if self._corporate_actions and self.asset_class not in {"cn_stock", "cn_etf"}:
            raise SimulationContractError("分钟公司行动只支持股票和 ETF 现金账户")
        self._corporate_action_records: dict[tuple[str, date], CorporateActionRecordPosition] = {}
        self._corporate_action_events: list[FinancialEvent] = []
        self._applied_actions: dict[str, CorporateAction] = {}
        self._arrived_actions: set[str] = set()
        self._first_session: date | None = None
        self._record_state: SpotLedgerState | None = None
        self._cash_scale: int | None = None
        self._session_closed = False
        self._session_delisted: set[str] = set()
        self._delisted_instruments: set[str] = set()

    @property
    def asset_class(self) -> str | None:
        return self.explicit_execution.asset_class if self.explicit_execution is not None else self.targets.asset_class

    @property
    def instruments(self) -> dict[str, InstrumentKey]:
        return self.explicit_execution.instruments if self.explicit_execution is not None else self.targets.instruments

    @property
    def current_session(self) -> date | None:
        return self._current_session

    def record_input(self, bar: MinuteExecutionBar) -> None:
        self._bar_count += 1
        self._bar_digest.update(bar.bar_hash.encode("ascii"))
        self._bar_digest.update(b"\n")

    def collect_bar(self, bar: MinuteExecutionBar) -> None:
        self._session_bars.append(bar)
        instrument_hash = self.instruments[bar.instrument_id].instrument_hash
        key = (bar.bar_end, bar.source_sequence)
        previous = self._session_price_keys.get(instrument_hash)
        if previous is None or key > previous:
            self._session_price_keys[instrument_hash] = key
            self._session_prices[instrument_hash] = bar.close_units
        if self._session_valuation_time is None or bar.available_time > self._session_valuation_time:
            self._session_valuation_time = bar.available_time
        self._remember_record_state(bar.available_time)

    def execute_target(
        self, active: _ActiveTarget, bar: MinuteExecutionBar, execute_order: Callable,
    ) -> None:
        if active.prepared.instrument.instrument_hash in self._delisted_instruments:
            return
        order_start, fill_start = len(self._session_orders), len(self._session_fills)
        common = dict(
            resolver=self.resolver, bundle=self.rule_bundle, policy=self.policy,
            execute_order=execute_order, initial_cash_units=self.initial_cash_units,
            used_capacity={}, order_rows=self._session_orders,
            fill_rows=self._session_fills, cost_rows=self._session_costs,
        )
        if self.asset_class == "cn_future":
            self._futures_state = _reconcile_futures_target(
                active, bar, state=self._futures_state, **common,
            )
        else:
            self._spot_state = _reconcile_spot_target(
                active, bar, state=self._spot_state, **common,
            )
        self._remember_record_state(bar.available_time)
        collect_execution_support(
            orders=self._session_orders[order_start:], fills=self._session_fills[fill_start:],
            decision_bar=active.decision_bar, bar=bar,
            decision_benchmarks=self._session_decision_benchmarks,
            execution_observations=self._session_execution_observations,
        )

    @property
    def explicit_order_context(self) -> dict[str, object] | None:
        return None if self.explicit_execution is None else self.explicit_execution.context

    def _explicit_rules(self, command, at):
        instrument = command.instrument
        if instrument.asset_class != "cn_future":
            rules, policy = resolve_minute_cash_policy(self.resolver, bundle=self.rule_bundle,
                instrument=instrument, trading_date=command.trading_date, decision_at=at,
                cash_shortage_policy="clip_current_lot_continue_v1" if command.funds_policy == "resize" else "reject_v1")
            return rules.identity_hash, rules.parameters, policy
        rules = resolve_minute_execution_rules(self.resolver, bundle=self.rule_bundle, asset_class="cn_future",
            instrument_id=instrument.instrument_id, effective_on=command.trading_date, as_of=at)
        return rules.identity_hash, rules.parameters, None

    def execute_explicit_orders(self, bar: MinuteExecutionBar) -> None:
        controller = self.explicit_execution
        state = self._futures_state if self.asset_class == "cn_future" else self._spot_state
        state = controller.process_commands(through=bar.available_time, session=bar.trading_date,
            state=state, resolve=self._explicit_rules)
        instrument = self.instruments[bar.instrument_id]
        command = next(item for item in controller.commands
                       if item.instrument == instrument and item.action == "submit")
        rule_command = replace(command, trading_date=bar.trading_date)
        rule_hash, parameters, policy = self._explicit_rules(rule_command, bar.available_time)
        scale = int(parameters["price_scale"])
        capacity = bar.volume * self.policy.participation_ppm // 1_000_000
        if self.asset_class in {"cn_stock", "cn_etf"} and minute_cash_bar_suspended(
            self.resolver, bundle=self.rule_bundle, asset_class=self.asset_class,
            instrument_id=bar.instrument_id, trading_date=bar.trading_date,
            bar_start=bar.bar_start, bar_end=bar.bar_end, available_at=bar.available_time,
        ):
            capacity = 0
        observation_start = len(controller.execution_observations)
        state = controller.execute(instrument=instrument, event_start=bar.bar_start,
            event_time=bar.available_time, session=bar.trading_date,
            reference_price=Price(bar.avg_units if bar.avg_units is not None else bar.close_units, scale, "CNY"),
            visible_capacity=capacity,
            rules_identity_hash=rule_hash, parameters=parameters, cash_policy=policy, state=state)
        for observation in controller.execution_observations[observation_start:]:
            observation.update(arrival_price_units=bar.open_units,
                               arrival_price_available_at=bar.available_time,
                               visible_capacity=bar.volume, capacity_available_at=bar.available_time)
        if self.asset_class == "cn_future":
            self._futures_state = state
        else:
            self._spot_state = state
        self._remember_record_state(bar.available_time)

    def _explicit_session_end(self) -> datetime:
        bundle = load_session_policy_bundle()
        endings = []
        for instrument in self.instruments.values():
            candidates = [item for item in bundle.policies
                          if item.instrument.instrument_id == instrument.instrument_id
                          and self._current_session in item.trading_dates]
            if not candidates:
                raise SimulationContractError("DAY 到期缺少该标的交易会话来源")
            revision = max(candidates, key=lambda item: item.revision)
            session = revision.build_session(self._current_session,
                scope_binding_hash=typed_canonical_hash(bundle.scope_coverage_payload()))
            endings.append(max(item.ends_at for item in session.segments if item.bar_eligible))
        if len(set(endings)) != 1:
            raise SimulationContractError("当前单账户显式订单要求相同会话终点")
        return endings[0]

    def start_session(self, session: date, event_time: datetime) -> None:
        if session == self._current_session:
            return
        if self._current_session is not None and session < self._current_session:
            raise SimulationContractError("分钟会话不能倒退")
        first = self._current_session is None
        if self._corporate_actions:
            self._initialize_corporate_account(session, event_time)
        self._reset_session(session)
        if not first and self._futures_state is not None:
            self._futures_state = replace(self._futures_state, positions=tuple(
                replace(position, opened_today=0) for position in self._futures_state.positions
            ))
        if self.asset_class in {"cn_stock", "cn_etf"} and self._spot_state is not None:
            if not first or self._corporate_actions:
                self._spot_state, events = settle_cash_daily_open(
                    self._spot_state, effective_time=event_time,
                    rule_hash=self.rule_bundle.bundle_hash,
                )
                self._record_non_trade_events(events)
            if self._corporate_actions:
                self._advance_corporate_actions(session, event_time)
        self._record_state = self._spot_state

    @property
    def corporate_action_context(self) -> dict[str, object]:
        """返回实际登记与账本事件，供正式金融上下文封存。"""
        return {
            "contract_version": "research-minute-corporate-actions-v1",
            "actions": [item.to_dict() for item in self._corporate_actions],
            "records": [item.to_dict() for _, item in sorted(self._corporate_action_records.items())],
            "events": [item.to_dict() for item in self._corporate_action_events],
        }

    def _initialize_corporate_account(self, session: date, event_time: datetime) -> None:
        if self._first_session is not None:
            return
        self._first_session = session
        instrument = next(iter(self.instruments.values()))
        prefix = "rule.cn_stock" if self.asset_class == "cn_stock" else "rule.cn_fund"
        settlement = self.resolver.resolve(
            rule_id=f"{prefix}.settlement.v1", instrument_id=instrument.instrument_id,
            effective_on=session, as_of=event_time,
        )
        days = dict(settlement.rule.parameters).get("settlement_days")
        if type(days) is not int or days not in {0, 1}:
            raise SimulationContractError("分钟公司行动账户缺少 T+0/T+1 结算规则")
        self._cash_scale = self._instrument_cash_scale(instrument, session, event_time)
        self._spot_state = SpotLedgerState(
            ExecutionGroup(f"minute-default-{self.asset_class}", self.asset_class, "CNY", f"t{days}"),
            self.initial_cash_units,
        )
        # 状态机起始空仓，窗口前的登记只能声明明确的零持仓。
        for action in self._corporate_actions:
            if action.record_date < session:
                key = (action.instrument_hash, action.record_date)
                self._corporate_action_records[key] = CorporateActionRecordPosition(
                    action.instrument_hash, self._record_time(action.record_date), 0,
                    f"minute-initial-empty:{session.isoformat()}",
                )

    def _instrument_cash_scale(
        self, instrument: InstrumentKey, session: date, event_time: datetime,
    ) -> int:
        prefix = "rule.cn_stock" if instrument.asset_class == "cn_stock" else "rule.cn_fund"
        binding = self.resolver.resolve(
            rule_id=f"{prefix}.price_limit.v1", instrument_id=instrument.instrument_id,
            effective_on=session, as_of=event_time,
        )
        scale = dict(binding.rule.parameters).get("price_scale")
        if type(scale) is not int or not 0 <= scale <= 8:
            raise SimulationContractError("分钟公司行动缺少现金报价精度")
        return scale

    def _minute_cash_event(self, event: FinancialEvent) -> FinancialEvent:
        # 公共权益原语以分计量，分钟成交账本以规则报价精度计量。
        values = dict(event.payload)
        for field in ("cash_delta_units", "cash_receivable_units"):
            if field in values:
                scaled, remainder = divmod(int(values[field]) * 10 ** self._cash_scale, 100)
                if remainder:
                    raise SimulationContractError("公司行动现金不能精确换算为分钟账户单位")
                values[field] = scaled
        return replace(event, payload=tuple(sorted(values.items())))

    def _record_non_trade_events(self, events: tuple[FinancialEvent, ...]) -> None:
        self._session_non_trade_events.extend(item.event_hash for item in events)
        if self._corporate_actions:
            self._corporate_action_events.extend(events)

    def _advance_corporate_actions(self, session: date, event_time: datetime) -> None:
        visible_ids = {item.action_id for item in self._corporate_actions
                       if item.announcement_available_time <= event_time}
        for action in self._corporate_actions:
            if (self._first_session <= action.effective_date <= session
                    and action.action_id not in visible_ids):
                raise SimulationContractError("公司行动生效时公告迟到，不能未来回填")
        selected = tuple(
            action
            for candidates in self._corporate_action_groups.values()
            for effective in sorted({item.effective_date for item in candidates})
            for action in resolve_corporate_actions(
                tuple(candidates), as_of=event_time, effective_date=effective,
            )
        )
        for action in selected:
            previous = self._applied_actions.get(action.action_id)
            if previous is not None and previous != action:
                raise SimulationContractError("公司行动已落账，不能回填另一修订")
            if (self._first_session <= action.effective_date < session
                    and previous is None):
                raise SimulationContractError("分钟流缺少公司行动生效会话")
        due_actions = tuple(item for item in selected if item.effective_date == session)
        for action in due_actions:
            instrument = next((item for item in self.instruments.values()
                               if item.instrument_hash == action.instrument_hash), None)
            if instrument is None:
                raise SimulationContractError("公司行动标的不在分钟账户声明中")
            if (action.kind != "delisting_cash"
                    and self._instrument_cash_scale(instrument, session, event_time) != self._cash_scale):
                raise SimulationContractError("分钟公司行动账户现金精度不一致")
            previous = self._applied_actions.get(action.action_id)
            if previous is not None:
                if previous != action:
                    raise SimulationContractError("公司行动已落账，不能回填另一修订")
                continue
            _, events = apply_cash_corporate_actions(
                self._spot_state, (action,), effective_time=event_time,
                rule_hash=self.rule_bundle.bundle_hash,
                record_positions=self._corporate_action_records,
            )
            actual = tuple(self._minute_cash_event(item) for item in events)
            for event in actual:
                self._spot_state = reduce_spot(self._spot_state, event)
            self._record_non_trade_events(actual)
            self._applied_actions[action.action_id] = action
            if action.kind == "delisting_cash":
                self._session_delisted.add(action.instrument_hash)
                self._delisted_instruments.add(action.instrument_hash)
        for action in sorted(self._applied_actions.values(), key=lambda item: item.action_id):
            if action.shares_arrival_date is None or action.action_hash in self._arrived_actions:
                continue
            if action.shares_arrival_date < session:
                raise SimulationContractError("分钟流缺少股份到账会话")
            if action.shares_arrival_date == session:
                events = compile_corporate_action_share_arrival(
                    action,
                    record_position=self._corporate_action_records[(action.instrument_hash, action.record_date)],
                    effective_time=event_time, group_id=self._spot_state.group.group_id,
                    rule_hash=self.rule_bundle.bundle_hash,
                )
                for event in events:
                    self._spot_state = reduce_spot(self._spot_state, event)
                self._record_non_trade_events(events)
                self._arrived_actions.add(action.action_hash)

    @staticmethod
    def _record_time(session: date) -> datetime:
        return datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai"))

    def _remember_record_state(self, event_time: datetime) -> None:
        if self._corporate_actions and event_time <= self._record_time(self._current_session):
            self._record_state = self._spot_state

    def _capture_record_positions(self) -> None:
        hashes = tuple(sorted({item.instrument_hash for item in self._corporate_actions
                               if item.record_date == self._current_session}))
        if not hashes:
            return
        records = capture_corporate_action_record_positions(
            self._record_state, record_time=self._record_time(self._current_session),
            source_ref=f"minute-ledger:{self._current_session.isoformat()}:close",
            instrument_hashes=hashes,
        )
        for record in records:
            if record.instrument_hash in hashes:
                self._corporate_action_records[(record.instrument_hash, record.record_date)] = record

    def consume_bar(
        self, bar: MinuteExecutionBar,
    ) -> tuple[MinuteSimulationSessionOutput, ...]:
        return self.engine.consume_bar(self, bar)

    def finish(self) -> tuple[MinuteSimulationSessionOutput, ...]:
        output = self.engine.finish(self)
        if self.explicit_execution is not None:
            self.explicit_execution.require_finished()
        return output

    @property
    def source_simulation_hash(self) -> str:
        if not self.engine.finished:
            raise SimulationContractError("分钟仿真未结束，不能生成正式输入身份")
        identity = {
            "stream_contract": "minute-event-simulation-input-v1",
            "target_count": self.targets._target_count,
            "target_stream_sha256": self.targets._target_digest.hexdigest(),
            "bar_count": self._bar_count,
            "bar_stream_sha256": self._bar_digest.hexdigest(),
            "rule_bundle_hash": self.rule_bundle.bundle_hash,
            "policy": self.policy.to_dict(),
            "initial_cash_units": self.initial_cash_units,
        }
        if self.explicit_execution is not None:
            identity["explicit_orders"] = [item.to_dict() for item in self.explicit_execution.commands]
        if self._corporate_actions:
            identity["corporate_action_snapshot_hash"] = corporate_action_snapshot_hash(self._corporate_actions)
        return typed_canonical_hash(identity)

    @property
    def semantics(self) -> SimulationResultSemantics:
        if not self.engine.finished or self.asset_class is None:
            raise SimulationContractError("分钟仿真未结束，不能生成正式语义")
        return SimulationResultSemantics(
            contract_version=SIMULATION_RESULT_LIFECYCLE_SEMANTICS_VERSION,
            asset_class=self.asset_class,
            frequency="minute",
            decision_time_convention=("explicit_visible_order_stream" if self.explicit_execution is not None else "completed_bar_portfolio_target"),
            execution_time_convention="next_eligible_completed_bar",
            valuation_time_convention="last_completed_bar_per_trading_session",
            price_convention=(
                "raw_integer_cny_rule_scale_times_contract_multiplier"
                if self.asset_class == "cn_future"
                else "raw_integer_cny_rule_scale"
            ),
            fee_model_version=("order_cumulative_cash_fee_v1" if self.explicit_execution is not None else "minute_rule_snapshot_fee_v1"),
            calendar_id="minute_rule_snapshot_session",
            settlement_policy_id=(
                "futures_mark_to_market"
                if self.asset_class == "cn_future"
                else "cash_market_rule"
            ),
            missing_data_policy="fail_closed",
            negative_cash_allowed=False,
            timeline_semantics_hash=typed_canonical_hash({
                **({"explicit_orders": [item.to_dict() for item in self.explicit_execution.commands]} if self.explicit_execution is not None else {}),
                "stream_contract": "minute-target-timeline-v1",
                "rule_bundle_hash": self.rule_bundle.bundle_hash,
                "execution_policy_hash": self.policy.policy_hash,
                "target_count": self.targets._target_count,
                "target_stream_sha256": self.targets._target_digest.hexdigest(),
            }),
        )

    def close_current_session(self) -> MinuteSimulationSessionOutput | None:
        if self._current_session is None or self.asset_class is None or self._session_closed:
            return None
        self._session_closed = True
        if self.explicit_execution is not None:
            controller = self.explicit_execution
            end_at = self._explicit_session_end()
            state = self._futures_state if self.asset_class == "cn_future" else self._spot_state
            state = controller.process_commands(through=end_at, session=self._current_session,
                state=state, resolve=self._explicit_rules)
            state = controller.close_session(self._current_session, end_at, state)
            if self.asset_class == "cn_future":
                self._futures_state = state
            else:
                self._spot_state = state
            self._session_orders = list(controller.order_rows)
            self._session_fills = list(controller.fill_rows)
            self._session_costs = list(controller.cost_rows)
            self._session_decision_benchmarks = list(controller.decision_benchmarks)
            self._session_execution_observations = list(controller.execution_observations)
        if self._corporate_actions:
            self._capture_record_positions()
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
            terminated_instruments=frozenset(self._session_delisted),
        )
        self.engine.close_session(self._current_session, self._session_valuation_time)
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
            order_lifecycle=self.engine.drain_lifecycle(),
        )

    def _reset_session(self, session: date) -> None:
        self._current_session = session
        if self.explicit_execution is not None:
            self.explicit_execution.drain_session()
        self._session_closed = False
        self._session_delisted = set()
        self._session_bars = []
        self._session_orders = []
        self._session_fills = []
        self._session_costs = []
        self._session_prices = {}
        self._session_price_keys = {}
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
    corporate_actions: tuple[CorporateAction, ...] = (),
) -> SimulationResultContract:
    """小样本兼容入口；正式 Runtime 使用同一状态机逐交易日写出。"""

    ordered_bars = _validate_and_sort_bars(bars)
    prepared, _asset_class, _instruments = _prepare_targets(targets, ordered_bars)
    machine = MinuteEventSimulationStateMachine(
        prepared,
        rule_bundle=rule_bundle,
        policy=policy,
        initial_cash_units=initial_cash_units,
        corporate_actions=corporate_actions,
    )
    rows = {name: [] for name in (
        "orders", "fills", "positions", "cash", "costs", "valuations"
    )}
    lifecycle_rows: list[dict[str, object]] = []
    for bar in ordered_bars:
        for output in machine.consume_bar(bar):
            lifecycle_rows.extend(output.order_lifecycle)
            for name, values in output.rows.items():
                rows[name].extend(values)
    for output in machine.finish():
        lifecycle_rows.extend(output.order_lifecycle)
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
        order_lifecycle=tuple(lifecycle_rows),
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
            PreparedMinuteTarget(target, instrument, desired, decision_bar.bar_end, decision_bar)
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


__all__ = [
    "INTRADAY_EXECUTION_POLICY_VERSION",
    "IntradayExecutionPolicy",
    "MinuteEventSimulationStateMachine",
    "MinuteExecutionBar",
    "MinuteSimulationSessionOutput",
    "PreparedMinuteTarget",
    "run_minute_event_simulation",
]
