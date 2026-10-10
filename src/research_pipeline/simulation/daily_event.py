"""只依赖公共 Target、OrderIntent 与现货账本的日频事件仿真。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time
import json
import math
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from research_pipeline.domain import CorporateAction, MarketRuleSnapshot, Money, Price
from research_pipeline.domain.corporate_actions import resolve_corporate_actions
from research_pipeline.domain.external_cashflows import ExternalCashflow, parse_external_cashflows
from research_pipeline.domain.non_trading_sessions import parse_non_trading_sessions
from research_pipeline.domain.order_stream import ExplicitOrderCommand, parse_order_commands
from research_pipeline.domain.trading import OrderIntent, PortfolioTarget, TradingRuleBinding
from research_pipeline.platform import typed_canonical_hash

from .cash_market import (
    CashExecutionResult,
    OpeningSnapshot,
    apply_cash_corporate_actions,
    cash_daily_nav_units,
    cash_daily_preopen_at,
    cash_position_quantities,
    cash_price_amount_units,
    execute_cash_order,
    settle_cash_daily_open,
)
from .cn_etf import etf_policy_from_rule
from .cn_stock import stock_policy_from_rule
from .corporate_actions import (CorporateActionRecordPosition, compile_corporate_action_share_arrival, corporate_action_snapshot_hash)
from .engine import ExecutionEngine, ExecutionOutcome
from .events import FinancialEvent
from .intent_port import CASH_DAILY_BACKEND, IntentToOrderPort
from .ledger import ExecutionGroup, SpotLedgerState, reduce_spot
from .orders import SimulationContractError
from .target_execution import daily_target_deltas


DAILY_EVENT_SIMULATION_VERSION = "research-daily-event-simulation-v4"
_TIMEZONE = ZoneInfo("Asia/Shanghai")
DAILY_CASH_PRICE_SCALE = 3


@dataclass(frozen=True)
class DailyCashSimulationResult:
    orders: pd.DataFrame
    trades: pd.DataFrame
    ledger: pd.DataFrame
    nav: pd.DataFrame
    metrics: pd.DataFrame
    position_snapshots: pd.DataFrame
    cash_snapshots: pd.DataFrame
    simulation_hash: str
    corporate_actions: tuple[CorporateAction, ...] = ()
    backend_id: str = "cash-daily-v1"
    fidelity: str = "bar_level_historical_research"
    limitations: tuple[str, ...] = (
        "日线开盘事件成交，不模拟 tick、排队位置或实盘成交概率",
        "佣金是显式研究成本假设，不代表所有券商账户",
    )
    contract_version: str = DAILY_EVENT_SIMULATION_VERSION
    order_lifecycle: tuple[dict[str, object], ...] = ()
    corporate_action_records: tuple[dict[str, object], ...] = ()
    account_context: Mapping[str, object] | None = None
    external_cashflow_context: Mapping[str, object] | None = None
    credit_context: Mapping[str, object] | None = None
    execution_mode: str = "target"
    explicit_order_context: Mapping[str, object] | None = None
    explicit_order_rows: tuple[dict[str, object], ...] = ()
    explicit_fill_rows: tuple[dict[str, object], ...] = ()
    explicit_cost_rows: tuple[dict[str, object], ...] = ()
    explicit_execution_observations: tuple[dict[str, object], ...] = ()
    non_trading_sessions: Mapping[str, object] | None = None


def run_daily_cash_event_simulation(
    *,
    market: pd.DataFrame,
    intent_plans: pd.DataFrame,
    corporate_actions: Sequence[CorporateAction],
    rule_by_code: Mapping[str, MarketRuleSnapshot],
    initial_cash_cny: float,
    market_data_artifact_hash: str,
    columns: Mapping[str, str] | None = None,
    rule_history_by_code: Mapping[str, Sequence[MarketRuleSnapshot]] | None = None,
    corporate_action_records: Sequence[CorporateActionRecordPosition] = (),
    account: Mapping[str, object] | None = None,
    execution_mode: str = "target",
    order_commands: Sequence[ExplicitOrderCommand] = (),
    external_cashflows: str | Sequence[ExternalCashflow | Mapping[str, object]] = (),
    credit_account=None,
    non_trading_sessions: Mapping[str, object] | None = None,
) -> DailyCashSimulationResult:
    """按结算→公司行动→卖出→买入→收盘估值顺序重放日线事件。"""
    if credit_account is not None and (account is None or execution_mode != "explicit_orders"):
        raise SimulationContractError("融资信用账户必须提供现货期初账户并使用explicit_orders")
    if not math.isfinite(initial_cash_cny) or initial_cash_cny < 0 or (initial_cash_cny == 0 and account is None):
        raise SimulationContractError("initial_cash_cny 必须为正")
    declaration = parse_non_trading_sessions(non_trading_sessions)
    flows = parse_external_cashflows(external_cashflows)
    commands = parse_order_commands(order_commands)
    if execution_mode not in {"target", "explicit_orders"}:
        raise SimulationContractError("execution_mode 只能是 target 或 explicit_orders")
    if declaration is None and execution_mode == "explicit_orders" and credit_account is None and not any(command.action == "submit" for command in commands):
        raise SimulationContractError("explicit_orders 模式至少需要一条 submit 命令")
    if not intent_plans.empty and commands:
        raise SimulationContractError("目标与显式命令不能同时控制同一账户，存在标的冲突")
    if execution_mode == "target" and commands:
        raise SimulationContractError("target 模式不能携带显式命令")
    if execution_mode == "explicit_orders" and not intent_plans.empty:
        raise SimulationContractError("explicit_orders 模式不能携带非空目标")
    _require_hash(market_data_artifact_hash, "market_data_artifact_hash")
    frame = _canonical_market(market, columns)
    if declaration is not None:
        if not frame.empty or not intent_plans.empty or commands:
            raise SimulationContractError("non_trading_sessions 只允许空行情、空目标且无订单命令")
        if account is None or credit_account is not None or flows:
            raise SimulationContractError("non_trading_sessions 必须提供普通 cash 期初账户且不能携带信用或外部资金流")
        codes = tuple(sorted(rule_by_code))
        sessions = tuple(date.fromisoformat(item) for item in declaration["dates"])
    else:
        codes = tuple(sorted(frame["code"].unique()))
        sessions = tuple(sorted(frame["date"].unique()))
    if set(rule_by_code) != set(codes):
        raise SimulationContractError("现货规则快照必须与行情标的一一对应")
    markets = {rule.market for rule in rule_by_code.values()}
    if len(markets) != 1 or next(iter(markets)) not in {"cn_stock", "cn_etf"}:
        raise SimulationContractError("日频现金仿真必须使用唯一股票或 ETF 市场")
    market_id = next(iter(markets))
    policy_factory = (
        stock_policy_from_rule if market_id == "cn_stock" else etf_policy_from_rule
    )
    current_rules = dict(rule_by_code)
    if rule_history_by_code is not None and set(rule_history_by_code) != set(codes):
        raise SimulationContractError("历史规则必须与行情标的一一对应")
    policies = {code: policy_factory(rule_by_code[code]) for code in codes}
    action_hash = corporate_action_snapshot_hash(corporate_actions)
    records = {(item.instrument_hash, item.record_date): item for item in corporate_action_records}
    if len(records) != len(corporate_action_records):
        raise SimulationContractError("公司行动登记持仓身份重复")
    needed_records = {(item.instrument_hash, item.record_date) for item in corporate_actions
                      if item.contract_version == 2 and item.kind != "delisting_cash"}
    state = SpotLedgerState(
        ExecutionGroup(
            f"{market_id}-cny-daily",
            market_id,
            "CNY",
            "mixed-t0-t1" if market_id == "cn_etf" else "t1",
        ),
        _money_units(initial_cash_cny),
    )
    account_execution = None
    opening_nav_units = _money_units(initial_cash_cny)
    if account is not None:
        from .spot_account_execution import SpotAccountExecution
        account_execution = SpotAccountExecution(account, group=state.group)
        state = account_execution.initial_state
        opening_nav_units = account_execution.opening_nav_units
        if state.total_cash_units != _money_units(initial_cash_cny):
            raise SimulationContractError("initial_cash_cny 必须与期初现金及应收合计一致")
        if any(lot.instrument_hash not in {_instrument_hash(code, market_id) for code in codes}
               for lot in state.positions):
            raise SimulationContractError("期初持仓不在声明行情证券范围")
        if account_execution.snapshot.started_at > cash_daily_preopen_at(sessions[0]):
            raise SimulationContractError("期初快照不能晚于首个交易会话盘前")
        overlaps = {plan.instrument_hash for plan in account_execution.dividends} & {
            action.instrument_hash for action in corporate_actions if action.kind == "cash_dividend"}
        if overlaps:
            raise SimulationContractError("同一证券的分红须统一由账户权益计划声明，不能重复登记公司行动")
    if declaration is not None:
        opening_instruments = {lot.instrument_hash for lot in state.positions
                               if lot.sellable + lot.unsettled + lot.frozen > 0}
        first_actions = resolve_corporate_actions(
            tuple(corporate_actions), as_of=cash_daily_preopen_at(sessions[0]), effective_date=sessions[0],
        )
        exiting_instruments = {action.instrument_hash for action in first_actions
                               if action.contract_version == 2 and action.kind == "delisting_cash"}
        if not opening_instruments or not opening_instruments <= exiting_instruments:
            raise SimulationContractError("non_trading_sessions 首会话必须以可见 v2 delisting_cash 退出全部非零期初证券")
    plan_rows = tuple(intent_plans.itertuples(index=False))
    normalized_plan_dates = tuple(
        _normalized_execution_date(row.order_time) for row in plan_rows
    )
    seen_dates: set[date] = set()
    duplicate_plan_dates: set[date] = set()
    for plan_date in normalized_plan_dates:
        if plan_date in seen_dates:
            duplicate_plan_dates.add(plan_date)
        seen_dates.add(plan_date)
    if duplicate_plan_dates:
        dates = ", ".join(value.isoformat() for value in sorted(duplicate_plan_dates))
        raise SimulationContractError(f"日频现金计划的执行日必须唯一，重复日期: {dates}")
    plans = {
        plan_date: row
        for plan_date, row in sorted(
            zip(normalized_plan_dates, plan_rows, strict=True),
            key=lambda item: item[0],
        )
    }
    action_by_date: dict[date, list[CorporateAction]] = {}
    for action in corporate_actions:
        action_by_date.setdefault(action.effective_date, []).append(action)
    order_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    ledger_rows: list[dict[str, object]] = []
    if account_execution is not None:
        ledger_rows.append({"event_id": f"opening:{account_execution.snapshot.snapshot_id}",
            "kind": "account_opening", "effective_time": account_execution.snapshot.started_at,
            "session": str(sessions[0]), "parent_id": None,
            "payload_hash": typed_canonical_hash(account_execution.snapshot.to_dict()),
            "state_hash": state.state_hash, "available_cash_units": state.available_cash_units,
            "frozen_cash_units": state.frozen_cash_units, "unsettled_cash_units": state.unsettled_cash_units})
    nav_rows: list[dict[str, object]] = []
    position_snapshot_rows: list[dict[str, object]] = []
    cash_snapshot_rows: list[dict[str, object]] = []
    order_lifecycle_rows: list[dict[str, object]] = []
    applied_actions: dict[str, CorporateAction] = {}
    total_fees = 0
    total_turnover = 0
    previous_quantities: dict[str, int] = {}
    credit_execution = None
    if credit_account is not None:
        from .credit_account_execution import CreditAccountExecution
        credit_execution = CreditAccountExecution(credit_account, account_execution=account_execution, commands=commands, sessions=sessions)
        state = credit_execution.initialize(state)
        opening_nav_units = credit_execution.opening_nav_units
        account_execution.credit_hook = credit_execution
        if ledger_rows:
            ledger_rows[-1]["state_hash"] = state.state_hash
    cashflow_execution = None
    if flows or opening_nav_units == 0 or credit_execution is not None:
        from .external_cashflows import ExternalCashflowExecution
        cashflow_execution = ExternalCashflowExecution(
            flows, state=state,
            account_id="default" if account_execution is None else account_execution.snapshot.account_id,
            sessions=sessions,
            started_at=datetime.combine(sessions[0], time(0), _TIMEZONE) if account_execution is None else account_execution.snapshot.started_at,
            opening_nav_units=opening_nav_units,
            withdrawal_check=None if credit_execution is None else credit_execution.withdrawal_allowed,
        )
        state = replace(state, cashflow_tracking=True)
    port = IntentToOrderPort(CASH_DAILY_BACKEND)
    engine = ExecutionEngine()
    explicit = None
    initial_order_cash_units = state.total_cash_units
    initial_order_positions = [{"instrument_hash": lot.instrument_hash,
        "quantity": lot.sellable + lot.unsettled + lot.frozen,
        "sellable_quantity": lot.sellable} for lot in state.positions]
    explicit_event_index = 0
    command_index = 0
    explicit_order_rows: list[dict[str, object]] = []
    explicit_fill_rows: list[dict[str, object]] = []
    explicit_cost_rows: list[dict[str, object]] = []
    explicit_execution_observations: list[dict[str, object]] = []
    if execution_mode == "explicit_orders":
        from .explicit_orders import ExplicitOrderExecution
        if any(command.instrument != _instrument(command.instrument.instrument_id, market_id)
               or command.instrument.instrument_id not in codes for command in commands):
            raise SimulationContractError("显式命令标的必须与日频行情、现金市场一致")
        if any(command.trading_date not in sessions or command.submitted_at > datetime.combine(
                command.trading_date, time(15), _TIMEZONE) for command in commands):
            raise SimulationContractError("显式命令必须归属输入交易会话且不晚于会话收尾")
        if account_execution is not None and any(command.submitted_at < account_execution.snapshot.started_at
                                                  for command in commands):
            raise SimulationContractError("显式命令不能早于账户期初时点")
        if commands or credit_execution is not None:
            explicit = ExplicitOrderExecution(commands, broker=engine.broker,
                                              initial_cash_units=initial_order_cash_units, cash_scale=2, credit_hook=credit_execution)

    def explicit_context():
        facts = explicit.context if explicit is not None else {
            "contract_version": "research-explicit-order-execution-v1", "commands": [],
            "initial_cash_units": initial_order_cash_units, "cash_scale": 2,
            "events": [], "observations": [], "fee_facts": [], "command_rules": [],
            "session_ends": {session.isoformat(): datetime.combine(session, time(15), _TIMEZONE).isoformat()
                             for session in sessions},
        }
        return {**facts, "initial_positions": initial_order_positions}

    def resolve_explicit(command, at):
        code = command.instrument.instrument_id
        candidates = (rule_by_code[code],) if rule_history_by_code is None else rule_history_by_code[code]
        visible = [rule for rule in candidates if rule.effective_start <= at.date()
                   and (rule.effective_end is None or at.date() <= rule.effective_end)
                   and rule.available_time <= at]
        if len(visible) != 1:
            raise SimulationContractError("显式订单在提交时没有唯一可见的历史规则")
        rule = visible[0]
        return rule.content_hash, dict(rule.parameters), policy_factory(rule)

    def collect_explicit(before):
        nonlocal explicit_event_index, total_fees, total_turnover
        replay = before
        for event in explicit.events[explicit_event_index:]:
            replay = reduce_spot(replay, event)
            ledger_rows.append(_ledger_event_row(event, replay))
            if credit_execution is not None:
                credit_execution.record(event, replay)
            if event.kind != "fill":
                if account_execution is not None and event.kind == "settlement":
                    account_execution.events.append(event)
                continue
            values = event.values()
            code = next(code for code in codes if _instrument_hash(code, market_id) == values["instrument_hash"])
            if account_execution is not None and credit_execution is None:
                sellable_at = event.effective_time
                if values["side"] == "buy" and policies[code].settlement_days:
                    next_session = account_execution.next_settlement_session(event.effective_time, sessions)
                    sellable_at = cash_daily_preopen_at(next_session)
                account_execution.record_fill(event, sellable_at=sellable_at,
                                              security_class="equity" if market_id == "cn_stock" else "etf")
            fee, notional = int(values["fee_units"]), int(values["notional_units"])
            total_fees += fee
            total_turnover += notional
            trade_rows.append({
                "fill_id": event.event_id, "fill_hash": event.event_hash,
                "fill_time": event.effective_time, "session": date.fromisoformat(event.session),
                "code": code, "side": values["side"], "quantity": values["quantity"],
                "notional_units": notional, "fee_units": fee,
                "price_cny": float(Price(int(values["execution_price_units"]), int(values["price_scale"]), "CNY").decimal),
                "execution_price_units": int(values["execution_price_units"]),
                "price_scale": int(values["price_scale"]), "order_id": event.parent_id,
                "intent_hash": event.parent_id,
            })
        explicit_event_index = len(explicit.events)
        if replay != state:
            raise SimulationContractError("显式订单金融事件与日频账本不一致")

    code_by_instrument = {_instrument_hash(code, market_id): code for code in codes}

    def cashflow_valuation(value, at):
        local = at.astimezone(_TIMEZONE)
        price_column = "open" if local.time() == time(9, 30) else "close"
        day = frame.loc[frame["date"] == local.date()].set_index("code")
        prices = {}
        components = []
        for lot in value.positions:
            quantity = lot.sellable + lot.unsettled + lot.frozen
            if not quantity:
                continue
            code = code_by_instrument.get(lot.instrument_hash)
            if code is None or code not in day.index:
                raise SimulationContractError("资金流精确估值缺少持仓时点行情")
            raw = day.loc[code, price_column]
            if not math.isfinite(float(raw)) or float(raw) <= 0:
                raise SimulationContractError("资金流精确估值价格必须为正且有限")
            price = _price(raw)
            prices[lot.instrument_hash] = price
            components.append({"instrument_hash": lot.instrument_hash, "code": code, "quantity": quantity,
                "price_units": price.units, "price_scale": price.scale,
                "market_value_units": cash_price_amount_units(price, quantity, cash_scale=2),
                "price_column": price_column, "observed_at": at.isoformat(), "available_at": at.isoformat(),
                "source_ref": market_data_artifact_hash})
        account_components = {"pending_successor_units": 0, "payable_units": value.payable_tax_units}
        if account_execution is not None:
            account_components = account_execution.cashflow_valuation_components(value, at=at)
        nav_units = cash_daily_nav_units(value, prices) + account_components["pending_successor_units"]
        if credit_execution is not None:
            for key, price in prices.items():
                credit_execution.mark(key, price, at=at, source_ref=market_data_artifact_hash)
            nav_units -= value.credit_state.principal_units + value.credit_state.interest_units
            account_components["credit"] = credit_execution.value(value, at)
        return {"nav_units": nav_units, "valuation_at": at.isoformat(),
                "valuation_source": f"daily_raw_{price_column}",
                "components": {"cash_units": value.total_cash_units, "positions": components,
                               "account": account_components, "market_data_artifact_hash": market_data_artifact_hash}}

    def append_output(output):
        for event, value in output:
            ledger_rows.append(_ledger_event_row(event, value))
            if credit_execution is not None:
                credit_execution.record(event, value)

    def advance_daily_commands(until, session, *, before_execution=False, strict_before=False):
        nonlocal state, command_index
        while True:
            command_at = None
            if explicit is not None and command_index < len(commands) and commands[command_index].trading_date <= session:
                command_at = commands[command_index].submitted_at
            flow_at = None if cashflow_execution is None else cashflow_execution.next_at
            credit_at = None if credit_execution is None else credit_execution.next_at
            account_at = None
            if credit_execution is not None and account_execution.cursor < len(account_execution.schedule):
                candidate = account_execution.schedule[account_execution.cursor]
                if not (before_execution and candidate[0] == until and candidate[3] in {"assess", "collect"}):
                    account_at = candidate[0]
            pending = [at for at in (command_at, flow_at, credit_at, account_at) if at is not None]
            if not pending:
                break
            at = min(pending)
            if at > until or strict_before and at == until:
                break
            if credit_execution is not None:
                state, output = credit_execution.accrue(state, at)
                append_output(output)
            if account_execution is not None:
                state, output = account_execution.advance(state, until=at, before_execution=before_execution)
                append_output(output)
            if credit_execution is not None:
                state, output = credit_execution.advance(state, until=at)
                append_output(output)
            if flow_at is not None and flow_at == at:
                state, output = cashflow_execution.advance(state, until=at, valuation=cashflow_valuation)
                append_output(output)
            if command_at is not None and command_at == at:
                before = state
                state = explicit.process_commands(through=at, session=session, state=state, resolve=resolve_explicit)
                collect_explicit(before)
                while command_index < len(commands) and commands[command_index].submitted_at == at:
                    command_index += 1
        if credit_execution is not None:
            state, output = credit_execution.accrue(state, until)
            append_output(output)
        if account_execution is not None:
            state, output = account_execution.advance(state, until=until, before_execution=before_execution)
            append_output(output)
        if credit_execution is not None and not strict_before:
            state, output = credit_execution.advance(state, until=until)
            append_output(output)

    def before_open(session):
        nonlocal state, current_rules, policies
        session_trade_start = len(trade_rows)
        opening_time = datetime.combine(session, time(9, 30), _TIMEZONE)
        preopen_time = cash_daily_preopen_at(session)
        if rule_history_by_code is not None:
            current_rules = {}
            # 换股前后证券的实际行情窗口可以不重叠；只绑定本会话可观察的证券。
            session_codes = codes if declaration is not None else tuple(frame.loc[frame["date"] == session, "code"])
            for code in session_codes:
                matches = [rule for rule in rule_history_by_code[code]
                           if rule.effective_start <= session
                           and (rule.effective_end is None or session <= rule.effective_end)
                           and rule.available_time <= (preopen_time if declaration is not None else opening_time)]
                if len(matches) != 1 or matches[0].market != market_id:
                    raise SimulationContractError("日频历史规则缺失、重叠或在开盘时尚不可见")
                current_rules[code] = matches[0]
            policies = {code: policy_factory(rule) for code, rule in current_rules.items()}
        if declaration is not None and any(
            rule.effective_start > session
            or (rule.effective_end is not None and session > rule.effective_end)
            or rule.available_time > preopen_time for rule in current_rules.values()
        ):
            raise SimulationContractError("非交易清算会话缺少当时可见的有效历史规则")
        settlement_rule_hash = typed_canonical_hash({
            "market": market_id,
            "policy": "cash-daily-settlement-v1",
        })
        advance_daily_commands(preopen_time, session, strict_before=True)
        if cashflow_execution is not None:
            state, output = cashflow_execution.settle_sales(state, preopen_time)
            ledger_rows.extend(_ledger_event_row(event, value) for event, value in output)
        if credit_execution is not None:
            state, output = credit_execution.advance(state, until=preopen_time)
            append_output(output)
        before = state
        state, settlement_events = settle_cash_daily_open(
            state,
            effective_time=preopen_time,
            rule_hash=settlement_rule_hash,
            protected_quantities=None if account_execution is None else account_execution.protected_quantities(preopen_time),
            managed_receivables=(frozenset() if account_execution is None else account_execution.managed_receivables) | (frozenset() if credit_execution is None else frozenset(item.claim_id for item in state.credit_state.sale_claims)),
            managed_position_entitlements=frozenset() if account_execution is None else account_execution.managed_position_entitlements,
        )
        replay = before
        for event in settlement_events:
            replay = reduce_spot(replay, event)
            ledger_rows.append(_ledger_event_row(event, replay))
            if credit_execution is not None:
                credit_execution.record(event, replay)
            if account_execution is not None:
                account_execution.events.append(event)
        before = state
        selected_actions = resolve_corporate_actions(tuple(corporate_actions), as_of=preopen_time, effective_date=session)
        for candidate in action_by_date.get(session, ()):
            if not any(item.action_id == candidate.action_id and item.announcement_available_time <= preopen_time
                       for item in corporate_actions):
                raise SimulationContractError("公司行动生效时仍不可见")
        state, action_events = apply_cash_corporate_actions(
            state,
            selected_actions,
            effective_time=preopen_time,
            rule_hash=typed_canonical_hash({
                "market": market_id,
                "policy": "cash-daily-corporate-action-v1",
            }),
            record_positions=records,
        )
        replay = before
        for event in action_events:
            replay = reduce_spot(replay, event)
            ledger_rows.append(_ledger_event_row(event, replay))
            if credit_execution is not None:
                credit_execution.record(event, replay)
            if account_execution is not None:
                from research_pipeline.domain.spot_account import AccountSource
                action = next(item for item in selected_actions if item.action_id == event.parent_id)
                account_execution.book.record_corporate_action(action, event,
                    source=AccountSource(action.source_ref or f"corporate:{action.action_id}", action.announcement_available_time),
                    lot_rule=account_execution.corporate_action_lot_rules.get(action.action_id))
                account_execution.events.append(event)
        applied_actions.update((item.action_id, item) for item in selected_actions)
        for action in applied_actions.values():
            if action.contract_version != 2 or action.shares_arrival_date != session:
                continue
            for event in compile_corporate_action_share_arrival(
                action, record_position=records.get((action.instrument_hash, action.record_date)),
                effective_time=preopen_time, group_id=state.group.group_id,
                rule_hash=action.action_hash,
            ):
                state = reduce_spot(state, event)
                ledger_rows.append(_ledger_event_row(event, state))
                if account_execution is not None:
                    from research_pipeline.domain.spot_account import AccountSource
                    account_execution.book.record_corporate_action(action, event,
                        source=AccountSource(action.source_ref or f"corporate:{action.action_id}", action.announcement_available_time),
                        lot_rule=account_execution.corporate_action_lot_rules.get(action.action_id))
                    account_execution.events.append(event)
        if credit_execution is not None:
            state, output = account_execution.advance(state, until=preopen_time)
            append_output(output)
            before_risk = state
            state, output = credit_execution.check_risk(state, preopen_time, explicit=explicit)
            append_output(output)
            collect_explicit(output[-1][1] if output else before_risk)
            before_risk = state
            state = credit_execution.submit_risk_orders(state, at=preopen_time, explicit=explicit,
                instruments={_instrument_hash(code, market_id): _instrument(code, market_id) for code in codes}, resolve=resolve_explicit)
            collect_explicit(before_risk)
        day = frame.loc[frame["date"] == session].set_index("code")
        if declaration is not None:
            # 期初 marks 只用于起点估值，清算行动后不得替代会话的缺价门禁。
            _nav_units(state, day, price_column="open", market=market_id)
        return day, session_trade_start, opening_time

    def reconcile(session, context):
        nonlocal state, total_fees, total_turnover
        day, _session_trade_start, opening_time = context
        advance_daily_commands(opening_time, session, before_execution=True)
        if credit_execution is not None:
            for code in day.index:
                credit_execution.mark(_instrument_hash(code, market_id), _price(day.loc[code, "open"]), at=opening_time, source_ref=market_data_artifact_hash)
        if credit_execution is not None:
            before_risk = state
            state, output = credit_execution.check_risk(state, opening_time, explicit=explicit)
            append_output(output)
            collect_explicit(output[-1][1] if output else before_risk)
        if explicit is not None:
            for code in day.index:
                instrument = _instrument(code, market_id)
                raw = day.loc[code]
                rule = current_rules[code]
                parameters = dict(rule.parameters)
                parameters.update(paused=bool(raw["paused"]), price_scale=DAILY_CASH_PRICE_SCALE)
                if parameters.get("price_limit_mode") != "unbounded":
                    parameters.update(high_limit_units=_price(raw["high_limit"]).units,
                                      low_limit_units=_price(raw["low_limit"]).units)
                terminated = any(action.instrument_hash == instrument.instrument_hash
                    and action.contract_version == 2 and action.trading_termination_date is not None
                    and action.trading_termination_date <= session
                    and action.announcement_available_time <= opening_time for action in corporate_actions)
                if account_execution is not None:
                    terminated = terminated or any(item.plan.old_instrument_hash == instrument.instrument_hash
                                                  for item in account_execution.book.conversions)
                if terminated:
                    parameters["paused"] = True
                capacity = _opening_capacity(raw)
                before = state
                fill_start = len(explicit.fill_rows)
                state = explicit.execute(instrument=instrument, event_start=opening_time,
                    event_time=opening_time, session=session, reference_price=_price(raw["open"]),
                    visible_capacity=capacity, rules_identity_hash=rule.content_hash,
                    parameters=parameters, cash_policy=policies[code], state=state)
                collect_explicit(before)
                if credit_execution is not None:
                    before_risk = state
                    state, output = credit_execution.check_risk(state, opening_time, explicit=explicit)
                    append_output(output)
                    collect_explicit(output[-1][1] if output else before_risk)
                explicit.observations[-1]["capacity_model"] = (
                    "visible_capacity" if "visible_capacity" in raw else "assumed_unbounded")
                explicit_execution_observations.extend({
                    "source_fill_id": row["fill_id"], "arrival_price_units": _price(raw["open"]).units,
                    "arrival_price_available_at": opening_time, "visible_capacity": capacity,
                    "capacity_available_at": opening_time,
                } for row in explicit.fill_rows[fill_start:])
        plan_row = plans.get(session)
        if plan_row is not None:
            target = _portfolio_target_from_json(str(plan_row.target_json))
            _validate_cash_target(
                target,
                codes=codes,
                rule_by_code=current_rules,
                market=market_id,
            )
            if pd.Timestamp(plan_row.order_time).to_pydatetime() != opening_time:
                raise SimulationContractError("OrderIntent plan 不是下一交易日开盘事件")
            nav_at_open = _nav_units(
                state, day, price_column="open", market=market_id
            )
            if account_execution is not None:
                nav_at_open += sum(value for _, value in account_execution.book.pending_successor_values(as_of=opening_time))
            weights = {
                item.instrument.instrument_id: float(item.value)
                for item in target.entries
            }
            terminated = {item.instrument_hash for item in corporate_actions
                          if item.contract_version == 2 and item.trading_termination_date is not None
                          and item.trading_termination_date <= session
                          and item.announcement_available_time <= opening_time}
            if account_execution is not None:
                terminated.update(item.plan.old_instrument_hash for item in account_execution.book.conversions)
            for entry in target.entries:
                if float(entry.value) and (entry.instrument.instrument_id not in day.index
                                          or entry.instrument.instrument_hash in terminated):
                    raise SimulationContractError("目标包含已终止交易或缺少当日行情的证券")
            code_to_instrument = {
                code: _instrument(code, market_id) for code in day.index
                if _instrument_hash(code, market_id) not in terminated
            }
            current = cash_position_quantities(
                state,
                {
                    code: instrument.instrument_hash
                    for code, instrument in code_to_instrument.items()
                },
            )
            sequence = daily_target_deltas(
                weights,
                code_to_instrument=code_to_instrument,
                open_prices={code: _price(day.loc[code, "open"]) for code in code_to_instrument},
                nav_units=nav_at_open,
                policies=policies,
                current_quantities=current,
            )
            for ordinal, (code, side, quantity) in enumerate(sequence):
                instrument = target_entry_instrument(target, code)
                if instrument is None:
                    instrument = _instrument(code, market_id)
                binding = TradingRuleBinding(
                    instrument.instrument_hash,
                    current_rules[code].content_hash,
                    current_rules[code].available_time,
                    corporate_action_snapshot_hash=action_hash,
                )
                intent = OrderIntent(
                    instrument,
                    side,
                    quantity,
                    "auto",
                    target.decision_time,
                    opening_time,
                    target.target_hash,
                    market_data_artifact_hash,
                    binding,
                    tuple(sorted(set((*target.source_hashes, str(plan_row.intent_plan_hash))))),
                )
                order = port.to_order(intent, ordinal=ordinal)
                raw = day.loc[code]
                snapshot = OpeningSnapshot(
                    instrument.instrument_hash,
                    _price(raw["open"]),
                    None if dict(current_rules[code].parameters).get("price_limit_mode") == "unbounded" else _price(raw["high_limit"]),
                    None if dict(current_rules[code].parameters).get("price_limit_mode") == "unbounded" else _price(raw["low_limit"]),
                    bool(raw["paused"]),
                    _opening_capacity(raw) if "visible_capacity" in raw else order.quantity,
                    opening_time,
                )
                def execute() -> ExecutionOutcome[CashExecutionResult]:
                    result = execute_cash_order(
                        order,
                        policy=policies[code],
                        snapshot=snapshot,
                        state=state,
                        execution_at=opening_time,
                        cash_scale=2,
                    )
                    return ExecutionOutcome(
                        filled_quantity=result.filled_quantity,
                        reason=result.reason_code,
                        value=result,
                    )

                result = engine.execute_order(
                    order,
                    trading_session=session,
                    event_time=opening_time,
                    execute=execute,
                )
                order_rows.append({
                    "session": session,
                    "code": code,
                    "side": order.side,
                    "requested_quantity": order.quantity,
                    "filled_quantity": result.filled_quantity,
                    "reason_code": result.reason_code,
                    "intent_hash": intent.intent_hash,
                    "order_id": order.order_id,
                    "target_hash": target.target_hash,
                    "rule_hash": result.rule_hash,
                })
                before = state
                state = result.state
                replay = before
                for event in result.events:
                    replay = reduce_spot(replay, event)
                    values = event.values()
                    ledger_rows.append(_ledger_event_row(event, replay))
                    if event.kind == "fill":
                        if account_execution is not None:
                            sellable_at = opening_time
                            if values["side"] == "buy" and policies[code].settlement_days:
                                next_session = account_execution.next_settlement_session(opening_time, sessions)
                                sellable_at = cash_daily_preopen_at(next_session)
                            account_execution.record_fill(event, sellable_at=sellable_at,
                                                          security_class="equity" if market_id == "cn_stock" else "etf")
                        fee = int(values["fee_units"])
                        notional = int(values["notional_units"])
                        total_fees += fee
                        total_turnover += notional
                        trade_rows.append({
                            "fill_id": event.event_id,
                            "fill_hash": event.event_hash,
                            "fill_time": event.effective_time,
                            "session": session,
                            "code": code,
                            "side": values["side"],
                            "quantity": values["quantity"],
                            "notional_units": notional,
                            "fee_units": fee,
                            "price_cny": float(Price(int(values["execution_price_units"]), int(values["price_scale"]), "CNY").decimal),
                            "execution_price_units": int(values["execution_price_units"]),
                            "price_scale": int(values["price_scale"]),
                            "order_id": order.order_id,
                            "intent_hash": intent.intent_hash,
                        })

    def after_close(session, context):
        nonlocal state
        day, session_trade_start, _opening_time = context
        valuation_time = datetime.combine(session, time(15), _TIMEZONE)
        advance_daily_commands(valuation_time, session)
        if credit_execution is not None:
            for code in day.index:
                credit_execution.mark(_instrument_hash(code, market_id), _price(day.loc[code, "close"]), at=valuation_time, source_ref=market_data_artifact_hash)
        if explicit is not None:
            before = state
            state = explicit.close_session(session, valuation_time, state)
            collect_explicit(before)
            explicit_order_rows.extend(dict(row) for row in explicit.order_rows)
            explicit_fill_rows.extend(dict(row) for row in explicit.fill_rows)
            explicit_cost_rows.extend(dict(row) for row in explicit.cost_rows)
        pending_value = 0
        if account_execution is not None:
            from research_pipeline.domain.spot_account import AccountSource
            visible_actions = {}
            for action in corporate_actions:
                if action.record_date == session and action.announcement_available_time <= valuation_time:
                    prior = visible_actions.get(action.action_id)
                    if prior is None or action.revision > prior.revision:
                        visible_actions[action.action_id] = action
            for action in visible_actions.values():
                if any(item.action_id == action.action_id and item.applied for item in account_execution.book.corporate_action_records):
                    continue
                account_execution.book.register_corporate_action(action, effective_time=valuation_time,
                    source=AccountSource(action.source_ref or f"corporate:{action.action_id}", action.announcement_available_time))
            pending_value = account_execution.snapshot_at(state, at=valuation_time)
        closing_nav = pending_value + _nav_units(
            state, day, price_column="close", market=market_id
        )
        valuation_time = datetime.combine(session, time(15), _TIMEZONE)
        if credit_execution is not None:
            before_risk = state
            state, output = credit_execution.check_risk(state, valuation_time, explicit=explicit)
            append_output(output)
            collect_explicit(output[-1][1] if output else before_risk)
            closing_nav -= state.credit_state.principal_units + state.credit_state.interest_units
            credit_execution.snapshot(state, valuation_time)
        if cashflow_execution is not None:
            cashflow_execution.returns.close(session.isoformat(), closing_nav)
        session_trades = trade_rows[session_trade_start:]
        trade_quantity_by_code: dict[str, int] = {}
        trade_cash_change = 0
        for trade in session_trades:
            signed = int(trade["quantity"]) if trade["side"] == "buy" else -int(trade["quantity"])
            trade_quantity_by_code[str(trade["code"])] = (
                trade_quantity_by_code.get(str(trade["code"]), 0) + signed
            )
            cash_delta = int(trade["notional_units"])
            if trade["side"] == "buy":
                cash_delta = -cash_delta
            trade_cash_change += cash_delta - int(trade["fee_units"])
        for code in codes:
            instrument_hash = _instrument_hash(code, market_id)
            lot = next(
                (item for item in state.positions if item.instrument_hash == instrument_hash),
                None,
            )
            quantity = 0 if lot is None else lot.sellable + lot.unsettled + lot.frozen
            if (instrument_hash, session) in needed_records:
                record = CorporateActionRecordPosition(instrument_hash, valuation_time, quantity,
                                                      f"ledger:{state.state_hash}")
                prior = records.get((instrument_hash, session))
                if prior is not None and prior.quantity != quantity:
                    raise SimulationContractError("输入登记持仓与实际收盘数量不一致")
                records[(instrument_hash, session)] = record
            previous = previous_quantities.get(instrument_hash, 0)
            trade_change = trade_quantity_by_code.get(code, 0)
            position_snapshot_rows.append({
                "session": session,
                "valuation_time": valuation_time,
                "code": code,
                "instrument_hash": instrument_hash,
                "quantity": quantity,
                "sellable_quantity": 0 if lot is None else lot.sellable,
                "unsettled_quantity": 0 if lot is None else lot.unsettled,
                "frozen_quantity": 0 if lot is None else lot.frozen,
                "market_value_units": 0 if quantity == 0 else cash_price_amount_units(_price(day.loc[code, "close"]), quantity, cash_scale=2),
                "trade_quantity_change": trade_change,
                "non_trade_quantity_change": quantity - previous - trade_change,
                "source_state_hash": state.state_hash,
            })
            previous_quantities[instrument_hash] = quantity
        previous_cash = (
            _money_units(initial_cash_cny)
            if not cash_snapshot_rows
            else int(cash_snapshot_rows[-1]["total_cash_units"])
        )
        cash_snapshot_rows.append({
            "session": session,
            "valuation_time": valuation_time,
            "total_cash_units": state.total_cash_units,
            "available_cash_units": state.available_cash_units,
            "receivable_cash_units": sum(
                item.cash_units for item in state.cash_receivables
            ),
            "payable_tax_units": state.payable_tax_units,
            "pending_successor_units": pending_value,
            "trade_cash_change_units": trade_cash_change,
            "non_trade_cash_change_units": (
                state.total_cash_units - previous_cash - trade_cash_change
            ),
            "opening_cash_units": _money_units(initial_cash_cny),
            "source_state_hash": state.state_hash,
        })
        nav_rows.append({
            "session": session,
            "nav_units": closing_nav,
            "nav_cny": _money_value(closing_nav),
            "available_cash_cny": _money_value(state.available_cash_units),
            "receivable_cash_cny": _money_value(
                sum(item.cash_units for item in state.cash_receivables)
            ),
            "position_count": sum(
                _held_quantity(state, _instrument_hash(code, market_id)) > 0
                for code in codes
            ),
            "state_hash": state.state_hash,
        })
        return valuation_time

    for lifecycle in engine.run_daily_sessions(
        sessions, before_open=before_open, reconcile=reconcile, after_close=after_close,
    ):
        order_lifecycle_rows.extend(lifecycle)
        if explicit is not None:
            explicit.drain_session()
            explicit_event_index = len(explicit.events)
    if explicit is not None:
        explicit.require_finished()
    if account_execution is not None:
        account_execution.finish(datetime.combine(sessions[-1], time(15), _TIMEZONE))
    credit_context = None if credit_execution is None else credit_execution.finish(state, datetime.combine(sessions[-1], time(15), _TIMEZONE))
    nav = pd.DataFrame(nav_rows)
    cashflow_context = None
    if cashflow_execution is not None:
        cashflow_context = cashflow_execution.context(int(nav.iloc[-1]["nav_units"]))
        metric_rows = []
        if cashflow_context["return_status"] == "applicable":
            metric_rows.extend((
                {"metric_ref": "portfolio.total_return@1.0.0", "value": cashflow_context["total_return"], "unit": "decimal_return"},
                {"metric_ref": "portfolio.max_drawdown@1.0.0", "value": cashflow_context["max_drawdown"], "unit": "decimal_return"},
            ))
        if opening_nav_units > 0:
            metric_rows.append({"metric_ref": "portfolio.turnover@1.0.0", "value": total_turnover / opening_nav_units, "unit": "ratio"})
        metric_rows.append({"metric_ref": "portfolio.transaction_cost@1.0.0", "value": _money_value(total_fees), "unit": "CNY"})
        metrics = pd.DataFrame(metric_rows)
    else:
        running_max = nav["nav_cny"].cummax()
        if account_execution is not None:
            running_max = running_max.clip(lower=_money_value(opening_nav_units))
        drawdowns = nav["nav_cny"] / running_max - 1.0
        metrics = pd.DataFrame((
            {"metric_ref": "portfolio.total_return@1.0.0", "value": nav.iloc[-1]["nav_cny"] / _money_value(opening_nav_units) - 1.0, "unit": "decimal_return"},
            {"metric_ref": "portfolio.max_drawdown@1.0.0", "value": float(drawdowns.min()), "unit": "decimal_return"},
            {"metric_ref": "portfolio.turnover@1.0.0", "value": _money_value(total_turnover) / _money_value(opening_nav_units), "unit": "ratio"},
            {"metric_ref": "portfolio.transaction_cost@1.0.0", "value": _money_value(total_fees), "unit": "CNY"},
        ))

    if explicit is not None:
        terminal_reasons = {row["order_id"]: row["reason"] for row in order_lifecycle_rows}
        for row in explicit_order_rows:
            if row["status"] != "filled" and not row["terminal_reason"]:
                row["terminal_reason"] = terminal_reasons[row["order_id"]]
        order_rows = [{"session": row["session"], "code": row["instrument_id"],
            "side": row["side"], "requested_quantity": row["requested_quantity"],
            "filled_quantity": row["filled_quantity"], "reason_code": row["terminal_reason"],
            "order_id": row["order_id"], "intent_hash": row["source_order_hash"],
            "target_hash": row["source_order_hash"], "rule_hash": ""}
            for row in explicit_order_rows]
    orders = pd.DataFrame(order_rows)
    trades = pd.DataFrame(trade_rows)
    ledger = pd.DataFrame(ledger_rows)
    position_snapshots = pd.DataFrame(position_snapshot_rows)
    cash_snapshots = pd.DataFrame(cash_snapshot_rows)
    identity = {
        "tables": {
            "orders": orders.to_json(orient="records", date_format="iso", double_precision=15),
            "trades": trades.to_json(orient="records", date_format="iso", double_precision=15),
            "ledger": ledger.to_json(orient="records", date_format="iso", double_precision=15),
            "nav": nav.to_json(orient="records", date_format="iso", double_precision=15),
            "metrics": metrics.to_json(orient="records", date_format="iso", double_precision=15),
            "position_snapshots": position_snapshots.to_json(
                orient="records", date_format="iso", double_precision=15
            ),
            "cash_snapshots": cash_snapshots.to_json(
                orient="records", date_format="iso", double_precision=15
            ),
        },
        "order_lifecycle": pd.DataFrame(order_lifecycle_rows).to_json(
            orient="records", date_format="iso", double_precision=15
        ),
        "corporate_action_snapshot_hash": action_hash,
        **({"non_trading_sessions": declaration} if declaration is not None else {}),
        **({"explicit_order_context": explicit_context()} if execution_mode == "explicit_orders" else {}),
        **({"account_context": account_execution.context()} if account_execution is not None else {}),
        **({"credit_context": credit_context} if credit_context is not None else {}),
        **({"external_cashflow_context": cashflow_context} if cashflow_context is not None else {}),
        **({"corporate_action_records": [item.to_dict() for _, item in sorted(records.items())]} if records else {}),
        "backend_id": "cash-daily-v1",
        "fidelity": "bar_level_historical_research",
        "contract_version": DAILY_EVENT_SIMULATION_VERSION,
    }
    return DailyCashSimulationResult(
        orders=orders,
        trades=trades,
        ledger=ledger,
        nav=nav,
        metrics=metrics,
        position_snapshots=position_snapshots,
        cash_snapshots=cash_snapshots,
        simulation_hash=typed_canonical_hash(identity),
        corporate_actions=tuple(corporate_actions),
        order_lifecycle=tuple(order_lifecycle_rows),
        corporate_action_records=tuple(item.to_dict() for _, item in sorted(records.items())),
        account_context=None if account_execution is None else account_execution.context(),
        external_cashflow_context=cashflow_context,
        credit_context=credit_context,
        execution_mode=execution_mode,
        explicit_order_context=explicit_context() if execution_mode == "explicit_orders" else None,
        explicit_order_rows=tuple(explicit_order_rows),
        explicit_fill_rows=tuple(explicit_fill_rows),
        explicit_cost_rows=tuple(explicit_cost_rows),
        explicit_execution_observations=tuple(explicit_execution_observations),
        non_trading_sessions=declaration,
    )


def target_entry_instrument(target: PortfolioTarget, code: str):
    return next(
        (item.instrument for item in target.entries if item.instrument.instrument_id == code),
        None,
    )


def _normalized_execution_date(value: object) -> date:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise SimulationContractError("日频现金计划的 order_time 不是有效时间") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise SimulationContractError("日频现金计划的 order_time 必须带时区")
    return timestamp.tz_convert(_TIMEZONE).date()


def _validate_cash_target(
    target: PortfolioTarget,
    *,
    codes: Sequence[str],
    rule_by_code: Mapping[str, MarketRuleSnapshot],
    market: str,
) -> None:
    if (
        target.target_type != "weight"
        or target.base_currency != "CNY"
        or target.short_allowed
        or target.leverage_limit > 1.0
    ):
        raise SimulationContractError("日频现金目标必须是 CNY long-only 权重目标")
    known = set(codes)
    for entry in target.entries:
        code = entry.instrument.instrument_id
        if code not in known:
            raise SimulationContractError("日频现金目标包含行情范围外标的")
        expected = _instrument(code, market)
        if entry.instrument != expected:
            raise SimulationContractError("日频现金目标资产身份与规则市场不一致")
        if rule_by_code[code].instrument_type != expected.contract_kind:
            raise SimulationContractError("日频现金规则标的类型与目标不一致")


def _portfolio_target_from_json(value: str) -> PortfolioTarget:
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise SimulationContractError("PortfolioTarget JSON 必须是对象")
    return PortfolioTarget.from_dict(payload)


def _canonical_market(frame: pd.DataFrame, columns: Mapping[str, str] | None) -> pd.DataFrame:
    names = {
        "date": "fld_etf_date",
        "code": "fld_etf_code",
        "open": "fld_etf_open",
        "close": "fld_etf_close",
        "high_limit": "fld_etf_high_limit",
        "low_limit": "fld_etf_low_limit",
        "paused": "fld_etf_paused",
    }
    if columns is not None:
        names.update(columns)
    if "visible_capacity" in frame and "visible_capacity" not in names:
        names["visible_capacity"] = "visible_capacity"
    missing = set(names.values()) - set(frame.columns)
    if missing:
        raise SimulationContractError(f"现货日频仿真行情缺少字段: {sorted(missing)}")
    result = frame[list(names.values())].copy()
    result.columns = list(names)
    result["date"] = pd.to_datetime(result["date"], errors="raise").dt.date
    result["code"] = result["code"].astype(str)
    if result.duplicated(["date", "code"]).any():
        raise SimulationContractError("现货日频仿真行情存在重复 date/code")
    for field in ("open", "close", "high_limit", "low_limit"):
        result[field] = pd.to_numeric(result[field], errors="raise")
    if result[["open", "close"]].isna().any().any():
        raise SimulationContractError("现货日频仿真价格不能缺失")
    if "visible_capacity" in result:
        for value in result["visible_capacity"]:
            if pd.isna(value) or isinstance(value, bool) or int(value) != value or int(value) < 0:
                raise SimulationContractError("开盘 visible_capacity 必须为非负整数")
    return result.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)


def _opening_capacity(row: pd.Series) -> int:
    """仅消费开盘已声明容量，不以日终成交量替代。"""
    return int(row.get("visible_capacity", 2**62))


def _nav_units(
    state: SpotLedgerState,
    day: pd.DataFrame,
    *,
    price_column: str,
    market: str,
) -> int:
    prices = {
        _instrument_hash(str(code), market): _price(row[price_column])
        for code, row in day.iterrows()
    }
    return cash_daily_nav_units(state, prices)


def _held_quantity(state: SpotLedgerState, instrument_hash: str) -> int:
    lot = next((item for item in state.positions if item.instrument_hash == instrument_hash), None)
    return 0 if lot is None else lot.sellable + lot.unsettled + lot.frozen


def _instrument(code: str, market: str):
    from research_pipeline.domain.trading import InstrumentKey

    _, separator, venue = code.rpartition(".")
    if separator != "." or venue not in {"XSHG", "XSHE", "XBSE"}:
        raise SimulationContractError("现货代码必须带交易所后缀")
    return InstrumentKey(
        code,
        market,
        venue,
        "CNY",
        "stock" if market == "cn_stock" else "etf",
    )


def _instrument_hash(code: str, market: str) -> str:
    return _instrument(code, market).instrument_hash


def _price(value: object) -> Price:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0:
        raise SimulationContractError("价格必须为有限正数")
    return Price.from_decimal(value, scale=DAILY_CASH_PRICE_SCALE, currency="CNY")


def _money_units(value: float) -> int:
    return Money.from_decimal(value, scale=2, currency="CNY").units


def _money_value(value: int) -> float:
    return value / 100.0


def _ledger_event_row(event: FinancialEvent, state: SpotLedgerState) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "kind": event.kind,
        "effective_time": event.effective_time,
        "session": event.session,
        "parent_id": event.parent_id,
        "payload_hash": event.event_hash,
        "state_hash": state.state_hash,
        "available_cash_units": state.available_cash_units,
        "total_cash_units": state.total_cash_units,
    }


def _require_hash(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise SimulationContractError(f"{field} 必须是 sha256")
    return value


__all__ = [
    "DAILY_EVENT_SIMULATION_VERSION",
    "DailyCashSimulationResult",
    "run_daily_cash_event_simulation",
]
