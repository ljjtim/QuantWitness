"""只依赖公共 Target、OrderIntent 与现货账本的日频事件仿真。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import json
import math
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from research_pipeline.domain import CorporateAction, MarketRuleSnapshot, Price
from research_pipeline.domain.trading import OrderIntent, PortfolioTarget, TradingRuleBinding
from research_pipeline.platform import typed_canonical_hash

from .cash_market import (
    OpeningSnapshot,
    apply_cash_corporate_actions,
    cash_daily_nav_units,
    cash_daily_preopen_at,
    cash_position_quantities,
    cash_rebalance_deltas,
    cash_target_quantity,
    execute_cash_order,
    settle_cash_daily_open,
)
from .cn_etf import etf_policy_from_rule
from .cn_stock import stock_policy_from_rule
from .corporate_actions import corporate_action_snapshot_hash
from .events import FinancialEvent
from .intent_port import CASH_DAILY_BACKEND, IntentToOrderPort
from .ledger import ExecutionGroup, SpotLedgerState, reduce_spot
from .orders import SimulationContractError


DAILY_EVENT_SIMULATION_VERSION = "research-daily-event-simulation-v2"
_TIMEZONE = ZoneInfo("Asia/Shanghai")
_PRICE_SCALE = 2


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
    backend_id: str = "cash-daily-v1"
    fidelity: str = "bar_level_historical_research"
    limitations: tuple[str, ...] = (
        "日线开盘事件成交，不模拟 tick、排队位置或实盘成交概率",
        "佣金是显式研究成本假设，不代表所有券商账户",
    )
    contract_version: str = DAILY_EVENT_SIMULATION_VERSION


def run_daily_cash_event_simulation(
    *,
    market: pd.DataFrame,
    intent_plans: pd.DataFrame,
    corporate_actions: Sequence[CorporateAction],
    rule_by_code: Mapping[str, MarketRuleSnapshot],
    initial_cash_cny: float,
    market_data_artifact_hash: str,
    columns: Mapping[str, str] | None = None,
) -> DailyCashSimulationResult:
    """按结算→公司行动→卖出→买入→收盘估值顺序重放日线事件。"""
    if not math.isfinite(initial_cash_cny) or initial_cash_cny <= 0:
        raise SimulationContractError("initial_cash_cny 必须为正")
    _require_hash(market_data_artifact_hash, "market_data_artifact_hash")
    frame = _canonical_market(market, columns)
    codes = tuple(sorted(frame["code"].unique()))
    if set(rule_by_code) != set(codes):
        raise SimulationContractError("现货规则快照必须与行情标的一一对应")
    markets = {rule.market for rule in rule_by_code.values()}
    if len(markets) != 1 or next(iter(markets)) not in {"cn_stock", "cn_etf"}:
        raise SimulationContractError("日频现金仿真必须使用唯一股票或 ETF 市场")
    market_id = next(iter(markets))
    policy_factory = (
        stock_policy_from_rule if market_id == "cn_stock" else etf_policy_from_rule
    )
    policies = {code: policy_factory(rule_by_code[code]) for code in codes}
    action_hash = corporate_action_snapshot_hash(corporate_actions)
    state = SpotLedgerState(
        ExecutionGroup(
            f"{market_id}-cny-daily",
            market_id,
            "CNY",
            "mixed-t0-t1" if market_id == "cn_etf" else "t1",
        ),
        _money_units(initial_cash_cny),
    )
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
    nav_rows: list[dict[str, object]] = []
    position_snapshot_rows: list[dict[str, object]] = []
    cash_snapshot_rows: list[dict[str, object]] = []
    total_fees = 0
    total_turnover = 0
    previous_quantities: dict[str, int] = {}
    sessions = tuple(sorted(frame["date"].unique()))
    port = IntentToOrderPort(CASH_DAILY_BACKEND)
    for session in sessions:
        session_trade_start = len(trade_rows)
        opening_time = datetime.combine(session, time(9, 30), _TIMEZONE)
        preopen_time = cash_daily_preopen_at(session)
        settlement_rule_hash = typed_canonical_hash({
            "market": market_id,
            "policy": "cash-daily-settlement-v1",
        })
        before = state
        state, settlement_events = settle_cash_daily_open(
            state,
            effective_time=preopen_time,
            rule_hash=settlement_rule_hash,
        )
        replay = before
        for event in settlement_events:
            replay = reduce_spot(replay, event)
            ledger_rows.append(_ledger_event_row(event, replay))
        before = state
        state, action_events = apply_cash_corporate_actions(
            state,
            action_by_date.get(session, ()),
            effective_time=preopen_time,
            rule_hash=typed_canonical_hash({
                "market": market_id,
                "policy": "cash-daily-corporate-action-v1",
            }),
        )
        replay = before
        for event in action_events:
            replay = reduce_spot(replay, event)
            ledger_rows.append(_ledger_event_row(event, replay))
        day = frame.loc[frame["date"] == session].set_index("code")
        plan_row = plans.get(session)
        if plan_row is not None:
            target = _portfolio_target_from_json(str(plan_row.target_json))
            _validate_cash_target(
                target,
                codes=codes,
                rule_by_code=rule_by_code,
                market=market_id,
            )
            if pd.Timestamp(plan_row.order_time).to_pydatetime() != opening_time:
                raise SimulationContractError("OrderIntent plan 不是下一交易日开盘事件")
            nav_at_open = _nav_units(
                state, day, price_column="open", market=market_id
            )
            weights = {
                item.instrument.instrument_id: float(item.value)
                for item in target.entries
            }
            desired = {
                code: cash_target_quantity(
                    nav_units=nav_at_open,
                    target_weight=weights.get(code, 0.0),
                    price_units=_price_units(day.loc[code, "open"]),
                    lot_size=policies[code].lot_size,
                )
                for code in codes
            }
            current = cash_position_quantities(
                state,
                {code: _instrument_hash(code, market_id) for code in codes},
            )
            sequence = cash_rebalance_deltas(current, desired)
            for ordinal, (code, side, quantity) in enumerate(sequence):
                instrument = target_entry_instrument(target, code)
                if instrument is None:
                    instrument = _instrument(code, market_id)
                binding = TradingRuleBinding(
                    instrument.instrument_hash,
                    rule_by_code[code].content_hash,
                    rule_by_code[code].available_time,
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
                    Price(_price_units(raw["open"]), _PRICE_SCALE, "CNY"),
                    Price(_price_units(raw["high_limit"]), _PRICE_SCALE, "CNY"),
                    Price(_price_units(raw["low_limit"]), _PRICE_SCALE, "CNY"),
                    bool(raw["paused"]),
                    order.quantity,
                    opening_time,
                )
                result = execute_cash_order(
                    order,
                    policy=policies[code],
                    snapshot=snapshot,
                    state=state,
                    execution_at=opening_time,
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
                            "price_cny": _money_value(notional) / int(values["quantity"]),
                            "order_id": order.order_id,
                            "intent_hash": intent.intent_hash,
                        })
        closing_nav = _nav_units(
            state, day, price_column="close", market=market_id
        )
        valuation_time = datetime.combine(session, time(15), _TIMEZONE)
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
                "market_value_units": quantity * _price_units(day.loc[code, "close"]),
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
    nav = pd.DataFrame(nav_rows)
    running_max = nav["nav_cny"].cummax()
    drawdowns = nav["nav_cny"] / running_max - 1.0
    metrics = pd.DataFrame((
        {"metric_ref": "portfolio.total_return@1.0.0", "value": nav.iloc[-1]["nav_cny"] / initial_cash_cny - 1.0, "unit": "decimal_return"},
        {"metric_ref": "portfolio.max_drawdown@1.0.0", "value": float(drawdowns.min()), "unit": "decimal_return"},
        {"metric_ref": "portfolio.turnover@1.0.0", "value": _money_value(total_turnover) / initial_cash_cny, "unit": "ratio"},
        {"metric_ref": "portfolio.transaction_cost@1.0.0", "value": _money_value(total_fees), "unit": "CNY"},
    ))
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
        "corporate_action_snapshot_hash": action_hash,
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
    if result[["open", "close", "high_limit", "low_limit"]].isna().any().any():
        raise SimulationContractError("现货日频仿真价格不能缺失")
    return result.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)


def _nav_units(
    state: SpotLedgerState,
    day: pd.DataFrame,
    *,
    price_column: str,
    market: str,
) -> int:
    prices = {
        _instrument_hash(str(code), market): _price_units(row[price_column])
        for code, row in day.iterrows()
    }
    return cash_daily_nav_units(state, prices)


def _held_quantity(state: SpotLedgerState, instrument_hash: str) -> int:
    lot = next((item for item in state.positions if item.instrument_hash == instrument_hash), None)
    return 0 if lot is None else lot.sellable + lot.unsettled + lot.frozen


def _instrument(code: str, market: str):
    from research_pipeline.domain.trading import InstrumentKey

    _, separator, venue = code.rpartition(".")
    if separator != "." or venue not in {"XSHG", "XSHE"}:
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


def _price_units(value: object) -> int:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0:
        raise SimulationContractError("现货日频价格必须为正有限数")
    return int(round(numeric * 100))


def _money_units(value: float) -> int:
    return int(round(value * 100))


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
