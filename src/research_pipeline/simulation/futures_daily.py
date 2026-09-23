"""实际合约期货日频事件仿真、规则解析与逐日盯市账本。"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from research_pipeline.domain.trading import (
    InstrumentKey,
    OrderIntent,
    PortfolioTarget,
    TradingRuleBinding,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.simulation.intent_port import CN_FUTURES_DAILY_BACKEND, IntentToOrderPort
from research_pipeline.simulation.orders import SimulationContractError


FUTURES_DAILY_SIMULATION_VERSION = "research-futures-daily-simulation-v1"
FUTURES_DYNAMIC_ROLL_SIMULATION_VERSION = "research-futures-dynamic-roll-simulation-v2"
FUTURES_PORTFOLIO_SIMULATION_VERSION = "research-futures-portfolio-simulation-v1"
_TIMEZONE = ZoneInfo("Asia/Shanghai")
_CENT = Decimal("0.01")
_FEN = Decimal("1")


@dataclass(frozen=True)
class MarginOverride:
    effective_from: date
    effective_to: date
    available_at: datetime
    speculation_rate_pct: Decimal
    source_hash: str

    def __post_init__(self) -> None:
        if self.effective_from > self.effective_to:
            raise SimulationContractError("保证金公告生效区间倒置")
        if self.available_at.tzinfo is None or self.speculation_rate_pct <= 0:
            raise SimulationContractError("保证金公告时间或费率无效")
        _hash(self.source_hash, "保证金公告 source_hash")


@dataclass(frozen=True)
class FuturesExecutionRule:
    contract_code: str
    trading_date: date
    multiplier: int
    margin_rate_pct: Decimal
    open_fee_permyriad: Decimal
    close_fee_permyriad: Decimal
    close_today_fee_permyriad: Decimal
    available_at: datetime
    multiplier_hash: str
    margin_hash: str
    fee_hash: str
    settlement_policy_hash: str
    lifecycle_hash: str
    fee_unit: str = "notional_permyriad"

    @property
    def rule_snapshot_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_code": self.contract_code,
            "trading_date": self.trading_date.isoformat(),
            "multiplier": self.multiplier,
            "margin_rate_pct": str(self.margin_rate_pct),
            "open_fee_permyriad": str(self.open_fee_permyriad),
            "close_fee_permyriad": str(self.close_fee_permyriad),
            "close_today_fee_permyriad": str(self.close_today_fee_permyriad),
            "fee_unit": self.fee_unit,
            "available_at": self.available_at.isoformat(),
            "multiplier_hash": self.multiplier_hash,
            "margin_hash": self.margin_hash,
            "fee_hash": self.fee_hash,
            "settlement_policy_hash": self.settlement_policy_hash,
            "lifecycle_hash": self.lifecycle_hash,
        }


class FuturesRuleBook:
    """按应用时点解析唯一真实规则；缺失、重叠和未来规则全部拒绝。"""

    def __init__(
        self,
        *,
        lifecycle: pd.DataFrame,
        multipliers: pd.DataFrame,
        margins: pd.DataFrame,
        fees: pd.DataFrame,
        settlement_policy_hash: str,
        margin_overrides: Sequence[MarginOverride] = (),
    ) -> None:
        _hash(settlement_policy_hash, "settlement_policy_hash")
        self.lifecycle = lifecycle.copy()
        self.multipliers = multipliers.copy()
        self.margins = margins.copy()
        self.fees = fees.copy()
        self.settlement_policy_hash = settlement_policy_hash
        self.margin_overrides = tuple(margin_overrides)
        self._normalize()

    def resolve_order(self, *, contract_code: str, trading_date: date, as_of: datetime) -> FuturesExecutionRule:
        if as_of.tzinfo is None:
            raise SimulationContractError("规则应用时点必须带时区")
        lifecycle, lifecycle_hash = self._lifecycle(contract_code, trading_date)
        del lifecycle
        multiplier, multiplier_hash, multiplier_available = self._multiplier(contract_code, trading_date)
        margin_rate, margin_hash, margin_available = self._order_margin(contract_code, trading_date, as_of)
        fees, fee_unit, fee_hash, fee_available = self._order_fee(contract_code, trading_date, as_of)
        available_at = max(multiplier_available, margin_available, fee_available)
        if available_at > as_of:
            raise SimulationContractError("期货规则在下单时点尚不可见")
        return FuturesExecutionRule(
            contract_code,
            trading_date,
            multiplier,
            margin_rate,
            fees[0],
            fees[1],
            fees[2],
            available_at,
            multiplier_hash,
            margin_hash,
            fee_hash,
            self.settlement_policy_hash,
            lifecycle_hash,
            fee_unit,
        )

    def settlement_margin(self, *, contract_code: str, trading_date: date, as_of: datetime) -> tuple[Decimal, str]:
        rows = self.margins.loc[
            (self.margins["code"] == contract_code)
            & (self.margins["day"] == trading_date)
            & (self.margins["addTime"] <= _naive_local(as_of))
            & (self.margins["modTime"] <= _naive_local(as_of))
        ]
        if len(rows) != 1:
            raise SimulationContractError("当日收盘结算保证金规则缺失或重叠")
        row = rows.iloc[0]
        rate = _positive_decimal(row["specul_buy_margin_rate"], "结算保证金率")
        return rate, typed_canonical_hash(_row_payload(row, self.margins.columns))

    def _normalize(self) -> None:
        required = {
            "lifecycle": (self.lifecycle, {"code", "start_date", "end_date"}),
            "multiplier": (self.multipliers, {"underlying_symbol", "exchange", "contract_multiplier", "effective_date", "cancel_date"}),
            "margin": (self.margins, {"day", "code", "specul_buy_margin_rate", "addTime", "modTime"}),
            "fee": (self.fees, {"day", "code", "unit", "clearance_charge", "opening_charge", "short_clearance_charge", "addTime", "modTime"}),
        }
        for name, (frame, fields) in required.items():
            missing = fields - set(frame.columns)
            if missing:
                raise SimulationContractError(f"{name} 规则缺少字段: {sorted(missing)}")
        for frame, fields in (
            (self.lifecycle, ("start_date", "end_date")),
            (self.multipliers, ("effective_date", "cancel_date")),
            (self.margins, ("day",)),
            (self.fees, ("day",)),
        ):
            for field in fields:
                frame[field] = pd.to_datetime(frame[field]).dt.date
        for frame in (self.margins, self.fees):
            for field in ("addTime", "modTime"):
                frame[field] = pd.to_datetime(frame[field])

    def _lifecycle(self, code: str, trading_date: date) -> tuple[pd.Series, str]:
        rows = self.lifecycle.loc[
            (self.lifecycle["code"] == code)
            & (self.lifecycle["start_date"] <= trading_date)
            & (self.lifecycle["end_date"] >= trading_date)
        ]
        if len(rows) != 1:
            raise SimulationContractError("实际期货合约生命周期缺失或重叠")
        row = rows.iloc[0]
        return row, typed_canonical_hash(_row_payload(row, self.lifecycle.columns))

    def _multiplier(self, code: str, trading_date: date) -> tuple[int, str, datetime]:
        symbol = _underlying_symbol(code)
        rows = self.multipliers.loc[
            (self.multipliers["underlying_symbol"].astype(str).str.upper() == symbol)
            & (self.multipliers["effective_date"] <= trading_date)
            & (self.multipliers["cancel_date"] >= trading_date)
        ]
        if len(rows) != 1:
            raise SimulationContractError("合约乘数区间缺失或重叠；禁止使用默认乘数")
        row = rows.iloc[0]
        value = Decimal(str(row["contract_multiplier"]))
        if value != value.to_integral_value() or value <= 0:
            raise SimulationContractError("合约乘数必须是正整数")
        available = datetime.combine(row["effective_date"], time.min, _TIMEZONE)
        return int(value), typed_canonical_hash(_row_payload(row, self.multipliers.columns)), available

    def _order_margin(self, code: str, trading_date: date, as_of: datetime) -> tuple[Decimal, str, datetime]:
        overrides = tuple(
            item for item in self.margin_overrides
            if item.effective_from <= trading_date <= item.effective_to and item.available_at <= as_of
        )
        if len(overrides) > 1:
            raise SimulationContractError("保证金事前公告规则重叠")
        if overrides:
            item = overrides[0]
            payload = {
                "effective_from": item.effective_from.isoformat(),
                "effective_to": item.effective_to.isoformat(),
                "available_at": item.available_at.isoformat(),
                "speculation_rate_pct": str(item.speculation_rate_pct),
                "source_hash": item.source_hash,
            }
            return item.speculation_rate_pct, typed_canonical_hash(payload), item.available_at
        rows = self.margins.loc[
            (self.margins["code"] == code)
            & (self.margins["day"] <= trading_date)
            & (self.margins["addTime"] <= _naive_local(as_of))
            & (self.margins["modTime"] <= _naive_local(as_of))
        ].sort_values(["addTime", "id"], kind="mergesort")
        if rows.empty:
            raise SimulationContractError("下单前没有可见保证金规则")
        latest_day = rows["day"].max()
        day_rows = rows.loc[rows["day"] == latest_day]
        latest_time = day_rows["addTime"].max()
        latest = day_rows.loc[day_rows["addTime"] == latest_time]
        if len(latest) != 1:
            raise SimulationContractError("下单保证金规则重叠")
        row = latest.iloc[0]
        rate = _positive_decimal(row["specul_buy_margin_rate"], "下单保证金率")
        available = _aware_local(row["addTime"])
        return rate, typed_canonical_hash(_row_payload(row, self.margins.columns)), available

    def _order_fee(self, code: str, trading_date: date, as_of: datetime) -> tuple[tuple[Decimal, Decimal, Decimal], str, str, datetime]:
        rows = self.fees.loc[
            (self.fees["code"] == code)
            & (self.fees["day"] == trading_date)
            & (self.fees["addTime"] <= _naive_local(as_of))
            & (self.fees["modTime"] <= _naive_local(as_of))
        ]
        if len(rows) != 1:
            raise SimulationContractError("下单手续费规则缺失或重叠")
        row = rows.iloc[0]
        raw_unit = str(row["unit"]).strip()
        if raw_unit == "‱":
            fee_unit = "notional_permyriad"
        elif raw_unit in {"元/手", "元／手", "元/张", "元／张", "CNY/lot", "per_lot"}:
            fee_unit = "per_lot_cny"
        else:
            raise SimulationContractError("期货手续费单位只支持成交额万分比或每手人民币")
        common = _nonnegative_decimal(row["clearance_charge"], "普通手续费")
        opening = common if pd.isna(row["opening_charge"]) else _nonnegative_decimal(row["opening_charge"], "开仓手续费")
        close_today = common if pd.isna(row["short_clearance_charge"]) else _nonnegative_decimal(row["short_clearance_charge"], "平今手续费")
        available = _aware_local(row["addTime"])
        return (opening, common, close_today), fee_unit, typed_canonical_hash(_row_payload(row, self.fees.columns)), available


@dataclass(frozen=True)
class FuturesSimulationResult:
    intents: pd.DataFrame
    fills: pd.DataFrame
    settlements: pd.DataFrame
    nav: pd.DataFrame
    rule_snapshots: pd.DataFrame
    backend_id: str = "cn-futures-daily-v1"
    fidelity: str = "bar_level_historical_research"
    contract_version: str = FUTURES_DAILY_SIMULATION_VERSION
    rejections: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)
    contributions: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)
    portfolio: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)

    @property
    def result_hash(self) -> str:
        return typed_canonical_hash({
            "intents": _frame_payload(self.intents),
            "fills": _frame_payload(self.fills),
            "settlements": _frame_payload(self.settlements),
            "nav": _frame_payload(self.nav),
            "rule_snapshots": _frame_payload(self.rule_snapshots),
            "backend_id": self.backend_id,
            "fidelity": self.fidelity,
            "contract_version": self.contract_version,
            "rejections": _frame_payload(self.rejections),
            "contributions": _frame_payload(self.contributions),
            "portfolio": _frame_payload(self.portfolio),
        })


def build_futures_order_intents(
    targets: pd.DataFrame,
    *,
    execution_sessions: Sequence[date],
    market_data_artifact_hash: str,
    rule_book: FuturesRuleBook,
) -> pd.DataFrame:
    """生成显式开平仓 OrderIntent；普通日频调仓不会把历史仓伪装成平今仓。"""
    return build_futures_roll_order_intents(
        targets,
        execution_sessions=execution_sessions,
        market_data_artifact_hash=market_data_artifact_hash,
        rule_book=rule_book,
    )


def build_futures_roll_order_intents(
    targets: pd.DataFrame,
    *,
    execution_sessions: Sequence[date],
    market_data_artifact_hash: str,
    rule_book: FuturesRuleBook,
    execution_times: Mapping[date, datetime] | None = None,
) -> pd.DataFrame:
    """按动态真实合约生成订单；换月必须先平旧腿、再开新腿。"""

    _hash(market_data_artifact_hash, "market_data_artifact_hash")
    sessions = tuple(sorted(set(execution_sessions)))
    rows = []
    previous_quantity = 0
    previous_instrument: InstrumentKey | None = None
    for target_sequence, target_row in enumerate(
        targets.sort_values("session", kind="mergesort").itertuples(index=False)
    ):
        later = tuple(item for item in sessions if item > target_row.session)
        if not later:
            raise SimulationContractError("期货目标缺少下一交易日执行事件")
        execution_date = later[0]
        declared_execution_session = getattr(target_row, "execution_session", None)
        if declared_execution_session is not None and pd.Timestamp(
            declared_execution_session
        ).date() != execution_date:
            raise SimulationContractError("期货目标执行会话与正式映射漂移")
        order_time = (
            datetime.combine(execution_date, time(9, 0), _TIMEZONE)
            if execution_times is None
            else execution_times.get(execution_date)
        )
        if order_time is None or order_time.tzinfo is None:
            raise SimulationContractError("期货目标缺少正式带时区执行时点")
        declared_execution_time = getattr(target_row, "execution_time", None)
        if declared_execution_time is not None and pd.Timestamp(
            declared_execution_time
        ).to_pydatetime() != order_time:
            raise SimulationContractError("期货目标执行时点与正式映射漂移")
        target_payload = json.loads(str(target_row.target_json))
        if not isinstance(target_payload, Mapping):
            raise SimulationContractError("PortfolioTarget JSON 必须是对象")
        target = PortfolioTarget.from_dict(target_payload)
        target_quantity = int(target_row.target_quantity)
        instrument = _target_instrument(target, target_row)
        if (
            previous_instrument is not None
            and previous_instrument.instrument_id != instrument.instrument_id
        ):
            legs: tuple[tuple[InstrumentKey, str, int, str], ...] = tuple(
                item
                for item in (
                    (
                        previous_instrument,
                        "sell" if previous_quantity > 0 else "buy",
                        abs(previous_quantity),
                        "close_yesterday",
                    )
                    if previous_quantity
                    else None,
                    (
                        instrument,
                        "buy" if target_quantity > 0 else "sell",
                        abs(target_quantity),
                        "open",
                    )
                    if target_quantity
                    else None,
                )
                if item is not None
            )
        else:
            legs = tuple(
                (instrument, side, quantity, position_effect)
                for side, quantity, position_effect in _target_legs(
                    previous_quantity, target_quantity
                )
            )
        for ordinal, (leg_instrument, side, quantity, position_effect) in enumerate(legs):
            rule = rule_book.resolve_order(
                contract_code=leg_instrument.instrument_id,
                trading_date=execution_date,
                as_of=order_time,
            )
            binding = TradingRuleBinding(
                instrument_hash=leg_instrument.instrument_hash,
                rule_snapshot_hash=rule.rule_snapshot_hash,
                available_at=rule.available_at,
                multiplier_rule_hash=rule.multiplier_hash,
                fee_rule_hash=rule.fee_hash,
                margin_rule_hash=rule.margin_hash,
                settlement_rule_hash=rule.settlement_policy_hash,
            )
            intent = OrderIntent(
                instrument=leg_instrument,
                side=side,
                quantity=quantity,
                position_effect=position_effect,
                decision_time=target.decision_time,
                order_time=order_time,
                portfolio_target_hash=target.target_hash,
                market_data_artifact_hash=market_data_artifact_hash,
                rule_binding=binding,
                source_hashes=tuple(sorted({target.target_hash, rule.rule_snapshot_hash})),
            )
            order = IntentToOrderPort(CN_FUTURES_DAILY_BACKEND).to_order(intent, ordinal=ordinal)
            rows.append({
                "execution_session": execution_date,
                "intent_hash": intent.intent_hash,
                "intent_json": json.dumps(intent.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                "order_id": order.order_id,
                "actual_contract": leg_instrument.instrument_id,
                "side": side,
                "quantity": quantity,
                "position_effect": position_effect,
                "target_sequence": target_sequence,
                "leg_ordinal": ordinal,
                "target_hash": target.target_hash,
                "rule_snapshot_hash": rule.rule_snapshot_hash,
            })
        previous_quantity = target_quantity
        previous_instrument = instrument
    if not rows:
        return pd.DataFrame(columns=(
            "execution_session", "intent_hash", "intent_json", "order_id",
            "actual_contract", "side", "quantity", "position_effect",
            "target_sequence", "leg_ordinal", "target_hash", "rule_snapshot_hash",
        ))
    return pd.DataFrame(rows).sort_values(
        ["execution_session", "target_sequence", "leg_ordinal"], kind="mergesort"
    ).reset_index(drop=True)


def _target_instrument(
    target: PortfolioTarget,
    target_row: object,
) -> InstrumentKey:
    if target.entries:
        return target.entries[0].instrument
    code = getattr(target_row, "actual_contract", None)
    if not isinstance(code, str) or "." not in code:
        raise SimulationContractError("空目标缺少主动实际合约身份")
    instrument_id, venue = code.rsplit(".", 1)
    if instrument_id.endswith(("8888", "9999")):
        raise SimulationContractError("连续合约不能进入订单意图")
    return InstrumentKey(code, "cn_future", venue, "CNY", "future_contract")


def run_futures_daily_simulation(
    *,
    market: pd.DataFrame,
    settlements: pd.DataFrame,
    intents: pd.DataFrame,
    rule_book: FuturesRuleBook,
    initial_cash_cny: Decimal,
    active_contract_by_session: Mapping[date, str] | None = None,
    slippage_ticks: int = 0,
    tick_size_by_session_contract: Mapping[tuple[date, str], Decimal] | None = None,
) -> FuturesSimulationResult:
    """按订单、成交、收盘结算顺序重放单品种期货账本。

    ``active_contract_by_session`` 为空时使用每日唯一实际合约模式；
    传入时允许同日存在多个候选合约，但只有映射指定的合约可成为收盘持仓。
    """
    if initial_cash_cny <= 0:
        raise SimulationContractError("初始资金必须为正")
    if type(slippage_ticks) is not int or slippage_ticks < 0:
        raise SimulationContractError("slippage_ticks 必须是非负整数")
    market = _normalize_market(market)
    settlement = _normalize_settlement(settlements)
    if market[["date", "code"]].duplicated().any() or settlement[["date", "code"]].duplicated().any():
        raise SimulationContractError("期货行情或结算存在重复主键")
    cash_fen = cny_to_fen(initial_cash_cny)
    position = 0
    position_contract: str | None = None
    basis_price: Decimal | None = None
    opened_today = 0
    fill_rows = []
    rejection_rows = []
    settle_rows = []
    nav_rows = []
    rule_rows = []
    last_settlements: dict[str, Decimal] = {}
    intent_frame = intents.copy()
    market_sessions = set(market["date"])
    if active_contract_by_session is None:
        sessions = sorted(market_sessions)
    else:
        normalized_mapping = {
            pd.Timestamp(session).date(): str(contract)
            for session, contract in active_contract_by_session.items()
        }
        if set(normalized_mapping) != market_sessions:
            raise SimulationContractError("主动合约映射必须与执行行情交易日完全一致")
        if any(not _actual_future_contract(code) for code in normalized_mapping.values()):
            raise SimulationContractError("主动合约映射只能引用真实期货合约")
        sessions = sorted(normalized_mapping)
    for session in sessions:
        day_market = market.loc[market["date"] == session]
        if active_contract_by_session is None:
            if len(day_market) != 1:
                raise SimulationContractError("期货每个交易日必须唯一实际合约行情")
            contract = str(day_market.iloc[0]["code"])
        else:
            contract = normalized_mapping[session]
        selected_rows = day_market.loc[day_market["code"] == contract]
        if len(selected_rows) != 1:
            raise SimulationContractError("主动合约执行行情缺失或重叠")
        settlement_time = datetime.combine(session, time(17, 0), _TIMEZONE)
        rule = rule_book.resolve_order(
            contract_code=contract,
            trading_date=session,
            as_of=settlement_time,
        )
        day_intents = intent_frame.loc[intent_frame["execution_session"] == session]
        opened_today = 0
        blocked_target_sequences: set[int] = set()
        for raw in day_intents.itertuples(index=False):
            intent = OrderIntent.from_dict(json.loads(raw.intent_json))
            intent.rule_binding.require_for(intent.instrument, application_time=intent.order_time)
            if intent.order_time > settlement_time:
                raise SimulationContractError("订单执行时点晚于当日结算时点")
            leg_contract = intent.instrument.instrument_id
            if not _actual_future_contract(leg_contract) or intent.instrument.contract_kind != "future_contract":
                raise SimulationContractError("成交必须引用真实期货合约")
            leg_rows = day_market.loc[day_market["code"] == leg_contract]
            if len(leg_rows) != 1:
                raise SimulationContractError("订单腿执行行情缺失或重叠")
            target_sequence = int(getattr(raw, "target_sequence", -1))
            rejection_reason = (
                "prior_leg_unfilled"
                if target_sequence in blocked_target_sequences
                else _futures_unfilled_reason(leg_rows.iloc[0], side=intent.side)
            )
            if rejection_reason is not None:
                rejection_rows.append({
                    "order_id": raw.order_id,
                    "trading_date": session,
                    "actual_contract": leg_contract,
                    "side": intent.side,
                    "quantity": intent.quantity,
                    "position_effect": intent.position_effect,
                    "reason_code": rejection_reason,
                    "intent_hash": intent.intent_hash,
                    "source_target_hash": intent.portfolio_target_hash,
                    "decision_time": intent.decision_time,
                    "order_time": intent.order_time,
                })
                if intent.position_effect in {"close", "close_yesterday", "close_today"}:
                    blocked_target_sequences.add(target_sequence)
                continue
            raw_open = _positive_decimal(leg_rows.iloc[0]["open"], "订单腿开盘价")
            leg_open = raw_open
            if slippage_ticks:
                if tick_size_by_session_contract is None:
                    raise SimulationContractError("非零滑点必须绑定可信 tick size")
                tick_size = tick_size_by_session_contract.get((session, leg_contract))
                if tick_size is None:
                    raise SimulationContractError("订单腿缺少执行日 tick size")
                tick = _positive_decimal(tick_size, "tick size")
                leg_open = raw_open + (tick * slippage_ticks if intent.side == "buy" else -tick * slippage_ticks)
                if leg_open <= 0:
                    raise SimulationContractError("tick 滑点后的执行价必须为正")
            leg_rule = rule_book.resolve_order(
                contract_code=leg_contract,
                trading_date=session,
                as_of=intent.order_time,
            )
            if intent.rule_binding.rule_snapshot_hash != leg_rule.rule_snapshot_hash:
                raise SimulationContractError("订单规则快照与执行日规则漂移")
            signed = intent.quantity if intent.side == "buy" else -intent.quantity
            position_before = position
            basis_before = basis_price
            realized_fen = 0
            if intent.position_effect == "open":
                if position_contract not in {None, leg_contract}:
                    raise SimulationContractError("换月必须先平旧合约再开新合约")
                if position and (position > 0) != (signed > 0):
                    raise SimulationContractError("open 不能隐式平掉反向持仓")
                if position == 0:
                    basis_price = leg_open
                    position_contract = leg_contract
                elif basis_price != leg_open:
                    basis_price = (
                        basis_price * abs(position) + leg_open * abs(signed)
                    ) / Decimal(abs(position + signed))
                position += signed
                opened_today += abs(signed)
            elif intent.position_effect in {"close", "close_yesterday", "close_today"}:
                if position_contract != leg_contract:
                    raise SimulationContractError("平仓腿与当前持仓合约不一致")
                if position == 0 or (position > 0) == (signed > 0):
                    raise SimulationContractError("平仓方向或持仓无效")
                if intent.quantity > abs(position):
                    raise SimulationContractError("平仓数量超过持仓")
                if intent.position_effect == "close_today" and intent.quantity > opened_today:
                    raise SimulationContractError("close_today 只能消费同交易日新仓")
                if intent.position_effect == "close_yesterday" and intent.quantity > abs(position) - opened_today:
                    raise SimulationContractError("close_yesterday 不能消费同交易日新仓")
                realized_fen = _pnl_fen(
                    side=1 if position > 0 else -1,
                    quantity=intent.quantity,
                    from_price=basis_price,
                    to_price=leg_open,
                    multiplier=leg_rule.multiplier,
                )
                cash_fen += realized_fen
                position += signed
                if intent.position_effect == "close_today":
                    opened_today -= intent.quantity
                if position == 0:
                    basis_price = None
                    position_contract = None
                    opened_today = 0
            else:
                raise SimulationContractError("期货 position_effect 不受支持")
            fee_rate = {
                "open": leg_rule.open_fee_permyriad,
                "close": leg_rule.close_fee_permyriad,
                "close_yesterday": leg_rule.close_fee_permyriad,
                "close_today": leg_rule.close_today_fee_permyriad,
            }[intent.position_effect]
            fee_fen = _fee_fen(
                leg_open, leg_rule.multiplier, intent.quantity, fee_rate, fee_unit=leg_rule.fee_unit,
            )
            cash_fen -= fee_fen
            fill_rows.append({
                "fill_id": typed_canonical_hash({
                    "formal_fill": raw.order_id,
                    "trading_date": session.isoformat(),
                    "position_effect": intent.position_effect,
                }),
                "fill_time": intent.order_time,
                "trading_date": session,
                "order_id": raw.order_id,
                "intent_hash": intent.intent_hash,
                "source_target_hash": intent.portfolio_target_hash,
                "actual_contract": leg_contract,
                "side": intent.side,
                "quantity": intent.quantity,
                "position_effect": intent.position_effect,
                "fill_price": float(leg_open),
                "multiplier": leg_rule.multiplier,
                "fee_fen": fee_fen,
                "realized_pnl_fen": realized_fen,
                "position_before": position_before,
                "position_after": position,
                "basis_before": None if basis_before is None else float(basis_before),
                "basis_after": None if basis_price is None else float(basis_price),
                "rule_snapshot_hash": leg_rule.rule_snapshot_hash,
            })
            if active_contract_by_session is not None:
                rule_rows.append({**leg_rule.to_dict(), "application": "order"})
        settlement_contract = position_contract if position_contract is not None else contract
        day_settlement = settlement.loc[
            (settlement["date"] == session) & (settlement["code"] == settlement_contract)
        ]
        if len(day_settlement) != 1:
            raise SimulationContractError("收盘后结算价缺失或重叠")
        settle_price = _positive_decimal(day_settlement.iloc[0]["settle_price"], "结算价")
        margin_rate, settlement_margin_hash = rule_book.settlement_margin(
            contract_code=settlement_contract,
            trading_date=session,
            as_of=settlement_time,
        )
        settlement_rule = rule_book.resolve_order(
            contract_code=settlement_contract,
            trading_date=session,
            as_of=settlement_time,
        )
        mtm_fen = 0
        if position:
            if basis_price is None:
                raise SimulationContractError("非零期货持仓缺少结算基价")
            mtm_fen = _pnl_fen(
                side=1 if position > 0 else -1,
                quantity=abs(position),
                from_price=basis_price,
                to_price=settle_price,
                multiplier=settlement_rule.multiplier,
            )
            cash_fen += mtm_fen
            basis_price = settle_price
        margin_fen = _margin_fen(settle_price, settlement_rule.multiplier, abs(position), margin_rate)
        forced = False
        if margin_fen > cash_fen and position:
            forced = True
            fee_fen = _fee_fen(
                settle_price,
                settlement_rule.multiplier,
                abs(position),
                settlement_rule.close_today_fee_permyriad if opened_today else settlement_rule.close_fee_permyriad,
                fee_unit=settlement_rule.fee_unit,
            )
            cash_fen -= fee_fen
            fill_rows.append({
                "fill_id": typed_canonical_hash({
                    "formal_forced_fill": session.isoformat(),
                    "contract": settlement_contract,
                }),
                "fill_time": settlement_time,
                "trading_date": session,
                "order_id": typed_canonical_hash({"forced": session.isoformat(), "contract": settlement_contract}),
                "intent_hash": None,
                "source_target_hash": None,
                "actual_contract": settlement_contract,
                "side": "sell" if position > 0 else "buy",
                "quantity": abs(position),
                "position_effect": "close_today" if opened_today else "close_yesterday",
                "fill_price": float(settle_price),
                "multiplier": settlement_rule.multiplier,
                "fee_fen": fee_fen,
                "realized_pnl_fen": 0,
                "position_before": position,
                "position_after": 0,
                "basis_before": float(basis_price),
                "basis_after": None,
                "rule_snapshot_hash": settlement_rule.rule_snapshot_hash,
            })
            position = 0
            position_contract = None
            basis_price = None
            opened_today = 0
            margin_fen = 0
        settle_rows.append({
            "trading_date": session,
            "actual_contract": settlement_contract,
            "settlement_price": float(settle_price),
            "previous_settlement_price": None if settlement_contract not in last_settlements else float(last_settlements[settlement_contract]),
            "position": position,
            "mtm_pnl_fen": mtm_fen,
            "margin_rate_pct": float(margin_rate),
            "required_margin_fen": margin_fen,
            "equity_fen": cash_fen,
            "free_equity_fen": cash_fen - margin_fen,
            "forced_liquidation": forced,
            "settlement_margin_hash": settlement_margin_hash,
            "settlement_policy_hash": rule.settlement_policy_hash,
        })
        nav_rows.append({
            "trading_date": session,
            "nav_fen": cash_fen,
            "margin_fen": margin_fen,
            "free_equity_fen": cash_fen - margin_fen,
            "position": position,
        })
        settlement_rule_row = {
            **settlement_rule.to_dict(),
            "settlement_margin_hash": settlement_margin_hash,
            "settlement_margin_rate_pct": str(margin_rate),
        }
        if active_contract_by_session is not None:
            settlement_rule_row["application"] = "settlement"
        rule_rows.append(settlement_rule_row)
        last_settlements[settlement_contract] = settle_price
    result = FuturesSimulationResult(
        intents=intent_frame.reset_index(drop=True),
        fills=pd.DataFrame(fill_rows),
        settlements=pd.DataFrame(settle_rows),
        nav=pd.DataFrame(nav_rows),
        rule_snapshots=pd.DataFrame(rule_rows),
        rejections=pd.DataFrame(rejection_rows),
        backend_id=(
            "cn-futures-daily-v1"
            if active_contract_by_session is None
            else "cn-futures-dynamic-roll-v2"
        ),
        contract_version=(
            FUTURES_DAILY_SIMULATION_VERSION
            if active_contract_by_session is None
            else FUTURES_DYNAMIC_ROLL_SIMULATION_VERSION
        ),
    )
    _require_cash_conservation(result, cny_to_fen(initial_cash_cny))
    return result


def _target_legs(previous: int, target: int) -> tuple[tuple[str, int, str], ...]:
    if previous == target:
        return ()
    legs = []
    if previous and (target == 0 or (previous > 0) != (target > 0)):
        legs.append(("sell" if previous > 0 else "buy", abs(previous), "close_yesterday"))
        previous = 0
    delta = target - previous
    if delta:
        if previous and (previous > 0) == (delta < 0) and abs(delta) <= abs(previous):
            legs.append(("sell" if previous > 0 else "buy", abs(delta), "close_yesterday"))
        else:
            legs.append(("buy" if delta > 0 else "sell", abs(delta), "open"))
    return tuple(legs)


def _normalize_market(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "code", "open"}
    if required - set(frame.columns):
        raise SimulationContractError("期货执行行情字段不完整")
    optional = [
        column for column in ("paused", "tradable", "no_trade", "high_limit", "low_limit")
        if column in frame.columns
    ]
    result = frame[list(required) + optional].copy()
    result["date"] = pd.to_datetime(result["date"]).dt.date
    return result.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)


def _futures_unfilled_reason(row: pd.Series, *, side: str) -> str | None:
    if "paused" in row.index and pd.notna(row["paused"]) and bool(row["paused"]):
        return "market_paused"
    if "tradable" in row.index and pd.notna(row["tradable"]) and not bool(row["tradable"]):
        return "market_not_tradable"
    if "no_trade" in row.index and pd.notna(row["no_trade"]) and bool(row["no_trade"]):
        return "no_trade"
    open_price = _positive_decimal(row["open"], "订单腿开盘价")
    if side == "buy" and "high_limit" in row.index and pd.notna(row["high_limit"]):
        if open_price >= _positive_decimal(row["high_limit"], "涨停价"):
            return "limit_up"
    if side == "sell" and "low_limit" in row.index and pd.notna(row["low_limit"]):
        if open_price <= _positive_decimal(row["low_limit"], "跌停价"):
            return "limit_down"
    return None


def _normalize_settlement(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "code", "settle_price"}
    if required - set(frame.columns):
        raise SimulationContractError("期货结算字段不完整")
    result = frame[list(required)].copy()
    result["date"] = pd.to_datetime(result["date"]).dt.date
    return result.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)


def _require_cash_conservation(result: FuturesSimulationResult, initial_cash_fen: int) -> None:
    fees = int(result.fills["fee_fen"].sum()) if not result.fills.empty else 0
    realized = int(result.fills["realized_pnl_fen"].sum()) if not result.fills.empty else 0
    mtm = int(result.settlements["mtm_pnl_fen"].sum())
    final = int(result.nav.iloc[-1]["nav_fen"])
    if final != initial_cash_fen + realized + mtm - fees:
        raise SimulationContractError("期货现金、逐日盯市和费用不守恒")


def _pnl_fen(*, side: int, quantity: int, from_price: Decimal | None, to_price: Decimal, multiplier: int) -> int:
    if from_price is None:
        raise SimulationContractError("期货盈亏缺少成本或前结算价")
    return int(((to_price - from_price) * side * quantity * multiplier * 100).quantize(_FEN, rounding=ROUND_HALF_UP))


def _fee_fen(
    price: Decimal,
    multiplier: int,
    quantity: int,
    fee_value: Decimal,
    *,
    fee_unit: str,
) -> int:
    if fee_unit == "notional_permyriad":
        amount = price * multiplier * quantity * fee_value / 100
    elif fee_unit == "per_lot_cny":
        amount = fee_value * quantity * 100
    else:
        raise SimulationContractError("期货手续费单位未登记")
    return int(amount.quantize(_FEN, rounding=ROUND_HALF_UP))


def _margin_fen(price: Decimal, multiplier: int, quantity: int, rate_pct: Decimal) -> int:
    return int((price * multiplier * quantity * rate_pct).quantize(_FEN, rounding=ROUND_HALF_UP))


def cny_to_fen(value: Decimal) -> int:
    """按正式期货账本的半升规则把人民币转换为整数分。"""

    return int((value.quantize(_CENT, rounding=ROUND_HALF_UP) * 100).to_integral_value())


def _underlying_symbol(code: str) -> str:
    base = code.split(".", 1)[0]
    symbol = "".join(character for character in base if character.isalpha()).upper()
    if not symbol:
        raise SimulationContractError("无法从实际合约提取品种代码")
    return symbol


def _actual_future_contract(code: object) -> bool:
    if not isinstance(code, str) or "." not in code:
        return False
    base = code.rsplit(".", 1)[0]
    return bool(base) and not base.endswith(("8888", "9999"))


def _row_payload(row: pd.Series, columns: Sequence[str]) -> dict[str, object]:
    return {str(column): _scalar(row[column]) for column in columns}


def _frame_payload(frame: pd.DataFrame) -> list[dict[str, object]]:
    return [
        {str(column): _scalar(row[column]) for column in frame.columns}
        for _, row in frame.iterrows()
    ]


def _scalar(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def _positive_decimal(value: object, field: str) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite() or result <= 0:
        raise SimulationContractError(f"{field} 必须为正数")
    return result


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite() or result < 0:
        raise SimulationContractError(f"{field} 必须为非负数")
    return result


def _aware_local(value: object) -> datetime:
    result = pd.Timestamp(value).to_pydatetime()
    return result.replace(tzinfo=_TIMEZONE) if result.tzinfo is None else result.astimezone(_TIMEZONE)


def _naive_local(value: datetime) -> datetime:
    return value.astimezone(_TIMEZONE).replace(tzinfo=None)


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SimulationContractError(f"{field} 必须是 sha256")
    return value


__all__ = [
    "FUTURES_DAILY_SIMULATION_VERSION",
    "FUTURES_DYNAMIC_ROLL_SIMULATION_VERSION",
    "FUTURES_PORTFOLIO_SIMULATION_VERSION",
    "FuturesExecutionRule",
    "FuturesRuleBook",
    "FuturesSimulationResult",
    "MarginOverride",
    "build_futures_order_intents",
    "build_futures_roll_order_intents",
    "cny_to_fen",
    "run_futures_daily_simulation",
]
