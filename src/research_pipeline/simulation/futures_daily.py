"""实际合约期货日频事件仿真、规则解析与逐日盯市账本。"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from datetime import date, datetime, time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
import json
from fractions import Fraction
from typing import Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from research_pipeline.domain.order_stream import ExplicitOrderCommand, parse_order_commands
from research_pipeline.domain.trading import (
    InstrumentKey,
    OrderIntent,
    PortfolioTarget,
    TradingRuleBinding,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.canonical import typed_canonical_hash_streamed
from research_pipeline.simulation.costs import futures_fee_fen as _fee_fen
from research_pipeline.simulation.margin import futures_margin_fen as _margin_fen
from research_pipeline.simulation.intent_port import CN_FUTURES_DAILY_BACKEND, IntentToOrderPort
from research_pipeline.simulation.engine import ExecutionEngine
from research_pipeline.simulation.events import ExecutionOutcome, FinancialEvent
from research_pipeline.simulation.ledger import (
    ExecutionGroup, FuturesAccountCore, FuturesLedgerState, FuturesDailySessionLedger,
)
from research_pipeline.simulation.orders import ORDER_TERMINAL_STATES, SimulationContractError


FUTURES_DAILY_SIMULATION_VERSION = "research-futures-daily-simulation-v2"
FUTURES_DYNAMIC_ROLL_SIMULATION_VERSION = "research-futures-dynamic-roll-simulation-v3"
FUTURES_PORTFOLIO_SIMULATION_VERSION = "research-futures-portfolio-simulation-v2"
_TIMEZONE = ZoneInfo("Asia/Shanghai")
_CENT = Decimal("0.01")


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
        rate, source_hash, _ = self.settlement_margin_details(
            contract_code=contract_code, trading_date=trading_date, as_of=as_of,
        )
        return rate, source_hash

    def settlement_margin_details(self, *, contract_code: str, trading_date: date,
                                  as_of: datetime) -> tuple[Decimal, str, datetime]:
        """结算费率与来源行实际可见时点，沿用同一规则选择。"""
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
        available_at = max(_aware_local(row["addTime"]), _aware_local(row["modTime"]))
        return rate, typed_canonical_hash(_row_payload(row, self.margins.columns)), available_at

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
    order_lifecycle: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)
    initial_cash_fen: int = 0
    market_inputs: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)
    settlement_inputs: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)
    slippage_ticks: int = 0
    tick_size_inputs: pd.DataFrame = dataclass_field(default_factory=pd.DataFrame)
    timing_policy: dict[str, object] = dataclass_field(default_factory=dict)

    explicit_order_context: Mapping[str, object] | None = None

    @property
    def result_hash(self) -> str:
        payload = {
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
            "order_lifecycle": _frame_payload(self.order_lifecycle),
            "initial_cash_fen": self.initial_cash_fen,
            "market_inputs": _frame_payload(self.market_inputs),
            "settlement_inputs": _frame_payload(self.settlement_inputs),
            "slippage_ticks": self.slippage_ticks,
            "tick_size_inputs": _frame_payload(self.tick_size_inputs),
            "timing_policy": self.timing_policy,
        }
        if self.explicit_order_context is not None:
            payload["explicit_order_context"] = self.explicit_order_context
        return typed_canonical_hash_streamed(payload)


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
        target_hash = target.target_hash
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
            rule_snapshot_hash = rule.rule_snapshot_hash
            binding = TradingRuleBinding(
                instrument_hash=leg_instrument.instrument_hash,
                rule_snapshot_hash=rule_snapshot_hash,
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
                portfolio_target_hash=target_hash,
                market_data_artifact_hash=market_data_artifact_hash,
                rule_binding=binding,
                source_hashes=tuple(sorted({target_hash, rule_snapshot_hash})),
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
                "target_hash": target_hash,
                "rule_snapshot_hash": rule_snapshot_hash,
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
    execution_mode: str = "intents",
    order_commands: Sequence[ExplicitOrderCommand] = (),
) -> FuturesSimulationResult:
    """由公共执行引擎推进单品种日频会话，结算后检查保证金并完成强平批次。"""
    commands = parse_order_commands(order_commands)
    if execution_mode not in {"intents", "explicit_orders"}:
        raise SimulationContractError("execution_mode 必须是 intents 或 explicit_orders")
    if (execution_mode == "intents" and commands) or (execution_mode == "explicit_orders" and not intents.empty):
        raise SimulationContractError("intents 与显式命令模式不能混合执行")
    if execution_mode == "explicit_orders" and not any(command.action == "submit" for command in commands):
        raise SimulationContractError("显式订单模式至少需要一条 submit")
    if initial_cash_cny <= 0:
        raise SimulationContractError("初始资金必须为正")
    if type(slippage_ticks) is not int or slippage_ticks < 0:
        raise SimulationContractError("slippage_ticks 必须是非负整数")
    market_input_frame = market.copy()
    settlement_input_frame = settlements.copy()
    market = _normalize_market(market)
    settlement = _normalize_settlement(settlements)
    if market[["date", "code"]].duplicated().any() or settlement[["date", "code"]].duplicated().any():
        raise SimulationContractError("期货行情或结算存在重复主键")
    initial_fen = cny_to_fen(initial_cash_cny)
    state = FuturesLedgerState(
        ExecutionGroup("futures-daily", "cn_future", "CNY", "daily_settlement",
                       "settlement_margin_check"), initial_fen,
    )
    engine = ExecutionEngine()
    intent_port = IntentToOrderPort(CN_FUTURES_DAILY_BACKEND)
    forced_intent_rows = []
    fill_rows, rejection_rows, settle_rows, nav_rows, rule_rows, lifecycle_rows = [], [], [], [], [], []
    fill_sequence = 0
    last_settlements: dict[str, Decimal] = {}
    intent_frame = intents.copy()
    market_sessions = set(market["date"])
    source_availability = {}
    for role, frame, columns in (
        ("market", market_input_frame, ("open_available_at", "available_at")),
        ("settlement", settlement_input_frame, ("available_at",)),
    ):
        for row in frame.itertuples(index=False):
            for column in columns:
                value = getattr(row, column, None)
                if value is not None and pd.notna(value):
                    source_availability[(role, pd.Timestamp(row.date).date(), str(row.code))] = _aware_local(value)
                    break
    normalized_mapping = None
    if active_contract_by_session is not None:
        normalized_mapping = {
            pd.Timestamp(session).date(): str(contract)
            for session, contract in active_contract_by_session.items()
        }
        if set(normalized_mapping) != market_sessions:
            raise SimulationContractError("主动合约映射必须与执行行情交易日完全一致")
        if any(not _actual_future_contract(code) for code in normalized_mapping.values()):
            raise SimulationContractError("主动合约映射只能引用真实期货合约")
    sessions = sorted(market_sessions)
    explicit = (_DailyFuturesExplicitExecution(commands, engine, rule_book, initial_fen,
                slippage_ticks, tick_size_by_session_contract or {})
                if execution_mode == "explicit_orders" else None)
    if explicit is not None:
        explicit.require_window(market_input_frame)
    market_positions = market.groupby("date", sort=False).indices
    intent_positions = intent_frame.groupby("execution_session", sort=False).indices if not intent_frame.empty else {}
    settlement_positions = {
        (row.date, row.code): position
        for position, row in enumerate(settlement.itertuples(index=False))
    }

    def before_open(session: date) -> tuple[dict[str, pd.Series], str, FuturesDailySessionLedger]:
        nonlocal fill_sequence
        fill_sequence = 0
        day_market = {}
        for position in market_positions[session]:
            row = market.iloc[position]
            day_market[row["code"]] = row
        if normalized_mapping is None:
            if len(day_market) != 1:
                raise SimulationContractError("期货每个交易日必须唯一实际合约行情")
            contract = str(next(iter(day_market)))
        else:
            contract = normalized_mapping[session]
        if contract not in day_market:
            raise SimulationContractError("主动合约执行行情缺失或重叠")
        return day_market, contract, FuturesDailySessionLedger(state, session)

    def append_fill(*, ledger: FuturesDailySessionLedger, instrument: InstrumentKey,
                    side: str, quantity: int, effect: str, price: Decimal,
                    rule: FuturesExecutionRule, rule_snapshot_hash: str,
                    instrument_hash: str, intent_hash: str, fee: int, order_id: str,
                    fill_id: str, at: datetime, intent: OrderIntent | None,
                    execution_reason: str = "target_order",
                    financial_event: FinancialEvent | None = None) -> None:
        nonlocal fill_sequence
        fill_sequence += 1
        before = ledger.position
        realized = ledger.apply_event(financial_event) if financial_event is not None else ledger.apply_fill(
            instrument_hash=instrument_hash, contract_code=instrument.instrument_id,
            side=side, quantity=quantity, position_effect=effect, price=price,
            multiplier=rule.multiplier, fee_units=fee,
        )
        after = ledger.position
        fill_rows.append({
            "fill_id": fill_id, "fill_time": at, "trading_date": ledger.trading_date,
            "fill_sequence": fill_sequence,
            "order_id": order_id, "intent_hash": intent_hash,
            "source_target_hash": None if intent is None else intent.portfolio_target_hash,
            "execution_reason": execution_reason,
            "actual_contract": instrument.instrument_id, "side": side, "quantity": quantity,
            "position_effect": effect, "fill_price": float(price), "multiplier": rule.multiplier,
            "execution_price_units": int(Fraction(price) * 10 ** ledger.positions[instrument_hash].price_scale),
            "fee_fen": fee, "realized_pnl_fen": realized,
            "position_before": 0 if before is None else before.contracts,
            "position_after": 0 if after is None else after.contracts,
            "opened_today_before": 0 if before is None else before.opened_today,
            "opened_today_after": 0 if after is None else after.opened_today,
            "basis_before": None if before is None else float(before.cost_price / 10 ** before.price_scale),
            "basis_after": None if after is None else float(after.cost_price / 10 ** after.price_scale),
            "basis_before_numerator": None if before is None else before.cost_numerator,
            "basis_before_denominator": None if before is None else before.cost_denominator,
            "basis_before_price_scale": None if before is None else before.price_scale,
            "basis_after_numerator": None if after is None else after.cost_numerator,
            "basis_after_denominator": None if after is None else after.cost_denominator,
            "price_scale": ledger.positions[instrument_hash].price_scale,
            "pnl_rounding_policy": ledger.pnl_rounding_policy,
            "rule_snapshot_hash": rule_snapshot_hash,
        })

    def reconcile(session: date, context: tuple[dict[str, pd.Series], str, FuturesDailySessionLedger]) -> None:
        day_market, _, ledger = context
        if explicit is not None:
            explicit.reconcile(session, day_market, ledger, append_fill, rule_rows, rejection_rows)
            return
        settlement_time = datetime.combine(session, time(17, 0), _TIMEZONE)
        day_intents = intent_frame.iloc[intent_positions.get(session, [])]
        blocked_target_sequences: set[int] = set()
        for raw in day_intents.itertuples(index=False):
            intent = OrderIntent.from_dict(json.loads(raw.intent_json))
            intent.rule_binding.require_for(intent.instrument, application_time=intent.order_time)
            if intent.order_time > settlement_time:
                raise SimulationContractError("订单执行时点晚于当日结算时点")
            leg_contract = intent.instrument.instrument_id
            if not _actual_future_contract(leg_contract) or intent.instrument.contract_kind != "future_contract":
                raise SimulationContractError("成交必须引用真实期货合约")
            leg_row = day_market.get(leg_contract)
            if leg_row is None:
                raise SimulationContractError("订单腿执行行情缺失或重叠")
            available_at = source_availability.get(("market", session, leg_contract))
            if available_at is not None and available_at > intent.order_time:
                raise SimulationContractError("开盘价在执行时点尚不可见")
            target_sequence = int(getattr(raw, "target_sequence", -1))
            rejection_reason = (
                "prior_leg_unfilled" if target_sequence in blocked_target_sequences
                else _futures_unfilled_reason(leg_row, side=intent.side)
            )
            order = intent_port.to_order(intent, ordinal=int(getattr(raw, "leg_ordinal", 0)))
            if order.order_id != str(raw.order_id):
                raise SimulationContractError("订单身份与正式意图端口不一致")
            intent_hash = intent.intent_hash
            if rejection_reason is None:
                leg_open = _positive_decimal(leg_row["open"], "订单腿开盘价")
                if slippage_ticks:
                    if tick_size_by_session_contract is None:
                        raise SimulationContractError("非零滑点必须绑定可信 tick size")
                    tick_size = tick_size_by_session_contract.get((session, leg_contract))
                    if tick_size is None:
                        raise SimulationContractError("订单腿缺少执行日 tick size")
                    tick = _positive_decimal(tick_size, "tick size")
                    leg_open += tick * slippage_ticks if intent.side == "buy" else -tick * slippage_ticks
                    if leg_open <= 0:
                        raise SimulationContractError("tick 滑点后的执行价必须为正")
                rejection_reason = _futures_execution_price_limit_reason(leg_row, price=leg_open)
            if rejection_reason is not None:
                rejection_rows.append({
                    "order_id": raw.order_id, "trading_date": session,
                    "actual_contract": leg_contract, "side": intent.side, "quantity": intent.quantity,
                    "position_effect": intent.position_effect, "reason_code": rejection_reason,
                    "intent_hash": intent_hash, "source_target_hash": intent.portfolio_target_hash,
                    "decision_time": intent.decision_time, "order_time": intent.order_time,
                })
                engine.execute_order(order, trading_session=session, event_time=intent.order_time,
                                     execute=lambda: ExecutionOutcome(filled_quantity=0, reason=rejection_reason, value=None))
                if intent.position_effect in {"close", "close_yesterday", "close_today"}:
                    blocked_target_sequences.add(target_sequence)
                continue
            leg_rule = rule_book.resolve_order(contract_code=leg_contract, trading_date=session,
                                                as_of=intent.order_time)
            leg_rule_snapshot_hash = leg_rule.rule_snapshot_hash
            if intent.rule_binding.rule_snapshot_hash != leg_rule_snapshot_hash:
                raise SimulationContractError("订单规则快照与执行日规则漂移")
            fee_rate = {"open": leg_rule.open_fee_permyriad, "close": leg_rule.close_fee_permyriad,
                        "close_yesterday": leg_rule.close_fee_permyriad,
                        "close_today": leg_rule.close_today_fee_permyriad}[intent.position_effect]
            fee = _fee_fen(leg_open, leg_rule.multiplier, intent.quantity, fee_rate,
                           fee_unit=leg_rule.fee_unit)

            def execute() -> ExecutionOutcome:
                append_fill(ledger=ledger, instrument=intent.instrument, side=intent.side,
                            quantity=intent.quantity, effect=intent.position_effect, price=leg_open,
                            rule=leg_rule, rule_snapshot_hash=leg_rule_snapshot_hash,
                            instrument_hash=intent.instrument.instrument_hash, intent_hash=intent_hash,
                            fee=fee, order_id=str(raw.order_id),
                            fill_id=typed_canonical_hash({"formal_fill": raw.order_id,
                                "trading_date": session.isoformat(), "position_effect": intent.position_effect}),
                            at=intent.order_time, intent=intent)
                return ExecutionOutcome(filled_quantity=intent.quantity, reason=None, value=None)

            engine.execute_order(order, trading_session=session, event_time=intent.order_time,
                                 execute=execute)
            rule_rows.append({**leg_rule.to_dict(), "rule_snapshot_hash": leg_rule_snapshot_hash, "application": "order"})

    def after_close(session: date, context: tuple[dict[str, pd.Series], str, FuturesDailySessionLedger]) -> datetime:
        nonlocal state
        _, contract, ledger = context
        settlement_time = datetime.combine(session, time(17, 0), _TIMEZONE)
        if explicit is not None:
            explicit.close_session(session, settlement_time, ledger, rule_rows, rejection_rows)
        before = ledger.position
        settlement_contract = before.contract_code if before is not None else contract
        settlement_position = settlement_positions.get((session, settlement_contract))
        if settlement_position is None:
            raise SimulationContractError("收盘后结算价缺失或重叠")
        settle_price = _positive_decimal(settlement.iloc[settlement_position]["settle_price"], "结算价")
        available_at = source_availability.get(("settlement", session, settlement_contract))
        if available_at is not None and available_at > settlement_time:
            raise SimulationContractError("结算价在日频结算时点尚不可见")
        margin_rate, settlement_margin_hash, settlement_margin_available_at = rule_book.settlement_margin_details(
            contract_code=settlement_contract, trading_date=session, as_of=settlement_time,
        )
        settlement_rule = rule_book.resolve_order(contract_code=settlement_contract,
            trading_date=session, as_of=settlement_time)
        settlement_rule_snapshot_hash = settlement_rule.rule_snapshot_hash
        margin_before = _margin_fen(settle_price, settlement_rule.multiplier,
                                   0 if before is None else abs(before.contracts), margin_rate)
        mtm_fen = ledger.settle(price=settle_price, multiplier=settlement_rule.multiplier,
                                margin_units=margin_before)
        equity_before = ledger.equity_units
        forced = before is not None and margin_before > equity_before
        forced_fee = 0
        settlement_source_hash = typed_canonical_hash({
            "trading_date": session.isoformat(), "actual_contract": settlement_contract,
            "settlement_time": settlement_time.isoformat(), "settlement_price": str(settle_price),
            "settlement_margin_hash": settlement_margin_hash,
            "rule_snapshot_hash": settlement_rule_snapshot_hash,
        })
        risk_target_hash = typed_canonical_hash({
            "settlement_source_hash": settlement_source_hash,
            "position": 0 if before is None else before.contracts,
            "equity_fen": equity_before, "required_margin_fen": margin_before,
            "target_quantity": 0, "reason": "required_margin_exceeds_equity",
        })
        if forced:
            _, venue = settlement_contract.rsplit(".", 1)
            instrument = InstrumentKey(settlement_contract, "cn_future", venue, "CNY", "future_contract")
            instrument_hash = instrument.instrument_hash
            for ordinal, (effect, quantity, rate) in enumerate((
                ("close_yesterday", before.yesterday_contracts, settlement_rule.close_fee_permyriad),
                ("close_today", before.opened_today, settlement_rule.close_today_fee_permyriad),
            )):
                if not quantity:
                    continue
                fee = _fee_fen(settle_price, settlement_rule.multiplier, quantity, rate,
                               fee_unit=settlement_rule.fee_unit)
                forced_fee += fee
                side = "sell" if before.contracts > 0 else "buy"
                binding = TradingRuleBinding(
                    instrument_hash, settlement_rule_snapshot_hash,
                    settlement_rule.available_at, multiplier_rule_hash=settlement_rule.multiplier_hash,
                    fee_rule_hash=settlement_rule.fee_hash, margin_rule_hash=settlement_margin_hash,
                    settlement_rule_hash=settlement_rule.settlement_policy_hash,
                )
                forced_intent = OrderIntent(
                    instrument, side, quantity, effect, settlement_time, settlement_time,
                    risk_target_hash, settlement_source_hash, binding,
                    tuple(sorted({risk_target_hash, settlement_source_hash, settlement_rule_snapshot_hash})),
                )
                order = intent_port.to_order(forced_intent, ordinal=ordinal)
                order_id = order.order_id
                forced_intent_hash = forced_intent.intent_hash
                forced_intent_rows.append({
                    "execution_session": session, "intent_hash": forced_intent_hash,
                    "intent_json": json.dumps(forced_intent.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    "order_id": order_id, "actual_contract": settlement_contract,
                    "side": side, "quantity": quantity, "position_effect": effect,
                    "target_sequence": -1, "leg_ordinal": ordinal, "target_hash": risk_target_hash,
                    "rule_snapshot_hash": settlement_rule_snapshot_hash,
                    "execution_reason": "settlement_margin_shortfall",
                })

                def execute_forced() -> ExecutionOutcome:
                    append_fill(ledger=ledger, instrument=instrument, side=side, quantity=quantity,
                                effect=effect, price=settle_price, rule=settlement_rule,
                                rule_snapshot_hash=settlement_rule_snapshot_hash,
                                instrument_hash=instrument_hash, intent_hash=forced_intent_hash, fee=fee,
                                order_id=order_id, fill_id=typed_canonical_hash({"formal_forced_fill": order_id}),
                                at=settlement_time, intent=forced_intent,
                                execution_reason="settlement_margin_shortfall")
                    return ExecutionOutcome(filled_quantity=quantity, reason=None, value=None)

                engine.execute_order(order, trading_session=session, event_time=settlement_time,
                                     execute=execute_forced)
        state = ledger.publish()
        after = ledger.position
        settle_rows.append({
            "trading_date": session, "settlement_time": settlement_time,
            "actual_contract": settlement_contract, "settlement_price": float(settle_price),
            "previous_settlement_price": None if settlement_contract not in last_settlements else float(last_settlements[settlement_contract]),
            "position": 0 if after is None else after.contracts,
            "mtm_pnl_fen": mtm_fen, "margin_rate_pct": float(margin_rate),
            "required_margin_fen": state.margin_units, "equity_fen": state.equity_units,
            "free_equity_fen": state.free_equity_units, "forced_liquidation": forced,
            "settlement_margin_hash": settlement_margin_hash,
            "settlement_policy_hash": settlement_rule.settlement_policy_hash,
            "settlement_rule_snapshot_hash": settlement_rule_snapshot_hash,
            "settlement_source_hash": settlement_source_hash,
            "opening_equity_fen": nav_rows[-1]["nav_fen"] if nav_rows else initial_fen,
            "position_before_settlement": 0 if before is None else before.contracts,
            "basis_before_settlement": None if before is None else float(before.cost_price / 10 ** before.price_scale),
            "basis_before_settlement_numerator": None if before is None else before.cost_numerator,
            "basis_before_settlement_denominator": None if before is None else before.cost_denominator,
            "basis_before_settlement_price_scale": None if before is None else before.price_scale,
            "opened_today_before_liquidation": 0 if before is None else before.opened_today,
            "equity_before_liquidation_fen": equity_before,
            "required_margin_before_liquidation_fen": margin_before,
            "forced_liquidation_reason": "required_margin_exceeds_equity" if forced else None,
            "forced_liquidation_fee_fen": forced_fee,
            "margin_check_policy": ledger.margin_check_policy,
            "close_bucket_order": "yesterday_then_today",
            "pnl_rounding_policy": ledger.pnl_rounding_policy,
            "position_opened_today": 0 if after is None else after.opened_today,
            "basis_after_settlement": None if after is None else float(after.cost_price / 10 ** after.price_scale),
            "basis_after_settlement_numerator": None if after is None else after.cost_numerator,
            "basis_after_settlement_denominator": None if after is None else after.cost_denominator,
            "basis_after_settlement_price_scale": None if after is None else after.price_scale,
            "position_yesterday_contracts": 0 if after is None else after.yesterday_contracts,
            "pnl_remainder_numerator": 0 if after is None else after.pnl_remainder_numerator,
            "pnl_remainder_denominator": 1 if after is None else after.pnl_remainder_denominator,
        })
        nav_rows.append({"trading_date": session, "nav_fen": state.equity_units,
                         "margin_fen": state.margin_units, "free_equity_fen": state.free_equity_units,
                         "position": 0 if after is None else after.contracts})
        rule_rows.append({**settlement_rule.to_dict(), "rule_snapshot_hash": settlement_rule_snapshot_hash, "application": "settlement",
                          "settlement_margin_available_at": settlement_margin_available_at,
                          "settlement_margin_hash": settlement_margin_hash,
                          "settlement_margin_rate_pct": str(margin_rate)})
        last_settlements[settlement_contract] = settle_price
        return settlement_time

    for lifecycle in engine.run_daily_sessions(sessions, before_open=before_open,
                                               reconcile=reconcile, after_close=after_close):
        lifecycle_rows.extend(lifecycle)
    if explicit is not None:
        explicit.require_finished()
    intent_frame["execution_reason"] = "target_order"
    if forced_intent_rows:
        intent_frame = pd.concat([intent_frame, pd.DataFrame(forced_intent_rows)], ignore_index=True)
    order_times: dict[tuple[date, str], datetime] = {}
    for row in intents.itertuples(index=False):
        intent = OrderIntent.from_dict(json.loads(row.intent_json))
        key = (pd.Timestamp(row.execution_session).date(), intent.instrument.instrument_id)
        order_times[key] = min(order_times.get(key, intent.order_time), intent.order_time)
    if explicit is not None:
        order_times.update(explicit.open_times)
    timing_policy = {
        "policy_id": "daily-futures-research-clock-v1",
        "timezone": "Asia/Shanghai",
        "execution": "explicit_order_time_at_daily_open_price",
        "missing_open_time": "earliest_explicit_order_time_research_assumption",
        "settlement_time": "17:00",
        "missing_settlement_availability": "settlement_time_research_assumption",
        "missing_tick_availability": "earliest_explicit_order_time_research_assumption",
    }
    if explicit is not None:
        timing_policy.update(policy_id="daily-futures-explicit-opening-clock-v1",
                             execution="strictly_after_submission_at_daily_open",
                             missing_open_time="09:00_research_assumption",
                             missing_tick_availability="daily_open_research_assumption")
    for frame, is_market in ((market_input_frame, True), (settlement_input_frame, False)):
        assumed_times = [
            order_times.get((pd.Timestamp(row.date).date(), str(row.code)),
                            datetime.combine(pd.Timestamp(row.date).date(), time(9), _TIMEZONE))
            if is_market else datetime.combine(pd.Timestamp(row.date).date(), time(17), _TIMEZONE)
            for row in frame.itertuples(index=False)
        ]
        time_column = "execution_time" if is_market else "settlement_time"
        availability_column = "open_available_at" if is_market else "available_at"
        for column in (time_column, availability_column):
            values, bases = [], []
            for row, assumption in zip(frame.itertuples(index=False), assumed_times):
                value = getattr(row, column, None)
                if column == "open_available_at" and (value is None or pd.isna(value)):
                    value = getattr(row, "available_at", None)
                supplied = value is not None and pd.notna(value)
                values.append(_aware_local(value) if supplied else assumption)
                bases.append("source_timestamp" if supplied else "research_timing_assumption")
            frame[column] = values
            frame[column + "_basis"] = bases
        if is_market and "available_at" not in frame:
            frame["available_at"] = frame["open_available_at"]
        if not is_market:
            frame["ledger_settlement_time"] = assumed_times
    tick_input_frame = pd.DataFrame([
        {"date": session, "code": code, "tick_size": str(tick),
         "available_at": order_times.get((session, code), datetime.combine(session, time(9), _TIMEZONE)),
         "tick_available_at": order_times.get((session, code), datetime.combine(session, time(9), _TIMEZONE)),
         "available_at_basis": "research_timing_assumption"}
        for (session, code), tick in sorted((tick_size_by_session_contract or {}).items())
    ], columns=("date", "code", "tick_size", "available_at", "tick_available_at", "available_at_basis"))
    snapshots_by_hash = {}
    for row in rule_rows:
        reference = row["rule_snapshot_hash"]
        previous = snapshots_by_hash.get(reference)
        if previous is not None:
            combined = {**previous, **row}
            if previous["application"] != row["application"]:
                combined["application"] = "order_and_settlement"
            snapshots_by_hash[reference] = combined
        else:
            snapshots_by_hash[reference] = row
    fills_frame = pd.DataFrame(fill_rows)
    settlements_frame = pd.DataFrame(settle_rows)
    for frame, price_columns in (
        (fills_frame, ("basis_before", "basis_after")),
        (settlements_frame, ("previous_settlement_price", "basis_before_settlement", "basis_after_settlement")),
    ):
        for column in price_columns:
            if column in frame.columns:
                frame[column] = pd.array(frame[column], dtype="Float64")
        for column in frame.columns:
            if column.endswith(("_numerator", "_denominator", "_price_scale")):
                frame[column] = pd.array([row.get(column) for row in (
                    fill_rows if frame is fills_frame else settle_rows
                )], dtype="Int64")
    result = FuturesSimulationResult(
        intents=intent_frame.reset_index(drop=True), fills=fills_frame,
        settlements=settlements_frame, nav=pd.DataFrame(nav_rows),
        rule_snapshots=pd.DataFrame(snapshots_by_hash.values()), rejections=pd.DataFrame(rejection_rows),
        order_lifecycle=pd.DataFrame(lifecycle_rows), initial_cash_fen=initial_fen,
        market_inputs=market_input_frame, settlement_inputs=settlement_input_frame,
        slippage_ticks=slippage_ticks, tick_size_inputs=tick_input_frame, timing_policy=timing_policy,
        explicit_order_context=None if explicit is None else explicit.context,
        backend_id="cn-futures-daily-v1" if normalized_mapping is None else "cn-futures-dynamic-roll-v2",
        contract_version=FUTURES_DAILY_SIMULATION_VERSION if normalized_mapping is None else FUTURES_DYNAMIC_ROLL_SIMULATION_VERSION,
    )
    _require_cash_conservation(result, initial_fen)
    return result


class _DailyFuturesExplicitExecution:
    """日开盘订单事件；费用、盈亏和保证金继续采用日频口径。"""

    def __init__(self, commands, engine, rule_book, initial_fen, slippage_ticks, ticks):
        self.commands, self.engine, self.rule_book = commands, engine, rule_book
        self.slippage_ticks, self.ticks = slippage_ticks, ticks
        self.cursor = 0
        self.active = {}
        self.terminal = {}
        self.open_times = {}
        self.context = {
            "contract_version": "research-explicit-futures-daily-execution-v1",
            "commands": [command.to_dict() for command in commands],
            "events": [], "observations": [], "command_rules": [],
            "cancel_results": [], "session_ends": [], "fee_facts": [],
            "order_states": [], "initial_cash_fen": initial_fen, "cash_scale": 2,
        }

    def require_window(self, market):
        keys = {(pd.Timestamp(row.date).date(), str(row.code)) for row in market.itertuples(index=False)}
        for row in market.itertuples(index=False):
            session = pd.Timestamp(row.date).date()
            value = getattr(row, "execution_time", None)
            at = (_aware_local(value) if value is not None and pd.notna(value)
                  else datetime.combine(session, time(9), _TIMEZONE))
            if at > datetime.combine(session, time(17), _TIMEZONE):
                raise SimulationContractError("日开盘事件不能晚于结算时点")
            self.open_times[(session, str(row.code))] = at
        for command in self.commands:
            if (command.trading_date, command.instrument.instrument_id) not in keys:
                raise SimulationContractError("显式期货命令超出执行会话或实际合约行情窗口")
            if (command.instrument.asset_class != "cn_future"
                    or not _actual_future_contract(command.instrument.instrument_id)):
                raise SimulationContractError("日频期货显式订单只能引用真实期货合约")
            if command.submitted_at > datetime.combine(command.trading_date, time(17), _TIMEZONE):
                raise SimulationContractError("显式期货命令晚于所属会话结算")
            if command.action == "submit":
                if command.position_effect not in {"open", "close_today", "close_yesterday"}:
                    raise SimulationContractError("显式日频平仓必须明确 close_today 或 close_yesterday")
        if any(left.trading_date > right.trading_date for left, right in zip(self.commands, self.commands[1:])):
            raise SimulationContractError("显式期货命令会话必须按时间递增")

    def _event(self, ledger, command, kind, at, rule_hash, payload, *, event_id=None):
        return FinancialEvent(
            event_id=event_id or typed_canonical_hash({"command_id": command.command_id,
                "event_sequence": len(self.context["events"]), "kind": kind}),
            kind=kind, effective_time=at, session=ledger.trading_date.isoformat(),
            group_id=ledger.group.group_id, rule_hash=rule_hash,
            payload=tuple(sorted(payload.items())), parent_id=command.command_id,
        )

    def _apply(self, ledger, event):
        ledger.apply_event(event)
        self.context["events"].append(event.to_dict())

    def _release(self, ledger, command, at, rule_hash):
        if any(item.order_id == command.order_id for item in ledger.order_reservations):
            self._apply(ledger, self._event(ledger, command, "cash_reserved", at, rule_hash,
                {"order_id": command.order_id, "action": "release"}))

    @staticmethod
    def _rate(command, rule):
        return {"open": rule.open_fee_permyriad, "close_today": rule.close_today_fee_permyriad,
                "close_yesterday": rule.close_fee_permyriad}[command.position_effect]

    def _reservation(self, command, rule, quantity):
        reference = Decimal(command.reference_price.units).scaleb(-command.reference_price.scale)
        if command.limit_price is not None:
            reference = max(reference, Decimal(command.limit_price.units).scaleb(-command.limit_price.scale))
        fee = _fee_fen(reference, rule.multiplier, quantity, self._rate(command, rule), fee_unit=rule.fee_unit)
        margin = _margin_fen(reference, rule.multiplier, quantity, rule.margin_rate_pct) if command.position_effect == "open" else 0
        return fee, margin

    def _reserve(self, ledger, command, rule, quantity, at):
        fee, margin = self._reservation(command, rule, quantity)
        self._apply(ledger, self._event(ledger, command, "cash_reserved", at, rule.rule_snapshot_hash,
            {"order_id": command.order_id, "cash_units": fee, "margin_units": margin,
             "quantity": quantity, "reference_price": command.reference_price.to_dict(),
             "limit_price": None if command.limit_price is None else command.limit_price.to_dict()}))
        if command.position_effect != "open":
            self._apply(ledger, self._event(ledger, command, "position_reserved", at, rule.rule_snapshot_hash,
                {"order_id": command.order_id, "instrument_hash": command.instrument.instrument_hash,
                 "position_effect": command.position_effect, "quantity": quantity}))

    @staticmethod
    def _maximum(quantity, predicate):
        low, high = 0, quantity
        while low < high:
            middle = (low + high + 1) // 2
            if predicate(middle):
                low = middle
            else:
                high = middle - 1
        return low

    def _finish(self, ledger, command, at, reason, action="cancel"):
        current = self.engine.broker.orders[command.order_id]
        if current.status not in ORDER_TERMINAL_STATES:
            current = self.engine.broker.advance(command.order_id, action, at, reason=reason)
        rule = self.active[command.order_id][1]
        self._release(ledger, command, at, rule.rule_snapshot_hash)
        self.terminal[command.order_id] = current.status
        self.context["order_states"].append({"order_id": command.order_id,
            "status": current.status, "filled_quantity": current.filled_quantity,
            "remaining_quantity": current.quantity - current.filled_quantity,
            "reason": current.rejection_code, "at": at.isoformat()})
        del self.active[command.order_id]

    def _process(self, through, session, ledger, rule_rows, rejections):
        while self.cursor < len(self.commands):
            command = self.commands[self.cursor]
            if command.trading_date != session or command.submitted_at > through:
                break
            self.cursor += 1
            if command.action == "cancel":
                original = self.active.get(command.order_id)
                reason = "cancelled" if original is not None else "already_terminal"
                if original is not None:
                    self._finish(ledger, original[0], command.submitted_at, "user_cancel")
                self.context["cancel_results"].append({"command_id": command.command_id,
                    "order_id": command.order_id, "at": command.submitted_at.isoformat(), "reason": reason})
                continue
            rule = self.rule_book.resolve_order(contract_code=command.instrument.instrument_id,
                trading_date=session, as_of=command.submitted_at)
            if rule.available_at > command.submitted_at:
                raise SimulationContractError("提交规则在命令时点尚不可见")
            self.context["command_rules"].append({"command_id": command.command_id,
                "rules_identity_hash": rule.rule_snapshot_hash, "parameters": rule.to_dict()})
            rule_rows.append({**rule.to_dict(), "rule_snapshot_hash": rule.rule_snapshot_hash, "application": "order"})
            self.engine.broker.submit_command(command, session)
            quantity = command.quantity
            reason = None
            if command.position_effect != "open":
                position = ledger.positions.get(command.instrument.instrument_hash)
                if position is None or not position.contracts or (position.contracts > 0) == (command.side == "buy"):
                    quantity, reason = 0, "invalid_close_direction"
                else:
                    available = ledger.publish().available_position_quantity(command.instrument.instrument_hash,
                        position_effect=command.position_effect)
                    quantity = min(quantity, available)
                    if quantity < command.quantity:
                        reason = "insufficient_close_bucket"
            free = ledger.publish().free_equity_units
            quantity = self._maximum(quantity, lambda count: sum(self._reservation(command, rule, count)) <= free)
            if quantity < command.quantity:
                reason = reason or "insufficient_free_equity"
            self.active[command.order_id] = (command, rule, quantity)
            if not quantity or (quantity < command.quantity and command.funds_policy == "reject"):
                self.engine.broker.advance(command.order_id, "reject", command.submitted_at, reason=reason)
                rejections.append({"order_id": command.order_id, "trading_date": session,
                    "actual_contract": command.instrument.instrument_id, "side": command.side,
                    "quantity": command.quantity, "position_effect": command.position_effect,
                    "reason_code": reason, "intent_hash": command.command_hash, "source_target_hash": None})
                self._finish(ledger, command, command.submitted_at, reason)
                continue
            self._reserve(ledger, command, rule, quantity, command.submitted_at)
            self.engine.broker.advance(command.order_id, "accept", command.submitted_at)

    def reconcile(self, session, market, ledger, append_fill, rule_rows, rejections):
        capacities = {}
        for code, row in market.items():
            value = row.get("visible_capacity")
            supplied = value is not None and pd.notna(value)
            if supplied and (isinstance(value, bool) or int(value) != value or value < 0):
                raise SimulationContractError("visible_capacity 必须是非负整数")
            capacities[code] = int(value) if supplied else None
        for at in sorted({self.open_times[(session, code)] for code in market}):
            self._process(at, session, ledger, rule_rows, rejections)
            for command, submitted_rule, accepted_quantity in tuple(self.active.values()):
                code = command.instrument.instrument_id
                if code not in market or self.open_times[(session, code)] != at or command.submitted_at >= at:
                    continue
                row = market[code]
                availability = row.get("open_available_at")
                if availability is None or pd.isna(availability):
                    availability = row.get("available_at")
                if availability is not None and pd.notna(availability) and _aware_local(availability) > at:
                    raise SimulationContractError("日开盘价在开盘执行事件尚不可见")
                raw_capacity = capacities[code]
                supplied = raw_capacity is not None
                capacity = raw_capacity if supplied else accepted_quantity
                opening = _positive_decimal(row["open"], "日开盘价")
                order = self.engine.broker.orders[command.order_id]
                requested = min(order.quantity - order.filled_quantity, accepted_quantity)
                rule = self.rule_book.resolve_order(contract_code=code, trading_date=session, as_of=at)
                if rule.available_at > at:
                    raise SimulationContractError("执行规则在开盘时点尚不可见")
                rule_rows.append({**rule.to_dict(), "rule_snapshot_hash": rule.rule_snapshot_hash, "application": "order"})
                reason = _futures_unfilled_reason(row, side=command.side)
                tick_count = self.slippage_ticks + command.slippage_ticks
                tick = self.ticks.get((session, code))
                if tick_count and tick is None:
                    raise SimulationContractError("显式日频滑点缺少当时可见 tick size")
                if tick is not None:
                    tick = _positive_decimal(tick, "tick size")
                supplied_scale = row.get("price_scale")
                if supplied_scale is not None and pd.notna(supplied_scale):
                    if (isinstance(supplied_scale, bool) or int(supplied_scale) != supplied_scale
                            or not 0 <= supplied_scale <= 18):
                        raise SimulationContractError("行情 price_scale 必须是 0..18 的整数")
                    quote_scale = int(supplied_scale)
                    quote_scale_basis = "market_price_scale"
                else:
                    quote_scale = max(command.reference_price.scale, -opening.as_tuple().exponent)
                    quote_scale_basis = "input_open_and_reference_scale"
                direction = 1 if command.side == "buy" else -1
                raw_price = opening * (1 + direction * Decimal(str(command.slippage_bps)) / 10000)
                raw_price += direction * (tick or Decimal(0)) * tick_count
                quote_step = tick if tick is not None else Decimal(1).scaleb(-quote_scale)
                rounding = ROUND_CEILING if direction == 1 else ROUND_FLOOR
                price = (raw_price / quote_step).to_integral_value(rounding=rounding) * quote_step
                if price <= 0:
                    raise SimulationContractError("滑点后日频成交价必须为正")
                reason = reason or _futures_execution_price_limit_reason(row, price=price)
                if command.limit_price is not None:
                    limit = Decimal(command.limit_price.units).scaleb(-command.limit_price.scale)
                    if (command.side == "buy" and price > limit) or (command.side == "sell" and price < limit):
                        reason = reason or "limit_not_reached"
                position = ledger.position
                if command.position_effect == "open" and position is not None and (
                    position.contract_code != code or (position.contracts > 0) != (command.side == "buy")
                ):
                    reason = reason or "open_requires_flat_or_same_direction"
                quantity = 0 if reason else min(requested, capacity)
                own = sum(item.cash_units + item.margin_units for item in ledger.order_reservations if item.order_id == command.order_id)
                other = sum(item.cash_units + item.margin_units for item in ledger.order_reservations) - own
                old = ledger.positions.get(command.instrument.instrument_hash)
                def facts(count):
                    fee = _fee_fen(price, rule.multiplier, count, self._rate(command, rule), fee_unit=rule.fee_unit)
                    old_margin = 0 if old is None else old.margin_units
                    if command.position_effect == "open":
                        total = count + (0 if old is None else abs(old.contracts))
                        margin = _margin_fen(price, rule.multiplier, total, rule.margin_rate_pct)
                        pnl = 0
                    else:
                        margin = old_margin * (abs(old.contracts) - count) // abs(old.contracts)
                        scale = max(old.price_scale, -price.as_tuple().exponent)
                        pnl, _ = ledger._priced_position(instrument_hash=command.instrument.instrument_hash,
                            contract_code=code, price=price)[0].realize(price_units=int(Fraction(price) * 10 ** scale),
                            contracts=count if old.contracts > 0 else -count, multiplier=rule.multiplier)
                    return fee, margin, pnl, old_margin
                def affordable(count):
                    fee, margin, pnl, old_margin = facts(count)
                    return FuturesAccountCore.available(
                        equity_units=ledger.equity_units + pnl - fee,
                        margin_units=ledger.margin_units + margin - old_margin,
                        frozen_units=other,
                    ) >= 0
                if quantity:
                    affordable_quantity = self._maximum(quantity, affordable)
                    if affordable_quantity < quantity:
                        reason = "insufficient_free_equity"
                        quantity = affordable_quantity if command.funds_policy == "resize" else 0
                observation = {"order_id": command.order_id, "instrument": command.instrument.to_dict(),
                    "trading_date": session.isoformat(), "event_start": at.isoformat(), "event_time": at.isoformat(),
                    "reference_price": str(opening), "execution_price": str(price),
                    "visible_capacity": int(row["visible_capacity"]) if supplied else None,
                    "capacity_model": "visible_capacity" if supplied else "assumed_unbounded",
                    "capacity_before": capacity if supplied else None,
                    "rules_identity_hash": rule.rule_snapshot_hash, "parameters": rule.to_dict(),
                    "market_status": {name: (bool(row[name]) if name in {"paused", "tradable", "no_trade"} else str(row[name]))
                                      for name in ("paused", "tradable", "no_trade", "high_limit", "low_limit")
                                      if name in row and pd.notna(row[name])},
                    "slippage_bps": str(command.slippage_bps),
                    "slippage_ticks": tick_count, "tick_size": None if tick is None else str(tick),
                    "quote_price_scale": quote_scale, "quote_price_scale_basis": quote_scale_basis,
                    "price_rounding_policy": "adverse_direction",
                    "filled_quantity": quantity, "reason": reason or ("visible_capacity_exhausted" if not quantity else None)}
                self.context["observations"].append(observation)
                if quantity:
                    self._release(ledger, command, at, rule.rule_snapshot_hash)
                    fee, margin, _, _ = facts(quantity)
                    scale = max(2, -price.as_tuple().exponent, 0 if old is None else old.price_scale)
                    fill_id = typed_canonical_hash({"command_hash": command.command_hash,
                        "event_time": at.isoformat(), "filled_before": order.filled_quantity})
                    event = self._event(ledger, command, "fill", at, rule.rule_snapshot_hash,
                        {"order_id": command.order_id, "instrument_hash": command.instrument.instrument_hash,
                         "contract_code": code, "contracts_delta": quantity if command.side == "buy" else -quantity,
                         "position_effect": command.position_effect, "settlement_price_units": int(Fraction(price) * 10 ** scale),
                         "price_scale": scale, "multiplier": rule.multiplier, "fee_units": fee,
                         "position_margin_units": margin}, event_id=fill_id)
                    append_fill(ledger=ledger, instrument=command.instrument, side=command.side, quantity=quantity,
                        effect=command.position_effect, price=price, rule=rule, rule_snapshot_hash=rule.rule_snapshot_hash,
                        instrument_hash=command.instrument.instrument_hash, intent_hash=command.command_hash, fee=fee,
                        order_id=command.order_id, fill_id=fill_id, at=at, intent=None,
                        execution_reason="explicit_order", financial_event=event)
                    self.context["events"].append(event.to_dict())
                    self.context["fee_facts"].append({"fill_id": fill_id, "order_id": command.order_id,
                        "quantity": quantity, "fee_fen": fee, "fee_rate": str(self._rate(command, rule)),
                        "fee_unit": rule.fee_unit, "rules_identity_hash": rule.rule_snapshot_hash,
                        "rounding_policy": "half_up_per_event"})
                    capacity -= quantity
                    if supplied:
                        capacities[code] = capacity
                    self.engine.broker.advance(command.order_id, "fill", at, quantity=quantity)
                    remaining = accepted_quantity - quantity
                    self.active[command.order_id] = (command, submitted_rule, remaining)
                    if remaining and command.time_in_force == "DAY":
                        free = ledger.publish().free_equity_units
                        if sum(self._reservation(command, submitted_rule, remaining)) <= free:
                            self._reserve(ledger, command, submitted_rule, remaining, at)
                        else:
                            self._finish(ledger, command, at, "insufficient_remainder_equity")
                            continue
                current = self.engine.broker.orders[command.order_id]
                if current.status == "filled":
                    self._finish(ledger, command, at, None)
                elif command.time_in_force == "IOC":
                    self._finish(ledger, command, at, reason or "ioc_remainder_cancelled")
                elif self.active[command.order_id][2] == 0:
                    self._finish(ledger, command, at, "funds_resized")

    def close_session(self, session, at, ledger, rule_rows, rejections):
        self._process(at, session, ledger, rule_rows, rejections)
        for command, _, _ in tuple(self.active.values()):
            self._finish(ledger, command, at, "session_end",
                         "expire" if command.time_in_force == "DAY" else "cancel")
        self.context["session_ends"].append({"trading_date": session.isoformat(), "at": at.isoformat()})

    def require_finished(self):
        if self.cursor != len(self.commands) or self.active:
            raise SimulationContractError("日频期货显式命令没有完整消费")


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
        column for column in ("paused", "tradable", "no_trade", "high_limit", "low_limit",
                              "visible_capacity", "execution_time", "open_available_at", "available_at", "price_scale")
        if column in frame.columns
    ]
    result = frame[list(required) + optional].copy()
    result["date"] = pd.to_datetime(result["date"]).dt.date
    return result.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)


def _futures_execution_price_limit_reason(row: pd.Series, *, price: Decimal) -> str | None:
    for column, upper in (("high_limit", True), ("low_limit", False)):
        bound = row.get(column)
        if bound is not None and pd.notna(bound):
            bound = _positive_decimal(bound, "涨跌停价")
            if (upper and price > bound) or (not upper and price < bound):
                return "execution_price_outside_limit"
    return None


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


def _frame_payload(frame: pd.DataFrame) -> Iterator[dict[str, object]]:
    columns = tuple(str(column) for column in frame.columns)
    # 延续逐行读取的公共 dtype，保持纯数值混合列原有的身份字节。
    values = frame.to_numpy(copy=False)
    rows = frame.itertuples(index=False, name=None) if values.dtype.kind == "M" else values
    for row in rows:
        yield {column: _scalar(value) for column, value in zip(columns, row)}


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
