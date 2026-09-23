"""正式仿真结果的最小公共事实合同。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import os
import shutil
import uuid
from typing import Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.platform import (
    canonical_json,
    typed_canonical_bytes,
    typed_canonical_hash,
)
from research_pipeline.domain.simulation_result import (
    CANONICAL_SIMULATION_COLUMNS,
    CANONICAL_SIMULATION_INTEGER_COLUMNS,
    CANONICAL_SIMULATION_TIMESTAMP_COLUMNS,
)

from .orders import SimulationContractError


SIMULATION_RESULT_CONTRACT_VERSION = "research-simulation-result-v1"
SIMULATION_RESULT_SEMANTICS_VERSION = "research-simulation-result-semantics-v1"
CANONICAL_SIMULATION_TABLES = (
    "orders",
    "fills",
    "positions",
    "cash",
    "costs",
    "valuations",
)
_EMPTY_NON_TRADE_SOURCE_HASH = typed_canonical_hash([])

_REQUIRED_COLUMNS = CANONICAL_SIMULATION_COLUMNS
_TIMESTAMP_COLUMNS = CANONICAL_SIMULATION_TIMESTAMP_COLUMNS
_INTEGER_COLUMNS = CANONICAL_SIMULATION_INTEGER_COLUMNS


def _arrow_schema(name: str) -> pa.Schema:
    """六表唯一物理 schema；空表与非空表必须具有相同类型。"""

    fields = []
    for column in sorted(_REQUIRED_COLUMNS[name]):
        if column == "session":
            data_type = pa.date32()
        elif column in _TIMESTAMP_COLUMNS:
            data_type = pa.timestamp("ns", tz="Asia/Shanghai")
        elif column in _INTEGER_COLUMNS:
            data_type = pa.int64()
        else:
            data_type = pa.string()
        fields.append(pa.field(column, data_type, nullable=True))
    return pa.schema(fields)


_ARROW_SCHEMAS = {
    name: _arrow_schema(name) for name in CANONICAL_SIMULATION_TABLES
}


def _schema_payload(name: str) -> list[dict[str, object]]:
    return [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in _ARROW_SCHEMAS[name]
    ]


@dataclass(frozen=True)
class SimulationResultSemantics:
    asset_class: str
    frequency: str
    decision_time_convention: str
    execution_time_convention: str
    valuation_time_convention: str
    price_convention: str
    fee_model_version: str
    calendar_id: str
    settlement_policy_id: str
    missing_data_policy: str
    negative_cash_allowed: bool
    timeline_semantics_hash: str
    contract_version: str = SIMULATION_RESULT_SEMANTICS_VERSION

    def __post_init__(self) -> None:
        for field in (
            "asset_class", "frequency", "decision_time_convention",
            "execution_time_convention", "valuation_time_convention",
            "price_convention", "fee_model_version", "calendar_id",
            "settlement_policy_id", "missing_data_policy",
        ):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise SimulationContractError(f"{field} 不能为空")
        _require_hash(self.timeline_semantics_hash, "timeline_semantics_hash")
        if type(self.negative_cash_allowed) is not bool:
            raise SimulationContractError("negative_cash_allowed 必须是布尔值")
        if self.contract_version != SIMULATION_RESULT_SEMANTICS_VERSION:
            raise SimulationContractError("SimulationResultSemantics 版本无效")

    def to_dict(self) -> dict[str, object]:
        return self.__dict__.copy()

    @property
    def semantics_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "SimulationResultSemantics":
        if set(payload) != set(cls.__dataclass_fields__):
            raise SimulationContractError("SimulationResultSemantics schema 无效")
        try:
            return cls(**payload)
        except TypeError as exc:
            raise SimulationContractError("SimulationResultSemantics 字段无效") from exc


@dataclass(frozen=True)
class SimulationResultContract:
    tables: Mapping[str, pd.DataFrame]
    semantics: SimulationResultSemantics
    source_simulation_hash: str
    contract_version: str = SIMULATION_RESULT_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_hash(self.source_simulation_hash, "source_simulation_hash")
        if self.contract_version != SIMULATION_RESULT_CONTRACT_VERSION:
            raise SimulationContractError("仿真结果合同版本无效")
        if set(self.tables) != set(CANONICAL_SIMULATION_TABLES):
            raise SimulationContractError("仿真结果必须完整包含六张规范表")
        verify_simulation_result_contract(self)

    @property
    def table_hashes(self) -> dict[str, str]:
        return {
            name: _frame_hash(name, self.tables[name])
            for name in CANONICAL_SIMULATION_TABLES
        }

    @property
    def result_hash(self) -> str:
        return typed_canonical_hash({
            "contract_version": self.contract_version,
            "semantics_hash": self.semantics.semantics_hash,
            "source_simulation_hash": self.source_simulation_hash,
            "table_hashes": self.table_hashes,
        })

    @property
    def manifest(self) -> dict[str, object]:
        body = {
            "contract_version": self.contract_version,
            "semantics": self.semantics.to_dict(),
            "semantics_hash": self.semantics.semantics_hash,
            "source_simulation_hash": self.source_simulation_hash,
            "table_hashes": self.table_hashes,
            "table_rows": {
                name: len(self.tables[name]) for name in CANONICAL_SIMULATION_TABLES
            },
            "table_schemas": {
                name: _schema_payload(name) for name in CANONICAL_SIMULATION_TABLES
            },
            "result_hash": self.result_hash,
        }
        return {**body, "manifest_hash": typed_canonical_hash(body)}


def build_simulation_result_contract(
    *,
    tables: Mapping[str, pd.DataFrame],
    semantics: SimulationResultSemantics,
    source_simulation_hash: str,
) -> SimulationResultContract:
    """复制规范表并冻结列顺序，调用方不能在验证后原地改写事实。"""

    normalized = {}
    for name in CANONICAL_SIMULATION_TABLES:
        if name not in tables or not isinstance(tables[name], pd.DataFrame):
            raise SimulationContractError(f"缺少规范表: {name}")
        normalized[name] = _normalize_table(name, tables[name])
    return SimulationResultContract(normalized, semantics, source_simulation_hash)


def canonical_simulation_table(
    name: str,
    rows: list[Mapping[str, object]],
) -> pd.DataFrame:
    """即使没有行也保留稳定 schema，避免空成交窗口丢失合同。"""

    if name not in _REQUIRED_COLUMNS:
        raise SimulationContractError(f"未知规范仿真表: {name}")
    return _normalize_table(
        name,
        pd.DataFrame(rows, columns=sorted(_REQUIRED_COLUMNS[name])),
    )


def project_cash_daily_result(
    *,
    result: object,
    intents: pd.DataFrame,
    asset_class: str,
    timeline_semantics: Mapping[str, object],
    fee_model_version: str,
    calendar_id: str,
    settlement_policy_id: str,
) -> SimulationResultContract:
    """把日频现金引擎已有事实无损投影到公共合同。"""

    if asset_class not in {"cn_stock", "cn_etf"}:
        raise SimulationContractError("现金日频结果资产类别无效")
    orders = result.orders.copy()
    fills = result.trades.copy()
    positions = result.position_snapshots.copy()
    cash = result.cash_snapshots.copy()
    nav = result.nav.copy()
    intent_time = {
        str(row.target_hash): (
            _require_aware(row.decision_time, "decision_time"),
            _require_aware(row.order_time, "order_time"),
        )
        for row in intents.itertuples(index=False)
    }
    instrument_hash = {
        str(row.code): str(row.instrument_hash)
        for row in positions.itertuples(index=False)
    }
    order_rows = []
    for row in orders.itertuples(index=False):
        target_hash = str(row.target_hash)
        if target_hash not in intent_time:
            raise SimulationContractError("现金订单无法绑定目标决策时点")
        requested, filled = int(row.requested_quantity), int(row.filled_quantity)
        status = _terminal_status(requested, filled)
        order_rows.append({
            "portfolio_id": "default",
            "order_id": str(row.order_id),
            "session": row.session,
            "instrument_id": str(row.code),
            "instrument_hash": instrument_hash[str(row.code)],
            "asset_class": asset_class,
            "side": str(row.side),
            "requested_quantity": requested,
            "filled_quantity": filled,
            "status": status,
            "terminal_reason": None if status == "filled" else str(
                row.reason_code or "formal_simulation_unfilled"
            ),
            "decision_time": intent_time[target_hash][0],
            "submitted_at": intent_time[target_hash][1],
            "source_order_hash": _row_hash(row),
        })
    fill_rows = []
    cost_rows = []
    for row in fills.itertuples(index=False):
        price_units = int(row.notional_units) // int(row.quantity)
        source_hash = str(row.fill_hash)
        fill_rows.append({
            "portfolio_id": "default",
            "fill_id": str(row.fill_id),
            "order_id": str(row.order_id),
            "session": row.session,
            "instrument_id": str(row.code),
            "instrument_hash": instrument_hash[str(row.code)],
            "asset_class": asset_class,
            "side": str(row.side),
            "quantity": int(row.quantity),
            "fill_time": row.fill_time,
            "execution_price_units": price_units,
            "price_scale": 2,
            "contract_multiplier": 1,
            "notional_units": int(row.notional_units),
            "fee_units": int(row.fee_units),
            "realized_pnl_units": 0,
            "position_effect": "auto",
            "source_fill_hash": source_hash,
        })
        cost_rows.append({
            "portfolio_id": "default",
            "cost_id": typed_canonical_hash({"fill_id": str(row.fill_id), "cost": "transaction"}),
            "fill_id": str(row.fill_id),
            "session": row.session,
            "cost_type": "transaction_fee",
            "amount_units": int(row.fee_units),
            "currency": "CNY",
            "source_cost_hash": source_hash,
        })
    snapshot_by_session = {}
    position_rows = []
    ledger_for_sources = result.ledger.copy()
    source_hashes_by_session: dict[object, list[str]] = {}
    if not ledger_for_sources.empty:
        ledger_for_sources["_session_date"] = pd.to_datetime(
            ledger_for_sources["session"]
        ).dt.date
        for session_value, payload_hash in ledger_for_sources.loc[
            ledger_for_sources["kind"] != "fill",
            ["_session_date", "payload_hash"],
        ].itertuples(index=False, name=None):
            source_hashes_by_session.setdefault(session_value, []).append(
                str(payload_hash)
            )
    non_trade_hash_by_session = {}
    for session in positions["session"].unique():
        non_trade_hash_by_session[session] = typed_canonical_hash(sorted(
            source_hashes_by_session.get(pd.Timestamp(session).date(), [])
        ))
    for session, frame in positions.groupby("session", sort=True):
        first = frame.iloc[0]
        snapshot_id = _snapshot_id("default", session, first["valuation_time"], first["source_state_hash"])
        snapshot_by_session[session] = snapshot_id
        for row in frame.itertuples(index=False):
            if (
                int(row.quantity) == 0
                and int(row.trade_quantity_change) == 0
                and int(row.non_trade_quantity_change) == 0
            ):
                continue
            position_rows.append({
                "portfolio_id": "default",
                "snapshot_id": snapshot_id,
                "session": row.session,
                "valuation_time": row.valuation_time,
                "instrument_id": str(row.code),
                "instrument_hash": str(row.instrument_hash),
                "asset_class": asset_class,
                "quantity": int(row.quantity),
                "sellable_quantity": int(row.sellable_quantity),
                "unsettled_quantity": int(row.unsettled_quantity),
                "frozen_quantity": int(row.frozen_quantity),
                "market_value_units": int(row.market_value_units),
                "trade_quantity_change": int(row.trade_quantity_change),
                "non_trade_quantity_change": int(row.non_trade_quantity_change),
                "source_state_hash": str(row.source_state_hash),
                "non_trade_source_hash": non_trade_hash_by_session[row.session],
            })
    cash_rows = []
    for row in cash.itertuples(index=False):
        snapshot_id = snapshot_by_session[row.session]
        cash_rows.append({
            "portfolio_id": "default",
            "snapshot_id": snapshot_id,
            "session": row.session,
            "valuation_time": row.valuation_time,
            "currency": "CNY",
            "total_cash_units": int(row.total_cash_units),
            "available_cash_units": int(row.available_cash_units),
            "receivable_cash_units": int(row.receivable_cash_units),
            "margin_units": 0,
            "trade_cash_change_units": int(row.trade_cash_change_units),
            "non_trade_cash_change_units": int(row.non_trade_cash_change_units),
            "opening_cash_units": int(row.opening_cash_units),
            "source_state_hash": str(row.source_state_hash),
            "non_trade_source_hash": non_trade_hash_by_session[row.session],
        })
    valuation_rows = []
    cash_by_session = {row["session"]: row for row in cash_rows}
    for row in nav.itertuples(index=False):
        cash_row = cash_by_session[row.session]
        valuation_rows.append({
            "portfolio_id": "default",
            "snapshot_id": snapshot_by_session[row.session],
            "session": row.session,
            "valuation_time": cash_row["valuation_time"],
            "nav_units": int(row.nav_units),
            "currency": "CNY",
            "valuation_model": "cash_plus_position_market_value",
            "source_state_hash": str(row.state_hash),
        })
    semantics = SimulationResultSemantics(
        asset_class=asset_class,
        frequency="daily",
        decision_time_convention="declared_portfolio_target_time",
        execution_time_convention="next_exchange_session_open",
        valuation_time_convention="exchange_session_close",
        price_convention="integer_cny_cent",
        fee_model_version=fee_model_version,
        calendar_id=calendar_id,
        settlement_policy_id=settlement_policy_id,
        missing_data_policy="fail_closed",
        negative_cash_allowed=False,
        timeline_semantics_hash=typed_canonical_hash(timeline_semantics),
    )
    return build_simulation_result_contract(
        tables={
            "orders": pd.DataFrame(order_rows, columns=sorted(_REQUIRED_COLUMNS["orders"])),
            "fills": pd.DataFrame(fill_rows, columns=sorted(_REQUIRED_COLUMNS["fills"])),
            "positions": pd.DataFrame(position_rows, columns=sorted(_REQUIRED_COLUMNS["positions"])),
            "cash": pd.DataFrame(cash_rows, columns=sorted(_REQUIRED_COLUMNS["cash"])),
            "costs": pd.DataFrame(cost_rows, columns=sorted(_REQUIRED_COLUMNS["costs"])),
            "valuations": pd.DataFrame(valuation_rows, columns=sorted(_REQUIRED_COLUMNS["valuations"])),
        },
        semantics=semantics,
        source_simulation_hash=str(result.simulation_hash),
    )


def project_futures_daily_result(
    *,
    result: object,
    timeline_semantics: Mapping[str, object],
    fee_model_version: str,
    calendar_id: str,
    settlement_policy_id: str,
    initial_cash_units: int,
) -> SimulationResultContract:
    """把逐日盯市期货结果投影到公共合同，不改变成交或结算事实。"""

    intents = result.intents.copy()
    fills = result.fills.copy()
    settlements = result.settlements.copy()
    rejections = getattr(result, "rejections", pd.DataFrame()).copy()
    intent_by_order = {str(row.order_id): row for row in intents.itertuples(index=False)}
    fills_by_order = {} if fills.empty else {
        str(order_id): frame for order_id, frame in fills.groupby("order_id", sort=True)
    }
    rejection_by_order = {} if rejections.empty else {
        str(row.order_id): row for row in rejections.itertuples(index=False)
    }
    order_rows = []
    instrument_hash_by_id: dict[str, str] = {}
    for order_id, raw in intent_by_order.items():
        payload = json.loads(str(raw.intent_json))
        instrument = payload["instrument"]
        instrument_id = str(instrument["instrument_id"])
        instrument_hash = typed_canonical_hash(instrument)
        instrument_hash_by_id[instrument_id] = instrument_hash
        frame = fills_by_order.get(order_id)
        filled = 0 if frame is None else int(frame["quantity"].sum())
        rejection = rejection_by_order.get(order_id)
        status = "rejected" if rejection is not None and filled == 0 else _terminal_status(int(raw.quantity), filled)
        first_fill = None if frame is None else frame.iloc[0]
        session = getattr(raw, "execution_session", None)
        side = getattr(raw, "side", None)
        if session is None:
            session = getattr(rejection, "trading_date", None) if first_fill is None else first_fill["trading_date"]
        if side is None:
            side = getattr(rejection, "side", None) if first_fill is None else first_fill["side"]
        if session is None or side is None:
            raise SimulationContractError("期货订单缺少执行会话或方向")
        order_rows.append({
            "portfolio_id": "default", "order_id": order_id,
            "session": session,
            "instrument_hash": instrument_hash, "asset_class": "cn_future",
            "instrument_id": instrument_id,
            "side": str(side),
            "requested_quantity": int(raw.quantity),
            "filled_quantity": filled, "status": status,
            "terminal_reason": (
                str(rejection.reason_code)
                if rejection is not None
                else (None if status == "filled" else "formal_simulation_partial_fill")
            ),
            "decision_time": payload["decision_time"], "submitted_at": payload["order_time"],
            "source_order_hash": _row_hash(raw),
        })
    for order_id, frame in fills_by_order.items():
        if order_id not in intent_by_order:
            first = frame.iloc[0]
            instrument_id = str(first["actual_contract"])
            decision_time = first["fill_time"]
            submitted_at = first["fill_time"]
            requested = int(frame["quantity"].sum())
            source_order_hash = typed_canonical_hash({
                "formal_forced_liquidation_order": order_id,
                "source_simulation_hash": result.result_hash,
            })
            instrument_hash = typed_canonical_hash({
                "instrument_id": instrument_id, "asset_class": "cn_future"
            })
            instrument_hash_by_id[instrument_id] = instrument_hash
            order_rows.append({
                "portfolio_id": "default", "order_id": order_id,
                "session": first["trading_date"], "instrument_id": instrument_id,
                "instrument_hash": instrument_hash, "asset_class": "cn_future",
                "side": str(first["side"]), "requested_quantity": requested,
                "filled_quantity": requested, "status": "filled", "terminal_reason": None,
                "decision_time": decision_time, "submitted_at": submitted_at,
                "source_order_hash": source_order_hash,
            })
    fill_rows = []
    cost_rows = []
    for row in fills.itertuples(index=False):
        instrument_id = str(row.actual_contract)
        instrument_hash = instrument_hash_by_id[instrument_id]
        price_units = int(round(float(row.fill_price) * 100))
        notional = price_units * int(row.quantity) * int(row.multiplier)
        source_hash = _row_hash(row)
        fill_rows.append({
            "portfolio_id": "default", "fill_id": str(row.fill_id),
            "order_id": str(row.order_id), "session": row.trading_date,
            "instrument_id": instrument_id, "instrument_hash": instrument_hash,
            "asset_class": "cn_future", "side": str(row.side),
            "quantity": int(row.quantity), "fill_time": row.fill_time,
            "execution_price_units": price_units, "price_scale": 2,
            "contract_multiplier": int(row.multiplier), "notional_units": notional,
            "fee_units": int(row.fee_fen), "position_effect": str(row.position_effect),
            "realized_pnl_units": int(row.realized_pnl_fen),
            "source_fill_hash": source_hash,
        })
        cost_rows.append({
            "portfolio_id": "default",
            "cost_id": typed_canonical_hash({"fill_id": str(row.fill_id), "cost": "transaction"}),
            "fill_id": str(row.fill_id), "session": row.trading_date,
            "cost_type": "transaction_fee", "amount_units": int(row.fee_fen),
            "currency": "CNY", "source_cost_hash": source_hash,
        })
    running: dict[str, int] = {}
    position_rows = []
    cash_rows = []
    valuation_rows = []
    previous_cash = initial_cash_units
    fills_by_session = {} if fills.empty else {
        session: frame
        for session, frame in fills.groupby("trading_date", sort=False)
    }
    nav = getattr(
        result,
        "nav",
        settlements.rename(columns={
            "equity_fen": "nav_fen",
            "required_margin_fen": "margin_fen",
        })[["trading_date", "nav_fen", "margin_fen"]],
    ).copy()
    for session, settlement_group in settlements.sort_values(
        ["trading_date", "actual_contract"], kind="mergesort"
    ).groupby("trading_date", sort=True):
        session_fills = fills_by_session.get(session, fills.iloc[0:0])
        trade_change_by_id: dict[str, int] = {}
        for fill in session_fills.itertuples(index=False):
            signed = int(fill.quantity) if str(fill.side) == "buy" else -int(fill.quantity)
            instrument = str(fill.actual_contract)
            trade_change_by_id[instrument] = trade_change_by_id.get(instrument, 0) + signed
        previous_running = running.copy()
        for instrument, change in trade_change_by_id.items():
            running[instrument] = running.get(instrument, 0) + change
        expected_positions = {
            str(row.actual_contract): int(row.position)
            for row in settlement_group.itertuples(index=False)
            if int(row.position) != 0
        }
        if {key: value for key, value in running.items() if value != 0} != expected_positions:
            raise SimulationContractError("期货逐合约持仓与结算持仓不一致")
        valuation_time = pd.Timestamp(session).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=17)
        state_hash = typed_canonical_hash([
            _row_hash(row) for row in settlement_group.itertuples(index=False)
        ])
        snapshot_id = _snapshot_id("default", session, valuation_time, state_hash)
        for instrument in sorted(set(previous_running) | set(running) | set(trade_change_by_id)):
            if (
                running.get(instrument, 0) == 0
                and previous_running.get(instrument, 0) == 0
                and trade_change_by_id.get(instrument, 0) == 0
            ):
                continue
            position_rows.append({
                "portfolio_id": "default", "snapshot_id": snapshot_id,
                "session": session, "valuation_time": valuation_time,
                "instrument_id": instrument,
                "instrument_hash": instrument_hash_by_id[instrument],
                "asset_class": "cn_future", "quantity": running.get(instrument, 0),
                "sellable_quantity": 0, "unsettled_quantity": 0, "frozen_quantity": 0,
                "market_value_units": 0,
                "trade_quantity_change": trade_change_by_id.get(instrument, 0),
                "non_trade_quantity_change": 0, "source_state_hash": state_hash,
                "non_trade_source_hash": state_hash,
            })
        running = {instrument: quantity for instrument, quantity in running.items() if quantity != 0}
        trade_cash_change = 0 if session_fills.empty else (
            int(session_fills["realized_pnl_fen"].sum())
            - int(session_fills["fee_fen"].sum())
        )
        mtm_change = int(settlement_group["mtm_pnl_fen"].sum())
        nav_rows = nav.loc[pd.to_datetime(nav["trading_date"]).dt.date == pd.Timestamp(session).date()]
        if len(nav_rows) != 1:
            raise SimulationContractError("期货组合会话净值缺失或重叠")
        nav_row = nav_rows.iloc[0]
        equity = int(nav_row["nav_fen"])
        if equity != previous_cash + trade_cash_change + mtm_change:
            raise SimulationContractError("期货权益变化与逐日盯市、已实现盈亏和费用不一致")
        cash_rows.append({
            "portfolio_id": "default", "snapshot_id": snapshot_id,
            "session": session, "valuation_time": valuation_time, "currency": "CNY",
            "total_cash_units": equity,
            "available_cash_units": int(nav_row.get("free_equity_fen", equity - int(nav_row["margin_fen"]))),
            "receivable_cash_units": 0, "margin_units": int(nav_row["margin_fen"]),
            "trade_cash_change_units": trade_cash_change,
            "non_trade_cash_change_units": mtm_change,
            "opening_cash_units": initial_cash_units, "source_state_hash": state_hash,
            "non_trade_source_hash": state_hash,
        })
        valuation_rows.append({
            "portfolio_id": "default", "snapshot_id": snapshot_id,
            "session": session, "valuation_time": valuation_time,
            "nav_units": equity, "currency": "CNY",
            "valuation_model": "futures_settlement_equity", "source_state_hash": state_hash,
        })
        previous_cash = equity
    semantics = SimulationResultSemantics(
        asset_class="cn_future", frequency="daily",
        decision_time_convention="declared_portfolio_target_time",
        execution_time_convention="next_exchange_session_open_or_forced_settlement",
        valuation_time_convention="exchange_settlement_event",
        price_convention="integer_cny_cent_times_contract_multiplier",
        fee_model_version=fee_model_version, calendar_id=calendar_id,
        settlement_policy_id=settlement_policy_id, missing_data_policy="fail_closed",
        negative_cash_allowed=False,
        timeline_semantics_hash=typed_canonical_hash(timeline_semantics),
    )
    return build_simulation_result_contract(
        tables={
            "orders": pd.DataFrame(order_rows, columns=sorted(_REQUIRED_COLUMNS["orders"])),
            "fills": pd.DataFrame(fill_rows, columns=sorted(_REQUIRED_COLUMNS["fills"])),
            "positions": pd.DataFrame(position_rows, columns=sorted(_REQUIRED_COLUMNS["positions"])),
            "cash": pd.DataFrame(cash_rows, columns=sorted(_REQUIRED_COLUMNS["cash"])),
            "costs": pd.DataFrame(cost_rows, columns=sorted(_REQUIRED_COLUMNS["costs"])),
            "valuations": pd.DataFrame(valuation_rows, columns=sorted(_REQUIRED_COLUMNS["valuations"])),
        },
        semantics=semantics,
        source_simulation_hash=str(result.result_hash),
    )


def verify_simulation_result_contract(result: SimulationResultContract) -> None:
    tables = result.tables
    for name in CANONICAL_SIMULATION_TABLES:
        frame = tables[name]
        missing = _REQUIRED_COLUMNS[name] - set(frame.columns)
        if missing:
            raise SimulationContractError(f"{name} 缺少字段: {sorted(missing)}")
    _require_unique(tables["orders"], ["portfolio_id", "order_id"], "orders")
    _require_unique(tables["fills"], ["portfolio_id", "fill_id"], "fills")
    _require_unique(tables["positions"], ["portfolio_id", "snapshot_id", "instrument_hash"], "positions")
    _require_unique(tables["cash"], ["portfolio_id", "snapshot_id"], "cash")
    _require_unique(tables["costs"], ["portfolio_id", "cost_id"], "costs")
    _require_unique(tables["valuations"], ["portfolio_id", "snapshot_id"], "valuations")
    declared_assets = {
        str(value)
        for name in ("orders", "fills", "positions")
        for value in tables[name]["asset_class"]
    }
    if declared_assets and declared_assets != {result.semantics.asset_class}:
        raise SimulationContractError("规范表资产类别与 SimulationSemantics 不一致")
    _verify_orders_and_fills(tables["orders"], tables["fills"])
    _verify_costs(tables["fills"], tables["costs"])
    _verify_currencies(tables["costs"], tables["cash"], tables["valuations"])
    _verify_snapshots(
        tables["fills"], tables["positions"], tables["cash"], tables["valuations"],
        semantics=result.semantics,
    )


def write_simulation_result_contract(
    result: SimulationResultContract,
    output_root: str | Path,
) -> dict[str, object]:
    """原子写入六张规范表；manifest 绑定 schema、语义和逐表内容。"""

    root = Path(output_root).resolve()
    staging = root.parent / f".{root.name}.staging-{uuid.uuid4().hex}"
    if root.exists():
        raise FileExistsError(f"仿真结果合同已存在: {root}")
    staging.mkdir(parents=True)
    try:
        for name in CANONICAL_SIMULATION_TABLES:
            directory = staging / name
            directory.mkdir()
            pq.write_table(
                _to_arrow_table(name, result.tables[name]),
                directory / "part-00000.parquet",
            )
        manifest = result.manifest
        (staging / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
        (staging / "COMMITTED").write_text(str(manifest["manifest_hash"]), encoding="ascii")
        os.replace(staging, root)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    for name in CANONICAL_SIMULATION_TABLES:
        path = root / name / "part-00000.parquet"
        if (
            pq.read_schema(path) != _ARROW_SCHEMAS[name]
            or pq.read_metadata(path).num_rows != len(result.tables[name])
        ):
            raise SimulationContractError(
                f"{name} 写入后的 schema 或行数不闭合"
            )
    # 输入 ResultContract 已在构造时完成金融校验；消费或 resume 时再做
    # 独立全表复核，避免写入阶段同时保留原 DataFrame 和读回副本。
    return manifest


class SimulationResultArtifactWriter:
    """逐交易日写入六表，避免 Runtime 为了发布工件重新合并完整结果。"""

    def __init__(self, output_root: str | Path) -> None:
        self.root = Path(output_root).resolve()
        if self.root.exists():
            raise FileExistsError(f"仿真结果合同已存在: {self.root}")
        self._staging = (
            self.root.parent / f".{self.root.name}.staging-{uuid.uuid4().hex}"
        )
        self._staging.mkdir(parents=True)
        self._digests: dict[str, object] = {}
        self._row_counts = {name: 0 for name in CANONICAL_SIMULATION_TABLES}
        self._part_counts = {name: 0 for name in CANONICAL_SIMULATION_TABLES}
        self._closed = False
        for name in CANONICAL_SIMULATION_TABLES:
            (self._staging / name).mkdir()
            digest = hashlib.sha256()
            digest.update(typed_canonical_bytes({
                "table_hash_contract": "research-simulation-table-hash-v2",
                "schema": _schema_payload(name),
            }))
            self._digests[name] = digest

    def append_rows(
        self,
        rows: Mapping[str, list[Mapping[str, object]] | tuple[Mapping[str, object], ...]],
    ) -> None:
        if self._closed:
            raise SimulationContractError("仿真结果 writer 已经关闭")
        if set(rows) != set(CANONICAL_SIMULATION_TABLES):
            raise SimulationContractError("仿真结果分区必须完整提供六表行")
        for name in CANONICAL_SIMULATION_TABLES:
            values = list(rows[name])
            if not values:
                continue
            frame = canonical_simulation_table(name, values)
            table = _to_arrow_table(name, frame)
            part_index = self._part_counts[name]
            pq.write_table(
                table,
                self._staging / name / f"part-{part_index:05d}.parquet",
            )
            digest = self._digests[name]
            for row in frame.itertuples(index=False, name=None):
                digest.update(typed_canonical_bytes({
                    str(column): _scalar(value)
                    for column, value in zip(frame.columns, row, strict=True)
                }))
            self._row_counts[name] += len(frame)
            self._part_counts[name] += 1

    def finalize(
        self,
        *,
        semantics: SimulationResultSemantics,
        source_simulation_hash: str,
    ) -> dict[str, object]:
        if self._closed:
            raise SimulationContractError("仿真结果 writer 只能 finalize 一次")
        _require_hash(source_simulation_hash, "source_simulation_hash")
        self._closed = True
        try:
            for name in CANONICAL_SIMULATION_TABLES:
                if self._part_counts[name] == 0:
                    pq.write_table(
                        pa.Table.from_pylist([], schema=_ARROW_SCHEMAS[name]),
                        self._staging / name / "part-00000.parquet",
                    )
                    self._part_counts[name] = 1
            table_hashes = {
                name: self._digests[name].hexdigest()
                for name in CANONICAL_SIMULATION_TABLES
            }
            result_hash = typed_canonical_hash({
                "contract_version": SIMULATION_RESULT_CONTRACT_VERSION,
                "semantics_hash": semantics.semantics_hash,
                "source_simulation_hash": source_simulation_hash,
                "table_hashes": table_hashes,
            })
            body = {
                "contract_version": SIMULATION_RESULT_CONTRACT_VERSION,
                "semantics": semantics.to_dict(),
                "semantics_hash": semantics.semantics_hash,
                "source_simulation_hash": source_simulation_hash,
                "table_hashes": table_hashes,
                "table_rows": dict(self._row_counts),
                "table_schemas": {
                    name: _schema_payload(name)
                    for name in CANONICAL_SIMULATION_TABLES
                },
                "result_hash": result_hash,
            }
            manifest = {**body, "manifest_hash": typed_canonical_hash(body)}
            (self._staging / "manifest.json").write_text(
                canonical_json(manifest), encoding="utf-8"
            )
            (self._staging / "COMMITTED").write_text(
                str(manifest["manifest_hash"]), encoding="ascii"
            )
            os.replace(self._staging, self.root)
            return manifest
        except BaseException:
            shutil.rmtree(self._staging, ignore_errors=True)
            raise

    def abort(self) -> None:
        if self.root.exists():
            return
        self._closed = True
        shutil.rmtree(self._staging, ignore_errors=True)


def verify_simulation_result_artifact(
    path: str | Path,
    *,
    max_live_bytes: int = 512 * 1024**2,
) -> SimulationResultContract:
    root = Path(path).resolve()
    if type(max_live_bytes) is not int or max_live_bytes <= 0:
        raise SimulationContractError("仿真结果复核内存预算必须是正整数")
    required_paths = [root / "manifest.json", root / "COMMITTED"]
    partition_paths = {
        name: tuple(sorted((root / name).glob("part-*.parquet")))
        for name in CANONICAL_SIMULATION_TABLES
    }
    if (
        any(not item.is_file() for item in required_paths)
        or any(not paths for paths in partition_paths.values())
    ):
        raise SimulationContractError("仿真结果工件缺少规范表、manifest 或提交标记")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    committed = (root / "COMMITTED").read_text(encoding="ascii")
    unsigned = {key: value for key, value in manifest.items() if key != "manifest_hash"}
    if committed != manifest.get("manifest_hash") or typed_canonical_hash(unsigned) != committed:
        raise SimulationContractError("仿真结果 manifest 或提交标记损坏")
    semantics = SimulationResultSemantics.from_dict(manifest.get("semantics", {}))
    uncompressed_bytes = 0
    for name, paths in partition_paths.items():
        for partition_path in paths:
            parquet = pq.ParquetFile(partition_path)
            if parquet.schema_arrow != _ARROW_SCHEMAS[name]:
                raise SimulationContractError(f"{name} 物理 schema 与规范不一致")
            metadata = parquet.metadata
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                uncompressed_bytes += sum(
                    int(row_group.column(index).total_uncompressed_size)
                    for index in range(row_group.num_columns)
                )
    # 六张 Arrow 表、拼接视图和 pandas 结果会同时存活；先用
    # footer 的实际未压缩列块字节做保守包络，再读取任何数据页。
    if uncompressed_bytes * 4 > max_live_bytes:
        raise SimulationContractError("仿真结果完整复核超过内存支持包络")
    tables = {}
    for name in CANONICAL_SIMULATION_TABLES:
        parts = []
        for partition_path in partition_paths[name]:
            part = pq.read_table(partition_path)
            if part.schema != _ARROW_SCHEMAS[name]:
                raise SimulationContractError(f"{name} 物理 schema 与规范不一致")
            parts.append(part)
        arrow_table = pa.concat_tables(parts)
        tables[name] = arrow_table.to_pandas()
    if manifest.get("table_schemas") != {
        name: _schema_payload(name) for name in CANONICAL_SIMULATION_TABLES
    }:
        raise SimulationContractError("仿真结果物理 schema 与 manifest 不一致")
    if {
        name: _frame_hash(name, tables[name]) for name in CANONICAL_SIMULATION_TABLES
    } != manifest.get("table_hashes"):
        raise SimulationContractError("仿真结果表摘要与 manifest 不一致")
    result = build_simulation_result_contract(
        tables=tables,
        semantics=semantics,
        source_simulation_hash=str(manifest.get("source_simulation_hash")),
    )
    if result.manifest != manifest:
        raise SimulationContractError("仿真结果表、语义或摘要与 manifest 不一致")
    return result


def _verify_orders_and_fills(orders: pd.DataFrame, fills: pd.DataFrame) -> None:
    order_index = {
        (str(row.portfolio_id), str(row.order_id)): row
        for row in orders.itertuples(index=False)
    }
    order_keys = set(order_index)
    fill_keys = {
        (str(row.portfolio_id), str(row.order_id)) for row in fills.itertuples(index=False)
    }
    missing = fill_keys - order_keys
    if missing:
        raise SimulationContractError(f"fills 引用了不存在的 order: {sorted(missing)}")
    allowed_status = {"filled", "partially_filled", "rejected"}
    grouped = (
        fills.groupby(["portfolio_id", "order_id"], dropna=False)["quantity"].sum().to_dict()
        if not fills.empty else {}
    )
    for row in orders.itertuples(index=False):
        if str(row.side) not in {"buy", "sell"}:
            raise SimulationContractError("订单方向无效")
        requested = _positive_int(row.requested_quantity, "requested_quantity")
        declared = _nonnegative_int(row.filled_quantity, "filled_quantity")
        actual = int(grouped.get((row.portfolio_id, row.order_id), 0))
        if declared != actual or declared > requested:
            raise SimulationContractError("订单成交数量与 fills 不一致")
        status = str(row.status)
        expected = "filled" if declared == requested and requested > 0 else (
            "rejected" if declared == 0 else "partially_filled"
        )
        if status not in allowed_status or status != expected:
            raise SimulationContractError("订单终态与成交数量不一致")
        if status == "filled" and pd.notna(row.terminal_reason):
            raise SimulationContractError("已成交订单不能声明终止原因")
        if status != "filled" and (
            not isinstance(row.terminal_reason, str) or not row.terminal_reason.strip()
        ):
            raise SimulationContractError("未完全成交订单必须声明终止原因")
        _require_aware(row.decision_time, "decision_time")
        submitted = _require_aware(row.submitted_at, "submitted_at")
        if _require_aware(row.decision_time, "decision_time") > submitted:
            raise SimulationContractError("订单决策晚于提交")
        _require_hash(str(row.source_order_hash), "source_order_hash")
    for row in fills.itertuples(index=False):
        order = order_index[(str(row.portfolio_id), str(row.order_id))]
        if (
            str(row.instrument_id), str(row.instrument_hash), str(row.asset_class), str(row.side)
        ) != (
            str(order.instrument_id), str(order.instrument_hash), str(order.asset_class), str(order.side)
        ):
            raise SimulationContractError("fill 与 order 的标的、资产类别或方向不一致")
        if row.session != order.session:
            raise SimulationContractError("fill 与 order 的交易会话不一致")
        quantity = _positive_int(row.quantity, "fill.quantity")
        price = _positive_int(row.execution_price_units, "execution_price_units")
        scale = _nonnegative_int(row.price_scale, "price_scale")
        multiplier = _positive_int(row.contract_multiplier, "contract_multiplier")
        expected_notional = price * quantity * multiplier
        if int(row.notional_units) != expected_notional:
            raise SimulationContractError("成交额与价格、数量、合约乘数不一致")
        if scale > 9:
            raise SimulationContractError("price_scale 超出支持范围")
        _nonnegative_int(row.fee_units, "fee_units")
        _integer(row.realized_pnl_units, "realized_pnl_units")
        fill_time = _require_aware(row.fill_time, "fill_time")
        if fill_time < _require_aware(order.submitted_at, "submitted_at"):
            raise SimulationContractError("fill 早于订单提交")
        if str(row.position_effect) not in {
            "auto", "open", "close", "close_today", "close_yesterday"
        }:
            raise SimulationContractError("fill position_effect 无效")
        _require_hash(str(row.source_fill_hash), "source_fill_hash")


def _verify_costs(fills: pd.DataFrame, costs: pd.DataFrame) -> None:
    fill_rows = {
        (str(row.portfolio_id), str(row.fill_id)): row
        for row in fills.itertuples(index=False)
    }
    fill_index = {key: int(row.fee_units) for key, row in fill_rows.items()}
    seen: dict[tuple[str, str], int] = {}
    for row in costs.itertuples(index=False):
        key = (str(row.portfolio_id), str(row.fill_id))
        if key not in fill_index:
            raise SimulationContractError("costs 引用了不存在的 fill")
        fill = fill_rows[key]
        if row.session != fill.session:
            raise SimulationContractError("cost 与 fill 的交易会话不一致")
        amount = _nonnegative_int(row.amount_units, "cost.amount_units")
        seen[key] = seen.get(key, 0) + amount
        _require_hash(str(row.source_cost_hash), "source_cost_hash")
    if seen != fill_index:
        raise SimulationContractError("fills 手续费与 costs 不一致")


def _verify_currencies(
    costs: pd.DataFrame,
    cash: pd.DataFrame,
    valuations: pd.DataFrame,
) -> None:
    """当前中国资产合同只接受 CNY，并按组合与会话闭合费用币种。"""

    cash_by_session = {
        (str(row.portfolio_id), row.session): str(row.currency)
        for row in cash.itertuples(index=False)
    }
    for row in cash.itertuples(index=False):
        if str(row.currency) != "CNY":
            raise SimulationContractError("中国资产现金币种必须为 CNY")
    for row in valuations.itertuples(index=False):
        if str(row.currency) != "CNY":
            raise SimulationContractError("中国资产估值币种必须为 CNY")
    for row in costs.itertuples(index=False):
        currency = cash_by_session.get((str(row.portfolio_id), row.session))
        if currency is None or str(row.currency) != currency:
            raise SimulationContractError("costs 币种与同会话现金币种不一致")


def _verify_snapshots(
    fills: pd.DataFrame,
    positions: pd.DataFrame,
    cash: pd.DataFrame,
    valuations: pd.DataFrame,
    *,
    semantics: SimulationResultSemantics,
) -> None:
    cash_keys = {(str(row.portfolio_id), str(row.snapshot_id)) for row in cash.itertuples(index=False)}
    valuation_keys = {
        (str(row.portfolio_id), str(row.snapshot_id)) for row in valuations.itertuples(index=False)
    }
    if cash_keys != valuation_keys:
        raise SimulationContractError("cash 与 valuations 快照集合不一致")
    if {
        (str(row.portfolio_id), str(row.snapshot_id)) for row in positions.itertuples(index=False)
    } - valuation_keys:
        raise SimulationContractError("positions 引用了不存在的估值快照")
    _require_unique(cash, ["portfolio_id", "session"], "cash session")
    _require_unique(valuations, ["portfolio_id", "session"], "valuation session")
    snapshot_index = {
        (str(row.portfolio_id), str(row.snapshot_id)): row
        for row in cash.itertuples(index=False)
    }
    fill_position_keys = {
        (str(row.portfolio_id), row.session, str(row.instrument_hash))
        for row in fills.itertuples(index=False)
    }
    position_keys = {
        (str(row.portfolio_id), row.session, str(row.instrument_hash))
        for row in positions.itertuples(index=False)
    }
    if fill_position_keys - position_keys:
        raise SimulationContractError("正式 fill 缺少同会话持仓快照")
    valuation_time_by_session = {
        (str(row.portfolio_id), row.session): _require_aware(
            row.valuation_time, "cash.valuation_time"
        )
        for row in cash.itertuples(index=False)
    }
    for row in fills.itertuples(index=False):
        valuation_time = valuation_time_by_session.get((str(row.portfolio_id), row.session))
        if valuation_time is None:
            raise SimulationContractError("正式 fill 缺少同会话现金估值快照")
        if _require_aware(row.fill_time, "fill_time") > valuation_time:
            raise SimulationContractError("正式 fill 晚于同会话估值时点")
    for row in cash.itertuples(index=False):
        total = int(row.total_cash_units)
        available = int(row.available_cash_units)
        receivable = _nonnegative_int(row.receivable_cash_units, "receivable_cash_units")
        margin = _nonnegative_int(row.margin_units, "margin_units")
        if not semantics.negative_cash_allowed and min(total, available) < 0:
            raise SimulationContractError("合同语义禁止负现金")
        if available > total:
            raise SimulationContractError("可用现金/权益超过总现金/权益")
        if margin > total:
            raise SimulationContractError("保证金超过现金/权益")
        if semantics.asset_class == "cn_future":
            if receivable != 0 or available + margin != total:
                raise SimulationContractError("期货可用权益与保证金未闭合到总权益")
        elif margin != 0 or available + receivable > total:
            raise SimulationContractError("现货现金 bucket 超过总现金")
        _require_aware(row.valuation_time, "cash.valuation_time")
        _require_hash(str(row.source_state_hash), "cash.source_state_hash")
        source_hash = _require_hash(
            str(row.non_trade_source_hash), "cash.non_trade_source_hash"
        )
        if int(row.non_trade_cash_change_units) != 0 and source_hash == _EMPTY_NON_TRADE_SOURCE_HASH:
            raise SimulationContractError("非交易现金变化缺少来源事件")
    _verify_cash_conservation(fills, cash, semantics=semantics)
    for row in positions.itertuples(index=False):
        quantity = int(row.quantity)
        if row.asset_class != "cn_future" and quantity < 0:
            raise SimulationContractError("现货持仓不能为负")
        buckets = sum(int(getattr(row, name)) for name in (
            "sellable_quantity", "unsettled_quantity", "frozen_quantity"
        ))
        if row.asset_class != "cn_future" and buckets != quantity:
            raise SimulationContractError("现货持仓 bucket 与总数量不一致")
        _require_aware(row.valuation_time, "position.valuation_time")
        _require_hash(str(row.source_state_hash), "position.source_state_hash")
        snapshot = snapshot_index[(str(row.portfolio_id), str(row.snapshot_id))]
        if row.session != snapshot.session or _require_aware(
            row.valuation_time, "position.valuation_time"
        ) != _require_aware(snapshot.valuation_time, "cash.valuation_time"):
            raise SimulationContractError("position 与 cash 快照时点不一致")
        source_hash = _require_hash(
            str(row.non_trade_source_hash), "position.non_trade_source_hash"
        )
        if int(row.non_trade_quantity_change) != 0 and source_hash == _EMPTY_NON_TRADE_SOURCE_HASH:
            raise SimulationContractError("非交易持仓变化缺少来源事件")
    _verify_position_conservation(fills, positions)
    position_values = (
        positions.groupby(["portfolio_id", "snapshot_id"])["market_value_units"].sum().to_dict()
        if not positions.empty else {}
    )
    cash_by_key = {
        (str(row.portfolio_id), str(row.snapshot_id)): int(row.total_cash_units)
        for row in cash.itertuples(index=False)
    }
    for row in valuations.itertuples(index=False):
        key = (str(row.portfolio_id), str(row.snapshot_id))
        snapshot = snapshot_index[key]
        if (
            row.session != snapshot.session
            or _require_aware(row.valuation_time, "valuation_time")
            != _require_aware(snapshot.valuation_time, "cash.valuation_time")
            or str(row.currency) != str(snapshot.currency)
            or str(row.source_state_hash) != str(snapshot.source_state_hash)
        ):
            raise SimulationContractError("valuation 与 cash 快照身份不一致")
        expected = cash_by_key[key]
        if str(row.valuation_model) == "cash_plus_position_market_value":
            expected += int(position_values.get(key, 0))
        elif str(row.valuation_model) != "futures_settlement_equity":
            raise SimulationContractError("valuation_model 不受支持")
        if int(row.nav_units) != expected:
            raise SimulationContractError("估值与现金、持仓市值不守恒")
        _require_aware(row.valuation_time, "valuation_time")
        _require_hash(str(row.source_state_hash), "valuation.source_state_hash")


def _verify_position_conservation(fills: pd.DataFrame, positions: pd.DataFrame) -> None:
    if positions.empty:
        return
    fill_changes: dict[tuple[str, str, object], int] = {}
    for row in fills.itertuples(index=False):
        key = (str(row.portfolio_id), str(row.instrument_hash), row.session)
        signed = int(row.quantity) if str(row.side) == "buy" else -int(row.quantity)
        fill_changes[key] = fill_changes.get(key, 0) + signed
    for (portfolio_id, instrument_hash), frame in positions.groupby(
        ["portfolio_id", "instrument_hash"], sort=True
    ):
        previous = 0
        for row in frame.sort_values(["valuation_time", "snapshot_id"], kind="mergesort").itertuples(index=False):
            declared_trade = int(row.trade_quantity_change)
            actual_trade = fill_changes.get((str(portfolio_id), str(instrument_hash), row.session), 0)
            if declared_trade != actual_trade:
                raise SimulationContractError("positions 的交易数量变化与 fills 不一致")
            expected = previous + declared_trade + int(row.non_trade_quantity_change)
            if int(row.quantity) != expected:
                raise SimulationContractError("持仓数量不守恒")
            previous = int(row.quantity)


def _verify_cash_conservation(
    fills: pd.DataFrame,
    cash: pd.DataFrame,
    *,
    semantics: SimulationResultSemantics,
) -> None:
    trade_changes: dict[tuple[str, object], int] = {}
    for row in fills.itertuples(index=False):
        if semantics.asset_class == "cn_future":
            change = int(row.realized_pnl_units) - int(row.fee_units)
        else:
            notional = int(row.notional_units)
            change = (-notional if str(row.side) == "buy" else notional) - int(
                row.fee_units
            )
        key = (str(row.portfolio_id), row.session)
        trade_changes[key] = trade_changes.get(key, 0) + change
    for portfolio_id, frame in cash.groupby("portfolio_id", sort=True):
        previous: int | None = None
        opening: int | None = None
        for row in frame.sort_values(["valuation_time", "snapshot_id"], kind="mergesort").itertuples(index=False):
            declared_opening = _positive_int(row.opening_cash_units, "opening_cash_units")
            if opening is None:
                opening = declared_opening
                previous = opening
            elif declared_opening != opening:
                raise SimulationContractError("同一组合的初始现金身份漂移")
            declared_trade = int(row.trade_cash_change_units)
            actual_trade = trade_changes.get((str(portfolio_id), row.session), 0)
            if declared_trade != actual_trade:
                raise SimulationContractError("cash 的交易现金变化与 fills 不一致")
            expected = previous + declared_trade + int(row.non_trade_cash_change_units)
            if int(row.total_cash_units) != expected:
                raise SimulationContractError("现金变化不守恒")
            previous = int(row.total_cash_units)


def _frame_hash(name: str, frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(typed_canonical_bytes({
        "table_hash_contract": "research-simulation-table-hash-v2",
        "schema": _schema_payload(name),
    }))
    for row in frame.itertuples(index=False, name=None):
        digest.update(typed_canonical_bytes({
            str(column): _scalar(value)
            for column, value in zip(frame.columns, row, strict=True)
        }))
    return digest.hexdigest()


def _normalize_table(name: str, frame: pd.DataFrame) -> pd.DataFrame:
    required = _REQUIRED_COLUMNS[name]
    if set(frame.columns) != required:
        missing = sorted(required - set(frame.columns))
        extra = sorted(set(frame.columns) - required)
        raise SimulationContractError(
            f"{name} schema 无效: missing={missing}, extra={extra}"
        )
    try:
        return _to_arrow_table(name, frame).to_pandas().reset_index(drop=True)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise SimulationContractError(f"{name} 字段类型与规范 schema 不一致") from exc


def _to_arrow_table(name: str, frame: pd.DataFrame) -> pa.Table:
    normalized = frame.loc[:, sorted(_REQUIRED_COLUMNS[name])].copy()
    for column in _TIMESTAMP_COLUMNS & set(normalized.columns):
        values = pd.to_datetime(normalized[column], utc=True, errors="raise")
        normalized[column] = values.dt.tz_convert("Asia/Shanghai")
    if "session" in normalized.columns:
        normalized["session"] = pd.to_datetime(
            normalized["session"], errors="raise"
        ).dt.date
    return pa.Table.from_pandas(
        normalized,
        schema=_ARROW_SCHEMAS[name],
        preserve_index=False,
        safe=True,
    )


def _terminal_status(requested: int, filled: int) -> str:
    if requested <= 0 or filled < 0 or filled > requested:
        raise SimulationContractError("订单请求或成交数量无效")
    if filled == requested:
        return "filled"
    if filled == 0:
        return "rejected"
    return "partially_filled"


def _row_hash(row: object) -> str:
    if hasattr(row, "_asdict"):
        payload = {
            str(key): _scalar(value) for key, value in row._asdict().items()
        }
    elif isinstance(row, Mapping):
        payload = {str(key): _scalar(value) for key, value in row.items()}
    else:
        raise SimulationContractError("无法计算来源行摘要")
    return typed_canonical_hash(payload)


def _snapshot_id(
    portfolio_id: str,
    session: object,
    valuation_time: object,
    source_state_hash: object,
) -> str:
    return typed_canonical_hash({
        "portfolio_id": portfolio_id,
        "session": _scalar(session),
        "valuation_time": _scalar(valuation_time),
        "source_state_hash": str(source_state_hash),
    })


def _scalar(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _require_unique(frame: pd.DataFrame, columns: list[str], name: str) -> None:
    if frame.duplicated(columns).any():
        raise SimulationContractError(f"{name} 主键重复")


def _require_aware(value: object, field: str) -> pd.Timestamp:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SimulationContractError(f"{field} 必须带时区")
    return parsed


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SimulationContractError(f"{field} 必须是小写 sha256")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if type(value) is bool:
        raise SimulationContractError(f"{field} 必须是非负整数")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SimulationContractError(f"{field} 必须是非负整数") from exc
    if parsed != value or parsed < 0:
        raise SimulationContractError(f"{field} 必须是非负整数")
    return parsed


def _integer(value: object, field: str) -> int:
    if type(value) is bool:
        raise SimulationContractError(f"{field} 必须是整数")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SimulationContractError(f"{field} 必须是整数") from exc
    if parsed != value:
        raise SimulationContractError(f"{field} 必须是整数")
    return parsed


def _positive_int(value: object, field: str) -> int:
    parsed = _nonnegative_int(value, field)
    if parsed == 0:
        raise SimulationContractError(f"{field} 必须为正")
    return parsed


__all__ = [
    "CANONICAL_SIMULATION_TABLES",
    "SIMULATION_RESULT_CONTRACT_VERSION",
    "SIMULATION_RESULT_SEMANTICS_VERSION",
    "SimulationResultArtifactWriter",
    "SimulationResultContract",
    "SimulationResultSemantics",
    "build_simulation_result_contract",
    "canonical_simulation_table",
    "project_cash_daily_result",
    "project_futures_daily_result",
    "verify_simulation_result_artifact",
    "verify_simulation_result_contract",
    "write_simulation_result_contract",
]
