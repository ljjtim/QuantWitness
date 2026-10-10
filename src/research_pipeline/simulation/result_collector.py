"""将分钟成交和已确认会话状态投影为规范结果行。"""
from __future__ import annotations
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import date, datetime
from typing import TYPE_CHECKING
from research_pipeline.domain import InstrumentKey, PortfolioTarget
from research_pipeline.platform import typed_canonical_hash
from .ledger import FuturesLedgerState, SpotLedgerState
from .orders import Order, SimulationContractError
from .valuation import value_minute_account, value_minute_position
if TYPE_CHECKING:
    from .execution_market import MinuteExecutionBar

_EMPTY_EVENT_HASH = typed_canonical_hash([])


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
    terminated_instruments: frozenset[str] = frozenset(),
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
                value = 0 if quantity == 0 and lot.instrument_hash in terminated_instruments else value_minute_position(
                    quantity=quantity, price_units=prices.get(lot.instrument_hash),
                )
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
    valuation = value_minute_account(state, market_value_units=market_value)
    total_cash = valuation.total_cash_units
    available_cash = valuation.available_cash_units
    receivable_cash = valuation.receivable_cash_units
    margin_units = valuation.margin_units
    valuation_model = valuation.model
    nav_units = valuation.nav_units
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


def _futures_state_hash(state: FuturesLedgerState) -> str:
    return typed_canonical_hash({
        **({"order_reservations": [item.__dict__ for item in state.order_reservations]} if state.order_reservations else {}),
        "group": state.group.__dict__,
        "equity_units": state.equity_units,
        "margin_units": state.margin_units,
        "realized_pnl_units": state.realized_pnl_units,
        "positions": [item.__dict__ for item in state.positions],
        "applied_event_ids": list(state.applied_event_ids),
    })




def collect_execution_support(
    *, orders: Iterable[dict[str, object]], fills: Iterable[dict[str, object]],
    decision_bar: MinuteExecutionBar, bar: MinuteExecutionBar,
    decision_benchmarks: list[dict[str, object]],
    execution_observations: list[dict[str, object]],
) -> None:
    """从已确认订单和成交收集 TCA 输入，不参与成交或费用计算。"""
    for order in orders:
        decision_benchmarks.append({
            "portfolio_id": str(order["portfolio_id"]),
            "order_id": str(order["order_id"]),
            "decision_price_units": decision_bar.close_units,
            "available_at": decision_bar.available_time,
            "source_hash": decision_bar.bar_hash,
        })
    for fill in fills:
        if bar.volume <= 0:
            raise SimulationContractError(
                "分钟正式 fill 对应执行 bar 缺少正的可见容量"
            )
        execution_observations.append({
            "source_fill_id": str(fill["fill_id"]),
            "arrival_price_units": bar.open_units,
            "arrival_price_available_at": bar.available_time,
            "visible_capacity": bar.volume,
            "capacity_available_at": bar.available_time,
        })

