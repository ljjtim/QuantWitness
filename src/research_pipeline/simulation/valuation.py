"""分钟会话的持仓估值与账户净值事实，不构造结果表。"""
from __future__ import annotations
from dataclasses import dataclass
from .ledger import FuturesLedgerState, SpotLedgerState
from .orders import SimulationContractError


@dataclass(frozen=True)
class MinuteAccountValuation:
    total_cash_units: int
    available_cash_units: int
    receivable_cash_units: int
    margin_units: int
    nav_units: int
    model: str


def value_minute_position(*, quantity: int, price_units: int | None) -> int:
    # 归零终止行也沿用原路径的当日合格价格要求。
    if price_units is None:
        raise SimulationContractError("分钟估值缺少持仓标的当日完成 bar")
    return quantity * price_units


def value_minute_account(
    state: SpotLedgerState | FuturesLedgerState, *, market_value_units: int,
) -> MinuteAccountValuation:
    if isinstance(state, SpotLedgerState):
        return MinuteAccountValuation(
            total_cash_units=state.total_cash_units,
            available_cash_units=state.available_cash_units,
            receivable_cash_units=(
                state.unsettled_cash_units
                + sum(item.cash_units for item in state.cash_receivables)
            ),
            margin_units=0,
            nav_units=state.total_cash_units + market_value_units,
            model="cash_plus_position_market_value",
        )
    return MinuteAccountValuation(
        total_cash_units=state.equity_units,
        available_cash_units=state.free_equity_units,
        receivable_cash_units=0,
        margin_units=state.margin_units,
        nav_units=state.equity_units,
        model="futures_settlement_equity",
    )
