"""公共 SimulationResult 六表的字段与物理类型合同。"""

from __future__ import annotations

from types import MappingProxyType


CANONICAL_SIMULATION_COLUMNS = MappingProxyType({
    "orders": frozenset({
        "portfolio_id", "order_id", "session", "instrument_id", "instrument_hash",
        "asset_class", "side", "requested_quantity", "filled_quantity", "status",
        "terminal_reason", "decision_time", "submitted_at", "source_order_hash",
    }),
    "fills": frozenset({
        "portfolio_id", "fill_id", "order_id", "session", "instrument_id",
        "instrument_hash", "asset_class", "side", "quantity", "fill_time",
        "execution_price_units", "price_scale", "contract_multiplier", "notional_units",
        "fee_units", "realized_pnl_units", "position_effect", "source_fill_hash",
    }),
    "positions": frozenset({
        "portfolio_id", "snapshot_id", "session", "valuation_time", "instrument_id",
        "instrument_hash", "asset_class", "quantity", "sellable_quantity",
        "unsettled_quantity", "frozen_quantity", "market_value_units",
        "trade_quantity_change", "non_trade_quantity_change", "source_state_hash",
        "non_trade_source_hash",
    }),
    "cash": frozenset({
        "portfolio_id", "snapshot_id", "session", "valuation_time", "currency",
        "total_cash_units", "available_cash_units", "receivable_cash_units",
        "margin_units", "trade_cash_change_units", "non_trade_cash_change_units",
        "opening_cash_units", "source_state_hash", "non_trade_source_hash",
    }),
    "costs": frozenset({
        "portfolio_id", "cost_id", "fill_id", "session", "cost_type", "amount_units",
        "currency", "source_cost_hash",
    }),
    "valuations": frozenset({
        "portfolio_id", "snapshot_id", "session", "valuation_time", "nav_units",
        "currency", "valuation_model", "source_state_hash",
    }),
})
CANONICAL_SIMULATION_TIMESTAMP_COLUMNS = frozenset({
    "decision_time", "submitted_at", "fill_time", "valuation_time",
})
CANONICAL_SIMULATION_INTEGER_COLUMNS = frozenset({
    "requested_quantity", "filled_quantity", "quantity", "execution_price_units",
    "price_scale", "contract_multiplier", "notional_units", "fee_units",
    "realized_pnl_units", "sellable_quantity", "unsettled_quantity",
    "frozen_quantity", "market_value_units", "trade_quantity_change",
    "non_trade_quantity_change", "total_cash_units", "available_cash_units",
    "receivable_cash_units", "margin_units", "trade_cash_change_units",
    "non_trade_cash_change_units", "opening_cash_units", "amount_units", "nav_units",
})


__all__ = [
    "CANONICAL_SIMULATION_COLUMNS",
    "CANONICAL_SIMULATION_INTEGER_COLUMNS",
    "CANONICAL_SIMULATION_TIMESTAMP_COLUMNS",
]
