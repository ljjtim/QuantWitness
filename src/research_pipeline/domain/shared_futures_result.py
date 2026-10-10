"""共享期货账户独立于净仓六表的字段、类型和唯一键。"""
from types import MappingProxyType

SHARED_FUTURES_RESULT_VERSION = "research-shared-futures-result-v1"
SHARED_FUTURES_SCHEMA_PREFIX = "research.shared-futures"
_COMMON = ("portfolio_id", "account_id", "currency")
_SNAPSHOT = ("event_time", "sequence", "session")
SHARED_FUTURES_COLUMNS = MappingProxyType({
    "cash": _COMMON + _SNAPSHOT + ("cash_units", "unrealized_pnl_units", "equity_units", "margin_units", "frozen_units", "available_units", "cash_scale", "risk_state"),
    "positions": _COMMON + _SNAPSHOT + ("instrument_id", "direction", "position_bucket", "quantity", "base_price", "cost_numerator", "cost_denominator", "price_scale", "margin_units"),
    "orders": _COMMON + ("order_id", "instrument_id", "direction", "position_effect", "order_type", "time_in_force", "submitted_at", "decision_time", "session", "requested_quantity", "filled_quantity", "status", "terminal_reason", "origin", "roll_plan_id"),
    "fills": _COMMON + _SNAPSHOT + ("fill_id", "order_id", "instrument_id", "direction", "position_bucket", "position_effect", "quantity", "execution_price_units", "reference_price_units", "price_scale", "contract_multiplier", "notional_units", "fee_units", "realized_pnl_units", "origin"),
    "costs": _COMMON + _SNAPSHOT + ("cost_id", "fill_id", "cost_type", "amount_units"),
    "valuations": _COMMON + _SNAPSHOT + ("nav_units", "valuation_model"),
    "reservations": _COMMON + _SNAPSHOT + ("order_id", "reservation_type", "delta_units", "remaining_units", "reason"),
    "risks": _COMMON + _SNAPSHOT + ("action", "trigger", "risk_order_id", "cash_units", "unrealized_pnl_units", "equity_units", "margin_units", "frozen_units", "available_units", "deficit_units"),
    "rolls": _COMMON + _SNAPSHOT + ("roll_plan_id", "old_instrument_id", "new_instrument_id", "closed_quantity", "opened_quantity", "reserved_new_quantity", "available_new_quantity", "order_id"),
})
SHARED_FUTURES_INTEGER_COLUMNS = frozenset({
    "sequence", "cash_units", "unrealized_pnl_units", "equity_units", "margin_units", "frozen_units", "available_units", "cash_scale", "quantity", "price_scale", "requested_quantity", "filled_quantity", "execution_price_units", "reference_price_units", "notional_units", "fee_units", "realized_pnl_units", "amount_units", "nav_units", "delta_units", "remaining_units", "deficit_units", "closed_quantity", "opened_quantity", "reserved_new_quantity", "available_new_quantity",
})
SHARED_FUTURES_TIMESTAMP_COLUMNS = frozenset({"event_time", "submitted_at", "decision_time"})
SHARED_FUTURES_KEYS = MappingProxyType({
    "cash": _COMMON + ("event_time", "sequence"),
    "positions": _COMMON + ("event_time", "sequence", "instrument_id", "direction", "position_bucket"),
    "orders": _COMMON + ("order_id",),
    "fills": _COMMON + ("fill_id",),
    "costs": _COMMON + ("cost_id",),
    "valuations": _COMMON + ("event_time", "sequence"),
    "reservations": _COMMON + ("sequence", "order_id", "reservation_type"),
    "risks": _COMMON + ("sequence",),
    "rolls": _COMMON + ("sequence", "roll_plan_id"),
})
SHARED_FUTURES_SCHEMA_IDS = MappingProxyType({name: f"{SHARED_FUTURES_SCHEMA_PREFIX}.{name}.v1" for name in SHARED_FUTURES_COLUMNS})
