"""LEAN 原生事件的固定 Arrow 列定义与转换。"""

from datetime import date
from decimal import Decimal
from functools import lru_cache

import pandas as pd
import pyarrow as pa


_TEXT = pa.string()
_INT = pa.int64()
_BOOL = pa.bool_()
_DATE = pa.date32()
_TIME = pa.timestamp("ns", tz="UTC")
# .NET Decimal 的整数最多 29 位，除法结果可以带 28 位小数。
_DECIMAL = pa.decimal256(57, 28)
# 费用及实际成交腿的已实现盈亏以分落账，现货分红来源采用每股微元。
_CENTS = pa.decimal128(38, 2)
_CASH = pa.decimal128(38, 6)
_PRICE = pa.decimal128(38, 9)
_FLOAT = pa.float64()


def _fields(*groups):
    return [pa.field(name, dtype) for dtype, names in groups for name in names.split()]


_MONEY = pa.struct(_fields((_INT, "units scale"), (_TEXT, "currency")))
_INSTRUMENT = pa.struct(_fields(
    (_TEXT, "instrument_id asset_class venue currency contract_kind contract_version"),
))
_MARK = pa.struct(_fields(
    (_TEXT, "instrument_id source source_ref"),
    (_DECIMAL, "price"),
    (_TIME, "price_source_at price_available_at"),
    (_BOOL, "is_current_session is_current_session_close"),
    (_DATE, "delisted_date"),
))
_POSITION_FIELDS = _fields(
    (_TEXT, "account_id instrument_id direction bucket mark_source"),
    (_INT, "event_sequence price_scale"),
    (_INT, "quantity reserved_quantity sellable_quantity locked_bonus_quantity"),
    (_DECIMAL, "price cost base_price multiplier unrealized_pnl average_price"),
    (_DATE, "trading_date"),
    (_TIME, "at valuation_at"),
)
_ACCOUNT_FIELDS = _fields(
    (_TEXT, "account_id asset_class equity_semantics"),
    (_INT, "event_sequence"),
    (_DATE, "trading_date"),
    (_TIME, "valuation_at"),
    (_CASH, "cash"),
    (_DECIMAL, "equity additional_equity unrealized_pnl margin frozen_funds available_funds"),
    (pa.map_(_TEXT, _DATE), "settled_through"),
    (pa.list_(_MARK), "valuation_marks"),
)
_INTENT = pa.struct(_fields(
    (_TEXT, "intent_id kind side weight order_type time_in_force funds_policy price_input_id order_id position_effect"),
    (_INSTRUMENT, "instrument"),
    (_INT, "quantity"),
    (_MONEY, "amount limit_price protection_price"),
))
_SHARE_RECEIVABLE = pa.struct(_fields(
    (_TEXT, "action_id share_settlement_policy source_ref"),
    (_INT, "revision quantity"),
    (_INSTRUMENT, "instrument"),
    (_MONEY, "value"),
    (_TIME, "sellable_at available_at source_shares_arrival_at"),
))
_TRACE_PAYLOAD = pa.struct(_fields(
    (_TEXT, "algorithm_type algorithm_language engine_status status fee_currency message request_type request_status "
            "tag order_type response_error_code response_message"),
    (_INT, "order_id event_id open_orders"),
    (_TIME, "request_at"),
    (_DECIMAL, "quantity limit_price stop_price"),
    (_PRICE, "price"),
    (_CENTS, "fee"),
    (_BOOL, "response_is_error"),
))
_LIFECYCLE_DETAIL = pa.struct(_fields(
    (_TEXT, "instrument_id plan_id old_instrument_id new_instrument_id direction source_ref"),
    (_INT, "order_id"),
    (_DECIMAL, "amount price available_funds"),
    (_INT, "quantity closed opened"),
    (_DATE, "trading_date"),
    (_TIME, "available_at"),
))


LEAN_EXECUTION_SCHEMAS = {
    "orders": pa.schema(_fields(
        (_TEXT, "account_id instrument_id status order_type time_in_force tag message position_effect direction roll_plan_id"),
        (_INT, "event_sequence order_event_count order_id event_id"),
        (_TIME, "submitted_at at"),
        (_DATE, "trading_date"),
        (_DECIMAL, "order_quantity limit_price stop_price budget_cny remaining_quantity"),
        (_CASH, "cash"),
        (_PRICE, "price"),
        (_INT, "quantity holding"),
        (_CENTS, "fee"),
        (_BOOL, "risk_order"),
    )),
    "intents": pa.schema(_fields(
        (_TIME, "at"),
        (_INT, "event_sequence order_id"),
        (_INTENT, "intent"),
        (_INT, "requested_quantity submitted_quantity"),
        (_DECIMAL, "budget_cny"),
        (_TEXT, "skip_reason"),
    )),
    "daily": pa.schema(_ACCOUNT_FIELDS + _fields(
        (_DATE, "date"),
        (_INT, "rows active_orders"),
        (_BOOL, "session_complete"),
        (_FLOAT, "day_wall_seconds"),
        (pa.list_(pa.struct(_POSITION_FIELDS)), "positions"),
    )),
    "trace": pa.schema(_fields(
        (_INT, "event_seq"),
        (_TEXT, "kind"),
        (_TIME, "at"),
        (_TRACE_PAYLOAD, "payload"),
    )),
    "fills": pa.schema(_fields(
        (_TEXT, "account_id fill_id leg_id instrument_id direction bucket position_effect rule_reference roll_plan_id "
                "fee_currency status"),
        (_INT, "event_sequence order_id event_id price_scale"),
        (_TIME, "at available_at"),
        (_DATE, "trading_date"),
        (_INT, "quantity"),
        (_PRICE, "price"),
        (_DECIMAL, "multiplier"),
        (_CENTS, "fee realized_pnl"),
        (_BOOL, "risk_order"),
    )),
    "positions": pa.schema(_POSITION_FIELDS),
    "account_events": pa.schema(_ACCOUNT_FIELDS + _fields(
        (_TEXT, "reason"),
        (_TIME, "at available_at"),
        (_BOOL, "risk_active"),
        (pa.list_(_SHARE_RECEIVABLE), "share_receivables"),
    )),
    "lifecycle": pa.schema(_fields(
        (_TEXT, "account_id instrument_id kind action_id phase"),
        (_INT, "event_sequence revision"),
        (_DATE, "trading_date"),
        (_TIME, "at available_at"),
        (_INT, "quantity registered_quantity share_delta"),
        (_CASH, "cash_delta"),
        (_MARK, "valuation_mark"),
        (_LIFECYCLE_DETAIL, "detail"),
    )),
}


@lru_cache(maxsize=None)
def _field_types(fields):
    return {field.name: field.type for field in fields}


def _encode_record(fields, record, path):
    field_types = _field_types(fields)
    extra = record.keys() - field_types.keys()
    if extra:
        raise ValueError(f"LEAN执行表 {path} 存在未声明字段：{', '.join(sorted(extra))}")
    return {
        name: _encode_value(value, field_types[name], f"{path}.{name}")
        for name, value in record.items()
    }


def _encode_value(value, dtype, path):
    if value is None:
        return None
    if dtype == _TIME:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            raise ValueError(f"LEAN执行表 {path} 的时间必须带时区")
        return timestamp.value
    if dtype == _DATE:
        return date.fromisoformat(value)
    if dtype == _INT and isinstance(value, Decimal):
        integer = int(value)
        if integer != value:
            raise ValueError(f"LEAN执行表 {path} 的数量必须是整数：{value}")
        return integer
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_struct(dtype):
        return _encode_record(dtype, value, path)
    if pa.types.is_map(dtype):
        return [(key, _encode_value(item, dtype.item_type, path)) for key, item in value.items()]
    if pa.types.is_list(dtype):
        return [_encode_value(item, dtype.value_type, path) for item in value]
    if dtype == _FLOAT:
        return float(value)
    return value


def encode_lean_execution_row(table, record):
    """转换单条引擎事实，使不同资产和事件变体可写入同一 schema。"""
    return _encode_record(LEAN_EXECUTION_SCHEMAS[table], record, table)


def _decode_record(fields, record):
    field_types = _field_types(fields)
    return {
        name: _decode_value(value, field_types[name], name)
        for name, value in record.items()
        if value is not None
    }


def _decode_value(value, dtype, name):
    if dtype == _TIME:
        if name not in {"submitted_at", "request_at"}:
            value = value.tz_convert("Asia/Shanghai")
        return value.isoformat()
    if dtype == _DATE:
        return value.isoformat()
    if pa.types.is_struct(dtype):
        return _decode_record(dtype, value)
    if pa.types.is_map(dtype):
        return {key: _decode_value(item, dtype.item_type, name) for key, item in value}
    if pa.types.is_list(dtype):
        return [_decode_value(item, dtype.value_type, name) for item in value]
    return value


def decode_lean_execution_row(table, record):
    """为研究报告还原日期文本与嵌套事实。"""
    decoded = _decode_record(LEAN_EXECUTION_SCHEMAS[table], record)
    if table == "trace" and "at" in decoded:
        decoded["at"] = record["at"].isoformat()
    return decoded
