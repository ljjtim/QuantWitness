"""核对 LEAN 原生执行表中的金额、时间及嵌套事件。"""

from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform.lean_execution_schema import (
    LEAN_EXECUTION_SCHEMAS,
    decode_lean_execution_row,
    encode_lean_execution_row,
)


def _round_trip(name, *rows):
    schema = LEAN_EXECUTION_SCHEMAS[name]
    output = pa.BufferOutputStream()
    with pq.ParquetWriter(output, schema, version="2.6") as writer:
        writer.write_table(pa.Table.from_pylist([], schema=schema))
        for row in rows:
            writer.write_table(pa.Table.from_pylist(
                [encode_lean_execution_row(name, row)], schema=schema,
            ))
    with pq.ParquetFile(pa.BufferReader(output.getvalue())) as parquet:
        assert parquet.schema_arrow.equals(schema)
        return parquet.read().to_pylist()


def test_daily_keeps_one_mark_per_instrument_and_futures_position_facts():
    at = "2026-01-05T15:00:00+08:00"
    mark = {
        "instrument_id": "510300.XSHG", "price": Decimal("3.123"), "source": "completed_minute_close",
        "price_source_at": at, "price_available_at": at, "is_current_session": True,
        "is_current_session_close": True,
    }
    spot = {
        "date": "2026-01-05", "valuation_at": at, "equity": Decimal("12345.678"),
        "positions": [{"instrument_id": mark["instrument_id"], "quantity": 100, "price": mark["price"]}],
        "valuation_marks": [mark],
    }
    futures = {
        "date": "2026-01-05", "asset_class": "cn_future", "valuation_at": at,
        "settled_through": {"IF2601": "2025-12-31", "IM2601": "2026-01-05"},
        "positions": [{"instrument_id": "IF2601", "direction": "long", "bucket": "today", "quantity": 3,
                       "base_price": Decimal("123.33333333333333333333333333"), "mark_source": None}],
    }
    spot_row, futures_row = _round_trip("daily", spot, futures)
    assert "valuation_mark" not in spot_row["positions"][0]
    assert spot_row["valuation_marks"][0]["price"] == Decimal("3.123")
    assert spot_row["equity"] == Decimal("12345.678")
    assert spot_row["valuation_marks"][0]["is_current_session_close"] is True
    assert spot_row["settled_through"] is None
    assert futures_row["date"] == date(2026, 1, 5)
    assert futures_row["positions"][0]["base_price"] == futures["positions"][0]["base_price"]
    assert futures_row["positions"][0]["mark_source"] is None
    decoded = decode_lean_execution_row("daily", futures_row)
    assert decoded["valuation_at"] == at
    assert decoded["settled_through"] == futures["settled_through"]


def test_cash_and_equity_cover_large_amounts_and_exact_derived_values():
    row, = _round_trip("daily", {
        "date": "2026-01-05",
        "cash": Decimal("92233720368547758.070001"),
        "equity": Decimal("92233720368547758.071234567891"),
        "unrealized_pnl": Decimal("0.0012335678911234567891234567"),
    })
    assert row["cash"] == Decimal("92233720368547758.070001")
    assert row["equity"] == Decimal("92233720368547758.071234567891")
    assert row["unrealized_pnl"] == Decimal("0.0012335678911234567891234567")


def test_order_fills_and_intent_keep_writable_types_and_empty_values():
    spot, future = _round_trip("fills",
        {"fill_id": "3:1", "order_id": 3, "event_id": 1, "quantity": -100,
         "price": Decimal("3.456"), "fee": Decimal("1.01"), "fee_currency": "CNY"},
        {"fill_id": "8:2", "leg_id": "8:2:1", "order_id": 8, "quantity": 3,
         "price": Decimal("123.456789012"), "price_scale": 9, "fee": Decimal("27.01"),
         "realized_pnl": Decimal("-7.13"), "rule_reference": "future-rule:IF2601"})
    assert spot["quantity"] == -100
    assert spot["leg_id"] is None
    assert future["price"] == Decimal("123.456789012")
    assert future["realized_pnl"] == Decimal("-7.13")
    assert future["fee"] == Decimal("27.01")

    row, = _round_trip("intents", {
        "order_id": None, "submitted_quantity": 0, "skip_reason": "预算不足历史最小申报量",
        "intent": {"intent_id": "5:0", "kind": "amount", "instrument": {"instrument_id": "510300.XSHG"},
                   "amount": {"units": 540, "scale": 2, "currency": "CNY"}},
    })
    assert row["order_id"] is None
    assert row["submitted_quantity"] == 0
    assert row["intent"]["amount"] == {"units": 540, "scale": 2, "currency": "CNY"}


def test_native_trace_preserves_time_and_rejected_request_values():
    stamp = "2026-01-05T01:30:00.1234567+00:00"
    trace = {
        "event_seq": 1, "kind": "order_request", "at": stamp,
        "payload": {"order_id": 31, "request_at": stamp, "quantity": Decimal("1.5"),
                    "response_is_error": True, "response_error_code": "InvalidRequest"},
    }
    result, = _round_trip("trace", trace)
    payload = result["payload"]
    assert payload["quantity"] == Decimal("1.5")
    assert payload["response_is_error"] is True
    assert payload["price"] is None
    decoded = decode_lean_execution_row("trace", result)
    assert decoded["at"] == "2026-01-05T01:30:00.123456700+00:00"
    assert decoded["payload"]["request_at"] == decoded["at"]


def test_account_entitlements_and_lifecycle_details_keep_asset_specific_facts():
    stamp = "2026-01-05T09:30:00+08:00"
    account = {
        "at": stamp, "event_sequence": 3, "share_receivables": [{
            "action_id": "gift", "revision": 2, "instrument": {"instrument_id": "000001.XSHE"},
            "quantity": 105, "value": {"units": 80565, "scale": 2, "currency": "CNY"},
            "source_shares_arrival_at": None, "sellable_at": stamp, "available_at": stamp,
        }],
    }
    row, = _round_trip("account_events", account)
    receivable = row["share_receivables"][0]
    assert receivable["value"] == {"units": 80565, "scale": 2, "currency": "CNY"}
    assert receivable["source_shares_arrival_at"] is None

    settlement = {"kind": "settlement", "detail": {
        "instrument_id": "IF2601", "trading_date": "2026-01-05", "amount": Decimal("-10.11"),
        "available_at": stamp, "source_ref": "settlement:IF2601:2026-01-05",
    }}
    action = {"kind": "corporate_action", "action_id": "gift", "phase": "shares_arrived",
              "registered_quantity": 100, "share_delta": 105}
    rows = _round_trip("lifecycle", settlement, action)
    assert decode_lean_execution_row("lifecycle", rows[0])["detail"] == settlement["detail"]
    assert rows[1]["detail"] is None
    assert rows[1]["share_delta"] == 105


@pytest.mark.parametrize("name", LEAN_EXECUTION_SCHEMAS)
def test_every_empty_table_keeps_physical_schema(name):
    assert _round_trip(name) == []
    assert "payload_json" not in LEAN_EXECUTION_SCHEMAS[name].names


def test_unknown_engine_field_cannot_disappear_silently():
    with pytest.raises(ValueError, match=r"trace.payload.*new_fact"):
        encode_lean_execution_row("trace", {"payload": {"new_fact": 1}})
