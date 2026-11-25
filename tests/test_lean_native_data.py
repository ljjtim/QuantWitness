"""原生数据流的PIT、独立游标和有界事件索引验收。"""

from datetime import datetime
from decimal import Decimal

import pytest

from research_pipeline.runtime.lean_native_data import NativeDataBinding, NativeDailyBarReader


def _row(day, *, symbol="000001.XSHE", value=1, execution_open=False):
    stamp = f"2026-11-{day:02d}T09:30:00+08:00"
    return {"symbol": symbol, "time": stamp, "end_time": stamp,
            "available_at": stamp, "values": {"value": value}, "execution_open": execution_open}


def _request(**changes):
    return {"binding_id": "events", "symbol": "000001.XSHE", "purpose": "history",
            "start_at": "2026-11-01T00:00:00+08:00", "end_at": "2026-11-10T00:00:00+08:00",
            "consumer_time": "2026-11-04T09:30:00+08:00", **changes}


def _binding(reader):
    return NativeDataBinding({"binding_id": "events", "kind": "event", "resolution": "event",
        "symbols": ["000001.XSHE"], "history_start_at": "2026-11-01T00:00:00+08:00",
        "end_at": "2026-11-11T00:00:00+08:00"}, reader)


def test_history_visibility_and_open_events():
    binding = _binding(lambda request: iter([_row(2), _row(3, execution_open=True), _row(4), _row(5)]))
    assert [row["available_at"][8:10] for row in binding.rows(_request())] == ["02", "04"]
    with pytest.raises(ValueError, match="研究数据域"):
        list(binding.rows(_request(symbol="600000.XSHG")))


def test_native_utc_request_preserves_microsecond_visibility():
    from datetime import datetime, timezone
    from research_pipeline.runtime.lean_native_data import _at

    assert _at("2021-10-07T16:00:00.000000Z") == datetime(2021, 10, 7, 16, tzinfo=timezone.utc)
    assert _at("2021-10-07T16:00:00.000001Z").microsecond == 1
    binding = _binding(lambda request: iter([_row(2), _row(4)]))
    assert list(binding.rows(_request(consumer_time="2026-11-04T01:29:59.999999Z"))) == [_row(2)]


@pytest.mark.parametrize("as_table", [False, True])
def test_native_batch_rows_preserves_arrow_values(as_table):
    from datetime import date
    import pyarrow as pa
    from research_pipeline.runtime.lean_native_data import native_batch_rows

    batch = pa.record_batch({
        "day": pa.array([date.min, date.max, date.min], type=pa.date32()),
        "announcement": pa.array([date.min, None, date.max], type=pa.date32()),
        "quantity": pa.array([2**63 + 1, 0, 1], type=pa.uint64()),
        "nullable_quantity": pa.array([2**63 + 1, None, 0], type=pa.uint64()),
        "money": pa.array([Decimal("12.001"), None, Decimal("-0.001")]),
        "exact": pa.array([Decimal("12345678901234567890.120000000000000000"), None, Decimal("0E-18")],
                          type=pa.decimal128(38, 18)),
        "symbol": ["深市", "", None],
        "flag": [True, False, True],
        "price": [1.1, -0.0, 0.0],
    })
    if as_table:
        batch = pa.Table.from_batches([batch.slice(0, 1), batch.slice(1)])
    assert native_batch_rows(batch) == batch.to_pylist()
    assert [str(row["exact"]) for row in native_batch_rows(batch)] == [
        "12345678901234567890.120000000000000000", "None", "0E-18"]
    assert native_batch_rows(batch.slice(0, 0)) == []


def test_external_reader_can_reuse_a_row_dictionary():
    def reader(request):
        row = _row(2)
        yield row
        row.update(_row(4))
        yield row
    assert list(_binding(reader).rows(_request())) == [_row(2), _row(4)]


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("zone", [None, "Asia/Shanghai", "America/New_York"])
def test_timestamp_columns_preserve_precision_and_offset(unit, zone):
    import pyarrow as pa
    from research_pipeline.runtime.lean_native_data import _native_column_values

    scale = {"s": 1, "ms": 1000, "us": 10**6, "ns": 10**9}[unit]
    # 纽约秋季回拨前后的两个 UTC 时刻，对应同一个本地钟面时间。
    ticks = [-1, 1636263000 * scale + 1, 1636266600 * scale + 1, -1]
    dtype = pa.timestamp(unit, tz=zone)
    raw = pa.array(ticks, type=dtype)
    for column in (raw, pa.chunked_array([raw.slice(0, 1), raw.slice(1)]),
                   pa.array([None, *ticks], type=dtype)):
        observed = _native_column_values(column)
        expected = column.to_pylist()
        assert [(type(v), v.isoformat() if v is not None else None) for v in observed] == [
            (type(v), v.isoformat() if v is not None else None) for v in expected]


def test_first_daily_open_history_excludes_open_and_unpublished_daily_bar():
    opened = {"symbol": "000001.XSHE", "time": "2024-01-08T09:30:00+08:00",
              "end_time": "2024-01-08T09:30:00+08:00", "available_at": "2024-01-08T09:30:00+08:00",
              "execution_open": True, "open": 10}
    daily = {"symbol": "000001.XSHE", "time": "2024-01-08T09:30:00+08:00",
             "end_time": "2024-01-08T15:00:00+08:00", "available_at": "2024-01-09T09:30:00+08:00",
             "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100}
    binding = NativeDataBinding({"binding_id": "prices", "kind": "bars", "resolution": "1d",
        "symbols": ["000001.XSHE"], "history_start_at": "2024-01-08T00:00:00+08:00",
        "end_at": "2024-01-12T00:00:00+08:00"}, lambda request: iter([opened, daily]))
    request = _request(binding_id="prices", start_at="2024-01-08T00:00:00+08:00",
        end_at="2024-01-08T09:30:00+08:00", consumer_time="2024-01-08T09:30:00+08:00")
    assert list(binding.rows(request)) == []
    assert list(binding.rows({**request, "end_at": "2024-01-09T09:30:00+08:00",
                             "consumer_time": "2024-01-09T09:30:00+08:00"})) == [daily]


def test_daily_cache_shares_arrow_day_and_long_holiday_previous_session():
    import pyarrow as pa
    loads = []
    def load(day):
        loads.append(day)
        return pa.Table.from_pylist([{"instrument": "000001.XSHE", "day": day},
                                    {"instrument": "600000.XSHG", "day": day}])
    def project(raw, request):
        return {"symbol": raw["instrument"], "day": raw["day"]}
    reader = NativeDailyBarReader(["2026-09-30", "2026-10-12"], load, project, memory_bytes=65536)
    request = _request(start_at="2026-10-12T00:00:00+08:00", end_at="2026-10-12T15:00:00+08:00")
    assert len(list(reader(request))) == 2
    assert len(list(reader({**request, "symbol": "600000.XSHG"}))) == 2
    assert loads == ["2026-09-30", "2026-10-12"]

def test_pit_opening_and_weekend_cutoffs_follow_engine_clock():
    from datetime import date, datetime
    from research_pipeline.lean_data import _source_cutoff

    thursday, friday, monday = (date.fromisoformat(day) for day in ("2021-10-21", "2021-10-22", "2021-10-25"))
    sessions = thursday, friday, monday
    before = datetime(2021, 10, 25, 9, 29, 59)
    opening = datetime(2021, 10, 25, 9, 30)
    weekend = datetime(2021, 10, 24, 13)
    assert _source_cutoff(before, "next_session_open", sessions) == thursday
    assert _source_cutoff(opening, "next_session_open", sessions) == friday
    assert _source_cutoff(before, "session_open", sessions) == friday
    assert _source_cutoff(opening, "session_open", sessions) == monday
    for now in (before, weekend):
        assert _source_cutoff(now, "next_session_open_following_publication_date", sessions) == thursday
    assert _source_cutoff(opening, "next_session_open_following_publication_date", sessions) == weekend.date()
    assert _source_cutoff(before, "business_effective_date", sessions) == monday


def test_pit_shutdown_closes_connection_when_algorithm_ends_early(monkeypatch):
    from research_pipeline import lean_data

    events = []
    class Connection:
        def close(self):
            events.append("close")

    reader = lean_data.ReadOnlyPIT.__new__(lean_data.ReadOnlyPIT)
    reader._connection = Connection()
    from research_pipeline.platform import lean_python_lifetime
    monkeypatch.setattr(lean_python_lifetime, "_release_duckdb_module", lambda: events.append("release"))
    lean_data._open_readers.add(reader)
    lean_python_lifetime._shutdown_readers()
    reader.close()
    assert events == ["close", "release"]


def test_direct_fields_keep_row_order_and_reject_mixed_clocks():
    from types import SimpleNamespace
    from research_pipeline.lean_data import read_field_groups, read_fields

    opening = datetime(2021, 10, 25, 9, 30)
    def observation(value, at=opening, binding="statements"):
        return SimpleNamespace(BindingId=binding, AvailableAt=at,
                               FieldNames=("shares", "amount"), FieldValues=(value, "0.0100"))
    first, second = observation(2**63 + 1), observation(None)
    fields, clock = read_fields([second, first, second], "statements")
    assert fields == [{"shares": value, "amount": "0.0100"} for value in (None, 2**63 + 1, None)]
    assert clock.isoformat() == "2021-10-25T09:30:00+08:00"
    assert read_field_groups([], "statements") == ([], None)
    with pytest.raises(ValueError, match="可见时点"):
        read_fields([first, observation(1, at=opening.replace(minute=31))], "statements")
    with pytest.raises(ValueError, match="绑定"):
        read_fields([first, observation(1, binding="other")], "statements")
