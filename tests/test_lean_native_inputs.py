"""原生装配的来源时间与共享缓存合同。"""

from dataclasses import replace
from datetime import date, datetime
from types import SimpleNamespace as NS
from zoneinfo import ZoneInfo

from research_pipeline.domain.lean_algorithm import LeanDataBinding
from research_pipeline.runtime.lean_native_data import _bounds, _projector
from research_pipeline.runtime.lean_native_inputs import _calendar_events


ZONE = ZoneInfo("Asia/Shanghai")


def _plan(*, clocks=(), revision=None):
    return NS(event_time_field="event_date", instrument_field="code",
              temporal_selection=NS(visibility_filter=NS(time_fields=clocks) if clocks else None,
                                    revision_selector=revision, effective_interval_selector=None, source_timezone="Asia/Shanghai"),
              query=NS(field_ids=("event_date", "code", "pub_date", "revision_at", "amount")),
              session_close_binding=None, minute_availability_rule=None)


def test_publication_weekend_and_revisions_use_separate_clocks():
    binding = LeanDataBinding(binding_id="income", input_port="research", request_id="income",
        kind="event", resolution="event", columns=("event_date", "pub_date", "amount"),
        daily_time={"rule": "next_session_open_following_publication_date", "timezone": "Asia/Shanghai",
                    "sessions": ["2026-11-06", "2026-11-09", "2026-11-10"]})
    plan = _plan(clocks=(("pub_date", True),), revision=NS(order_fields=("revision_at",)))
    row = {"code": "000001.XSHE", "event_date": date(2026, 11, 7), "pub_date": date(2026, 11, 7),
           "revision_at": datetime(2026, 11, 10, 10, tzinfo=ZONE), "amount": 100}
    result = _projector(plan, binding)(row, {})
    assert result["time"].astimezone(ZONE).date() == date(2026, 11, 7)
    assert result["available_at"] == row["revision_at"]
    assert result["values"]["amount"] == 100


def test_event_batches_share_dates_but_keep_each_revision_clock(monkeypatch):
    import pyarrow as pa
    from research_pipeline.runtime import lean_native_data as module
    plan = _plan(clocks=(("pub_date", True),), revision=NS(order_fields=("revision_at",)))
    binding = LeanDataBinding(binding_id="income", input_port="research", request_id="income",
        kind="event", resolution="event", columns=("event_date", "pub_date", "amount"),
        daily_time={"rule": "next_session_open_following_publication_date", "timezone": "Asia/Shanghai",
                    "sessions": ["2026-11-06", "2026-11-09", "2026-11-10"]})
    rows = [{"code": "000001.XSHE", "event_date": date(2026, 11, 6), "pub_date": date(2026, 11, 7),
             "revision_at": datetime(2026, 11, day, 10, tzinfo=ZONE), "amount": day} for day in (6, 10)]
    batch = pa.RecordBatch.from_pylist(rows)
    scalar = module._projector(plan, binding)
    expected = [scalar(row, {}) for row in rows]
    calls = []
    original = module.daily_source_times
    def tracked(values, specification):
        calls.append(set(values))
        return original(values, specification)
    monkeypatch.setattr(module, "daily_source_times", tracked)
    project = module._event_batch_projector(plan, binding)
    assert list(project(batch, {})) == list(project(batch, {})) == expected
    assert calls == [{date(2026, 11, 6), date(2026, 11, 7)}]


def test_date_as_of_includes_full_source_local_day():
    plan = NS(query=NS(time_range=NS(start=date(2026, 11, 1), end=date(2026, 11, 9)), as_of=date(2026, 11, 9)))
    start, end = _bounds(plan)
    assert start == datetime(2026, 11, 1, tzinfo=ZONE)
    assert end == datetime(2026, 11, 10, tzinfo=ZONE)


def test_real_query_ir_iso_as_of_is_normalized():
    from dataclasses import replace
    from research_pipeline.data_plane.query_ir import (QueryIR, QueryPurpose, QueryBudget,
        DateRangeV1, UniverseSelection, SortKey)
    query = QueryIR("daily", 1, ("date", "code"), QueryPurpose.FEATURE,
        DateRangeV1(date(2024, 1, 8), date(2024, 1, 12)), UniverseSelection(("600000.XSHG",)),
        (), (SortKey("date"), SortKey("code")), QueryBudget(100, 100000, 100), as_of="2024-01-12")
    assert _bounds(NS(query=query))[1] == datetime(2024, 1, 13, tzinfo=ZONE)
    assert _bounds(NS(query=replace(query, as_of=None)))[1] == datetime(2024, 1, 13, tzinfo=ZONE)


def test_exclusive_timestamp_is_not_visible_at_publication_instant():
    from datetime import timedelta
    binding = LeanDataBinding(binding_id="event", input_port="research", request_id="event",
        kind="event", resolution="event", columns=("amount",))
    point = datetime(2026, 11, 9, 10, tzinfo=ZONE)
    row = {"event_date": point, "code": "000001.XSHE", "pub_date": point, "amount": 100}
    result = _projector(_plan(clocks=(("pub_date", False),)), binding)(row, {})
    assert result["available_at"] == point + timedelta(microseconds=1)


def test_research_field_projection_respects_declared_order_and_subset():
    point = datetime(2026, 11, 9, 10, tzinfo=ZONE)
    plan = _plan(clocks=(("pub_date", True),))
    row = dict(event_date=point, code="000001.XSHE", pub_date=point, revision_at=point, amount=100)
    full = LeanDataBinding(binding_id="event", input_port="research", request_id="event",
        kind="event", resolution="event", columns=plan.query.field_ids)
    for columns in (full.columns, full.columns[::-1], ("amount", "code")):
        result = _projector(plan, replace(full, columns=columns))(row, {})
        assert list(result["values"].items()) == [(name, row[name]) for name in columns]


def test_interval_source_requires_public_effective_boundaries():
    import pytest
    from research_pipeline.data_plane.temporal import EffectiveIntervalSelector
    plan = _plan(clocks=(("pub_date", True),))
    plan.temporal_selection.effective_interval_selector = EffectiveIntervalSelector(
        entity_fields=("code",), effective_from_field="valid_from", effective_to_field="valid_to",
        left_closed=True, right_closed=False)
    plan.query.field_ids += ("valid_from", "valid_to")
    binding = LeanDataBinding(binding_id="industry", input_port="research", request_id="industry",
        kind="event", resolution="event", columns=("amount",))
    with pytest.raises(ValueError, match="生效/失效字段"):
        _projector(plan, binding)
    binding = __import__("dataclasses").replace(binding, columns=("amount", "code", "valid_from", "valid_to"))
    point = datetime(2026, 11, 9, 10, tzinfo=ZONE)
    raw = {"event_date": point, "code": "000001.XSHE", "pub_date": point, "amount": 100,
           "valid_from": date(2026, 11, 10), "valid_to": date(2026, 11, 20)}
    assert _projector(plan, binding)(raw, {})["values"]["valid_to"] == date(2026, 11, 20)


def test_futures_segment_edges_are_independent_clock_events():
    events = _calendar_events({"sessions": [{"start_at": "2026-11-08T21:00:00+08:00",
        "end_at": "2026-11-09T15:00:00+08:00", "segments": [
            {"start_at": "2026-11-08T21:00:00+08:00", "end_at": "2026-11-08T23:00:00+08:00"},
            {"start_at": "2026-11-09T09:00:00+08:00", "end_at": "2026-11-09T15:00:00+08:00"}]}]})
    assert events == ["2026-11-08T21:00:00+08:00", "2026-11-08T23:00:00+08:00",
                      "2026-11-09T09:00:00+08:00", "2026-11-09T15:00:00+08:00"]


def test_settlement_tail_advances_clock_without_extending_trading_windows():
    from research_pipeline.runtime.lean_native_inputs import _calendar_windows
    strategy = {"end_at": "2026-11-10T09:30:00+08:00", "sessions": [{
        "start_at": "2026-11-09T09:00:00+08:00", "end_at": "2026-11-09T15:00:00+08:00"}]}
    assert _calendar_events(strategy)[-1] == strategy["end_at"]
    assert _calendar_windows(strategy) == [{"start_at": "2026-11-09T09:00:00+08:00",
                                           "end_at": "2026-11-09T15:00:00+08:00"}]


def test_intraday_clock_does_not_add_session_edges_outside_execution():
    strategy = {"execution_windows": {"2026-11-09": {
        "start_at": "2026-11-09T10:00:00+08:00", "end_at": "2026-11-09T10:03:00+08:00",
        "session_open_at": "2026-11-09T09:30:00+08:00", "session_close_at": "2026-11-09T15:00:00+08:00"}}}
    assert _calendar_events(strategy) == ["2026-11-09T10:00:00+08:00", "2026-11-09T10:03:00+08:00"]


def test_primary_minute_freezes_shared_source_without_opening_a_reader(tmp_path, monkeypatch):
    from research_pipeline.runtime import lean_native_inputs as module
    scan = NS(start_at=datetime(2026, 11, 9, tzinfo=ZONE),
              end_at=datetime(2026, 11, 9, 15, tzinfo=ZONE), as_of=datetime(2026, 11, 9, 15, tzinfo=ZONE))
    market = NS(plan=scan, dataset=NS(instruments=("000001.XSHE", "600000.XSHG")),
                native_minute_calendar_windows=lambda: [])
    monkeypatch.setattr(module, "_environment", lambda context: NS(admitted_plans={}))
    strategy = {"symbols": ["000001.XSHE"], "execution_windows": {"2026-11-09": {
        "start_at": "2026-11-09T09:30:00+08:00", "end_at": "2026-11-09T15:00:00+08:00",
        "session_open_at": "2026-11-09T09:30:00+08:00", "session_close_at": "2026-11-09T15:00:00+08:00"}}}
    declaration = NS(input_bindings=(LeanDataBinding(binding_id="prices", input_port="bars", request_id="minute",
        kind="bars", resolution="1m", columns=("open", "high", "low", "close", "volume")),))
    context = NS(effective_resource_budget=NS(memory_bytes=1048576, temp_bytes=1048576))
    payload, direct = module.prepare_native_data(context, {}, declaration, market, strategy)
    assert not direct
    assert list(tmp_path.iterdir()) == []
    binding, = payload["bindings"]
    assert binding["symbols"] == ["000001.XSHE", "600000.XSHG"]
    assert "trade_symbols" not in binding
    assert binding["direct_minute"] and payload["spot_execution_clock"] and "endpoint" not in payload
    assert not payload["calendar_windows"] and not payload["calendar_events"]


def test_daily_universe_streams_complete_cross_section_and_caches_date_mapping(monkeypatch):
    import pyarrow as pa
    from research_pipeline.runtime import lean_native_data as module
    from research_pipeline.runtime.lean_native_data import NativeDataBinding
    calls, mappings = [], []
    dates = (date(2026, 11, 6), date(2026, 11, 9))
    plan = _plan()
    plan.query.field_ids = ("event_date", "code", "amount")
    plan.query.time_range = NS(start=dates[0], end=dates[1])
    binding = LeanDataBinding(binding_id="valuation", input_port="research", request_id="valuation",
        kind="universe", resolution="1d", columns=("amount",),
        daily_time={"rule": "next_session_open", "timezone": "Asia/Shanghai",
                    "sessions": ["2026-11-06", "2026-11-09", "2026-11-10"]})
    class Dataset:
        manifest = {"temporal_source": False}
        def iter_batches(self, *, columns, filters, batch_size):
            day = filters[0].value
            calls.append(day)
            yield pa.RecordBatch.from_pylist([{"event_date": day, "code": code, "amount": number}
                for number, code in enumerate(("000001.XSHE", "600000.XSHG"))]).select(columns)
    original = module.daily_source_times
    def mapped(values, specification):
        mappings.extend(values.tolist())
        return original(values, specification)
    monkeypatch.setattr(module, "daily_source_times", mapped)
    reader = module._daily_reader(Dataset(), plan, binding, module._projector(plan, binding), 1048576)
    declaration = {"binding_id": "valuation", "kind": "universe", "resolution": "1d",
        "symbols": ["000001.XSHE", "600000.XSHG"], "history_start_at": "2026-11-06T00:00:00+08:00",
        "end_at": "2026-11-10T09:30:00+08:00"}
    source = NativeDataBinding(declaration, reader)
    request = {"symbol": "*", "purpose": "universe", "start_at": declaration["history_start_at"],
        "end_at": declaration["end_at"], "consumer_time": "2026-11-09T09:30:00+08:00"}
    rows = list(source.rows(request))
    assert [row["symbol"] for row in rows] == ["000001.XSHE", "600000.XSHG"]
    assert {row["available_at"] for row in rows} == {datetime(2026, 11, 9, 9, 30, tzinfo=ZONE)}
    history = list(source.rows({**request, "purpose": "history", "symbol": "600000.XSHG"}))
    assert len(history) == 1 and history[0]["symbol"] == "600000.XSHG"
    assert calls == list(dates)
    assert mappings == list(dates)


def test_ordered_duckdb_universe_bounds_scan_and_closes_cancelled_cursor():
    import pyarrow as pa
    from research_pipeline.runtime import lean_native_data as module
    from research_pipeline.runtime.lean_native_data import NativeDataBinding

    days = tuple(date(2026, 11, day) for day in range(1, 12))
    symbols = ("000001.XSHE", "600000.XSHG")
    plan = _plan()
    plan.primary_key = ("event_date", "code")
    plan.query.sort = (NS(field_id="event_date", descending=False),)
    plan.query.field_ids = ("event_date", "code", "amount")
    plan.query.time_range = NS(start=days[0], end=days[-2])
    binding = LeanDataBinding(binding_id="valuation", input_port="research", request_id="valuation",
        kind="universe", resolution="1d", columns=("amount",),
        daily_time={"rule": "next_session_open", "timezone": "Asia/Shanghai",
                    "sessions": [day.isoformat() for day in days]})
    calls, closed = [], []

    class Dataset:
        manifest = {"source_kind": "duckdb", "temporal_source": False}
        max_batch_rows = 65536

        def iter_batches(self, *, columns, filters, batch_size):
            predicate = filters[0]
            calls.append(predicate)
            selected = predicate.value if predicate.operator == "in" else (predicate.value,)
            try:
                for day in selected:
                    yield pa.RecordBatch.from_pylist([
                        {"event_date": day, "code": code, "amount": day.day * 10 + number}
                        for number, code in enumerate(symbols)]).select(columns)
            finally:
                closed.append(predicate)

    dataset = Dataset()
    projector = module._projector(plan, binding)
    declaration = {"binding_id": "valuation", "kind": "universe", "resolution": "1d",
        "symbols": symbols, "history_start_at": "2026-11-01T00:00:00+08:00",
        "end_at": "2026-11-11T15:00:00+08:00"}
    request = {"symbol": "*", "purpose": "universe", "start_at": "2026-11-02T00:00:00+08:00",
        "end_at": declaration["end_at"], "consumer_time": "2026-11-09T01:29:59.999999Z"}
    source = NativeDataBinding(declaration, module._daily_reader(dataset, plan, binding, projector, 1048576))
    rows = list(source.rows(request))
    assert [row["values"]["amount"] for row in rows] == [
        day.day * 10 + number for day in days[:7] for number in range(len(symbols))]
    assert [(item.operator, item.value) for item in calls] == [("in", days[:8]), ("in", days[8:9])]
    assert calls == closed

    cursor = source.rows(request)
    assert next(cursor) == rows[0]
    cursor.close()
    assert calls == closed

    calls.clear()
    history = list(source.rows({**request, "purpose": "history", "symbol": symbols[1]}))
    assert history == rows[1::2]
    assert [item.operator for item in calls] == ["eq"] * 10

    calls.clear()
    effective = replace(binding, daily_time={**binding.daily_time, "rule": "business_effective_date"})
    source = NativeDataBinding(declaration,
        module._daily_reader(dataset, plan, effective, module._projector(plan, effective), 1048576))
    # 上海11月9日和10日的午夜在UTC中分别是前一天16点。
    rows = list(source.rows({**request, "start_at": "2026-11-08T16:00:00Z",
                            "end_at": "2026-11-09T16:00:00Z",
                            "consumer_time": "2026-11-09T16:00:00Z"}))
    assert [row["values"]["amount"] for row in rows] == [100, 101]
    assert [(item.operator, item.value) for item in calls] == [("in", days[7:10])]

    # 已准入证券与主键给出每天的行数上界，可在同样的流式批大小下合并多个日期。
    plan.query.universe = NS(instruments=symbols)
    plan.query.filters = ()
    dataset = Dataset()
    dataset.estimate = NS(projected_row_width_upper=16, provider_duckdb_memory_bytes=65536,
                          output_max_rows=1000, output_max_bytes=65536)
    calls.clear()
    combined = NativeDataBinding(declaration, module._daily_reader(dataset, plan, binding, projector, 65536))
    assert [item["values"]["amount"] for item in combined.rows(request)] == [
        day.day * 10 + number for day in days[:7] for number in range(len(symbols))]
    assert [item.value for item in calls] == [days[:9]]
    assert module._universe_scan_days(dataset, plan, 65536) == 128
    assert module._universe_scan_days(dataset, plan, 2 * 16 * 3) == 3
    dataset.estimate.output_max_rows = 4
    assert module._universe_scan_days(dataset, plan, 65536) == 2
    plan.primary_key = ("event_date", "code", "class")
    assert module._universe_scan_days(dataset, plan, 65536) == 8
    plan.query.filters = (NS(field_id="class", operator="eq"),)
    assert module._universe_scan_days(dataset, plan, 65536) == 2
