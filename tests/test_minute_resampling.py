from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
import struct

import pyarrow as pa
import pytest

from research_pipeline.data_plane import (
    MinuteResampleBudget,
    MinuteResampleStream,
    MinuteScanBudget,
    MinuteScanPlan,
    MinuteSourcePartition,
    build_minute_resample_plan,
)
from research_pipeline.domain import load_session_policy_bundle
from research_pipeline.platform import load_minute_capability_manifest


CAPABILITY_MANIFEST_HASH = load_minute_capability_manifest().manifest_hash


class _Batches:
    def __init__(self, rows: list[dict[str, object]], batch_size: int) -> None:
        self.rows = rows
        self.batch_size = batch_size
        self.content_hash = hashlib.sha256(
            json.dumps(rows, sort_keys=True, default=str).encode()
        ).hexdigest()

    def __iter__(self):
        schema = _source_schema("open_interest" in self.rows[0])
        for offset in range(0, len(self.rows), self.batch_size):
            yield pa.RecordBatch.from_pylist(
                self.rows[offset : offset + self.batch_size], schema=schema
            )


def _source_schema(futures: bool = False):
    fields = [
        ("code", pa.string()),
        ("dt", pa.timestamp("us")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("money", pa.float64()),
        ("avg", pa.float64()),
    ]
    if futures:
        fields.append(("open_interest", pa.float64()))
    return pa.schema(fields)


def _scan_plan(asset_class: str, code: str, start: str, end: str, as_of: str):
    columns = (
        "code", "dt", "open", "high", "low", "close", "volume", "money", "avg"
    )
    if asset_class == "cn_future":
        columns = (*columns, "open_interest")
    policy = {
        "cn_stock": "session.cn_stock.minute.v1",
        "cn_index": "session.cn_index.minute.v1",
        "cn_future": "session.cn_futures.minute.v1",
    }[asset_class]
    source_directory = {
        "cn_stock": "stock",
        "cn_index": "index",
        "cn_future": "futures",
    }[asset_class]
    partition_path = f"{source_directory}/year=2024/month=01/data.parquet"
    return MinuteScanPlan(
        "a" * 64,
        source_directory,
        columns,
        (code,),
        datetime.fromisoformat(start),
        datetime.fromisoformat(end),
        datetime.fromisoformat(as_of),
        (MinuteSourcePartition(partition_path, 1, 1, 1, 1, "b" * 64),),
        10_000,
        10_000_000,
        100,
        MinuteScanBudget(),
        asset_class,
        policy,
        {
            "cn_stock": ("quality.minute.common_fields.v1",),
            "cn_index": (
                "quality.minute.common_fields.v1",
                "quality.minute.index.v1",
            ),
            "cn_future": (
                "quality.minute.common_fields.v1",
                "quality.minute.futures.avg_coverage.v1",
                "quality.minute.futures.open_interest.v1",
            ),
        }[asset_class],
        CAPABILITY_MANIFEST_HASH,
    )


def _metadata(asset_class: str):
    bundle = load_session_policy_bundle()
    return bundle, next(
        item.instrument for item in bundle.policies if item.instrument.asset_class == asset_class
    )


def _session_rows(asset_class: str) -> list[dict[str, object]]:
    bundle, instrument = _metadata(asset_class)
    policy = next(item for item in bundle.policies if item.instrument == instrument)
    session = policy.build_session(
        policy.trading_dates[0],
        scope_binding_hash=bundle.capability_binding_hash,
    )
    rows = []
    sequence = 0
    for segment in session.segments:
        for offset in range(1, int((segment.ends_at - segment.starts_at) / timedelta(minutes=1)) + 1):
            sequence += 1
            price = 100.0 + sequence / 100
            row = {
                "code": instrument.instrument_id,
                "dt": (segment.starts_at + timedelta(minutes=offset)).replace(tzinfo=None),
                "open": price,
                "high": price + 0.2,
                "low": price - 0.2,
                "close": price + 0.1,
                "volume": float(sequence),
                "money": float(sequence * 100),
                "avg": None if asset_class == "cn_future" else price + 0.05,
            }
            if asset_class == "cn_future":
                row["open_interest"] = float(1000 + sequence)
            rows.append(row)
    return rows


def _collect(source: _Batches, plan):
    stream = MinuteResampleStream(source, plan=plan)
    rows = [row for batch in stream for row in batch.to_pylist()]
    return rows, stream.manifest


def test_one_minute_is_session_anchored_and_deterministic() -> None:
    bundle, instrument = _metadata("cn_index")
    scan = _scan_plan(
        "cn_index",
        instrument.instrument_id,
        "2024-01-02T09:31:00+08:00",
        "2024-01-02T15:01:00+08:00",
        "2024-01-02T15:00:00+08:00",
    )
    rows = _session_rows("cn_index")
    expected_counts = {1: 240}
    for interval, expected in expected_counts.items():
        plan = build_minute_resample_plan(
            scan,
            interval_minutes=interval,
            session_bundle=bundle,
            instruments=(instrument,),
            budget=MinuteResampleBudget(output_batch_rows=7),
        )
        first_rows, first = _collect(_Batches(rows, 1), plan)
        alternate_plan = build_minute_resample_plan(
            scan,
            interval_minutes=interval,
            session_bundle=bundle,
            instruments=(instrument,),
            budget=MinuteResampleBudget(output_batch_rows=31),
        )
        second_rows, second = _collect(_Batches(rows, 37), alternate_plan)
        assert len(first_rows) == expected
        assert first_rows == second_rows
        assert first.artifact_semantics_hash == second.artifact_semantics_hash
        assert first.output_rows == second.output_rows
        assert first.status_counts == second.status_counts
        assert "buckets" not in plan.to_dict()
        assert all(item["completed"] for item in first_rows)
        assert all(item["bar_start"].hour != 11 or item["dt"].hour <= 11 for item in first_rows)
        assert not any(item["bar_start"].hour < 12 < item["dt"].hour for item in first_rows)

    futures_bundle, future = _metadata("cn_future")
    futures_scan = _scan_plan(
        "cn_future",
        future.instrument_id,
        "2024-01-02T21:01:00+08:00",
        "2024-01-03T15:01:00+08:00",
        "2024-01-03T15:00:00+08:00",
    )
    futures_plan = build_minute_resample_plan(
        futures_scan,
        interval_minutes=1,
        session_bundle=futures_bundle,
        instruments=(future,),
    )
    future_rows, _ = _collect(_Batches(_session_rows("cn_future"), 53), futures_plan)
    assert {item["trading_date"].isoformat() for item in future_rows} == {"2024-01-03"}
    assert all(item["bar_status"] == "complete" for item in future_rows)
    assert all(item["open_interest"] is not None for item in future_rows)
    assert all(item["avg_status"] == "unavailable_source_avg" for item in future_rows)


def test_one_minute_prices_avg_and_zero_volume_are_preserved() -> None:
    bundle, instrument = _metadata("cn_index")
    scan = _scan_plan(
        "cn_index", instrument.instrument_id,
        "2024-01-02T09:31:00+08:00", "2024-01-02T09:36:00+08:00",
        "2024-01-02T09:35:00+08:00",
    )
    plan = build_minute_resample_plan(
        scan, interval_minutes=1, session_bundle=bundle, instruments=(instrument,),
    )
    rows = _session_rows("cn_index")[:5]
    actual, _ = _collect(_Batches(rows, 2), plan)
    assert len(actual) == len(rows)
    for source, output in zip(rows, actual):
        for key in ("code", "dt", "open", "high", "low", "close", "volume", "money"):
            assert output[key] == source[key]
        assert output["avg"] == pytest.approx(source["avg"])
    zero_rows = [dict(row, volume=0.0, money=0.0, avg=None) for row in rows]
    zero, _ = _collect(_Batches(zero_rows, 3), plan)
    assert all(row["avg"] is None and row["avg_status"] == "undefined_zero_volume" for row in zero)


@pytest.mark.parametrize("interval", [5, 15, 30, 60, 120])
def test_derived_minute_intervals_are_rejected_before_consumption(interval):
    from research_pipeline.data_plane.errors import QueryIRInvalidError

    bundle, instrument = _metadata("cn_index")
    scan = _scan_plan(
        "cn_index", instrument.instrument_id,
        "2024-01-02T09:31:00+08:00", "2024-01-02T09:36:00+08:00",
        "2024-01-02T09:35:00+08:00",
    )
    with pytest.raises(QueryIRInvalidError, match="仅支持一分钟"):
        build_minute_resample_plan(scan, interval_minutes=interval,
                                  session_bundle=bundle, instruments=(instrument,))
    current = build_minute_resample_plan(scan, interval_minutes=1,
                                        session_bundle=bundle, instruments=(instrument,))
    with pytest.raises(QueryIRInvalidError, match="周期不受支持"):
        replace(current, interval_minutes=interval)


@pytest.mark.parametrize("asset_class", ["cn_index", "cn_future"])
def test_current_session_reuse_keeps_rows_and_session_boundaries(asset_class, monkeypatch):
    bundle, instrument = _metadata(asset_class)
    rows = _session_rows(asset_class)
    start = rows[0]["dt"].replace(tzinfo=__import__("zoneinfo").ZoneInfo("Asia/Shanghai"))
    end = rows[-1]["dt"].replace(tzinfo=start.tzinfo)
    scan = _scan_plan(asset_class, instrument.instrument_id, start.isoformat(), (end + timedelta(minutes=1)).isoformat(), end.isoformat())
    plan = build_minute_resample_plan(scan, interval_minutes=1, session_bundle=bundle, instruments=(instrument,))
    stream = MinuteResampleStream(_Batches(rows, 17), plan=plan)
    from research_pipeline.domain.sessions import TradingSession
    original_hash = TradingSession.session_hash.fget
    hash_calls = []
    def session_hash(session):
        hash_calls.append(session.session_id)
        return original_hash(session)
    monkeypatch.setattr(TradingSession, "session_hash", property(session_hash))
    original_resolve = stream._resolver.resolve_completed_bar_end
    calls = []
    def resolve(*args, **kwargs):
        calls.append(args[1])
        return original_resolve(*args, **kwargs)
    monkeypatch.setattr(stream._resolver, "resolve_completed_bar_end", resolve)
    actual = [row for batch in stream for row in batch.to_pylist()]
    assert len(calls) == 1
    assert len(hash_calls) == 1
    uncached = MinuteResampleStream(_Batches(rows, 31), plan=plan)
    monkeypatch.setattr(uncached, "_one_minute_batches", uncached._aggregated_batches)
    original_bucket = uncached._bucket_spec
    def bucket(code, dt):
        uncached._current_session = None
        return original_bucket(code, dt)
    monkeypatch.setattr(uncached, "_bucket_spec", bucket)
    expected = [row for batch in uncached for row in batch.to_pylist()]
    assert actual == expected
    from research_pipeline.data_plane.errors import ProviderExecutionError
    gap = start.replace(hour=12, minute=0)
    with pytest.raises(ProviderExecutionError, match="批准 session"):
        stream._bucket_spec(instrument.instrument_id, gap)


@pytest.mark.parametrize("asset_class", ["cn_index", "cn_future"])
@pytest.mark.parametrize("input_rows,output_rows", [(1, 7), (17, 31), (4096, 1)])
def test_one_minute_columnar_matches_row_implementation(
    asset_class, input_rows, output_rows, monkeypatch,
):
    bundle, instrument = _metadata(asset_class)
    rows = _session_rows(asset_class)
    # 缺少源分钟不会生成占位行，零量均价仍按原合同忽略。
    del rows[7]
    rows[0].update(volume=0.0, money=-0.0, avg=float("nan"))
    rows[1].update(volume=3.0, avg=0.1)
    rows[2].update(avg=None)
    rows[3].update(volume=1e-200, avg=1e-200)
    rows[4].update(volume=1e200, avg=1e200)
    rows[5].update(avg=-0.0)
    if asset_class == "cn_future":
        rows[3]["open_interest"] = None
    zone = __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    start = rows[0]["dt"].replace(tzinfo=zone)
    end = rows[-1]["dt"].replace(tzinfo=zone)
    scan = _scan_plan(asset_class, instrument.instrument_id, start.isoformat(),
                      (end + timedelta(minutes=1)).isoformat(), end.isoformat())
    plan = build_minute_resample_plan(
        scan, interval_minutes=1, session_bundle=bundle, instruments=(instrument,),
        budget=MinuteResampleBudget(output_batch_rows=output_rows),
    )
    reference = MinuteResampleStream(_Batches(rows, input_rows), plan=plan)
    monkeypatch.setattr(reference, "_one_minute_batches", reference._aggregated_batches)
    expected = list(reference)
    stream = MinuteResampleStream(_Batches(rows, input_rows), plan=plan)
    def unexpected(*args, **kwargs):
        pytest.fail("对齐的一分钟列式路径不应逐行创建 bucket spec")
    monkeypatch.setattr(stream, "_bucket_spec", unexpected)
    actual = list(stream)
    assert [batch.num_rows for batch in actual] == [batch.num_rows for batch in expected]
    for left, right in zip(actual, expected):
        assert left.schema == right.schema
        for name in left.schema.names:
            assert left.column(name).equals(right.column(name)), name
            if pa.types.is_float64(left.schema.field(name).type):
                for actual_value, expected_value in zip(left.column(name).to_pylist(), right.column(name).to_pylist()):
                    if actual_value is not None:
                        assert struct.pack("d", actual_value) == struct.pack("d", expected_value), name
    assert stream.manifest == reference.manifest


def _one_minute_plan(*, output_rows=7):
    bundle, instrument = _metadata("cn_index")
    return build_minute_resample_plan(
        _scan_plan("cn_index", instrument.instrument_id,
                   "2024-01-02T09:31:00+08:00", "2024-01-02T15:01:00+08:00",
                   "2024-01-02T15:00:00+08:00"),
        interval_minutes=1, session_bundle=bundle, instruments=(instrument,),
        budget=MinuteResampleBudget(output_batch_rows=output_rows),
    )


@pytest.mark.parametrize("batch_rows", [1, 2, 17])
@pytest.mark.parametrize("unaligned_index", [0, 19])
def test_one_minute_unaligned_same_bucket_keeps_row_aggregation(batch_rows, unaligned_index, monkeypatch):
    rows = _session_rows("cn_index")[:24]
    row = rows[unaligned_index]
    rows.insert(unaligned_index + 1, dict(row, dt=row["dt"] + timedelta(seconds=30)))
    plan = _one_minute_plan()
    reference = MinuteResampleStream(_Batches(rows, batch_rows), plan=plan)
    monkeypatch.setattr(reference, "_one_minute_batches", reference._aggregated_batches)
    expected = [row for batch in reference for row in batch.to_pylist()]
    stream = MinuteResampleStream(_Batches(rows, batch_rows), plan=plan)
    actual = [row for batch in stream for row in batch.to_pylist()]
    assert actual == expected
    assert stream.manifest == reference.manifest


@pytest.mark.parametrize("field,value,message", [
    ("open", None, "数值"), ("high", float("inf"), "有限数"),
    ("low", 1000.0, "下界"), ("high", 100.05, "上界"),
    ("volume", -1.0, "不能为负"), ("money", -1.0, "不能为负"),
    ("avg", float("nan"), "有限数"), ("dt", None, "timestamp"),
    ("code", "not-admitted", "计划外 instrument"),
    ("dt", datetime(2024, 1, 2, 12), "批准 session"),
])
def test_one_minute_columnar_rejects_invalid_rows(field, value, message):
    from research_pipeline.data_plane.errors import ProviderExecutionError

    rows = _session_rows("cn_index")[:1]
    rows[0][field] = value
    with pytest.raises(ProviderExecutionError, match=message):
        list(MinuteResampleStream(_Batches(rows, 1), plan=_one_minute_plan()))


@pytest.mark.parametrize("batch_rows", [1, 17])
@pytest.mark.parametrize("duplicate", [True, False])
def test_one_minute_columnar_rejects_unsorted_or_duplicate(batch_rows, duplicate):
    from research_pipeline.data_plane.errors import ProviderExecutionError

    rows = _session_rows("cn_index")[:3]
    rows[-1] = dict(rows[0]) if duplicate else dict(rows[0], dt=rows[0]["dt"] - timedelta(minutes=1))
    with pytest.raises(ProviderExecutionError, match="严格递增"):
        list(MinuteResampleStream(_Batches(rows, batch_rows), plan=_one_minute_plan()))


@pytest.mark.parametrize("boundary", ["start_at", "end_at", "as_of"])
def test_one_minute_columnar_preserves_query_visibility(boundary):
    from research_pipeline.data_plane.errors import ProviderExecutionError

    rows = _session_rows("cn_index")[:3]
    plan = _one_minute_plan()
    zone = __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    value = rows[1]["dt"].replace(tzinfo=zone)
    plan = replace(plan, **{boundary: value})
    with pytest.raises(ProviderExecutionError, match="查询边界"):
        list(MinuteResampleStream(_Batches(rows, 2), plan=plan))


@pytest.mark.parametrize("timezone", [None, "UTC", "Asia/Shanghai"])
def test_one_minute_columnar_multiple_sessions_and_instruments(timezone, monkeypatch):
    from tests.test_minute_project_scope import project_inputs

    codes = tuple(f"{index:06d}.XSHE" for index in range(2, 12))
    bundle, _ = project_inputs(codes)
    policies = bundle.policies
    instruments = tuple(sorted((item.instrument for item in policies), key=lambda item: item.instrument_id))
    rows = []
    template = _session_rows("cn_index")[0]
    for instrument in instruments:
        policy = next(item for item in policies if item.instrument == instrument)
        for trading_date in policy.trading_dates:
            session = policy.build_session(trading_date, scope_binding_hash=bundle.capability_binding_hash)
            for segment in session.segments:
                for offset in range(1, int((segment.ends_at - segment.starts_at).total_seconds() // 60) + 1):
                    rows.append(dict(template, code=instrument.instrument_id,
                                     dt=(segment.starts_at + timedelta(minutes=offset)).replace(tzinfo=None)))
    rows.sort(key=lambda row: (row["code"], row["dt"]))
    zone = __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    start = min(row["dt"] for row in rows).replace(tzinfo=zone)
    end = max(row["dt"] for row in rows).replace(tzinfo=zone)
    assert len(rows) > 4096
    scan = _scan_plan("cn_stock", instruments[0].instrument_id, start.isoformat(),
                      (end + timedelta(minutes=1)).isoformat(), end.isoformat())
    scan = replace(scan, instruments=tuple(item.instrument_id for item in instruments))
    plan = build_minute_resample_plan(scan, interval_minutes=1, session_bundle=bundle,
                                     instruments=instruments, budget=MinuteResampleBudget(output_batch_rows=31))
    schema = _source_schema()
    if timezone is not None:
        target_zone = __import__("zoneinfo").ZoneInfo(timezone)
        rows = [dict(row, dt=row["dt"].replace(tzinfo=zone).astimezone(target_zone)) for row in rows]
        schema = schema.set(1, pa.field("dt", pa.timestamp("us", tz=timezone)))
    class Source:
        source_identity = "a" * 64
        def __iter__(self):
            # 一个源批次含多个证券和交易日，覆盖跨源批次的会话归属。
            yield pa.RecordBatch.from_pylist(rows, schema=schema)
    reference = MinuteResampleStream(Source(), plan=plan)
    monkeypatch.setattr(reference, "_one_minute_batches", reference._aggregated_batches)
    expected = pa.Table.from_batches(list(reference))
    stream = MinuteResampleStream(Source(), plan=plan)
    sizes = []
    normalize = stream._one_minute_table
    def bounded(table, previous_key):
        sizes.append(table.num_rows)
        return normalize(table, previous_key)
    monkeypatch.setattr(stream, "_one_minute_table", bounded)
    batches = list(stream)
    actual = pa.Table.from_batches(batches)
    assert actual.equals(expected)
    assert max(sizes) <= 65_537
    assert max(batch.num_rows for batch in batches) <= plan.budget.output_batch_rows
    assert stream.manifest == reference.manifest


@pytest.mark.parametrize("field,value", [("avg", float("inf")), ("avg", None),
                                         ("open_interest", float("nan"))])
def test_one_minute_futures_nullable_field_validation(field, value):
    from research_pipeline.data_plane.errors import ProviderExecutionError

    bundle, instrument = _metadata("cn_future")
    rows = _session_rows("cn_future")[:2]
    rows[0][field] = value
    zone = __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    start = rows[0]["dt"].replace(tzinfo=zone)
    end = rows[-1]["dt"].replace(tzinfo=zone)
    scan = _scan_plan("cn_future", instrument.instrument_id, start.isoformat(),
                      (end + timedelta(minutes=1)).isoformat(), end.isoformat())
    plan = build_minute_resample_plan(scan, interval_minutes=1, session_bundle=bundle, instruments=(instrument,))
    stream = MinuteResampleStream(_Batches(rows, 1), plan=plan)
    if value is None:
        actual = [row for batch in stream for row in batch.to_pylist()]
        assert actual[0]["avg_status"] == "unavailable_source_avg"
    else:
        with pytest.raises(ProviderExecutionError, match="有限数"):
            list(stream)
