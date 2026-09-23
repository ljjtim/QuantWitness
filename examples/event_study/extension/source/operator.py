"""项目内公告版本选择、重叠事件隔离与事后窗口收益。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


LOCAL_TIMEZONE = timezone(timedelta(hours=8))


def _visible(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TIMEZONE)
    return parsed


def evaluate(
    events: list[dict[str, object]], prices: list[dict[str, object]], *,
    fixed_clock: str, minimum_gap_days: int,
) -> list[dict[str, object]]:
    clock = _visible(fixed_clock)
    if minimum_gap_days < 1:
        raise ValueError("重叠事件的最小间隔必须为正")
    latest: dict[str, dict[str, object]] = {}
    for event in events:
        decision = _visible(event["decision_at"])
        if decision > clock or _visible(event["available_at"]) > decision:
            continue
        event_day = date.fromisoformat(str(event["event_day"]))
        if event_day > decision.date():
            raise ValueError("事件日期不能晚于决策日")
        event_id = str(event["event_id"])
        previous = latest.get(event_id)
        if previous is None or int(previous["revision"]) < int(event["revision"]):
            latest[event_id] = event
    ordered = sorted(latest.values(), key=lambda item: (item["event_day"], item["instrument"], item["event_id"]))
    chosen: list[dict[str, object]] = []
    last_event: dict[str, date] = {}
    for event in ordered:
        instrument = str(event["instrument"])
        event_day = date.fromisoformat(str(event["event_day"]))
        if instrument in last_event and (event_day - last_event[instrument]).days < minimum_gap_days:
            continue
        chosen.append(event)
        last_event[instrument] = event_day
    observed: dict[tuple[str, date], dict[str, object]] = {}
    for row in prices:
        key = (str(row["instrument"]), date.fromisoformat(str(row["session"])))
        if key in observed:
            raise ValueError("同一标的交易日行情重复")
        _visible(row["available_at"])
        observed[key] = row
    result = []
    for event in chosen:
        instrument = str(event["instrument"])
        event_day = date.fromisoformat(str(event["event_day"]))
        on_or_after = sorted(session for name, session in observed if name == instrument and session >= event_day)
        if len(on_or_after) < 2:
            continue
        base, next_session = on_or_after[:2]
        if base != event_day:
            continue
        start = observed[(instrument, base)]
        end = observed[(instrument, next_session)]
        decision = _visible(event["decision_at"])
        end_available = _visible(end["available_at"])
        if end_available <= decision or end_available > clock:
            raise ValueError("事件后结果必须在事件决策后、研究冻结前可见")
        start_price, end_price = float(start["close"]), float(end["close"])
        if min(start_price, end_price) <= 0:
            raise ValueError("事件窗口价格必须为正")
        result.append({
            "event_id": str(event["event_id"]),
            "revision": int(event["revision"]),
            "surprise": float(event["surprise"]),
            "window_return": end_price / start_price - 1.0,
        })
    return result


def run(context, inputs, output_root):
    import pyarrow as pa
    import pyarrow.parquet as pq

    by_port = {item.port: item for item in inputs}
    if set(by_port) != {"data"}:
        raise ValueError("事件研究只接受 data 输入")
    parameters = context.parameters
    source = by_port["data"].request(parameters["data_request_id"])
    field_map = {
        "fld_qw_event_kind": "kind",
        "fld_qw_event_id": "event_id",
        "fld_qw_event_instrument": "instrument",
        "fld_qw_event_day": "event_day",
        "fld_qw_event_revision": "revision",
        "fld_qw_event_decision_at": "decision_at",
        "fld_qw_event_surprise": "surprise",
        "fld_qw_event_session": "session",
        "fld_qw_event_close": "close",
        "fld_qw_event_available_at": "available_at",
    }
    source_rows = [
        {target: row[source] for source, target in field_map.items()}
        for batch in source.iter_batches(columns=tuple(field_map), batch_size=8192)
        for row in batch.to_pylist()
    ]
    events = [row for row in source_rows if row["kind"] == "event"]
    prices = [row for row in source_rows if row["kind"] == "price"]
    result = evaluate(
        events,
        prices,
        fixed_clock=parameters["fixed_clock"],
        minimum_gap_days=parameters["minimum_gap_days"],
    )
    if not result:
        raise ValueError("事件研究没有合格事件")
    metrics = pa.table({
        "metric_ref": ["project.quantwitness.event_study.mean_window_return@1.0.0"],
        "value": [sum(item["window_return"] for item in result) / len(result)],
        "unit": ["decimal_return"],
        "sample_start": [min(str(item["event_day"]) for item in events)],
        "sample_end": [max(str(item["session"]) for item in prices)],
        "sample_size": pa.array([len(result)], type=pa.int64()),
        "status": ["computed"],
    })
    observations = pa.Table.from_pylist([
        {
            **row,
            "fixed_clock": parameters["fixed_clock"],
            "minimum_gap_days": parameters["minimum_gap_days"],
        }
        for row in source_rows
    ])
    root = output_root / "result"
    (root / "metrics").mkdir(parents=True)
    (root / "observations").mkdir(parents=True)
    pq.write_table(metrics, root / "metrics/part-00000.parquet", row_group_size=8192)
    pq.write_table(observations, root / "observations/part-00000.parquet", row_group_size=8192)
    return output_root.commit_directory(
        port="result",
        artifact_type="project.quantwitness.event-study.v1",
        relative_path="result",
        files=("metrics/part-00000.parquet", "observations/part-00000.parquet"),
    )
