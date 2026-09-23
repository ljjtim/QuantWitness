"""ETF 项目按单标的历史日线生成信号并观察下一日表现。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


LOCAL_TIMEZONE = timezone(timedelta(hours=8))


def _time(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=LOCAL_TIMEZONE)


def evaluate(
    rows: list[dict[str, object]], *, decision_at: str,
    history_sessions: tuple[str, ...], outcome_session: str,
) -> list[dict[str, object]]:
    decision = _time(decision_at)
    if decision.tzinfo is None or len(history_sessions) < 2:
        raise ValueError("决策时间须带时区，且历史至少包含两日")
    history = tuple(date.fromisoformat(value) for value in history_sessions)
    future_day = date.fromisoformat(outcome_session)
    if tuple(sorted(set(history))) != history or not history[-1] < future_day:
        raise ValueError("交易日必须严格递增")
    by_instrument: dict[str, dict[date, dict[str, object]]] = {}
    future: dict[str, dict[str, object]] = {}
    for row in rows:
        session = row["session"] if isinstance(row["session"], date) else date.fromisoformat(str(row["session"]))
        visible = _time(row["available_at"])
        instrument = str(row["instrument"])
        if session in history and visible <= decision:
            known = by_instrument.setdefault(instrument, {})
            if session in known:
                raise ValueError("单标的历史日线重复")
            known[session] = row
        if session == future_day:
            if visible <= decision or instrument in future:
                raise ValueError("下一日记录必须在决策后可见且唯一")
            future[instrument] = row
    results = []
    for instrument, known in sorted(by_instrument.items()):
        if len(known) != len(history) or instrument not in future:
            continue
        first_close = float(known[history[0]]["close"])
        last_close = float(known[history[-1]]["close"])
        next_open = float(future[instrument]["open"])
        next_close = float(future[instrument]["close"])
        if min(first_close, last_close, next_open, next_close) <= 0:
            raise ValueError("价格必须为正")
        momentum = last_close / first_close - 1.0
        results.append({
            "instrument": instrument,
            "momentum": momentum,
            "signal": int(momentum > 0),
            "next_session_intraday_return": next_close / next_open - 1.0,
        })
    return results


def run(context, inputs, output_root):
    import pyarrow as pa
    import pyarrow.parquet as pq

    by_port = {item.port: item for item in inputs}
    if set(by_port) != {"data"}:
        raise ValueError("ETF 时间序列项目只接受 data 输入")
    parameters = context.parameters
    source = by_port["data"].request(parameters["data_request_id"])
    field_map = {
        "fld_qw_etf_session": "session",
        "fld_qw_etf_instrument": "instrument",
        "fld_qw_etf_open": "open",
        "fld_qw_etf_close": "close",
        "fld_qw_etf_available_at": "available_at",
    }
    source_rows = [
        {target: row[source] for source, target in field_map.items()}
        for batch in source.iter_batches(columns=tuple(field_map), batch_size=8192)
        for row in batch.to_pylist()
    ]
    result = evaluate(
        source_rows,
        decision_at=parameters["decision_at"],
        history_sessions=tuple(parameters["history_sessions"]),
        outcome_session=parameters["outcome_session"],
    )
    if not result:
        raise ValueError("ETF 时间序列项目没有完整标的")
    value = sum(item["signal"] * item["next_session_intraday_return"] for item in result) / len(result)
    metrics = pa.table({
        "metric_ref": ["project.quantwitness.etf_time_series.signal_return@1.0.0"],
        "value": [value],
        "unit": ["decimal_return"],
        "sample_start": [parameters["history_sessions"][0]],
        "sample_end": [parameters["outcome_session"]],
        "sample_size": pa.array([len(result)], type=pa.int64()),
        "status": ["computed"],
    })
    observations = pa.Table.from_pylist([
        {
            **row,
            "decision_at": parameters["decision_at"],
            "history_sessions": list(parameters["history_sessions"]),
            "outcome_session": parameters["outcome_session"],
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
        artifact_type="project.quantwitness.etf-time-series.v1",
        relative_path="result",
        files=("metrics/part-00000.parquet", "observations/part-00000.parquet"),
    )
