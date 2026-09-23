"""项目内的股票横截面排序与事后评价。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


LOCAL_TIMEZONE = timezone(timedelta(hours=8))


def _time(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TIMEZONE)
    return parsed


def _date(value: object) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def evaluate(
    rows: list[dict[str, object]], *, decision_at: str, observation_sessions: tuple[str, str],
    outcome_session: str,
) -> list[dict[str, object]]:
    """先冻结可见截面，再单独读取未来结果；未来数据不能影响排名。"""
    decision = _time(decision_at)
    first, last = map(date.fromisoformat, observation_sessions)
    outcome = date.fromisoformat(outcome_session)
    if not first < last < outcome or decision.date() <= last:
        raise ValueError("决策、观察和结果交易日必须严格有序")
    observations: dict[str, dict[date, float]] = {}
    future: dict[str, float] = {}
    for row in rows:
        session = _date(row["session"])
        instrument = str(row["instrument"])
        price = float(row["close"])
        available = _time(row["available_at"])
        if price <= 0:
            raise ValueError("收盘价必须为正")
        if session in (first, last):
            if available > decision:
                continue
            by_session = observations.setdefault(instrument, {})
            if session in by_session:
                raise ValueError("同一证券交易日有多条可见记录")
            by_session[session] = price
        elif session == outcome:
            if available <= decision:
                raise ValueError("结果记录不能在决策前可见")
            if instrument in future:
                raise ValueError("同一证券结果日有多条记录")
            future[instrument] = price
    selected = []
    for instrument, known in observations.items():
        if first not in known or last not in known or instrument not in future:
            continue
        selected.append({
            "instrument": instrument,
            "signal_return": known[last] / known[first] - 1.0,
            "outcome_return": future[instrument] / known[last] - 1.0,
        })
    return sorted(selected, key=lambda item: (-item["signal_return"], item["instrument"]))


def run(context, inputs, output_root):
    """从已准入 request 读取完整表，提交项目指标与可独立复核的输入快照。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    by_port = {item.port: item for item in inputs}
    if set(by_port) != {"data"}:
        raise ValueError("横截面项目只接受 data 输入")
    parameters = context.parameters
    table = by_port["data"].request(parameters["data_request_id"])
    field_map = {
        "fld_qw_eq_session": "session",
        "fld_qw_eq_instrument": "instrument",
        "fld_qw_eq_close": "close",
        "fld_qw_eq_available_at": "available_at",
    }
    source_rows = [
        {target: row[source] for source, target in field_map.items()}
        for batch in table.iter_batches(columns=tuple(field_map), batch_size=8192)
        for row in batch.to_pylist()
    ]
    result = evaluate(
        source_rows,
        decision_at=parameters["decision_at"],
        observation_sessions=tuple(parameters["observation_sessions"]),
        outcome_session=parameters["outcome_session"],
    )
    if len(result) < 2:
        raise ValueError("横截面结果至少需要两个证券")
    value = result[0]["outcome_return"] - result[-1]["outcome_return"]
    metric_ref = "project.quantwitness.equity_cross_section.spread@1.0.0"
    metrics = pa.table({
        "metric_ref": [metric_ref],
        "value": [value],
        "unit": ["decimal_return"],
        "sample_start": [parameters["observation_sessions"][0]],
        "sample_end": [parameters["outcome_session"]],
        "sample_size": pa.array([len(result)], type=pa.int64()),
        "status": ["computed"],
    })
    observations = pa.Table.from_pylist([
        {
            **row,
            "decision_at": parameters["decision_at"],
            "observation_sessions": list(parameters["observation_sessions"]),
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
        artifact_type="project.quantwitness.equity-cross-section.v1",
        relative_path="result",
        files=("metrics/part-00000.parquet", "observations/part-00000.parquet"),
    )
