"""项目内期限结构选择与斜率算法。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


LOCAL_TIMEZONE = timezone(timedelta(hours=8))


def _time(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=LOCAL_TIMEZONE)


def evaluate(
    rows: list[dict[str, object]], *, decision_at: str,
    minimum_days_to_expiry: int, minimum_volume: int,
) -> dict[str, object]:
    decision = _time(decision_at)
    if decision.tzinfo is None or minimum_days_to_expiry < 1 or minimum_volume < 0:
        raise ValueError("决策时间、到期天数和成交量阈值无效")
    eligible = []
    for row in rows:
        available = _time(row["available_at"])
        if available > decision:
            continue
        expiry = date.fromisoformat(str(row["expiry"]))
        days = (expiry - decision.date()).days
        settlement = float(row["settlement"])
        volume = int(row["volume"])
        if days >= minimum_days_to_expiry and volume >= minimum_volume and settlement > 0:
            eligible.append((expiry, str(row["contract"]), settlement, days))
    eligible.sort()
    if len(eligible) < 2:
        raise ValueError("期限结构至少需要两个当时可见的合格合约")
    near, deferred = eligible[:2]
    return {
        "selected_contract": near[1],
        "near_expiry": near[0].isoformat(),
        "deferred_contract": deferred[1],
        "annualized_slope": (deferred[2] / near[2] - 1.0) * 365.0 / (deferred[3] - near[3]),
        "signal": "carry_short" if deferred[2] > near[2] else "carry_long",
    }


def run(context, inputs, output_root):
    import pyarrow as pa
    import pyarrow.parquet as pq

    by_port = {item.port: item for item in inputs}
    if set(by_port) != {"data"}:
        raise ValueError("期货期限结构项目只接受 data 输入")
    parameters = context.parameters
    source = by_port["data"].request(parameters["data_request_id"])
    field_map = {
        "fld_qw_fut_contract": "contract",
        "fld_qw_fut_expiry": "expiry",
        "fld_qw_fut_settlement": "settlement",
        "fld_qw_fut_volume": "volume",
        "fld_qw_fut_available_at": "available_at",
    }
    source_rows = [
        {target: row[source] for source, target in field_map.items()}
        for batch in source.iter_batches(columns=tuple(field_map), batch_size=8192)
        for row in batch.to_pylist()
    ]
    result = evaluate(
        source_rows,
        decision_at=parameters["decision_at"],
        minimum_days_to_expiry=parameters["minimum_days_to_expiry"],
        minimum_volume=parameters["minimum_volume"],
    )
    metrics = pa.table({
        "metric_ref": ["project.quantwitness.futures_term_structure.annualized_slope@1.0.0"],
        "value": [result["annualized_slope"]],
        "unit": ["annualized_decimal_slope"],
        "sample_start": [parameters["decision_at"]],
        "sample_end": [parameters["decision_at"]],
        "sample_size": pa.array([2], type=pa.int64()),
        "status": ["computed"],
    })
    observations = pa.Table.from_pylist([
        {
            **row,
            "decision_at": parameters["decision_at"],
            "minimum_days_to_expiry": parameters["minimum_days_to_expiry"],
            "minimum_volume": parameters["minimum_volume"],
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
        artifact_type="project.quantwitness.futures-term-structure.v1",
        relative_path="result",
        files=("metrics/part-00000.parquet", "observations/part-00000.parquet"),
    )
