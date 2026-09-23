"""不调用生产算法的 ETF 时间序列 Result 复核。"""

from datetime import datetime, timedelta, timezone
import json

import pyarrow.parquet as pq


LOCAL_TIMEZONE = timezone(timedelta(hours=8))


def _time(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=LOCAL_TIMEZONE)


def _table(input_root, schema_id):
    manifest = json.loads((input_root / "manifest.json").read_text(encoding="utf-8"))
    item = next(table for table in manifest["tables"] if table["schema_id"] == schema_id)
    return pq.read_table([input_root / path for path in item["files"]]).to_pylist()


def verify(context, input_root):
    metrics = _table(input_root, "project.quantwitness.etf-time-series.metrics.v1")
    rows = _table(input_root, "project.quantwitness.etf-time-series.observations.v1")
    findings = []
    if len(metrics) != 1 or not rows:
        findings.append("ETF Result 表为空或指标不唯一")
    else:
        decision = _time(rows[0]["decision_at"])
        history = tuple(rows[0]["history_sessions"])
        outcome = rows[0]["outcome_session"]
        known = {}
        future = {}
        for row in rows:
            session = row["session"].isoformat()
            visible = _time(row["available_at"])
            if session in history and visible <= decision:
                known.setdefault(row["instrument"], {})[session] = row
            if session == outcome and visible > decision:
                future[row["instrument"]] = row
        values = []
        for instrument, observations in known.items():
            if set(observations) == set(history) and instrument in future:
                momentum = observations[history[-1]]["close"] / observations[history[0]]["close"] - 1
                outcome_row = future[instrument]
                realized = outcome_row["close"] / outcome_row["open"] - 1
                values.append(int(momentum > 0) * realized)
        expected = sum(values) / len(values) if values else None
        if expected is None or abs(float(metrics[0]["value"]) - expected) > 1e-12:
            findings.append("ETF 指标无法由封存输入独立重算")
    return {
        "contract_version": "project-verifier-output-v1",
        "status": "pass" if not findings else "fail",
        "result_id": context["result_id"],
        "findings": sorted(findings),
        "evidence_hashes": {},
    }
