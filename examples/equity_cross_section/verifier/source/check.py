"""不调用生产算法的横截面 Result 复核。"""

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
    metrics = _table(input_root, "project.quantwitness.equity-cross-section.metrics.v1")
    rows = _table(input_root, "project.quantwitness.equity-cross-section.observations.v1")
    finding = []
    if len(metrics) != 1 or not rows:
        finding.append("横截面 Result 表为空或指标不唯一")
    else:
        decision = _time(rows[0]["decision_at"])
        sessions = tuple(rows[0]["observation_sessions"])
        outcome = rows[0]["outcome_session"]
        known = {}
        future = {}
        for row in rows:
            if row["session"].isoformat() in sessions and _time(row["available_at"]) <= decision:
                known.setdefault(row["instrument"], {})[row["session"].isoformat()] = row["close"]
            if row["session"].isoformat() == outcome and _time(row["available_at"]) > decision:
                future[row["instrument"]] = row["close"]
        ranked = []
        for instrument, values in known.items():
            if set(values) == set(sessions) and instrument in future:
                ranked.append((values[sessions[-1]] / values[sessions[0]] - 1, instrument,
                               future[instrument] / values[sessions[-1]] - 1))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        expected = ranked[0][2] - ranked[-1][2] if len(ranked) >= 2 else None
        if expected is None or abs(float(metrics[0]["value"]) - expected) > 1e-12:
            finding.append("横截面指标无法由封存输入独立重算")
    return {
        "contract_version": "project-verifier-output-v1",
        "status": "pass" if not finding else "fail",
        "result_id": context["result_id"],
        "findings": sorted(finding),
        "evidence_hashes": {},
    }
