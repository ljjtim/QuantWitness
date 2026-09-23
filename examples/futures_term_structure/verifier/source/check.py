"""不调用生产算法的期货合约选择与期限结构复核。"""

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
    metrics = _table(input_root, "project.quantwitness.futures-term-structure.metrics.v1")
    rows = _table(input_root, "project.quantwitness.futures-term-structure.observations.v1")
    findings = []
    if len(metrics) != 1 or not rows:
        findings.append("期货 Result 表为空或指标不唯一")
    else:
        decision = _time(rows[0]["decision_at"])
        minimum_days = int(rows[0]["minimum_days_to_expiry"])
        minimum_volume = int(rows[0]["minimum_volume"])
        eligible = sorted(
            (row["expiry"], row["contract"], row["settlement"], (row["expiry"] - decision.date()).days)
            for row in rows
            if _time(row["available_at"]) <= decision
            and (row["expiry"] - decision.date()).days >= minimum_days
            and row["volume"] >= minimum_volume
            and row["settlement"] > 0
        )
        expected = None
        if len(eligible) >= 2:
            near, deferred = eligible[:2]
            expected = (deferred[2] / near[2] - 1) * 365.0 / (deferred[3] - near[3])
        if expected is None or abs(float(metrics[0]["value"]) - expected) > 1e-12:
            findings.append("期限结构指标无法由封存输入独立重算")
    return {
        "contract_version": "project-verifier-output-v1",
        "status": "pass" if not findings else "fail",
        "result_id": context["result_id"],
        "findings": sorted(findings),
        "evidence_hashes": {},
    }
