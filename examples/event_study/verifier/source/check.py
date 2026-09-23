"""不调用生产算法的事件版本、重叠和窗口复核。"""

from datetime import date, datetime, timedelta, timezone
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
    metrics = _table(input_root, "project.quantwitness.event-study.metrics.v1")
    rows = _table(input_root, "project.quantwitness.event-study.observations.v1")
    findings = []
    if len(metrics) != 1 or not rows:
        findings.append("事件研究 Result 表为空或指标不唯一")
    else:
        fixed_clock = _time(rows[0]["fixed_clock"])
        minimum_gap = int(rows[0]["minimum_gap_days"])
        latest = {}
        prices = {}
        for row in rows:
            if row["kind"] == "event":
                decision = _time(row["decision_at"])
                if decision <= fixed_clock and _time(row["available_at"]) <= decision:
                    previous = latest.get(row["event_id"])
                    if previous is None or previous["revision"] < row["revision"]:
                        latest[row["event_id"]] = row
            elif row["kind"] == "price":
                prices[(row["instrument"], row["session"])] = row
        chosen = []
        last = {}
        for event in sorted(latest.values(), key=lambda item: (item["event_day"], item["instrument"], item["event_id"])):
            prior = last.get(event["instrument"])
            if prior is not None and (event["event_day"] - prior).days < minimum_gap:
                continue
            chosen.append(event)
            last[event["instrument"]] = event["event_day"]
        returns = []
        for event in chosen:
            days = sorted(day for instrument, day in prices if instrument == event["instrument"] and day >= event["event_day"])
            if len(days) < 2 or days[0] != event["event_day"]:
                continue
            start = prices[(event["instrument"], days[0])]
            end = prices[(event["instrument"], days[1])]
            if not _time(event["decision_at"]) < _time(end["available_at"]) <= fixed_clock:
                findings.append("事件窗口的可见时间越界")
                continue
            returns.append(end["close"] / start["close"] - 1)
        expected = sum(returns) / len(returns) if returns else None
        if expected is None or abs(float(metrics[0]["value"]) - expected) > 1e-12:
            findings.append("事件研究指标无法由封存输入独立重算")
    return {
        "contract_version": "project-verifier-output-v1",
        "status": "pass" if not findings else "fail",
        "result_id": context["result_id"],
        "findings": sorted(set(findings)),
        "evidence_hashes": {},
    }
