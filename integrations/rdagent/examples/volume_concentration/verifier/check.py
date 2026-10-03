"""根据封存分钟与日历独立复算教学公式，不加载候选实现。"""
from datetime import datetime
import json
import math

import pyarrow.parquet as pq

PREFIX = "project.volume-concentration"
SCHEMAS = {name: PREFIX + "." + name + ".v1" for name in ("bars", "calendar", "coverage", "daily")}


def verify_tables(tables):
    calendar = tables["calendar"]
    coverage = tables["coverage"]
    dates = [row["session"] for row in calendar]
    entities = sorted({row["entity"] for row in coverage})
    if len(dates) != 5 or dates != sorted(set(dates)) or len(entities) != 2:
        return ["formula.scope_invalid"]
    grid = {(entity, date): {} for entity in entities for date in dates}
    by_date = {row["session"]: row for row in calendar}
    for day in calendar:
        times = [datetime.fromisoformat(value) for value in day["expected_bars"]]
        if (len(times) != 240 or times != sorted(set(times)) or any(value.tzinfo is None or value.date().isoformat() != day["session"] for value in times)):
            return ["formula.calendar_invalid"]
    for row in tables["bars"]:
        key, index = (row["entity"], row["session"]), row["bar_index"]
        if key not in grid or type(index) is not int or not 0 <= index < 240 or index in grid[key]:
            return ["formula.bars_invalid"]
        if row["dt"] != by_date[key[1]]["expected_bars"][index]:
            return ["formula.bar_time_mismatch"]
        grid[key][index] = row["volume"]
    if len(coverage) != len(grid) or {(row["entity"], row["session"]) for row in coverage} != set(grid):
        return ["formula.coverage_invalid"]
    if any(row["raw_rows"] != len(grid[(row["entity"], row["session"])]) for row in coverage):
        return ["formula.coverage_count_mismatch"]
    actual = {(row["entity"], row["session"]): row for row in tables["daily"]}
    if len(actual) != len(tables["daily"]) or set(actual) != set(grid):
        return ["formula.daily_keys_mismatch"]
    lengths = {row.get("window_length", 3) for row in tables["daily"]}
    if len(lengths) != 1 or any(type(length) is not int or not 1 <= length <= 5 for length in lengths):
        return ["formula.window_length_invalid"]
    window_length = next(iter(lengths))
    findings = []
    for entity in entities:
        history = []
        for date in dates:
            values = grid[(entity, date)]
            value = None
            if len(values) != 240:
                status = "missing_bars"
            elif any(v is None or not math.isfinite(v) or v < 0 for v in values.values()):
                status = "invalid_volume"
            else:
                total = math.fsum(values.values())
                status = "zero_volume" if total == 0 else "computed"
                if total:
                    value = math.fsum((v / total) ** 2 for v in values.values())
            history.append((date, value, status))
            window = history[-window_length:]
            rolling_status = "warmup" if len(window) < window_length else "invalid_window" if any(v[2] != "computed" for v in window) else "computed"
            rolling_value = math.fsum(v[1] for v in window) / window_length if rolling_status == "computed" else None
            wanted = {"value": value, "status": status, "rolling_value": rolling_value, "rolling_status": rolling_status,
                      "window_sessions": [row[0] for row in window], "available_at": by_date[date]["expected_bars"][-1]}
            row = actual[(entity, date)]
            for field, expected in wanted.items():
                observed = row[field]
                if isinstance(expected, float):
                    okay = observed is not None and math.isfinite(observed) and math.isclose(observed, expected, rel_tol=1e-10, abs_tol=1e-12)
                else:
                    okay = observed == expected
                if not okay:
                    findings.append("formula." + field + "_mismatch:" + entity + ":" + date)
    return findings


def verify(context, input_root):
    manifest = json.loads((input_root / "manifest.json").read_text(encoding="utf-8"))
    schemas = {table["schema_id"]: table for table in manifest["tables"]}
    if not set(SCHEMAS.values()) <= set(schemas):
        findings = ["formula.required_tables_missing"]
    else:
        tables = {name: pq.read_table([input_root / path for path in schemas[schema]["files"]], use_threads=False).to_pylist()
                  for name, schema in SCHEMAS.items()}
        findings = verify_tables(tables)
    return {"contract_version": "project-verifier-output-v1", "status": "fail" if findings else "pass",
            "result_id": context["result_id"], "findings": sorted(set(findings)), "evidence_hashes": {}}
