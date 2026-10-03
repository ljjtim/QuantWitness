"""固定教学输入、可见时点与输出证据；候选只实现compute。"""
from datetime import datetime, timezone, timedelta
import json
import math

import pyarrow as pa

from . import compute

TZ = timezone(timedelta(hours=8))
ARTIFACT = "project.volume-concentration.evidence.v1"
SCHEMAS = {
    "bars": pa.schema([("entity", pa.string()), ("session", pa.string()), ("bar_index", pa.int64()),
                       ("dt", pa.string()), ("volume", pa.float64())]),
    "calendar": pa.schema([("session", pa.string()), ("expected_bars", pa.list_(pa.string()))]),
    "coverage": pa.schema([("entity", pa.string()), ("session", pa.string()), ("raw_rows", pa.int64())]),
    "daily": pa.schema([("entity", pa.string()), ("session", pa.string()), ("value", pa.float64()),
                        ("status", pa.string()), ("rolling_value", pa.float64()), ("rolling_status", pa.string()),
                        ("window_sessions", pa.list_(pa.string())), ("available_at", pa.string()), ("window_length", pa.int64())]),
}


def aware(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    return value.replace(tzinfo=TZ) if value.tzinfo is None else value.astimezone(TZ)


def preflight(context):
    calendar = json.loads(context.parameters["calendar_json"])
    window_length = context.parameters.get("window_sessions", 3)
    if type(window_length) is not int or not 1 <= window_length <= 5:
        raise ValueError("窗口必须在1至5个session之间")
    entities = json.loads(context.parameters["entities_json"])
    if len(calendar) != 5 or len(entities) != 2 or entities != sorted(set(entities)):
        raise ValueError("教学案例固定两个证券与五个session")
    dates = [row["session"] for row in calendar]
    if dates != sorted(set(dates)) or len({value[:7] for value in dates}) != 1:
        raise ValueError("教学案例五个session必须有序且处于同月")
    cutoff = aware(context.parameters["decision_time"])
    for row in calendar:
        times = [aware(value) for value in row["expected_bars"]]
        if (len(times) != 240 or times != sorted(set(times))
                or any(value.date().isoformat() != row["session"] for value in times)
                or times[-1] > cutoff):
            raise ValueError("日历必须包含240个已完成分钟")


def _checked(value, statuses):
    if not isinstance(value, dict) or set(value) != {"value", "status"} or value["status"] not in statuses:
        raise ValueError("公式返回value/status合同不符")
    number = value["value"]
    if (value["status"] == "computed") != (number is not None):
        raise ValueError("公式数值与状态不一致")
    if number is not None and (isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number)):
        raise ValueError("公式数值必须有限")
    return value


def run(context, inputs, output_root):
    preflight(context)
    if len(inputs) != 1 or inputs[0].port != "bars":
        raise ValueError("仅接收正式bars输入")
    bars = inputs[0]
    calendar = json.loads(context.parameters["calendar_json"])
    window_length = context.parameters.get("window_sessions", 3)
    if type(window_length) is not int or not 1 <= window_length <= 5:
        raise ValueError("窗口必须在1至5个session之间")
    entities = json.loads(context.parameters["entities_json"])
    positions = {row["session"]: {aware(value): index for index, value in enumerate(row["expected_bars"])} for row in calendar}
    groups = {(entity, row["session"]): {} for entity in entities for row in calendar}
    raw_rows = []
    for batch in bars.iter_batches(columns=("code", "dt", "volume"), batch_size=4096):
        for row in batch.to_pylist():
            when = aware(row["dt"])
            key = (row["code"], when.date().isoformat())
            if key not in groups or when not in positions[key[1]]:
                raise ValueError("分钟超出冻结范围")
            index = positions[key[1]][when]
            if index in groups[key]:
                raise ValueError("同一证券分钟重复")
            groups[key][index] = row["volume"]
            raw_rows.append({"entity": key[0], "session": key[1], "bar_index": index,
                             "dt": when.isoformat(), "volume": row["volume"]})
    bars.assert_complete()
    daily, coverage = [], []
    for entity in entities:
        history = []
        for day in calendar:
            key = (entity, day["session"])
            value = _checked(compute.daily_value(groups[key], 240), {"computed", "missing_bars", "invalid_volume", "zero_volume"})
            history.append({"session": key[1], **value})
            rolling = _checked(compute.rolling_value(history) if window_length == 3 else compute.rolling_value(history, window_length), {"computed", "warmup", "invalid_window"})
            daily.append({"entity": entity, "session": key[1], **value,
                          "rolling_value": rolling["value"], "rolling_status": rolling["status"],
                          "window_sessions": [row["session"] for row in history[-window_length:]],
                          "available_at": aware(day["expected_bars"][-1]).isoformat(), "window_length": window_length})
            coverage.append({"entity": entity, "session": key[1], "raw_rows": len(groups[key])})
    tables = {"bars": raw_rows, "calendar": calendar, "coverage": coverage, "daily": daily}
    return [output_root.write_batches(port=name, artifact_type=ARTIFACT, relative_path=name+"/data.parquet",
                                     schema=SCHEMAS[name], batches=pa.Table.from_pylist(rows, schema=SCHEMAS[name]).to_batches())
            for name, rows in tables.items()]
