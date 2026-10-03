"""教学公式：完整日内成交量集中度与最近三日均值。"""
import math


def daily_value(volumes, expected_count):
    """缺少分钟不补零；非法量与真实零成交量分别记录。"""
    if set(volumes) != set(range(expected_count)):
        return {"value": None, "status": "missing_bars"}
    values = [volumes[index] for index in range(expected_count)]
    if any(value is None or not math.isfinite(value) or value < 0 for value in values):
        return {"value": None, "status": "invalid_volume"}
    total = math.fsum(values)
    if total == 0:
        return {"value": None, "status": "zero_volume"}
    return {"value": math.fsum(value * value for value in values) / (total * total), "status": "computed"}


def rolling_value(history, window_sessions=3):
    """窗口保留最近三个日历位置，不跳过缺失日。"""
    window = history[-window_sessions:]
    if len(window) != window_sessions:
        return {"value": None, "status": "warmup"}
    if any(row["status"] != "computed" for row in window):
        return {"value": None, "status": "invalid_window"}
    return {"value": math.fsum(row["value"] for row in window) / window_sessions, "status": "computed"}
