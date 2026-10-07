"""从封存日线逐项手算固定因子集合；不调用 Qlib、pandas 或生产求值器。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import math
import re
from statistics import fmean, stdev


_EPSILON = 1e-12
_NAN = float("nan")

# 独立保存已验收定义，避免验证器与生产计算共享公式来源。
_VOLUME = {
    "volume_ratio5": ("$volume/(Mean($volume,5)+1e-12)", ("volume",), 4, ("Add", "Div", "Mean")),
    "volume_change1": ("$volume/(Ref($volume,1)+1e-12)-1", ("volume",), 1, ("Add", "Div", "Ref", "Sub")),
    "volume_dispersion5": ("Std($volume,5)/(Mean($volume,5)+1e-12)", ("volume",), 4, ("Add", "Div", "Mean", "Std")),
    "price_volume_correlation5": ("Corr($close,Log($volume+1),5)", ("close", "volume"), 4, ("Add", "Corr", "Log")),
    "volume_weighted_return5": ("Sum(($close/Ref($close,1)-1)*$volume,5)/(Sum($volume,5)+1e-12)", ("close", "volume"), 5, ("Add", "Div", "Mul", "Ref", "Sub", "Sum")),
    "up_volume_share5": ("Sum(Greater($close-Ref($close,1),0)*$volume,5)/(Sum(Abs($close-Ref($close,1))*$volume,5)+1e-12)", ("close", "volume"), 5, ("Abs", "Add", "Div", "Greater", "Mul", "Ref", "Sub", "Sum")),
    "down_volume_share5": ("Sum(Greater(Ref($close,1)-$close,0)*$volume,5)/(Sum(Abs($close-Ref($close,1))*$volume,5)+1e-12)", ("close", "volume"), 5, ("Abs", "Add", "Div", "Greater", "Mul", "Ref", "Sub", "Sum")),
    "volume_change_balance5": ("Sum($volume-Ref($volume,1),5)/(Sum(Abs($volume-Ref($volume,1)),5)+1e-12)", ("volume",), 5, ("Abs", "Add", "Div", "Ref", "Sub", "Sum")),
}
_KBAR = {
    "KMID": ("($close-$open)/$open", ("close", "open"), ("Div", "Sub")),
    "KLEN": ("($high-$low)/$open", ("high", "low", "open"), ("Div", "Sub")),
    "KMID2": ("($close-$open)/($high-$low+1e-12)", ("close", "high", "low", "open"), ("Add", "Div", "Sub")),
    "KUP": ("($high-Greater($open,$close))/$open", ("close", "high", "open"), ("Div", "Greater", "Sub")),
    "KUP2": ("($high-Greater($open,$close))/($high-$low+1e-12)", ("close", "high", "low", "open"), ("Add", "Div", "Greater", "Sub")),
    "KLOW": ("(Less($open,$close)-$low)/$open", ("close", "low", "open"), ("Div", "Less", "Sub")),
    "KLOW2": ("(Less($open,$close)-$low)/($high-$low+1e-12)", ("close", "high", "low", "open"), ("Add", "Div", "Less", "Sub")),
    "KSFT": ("(2*$close-$high-$low)/$open", ("close", "high", "low", "open"), ("Div", "Mul", "Sub")),
    "KSFT2": ("(2*$close-$high-$low)/($high-$low+1e-12)", ("close", "high", "low"), ("Add", "Div", "Mul", "Sub")),
}


def _definitions(name):
    if name == "volume_price_v1":
        return dict(_VOLUME), None, "full"
    if name == "alpha158_selected_v1":
        result = {key: (expression, fields, 0, operators) for key, (expression, fields, operators) in _KBAR.items()}
        for field in ("OPEN", "HIGH", "LOW", "VWAP"):
            result[field + "0"] = (f"${field.lower()}/$close", tuple(sorted((field.lower(), "close"))), 0, ("Div",))
        result.update({
            "ROC5": ("Ref($close,5)/$close", ("close",), 5, ("Div", "Ref")),
            "MA5": ("Mean($close,5)/$close", ("close",), 4, ("Div", "Mean")),
            "STD5": ("Std($close,5)/$close", ("close",), 4, ("Div", "Std")),
            "MAX5": ("Max($high,5)/$close", ("close", "high"), 4, ("Div", "Max")),
            "MIN5": ("Min($low,5)/$close", ("close", "low"), 4, ("Div", "Min")),
            "RSV5": ("($close-Min($low,5))/(Max($high,5)-Min($low,5)+1e-12)", ("close", "high", "low"), 4, ("Add", "Div", "Max", "Min", "Sub")),
            "CORR5": ("Corr($close,Log($volume+1),5)", ("close", "volume"), 4, ("Add", "Corr", "Log")),
            "VMA5": ("Mean($volume,5)/($volume+1e-12)", ("volume",), 4, ("Add", "Div", "Mean")),
            "VSTD5": ("Std($volume,5)/($volume+1e-12)", ("volume",), 4, ("Add", "Div", "Std")),
        })
        return result, "Alpha158", "qlib"
    if name == "alpha360_selected_v1":
        result = {}
        for field in ("CLOSE", "OPEN", "HIGH", "LOW", "VWAP", "VOLUME"):
            for lag in (0, 1, 5):
                numerator = f"Ref(${field.lower()},{lag})" if lag else "$" + field.lower()
                denominator = "($volume+1e-12)" if field == "VOLUME" else "$close"
                operators = tuple(sorted({"Div", *( ("Ref",) if lag else ()), *( ("Add",) if field == "VOLUME" else ())}))
                result[field + str(lag)] = (numerator + "/" + denominator, tuple(sorted({field.lower(), "volume" if field == "VOLUME" else "close"})), lag, operators)
        return result, "Alpha360", "qlib"
    raise ValueError(f"未支持独立复核的因子集合: {name}")


def validate_factor_suite(suite: Mapping) -> None:
    """逐项核对固定定义、历史跨度和字段，不依赖生产模块。"""
    if not isinstance(suite, Mapping):
        raise ValueError("因子声明必须是映射")
    definitions, baseline, _ = _definitions(suite.get("name"))
    fields = sorted({field for _, used, _, _ in definitions.values() for field in used})
    expected_keys = {"schema", "name", "baseline", "qlib_version", "min_periods", "fields", "lookback", "features"}
    if set(suite) != expected_keys or suite["schema"] != "qlib-factor-suite-v1" or suite["qlib_version"] != "0.9.7":
        raise ValueError("因子声明结构或Qlib版本不符")
    if suite["baseline"] != baseline or suite["min_periods"] not in {"full", "qlib"} or tuple(suite["fields"]) != tuple(fields):
        raise ValueError("因子基线、窗口政策或输入字段不符")
    if type(suite["lookback"]) is not int or suite["lookback"] != max(value[2] for value in definitions.values()):
        raise ValueError("因子总历史跨度不符")
    features = suite["features"]
    if not isinstance(features, Mapping) or set(features) != set(definitions):
        raise ValueError("因子集合必须完整覆盖选定特征")
    for name, (expression, used, lookback, operators) in definitions.items():
        spec = features[name]
        expected = {"formula", "fields", "lookback", "operators", "window_sessions"}
        if not isinstance(spec, Mapping) or set(spec) != expected:
            raise ValueError(f"特征声明结构不符: {name}")
        compact = re.sub(r"\s+", "", spec["formula"]) if isinstance(spec["formula"], str) else None
        if compact != expression or tuple(spec["fields"]) != used or tuple(spec["operators"]) != operators:
            raise ValueError(f"特征原生定义或依赖不符: {name}")
        if type(spec["lookback"]) is not int or spec["lookback"] != lookback or type(spec["window_sessions"]) is not int or spec["window_sessions"] != lookback + 1:
            raise ValueError(f"特征历史窗口不符: {name}")


def _number(value):
    if value is None:
        return _NAN
    number = float(value)
    if math.isinf(number):
        raise ValueError("日线输入不允许无穷值")
    return number


def _rolling(values, full, *, operation="mean"):
    values = values[-5:]
    valid = [value for value in values if math.isfinite(value)]
    if (full and (len(values) != 5 or len(valid) != 5)) or not valid:
        return _NAN
    if operation == "std":
        return stdev(valid) if len(valid) >= 2 else _NAN
    return {"mean": fmean, "sum": math.fsum, "min": min, "max": max}[operation](valid)


def _correlation(left, right, full):
    left, right = left[-5:], right[-5:]
    if full and (len(left) != 5 or not all(math.isfinite(value) for value in [*left, *right])):
        return _NAN
    left_valid, right_valid = ([value for value in values if math.isfinite(value)] for values in (left, right))
    # Qlib 对两列各自的样本标准差使用2e-5平坦阈值。
    if len(left_valid) < 2 or len(right_valid) < 2 or stdev(left_valid) <= 2e-5 or stdev(right_valid) <= 2e-5:
        return _NAN
    pairs = [(a, b) for a, b in zip(left, right) if math.isfinite(a) and math.isfinite(b)]
    if len(pairs) < 2:
        return _NAN
    a, b = zip(*pairs)
    am, bm = fmean(a), fmean(b)
    covariance = math.fsum((x - am) * (y - bm) for x, y in pairs)
    scale = math.sqrt(math.fsum((x - am) ** 2 for x in a) * math.fsum((y - bm) ** 2 for y in b))
    return covariance / scale if scale else _NAN


def independent_factor_values(suite: Mapping, history: Sequence[Mapping]) -> dict[str, float | None]:
    """输入按声明日历排列的截至观察日行情，缺失会话保留空字段。"""
    validate_factor_suite(suite)
    if not history:
        return {name: None for name in suite["features"]}
    values = {field: [_number(row.get(field)) for row in history] for field in suite["fields"]}
    for field, series in values.items():
        if any(math.isfinite(value) and (value < 0 if field == "volume" else value <= 0) for value in series):
            raise ValueError("成交量必须非负，价格必须为正；缺失值应保持缺失")
    current = {field: series[-1] for field, series in values.items()}
    full = suite["min_periods"] == "full"
    roll = lambda field, operation="mean": _rolling(values[field], full, operation=operation)
    result = {}
    for name, spec in suite["features"].items():
        value = _NAN
        if full and len(history) <= spec["lookback"]:
            result[name] = None
            continue
        if suite["name"] == "volume_price_v1":
            volume = values["volume"]
            denominator = _rolling(volume, full, operation="sum") + _EPSILON
            if name == "volume_ratio5":
                value = current["volume"] / (roll("volume") + _EPSILON)
            elif name == "volume_change1":
                value = volume[-1] / (volume[-2] + _EPSILON) - 1 if len(volume) >= 2 else _NAN
            elif name == "volume_dispersion5":
                value = roll("volume", "std") / (roll("volume") + _EPSILON)
            elif name == "price_volume_correlation5":
                value = _correlation(values["close"], [math.log(item + 1) for item in volume], full)
            elif name == "volume_change_balance5":
                changes = [_NAN, *(b - a for a, b in zip(volume, volume[1:]))]
                value = _rolling(changes, full, operation="sum") / (_rolling([abs(item) for item in changes], full, operation="sum") + _EPSILON)
            else:
                close = values["close"]
                returns = [_NAN, *(b / a - 1 for a, b in zip(close, close[1:]))]
                changes = [_NAN, *(b - a for a, b in zip(close, close[1:]))]
                if name == "volume_weighted_return5":
                    value = _rolling([a * b for a, b in zip(returns, volume)], full, operation="sum") / denominator
                else:
                    direction = 1 if name == "up_volume_share5" else -1
                    positive = [max(direction * item, 0) if math.isfinite(item) else _NAN for item in changes]
                    numerator = _rolling([a * b for a, b in zip(positive, volume)], full, operation="sum")
                    total = _rolling([abs(a) * b for a, b in zip(changes, volume)], full, operation="sum")
                    value = numerator / (total + _EPSILON)
        elif name in _KBAR:
            o, h, lo, c = (current.get(field, _NAN) for field in ("open", "high", "low", "close"))
            numerator = {"KMID": c - o, "KLEN": h - lo, "KMID2": c - o,
                         "KUP": h - max(o, c) if math.isfinite(o) and math.isfinite(c) else _NAN,
                         "KUP2": h - max(o, c) if math.isfinite(o) and math.isfinite(c) else _NAN,
                         "KLOW": min(o, c) - lo if math.isfinite(o) and math.isfinite(c) else _NAN,
                         "KLOW2": min(o, c) - lo if math.isfinite(o) and math.isfinite(c) else _NAN,
                         "KSFT": 2 * c - h - lo, "KSFT2": 2 * c - h - lo}[name]
            value = numerator / (h - lo + _EPSILON if name.endswith("2") else o)
        elif suite["name"] == "alpha360_selected_v1" or name in {"OPEN0", "HIGH0", "LOW0", "VWAP0"}:
            field, lag = re.fullmatch(r"([A-Z]+)([0-9]+)", name).groups()
            field, lag = field.lower(), int(lag)
            value = values[field][-lag - 1] if len(history) > lag else _NAN
            value /= current["volume"] + _EPSILON if field == "volume" else current["close"]
        elif name == "ROC5":
            value = values["close"][-6] / current["close"] if len(history) >= 6 else _NAN
        elif name in {"MA5", "STD5", "MAX5", "MIN5"}:
            field, operation = {"MA5": ("close", "mean"), "STD5": ("close", "std"), "MAX5": ("high", "max"), "MIN5": ("low", "min")}[name]
            value = roll(field, operation) / current["close"]
        elif name == "RSV5":
            low, high = roll("low", "min"), roll("high", "max")
            value = (current["close"] - low) / (high - low + _EPSILON)
        elif name == "CORR5":
            value = _correlation(values["close"], [math.log(item + 1) for item in values["volume"]], full)
        elif name in {"VMA5", "VSTD5"}:
            value = roll("volume", "std" if name == "VSTD5" else "mean") / (current["volume"] + _EPSILON)
        if math.isinf(value):
            raise ValueError(f"独立手算产生无穷值: {name}")
        result[name] = value if math.isfinite(value) else None
    return result
