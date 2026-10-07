"""固定的量价与 Alpha 子集；原生数值定义来自 Qlib0.9.7。"""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
import json
from typing import Mapping, Sequence

import pandas as pd

if __package__:
    from .expressions import QlibExpressionError, baseline_expressions, evaluate_expressions, validate_expression
else:
    from expressions import QlibExpressionError, baseline_expressions, evaluate_expressions, validate_expression


VOLUME_PRICE_EXPRESSIONS = {
    "volume_ratio5": "$volume/(Mean($volume,5)+1e-12)",
    "volume_change1": "$volume/(Ref($volume,1)+1e-12)-1",
    "volume_dispersion5": "Std($volume,5)/(Mean($volume,5)+1e-12)",
    "price_volume_correlation5": "Corr($close,Log($volume+1),5)",
    "volume_weighted_return5": "Sum(($close/Ref($close,1)-1)*$volume,5)/(Sum($volume,5)+1e-12)",
    "up_volume_share5": "Sum(Greater($close-Ref($close,1),0)*$volume,5)/(Sum(Abs($close-Ref($close,1))*$volume,5)+1e-12)",
    "down_volume_share5": "Sum(Greater(Ref($close,1)-$close,0)*$volume,5)/(Sum(Abs($close-Ref($close,1))*$volume,5)+1e-12)",
    "volume_change_balance5": "Sum($volume-Ref($volume,1),5)/(Sum(Abs($volume-Ref($volume,1)),5)+1e-12)",
}

ALPHA158_SELECTED = (
    "KMID", "KLEN", "KMID2", "KUP", "KUP2", "KLOW", "KLOW2", "KSFT", "KSFT2",
    "OPEN0", "HIGH0", "LOW0", "VWAP0", "ROC5", "MA5", "STD5", "MAX5", "MIN5",
    "RSV5", "CORR5", "VMA5", "VSTD5",
)
ALPHA360_SELECTED = tuple(f"{field}{lag}" for field in ("CLOSE", "OPEN", "HIGH", "LOW", "VWAP", "VOLUME")
                          for lag in (0, 1, 5))
_FIELDS = ("open", "high", "low", "close", "vwap", "volume")
_PROFILES = {
    "volume_price_v1": (None, None, "full"),
    "alpha158_selected_v1": ("Alpha158", ALPHA158_SELECTED, "qlib"),
    "alpha360_selected_v1": ("Alpha360", ALPHA360_SELECTED, "qlib"),
}


def factor_suite(name: str, *, min_periods: str | None = None) -> dict:
    """生成可封存声明；window_sessions 是包含观察日的输入跨度。"""
    if name not in _PROFILES:
        raise QlibExpressionError(f"未支持的正式因子集合: {name}")
    baseline, selected, default_policy = _PROFILES[name]
    policy = default_policy if min_periods is None else min_periods
    if policy not in {"full", "qlib"}:
        raise QlibExpressionError("min_periods 必须选择 full 或 qlib")
    expressions = (dict(VOLUME_PRICE_EXPRESSIONS) if baseline is None else
                   baseline_expressions(baseline, selected=selected, fields=_FIELDS, max_window=5))
    features = {}
    for feature_id, expression in expressions.items():
        spec = validate_expression(expression, _FIELDS, max_window=5)
        definition = spec.to_dict()
        definition["formula"] = definition.pop("expression")
        features[feature_id] = {**definition, "window_sessions": spec.lookback + 1}
    return {"schema": "qlib-factor-suite-v1", "name": name, "baseline": baseline,
            "qlib_version": "0.9.7", "min_periods": policy,
            "fields": sorted({field for spec in features.values() for field in spec["fields"]}),
            "lookback": max(spec["lookback"] for spec in features.values()), "features": features}


def evaluate_factor_suite(frame: pd.DataFrame, suite: Mapping, *, sessions: Sequence,
                          output_start=None, output_end=None) -> pd.DataFrame:
    """在已准入日线求值；调用方绑定字段含义、可见时点和证券池。"""
    if __package__:
        from .factor_baseline_oracle import validate_factor_suite
    else:
        from factor_baseline_oracle import validate_factor_suite

    validate_factor_suite(suite)
    try:
        installed_version = version("pyqlib")
    except PackageNotFoundError as exc:
        raise QlibExpressionError("因子数值计算需要安装 quantwitness[ml]") from exc
    if installed_version != suite["qlib_version"]:
        raise QlibExpressionError("固定因子集合要求 Qlib0.9.7；其他版本需重新验收")
    for field in suite["fields"]:
        if field in frame.columns:
            values = frame[field].dropna()
            invalid = values.lt(0) if field == "volume" else values.le(0)
            if invalid.any():
                raise QlibExpressionError("成交量必须非负，价格必须为正；缺失值应保持缺失")
    result = evaluate_expressions(frame, {name: spec["formula"] for name, spec in suite["features"].items()},
                                  fields=suite["fields"], max_window=5, min_periods=suite["min_periods"],
                                  sessions=sessions, output_start=output_start, output_end=output_end)
    result.attrs["factor_suite"] = json.loads(json.dumps(dict(suite), default=dict))
    return result
