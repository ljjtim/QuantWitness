"""从正式LEAN日终账户和逐笔订单事件生成项目范围的金融指标。"""
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
from statistics import fmean, pstdev

import pyarrow as pa

from research_pipeline.platform.metric_contracts import MetricDefinition, MetricMeasurementSemantics
from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE, LEAN_METRICS_SCHEMA_ID


ANNUAL_SESSIONS = 252
METRIC_SPECS = {
    "initial_equity": ("CNY", "neutral", "initial_cash_units_div_100"),
    "final_equity": ("CNY", "neutral", "last_total_equity_including_action_entitlements"),
    "total_return": ("ratio", "higher_is_better", "last_equity_div_initial_equity_minus_one"),
    "annualized_return": ("ratio", "higher_is_better", "geometric_return_252_div_trading_sessions_minus_one_if_zero_terminal"),
    "max_drawdown": ("ratio", "lower_is_better", "max_one_minus_equity_div_running_peak_including_initial"),
    "annualized_volatility": ("ratio", "lower_is_better", "population_std_daily_returns_times_sqrt_252"),
    "sharpe": ("ratio", "higher_is_better", "mean_daily_return_div_population_std_times_sqrt_252_zero_if_flat"),
    "turnover_mean": ("ratio", "neutral", "mean_daily_half_gross_fill_notional_div_opening_equity"),
    "fee_total": ("CNY", "lower_is_better", "sum_fill_event_fees"),
    "fill_count": ("events", "neutral", "count_nonzero_filled_and_partially_filled_events"),
    "filled_order_count": ("orders", "neutral", "count_distinct_orders_with_fills"),
    "order_count": ("orders", "neutral", "count_distinct_native_order_ids"),
    "gross_traded_notional": ("CNY", "neutral", "sum_absolute_fill_quantity_times_execution_price"),
    "capital_utilization_mean": ("ratio", "neutral", "mean_daily_equity_minus_cash_div_equity"),
}
METRIC_COLUMNS = {
    "metric_ref": "string", "value": "float64", "unit": "string", "sample_start": "string",
    "sample_end": "string", "sample_size": "int64", "status": "string", "definition_json": "string",
}


def lean_metric_definitions(*, equity_semantics="session_close_mark"):
    """只供LEAN项目计划组合，不注册为公共研究指标。"""
    digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if equity_semantics not in {"session_close_mark", "latest_visible_mark_not_session_close"}:
        raise ValueError("LEAN权益估值口径不受支持")
    latest_visible = equity_semantics == "latest_visible_mark_not_session_close"
    denominators = {
        "total_return": "initial_equity", "annualized_return": "initial_equity_and_trading_session_count",
        "max_drawdown": "running_equity_peak_including_initial_equity",
        "sharpe": "population_standard_deviation_of_daily_returns",
        "turnover_mean": "two_times_daily_opening_equity", "capital_utilization_mean": "daily_total_equity",
    }
    return tuple(MetricDefinition.build(
        metric_id="project.lean." + name, version="1.0.0", input_artifact_type=LEAN_ARTIFACT_TYPE,
        result_schema_id=LEAN_METRICS_SCHEMA_ID, output_schema=METRIC_COLUMNS, unit=unit,
        frequency="daily_latest_visible_mark" if latest_visible else "daily_close", annualization_policy=("252_trading_sessions" if name in {
            "annualized_return", "annualized_volatility", "sharpe"} else "none"),
        risk_free_rate_policy="zero_annual_rate" if name == "sharpe" else "not_applicable",
        null_policy="forbid", direction=direction,
        implementation_ref="research_pipeline.research.lean_metrics:lean_metric_rows",
        implementation_digest=digest,
        measurement_semantics=MetricMeasurementSemantics(
            quantity=name, numerator=aggregation, denominator=denominators.get(name, "not_applicable"),
            observation_timing=("session_end_latest_visible_mark_including_first_day_return_zero_after_zero_without_external_flow"
                                if latest_visible else "completed_session_close_including_first_day_return_zero_after_zero_without_external_flow"),
            aggregation=aggregation),
    ) for name, (unit, direction, aggregation) in METRIC_SPECS.items())


def lean_metric_definitions_for_nodes(nodes):
    """准入计划按执行频率冻结与正式账户一致的估值口径。"""
    frequencies = {node.parameters.get("frequency", "1m") for node in nodes
                   if node.parameters.get("execution_backend") == "lean"}
    if len(frequencies) != 1:
        raise ValueError("同一组LEAN指标必须使用单一执行频率")
    semantics = ("latest_visible_mark_not_session_close" if frequencies == {"1d"}
                 else "session_close_mark")
    return lean_metric_definitions(equity_semantics=semantics)


def _number(value, label):
    try:
        number = Decimal(str(value))
    except (ValueError, ArithmeticError) as exc:
        raise ValueError(f"LEAN指标数值无效：{label}") from exc
    if not number.is_finite():
        raise ValueError(f"LEAN指标数值必须有限：{label}")
    return number


def lean_metric_rows(daily, orders, *, initial_equity, expected_dates=None):
    """含首日收益，年化按252交易日；回撤为包含期初资金的非负损失比例。"""
    initial = _number(initial_equity, "initial_equity")
    if initial <= 0:
        raise ValueError("LEAN指标要求正期初权益")
    days = list(daily)
    dates = [str(row["date"]) for row in days]
    if not dates or dates != sorted(set(dates)):
        raise ValueError("LEAN指标日终日期必须非空、唯一且升序")
    for day in dates:
        date.fromisoformat(day)
    if expected_dates is not None and dates != list(expected_dates):
        raise ValueError("LEAN指标日终日期与冻结交易日不一致")
    equity_semantics = {row.get("equity_semantics", "session_close_mark") for row in days}
    if len(equity_semantics) != 1:
        raise ValueError("LEAN日终序列混用权益估值口径")
    definitions = lean_metric_definitions(equity_semantics=equity_semantics.pop())
    equity = [_number(row["equity"], "equity") for row in days]
    cash = [_number(row["cash"], "cash") for row in days]
    if any(value < 0 for value in equity):
        raise ValueError("LEAN指标不支持负日终权益")
    if any(value == 0 and balance != 0 for value, balance in zip(equity, cash)):
        raise ValueError("LEAN零权益日的现金必须为零")
    opening = [initial, *equity[:-1]]
    if any(previous == 0 and close != 0 for close, previous in zip(equity, opening)):
        raise ValueError("LEAN零权益后恢复正值需要未支持的外部注资口径")
    returns = [0.0 if previous == 0 else float(close / previous - 1) for close, previous in zip(equity, opening)]
    peak, drawdown = initial, Decimal(0)
    for value in equity:
        peak = max(peak, value)
        drawdown = max(drawdown, 1 - value / peak)
    volatility = pstdev(returns)
    gross_by_day = dict.fromkeys(dates, Decimal(0))
    fees, fills, order_ids, filled_ids = Decimal(0), 0, set(), set()
    for event in orders:
        identifier = event["order_id"]
        order_ids.add(identifier)
        quantity = _number(event["quantity"], "fill quantity")
        if quantity == 0:
            continue
        if event["status"] not in {"Filled", "PartiallyFilled"}:
            raise ValueError("LEAN非成交订单事件带有成交数量")
        at = datetime.fromisoformat(str(event["at"]).replace("Z", "+00:00"))
        if at.tzinfo is None:
            raise ValueError("LEAN成交时间必须带时区")
        day = at.astimezone(timezone(timedelta(hours=8))).date().isoformat()
        if day not in gross_by_day:
            raise ValueError("LEAN成交日期超出正式日终样本")
        price, fee = _number(event["price"], "fill price"), _number(event["fee"], "fee")
        if price <= 0 or fee < 0:
            raise ValueError("LEAN成交价格或费用无效")
        gross_by_day[day] += abs(quantity) * price
        fees += fee
        fills += 1
        filled_ids.add(identifier)
    if any(previous == 0 and gross_by_day[day] != 0 for day, previous in zip(dates, opening)):
        raise ValueError("LEAN零期初权益日不能存在成交额")
    values = {
        "initial_equity": float(initial), "final_equity": float(equity[-1]),
        "total_return": float(equity[-1] / initial - 1),
        "annualized_return": -1.0 if equity[-1] == 0 else math.expm1(math.log(float(equity[-1] / initial)) * ANNUAL_SESSIONS / len(days)),
        "max_drawdown": float(drawdown), "annualized_volatility": volatility * math.sqrt(ANNUAL_SESSIONS),
        "sharpe": 0.0 if volatility == 0 else fmean(returns) / volatility * math.sqrt(ANNUAL_SESSIONS),
        "turnover_mean": fmean(0.0 if previous == 0 else float(gross_by_day[day] / (2 * previous)) for day, previous in zip(dates, opening)),
        "fee_total": float(fees), "fill_count": float(fills), "filled_order_count": float(len(filled_ids)),
        "order_count": float(len(order_ids)), "gross_traded_notional": float(sum(gross_by_day.values())),
        "capital_utilization_mean": fmean(0.0 if value == 0 else float((value - balance) / value) for value, balance in zip(equity, cash)),
    }
    if any(not math.isfinite(value) for value in values.values()):
        raise ValueError("LEAN金融指标超出有限数值范围")
    return [{"metric_ref": definition.metric_ref, "value": values[definition.metric_id.removeprefix("project.lean.")],
             "unit": definition.unit, "sample_start": dates[0], "sample_end": dates[-1], "sample_size": len(days),
             "status": "computed", "definition_json": json.dumps(
                 {**definition.payload(), "definition_digest": definition.definition_digest}, ensure_ascii=False, sort_keys=True)}
            for definition in definitions]


def lean_metrics_table(daily, orders, *, initial_equity, expected_dates=None):
    schema = pa.schema([(key, pa.int64() if kind == "int64" else pa.float64() if kind == "float64" else pa.string())
                        for key, kind in METRIC_COLUMNS.items()])
    return pa.Table.from_pylist(lean_metric_rows(daily, orders, initial_equity=initial_equity,
                                              expected_dates=expected_dates), schema=schema)
