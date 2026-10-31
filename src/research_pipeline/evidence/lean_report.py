"""从正式Result生成自包含LEAN收益和成交报告。"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
import html
import json
import math
from typing import Mapping

from research_pipeline.platform.lean_contract import LEAN_METRICS_SCHEMA_ID, LEAN_SCHEMA_IDS


LEAN_REPORT_VERSION = "lean-research-report-v1"
REPORT_SCHEMAS = {"daily": LEAN_SCHEMA_IDS["daily"], "orders": LEAN_SCHEMA_IDS["orders"],
                  "metrics": LEAN_METRICS_SCHEMA_ID}
LABELS = {
    "initial_equity": "期初权益", "final_equity": "期末权益", "total_return": "累计收益率",
    "annualized_return": "年化收益率", "max_drawdown": "最大回撤", "annualized_volatility": "年化波动率",
    "sharpe": "夏普比率", "turnover_mean": "日均换手率", "fee_total": "累计费用",
    "fill_count": "成交事件数", "filled_order_count": "有成交订单数", "order_count": "订单数",
    "gross_traded_notional": "双边累计成交额", "capital_utilization_mean": "日均非现金权益占比",
}


def validate_lean_report_request(value):
    if (not isinstance(value, Mapping) or set(value) != {"contract_version", "result_id", "tables", "budget"}
            or value["contract_version"] != LEAN_REPORT_VERSION):
        raise ValueError("LEAN报告请求须声明版本、result_id、三表和预算")
    tables, budget = value["tables"], value["budget"]
    if (not isinstance(value["result_id"], str) or not value["result_id"]
            or not isinstance(tables, Mapping) or set(tables) != set(REPORT_SCHEMAS)
            or any(not isinstance(v, str) or not v for v in tables.values())
            or len(set(tables.values())) != len(tables)):
        raise ValueError("LEAN报告必须唯一绑定daily/orders/metrics三张正式表")
    if (not isinstance(budget, Mapping) or set(budget) != {"max_rows", "memory_bytes"}
            or any(type(v) is not int or v <= 0 for v in budget.values())):
        raise ValueError("LEAN报告预算必须为正整数max_rows/memory_bytes")
    return {**value, "tables": dict(tables), "budget": dict(budget)}


def read_lean_report_data(context, request):
    request = validate_lean_report_request(request)
    snapshot = context.snapshot
    if snapshot.bundle.result_id != request["result_id"]:
        raise ValueError("LEAN报告请求与所读 Result 不一致")
    selected, artifact_keys = {}, set()
    total_rows, total_bytes = 0, 0
    for name, schema in REPORT_SCHEMAS.items():
        manifests = [t for t in snapshot.bundle.tables if t.table_id == request["tables"][name]]
        if len(manifests) != 1 or manifests[0].schema_id != schema:
            raise ValueError(f"LEAN报告正式表schema不一致：{name}")
        artifact_keys.add(manifests[0].artifact_key)
        rows = []
        for batch in snapshot.iter_table_batches(schema, batch_size=4096):
            total_rows += batch.num_rows
            total_bytes += batch.nbytes
            if total_rows > request["budget"]["max_rows"] or total_bytes > request["budget"]["memory_bytes"]:
                raise ValueError("LEAN报告读取超出声明预算")
            records = batch.to_pylist()
            if name != "metrics":
                records = [json.loads(row["payload_json"]) for row in records]
            rows.extend(records)
        selected[name] = rows
    if len(artifact_keys) != 1:
        raise ValueError("LEAN报告三表必须来自同一正式simulation工件")
    metric_rows = {row["metric_ref"]: row for row in selected["metrics"]}
    if len(metric_rows) != len(selected["metrics"]):
        raise ValueError("LEAN报告正式指标重复")
    expected = {"project.lean." + name + "@1.0.0" for name in LABELS}
    verified = {m.metric_ref: m for m in context.metrics if m.table_id == request["tables"]["metrics"]}
    if set(metric_rows) != expected or set(verified) != expected:
        raise ValueError("LEAN报告必须完整绑定正式金融指标")
    for ref, row in metric_rows.items():
        metric = verified[ref]
        if any(row[k] != getattr(metric, k) for k in (
            "value", "unit", "sample_start", "sample_end", "sample_size", "status")):
            raise ValueError("LEAN报告指标与正式证明读取结果不一致")
    days = selected["daily"]
    sessions = [row["date"] for row in days]
    if not sessions or sessions != sorted(set(sessions)):
        raise ValueError("LEAN报告日终序列必须非空、唯一且升序")
    for session in sessions:
        date.fromisoformat(session)
    if any(m.sample_start != sessions[0] or m.sample_end != sessions[-1] or m.sample_size != len(days)
           for m in verified.values()):
        raise ValueError("LEAN报告指标样本与正式日终序列不一致")
    if any(not math.isfinite(float(row["equity"])) or float(row["equity"]) < 0 for row in days):
        raise ValueError("LEAN报告日终权益必须为非负有限数")
    return selected, verified


def render_lean_report(context, request, *, result_summary):
    rows, metrics = read_lean_report_data(context, request)
    import plotly.graph_objects as go
    from plotly.io import to_html

    initial = metrics["project.lean.initial_equity@1.0.0"].value
    days = [row["date"] for row in rows["daily"]]
    equity = [float(row["equity"]) for row in rows["daily"]]
    latest_visible = any(row.get("equity_semantics") == "latest_visible_mark_not_session_close" for row in rows["daily"])
    valuation_text = ("日线权益使用最新已可见价格估值；当日开盘价可用时使用开盘价，否则沿用已可见历史价格。价格来源与时间保存在每日账户，图中净值不是当日收盘净值。"
                      if latest_visible else "权益使用当日已完成行情估值。")
    peak, drawdowns = initial, []
    for value in equity:
        peak = max(peak, value)
        drawdowns.append((1 - value / peak) * 100)
    net = go.Figure(go.Scatter(x=["期初", *days], y=[1.0, *(v / initial for v in equity)], name="净值"))
    net.update_layout(title=("账户净值（最新可见价格，包含公司行动权益）" if latest_visible else "账户净值（包含公司行动权益）"), xaxis_title="交易日", yaxis_title="期初=1")
    dd = go.Figure(go.Scatter(x=days, y=drawdowns, fill="tozeroy", name="回撤"))
    dd.update_layout(title="日终回撤", xaxis_title="交易日", yaxis_title="回撤（%）")
    gross, fees = defaultdict(float), defaultdict(float)
    from datetime import datetime, timezone, timedelta
    for event in rows["orders"]:
        if float(event["quantity"]) == 0:
            continue
        at = datetime.fromisoformat(event["at"].replace("Z", "+00:00"))
        session = at.astimezone(timezone(timedelta(hours=8))).date().isoformat()
        gross[session] += abs(float(event["quantity"])) * float(event["price"])
        fees[session] += float(event["fee"])
    trades = go.Figure(go.Bar(x=days, y=[gross[d] for d in days], name="双边成交额"))
    trades.add_trace(go.Scatter(x=days, y=[fees[d] for d in days], name="成交费用", yaxis="y2"))
    trades.update_layout(title="逐日成交与费用", yaxis_title="成交额（元）",
                         yaxis2={"title": "费用（元）", "overlaying": "y", "side": "right"})
    for figure in (net, dd, trades):
        figure.update_layout(font={"family": "Microsoft YaHei", "size": 14})
    values = []
    for name in LABELS:
        metric = metrics["project.lean." + name + "@1.0.0"]
        unit = metric.unit
        display = f"{metric.value:.4%}" if unit == "ratio" and name != "sharpe" else f"{metric.value:,.6g}"
        values.append(f"<tr><th>{html.escape(LABELS[name])}</th><td>{display}</td><td>{html.escape(unit)}</td></tr>")
    figures = "".join(to_html(figure, full_html=False, include_plotlyjs=(index == 0))
                      for index, figure in enumerate((net, dd, trades)))
    return ('<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>LEAN研究报告</title>'
            '<style>body{font-family:"Microsoft YaHei",sans-serif;max-width:1100px;margin:32px auto;padding:0 20px}'
            'table{border-collapse:collapse;width:100%}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:left}'
            'pre{white-space:pre-wrap}</style></head><body><h1>LEAN研究报告</h1>'
            f'<pre>{html.escape(result_summary)}</pre><h2>正式金融指标</h2>'
            '<table><thead><tr><th>指标</th><th>值</th><th>单位</th></tr></thead><tbody>'
            + "".join(values) + '</tbody></table>' + f'<p>{valuation_text}</p>' + figures
            + '<p>收益包含首日；年化按252交易日；夏普无风险利率为0，平坦收益记0；'
            '换手为双边成交额的一半除以当日期初权益。日终权益包含公司行动权益，费用按实际成交事件累计。</p>'
            '<p>权益归零后，无外部注资的后续零权益交易日继续计入样本，日收益、换手和非现金权益占比记0。</p>'
            '</body></html>')
