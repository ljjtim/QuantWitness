"""从正式Result生成自包含LEAN收益和成交报告。"""
from __future__ import annotations

from datetime import date
import html
import json
import math
from typing import Mapping

from research_pipeline.platform.lean_contract import LEAN_METRICS_SCHEMA_ID, LEAN_SCHEMA_IDS


LEAN_REPORT_VERSION = "lean-research-report-v2"
REPORT_SCHEMAS = {**LEAN_SCHEMA_IDS, "metrics": LEAN_METRICS_SCHEMA_ID}
LABELS = {
    "initial_equity": "期初权益", "final_equity": "期末权益", "total_return": "累计收益率",
    "annualized_return": "年化收益率", "max_drawdown": "最大回撤", "annualized_volatility": "年化波动率",
    "sharpe": "夏普比率", "turnover_mean": "日均换手率", "fee_total": "累计费用",
    "fill_count": "成交事件数", "filled_order_count": "有成交订单数", "order_count": "订单数",
    "gross_traded_notional": "双边累计成交额", "capital_utilization_mean": "日均非现金权益占比",
    "margin_utilization_mean": "日均保证金使用率",
}


def validate_lean_report_request(value):
    if (not isinstance(value, Mapping) or set(value) != {"contract_version", "result_id", "tables", "budget"}
            or value["contract_version"] != LEAN_REPORT_VERSION):
        raise ValueError("LEAN报告请求须声明版本、result_id、八张执行表、指标表和预算")
    tables, budget = value["tables"], value["budget"]
    if (not isinstance(value["result_id"], str) or not value["result_id"]
            or not isinstance(tables, Mapping) or set(tables) != set(REPORT_SCHEMAS)
            or any(not isinstance(v, str) or not v for v in tables.values())
            or len(set(tables.values())) != len(tables)):
        raise ValueError("LEAN报告必须唯一绑定八张执行表和正式指标表")
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
        if name in {"intents", "trace"}:
            continue
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
        raise ValueError("LEAN报告执行表和指标必须来自同一正式simulation工件")
    metric_rows = {row["metric_ref"]: row for row in selected["metrics"]}
    if len(metric_rows) != len(selected["metrics"]):
        raise ValueError("LEAN报告正式指标重复")
    from research_pipeline.platform.lean_result_projection import LEAN_METRIC_VERSION, lean_research_daily
    final_account = None
    if selected["daily"] and (selected["daily"][0].get("asset_class") == "cn_future" or "margin" in selected["daily"][0]
                              or any(row.get("reason") == "terminal" for row in selected["account_events"])):
        content = snapshot.support_bytes.get("lean/account.json")
        if content is None:
            matches = [item for item in getattr(snapshot.bundle, "support_files", ())
                       if item.source_path == "lean/account.json" and item.artifact_key in artifact_keys]
            if len(matches) == 1:
                path = snapshot.directory / matches[0].relative_path
                if total_bytes + path.stat().st_size > request["budget"]["memory_bytes"]:
                    raise ValueError("LEAN报告读取超出声明预算")
                content = path.read_bytes()
        if content is not None:
            total_bytes += len(content)
            if total_bytes > request["budget"]["memory_bytes"]:
                raise ValueError("LEAN报告读取超出声明预算")
            final_account = json.loads(content)
    selected["research_daily"] = lean_research_daily(
        selected["daily"], account_events=selected["account_events"], final_account=final_account)
    futures = bool(selected["research_daily"] and (selected["research_daily"][0].get("asset_class") == "cn_future"
                    or "margin" in selected["research_daily"][0]))
    names = set(LABELS) - (set() if futures else {"margin_utilization_mean"})
    expected = {"project.lean." + name + "@" + LEAN_METRIC_VERSION for name in names}
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
    if any(not math.isfinite(float(row.get("research_equity", row["equity"])))
           or float(row.get("research_equity", row["equity"])) < 0 for row in selected["research_daily"]):
        raise ValueError("LEAN报告日终权益必须为非负有限数")
    terminal = next(metric for ref, metric in verified.items() if ref.startswith("project.lean.final_equity@"))
    research_final = float(selected["research_daily"][-1].get("research_equity", days[-1]["equity"]))
    if terminal.value != research_final:
        raise ValueError("LEAN报告研究权益序列与正式期末权益不一致")
    return selected, verified


def render_lean_report(context, request, *, result_summary):
    rows, metrics = read_lean_report_data(context, request)
    import plotly.graph_objects as go
    from plotly.io import to_html

    from research_pipeline.platform.lean_result_projection import LEAN_METRIC_VERSION, lean_trade_totals
    initial = metrics["project.lean.initial_equity@" + LEAN_METRIC_VERSION].value
    daily = rows["research_daily"]
    days = [row["date"] for row in daily]
    futures = daily[0].get("asset_class") == "cn_future" or "margin" in daily[0]
    equity = [float(row.get("research_equity", row["equity"])) for row in daily]
    latest_visible = any(row.get("equity_semantics") == "latest_visible_mark_not_session_close" for row in rows["daily"])
    valuation_text = ("日线权益使用最新已可见价格估值；当日开盘价可用时使用开盘价，否则沿用已可见历史价格。价格来源与时间保存在每日账户，图中净值不是当日收盘净值。"
                      if latest_visible else "权益使用当日已完成行情估值。")
    if any(row.get("equity_semantics") == "execution_window_mark" for row in rows["daily"]):
        valuation_text = ("权益在实际执行窗口结束时按最新可见行情记录，盘中终态不是收盘净值。"
                          "实际估值时刻保存在valuation_at。年化及日频风险指标按252个交易日期换算，"
                          "包含不完整首尾日期，不宜与完整交易日样本直接比较。")
    if futures:
        valuation_text = ("研究收益按交易日权益E计算。末交易日包含仅结算观察尾段的最终权益；"
                          "实际估值时间单独保存，不回填历史现金、可用资金或策略可见状态。"
                          "持仓按long/short与today/yesterday四桶保存，双向毛仓分别占用保证金。")
    terminal_spot = not futures and "research_valuation_at" in daily[-1]
    if terminal_spot:
        valuation_text = ("末交易日研究权益包含请求终点的退市账户观察；已确认退市的股份按零估值，"
                          "保留股份数量，现金权益按真实事件到账。原日终账户保持原值，观察尾部不增加交易日。"
                          "研究权益实际估值时间：" + html.escape(str(daily[-1]["research_valuation_at"])) + "。")
    peak, drawdowns = initial, []
    for value in equity:
        peak = max(peak, value)
        drawdowns.append((1 - value / peak) * 100)
    net = go.Figure(go.Scatter(x=["期初", *days], y=[1.0, *(v / initial for v in equity)], name="净值"))
    if terminal_spot:
        net.data[0].customdata = ["期初", *(row["research_valuation_at"] for row in daily)]
        net.data[0].hovertemplate = "交易日：%{x}<br>净值：%{y}<br>实际估值时间：%{customdata}<extra></extra>"
    net.update_layout(title=("期货研究权益净值（含期末结算观察）" if futures else "账户净值（最新可见价格，包含公司行动权益）" if latest_visible else "账户净值（包含公司行动权益）"), xaxis_title="交易日", yaxis_title="期初=1")
    dd = go.Figure(go.Scatter(x=days, y=drawdowns, fill="tozeroy", name="回撤"))
    dd.update_layout(title="日终回撤", xaxis_title="交易日", yaxis_title="回撤（%）")
    totals = lean_trade_totals(days, rows["orders"], fills=rows["fills"], futures=futures)
    trades = go.Figure(go.Bar(x=days, y=[float(totals["gross_by_day"][d]) for d in days], name="双边成交额"))
    trades.add_trace(go.Scatter(x=days, y=[float(totals["fees_by_day"][d]) for d in days], name="成交费用", yaxis="y2"))
    trades.update_layout(title="逐日成交与费用", yaxis_title="成交额（元）",
                         yaxis2={"title": "费用（元）", "overlaying": "y", "side": "right"})
    figures_list = [net, dd, trades]
    details = ""
    if terminal_spot:
        final = daily[-1]
        details = "<h2>终点账户观察</h2><table>" + "".join(
            "<tr><th>" + html.escape(label) + "</th><td>" + html.escape(str(final[field])) + "</td></tr>"
            for field, label in (("equity", "原日终权益"), ("cash", "原日终现金"),
                                 ("research_equity", "研究权益"), ("research_cash", "研究现金"),
                                 ("valuation_at", "原日终估值时间"), ("research_valuation_at", "研究权益实际估值时间"))) + "</table>"
    if futures:
        funds = go.Figure()
        for field, label in (("research_margin", "保证金"), ("research_frozen_funds", "冻结资金"),
                             ("research_available_funds", "可用资金")):
            funds.add_trace(go.Scatter(x=days, y=[float(row[field]) for row in daily], name=label))
        funds.update_layout(title="研究估值时点的保证金与资金", xaxis_title="交易日", yaxis_title="元")
        figures_list.append(funds)
        details = _futures_details(rows, daily)
    for figure in figures_list:
        figure.update_layout(font={"family": "Microsoft YaHei", "size": 14})
    values = []
    for name in LABELS:
        ref = "project.lean." + name + "@" + LEAN_METRIC_VERSION
        if ref not in metrics:
            continue
        metric = metrics[ref]
        unit = metric.unit
        display = f"{metric.value:.4%}" if unit == "ratio" and name != "sharpe" else f"{metric.value:,.6g}"
        label = "日均保证金及冻结资金占用率" if futures and name == "capital_utilization_mean" else LABELS[name]
        values.append(f"<tr><th>{html.escape(label)}</th><td>{display}</td><td>{html.escape(unit)}</td></tr>")
    figures = "".join(to_html(figure, full_html=False, include_plotlyjs=(index == 0))
                      for index, figure in enumerate(figures_list))
    method_text = ("期货成交额按成交腿正手数×成交价×合约乘数计算；费用为实际成交腿费用之和，"
                   "成交次数按唯一fill_id计，同一成交拆成今昨腿不重复计次。保证金使用率为M/E，资金占用率为(M+F)/E。"
                   if futures else "日终权益包含公司行动权益，费用按实际订单成交事件累计。")
    return ('<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>LEAN研究报告</title>'
            '<style>body{font-family:"Microsoft YaHei",sans-serif;max-width:1100px;margin:32px auto;padding:0 20px}'
            'table{border-collapse:collapse;width:100%}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:left}'
            'pre{white-space:pre-wrap}</style></head><body><h1>LEAN研究报告</h1>'
            f'<pre>{html.escape(result_summary)}</pre><h2>正式金融指标</h2>'
            '<table><thead><tr><th>指标</th><th>值</th><th>单位</th></tr></thead><tbody>'
            + "".join(values) + '</tbody></table>' + f'<p>{valuation_text}</p>' + figures + details
            + '<p>收益包含首日；年化按252交易日；夏普无风险利率为0，平坦收益记0；'
            '换手为双边成交额的一半除以当日期初权益。' + method_text + '</p>'
            '<p>权益归零后，无外部注资的后续零权益交易日继续计入样本；没有资金占用与成交时，日收益、换手和资金占比记0。</p>'
            '</body></html>')


def _futures_details(rows, daily):
    """正式事实保持字段原值，表格文字统一转义。"""
    def table(title, records, columns):
        header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
        body = "".join("<tr>" + "".join(f"<td>{html.escape(str(row.get(key, '')))}</td>"
                                             for key, _ in columns) + "</tr>" for row in records)
        return f"<h2>{title}</h2><table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"
    sections = [table("研究收益归属与实际估值时间", daily, (
        ("date", "收益归属交易日"), ("equity", "原日终权益"), ("research_equity", "研究权益"),
        ("valuation_at", "原日终估值时间"), ("research_valuation_at", "研究权益实际估值时间"),
        ("settled_through", "原日终已结算范围"), ("research_settled_through", "研究估值已结算范围")))]
    sections.append(table("四桶持仓快照", rows["positions"], (
        ("account_id", "账户"), ("event_sequence", "事件序号"), ("instrument_id", "合约"),
        ("direction", "多空方向"), ("bucket", "今昨桶"), ("quantity", "手数"))))
    sections.append(table("实际成交腿与费用", rows["fills"], (
        ("trading_date", "交易日"), ("at", "成交时间"), ("order_id", "订单"), ("fill_id", "成交"),
        ("leg_id", "成交腿"), ("instrument_id", "合约"), ("direction", "方向"), ("bucket", "今昨桶"),
        ("position_effect", "开平"), ("quantity", "手数"), ("price", "价格"), ("multiplier", "乘数"),
        ("fee", "费用"), ("realized_pnl", "已实现盈亏"), ("rule_reference", "规则引用"))))
    sections.append(table("结算、风险与换月事件", rows["lifecycle"], (
        ("event_sequence", "事件序号"), ("at", "事件时间"), ("trading_date", "交易日"),
        ("reason", "原因"), ("kind", "类型"), ("detail", "订单、合约及计划引用"))))
    sections.append(table("真实账户事件", rows["account_events"], (
        ("event_sequence", "事件序号"), ("available_at", "可见时间"), ("trading_date", "所属交易日"),
        ("reason", "原因"), ("cash", "现金C"), ("unrealized_pnl", "浮盈亏U"), ("equity", "权益E"),
        ("margin", "保证金M"), ("frozen_funds", "冻结F"), ("available_funds", "可用A"))))
    return "".join(sections)
