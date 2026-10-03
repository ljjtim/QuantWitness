"""只消费正式日频组合结果表的离线研究图表。"""
from __future__ import annotations

from datetime import date
import html
import json
from typing import Mapping

import pyarrow as pa

PORTFOLIO_REPORT_VERSION = "qlib-portfolio-report-v1"
TABLE_SCHEMAS = {
    "cash": "research.simulation.cash.v1",
    "valuations": "research.simulation.valuations.v1",
    "orders": "research.simulation.orders.v1",
    "fills": "research.simulation.fills.v1",
    "positions": "research.simulation.positions.v1",
    "costs": "research.simulation.costs.v1",
    "metrics": "research.daily-simulation.metrics.v1",
}
_COLUMNS = {
    "cash": ("portfolio_id", "session", "currency", "opening_cash_units", "total_cash_units", "available_cash_units"),
    "valuations": ("portfolio_id", "session", "currency", "nav_units"),
    "orders": ("portfolio_id", "session", "order_id", "instrument_id", "side", "requested_quantity", "filled_quantity", "status", "terminal_reason", "submitted_at"),
    "fills": ("portfolio_id", "session", "fill_id", "order_id", "instrument_id", "side", "quantity", "execution_price_units", "price_scale", "notional_units", "fee_units", "fill_time"),
    "positions": ("portfolio_id", "session", "instrument_id", "quantity", "sellable_quantity", "market_value_units"),
    "costs": ("portfolio_id", "session", "fill_id", "cost_type", "currency", "amount_units"),
    "metrics": ("metric_ref", "value", "unit", "sample_start", "sample_end", "sample_size", "status"),
}


def validate_portfolio_request(value: Mapping) -> dict:
    request = dict(value)
    if (set(request) != {"contract_version", "result_id", "portfolio_id", "tables", "budget"}
            or request["contract_version"] != PORTFOLIO_REPORT_VERSION):
        raise ValueError("组合报告请求字段或 contract_version 无效")
    for name in ("result_id", "portfolio_id"):
        if not isinstance(request[name], str) or not request[name]:
            raise ValueError(f"组合报告 {name} 必须为非空字符串")
    tables, budget = request["tables"], request["budget"]
    if (not isinstance(tables, Mapping) or set(tables) != set(TABLE_SCHEMAS)
            or any(not isinstance(item, str) or not item for item in tables.values())
            or len(set(tables.values())) != len(tables)):
        raise ValueError("组合报告 tables 必须唯一绑定现金、估值、订单、成交、持仓、费用和指标七表")
    if (not isinstance(budget, Mapping) or set(budget) != {"max_rows", "memory_bytes"}
            or any(type(number) is not int or number < 1 for number in budget.values())):
        raise ValueError("组合报告预算必须给正整数 max_rows/memory_bytes")
    request["tables"], request["budget"] = dict(tables), dict(budget)
    return request


def read_portfolio_frames(context, request: Mapping) -> dict:
    """七表共享读取预算；完整组合窗口保留正式指标的样本口径。"""
    if context.snapshot.bundle.result_id != request["result_id"]:
        raise ValueError("报告请求 result_id 与 VerificationResult 关联结果不同")
    manifests = {item.table_id: item for item in context.snapshot.bundle.tables}
    selected = {}
    for role, table_id in request["tables"].items():
        manifest = manifests.get(table_id)
        if manifest is None or manifest.schema_id != TABLE_SCHEMAS[role]:
            raise ValueError(f"组合报告 {role} 的 table_id/schema_id 不匹配")
        selected[role] = manifest
    if len({(item.source_node_id, item.source_port) for item in selected.values()}) != 1:
        raise ValueError("组合报告七表必须来自同一仿真节点和输出端口")
    frames, rows, used, pandas_bytes = {}, 0, 0, 0
    budget = request["budget"]
    batch_size = max(1, min(8192, budget["max_rows"] + 1, budget["memory_bytes"] // 1024))
    for role, manifest in selected.items():
        schema = context.snapshot.table_schema(manifest.schema_id)
        columns = _COLUMNS[role]
        if not set(columns) <= set(schema.names):
            raise ValueError(f"组合报告 {role} 缺少必需字段")
        if role != "metrics" and not pa.types.is_date32(schema.field("session").type):
            raise ValueError(f"组合报告 {role}.session 必须为 Arrow date32")
        batches = []
        for batch in context.snapshot.iter_table_batches(manifest.schema_id, columns=columns, batch_size=batch_size):
            rows += batch.num_rows
            used += batch.nbytes
            if rows > budget["max_rows"] or used * 8 > budget["memory_bytes"]:
                raise ValueError("组合报告超出 max_rows/memory_bytes，请增加预算；不会自动抽样")
            batches.append(batch)
        projected_schema = pa.schema([schema.field(name) for name in columns])
        frame = pa.Table.from_batches(batches, schema=projected_schema).to_pandas()
        pandas_bytes += int(frame.memory_usage(deep=True).sum())
        if pandas_bytes * 3 > budget["memory_bytes"]:
            raise ValueError("组合报告 pandas 数据超出 memory_bytes")
        if role != "metrics":
            if frame["portfolio_id"].isna().any() or set(frame["portfolio_id"]) - {request["portfolio_id"]}:
                raise ValueError("组合报告只支持七表明确绑定的单个组合，不能混合其他组合的指标")
            if frame["session"].isna().any():
                raise ValueError(f"组合报告 {role} 日期为空")
            frame = frame.sort_values("session", kind="stable").reset_index(drop=True)
        if "currency" in frame and (frame["currency"].isna().any() or set(frame["currency"]) - {"CNY"}):
            raise ValueError("日频现金组合报告只支持 CNY 金额，单位为分")
        frames[role] = frame
    return frames


def portfolio_display_data(frames: Mapping) -> dict:
    """归一化估值和展示汇总，不重算交易、现金或正式指标。"""
    cash, nav = frames["cash"], frames["valuations"]
    if cash.empty or nav.empty or frames["metrics"].empty:
        raise ValueError("组合报告缺少现金、估值或正式指标")
    if cash["session"].duplicated().any() or nav["session"].duplicated().any():
        raise ValueError("日频组合报告现金或估值存在重复会话")
    if set(cash["session"]) != set(nav["session"]):
        raise ValueError("组合报告现金与估值会话不一致")
    if cash["opening_cash_units"].nunique() != 1 or cash["opening_cash_units"].isna().any():
        raise ValueError("组合报告初始资金必须唯一")
    initial = int(cash["opening_cash_units"].iloc[0])
    if initial <= 0 or nav["nav_units"].isna().any():
        raise ValueError("组合报告初始资金或估值无效")
    daily = nav.merge(cash.drop(columns=["portfolio_id", "currency"]), on="session", validate="one_to_one")
    daily = daily.sort_values("session", kind="stable").reset_index(drop=True)
    # 正式最大回撤以首个收盘估值为起点；首日损益保留在相对初始资金的净值中。
    daily["net_value"] = daily["nav_units"] / initial
    daily["drawdown"] = daily["nav_units"] / daily["nav_units"].cummax() - 1.0
    daily["nav_cny"] = daily["nav_units"] / 100
    daily["cash_cny"] = daily["total_cash_units"] / 100
    daily["available_cash_cny"] = daily["available_cash_units"] / 100
    sessions = set(daily["session"])
    for role in ("orders", "fills", "positions", "costs"):
        if set(frames[role]["session"]) - sessions:
            raise ValueError(f"组合报告 {role} 超出估值会话")
    positions = frames["positions"]
    holdings = positions.groupby("session")["market_value_units"].sum()
    daily["positions_cny"] = daily["session"].map(holdings).fillna(0) / 100
    fills = frames["fills"].sort_values(["fill_time", "fill_id"], kind="stable").copy()
    fills["price_cny"] = fills["execution_price_units"] / (10.0 ** fills["price_scale"])
    fills["notional_cny"] = fills["notional_units"] / 100
    fills["fee_cny"] = fills["fee_units"] / 100
    costs = frames["costs"].copy()
    costs["amount_cny"] = costs["amount_units"] / 100
    daily["cost_cny"] = daily["session"].map(costs.groupby("session")["amount_cny"].sum()).fillna(0)
    daily["cumulative_cost_cny"] = daily["cost_cny"].cumsum()
    for side in ("buy", "sell"):
        amounts = fills.loc[fills["side"] == side].groupby("session")["notional_cny"].sum()
        daily[side + "_cny"] = daily["session"].map(amounts).fillna(0)
    latest = positions.loc[positions["session"] == daily["session"].iloc[-1]].copy()
    latest["market_value_cny"] = latest["market_value_units"] / 100
    metrics = frames["metrics"]
    for row in metrics.itertuples(index=False):
        if date.fromisoformat(str(row.sample_start)) != daily["session"].iloc[0] or date.fromisoformat(str(row.sample_end)) != daily["session"].iloc[-1]:
            raise ValueError("组合报告正式指标窗口与估值窗口不一致")
    return {"daily": daily, "fills": fills, "costs": costs, "latest_positions": latest,
            "orders": frames["orders"].sort_values(["submitted_at", "order_id"], kind="stable"),
            "metrics": metrics, "initial_cash_cny": initial / 100,
            "order_status": frames["orders"].groupby("status", dropna=False).size(),
            "row_counts": {role: len(frame) for role, frame in frames.items()}}


def _figures(data):
    import plotly.graph_objects as go

    daily = data["daily"]
    sessions = [str(value) for value in daily["session"]]
    charts = []
    for title, axis, traces, percent in (
        ("含费用净值", "净值（初始资金 = 1）", (("净值", "net_value"),), False),
        ("收盘估值回撤", "相对历史最高收盘估值", (("回撤", "drawdown"),), True),
        ("资金与持仓", "人民币元（CNY）", (("资产净值", "nav_cny"), ("总现金", "cash_cny"), ("可用现金", "available_cash_cny"), ("持仓市值", "positions_cny")), False),
        ("交易费用", "人民币元（CNY）", (("当日费用", "cost_cny"), ("累计费用", "cumulative_cost_cny")), False),
        ("成交金额", "人民币元（CNY）", (("买入金额", "buy_cny"), ("卖出金额", "sell_cny")), False),
    ):
        figure = go.Figure()
        for label, column in traces:
            figure.add_trace(go.Scatter(x=sessions, y=daily[column].tolist(), name=label, mode="lines+markers"))
        figure.update_layout(title=title, xaxis_title="交易会话", yaxis_title=axis, template="plotly_white", hovermode="x unified", font={"family": "Microsoft YaHei"})
        if percent:
            figure.update_yaxes(tickformat=".2%")
        if title == "含费用净值":
            figure.add_hline(y=1, line_dash="dot", annotation_text="初始资金")
        charts.append(figure)
    statuses = data["order_status"]
    figure = go.Figure(go.Bar(x=[str(value) for value in statuses.index], y=statuses.tolist(), name="订单数"))
    figure.update_layout(title="订单终态", xaxis_title="正式订单状态", yaxis_title="订单数（笔）", template="plotly_white", font={"family": "Microsoft YaHei"})
    charts.append(figure)
    return charts


def render_portfolio_report(context, request: Mapping, *, verification_summary: str) -> str:
    from plotly.io import to_html

    request = validate_portfolio_request(request)
    data = portfolio_display_data(read_portfolio_frames(context, request))
    figures = _figures(data)
    sections = [to_html(figure, full_html=False, include_plotlyjs=index == 0) for index, figure in enumerate(figures)]

    def pre(value):
        return "<pre>" + html.escape(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)) + "</pre>"

    def table(title, frame, columns):
        return "<h2>" + title + "</h2><div class=table>" + frame.loc[:, list(columns)].rename(columns=columns).to_html(index=False, escape=True, na_rep="缺失") + "</div>"

    metrics = table("正式组合指标（原值）", data["metrics"], {
        "metric_ref": "指标", "value": "原值", "unit": "单位", "sample_start": "样本开始", "sample_end": "样本结束", "sample_size": "样本量", "status": "计算状态"})
    daily = table("每日资金与费用", data["daily"], {
        "session": "会话", "net_value": "含费用净值", "drawdown": "回撤（小数）", "nav_cny": "净资产（元）", "cash_cny": "总现金（元）", "positions_cny": "持仓市值（元）", "cost_cny": "当日费用（元）"})
    fills = table("成交明细", data["fills"], {
        "fill_time": "成交时间", "instrument_id": "证券", "side": "方向", "quantity": "数量（股/份）", "price_cny": "成交价（元）", "notional_cny": "成交金额（元）", "fee_cny": "成交费用（元）", "order_id": "订单 ID", "fill_id": "成交 ID"})
    orders = table("订单明细", data["orders"], {
        "submitted_at": "提交时间", "instrument_id": "证券", "side": "方向", "requested_quantity": "请求数量（股/份）", "filled_quantity": "成交数量（股/份）", "status": "状态", "terminal_reason": "终态原因", "order_id": "订单 ID"})
    costs = table("费用明细", data["costs"], {
        "session": "会话", "cost_type": "费用类型", "amount_cny": "金额（元）", "fill_id": "成交 ID"})
    holdings = table("期末持仓", data["latest_positions"], {
        "session": "会话", "instrument_id": "证券", "quantity": "数量（股/份）", "sellable_quantity": "可卖数量（股/份）", "market_value_cny": "市值（元）"})
    provenance = {"result_id": context.snapshot.bundle.result_id,
                  "verification_hash": context.verification.verification_hash,
                  "verification_status": context.verification.status,
                  "portfolio_id": request["portfolio_id"], "initial_cash_cny": data["initial_cash_cny"],
                  "row_counts": data["row_counts"], "request": request}
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Qlib 组合研究报告</title><style>body{font-family:"Microsoft YaHei",Arial,sans-serif;margin:2rem;color:#162334}'
        'pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f4f6f8;padding:1rem}.table{overflow-x:auto}'
        'table{border-collapse:collapse;font-size:14px}th,td{padding:.5rem;border:1px solid #d9e0e7;text-align:right}th{background:#edf2f7}</style></head><body>'
        '<h1>Qlib 组合研究报告</h1><h2>来源与验证范围</h2><pre>' + html.escape(verification_summary) + '</pre>'
        + pre(provenance)
        + '<h2>图表口径</h2><p>图表覆盖七张正式表的完整组合窗口。净值 = 已含交易费用的正式 nav_units / 初始资金，现金和持仓直接取正式账本。费用仅作展示，不再次从净值扣减。</p>'
        + '<p>回撤 = 当日收盘估值 / 截至当日最高收盘估值 − 1，首个收盘会话为 0，与正式最大回撤指标起点相同；首日相对初始资金的损益反映在净值图中。</p>'
        + '<p>金额由分换算为人民币元；成交价按 execution_price_units / 10^price_scale 换算。收益和回撤的小数指标乘 100 后为百分数；成交金额为双边实际金额，不等同于换手率。</p>'
        + metrics + ''.join(sections) + daily + fills + orders + costs + holdings
        + '<h2>已知限制</h2><p>验证状态来自所引用的 VerificationResult，图形生成不扩大其范围，也不代表投资有效。报告展示日频现金组合的账本事实；不提供未封存的基准、超额收益或假设成本曲线。</p>'
        + '</body></html>\n'
    )
