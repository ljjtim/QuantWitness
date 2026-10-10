"""只消费正式日频组合结果表的离线研究图表。"""
from __future__ import annotations

from datetime import date
import html
import json
from typing import Mapping

import pyarrow as pa

from research_pipeline.domain.shared_futures_result import SHARED_FUTURES_COLUMNS, SHARED_FUTURES_SCHEMA_IDS

SHARED_FUTURES_REPORT_VERSION = "shared-futures-portfolio-report-v1"
SHARED_FUTURES_REPORT_SCHEMAS = {**SHARED_FUTURES_SCHEMA_IDS, "context": "research.shared-futures.context.v1"}

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
            or request["contract_version"] not in {PORTFOLIO_REPORT_VERSION, SHARED_FUTURES_REPORT_VERSION}):
        raise ValueError("组合报告请求字段或 contract_version 无效")
    for name in ("result_id", "portfolio_id"):
        if not isinstance(request[name], str) or not request[name]:
            raise ValueError(f"组合报告 {name} 必须为非空字符串")
    tables, budget = request["tables"], request["budget"]
    expected_schemas = SHARED_FUTURES_REPORT_SCHEMAS if request["contract_version"] == SHARED_FUTURES_REPORT_VERSION else TABLE_SCHEMAS
    if (not isinstance(tables, Mapping) or set(tables) != set(expected_schemas)
            or any(not isinstance(item, str) or not item for item in tables.values())
            or len(set(tables.values())) != len(tables)):
        raise ValueError("组合报告 tables 必须唯一绑定现金、估值、订单、成交、持仓、费用和指标七表")
    if (not isinstance(budget, Mapping) or set(budget) != {"max_rows", "memory_bytes"}
            or any(type(number) is not int or number < 1 for number in budget.values())):
        raise ValueError("组合报告预算必须给正整数 max_rows/memory_bytes")
    request["tables"], request["budget"] = dict(tables), dict(budget)
    return request


def _read_daily_context(snapshot, manifest, *, memory_bytes: int):
    """只读取 Result 内同一仿真工件封存的日频账户事实。"""
    from research_pipeline.results import ResultStore

    source_path = "simulation/daily-context.json"
    supports = [item for item in snapshot.bundle.support_files if item.source_path == source_path]
    if not supports:
        return None, 0
    if len(supports) != 1 or supports[0].artifact_key != manifest.artifact_key:
        raise ValueError("组合报告日频上下文与七表仿真工件不一致")
    support = supports[0]
    raw = snapshot.support_bytes.get(source_path, snapshot.support_bytes.get(support.relative_path))
    if raw is None:
        # 消费者快照按需加载支持文件；导出 Result 保留相同 namespace。
        sealed = ResultStore(snapshot.directory.parents[2], create=False).open_snapshot(
            snapshot.directory, support_paths=(source_path,), verify_all_files=False,
        )
        if sealed.bundle != snapshot.bundle:
            raise ValueError("组合报告日频上下文的 Result 身份不一致")
        raw = sealed.support_bytes[source_path]
    if len(raw) * 8 > memory_bytes:
        raise ValueError("组合报告账户上下文超出 memory_bytes")
    daily_context = json.loads(raw)
    account = daily_context.get("account_context")
    if account is not None and (
        daily_context.get("contract_version") not in {
            "research-daily-cash-financial-context-v1", "research-daily-cash-financial-context-v2",
            "research-daily-cash-financial-context-v3", "research-daily-cash-financial-context-v4",
        }
        or not isinstance(account, Mapping)
        or account.get("contract_version") != "research-spot-account-context-v1"
    ):
        raise ValueError("组合报告账户上下文合同无效")
    cashflows = daily_context.get("external_cashflow_context")
    if cashflows is not None and (
        daily_context.get("contract_version") not in {"research-daily-cash-financial-context-v3", "research-daily-cash-financial-context-v4"}
        or not isinstance(cashflows, Mapping)
        or cashflows.get("contract_version") != "research-external-cashflow-context-v1"
    ):
        raise ValueError("组合报告资金流上下文合同无效")
    credit = daily_context.get("credit_context")
    if credit is not None and (
        daily_context.get("contract_version") != "research-daily-cash-financial-context-v4"
        or not isinstance(credit, Mapping)
        or credit.get("contract_version") != "research-credit-account-context-v1"
        or account is None
    ):
        raise ValueError("组合报告信用账户上下文合同无效")
    return daily_context, len(raw)


def _account_display(account, sessions, *, allow_zero=False):
    """按封存会话对齐负债和权益，核定与扣款分别汇总。"""
    import pandas as pd

    snapshots = account["snapshots"]
    if not snapshots:
        raise ValueError("组合报告缺少账户会话快照")
    for item in snapshots:
        if any(type(item[key]) is not int or item[key] < 0 for key in (
            "opening_nav_units", "liabilities_units", "pending_successor_units",
        )):
            raise ValueError("组合报告账户快照金额无效")
    daily = pd.DataFrame(snapshots).loc[:, [
        "session", "valuation_time", "opening_nav_units", "liabilities_units", "pending_successor_units",
    ]]
    daily["session"] = daily["session"].map(date.fromisoformat)
    if daily["session"].duplicated().any() or set(daily["session"]) != sessions:
        raise ValueError("组合报告账户快照与估值会话不一致")
    if daily["opening_nav_units"].nunique() != 1 or daily["opening_nav_units"].iloc[0] < 0 or (not allow_zero and daily["opening_nav_units"].iloc[0] == 0):
        raise ValueError("组合报告账户期初 NAV 必须唯一且为正")
    rows = []
    for event in account["financial_events"]:
        if event["kind"] not in {"tax_assessed", "tax_collected"}:
            continue
        session = date.fromisoformat(event["session"])
        if session not in sessions:
            raise ValueError("组合报告账户税款事件超出估值会话")
        payload = dict(event["payload"])
        assessed = event["kind"] == "tax_assessed"
        amount = payload["tax_units" if assessed else "cash_units"]
        if type(amount) is not int or amount < 0:
            raise ValueError("组合报告账户税款金额无效")
        rows.append({
            "session": session, "effective_time": event["effective_time"],
            "event_id": event["event_id"], "kind": event["kind"],
            "tax_action": "核定" if assessed else "扣收", "amount_cny": amount / 100,
            "assessment_id": payload["assessment_id"],
            "collection_id": payload.get("collection_id"), "transfer_id": payload.get("transfer_id"),
        })
    taxes = pd.DataFrame(rows, columns=[
        "session", "effective_time", "event_id", "kind", "tax_action", "amount_cny",
        "assessment_id", "collection_id", "transfer_id",
    ]).sort_values(["session", "effective_time", "event_id"], kind="stable")
    for kind, column in (("tax_assessed", "tax_assessed_cny"), ("tax_collected", "tax_collected_cny")):
        amounts = taxes.loc[taxes["kind"] == kind].groupby("session")["amount_cny"].sum()
        daily[column] = daily["session"].map(amounts).fillna(0)
    daily["liabilities_cny"] = daily["liabilities_units"] / 100
    daily["pending_successor_cny"] = daily["pending_successor_units"] / 100
    return daily, taxes


def read_portfolio_frames(context, request: Mapping) -> dict:
    """完整读取预算内的正式账户表，保留声明的样本口径。"""
    if request["contract_version"] == SHARED_FUTURES_REPORT_VERSION:
        return _read_shared_futures_frames(context, request)
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
        if role == "valuations" and "valuation_model" in schema.names:
            columns = (*columns, "valuation_model")
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
    daily_context, context_bytes = _read_daily_context(
        context.snapshot, selected["valuations"], memory_bytes=budget["memory_bytes"],
    )
    if daily_context is not None:
        account = daily_context.get("account_context")
        cashflows = daily_context.get("external_cashflow_context")
        if account is not None:
            rows += len(account["snapshots"]) + len(account["financial_events"])
            frames["account_context"] = account
        if cashflows is not None:
            rows += len(cashflows["plans"]) + len(cashflows["events"]) + len(cashflows["flow_valuations"]) + len(cashflows["return_series"])
            frames["external_cashflow_context"] = cashflows
        credit = daily_context.get("credit_context")
        if credit is not None:
            rows += len(credit["snapshots"]) + len(credit["events"]) + len(credit["valuations"])
            frames["credit_context"] = credit
        if rows > budget["max_rows"] or (used + context_bytes) * 8 + pandas_bytes * 3 > budget["memory_bytes"]:
            raise ValueError("组合报告账户事实超出 max_rows/memory_bytes；不会自动抽样")
    return frames


def portfolio_display_data(frames: Mapping) -> dict:
    if "shared_futures_context" in frames:
        return _shared_futures_display_data(frames)
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
    account = frames.get("account_context")
    cashflows = frames.get("external_cashflow_context")
    credit = frames.get("credit_context")
    if account is None and "valuation_model" in nav and nav["valuation_model"].eq(
        "cash_plus_positions_and_account_rights_v1"
    ).any():
        raise ValueError("组合报告账户估值模型缺少封存的 account_context，不能使用现金起点")
    if credit is None and "valuation_model" in nav and nav["valuation_model"].eq(
        "cash_plus_positions_and_credit_liabilities_v1"
    ).any():
        raise ValueError("组合报告信用估值缺少 credit_context")
    if initial < 0 or (account is None and cashflows is None and initial == 0) or nav["nav_units"].isna().any():
        raise ValueError("组合报告初始资金或估值无效")
    daily = nav.merge(cash.drop(columns=["portfolio_id", "currency"]), on="session", validate="one_to_one")
    daily = daily.sort_values("session", kind="stable").reset_index(drop=True)
    opening_nav, taxes = initial, None
    running_max = daily["nav_units"].cummax()
    if account is not None:
        account_daily, taxes = _account_display(account, set(daily["session"]), allow_zero=cashflows is not None)
        daily = daily.merge(account_daily, on="session", validate="one_to_one")
        opening_nav = int(daily["opening_nav_units"].iloc[0])
        # 账户收益与正式 metrics 共用声明起点，首日亏损也计入回撤。
        running_max = running_max.clip(lower=opening_nav)
    credit_details = None
    if credit is not None:
        if account is None:
            raise ValueError("组合报告信用账户缺少期初账户")
        credit_details = _credit_display(credit, daily)
        daily = daily.merge(credit_details, on="session", validate="one_to_one")
        opening_nav = credit["opening_nav_units"]
        if type(opening_nav) is not int or opening_nav < 0:
            raise ValueError("组合报告信用期初净资产无效")
        running_max = daily["nav_units"].cummax().clip(lower=opening_nav)
        daily["liabilities_cny"] += daily["credit_principal_cny"] + daily["credit_interest_cny"]
    if credit is not None and cashflows is None and (opening_nav <= 0 or daily["nav_units"].le(0).any()):
        raise ValueError("组合报告非正信用净资产必须有封存的收益适用状态")
    flow_details = None
    if cashflows is None:
        daily["net_value"] = daily["nav_units"] / opening_nav
        daily["drawdown"] = daily["nav_units"] / running_max - 1.0
    else:
        if cashflows["opening_nav_units"] != opening_nav:
            raise ValueError("组合报告资金流与账户期初净资产不一致")
        daily, flow_details = _cashflow_display(cashflows, daily)
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
            "opening_nav_cny": opening_nav / 100, "account_taxes": taxes,
            "credit_daily": credit_details, "credit_definition": None if credit is None else credit["definition"],
            "credit_events": None if credit is None else _credit_events_display(credit, sessions),
            "external_cashflows": flow_details, "cashflow_return_status": None if cashflows is None else cashflows["return_status"],
            "order_status": frames["orders"].groupby("status", dropna=False).size(),
            "row_counts": {role: len(frames[role]) for role in TABLE_SCHEMAS}}



def _credit_events_display(context, sessions):
    """利息确认与现金偿付分开展示，保留事件对应的协议事实。"""
    import pandas as pd

    rows = []
    for event in context["events"]:
        session = date.fromisoformat(event["session"])
        if session not in sessions:
            raise ValueError("组合报告信用事件超出估值会话")
        payload = dict(event["payload"])
        accrued = sum(item["interest_units"] for item in payload.get("accruals", ()))
        allocations = payload.get("allocations", ())
        rows.append({"session": session, "effective_time": event["effective_time"],
            "event_kind": event["kind"], "interest_accrued_cny": accrued / 100,
            "principal_paid_cny": sum(item["principal_paid_units"] for item in allocations) / 100,
            "interest_paid_cny": sum(item["interest_paid_units"] for item in allocations) / 100,
            "facts": json.dumps(payload, ensure_ascii=False, sort_keys=True)})
    return pd.DataFrame(rows, columns=["session", "effective_time", "event_kind", "interest_accrued_cny",
        "principal_paid_cny", "interest_paid_cny", "facts"])


def _credit_display(context, daily):
    """展示封存的信用快照，债务和利息不再次从正式净资产扣减。"""
    import pandas as pd

    snapshots = pd.DataFrame(context["snapshots"])
    snapshots["session"] = snapshots["session"].map(date.fromisoformat)
    if snapshots["session"].duplicated().any() or set(snapshots["session"]) != set(daily["session"]):
        raise ValueError("组合报告信用快照与估值会话不一致")
    snapshots = snapshots.sort_values("session", kind="stable").reset_index(drop=True)
    if snapshots["net_asset_units"].tolist() != daily["nav_units"].tolist():
        raise ValueError("组合报告信用净资产与正式估值不一致")
    result = pd.DataFrame({"session": snapshots["session"]})
    for source, target in (("principal_units", "credit_principal_cny"),
                           ("interest_units", "credit_interest_cny"),
                           ("margin_available_units", "credit_margin_available_cny")):
        result[target] = snapshots[source] / 100
    denominators = snapshots["maintenance_ratio_denominator"]
    result["credit_maintenance_ratio"] = snapshots["maintenance_ratio_numerator"] / denominators.where(denominators.ne(0))
    result["credit_risk_status"] = snapshots["risk_status"]
    return result


def _cashflow_display(context, daily):
    """展示独立验证所绑定的收益序列，不根据资产规模另算收益。"""
    import pandas as pd

    series = pd.DataFrame(context["return_series"])
    series["session"] = series["session"].map(date.fromisoformat)
    if series["session"].duplicated().any() or set(series["session"]) != set(daily["session"]):
        raise ValueError("组合报告资金流收益序列与估值会话不一致")
    series = series.sort_values("session", kind="stable").reset_index(drop=True)
    if series["nav_units"].tolist() != daily["nav_units"].tolist():
        raise ValueError("组合报告资金流收益序列与正式净资产不一致")
    applicable = series["return_status"].eq("applicable")
    for field in ("net_value", "drawdown"):
        values = series[field] if field in series else pd.Series(float("nan"), index=series.index)
        if values.loc[applicable].isna().any() or values.loc[~applicable].notna().any():
            raise ValueError("组合报告资金流收益值与适用状态不一致")
        daily[field] = values
    for field in ("net_flow", "cumulative_net_flow", "investment_pnl"):
        daily[field + "_cny"] = series[field + "_units"] / 100
    daily["return_status"] = series["return_status"]
    rows = []
    plans = {plan["event_id"]: plan for plan in context["plans"]}
    for flow in context["flow_valuations"]:
        plan = plans[flow["event_id"]]
        rows.append({"event_id": flow["event_id"], "requested_at": plan["requested_at"],
                     "effective_at": flow["effective_at"], "direction": plan["direction"],
                     "status": flow["status"], "reason": flow["reason"],
                     "amount_cny": plan["amount_units"] / 100,
                     "signed_flow_cny": flow["signed_flow_units"] / 100,
                     "nav_before_cny": flow["nav_before_units"] / 100,
                     "nav_after_cny": flow["nav_after_units"] / 100})
    return daily, pd.DataFrame(rows, columns=[
        "event_id", "requested_at", "effective_at", "direction", "status", "reason",
        "amount_cny", "signed_flow_cny", "nav_before_cny", "nav_after_cny",
    ])


def _figures(data):
    import plotly.graph_objects as go

    daily = data["daily"]
    sessions = [str(value) for value in daily["session"]]
    charts = []
    account = data["account_taxes"] is not None
    has_cashflows = data["external_cashflows"] is not None
    origin = "首个有效收益段" if has_cashflows else "期初净资产" if account else "初始资金"
    asset_traces = (("负债", "liabilities_cny"), ("待登记后继权益", "pending_successor_cny")) if account else ()
    specs = [
        ("含费用净值", f"净值（{origin} = 1）", (("净值", "net_value"),), False),
        ("收盘估值回撤", "相对期初及历史收盘最高净资产" if account else "相对历史最高收盘估值", (("回撤", "drawdown"),), True),
        ("资金与持仓", "人民币元（CNY）", (("资产净值", "nav_cny"), ("总现金", "cash_cny"), ("可用现金", "available_cash_cny"), ("持仓市值", "positions_cny")) + asset_traces, False),
        ("交易费用", "人民币元（CNY）", (("当日费用", "cost_cny"), ("累计费用", "cumulative_cost_cny")), False),
        ("成交金额", "人民币元（CNY）", (("买入金额", "buy_cny"), ("卖出金额", "sell_cny")), False),
    ]
    if has_cashflows:
        specs[0] = ("资金流调整后净值", "时间加权收益连乘，首个有效收益段 = 1", (("净值", "net_value"),), False)
        specs[1] = ("资金流调整后回撤", "相对资金流调整后历史最高净值", (("回撤", "drawdown"),), True)
        specs.append(("外部资金流与投资损益", "人民币元（CNY）",
                      (("累计净入金", "cumulative_net_flow_cny"), ("投资损益", "investment_pnl_cny")), False))
    if account:
        specs.append(("账户税款（独立于交易费用）", "人民币元（CNY）",
                      (("当日核定", "tax_assessed_cny"), ("当日扣收", "tax_collected_cny")), False))
    for title, axis, traces, percent in specs:
        figure = go.Figure()
        for label, column in traces:
            figure.add_trace(go.Scatter(x=sessions, y=daily[column].tolist(), name=label, mode="lines+markers"))
        figure.update_layout(title=title, xaxis_title="交易会话", yaxis_title=axis, template="plotly_white", hovermode="x unified", font={"family": "Microsoft YaHei"})
        if percent:
            figure.update_yaxes(tickformat=".2%")
        if title == "含费用净值":
            figure.add_hline(y=1, line_dash="dot", annotation_text=origin)
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
    if request["contract_version"] == SHARED_FUTURES_REPORT_VERSION:
        return _render_shared_futures_report(context, request, data, verification_summary)
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
    account_section = ""
    convention = (
        '<p>回撤 = 当日收盘估值 / 截至当日最高收盘估值 − 1，首个收盘会话为 0，与正式最大回撤指标起点相同；首日相对初始资金的损益反映在净值图中。</p>'
    )
    if data["account_taxes"] is not None:
        convention = (
            '<p>账户净值以封存 opening_nav_units（期初净资产）为分母，期初持仓按市场价值计入。'
            '回撤 = 当日收盘净资产 / 期初净资产与截至当日收盘净资产的最大值 − 1，包含首日亏损，与正式账户 metrics 起点一致。</p>'
            '<p>净资产 = 总现金（含应收） + 已登记持仓市值 + 待登记后继权益 − 负债。'
            '核定税款确认税费和应付，不扣现金；扣收减少现金及应付，不再次减少净资产。'
            '账户税款单列，不并入成交交易费用或 TCA；交易印花税仍保留在正式 costs 中。</p>'
        )
        account_section = table("账户负债、权益与税款", data["daily"], {
            "session": "会话", "valuation_time": "估值时间", "liabilities_cny": "负债（元）",
            "pending_successor_cny": "待登记后继权益（元）", "tax_assessed_cny": "当日核定税款（元）",
            "tax_collected_cny": "当日扣收税款（元）",
        }) + table("账户税款明细（按核定分配）", data["account_taxes"], {
            "session": "会话", "effective_time": "生效时间", "tax_action": "税款动作", "amount_cny": "金额（元）",
            "assessment_id": "核定 ID", "collection_id": "扣收 ID", "transfer_id": "转让 ID", "event_id": "事件 ID",
        })
    provenance = {"result_id": context.snapshot.bundle.result_id,
                  "verification_hash": context.verification.verification_hash,
                  "verification_status": context.verification.status,
                  "portfolio_id": request["portfolio_id"], "initial_cash_cny": data["initial_cash_cny"],
                  "row_counts": data["row_counts"], "request": request}
    if data["account_taxes"] is not None:
        provenance["opening_nav_cny"] = data["opening_nav_cny"]
    denominator = "期初净资产" if data["account_taxes"] is not None else "初始资金"
    chart_convention = '图表覆盖七张正式表的完整组合窗口。净值 = 已含交易费用的正式 nav_units / ' + denominator + '，现金和持仓直接取正式账本。费用仅作展示，不再次从净值扣减。'
    if data["external_cashflows"] is not None:
        chart_convention = ('净值和回撤使用封存的资金流调整后收益序列；有效资金流前先估值，入出金改变资产规模，不构成投资收益。'
                            '最大回撤覆盖资金流边界及会话收盘，图表展示会话收盘值。资产净值、累计净入金和投资损益分别展示。')
        convention = '<p>收益状态：' + html.escape(data["cashflow_return_status"]) + '。等待注资或经历非正净资产的区间不填造收益；空缺表示不适用。投资损益 = 期末净资产 − 期初净资产 − 净外部资金流。税费仍计入损益。</p>'
        provenance["cashflow_return_status"] = data["cashflow_return_status"]
        account_section += table("外部资金流与前后估值", data["external_cashflows"], {
            "event_id": "资金流 ID", "requested_at": "申请时间", "effective_at": "生效时间", "direction": "方向",
            "status": "状态", "reason": "原因", "amount_cny": "申请金额（元）", "signed_flow_cny": "净流入（元）",
            "nav_before_cny": "生效前净资产（元）", "nav_after_cny": "生效后净资产（元）",
        })
        account_section += table("账户投资损益", data["daily"], {
            "session": "会话", "nav_cny": "净资产（元）", "net_flow_cny": "当日净入金（元）",
            "cumulative_net_flow_cny": "累计净入金（元）", "investment_pnl_cny": "投资损益（元）", "return_status": "收益状态",
        })
    if data["credit_daily"] is not None:
        provenance["credit_account"] = data["credit_definition"]
        account_section += table("融资负债与担保状态", data["credit_daily"], {
            "session": "会话", "credit_principal_cny": "未还本金（元）",
            "credit_interest_cny": "应计未付利息（元）", "credit_margin_available_cny": "保证金可用余额（元）",
            "credit_maintenance_ratio": "维持担保比例（倍）", "credit_risk_status": "风险状态",
        })
        account_section += table("融资利息、偿还与风险事件", data["credit_events"], {
            "session": "会话", "effective_time": "生效时点", "event_kind": "事件",
            "interest_accrued_cny": "本次计息（元）", "principal_paid_cny": "偿还本金（元）",
            "interest_paid_cny": "支付利息（元）", "facts": "协议与执行事实",
        })
        convention += '<p>信用净资产已扣除融资本金和应计未付利息。借款、还本不属于投资者入出金；付息不再次扣减已计提的损益。融资利息单列，成交费用和TCA保持各自口径。无债务时维持担保比例不适用。</p>'
        account_section += '<h2>信用协议与规则声明</h2>' + pre(data["credit_definition"])
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Qlib 组合研究报告</title><style>body{font-family:"Microsoft YaHei",Arial,sans-serif;margin:2rem;color:#162334}'
        'pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f4f6f8;padding:1rem}.table{overflow-x:auto}'
        'table{border-collapse:collapse;font-size:14px}th,td{padding:.5rem;border:1px solid #d9e0e7;text-align:right}th{background:#edf2f7}</style></head><body>'
        '<h1>Qlib 组合研究报告</h1><h2>来源与验证范围</h2><pre>' + html.escape(verification_summary) + '</pre>'
        + pre(provenance)
        + '<h2>图表口径</h2><p>' + chart_convention + '</p>'
        + convention
        + '<p>金额由分换算为人民币元；成交价按 execution_price_units / 10^price_scale 换算。收益和回撤的小数指标乘 100 后为百分数；成交金额为双边实际金额，不等同于换手率。</p>'
        + metrics + ''.join(sections) + daily + account_section + fills + orders + costs + holdings
        + '<h2>已知限制</h2><p>验证状态来自所引用的 VerificationResult，图形生成不扩大其范围，也不代表投资有效。报告展示日频现金组合的账本事实；不提供未封存的基准、超额收益或假设成本曲线。</p>'
        + '</body></html>\n'
    )


def _read_shared_futures_frames(context, request):
    """完整读取同一工件的九表与上下文，保留方向桶及过程风险。"""
    if context.snapshot.bundle.result_id != request["result_id"]:
        raise ValueError("共享期货报告 result_id 与验证结果不一致")
    manifests = {item.table_id: item for item in context.snapshot.bundle.tables}
    frames, selected, rows, used = {}, [], 0, 0
    budget = request["budget"]
    for role, schema_id in SHARED_FUTURES_REPORT_SCHEMAS.items():
        item = manifests.get(request["tables"][role])
        if item is None or item.schema_id != schema_id or item.path_prefix != f"shared_futures/{role}":
            raise ValueError(f"共享期货报告 {role} 的正式表或路径不一致")
        selected.append(item)
        schema = context.snapshot.table_schema(schema_id)
        columns = ("payload",) if role == "context" else SHARED_FUTURES_COLUMNS[role]
        if set(schema.names) != set(columns):
            raise ValueError(f"共享期货报告 {role} schema 无效")
        batches = []
        for batch in context.snapshot.iter_table_batches(schema_id, columns=columns, batch_size=8192):
            rows += batch.num_rows
            used += batch.nbytes
            if rows > budget["max_rows"] or used * 8 > budget["memory_bytes"]:
                raise ValueError("共享期货报告超出 max_rows/memory_bytes")
            batches.append(batch)
        frames[role] = pa.Table.from_batches(batches, schema=pa.schema([schema.field(x) for x in columns])).to_pandas()
    if len({(x.source_node_id, x.source_port, x.artifact_key) for x in selected}) != 1:
        raise ValueError("共享期货报告十表必须来自同一仿真工件")
    payload = frames.pop("context")
    if len(payload) != 1 or not isinstance(payload.iloc[0]["payload"], str):
        raise ValueError("共享期货报告上下文必须为一行 JSON payload")
    sealed = json.loads(payload.iloc[0]["payload"])
    if sealed.get("version") != "research-shared-futures-context-v1" or not isinstance(sealed.get("spec"), Mapping):
        raise ValueError("共享期货报告上下文版本无效")
    spec = sealed["spec"]
    if spec["portfolio_id"] != request["portfolio_id"] or spec["currency"] != "CNY" or spec["cash_scale"] != 100:
        raise ValueError("共享期货报告账户或金额单位不一致")
    for role, frame in frames.items():
        for column in ("portfolio_id", "account_id", "currency"):
            if frame[column].isna().any() or set(frame[column]) - {spec[column]}:
                raise ValueError(f"共享期货报告 {role} 账户绑定错误")
    frames["shared_futures_context"] = sealed
    return frames


def _shared_futures_display_data(frames):
    """净值取正式 E；多空及今昨仓分别展示，不把保证金当持仓市值。"""
    from decimal import Decimal

    cash = frames["cash"].sort_values(["event_time", "sequence"], kind="stable").copy()
    nav = frames["valuations"]
    keys = ["portfolio_id", "account_id", "currency", "event_time", "sequence", "session"]
    if cash.empty or cash.duplicated(keys).any() or nav.duplicated(keys).any():
        raise ValueError("共享期货报告缺少唯一账户 NAV 快照")
    matched = cash.merge(nav, on=keys, how="outer", validate="one_to_one", indicator=True)
    if not matched["_merge"].eq("both").all() or not matched["nav_units"].eq(matched["equity_units"]).all():
        raise ValueError("共享期货报告 NAV 与账户权益不一致")
    if not matched["valuation_model"].eq("shared_futures_equity_v1").all():
        raise ValueError("共享期货报告估值模型无效")
    if not cash["equity_units"].eq(cash["cash_units"] + cash["unrealized_pnl_units"]).all() or not cash["available_units"].eq(cash["equity_units"] - cash["margin_units"] - cash["frozen_units"]).all():
        raise ValueError("共享期货报告 C/U/E/M/F/A 不守恒")
    spec = frames["shared_futures_context"]["spec"]
    initial = spec["initial_cash_units"]
    cash["net_value"] = cash["equity_units"] / initial
    cash["drawdown"] = cash["equity_units"] / cash["equity_units"].cummax().clip(lower=initial) - 1
    for column in ("cash", "unrealized_pnl", "equity", "margin", "frozen", "available"):
        cash[column + "_cny"] = cash[column + "_units"] / 100
    fills = frames["fills"].sort_values(["event_time", "sequence", "fill_id"], kind="stable").copy()
    fills["side"] = ["buy" if (row.direction == "long") == (row.position_effect == "open") else "sell" for row in fills.itertuples(index=False)]
    fills["price_cny"] = fills["execution_price_units"] / fills["price_scale"]
    fills["notional_cny"] = fills["notional_units"] / 100
    fills["fee_cny"] = fills["fee_units"] / 100
    # TCA 使用真实成交方向、手数及人民币报价乘数；同单的桶腿逐条计价。
    fills["slippage_cny"] = [float(Decimal(str(row.execution_price_units - row.reference_price_units)) / Decimal(str(row.price_scale)) * Decimal(row.contract_multiplier) * Decimal(str(row.quantity)) * (1 if row.side == "buy" else -1)) for row in fills.itertuples(index=False)]
    fills["total_cost_cny"] = fills["slippage_cny"] + fills["fee_cny"]
    positions = frames["positions"]
    last_sequence = cash.iloc[-1]["sequence"]
    latest = positions.loc[positions["sequence"] == last_sequence].copy()
    costs = frames["costs"].copy()
    costs["amount_cny"] = costs["amount_units"] / 100
    return {"shared_futures": True, "daily": cash, "fills": fills, "costs": costs,
            "latest_positions": latest, "positions": positions, "orders": frames["orders"],
            "risks": frames["risks"], "rolls": frames["rolls"], "reservations": frames["reservations"],
            "initial_cash_cny": initial / 100, "account_id": spec["account_id"],
            "order_status": frames["orders"].groupby("status", dropna=False).size(),
            "row_counts": {name: len(frames[name]) for name in SHARED_FUTURES_COLUMNS}}


def _render_shared_futures_report(context, request, data, verification_summary):
    import plotly.graph_objects as go
    from plotly.io import to_html

    daily = data["daily"]
    plots = []
    definitions = [
        ("共享账户净值", [("净值", "net_value")]),
        ("账户回撤", [("回撤", "drawdown")]),
        ("账户资金与保证金", [("现金 C", "cash_cny"), ("浮动损益 U", "unrealized_pnl_cny"), ("权益 E", "equity_cny"), ("保证金 M", "margin_cny"), ("订单冻结 F", "frozen_cny"), ("可用资金 A", "available_cny")]),
    ]
    for title, traces in definitions:
        figure = go.Figure()
        for label, column in traces:
            figure.add_trace(go.Scatter(x=daily["event_time"].tolist(), y=daily[column].tolist(), name=label, mode="lines+markers"))
        figure.update_layout(title=title, template="plotly_white", font={"family": "Microsoft YaHei"})
        plots.append(to_html(figure, full_html=False, include_plotlyjs=len(plots) == 0))
    def table(title, frame):
        return "<h2>" + title + "</h2>" + frame.to_html(index=False, escape=True, na_rep="缺失")
    sections = [table("账户权益与过程风险", daily), table("真实合约多空及今昨仓", data["latest_positions"]),
                table("分桶成交与 TCA（人民币元）", data["fills"]), table("订单终态", data["orders"]),
                table("交易费用", data["costs"]), table("冻结及释放", data["reservations"]),
                table("风险减仓", data["risks"]), table("换月额度", data["rolls"])]
    provenance = {"result_id": context.snapshot.bundle.result_id,
                  "verification_hash": context.verification.verification_hash,
                  "verification_status": context.verification.status,
                  "portfolio_id": request["portfolio_id"], "account_id": data["account_id"],
                  "row_counts": data["row_counts"], "request": request}
    return ('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>共享期货账户报告</title>'
            '<style>body{font-family:"Microsoft YaHei",sans-serif;margin:24px}table{border-collapse:collapse}td,th{padding:6px;border:1px solid #ddd}</style>'
            '<h1>共享期货账户报告</h1><p>' + html.escape(verification_summary) + '</p>'
            '<p>净值使用正式账户权益 E 与初始资金之比；E=C+U，A=E−M−F。费用已计入权益。多空持仓及今昨仓分别记账，保证金和订单冻结单独展示。</p>'
            + ''.join(plots) + ''.join(sections) + '<h2>结果来源</h2><pre>'
            + html.escape(json.dumps(provenance, ensure_ascii=False, indent=2)) + '</pre></html>')
