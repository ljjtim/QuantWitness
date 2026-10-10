"""非交易日频现金清算的声明、会话和封存事实独立复核。"""

from __future__ import annotations

from datetime import datetime, time
import json
from typing import Mapping
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction, InstrumentKey

from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer, ordered_rows


def non_trading_dates(context):
    """独立解析来源声明，不调用生产声明解析器或金融计算。"""
    raw = context.get("non_trading_sessions")
    if not isinstance(raw, Mapping) or set(raw) != {"dates", "available_at", "source_ref"}:
        raise EvidenceContractError("非交易会话声明字段无效")
    values = raw["dates"]
    if not isinstance(values, list) or not values:
        raise EvidenceContractError("非交易会话日期必须是非空列表")
    dates = []
    for value in values:
        parsed = date_value(value, "非交易会话日期")
        if not isinstance(value, str) or parsed.isoformat() != value:
            raise EvidenceContractError("非交易会话日期必须是 ISO 日期字符串")
        dates.append(parsed)
    if dates != sorted(set(dates)):
        raise EvidenceContractError("非交易会话日期必须严格升序且唯一")
    if not isinstance(raw["source_ref"], str) or not raw["source_ref"].strip():
        raise EvidenceContractError("非交易会话缺少来源")
    if not isinstance(raw["available_at"], str):
        raise EvidenceContractError("非交易会话可见时间必须是带时区字符串")
    first_open = datetime.combine(dates[0], time(9, 15), ZoneInfo("Asia/Shanghai"))
    if aware_datetime(raw["available_at"], "非交易会话可见时间") >= first_open:
        raise EvidenceContractError("非交易会话来源须严格早于首会话盘前可见")
    return tuple(dates)


def require_non_trading_observations(context):
    """空行情只属于声明的现金账户清算路径。"""
    dates = non_trading_dates(context)
    explicit = context.get("execution_mode", "targets") == "explicit_orders"
    expected_version = ("research-daily-cash-financial-context-v2" if explicit
                        else "research-daily-cash-financial-context-v1")
    if (context.get("contract_version") != expected_version
            or not isinstance(context.get("account_context"), Mapping)
            or context.get("execution_mode", "targets") not in {"targets", "explicit_orders"}
            or any(key in context for key in ("credit_context", "external_cashflow_context"))):
        raise EvidenceContractError("非交易清算须使用无订单、无信用和外部资金流的期初现金账户")
    if explicit:
        execution = context.get("explicit_order_context")
        if not isinstance(execution, Mapping) or any(execution.get(field) != [] for field in
                ("commands", "events", "observations", "fee_facts", "command_rules")):
            raise EvidenceContractError("非交易显式模式不能含命令、订单事件、执行观察或费用事实")
    elif "explicit_order_context" in context:
        raise EvidenceContractError("非交易目标模式不能含显式订单上下文")
    if context.get("market_observations") != []:
        raise EvidenceContractError("非交易清算必须封存真实空行情")
    artifact_hash = context.get("market_artifact_hash")
    if not isinstance(artifact_hash, str) or len(artifact_hash) != 64:
        raise EvidenceContractError("非交易清算缺少空行情工件身份")
    return dates


def verify_non_trading_cash_context(*, context, canonical, rule_codes, corporate_actions):
    """检查会话及行动绑定，金额、批次与缺省零持仓由账户 oracle 复算。"""
    dates = require_non_trading_observations(context)
    for name in ("orders", "fills", "costs"):
        if len(canonical[name]):
            raise EvidenceContractError("非交易清算不能含订单、成交或成交费用")
    account = context["account_context"]
    try:
        keys = {}
        for name in ("cash", "valuations"):
            rows = ordered_rows(canonical[name], order_by=("session", "snapshot_id"))
            sessions = []
            identity = {}
            for row in rows:
                session = date_value(row["session"], f"{name}.session")
                close = datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai"))
                if aware_datetime(row["valuation_time"], f"{name}.valuation_time") != close:
                    raise EvidenceContractError("非交易现金及估值须绑定声明会话收市时间")
                sessions.append(session)
                identity[session] = (row["portfolio_id"], row["snapshot_id"])
            if tuple(sessions) != dates:
                raise EvidenceContractError("非交易现金及估值会话与声明日期不闭合")
            keys[name] = identity
        if keys["cash"] != keys["valuations"]:
            raise EvidenceContractError("非交易现金与估值快照身份不一致")
        if context.get("execution_mode") == "explicit_orders":
            ends = context["explicit_order_context"].get("session_ends")
            if not isinstance(ends, Mapping) or set(ends) != {session.isoformat() for session in dates}:
                raise EvidenceContractError("非交易显式会话终点未覆盖声明日期")
            for session in dates:
                if aware_datetime(ends[session.isoformat()], "显式会话终点") != datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai")):
                    raise EvidenceContractError("非交易显式会话终点与声明收市时间不一致")
        for session in dates:
            preopen = datetime.combine(session, time(9, 15), ZoneInfo("Asia/Shanghai"))
            for entries in rule_codes.values():
                visible = [entry for entry in entries if entry["start"] <= session
                           and (entry["end"] is None or session <= entry["end"])
                           and entry["available"] <= preopen]
                if len(visible) != 1:
                    raise EvidenceContractError("非交易会话历史规则缺失、重叠或盘前尚不可见")
        instruments = {
            code: InstrumentKey(code, context["asset_class"], code.rsplit(".", 1)[-1], "CNY",
                                "stock" if context["asset_class"] == "cn_stock" else "etf").instrument_hash
            for code in rule_codes
        }
        grid = set()
        for row in ordered_rows(canonical["positions"], order_by=("session", "instrument_id")):
            session = date_value(row["session"], "position.session")
            code = row["instrument_id"]
            key = (session, code)
            if (session not in keys["cash"] or code not in instruments or key in grid
                    or row["instrument_hash"] != instruments[code]
                    or (row["portfolio_id"], row["snapshot_id"]) != keys["cash"][session]):
                raise EvidenceContractError("非交易持仓会话、证券或快照身份无效")
            grid.add(key)
            if any(integer(row[field], field, minimum=0) != 0 for field in
                   ("quantity", "sellable_quantity", "unsettled_quantity", "frozen_quantity", "market_value_units")):
                raise EvidenceContractError("非交易收市存在无报价非零持仓")
            if aware_datetime(row["valuation_time"], "position.valuation_time") != datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai")):
                raise EvidenceContractError("非交易持仓须绑定声明会话收市时间")
        opening_hashes = {lot["instrument_hash"] for lot in account["opening_snapshot"]["lots"]
                          if integer(lot["quantity"], "期初持仓数量", minimum=0)}
        if not opening_hashes:
            raise EvidenceContractError("非交易清算须包含非零期初证券持仓")
        if not opening_hashes <= set(instruments.values()):
            raise EvidenceContractError("非交易期初证券没有对应规则声明")
        first_open = datetime.combine(dates[0], time(9, 15), ZoneInfo("Asia/Shanghai"))
        cleared = {action.instrument_hash for action in corporate_actions
                   if action.contract_version == 2 and action.kind == "delisting_cash"
                   and action.effective_date == dates[0]
                   and action.announcement_available_time <= first_open
                   and action.settlement_available_time <= first_open}
        if not opening_hashes <= cleared:
            raise EvidenceContractError("非交易首会话须以已可见的 v2 退市现金行动清除全部期初持仓")
        declared = {action.action_id: action.to_dict() for action in corporate_actions}
        records = account["book"].get("corporate_action_records", [])
        sealed = [CorporateAction.from_dict(json.loads(row["action_payload"])) for row in records]
        if (len(sealed) != len(declared)
                or {action.action_id: action.to_dict() for action in sealed} != declared):
            raise EvidenceContractError("非交易公司行动声明与封存账户源事实不一致")
    except EvidenceContractError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise EvidenceContractError(f"非交易清算封存事实无效: {exc}") from exc
