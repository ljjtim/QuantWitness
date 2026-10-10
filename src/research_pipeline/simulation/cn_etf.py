"""显式品类和 T+ 规则的中国 ETF 执行适配器。"""

from __future__ import annotations

from datetime import date, datetime

from research_pipeline.domain import MarketRuleSnapshot
from .market_rules import (
    CashMarketPolicy, ETF_CATEGORIES, etf_policy_from_rule,
    require_cash_rule_applicable,
)
from .ledger import ExecutionGroup
from .orders import SimulationContractError


def group_etf_policies(policies: tuple[CashMarketPolicy, ...]) -> tuple[ExecutionGroup, ...]:
    keys = sorted({(policy.settlement_days, policy.lot_size, policy.rule.content_hash) for policy in policies})
    return tuple(ExecutionGroup(f"cn-etf-cny-t{settlement}-lot{lot}-{rule_hash[:12]}", "cn_etf", "CNY", f"t{settlement}") for settlement, lot, rule_hash in keys)


def require_visible_nav(*, nav_available_time, decision_time) -> None:
    if nav_available_time > decision_time:
        raise SimulationContractError("NAV/IOPV 在决策时尚不可见")


__all__ = ["ETF_CATEGORIES", "etf_policy_from_rule", "group_etf_policies", "require_visible_nav", "etf_policy_for_session"]


def etf_policy_for_session(rule: MarketRuleSnapshot, *, trading_date: date, decision_at: datetime) -> CashMarketPolicy:
    """按历史品类、生命周期和可见规则生成 ETF 会话政策。"""
    require_cash_rule_applicable(rule, trading_date=trading_date, decision_at=decision_at)
    parameters = dict(rule.parameters)
    if "listed_date" not in parameters or "delisted_date" not in parameters:
        raise SimulationContractError("ETF 会话政策缺少生命周期事实")
    return etf_policy_from_rule(rule)
