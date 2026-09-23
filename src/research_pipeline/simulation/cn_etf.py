"""显式品类和 T+ 规则的中国 ETF 执行适配器。"""

from __future__ import annotations

from research_pipeline.domain import MarketRuleSnapshot

from .cash_market import CashMarketPolicy
from .ledger import ExecutionGroup
from .orders import SimulationContractError


ETF_CATEGORIES = frozenset({"equity", "bond", "commodity", "cross_border", "money_market"})


def etf_policy_from_rule(rule: MarketRuleSnapshot) -> CashMarketPolicy:
    if (rule.market, rule.instrument_type) != ("cn_etf", "etf"):
        raise SimulationContractError("ETF adapter 收到错误资产规则")
    category = str(rule.parameter("etf_category"))
    if category not in ETF_CATEGORIES:
        raise SimulationContractError("ETF 品类必须由规则快照显式给出")
    return CashMarketPolicy(
        "cn_etf", rule, int(rule.parameter("lot_size")), int(rule.parameter("settlement_days")),
        int(rule.parameter("commission_ppm")), int(rule.parameter("min_commission_units")),
        int(rule.parameter("sell_tax_ppm")), int(rule.parameter("transfer_fee_ppm")),
    )


def group_etf_policies(policies: tuple[CashMarketPolicy, ...]) -> tuple[ExecutionGroup, ...]:
    keys = sorted({(policy.settlement_days, policy.lot_size, policy.rule.content_hash) for policy in policies})
    return tuple(ExecutionGroup(f"cn-etf-cny-t{settlement}-lot{lot}-{rule_hash[:12]}", "cn_etf", "CNY", f"t{settlement}") for settlement, lot, rule_hash in keys)


def require_visible_nav(*, nav_available_time, decision_time) -> None:
    if nav_available_time > decision_time:
        raise SimulationContractError("NAV/IOPV 在决策时尚不可见")


__all__ = ["ETF_CATEGORIES", "etf_policy_from_rule", "group_etf_policies", "require_visible_nav"]
