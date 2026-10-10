"""由 PIT 规则快照驱动的 A 股执行适配器。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping
from zoneinfo import ZoneInfo

from research_pipeline.domain import MarketRuleSnapshot
from research_pipeline.platform.research_cost_assumption import (
    ResearchCostAssumptionError,
    normalize_stock_research_cost_assumption,
)

from .market_rules import CashMarketPolicy, require_cash_rule_applicable, stock_policy_from_rule
from .orders import SimulationContractError


@dataclass(frozen=True)
class StockPostCloseAudit:
    one_price_limit_board: bool
    zero_full_day_volume: bool


def stock_policy_from_contract(
    *,
    lot_size: int | None = None,
    market_rule: MarketRuleSnapshot,
    decision_at: datetime | None = None,
    research_cost_assumption: Mapping[str, object],
    research_start: date,
    research_end: date,
    cash_shortage_policy: str,
) -> CashMarketPolicy:
    """将带窗口费用假设绑定到显式历史市场规则。"""

    try:
        assumption = normalize_stock_research_cost_assumption(
            research_cost_assumption,
            research_start=research_start,
            research_end=research_end,
        )
    except ResearchCostAssumptionError as exc:
        raise SimulationContractError(str(exc)) from exc
    if market_rule is None:
        raise SimulationContractError("股票费用假设必须绑定显式历史市场规则，不能补入固定 T+1 或整手规则")
    if (market_rule.market, market_rule.instrument_type) != ("cn_stock", "stock"):
        raise SimulationContractError("股票费用假设收到错误资产市场规则")
    if "stock_board" not in dict(market_rule.parameters):
        raise SimulationContractError("股票费用假设必须绑定完整板块、上市阶段和数量格点规则")
    decision_at = decision_at or datetime.combine(research_start, datetime.min.time(), ZoneInfo("Asia/Shanghai"))
    require_cash_rule_applicable(market_rule, trading_date=research_start, decision_at=decision_at)
    require_cash_rule_applicable(market_rule, trading_date=research_end, decision_at=decision_at)
    parameters = dict(market_rule.parameters)
    if lot_size is not None and lot_size != parameters.get("buy_quantity_step"):
        raise SimulationContractError("股票数量步长与历史市场规则不一致")
    applicable_start = max(market_rule.effective_start, date.fromisoformat(str(assumption["applicable_start"])))
    applicable_end = min(research_end, date.fromisoformat(str(assumption["applicable_end"])))
    rule = MarketRuleSnapshot(
        market_rule.rule_id, market_rule.version, "cn_stock", "stock",
        applicable_start, applicable_end, market_rule.available_time,
        market_rule.official_source_id, market_rule.evidence_url,
        tuple(sorted({
            **parameters,
            "lot_size": parameters["buy_quantity_step"],
            "commission_ppm": assumption["commission_ppm"],
            "min_commission_units": assumption["min_commission_units"],
            "research_cost_assumption": assumption,
            "cost_model_scope": "research_assumption",
            "sell_tax_ppm": assumption["sell_tax_ppm"],
            "transfer_fee_ppm": assumption["transfer_fee_ppm"],
            "market_rule_hash": market_rule.content_hash,
        }.items())),
    )
    return stock_policy_from_rule(
        rule,
        slippage_per_share=Decimal(
            int(assumption["slippage_per_share_units"])
        ).scaleb(-2),
        cash_shortage_policy=cash_shortage_policy,
    )


def audit_stock_after_close(*, high_units: int, low_units: int, limit_units: int, full_day_volume: int) -> StockPostCloseAudit:
    """盘后审计不能反向改变开盘成交结果。"""
    return StockPostCloseAudit(high_units == low_units == limit_units, full_day_volume == 0)


__all__ = [
    "StockPostCloseAudit",
    "audit_stock_after_close",
    "stock_policy_from_contract",
    "stock_policy_from_rule",
]
