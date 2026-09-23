"""由 PIT 规则快照驱动的 A 股执行适配器。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping
from zoneinfo import ZoneInfo

from research_pipeline.domain import MarketRuleSnapshot
from research_pipeline.platform.research_cost_assumption import (
    ResearchCostAssumptionError,
    normalize_stock_research_cost_assumption,
)

from .cash_market import CashMarketPolicy
from .orders import SimulationContractError


@dataclass(frozen=True)
class StockPostCloseAudit:
    one_price_limit_board: bool
    zero_full_day_volume: bool


def stock_policy_from_contract(
    *,
    lot_size: int,
    research_cost_assumption: Mapping[str, object],
    research_start: date,
    research_end: date,
    cash_shortage_policy: str,
) -> CashMarketPolicy:
    """把 ResearchPackage 的带窗口费用假设编译为执行策略。"""

    try:
        assumption = normalize_stock_research_cost_assumption(
            research_cost_assumption,
            research_start=research_start,
            research_end=research_end,
        )
    except ResearchCostAssumptionError as exc:
        raise SimulationContractError(str(exc)) from exc
    applicable_start = date.fromisoformat(str(assumption["applicable_start"]))
    applicable_end = date.fromisoformat(str(assumption["applicable_end"]))
    rule = MarketRuleSnapshot(
        str(assumption["assumption_id"]),
        1,
        "cn_stock",
        "stock",
        applicable_start,
        applicable_end,
        datetime.combine(
            applicable_start,
            datetime.min.time(),
            ZoneInfo("Asia/Shanghai"),
        ),
        "research_package.cost_assumption",
        "research_pipeline/docs/financial_simulation.md",
        tuple(sorted({
            "commission_ppm": assumption["commission_ppm"],
            "lot_size": lot_size,
            "min_commission_units": assumption["min_commission_units"],
            "research_cost_assumption": assumption,
            "sell_tax_ppm": assumption["sell_tax_ppm"],
            "settlement_days": 1,
            "transfer_fee_ppm": assumption["transfer_fee_ppm"],
        }.items())),
    )
    return stock_policy_from_rule(
        rule,
        slippage_per_share=Decimal(
            int(assumption["slippage_per_share_units"])
        ).scaleb(-2),
        cash_shortage_policy=cash_shortage_policy,
    )


def stock_policy_from_rule(
    rule: MarketRuleSnapshot,
    *,
    slippage_per_share: object = 0,
    cash_shortage_policy: str = "reject_v1",
) -> CashMarketPolicy:
    if (rule.market, rule.instrument_type) != ("cn_stock", "stock"):
        raise SimulationContractError("A 股 adapter 收到错误资产规则")
    settlement = int(rule.parameter("settlement_days"))
    if settlement != 1:
        raise SimulationContractError("A 股必须使用 T+1")
    try:
        amount = Decimal(str(slippage_per_share))
        slippage_units = int(amount.scaleb(2))
        if Decimal(slippage_units).scaleb(-2) != amount:
            raise ValueError
    except (InvalidOperation, ValueError) as exc:
        raise SimulationContractError("每股滑点必须是非负且最多两位小数的金额") from exc
    return CashMarketPolicy(
        "cn_stock", rule, int(rule.parameter("lot_size")), settlement,
        int(rule.parameter("commission_ppm")), int(rule.parameter("min_commission_units")),
        int(rule.parameter("sell_tax_ppm")), int(rule.parameter("transfer_fee_ppm")),
        slippage_units, cash_shortage_policy,
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
