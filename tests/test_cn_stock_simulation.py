from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import MarketRuleSnapshot
from research_pipeline.simulation import (
    SimulationContractError,
    audit_stock_after_close,
    stock_policy_from_contract,
    stock_policy_from_rule,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 1, 5, 9, 30, tzinfo=TZ)


def _rule() -> MarketRuleSnapshot:
    return MarketRuleSnapshot("stock-main-v1", 1, "cn_stock", "stock", date(2020, 1, 1), None, datetime(2020, 1, 1, tzinfo=TZ), "catalog.stock.rules", "https://example.invalid/stock", tuple(sorted({"commission_ppm": 300, "lot_size": 100, "min_commission_units": 500, "sell_tax_ppm": 1000, "settlement_days": 1, "transfer_fee_ppm": 10}.items())))


def _contract_market_rule() -> MarketRuleSnapshot:
    parameters = {
        **dict(_rule().parameters),
        "stock_board": "sz_main", "listing_phase": "regular", "is_st": False,
        "listed_date": "1991-04-03", "delisted_date": "2200-01-01",
        "trading_status": "trading", "price_limit_mode": "bounded",
        "buy_min_quantity": 100, "buy_quantity_step": 100,
        "sell_min_quantity": 100, "sell_quantity_step": 100,
        "sell_remainder_allowed": True,
    }
    return replace(_rule(), parameters=tuple(sorted(parameters.items())))


def _cost_assumption(**updates: object) -> dict[str, object]:
    value = {
        "contract_version": "research-cost-assumption-v1",
        "assumption_id": "test.cn-stock-cost.v1",
        "currency": "CNY",
        "rate_unit": "ppm_of_notional",
        "minimum_fee_unit": "CNY_cent",
        "slippage_unit": "CNY_cent_per_share",
        "applicable_start": "2024-01-01",
        "applicable_end": "2026-12-31",
        "commission_ppm": 300,
        "min_commission_units": 500,
        "sell_tax_ppm": 1000,
        "transfer_fee_ppm": 10,
        "slippage_per_share_units": 1,
    }
    value.update(updates)
    return value


def test_stock_contract_uses_windowed_research_cost_assumption() -> None:
    policy = stock_policy_from_contract(
        lot_size=100,
        market_rule=_contract_market_rule(),
        research_cost_assumption=_cost_assumption(),
        research_start=date(2025, 1, 1),
        research_end=date(2025, 12, 31),
        cash_shortage_policy="reject_v1",
    )
    assumption = policy.rule.parameter("research_cost_assumption")
    assert assumption["assumption_id"] == "test.cn-stock-cost.v1"
    assert assumption["slippage_per_share_units"] == 1
    assert policy.slippage_units_per_share == 1
    assert policy.settlement_days == 1
    assert policy.lot_size == 100
    assert policy.rule.parameter("market_rule_hash") == _contract_market_rule().content_hash
    assert policy.rule.parameter("stock_board") == "sz_main"


def test_stock_contract_rejects_cost_assumption_not_covering_research() -> None:
    with pytest.raises(SimulationContractError, match="未覆盖完整研究窗口"):
        stock_policy_from_contract(
            lot_size=100,
            market_rule=_contract_market_rule(),
            research_cost_assumption=_cost_assumption(
                applicable_start="2025-02-01"
            ),
            research_start=date(2025, 1, 1),
            research_end=date(2025, 12, 31),
            cash_shortage_policy="reject_v1",
        )


def test_post_close_audit_reports_limit_board_and_zero_volume() -> None:
    audit = audit_stock_after_close(high_units=1100, low_units=1100, limit_units=1100, full_day_volume=0)
    assert audit.one_price_limit_board and audit.zero_full_day_volume


def test_slippage_and_cash_shortage_policy_affect_identity() -> None:
    base = stock_policy_from_rule(_rule())
    policy = stock_policy_from_rule(_rule(), slippage_per_share=0.01, cash_shortage_policy="clip_current_lot_continue_v1")
    assert policy.slippage_units_per_share == 1
    assert policy.cash_shortage_policy == "clip_current_lot_continue_v1"
    assert base.policy_hash != policy.policy_hash
