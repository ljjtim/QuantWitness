from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import MarketRuleSnapshot, Price
from research_pipeline.simulation import (
    OpeningSnapshot,
    SimulationContractError,
    etf_policy_from_rule,
    group_etf_policies,
    require_visible_nav,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 1, 5, 9, 30, tzinfo=TZ)


def _rule(category: str, settlement: int) -> MarketRuleSnapshot:
    return MarketRuleSnapshot(f"etf-{category}-t{settlement}", 1, "cn_etf", "etf", date(2020, 1, 1), None, datetime(2020, 1, 1, tzinfo=TZ), "catalog.etf.rules", "https://example.invalid/etf", tuple(sorted({"commission_ppm": 300, "etf_category": category, "lot_size": 100, "min_commission_units": 500, "sell_tax_ppm": 0, "settlement_days": settlement, "transfer_fee_ppm": 0}.items())))


@pytest.mark.parametrize(("category", "settlement"), [("equity", 1), ("bond", 0), ("commodity", 0), ("cross_border", 0), ("money_market", 0)])
def test_etf_categories_use_explicit_rule(category: str, settlement: int) -> None:
    assert etf_policy_from_rule(_rule(category, settlement)).settlement_days == settlement


def test_future_nav_and_adjusted_execution_are_rejected() -> None:
    with pytest.raises(SimulationContractError, match="尚不可见"):
        require_visible_nav(nav_available_time=NOW + timedelta(minutes=1), decision_time=NOW)
    with pytest.raises(SimulationContractError, match="未复权"):
        OpeningSnapshot("x" * 64, Price(1000, 2, "CNY"), Price(1100, 2, "CNY"), Price(900, 2, "CNY"), False, 100, NOW, adjustment="post")


def test_etf_groups_keep_distinct_settlement_policies() -> None:
    groups = group_etf_policies((etf_policy_from_rule(_rule("bond", 0)), etf_policy_from_rule(_rule("equity", 1))))
    assert {group.settlement_policy_id for group in groups} == {"t0", "t1"}
