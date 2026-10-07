from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import InstrumentId, MarketRuleSnapshot, Price
from research_pipeline.simulation import (
    ExecutionGroup,
    OpeningSnapshot,
    Order,
    PositionLot,
    SimulationContractError,
    SpotLedgerState,
    etf_policy_from_rule,
    execute_cash_order,
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


def test_etf_t0_buy_is_immediately_sellable_and_groups_are_split() -> None:
    policy = etf_policy_from_rule(_rule("bond", 0))
    order = Order("e1", InstrumentId("511010.XSHG", "cn_etf", "etf"), "buy", 100, "market", "DAY", NOW)
    state = SpotLedgerState(ExecutionGroup("etf-t0", "cn_etf", "CNY", "t0"), 200_000)
    snapshot = OpeningSnapshot("x" * 64, Price(1000, 2, "CNY"), Price(1100, 2, "CNY"), Price(900, 2, "CNY"), False, 100, NOW)
    result = execute_cash_order(order, policy=policy, snapshot=snapshot, state=state)
    assert result.state.positions[0].sellable == 100
    groups = group_etf_policies((policy, etf_policy_from_rule(_rule("equity", 1))))
    assert {group.settlement_policy_id for group in groups} == {"t0", "t1"}


def test_etf_t1_buy_is_unsettled_and_same_day_sell_is_rejected() -> None:
    policy = etf_policy_from_rule(_rule("equity", 1))
    instrument = InstrumentId("510300.XSHG", "cn_etf", "etf")
    instrument_hash = "x" * 64
    snapshot = OpeningSnapshot(
        instrument_hash,
        Price(1000, 2, "CNY"),
        Price(1100, 2, "CNY"),
        Price(900, 2, "CNY"),
        False,
        100,
        NOW,
    )
    state = SpotLedgerState(ExecutionGroup("etf-t1", "cn_etf", "CNY", "t1"), 200_000)
    bought = execute_cash_order(
        Order("buy", instrument, "buy", 100, "market", "DAY", NOW),
        policy=policy,
        snapshot=snapshot,
        state=state,
    )
    assert bought.state.positions[0].unsettled == 100
    sold = execute_cash_order(
        Order("sell", instrument, "sell", 100, "market", "DAY", NOW),
        policy=policy,
        snapshot=snapshot,
        state=bought.state,
    )
    assert sold.reason_code == "t1_sell_blocked"


@pytest.mark.parametrize(
    ("paused", "open_units", "high_units", "low_units", "side", "reason"),
    [
        (True, 1000, 1100, 900, "buy", "suspended"),
        (False, 1100, 1100, 900, "buy", "limit_up_buy_blocked"),
        (False, 900, 1100, 900, "sell", "limit_down_sell_blocked"),
    ],
)
def test_etf_opening_rejections_are_explicit(
    paused: bool,
    open_units: int,
    high_units: int,
    low_units: int,
    side: str,
    reason: str,
) -> None:
    policy = etf_policy_from_rule(_rule("equity", 1))
    instrument = InstrumentId("510300.XSHG", "cn_etf", "etf")
    state = SpotLedgerState(
        ExecutionGroup("etf-reject", "cn_etf", "CNY", "t1"),
        200_000,
        positions=(PositionLot("x" * 64, sellable=100),),
    )
    result = execute_cash_order(
        Order("reject", instrument, side, 100, "market", "DAY", NOW),
        policy=policy,
        snapshot=OpeningSnapshot(
            "x" * 64,
            Price(open_units, 2, "CNY"),
            Price(high_units, 2, "CNY"),
            Price(low_units, 2, "CNY"),
            paused,
            100,
            NOW,
        ),
        state=state,
    )
    assert result.reason_code == reason


def test_future_nav_and_adjusted_execution_are_rejected() -> None:
    with pytest.raises(SimulationContractError, match="尚不可见"):
        require_visible_nav(nav_available_time=NOW + timedelta(minutes=1), decision_time=NOW)
    with pytest.raises(SimulationContractError, match="未复权"):
        OpeningSnapshot("x" * 64, Price(1000, 2, "CNY"), Price(1100, 2, "CNY"), Price(900, 2, "CNY"), False, 100, NOW, adjustment="post")
