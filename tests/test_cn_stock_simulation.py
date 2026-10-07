from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import InstrumentId, MarketRuleSnapshot, Price
from research_pipeline.simulation import (
    ExecutionGroup,
    OpeningSnapshot,
    Order,
    SimulationContractError,
    SpotLedgerState,
    audit_stock_after_close,
    execute_cash_order,
    stock_policy_from_contract,
    stock_policy_from_rule,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 1, 5, 9, 30, tzinfo=TZ)


def _rule() -> MarketRuleSnapshot:
    return MarketRuleSnapshot("stock-main-v1", 1, "cn_stock", "stock", date(2020, 1, 1), None, datetime(2020, 1, 1, tzinfo=TZ), "catalog.stock.rules", "https://example.invalid/stock", tuple(sorted({"commission_ppm": 300, "lot_size": 100, "min_commission_units": 500, "sell_tax_ppm": 1000, "settlement_days": 1, "transfer_fee_ppm": 10}.items())))


def _snapshot(**overrides: object) -> OpeningSnapshot:
    values = {"instrument_hash": "i" * 64, "open_price": Price(1000, 2, "CNY"), "high_limit": Price(1100, 2, "CNY"), "low_limit": Price(900, 2, "CNY"), "paused": False, "visible_capacity": 100, "available_time": NOW}
    values.update(overrides)
    return OpeningSnapshot(**values)  # type: ignore[arg-type]


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


def test_stock_contract_rejects_cost_assumption_not_covering_research() -> None:
    with pytest.raises(SimulationContractError, match="未覆盖完整研究窗口"):
        stock_policy_from_contract(
            lot_size=100,
            research_cost_assumption=_cost_assumption(
                applicable_start="2025-02-01"
            ),
            research_start=date(2025, 1, 1),
            research_end=date(2025, 12, 31),
            cash_shortage_policy="reject_v1",
        )


def test_stock_buy_uses_fixed_fee_t1_and_visible_capacity_only() -> None:
    order = Order("o1", InstrumentId("000001.XSHE", "cn_stock", "stock"), "buy", 200, "market", "DAY", NOW)
    state = SpotLedgerState(ExecutionGroup("stock", "cn_stock", "CNY", "t1"), 500_000)
    result = execute_cash_order(order, policy=stock_policy_from_rule(_rule()), snapshot=_snapshot(), state=state)
    assert result.filled_quantity == 100
    assert result.reason_code == "partially_filled"
    assert result.state.positions[0].unsettled == 100
    assert result.state.available_cash_units == 399_499


def test_open_limit_rejects_but_post_close_audit_does_not_mutate_execution() -> None:
    order = Order("o1", InstrumentId("000001.XSHE", "cn_stock", "stock"), "buy", 100, "market", "DAY", NOW)
    state = SpotLedgerState(ExecutionGroup("stock", "cn_stock", "CNY", "t1"), 500_000)
    result = execute_cash_order(order, policy=stock_policy_from_rule(_rule()), snapshot=_snapshot(open_price=Price(1100, 2, "CNY")), state=state)
    assert result.reason_code == "limit_up_buy_blocked"
    audit = audit_stock_after_close(high_units=1100, low_units=1100, limit_units=1100, full_day_volume=0)
    assert audit.one_price_limit_board and audit.zero_full_day_volume


def test_stock_limit_price_and_cash_admission_are_explicit() -> None:
    state = SpotLedgerState(ExecutionGroup("stock", "cn_stock", "CNY", "t1"), 100_000)
    limit = Order("limit", InstrumentId("000001.XSHE", "cn_stock", "stock"), "buy", 100, "limit", "DAY", NOW, limit_price=Price(999, 2, "CNY"))
    assert execute_cash_order(limit, policy=stock_policy_from_rule(_rule()), snapshot=_snapshot(), state=state).reason_code == "limit_price_not_reached"
    market = Order("cash", InstrumentId("000001.XSHE", "cn_stock", "stock"), "buy", 100, "market", "DAY", NOW)
    assert execute_cash_order(market, policy=stock_policy_from_rule(_rule()), snapshot=_snapshot(), state=state).reason_code == "insufficient_cash"


def test_adjustable_slippage_and_cash_clipping_are_part_of_policy_identity() -> None:
    order = Order("clip", InstrumentId("000001.XSHE", "cn_stock", "stock"), "buy", 200, "market", "DAY", NOW)
    state = SpotLedgerState(ExecutionGroup("stock", "cn_stock", "CNY", "t1"), 150_000)
    base = stock_policy_from_rule(_rule())
    policy = stock_policy_from_rule(
        _rule(),
        slippage_per_share=0.01,
        cash_shortage_policy="clip_current_lot_continue_v1",
    )
    result = execute_cash_order(
        order,
        policy=policy,
        snapshot=_snapshot(visible_capacity=200),
        state=state,
    )
    fill = next(event for event in result.events if event.kind == "fill")

    assert result.filled_quantity == 100
    assert result.reason_code == "partially_filled"
    assert fill.values()["notional_units"] == 100_100
    assert base.policy_hash != policy.policy_hash
