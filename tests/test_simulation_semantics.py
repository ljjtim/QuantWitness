from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import MarketRuleSnapshot
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.simulation import (
    CapacityMode,
    SimulationContractError,
    SimulationInputAvailability,
    SimulationSemanticsV1,
    build_cn_daily_simulation_semantics,
)


TZ = ZoneInfo("Asia/Shanghai")
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _rule() -> MarketRuleSnapshot:
    return MarketRuleSnapshot(
        "stock-main",
        1,
        "cn_stock",
        "stock",
        date(2020, 1, 1),
        None,
        datetime(2020, 1, 1, tzinfo=TZ),
        "approved-rules",
        "https://example.invalid/rules",
        tuple(
            sorted(
                {
                    "commission_ppm": 300,
                    "lot_size": 100,
                    "min_commission_units": 500,
                    "sell_tax_ppm": 1000,
                    "settlement_days": 1,
                    "transfer_fee_ppm": 10,
                }.items()
            )
        ),
    )


def _semantics(
    mode: CapacityMode = CapacityMode.ASSUMED_UNBOUNDED,
    *,
    capacity_available_at: datetime | None = None,
):
    return build_cn_daily_simulation_semantics(
        signal_session=date(2024, 1, 1),
        execution_session=date(2024, 1, 2),
        return_end_session=date(2024, 1, 3),
        signal_source_hash=HASH_A,
        execution_source_hash=HASH_B,
        rule_source_hash=_rule().content_hash,
        rule_available_at=_rule().available_time,
        capacity_mode=mode,
        capacity_model_id="historical-open-volume" if mode is CapacityMode.MODELED else (
            "none" if mode is CapacityMode.UNKNOWN else "assumed-unbounded"
        ),
        capacity_model_version="v1",
        cost_model_id="cash-market-cost",
        cost_model_version="v1",
        capacity_available_at=capacity_available_at,
        capacity_source_hash=HASH_C if capacity_available_at is not None else None,
    )


def test_time_contract_cost_and_capacity_identity_are_hash_bound() -> None:
    semantics = _semantics()
    assert semantics.decision_at.isoformat() == "2024-01-01T15:00:00+08:00"
    assert semantics.order_submitted_at.isoformat() == "2024-01-02T09:25:00+08:00"
    assert semantics.execution_at.isoformat() == "2024-01-02T09:30:00+08:00"
    assert semantics.return_start_at < semantics.return_end_at
    assert semantics.claim_ceiling == "research_only"
    assert replace(semantics, cost_model_version="v2").semantics_hash != semantics.semantics_hash
    with pytest.raises(SimulationContractError, match="收益窗口顺序"):
        replace(semantics, return_end_at=semantics.return_start_at)
    with pytest.raises(SimulationContractError, match="Asia/Shanghai"):
        replace(semantics, signal_at=semantics.signal_at.astimezone(timezone.utc))
    serialized = semantics.to_dict()
    serialized["signal_at"] = "2024-01-01T07:00:00+00:00"
    with pytest.raises(SimulationContractError, match="Asia/Shanghai"):
        SimulationSemanticsV1.from_dict(serialized)


def test_close_to_close_account_return_window_is_supported() -> None:
    semantics = _semantics()
    close_to_close = replace(
        semantics,
        valuation_at=datetime(2024, 1, 2, 15, 0, tzinfo=TZ),
        return_start_at=datetime(2024, 1, 1, 15, 0, tzinfo=TZ),
        return_end_at=datetime(2024, 1, 2, 15, 0, tzinfo=TZ),
    )
    assert close_to_close.return_start_at == close_to_close.decision_at
    assert close_to_close.return_end_at == close_to_close.valuation_at



def test_forward_holding_return_window_remains_supported() -> None:
    semantics = _semantics()
    forward_holding = replace(
        semantics,
        valuation_at=datetime(2024, 1, 2, 9, 30, tzinfo=TZ),
        return_start_at=datetime(2024, 1, 2, 15, 0, tzinfo=TZ),
        return_end_at=datetime(2024, 1, 3, 15, 0, tzinfo=TZ),
    )
    assert forward_holding.execution_at < forward_holding.return_start_at



def test_open_fill_cannot_use_same_day_total_volume() -> None:
    with pytest.raises(SimulationContractError, match="capacity.visible.*尚不可见"):
        _semantics(
            CapacityMode.MODELED,
            capacity_available_at=datetime(2024, 1, 2, 15, 0, tzinfo=TZ),
        )


def test_future_constituent_is_rejected() -> None:
    semantics = _semantics()
    future_constituent = SimulationInputAvailability(
        "universe.constituent",
        HASH_C,
        datetime(2024, 1, 1, 15, 1, tzinfo=TZ),
        "decision",
    )
    inputs = tuple(sorted((*semantics.input_availability, future_constituent)))
    with pytest.raises(SimulationContractError, match="universe.constituent.*尚不可见"):
        replace(semantics, input_availability=inputs)


def test_required_simulation_inputs_cannot_be_omitted() -> None:
    semantics = _semantics()
    market_rule_only = tuple(
        item for item in semantics.input_availability if item.input_id == "market.rule"
    )
    with pytest.raises(SimulationContractError, match="缺少基础输入"):
        replace(semantics, input_availability=market_rule_only)


def test_unknown_and_unbounded_have_distinct_claim_ceilings() -> None:
    unknown = _semantics(CapacityMode.UNKNOWN)
    unbounded = _semantics(CapacityMode.ASSUMED_UNBOUNDED)
    assert unknown.claim_ceiling == "no_liquidity_claim"
    assert unbounded.claim_ceiling == "research_only"
    assert unknown.semantics_hash != unbounded.semantics_hash
