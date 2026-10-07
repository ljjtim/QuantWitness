from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import Instrument, InstrumentId
from research_pipeline.simulation import (
    ContinuousMapping,
    ExecutionGroup,
    FuturesFeePolicy,
    FuturesLedgerState,
    MarginPolicy,
    SimulationContractError,
    decompose_roll,
    deterministic_liquidation_order,
    resolve_actual_contract,
    settle_futures_day,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 1, 5, 15, tzinfo=TZ)
RULE = "f" * 64


def _instrument(code: str) -> Instrument:
    return Instrument(InstrumentId(code, "cn_future", "future"), date(2025, 1, 1), date(2026, 12, 31), "XSGE", contract_multiplier=10, price_tick_units=1)


def test_pit_mapping_and_roll_decomposition() -> None:
    old = ContinuousMapping("RB9999.XSGE", date(2026, 1, 4), _instrument("RB2605.XSGE"), NOW - timedelta(days=1), False)
    new = ContinuousMapping("RB9999.XSGE", date(2026, 1, 5), _instrument("RB2610.XSGE"), NOW, True)
    assert resolve_actual_contract((new,), continuous_code="RB9999.XSGE", signal_date=date(2026, 1, 5), as_of=NOW) is new
    legs = decompose_roll(previous=old, current=new, previous_contracts=2, target_contracts=3)
    assert [(leg.offset, leg.contracts) for leg in legs] == [("close_yesterday", 2), ("open", 3)]
    with pytest.raises(SimulationContractError, match="未来可见"):
        resolve_actual_contract((new,), continuous_code="RB9999.XSGE", signal_date=date(2026, 1, 5), as_of=NOW - timedelta(seconds=1))


def test_daily_settlement_margin_and_close_today_fee() -> None:
    group = ExecutionGroup("rb", "cn_future", "CNY", "daily_settlement", "rb-margin")
    state = FuturesLedgerState(group, 1_000_000)
    policy = MarginPolicy("rb-margin", 120_000, 100_000)
    state = settle_futures_day(state, event_id="settle-1", instrument=_instrument("RB2605.XSGE"), contracts=2, previous_settlement_units=3500, settlement_units=3510, margin_policy=policy, effective_time=NOW, rule_hash=RULE)
    assert state.equity_units == 1_000_200
    assert state.margin_units == 8_424
    fees = FuturesFeePolicy("rb-fee", 4, 12, 5)
    assert fees.fee(offset="close_today", contracts=2) == 24


def test_forced_liquidation_order_is_deterministic() -> None:
    positions = (("CU", 1, 100), ("RB", 2, 50), ("AU", 1, 200))
    assert deterministic_liquidation_order(positions, ("RB",)) == ("RB", "AU", "CU")


def test_same_contract_position_reversal_has_close_and_open_legs() -> None:
    mapping = ContinuousMapping("RB9999.XSGE", date(2026, 1, 5), _instrument("RB2605.XSGE"), NOW, False)
    legs = decompose_roll(previous=mapping, current=mapping, previous_contracts=2, target_contracts=-1)
    assert [(item.offset, item.contracts) for item in legs] == [("close_today", 2), ("open", 1)]
