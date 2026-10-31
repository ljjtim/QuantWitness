from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import research_pipeline.simulation.settlement as minute_settlement
from research_pipeline.domain import (
    InstrumentKey,
    MinuteRuleBinding,
    MinuteRuleResolver,
    MinuteRuleSnapshot,
    PortfolioTarget,
    PortfolioTargetEntry,
    load_minute_rule_snapshot_bundle,
)
from research_pipeline.simulation import IntradayExecutionPolicy, MinuteExecutionBar, SimulationContractError, build_simulation_result_contract, canonical_simulation_table


TZ = ZoneInfo("Asia/Shanghai")
HASH = "a" * 64


def test_bar_identity_references_immutable_source_row() -> None:
    from dataclasses import asdict, replace

    bar = _bar("000001.XSHE", "cn_stock", _dt(2, 9, 31), 0)
    expected = f"{bar.source_snapshot_hash}:{bar.instrument_id}:{bar.bar_end.isoformat()}:0"
    assert bar.bar_id == expected
    assert bar.bar_id == expected
    assert replace(bar, volume=bar.volume + 1).bar_id == expected
    assert replace(bar, source_sequence=1).bar_id != expected
    assert "bar_id" not in asdict(bar)
    assert "bar_hash" not in dir(bar)
    assert set(bar.to_dict()) == set(MinuteExecutionBar.__dataclass_fields__)


@pytest.mark.parametrize("changes", [
    {"instrument_id": '含引号"与反斜线\\', "session_id": "交易日\n上午"},
    {"completed": False, "quality_status": "missing", "avg_units": None},
    {"source_sequence": 2**70, "volume": 0, "open_units": 2**54,
     "high_units": 2**54, "low_units": 2**54, "close_units": 2**54},
    {"source_sequence": 0.5},
])
def test_bar_logical_identity_uses_source_and_row_key(changes):
    from dataclasses import replace
    bar = replace(_bar("000001.XSHE", "cn_stock", _dt(2, 9, 31), 0), **changes)
    assert bar.bar_id == f"{bar.source_snapshot_hash}:{bar.instrument_id}:{bar.bar_end.isoformat()}:{bar.source_sequence}"


@pytest.mark.parametrize("identity", ["0" * 64, "abcdef0123456789" * 4])
def test_execution_bar_accepts_complete_lowercase_source_identity(identity):
    from dataclasses import replace
    bar = replace(_bar("000001.XSHE", "cn_stock", _dt(2, 9, 31), 0),
                  source_snapshot_hash=identity)
    assert bar.source_snapshot_hash == identity


@pytest.mark.parametrize("identity", [None, 123, "a" * 63, "a" * 65,
    "A" * 64, "g" * 64, "a" * 63 + "\n", "a" * 64 + "\n"])
def test_execution_bar_rejects_invalid_source_identity(identity):
    from dataclasses import replace
    with pytest.raises(SimulationContractError, match="source_snapshot_hash 必须是 sha256"):
        replace(_bar("000001.XSHE", "cn_stock", _dt(2, 9, 31), 0),
                source_snapshot_hash=identity)


from minute_execution_fixture import _dt, _bar, _target, _install_supported_rules, _etf_rules, _stock_rules, _futures_rules
