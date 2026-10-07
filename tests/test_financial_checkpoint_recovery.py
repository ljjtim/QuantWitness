"""金融 checkpoint 恢复不重复记账，损坏内容不得复用。"""
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest
from research_pipeline.runtime import CheckpointStore, RuntimeIntegrityError
from research_pipeline.simulation import ExecutionGroup, FinancialEvent, SpotLedgerState, reduce_spot

NOW = datetime(2026, 1, 5, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
RULE_HASH = "a" * 64


def _event(event_id: str, kind: str, **values: object) -> FinancialEvent:
    return FinancialEvent(
        event_id,
        kind,
        NOW,
        "2026-01-05",
        "stock-cny-t1",
        RULE_HASH,
        tuple(sorted(values.items())),
    )


def _financial_payload() -> bytes:
    state = SpotLedgerState(
        ExecutionGroup("stock-cny-t1", "cn_stock", "CNY", "t1"),
        20_000,
    )
    events = (
        _event("order-1:reserve", "cash_reserved", cash_units=10_005),
        _event(
            "order-1:fill",
            "fill",
            instrument_hash="instrument-1",
            side="buy",
            quantity=100,
            notional_units=10_000,
            fee_units=5,
        ),
        _event(
            "order-1:settlement",
            "settlement",
            instrument_hash="instrument-1",
            quantity=100,
            cash_units=0,
        ),
    )
    for event in events:
        state = reduce_spot(state, event)
    payload = {
        "order_ids": ["order-1"],
        "fill_event_ids": ["order-1:fill"],
        "ledger_event_ids": [event.event_id for event in events],
        "fee_units": 5,
        "cash_units": state.total_cash_units,
        "sellable_quantity": state.positions[0].sellable,
        "applied_event_count": state.applied_event_count,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def test_financial_checkpoint_resume_does_not_repeat_fill_fee_or_ledger_event(
    tmp_path,
) -> None:
    from research_pipeline.runtime import ArtifactRef, CheckpointExpectation

    expectation = CheckpointExpectation(
        "f" * 64,
        (ArtifactRef("orders", "research.order-intents.v1", "input", "1" * 64),),
        "research.simulation.v1",
        "2" * 64,
        "3" * 64,
        "4" * 64,
    )
    store = CheckpointStore(tmp_path / "recovered")

    def crash_after_durable_commit(phase: str) -> None:
        if phase == "renamed":
            raise OSError("模拟 checkpoint 已提交、成功事件尚未写入")

    with pytest.raises(OSError, match="成功事件尚未写入"):
        store.commit_bytes(
            expectation=expectation,
            attempt_id="attempt-1",
            content=_financial_payload(),
            output_name="simulation",
            output_type="research.simulation.v1",
            audit_environment_digest="5" * 64,
            execution_identity_digest="6" * 64,
            root_seed=7,
            fixed_clock="2026-07-26T00:00:00+08:00",
            phase_hook=crash_after_durable_commit,
        )

    resumed = store.verify(expectation)
    recovered_payload = json.loads(
        (store.checkpoints_root / resumed.node_execution_id / "content.bin").read_text(
            encoding="utf-8"
        )
    )
    clean_payload = json.loads(_financial_payload())

    assert recovered_payload == clean_payload
    assert recovered_payload == {
        "applied_event_count": 3,
        "cash_units": 9995,
        "fee_units": 5,
        "fill_event_ids": ["order-1:fill"],
        "ledger_event_ids": [
            "order-1:reserve",
            "order-1:fill",
            "order-1:settlement",
        ],
        "order_ids": ["order-1"],
        "sellable_quantity": 100,
    }
    assert len(recovered_payload["order_ids"]) == len(set(recovered_payload["order_ids"]))
    assert len(recovered_payload["fill_event_ids"]) == len(
        set(recovered_payload["fill_event_ids"])
    )
    assert len(recovered_payload["ledger_event_ids"]) == len(
        set(recovered_payload["ledger_event_ids"])
    )


def test_corrupt_financial_checkpoint_is_never_reused(tmp_path) -> None:
    from research_pipeline.runtime import ArtifactRef, CheckpointExpectation

    expectation = CheckpointExpectation(
        "e" * 64,
        (ArtifactRef("orders", "research.order-intents.v1", "input", "1" * 64),),
        "research.simulation.v1",
        "2" * 64,
        "3" * 64,
        "4" * 64,
    )
    store = CheckpointStore(tmp_path / "run")
    manifest = store.commit_bytes(
        expectation=expectation,
        attempt_id="attempt-1",
        content=_financial_payload(),
        output_name="simulation",
        output_type="research.simulation.v1",
        audit_environment_digest="5" * 64,
        execution_identity_digest="6" * 64,
        root_seed=7,
        fixed_clock="2026-07-26T00:00:00+08:00",
    )
    content_path = store.checkpoints_root / manifest.node_execution_id / "content.bin"
    content_path.write_bytes(b"tampered")
    with pytest.raises(RuntimeIntegrityError, match="内容"):
        store.verify(expectation)
