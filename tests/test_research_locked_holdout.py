from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import threading

import pytest

from research_pipeline.research.validation import (
    HoldoutAccessLedger,
    HoldoutAccessPlan,
    PersistentHoldoutLedger,
    ValidationError,
    persistent_holdout_ledger_root,
)


UNLOCK_AT = datetime(2024, 2, 1, tzinfo=timezone.utc)


def _freeze_payload(
    *,
    mode: str = "single_candidate_confirmation",
    candidates: tuple[str, ...] = ("candidate-a",),
    validation_purpose: str = "selection",
    uses_validation: bool = True,
    multiple_testing: dict[str, object] | None = None,
    exposed_intervals: tuple[dict[str, object], ...] = (),
    aliases: dict[str, str] | None = None,
) -> dict[str, object]:
    if multiple_testing is None and mode == "family_wide_confirmation":
        multiple_testing = {
            "method": "benjamini_yekutieli",
            "family_size": len(candidates),
        }
    return {
        "parent_research_purpose": "验证冻结模型在独立时期的泛化能力",
        "data_snapshot": "snapshot-2024-01-31",
        "holdout_split": {
            "split_id": "holdout-2024-02",
            "start": "2024-02-01",
            "end": "2024-02-29",
            "sample_ids": ["h1", "h2"],
        },
        "mode": mode,
        "candidates": list(candidates),
        "selection_rule": {
            "method": "validation_metric_argmax_v1",
            "uses_validation": uses_validation,
        },
        "validation": {
            "purpose": validation_purpose,
            "rule": {"metric": "neg_mean_squared_error"}
            if validation_purpose == "selection"
            else None,
        },
        "primary_estimand": "mean_out_of_sample_score",
        "direction": "greater",
        "alpha": 0.05,
        "multiple_testing": multiple_testing,
        "random_protocol": {
            "combinations": [],
            "repetitions": 0,
            "seed": 7,
        },
        "failure_policy": "opened_then_failure_is_consumed",
        "exposed_intervals": list(exposed_intervals),
        "package_plan_identity": "a" * 64,
        "implementation_identity": "b" * 64,
        "aliases": aliases
        or {"run": "run-a", "package": "package-a", "family": "family-a"},
    }


def _plan(**overrides: object) -> HoldoutAccessPlan:
    payload = _freeze_payload(**overrides)
    return HoldoutAccessPlan.build(
        freeze_payload=payload,
        actor="reviewer",
        reason="final_evaluation",
        unlock_at=UNLOCK_AT,
    )


def test_freeze_payload_is_closed_and_modes_are_mutually_exclusive() -> None:
    incomplete = _freeze_payload()
    incomplete.pop("primary_estimand")
    with pytest.raises(ValidationError, match="freeze payload"):
        HoldoutAccessPlan.build(
            freeze_payload=incomplete,
            actor="reviewer",
            reason="final_evaluation",
            unlock_at=UNLOCK_AT,
        )

    with pytest.raises(ValidationError, match="唯一候选"):
        _plan(candidates=("candidate-a", "candidate-b"))
    with pytest.raises(ValidationError, match="多重检验"):
        _plan(
            mode="family_wide_confirmation",
            candidates=("candidate-a", "candidate-b"),
            multiple_testing={},
        )


def test_validation_without_predeclared_selection_rule_must_be_diagnostic() -> None:
    with pytest.raises(ValidationError, match="diagnostic"):
        _plan(validation_purpose="selection", uses_validation=False)
    diagnostic = _plan(validation_purpose="diagnostic", uses_validation=False)
    assert diagnostic.freeze_payload["validation"]["purpose"] == "diagnostic"


def test_exposed_or_overlapping_holdout_interval_cannot_be_confirmed() -> None:
    with pytest.raises(ValidationError, match="已暴露"):
        _plan(
            exposed_intervals=(
                {
                    "start": "2024-01-01",
                    "end": "2024-02-10",
                    "reason": "legacy_family_results_seen",
                },
            )
        )


def test_holdout_access_is_once_only_and_exact_scope() -> None:
    plan = _plan()
    ledger = HoldoutAccessLedger(plan)
    event = ledger.access(
        token_hash=plan.token_hash,
        actor="reviewer",
        reason="final_evaluation",
        fixed_clock=plan.unlock_at,
        sample_ids=("h1", "h2"),
        result_hash="result",
    )
    assert event.committed is True
    ledger.verify()
    with pytest.raises(ValidationError, match="第二次"):
        ledger.access(
            token_hash=plan.token_hash,
            actor="reviewer",
            reason="final_evaluation",
            fixed_clock=plan.unlock_at,
            sample_ids=("h1", "h2"),
            result_hash="retry",
        )


def test_early_or_partial_access_is_rejected() -> None:
    plan = _plan()
    ledger = HoldoutAccessLedger(plan)
    with pytest.raises(ValidationError, match="解锁时间"):
        ledger.access(
            token_hash=plan.token_hash,
            actor="reviewer",
            reason="final_evaluation",
            fixed_clock=plan.unlock_at - timedelta(seconds=1),
            sample_ids=("h1", "h2"),
            result_hash="result",
        )
    with pytest.raises(ValidationError, match="完全一致"):
        ledger.access(
            token_hash=plan.token_hash,
            actor="reviewer",
            reason="final_evaluation",
            fixed_clock=plan.unlock_at,
            sample_ids=("h1",),
            result_hash="result",
        )


def test_preflight_failure_does_not_consume_and_opened_failure_does(tmp_path) -> None:
    ledger = PersistentHoldoutLedger(tmp_path / "holdout", _plan())
    calls = 0

    def failing_preflight() -> dict[str, object]:
        nonlocal calls
        calls += 1
        raise ValueError("footer 损坏")

    with pytest.raises(ValueError, match="footer"):
        ledger.prepare(prepared_at=UNLOCK_AT, preflight=failing_preflight)
    assert calls == 1
    assert ledger.verify()["status"] == "frozen"

    prepared = ledger.prepare(
        prepared_at=UNLOCK_AT,
        preflight=lambda: {"format": "parquet", "schema": "model-samples-v1"},
    )
    opened = ledger.open(
        opening_id="attempt-1",
        token_hash=ledger.plan.token_hash,
        actor="reviewer",
        reason="final_evaluation",
        fixed_clock=UNLOCK_AT,
        prepared_hash=str(prepared["prepared_hash"]),
    )
    terminal = ledger.finish(
        opened_hash=str(opened["opened_hash"]),
        status="consumed_failed",
        result_hash="failed-after-open",
        reason="ValueError",
    )
    assert terminal["status"] == "consumed_failed"
    assert ledger.verify()["status"] == "consumed_failed"
    with pytest.raises(ValidationError, match="已经 opened"):
        ledger.open(
            opening_id="attempt-2",
            token_hash=ledger.plan.token_hash,
            actor="reviewer",
            reason="final_evaluation",
            fixed_clock=UNLOCK_AT,
            prepared_hash=str(prepared["prepared_hash"]),
        )


@pytest.mark.parametrize("terminal_status", ["committed", "consumed_failed"])
def test_family_wide_terminal_is_always_retired(tmp_path, terminal_status) -> None:
    plan = _plan(
        mode="family_wide_confirmation",
        candidates=("candidate-a", "candidate-b"),
    )
    ledger = PersistentHoldoutLedger(tmp_path / terminal_status, plan)
    prepared = ledger.prepare(
        prepared_at=UNLOCK_AT,
        preflight=lambda: {
            "format": "parquet",
            "schema": "candidate-family-v1",
            "candidate_ids": ["candidate-a", "candidate-b"],
            "family_size": 2,
        },
    )
    opened = ledger.open(
        opening_id="family-attempt",
        token_hash=plan.token_hash,
        actor="reviewer",
        reason="final_evaluation",
        fixed_clock=UNLOCK_AT,
        prepared_hash=str(prepared["prepared_hash"]),
    )
    ledger.finish(
        opened_hash=str(opened["opened_hash"]),
        status=terminal_status,
        result_hash="family-result",
        reason=None if terminal_status == "committed" else "StatisticsError",
    )
    state = ledger.verify()
    assert state["status"] == "retired"
    assert state["final_status"] == terminal_status


def test_concurrent_open_has_one_winner_and_loser_reads_no_values(tmp_path) -> None:
    plan = _plan()
    ledger = PersistentHoldoutLedger(tmp_path / "holdout", plan)
    prepared = ledger.prepare(
        prepared_at=UNLOCK_AT,
        preflight=lambda: {"format": "parquet", "schema": "model-samples-v1"},
    )
    barrier = threading.Barrier(2)
    values_read: list[str] = []
    outcomes: list[str] = []

    def open_then_read(name: str) -> None:
        barrier.wait()
        try:
            PersistentHoldoutLedger(tmp_path / "holdout", plan).open(
                opening_id=name,
                token_hash=plan.token_hash,
                actor="reviewer",
                reason="final_evaluation",
                fixed_clock=UNLOCK_AT,
                prepared_hash=str(prepared["prepared_hash"]),
            )
            values_read.append(name)
            outcomes.append("pass")
        except ValidationError:
            outcomes.append("rejected")

    threads = [threading.Thread(target=open_then_read, args=(str(index),)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["pass", "rejected"]
    assert len(values_read) == 1


def test_alias_changes_do_not_create_a_new_holdout_identity(tmp_path) -> None:
    first = _plan()
    second = _plan(
        aliases={"run": "renamed", "package": "renamed", "family": "renamed"}
    )
    assert first.holdout_identity_hash == second.holdout_identity_hash
    ledger = PersistentHoldoutLedger(tmp_path / "holdout", first)
    prepared = ledger.prepare(
        prepared_at=UNLOCK_AT,
        preflight=lambda: {"format": "parquet", "schema": "model-samples-v1"},
    )
    opened = ledger.open(
        opening_id="attempt",
        token_hash=first.token_hash,
        actor="reviewer",
        reason="final_evaluation",
        fixed_clock=UNLOCK_AT,
        prepared_hash=str(prepared["prepared_hash"]),
    )
    ledger.finish(
        opened_hash=str(opened["opened_hash"]),
        status="committed",
        result_hash="result",
    )
    with pytest.raises(ValidationError, match="同一 holdout 身份"):
        PersistentHoldoutLedger(tmp_path / "holdout", second).initialize()


def test_sibling_run_artifact_roots_share_persistent_holdout_ledger(tmp_path) -> None:
    first = HoldoutAccessPlan.build(
        freeze_payload=_freeze_payload(),
        actor="researcher",
        reason="primary-evaluation",
        unlock_at=UNLOCK_AT,
    )
    second = HoldoutAccessPlan.build(
        freeze_payload=_freeze_payload(aliases={
            "run": "run-b",
            "package": "package-b",
            "family": "family-b",
        }),
        actor="researcher",
        reason="primary-evaluation",
        unlock_at=UNLOCK_AT,
    )
    first_root = persistent_holdout_ledger_root(
        tmp_path / "run-a-artifacts",
        scope="model",
        research_identity_hash=first.holdout_identity_hash,
    )
    second_root = persistent_holdout_ledger_root(
        tmp_path / "run-b-artifacts",
        scope="model",
        research_identity_hash=second.holdout_identity_hash,
    )

    assert first_root == second_root
    PersistentHoldoutLedger(
        first_root / first.holdout_identity_hash,
        first,
    ).initialize()
    with pytest.raises(ValidationError, match="同一 holdout 身份"):
        PersistentHoldoutLedger(
            second_root / second.holdout_identity_hash,
            second,
        ).initialize()


def test_tampered_event_is_detected() -> None:
    plan = _plan()
    ledger = HoldoutAccessLedger(plan)
    event = ledger.access(
        token_hash=plan.token_hash,
        actor="reviewer",
        reason="final_evaluation",
        fixed_clock=plan.unlock_at,
        sample_ids=("h1", "h2"),
        result_hash="result",
    )
    ledger._events[0] = replace(event, result_hash="tampered")
    with pytest.raises(ValidationError, match="损坏"):
        ledger.verify()
