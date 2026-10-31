from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.research.statistics import (
    MINUTE_STATISTICS_CLAIM_CEILING,
    MinuteLabelReturn,
    MinuteStatisticsProfile,
    StatisticsError,
    build_minute_statistics_artifact,
    derive_minute_overlap_floors,
    derive_minute_trial_universe_hash,
    recompute_minute_statistics_artifact,
)


CANDIDATES = ("candidate.a", "candidate.b")
FIXED_CLOCK = "2026-07-17T09:00:00+08:00"


def _observations() -> tuple[MinuteLabelReturn, ...]:
    rows = []
    entries = (100, 110, 120, 220, 230, 240, *(320 + 10 * index for index in range(30)))
    returns = tuple(((-1) ** index) * (0.005 + (index % 5) * 0.003) for index in range(len(entries)))
    for candidate_index, candidate in enumerate(CANDIDATES):
        for index, (entry, value) in enumerate(zip(entries, returns, strict=True)):
            rows.append(MinuteLabelReturn(
                candidate,
                f"{candidate}-{index:02d}",
                entry - 1,
                entry,
                entry + 15,
                value * (1 if candidate_index == 0 else -0.7),
            ))
    return tuple(rows)


def _profile(*, hac_lag: int = 1, embargo_ns: int = 15) -> MinuteStatisticsProfile:
    trial_hash = derive_minute_trial_universe_hash(
        candidate_ids=CANDIDATES,
        alpha=0.05,
        fixed_clock=FIXED_CLOCK,
    )
    assert len(trial_hash) == 64
    return MinuteStatisticsProfile(
        CANDIDATES,
        200,
        300,
        700,
        hac_lag,
        embargo_ns,
        "benjamini_yekutieli",
        0.05,
        30,
        FIXED_CLOCK,
    )


def _fact_rows():
    observations = [item.to_dict() for item in sorted(_observations())]
    profile = _profile()
    assignments = []
    for item in observations:
        entry = int(item["entry_at_ns"])
        exit_at = int(item["exit_at_ns"])
        if entry < profile.train_end_ns:
            split = "train" if exit_at <= profile.train_end_ns else "purged_train_boundary"
        elif entry < profile.train_end_ns + profile.embargo_ns:
            split = "embargo_after_train"
        elif entry < profile.validation_end_ns:
            split = "validation" if exit_at <= profile.validation_end_ns else "purged_validation_boundary"
        elif entry < profile.validation_end_ns + profile.embargo_ns:
            split = "embargo_after_validation"
        elif entry < profile.test_end_ns:
            split = "test" if exit_at <= profile.test_end_ns else "purged_test_boundary"
        else:
            split = "outside"
        assignments.append({
            "observation_id": item["observation_id"],
            "candidate_id": item["candidate_id"],
            "split": split,
        })
    return observations, assignments


def test_overlap_metadata_determines_hac_and_embargo_floor() -> None:
    observations = _observations()
    assert derive_minute_overlap_floors(observations) == (1, 15)
    artifact = build_minute_statistics_artifact(observations=observations, profile=_profile())
    assert "observations" not in artifact
    assert "split_assignments" not in artifact["reported"]
    assert artifact["observation_table"] == {
        "schema_id": "research.minute-statistics.observations.v1",
        "row_count": len(observations),
    }
    assert artifact["split_assignment_table"] == {
        "schema_id": "research.minute-statistics.split-assignments.v1",
        "row_count": len(observations),
    }
    assert artifact["reported"]["derived_hac_lag_floor"] == 1
    assert artifact["reported"]["derived_embargo_ns_floor"] == 15
    assert artifact["reported"]["status"] == "pass"
    assert artifact["reported"]["claim_ceiling"] == MINUTE_STATISTICS_CLAIM_CEILING
    _rows, assignments = _fact_rows()
    assert {item["split"] for item in assignments} == {
        "train", "validation", "test",
    }

    below = build_minute_statistics_artifact(
        observations=observations,
        profile=_profile(hac_lag=0, embargo_ns=14),
    )
    assert set(below["reported"]["issue_codes"]) >= {
        "statistics.hac_below_floor",
        "split.embargo_below_floor",
    }
    assert below["reported"]["status"] == "fail"
    with pytest.raises(StatisticsError, match="min_test_samples"):
        replace(_profile(), min_test_samples=29)


def test_future_split_and_omitted_trials_fail_closed() -> None:
    artifact = build_minute_statistics_artifact(
        observations=_observations(),
        profile=_profile(),
    )
    observations, assignments = _fact_rows()
    assignments[0]["split"] = "test"
    with pytest.raises(StatisticsError, match="独立重算"):
        recompute_minute_statistics_artifact(
            artifact,
            observations=observations,
            split_assignments=assignments,
        )

    omitted = deepcopy(artifact)
    omitted["reported"]["trial_results"].pop()
    body = {key: value for key, value in omitted.items() if key != "artifact_hash"}
    omitted["artifact_hash"] = typed_canonical_hash(body)
    with pytest.raises(StatisticsError, match="独立重算"):
        recompute_minute_statistics_artifact(
            omitted,
            observations=observations,
            split_assignments=_fact_rows()[1],
        )
