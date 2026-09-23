"""独立分钟 validity 重算；不导入研究层统计实现。"""

from __future__ import annotations

from datetime import datetime
from collections import deque
from dataclasses import dataclass
import math
import re
from statistics import NormalDist
from typing import Mapping

import numpy as np

from research_pipeline.platform import typed_canonical_hash


MINUTE_VERIFIER_ALGORITHM_VERSIONS = {
    "data.pit": "verifier.minute-data-pit.v2",
    "label.split": "verifier.minute-label-split.v2",
    "search.holdout": "verifier.minute-trial-universe.v2",
    "statistics": "verifier.minute-statistics.v2",
    "financial.tradability": "verifier.minute-financial.v2",
}
_CLAIM_CEILING = "historical_intraday_research_observation"
_SHA256 = re.compile(r"[0-9a-f]{64}")
def _split(item: Mapping[str, object], profile: Mapping[str, object]) -> str:
    entry = int(item["entry_at_ns"])
    exit_at = int(item["exit_at_ns"])
    train_end = int(profile["train_end_ns"])
    validation_end = int(profile["validation_end_ns"])
    test_end = int(profile["test_end_ns"])
    embargo = int(profile["embargo_ns"])
    if entry < train_end:
        return "train" if exit_at <= train_end else "purged_train_boundary"
    if entry < train_end + embargo:
        return "embargo_after_train"
    if entry < validation_end:
        return "validation" if exit_at <= validation_end else "purged_validation_boundary"
    if entry < validation_end + embargo:
        return "embargo_after_validation"
    if entry < test_end:
        return "test" if exit_at <= test_end else "purged_test_boundary"
    return "outside"


def _hac(values: list[float], lag: int) -> tuple[float, float]:
    array = np.asarray(values, dtype=float)
    if len(array) < 3 or np.any(~np.isfinite(array)) or float(np.std(array, ddof=0)) <= 0:
        raise ValueError("invalid sample")
    if lag < 0 or lag >= len(array):
        raise ValueError("invalid lag")
    centered = array - float(array.mean())
    long_run = float(centered @ centered / len(array))
    for offset in range(1, lag + 1):
        gamma = float(centered[offset:] @ centered[:-offset] / len(array))
        long_run += 2.0 * (1.0 - offset / (lag + 1.0)) * gamma
    variance = max(long_run / len(array), 0.0)
    if variance <= 0:
        raise ValueError("degenerate variance")
    standard_error = math.sqrt(variance)
    p_value = 2.0 * (1.0 - NormalDist().cdf(abs(float(array.mean()) / standard_error)))
    return float(array.mean()), float(max(0.0, min(1.0, p_value)))


@dataclass
class _HacAccumulator:
    lag: int
    count: int = 0
    total: float = 0.0
    total_square: float = 0.0

    def __post_init__(self) -> None:
        self.previous = deque(maxlen=self.lag)
        self.first: list[float] = []
        self.last = deque(maxlen=self.lag)
        self.cross_products = [0.0] * (self.lag + 1)

    def add(self, value: float) -> None:
        history = tuple(self.previous)
        for offset in range(1, min(self.lag, len(history)) + 1):
            self.cross_products[offset] += value * history[-offset]
        if len(self.first) < self.lag:
            self.first.append(value)
        self.previous.append(value)
        self.last.append(value)
        self.count += 1
        self.total += value
        self.total_square += value * value

    def result(self) -> tuple[float, float]:
        if self.count < 3 or self.lag >= self.count:
            raise ValueError("invalid sample")
        mean = self.total / self.count
        centered_square = self.total_square - self.count * mean * mean
        if centered_square <= 0.0:
            raise ValueError("invalid sample")
        long_run = centered_square / self.count
        first_prefix = [0.0]
        for value in self.first:
            first_prefix.append(first_prefix[-1] + value)
        last_suffix = [0.0]
        for value in reversed(tuple(self.last)):
            last_suffix.append(last_suffix[-1] + value)
        for offset in range(1, self.lag + 1):
            pair_count = self.count - offset
            leading_sum = self.total - first_prefix[offset]
            trailing_sum = self.total - last_suffix[offset]
            centered_cross = (
                self.cross_products[offset]
                - mean * (leading_sum + trailing_sum)
                + pair_count * mean * mean
            )
            long_run += (
                2.0
                * (1.0 - offset / (self.lag + 1.0))
                * centered_cross
                / self.count
            )
        variance = max(long_run / self.count, 0.0)
        if variance <= 0.0:
            raise ValueError("degenerate variance")
        standard_error = math.sqrt(variance)
        p_value = 2.0 * (
            1.0 - NormalDist().cdf(abs(mean / standard_error))
        )
        return float(mean), float(max(0.0, min(1.0, p_value)))


def _adjust(values: list[float], method: str) -> list[float]:
    array = np.asarray(values, dtype=float)
    count = len(array)
    order = np.argsort(array, kind="mergesort")
    sorted_values = array[order]
    if method == "holm":
        adjusted_sorted = np.maximum.accumulate(sorted_values * (count - np.arange(count)))
    elif method == "benjamini_yekutieli":
        harmonic = float(np.sum(1.0 / np.arange(1, count + 1)))
        raw = sorted_values * count * harmonic / np.arange(1, count + 1)
        adjusted_sorted = np.minimum.accumulate(raw[::-1])[::-1]
    else:
        raise ValueError("unsupported multiple testing")
    output = np.empty(count, dtype=float)
    output[order] = np.minimum(1.0, adjusted_sorted)
    return [float(item) for item in output]


def _family_hash(profile: Mapping[str, object]) -> str:
    return typed_canonical_hash({
        "family_id": "minute-trial-universe-v1",
        "candidate_ids": list(profile["trial_candidate_ids"]),
        "metric": "mean_minute_label_return",
        "direction": "two_sided",
        "alpha": float(profile["alpha"]),
        "dependency_assumption": "arbitrary",
        "frozen_at": datetime.fromisoformat(str(profile["fixed_clock"])).isoformat(),
        "stage": "pre_test",
    })


def recompute_minute_validity_issues(
    payload: Mapping[str, object],
    *,
    observations: object,
    split_assignments: object,
) -> dict[str, set[str]]:
    """从原始标签/收益/profile 重算，完全忽略上游 status/pass 布尔值。"""
    issues = {
        "data.pit": set(),
        "label.split": set(),
        "search.holdout": set(),
        "statistics": set(),
        "financial.tradability": set(),
    }
    try:
        if set(payload) != {
            "contract_version", "profile", "observation_table",
            "split_assignment_table", "reported", "artifact_hash",
        }:
            raise ValueError("schema")
        if payload["contract_version"] != "minute-statistics-artifact-v2":
            raise ValueError("version")
        body = {key: value for key, value in payload.items() if key != "artifact_hash"}
        if payload["artifact_hash"] != typed_canonical_hash(body):
            raise ValueError("hash")
        profile = payload["profile"]
        observation_table = payload["observation_table"]
        split_table = payload["split_assignment_table"]
        reported = payload["reported"]
        if (
            not isinstance(profile, Mapping)
            or not isinstance(observation_table, Mapping)
            or not isinstance(split_table, Mapping)
            or not isinstance(reported, Mapping)
        ):
            raise ValueError("container")
        if observation_table != {
            "schema_id": "research.minute-statistics.observations.v1",
            "row_count": observation_table.get("row_count"),
        } or split_table != {
            "schema_id": "research.minute-statistics.split-assignments.v1",
            "row_count": split_table.get("row_count"),
        } or type(observation_table.get("row_count")) is not int or (
            observation_table.get("row_count") != split_table.get("row_count")
        ):
            raise ValueError("table metadata")
        expected_profile = {
            "contract_version", "trial_candidate_ids", "train_end_ns", "validation_end_ns",
            "test_end_ns", "hac_lag", "embargo_ns", "multiple_testing_method", "alpha",
            "min_test_samples", "fixed_clock", "claim_ceiling",
        }
        if set(profile) != expected_profile or profile["contract_version"] != "minute-statistics-profile-v1":
            raise ValueError("profile schema")
        raw_candidates = profile["trial_candidate_ids"]
        if not isinstance(raw_candidates, list) or any(not isinstance(item, str) for item in raw_candidates):
            raise ValueError("candidate types")
        candidates = tuple(raw_candidates)
        if len(candidates) < 1 or candidates != tuple(sorted(set(candidates))):
            raise ValueError("trials")
        integer_fields = (
            "train_end_ns", "validation_end_ns", "test_end_ns", "hac_lag",
            "embargo_ns", "min_test_samples",
        )
        if any(type(profile[field]) is not int for field in integer_fields):
            raise ValueError("profile integers")
        if not 0 < int(profile["train_end_ns"]) < int(profile["validation_end_ns"]) < int(profile["test_end_ns"]):
            raise ValueError("split order")
        if int(profile["hac_lag"]) < 0 or int(profile["embargo_ns"]) < 0 or int(profile["min_test_samples"]) < 30:
            raise ValueError("profile bounds")
        if isinstance(profile["alpha"], bool) or not isinstance(profile["alpha"], (int, float)) or not 0 < float(profile["alpha"]) < 1:
            raise ValueError("alpha")
        if profile["multiple_testing_method"] not in {"holm", "benjamini_yekutieli"}:
            raise ValueError("multiple testing")
        if profile.get("claim_ceiling") != _CLAIM_CEILING:
            issues["statistics"].add("statistics.minute_recompute_mismatch")

        try:
            observation_iterator = iter(observations)
            assignment_iterator = iter(split_assignments)
        except TypeError as exc:
            raise ValueError("fact iterators") from exc
        hac_lag = int(profile["hac_lag"])
        states = {candidate: _HacAccumulator(hac_lag) for candidate in candidates}
        active_intervals: dict[str, list[list[int]]] = {
            candidate: [] for candidate in candidates
        }
        last_keys: dict[str, tuple[int, str]] = {}
        observed_candidates: set[str] = set()
        observation_count = 0
        lag_floor = 0
        embargo_floor = 0
        for raw in observation_iterator:
            if not isinstance(raw, Mapping):
                raise ValueError("observation")
            if set(raw) != {
                "candidate_id", "observation_id", "decision_at_ns", "entry_at_ns",
                "exit_at_ns", "return_value",
            } or any(type(raw[field]) is not int for field in (
                "decision_at_ns", "entry_at_ns", "exit_at_ns",
            )) or isinstance(raw["return_value"], bool) or not isinstance(raw["return_value"], (int, float)):
                raise ValueError("observation schema")
            candidate = str(raw["candidate_id"])
            observation_id = str(raw["observation_id"])
            if candidate not in states:
                raise ValueError("candidate")
            decision = int(raw["decision_at_ns"])
            entry = int(raw["entry_at_ns"])
            exit_at = int(raw["exit_at_ns"])
            return_value = float(raw["return_value"])
            key = (entry, observation_id)
            if candidate in last_keys and key <= last_keys[candidate]:
                raise ValueError("observation order")
            last_keys[candidate] = key
            if not 0 < decision < entry < exit_at or not math.isfinite(return_value):
                issues["label.split"].add("label.leakage")
            expected_assignment = {
                "observation_id": observation_id,
                "candidate_id": candidate,
                "split": _split(raw, profile),
            }
            try:
                declared_assignment = next(assignment_iterator)
            except StopIteration as exc:
                raise ValueError("missing assignment") from exc
            if (
                not isinstance(declared_assignment, Mapping)
                or dict(declared_assignment) != expected_assignment
            ):
                issues["label.split"].add("split.minute_recompute_mismatch")
            active = active_intervals[candidate]
            retained: list[list[int]] = []
            for interval in active:
                if interval[0] > entry:
                    interval[1] += 1
                    retained.append(interval)
                else:
                    lag_floor = max(lag_floor, interval[1])
            retained.append([exit_at, 0])
            active_intervals[candidate] = retained
            embargo_floor = max(embargo_floor, exit_at - entry)
            if expected_assignment["split"] == "test":
                states[candidate].add(return_value)
            observed_candidates.add(candidate)
            observation_count += 1
        try:
            next(assignment_iterator)
        except StopIteration:
            pass
        else:
            issues["label.split"].add("split.minute_recompute_mismatch")
        if observation_count != observation_table["row_count"]:
            raise ValueError("observation count")
        for active in active_intervals.values():
            lag_floor = max(lag_floor, *(item[1] for item in active), 0)
        if observed_candidates != set(candidates):
            issues["search.holdout"].add("search.minute_trial_universe_incomplete")
        embargo_ns = int(profile["embargo_ns"])
        if hac_lag < lag_floor:
            issues["statistics"].add("statistics.hac_below_floor")
        if embargo_ns < embargo_floor:
            issues["label.split"].add("split.embargo_below_floor")
        family_hash = _family_hash(profile)

        raw_p: list[float] = []
        estimates: list[float | None] = []
        sample_counts: list[int] = []
        min_samples = int(profile["min_test_samples"])
        for candidate in candidates:
            state = states[candidate]
            sample_counts.append(state.count)
            if state.count < min_samples or hac_lag >= state.count:
                issues["statistics"].add("statistics.minute_sample_invalid")
                estimates.append(None)
                raw_p.append(1.0)
                continue
            try:
                estimate, p_value = state.result()
            except ValueError:
                issues["statistics"].add("statistics.minute_sample_invalid")
                estimates.append(None)
                raw_p.append(1.0)
            else:
                estimates.append(estimate)
                raw_p.append(p_value)
        adjusted = _adjust(raw_p, str(profile["multiple_testing_method"]))
        trial_results = [
            {
                "candidate_id": candidate,
                "sample_count": sample_counts[index],
                "estimate": estimates[index],
                "raw_p_value": raw_p[index],
                "adjusted_p_value": adjusted[index],
                "passes_alpha": bool(adjusted[index] <= float(profile["alpha"])),
            }
            for index, candidate in enumerate(candidates)
        ]
        expected_reported = {
            "derived_hac_lag_floor": lag_floor,
            "derived_embargo_ns_floor": embargo_floor,
            "trial_results": trial_results,
            "trial_universe_hash": family_hash,
            "issue_codes": sorted(set().union(*issues.values())),
            "status": "pass" if not any(issues.values()) else "fail",
            "claim_ceiling": _CLAIM_CEILING,
        }
        comparable_fields = set(expected_reported) - {"issue_codes", "status"}
        if any(reported.get(field) != expected_reported[field] for field in comparable_fields):
            issues["statistics"].add("statistics.minute_recompute_mismatch")
        final_codes = sorted(set().union(*issues.values()))
        if reported.get("issue_codes") != final_codes or reported.get("status") != ("pass" if not final_codes else "fail"):
            issues["statistics"].add("statistics.minute_recompute_mismatch")
    except (KeyError, TypeError, ValueError, OverflowError):
        issues["statistics"].add("statistics.minute_recompute_mismatch")
    return issues


def recompute_minute_simulation_issues(
    payload: Mapping[str, object],
) -> set[str]:
    """独立复核分钟仿真载荷，并如实保留不受支持的规则路径。"""

    issues: set[str] = set()
    try:
        expected = {
            "contract_version", "ledger_hashes", "outcomes", "policy_hash",
            "result_hash", "rule_bundle_hash",
        }
        if set(payload) != expected or payload["contract_version"] != "minute-simulation-result-v2":
            raise ValueError("schema")
        body = {key: value for key, value in payload.items() if key != "result_hash"}
        if payload["result_hash"] != typed_canonical_hash(body):
            raise ValueError("result hash")
        if any(
            not isinstance(payload[field], str)
            or _SHA256.fullmatch(str(payload[field])) is None
            for field in ("policy_hash", "rule_bundle_hash", "result_hash")
        ):
            raise ValueError("identity")
        ledger_hashes = payload["ledger_hashes"]
        outcomes = payload["outcomes"]
        if (
            not isinstance(ledger_hashes, Mapping)
            or any(
                not isinstance(key, str)
                or not key
                or not isinstance(value, str)
                or _SHA256.fullmatch(value) is None
                for key, value in ledger_hashes.items()
            )
            or not isinstance(outcomes, list)
        ):
            raise ValueError("containers")
        order_ids: list[str] = []
        for raw in outcomes:
            if not isinstance(raw, Mapping):
                raise ValueError("outcome")
            required = {
                "bar_hash", "claim_ceiling", "decision_time", "eligible_event_time",
                "fill_count", "fill_hashes", "fill_price_units", "fill_time",
                "filled_quantity", "order_id", "policy_hash", "reason_code",
                "rule_identity_hash", "status", "submitted_at", "valuation_time",
                "visible_capacity",
            }
            if set(raw) != required:
                raise ValueError("outcome schema")
            order_id = raw["order_id"]
            status = raw["status"]
            if (
                not isinstance(order_id, str)
                or not order_id
                or status not in {"filled", "partially_filled", "rejected", "unsupported"}
                or raw["policy_hash"] != payload["policy_hash"]
                or not isinstance(raw["rule_identity_hash"], str)
                or _SHA256.fullmatch(str(raw["rule_identity_hash"])) is None
            ):
                raise ValueError("outcome identity")
            order_ids.append(order_id)
            if status == "unsupported":
                if not isinstance(raw["reason_code"], str) or not raw["reason_code"]:
                    raise ValueError("unsupported reason")
                issues.add("financial.minute_simulation_unsupported")
        if order_ids != sorted(set(order_ids)):
            raise ValueError("order ids")
    except (KeyError, TypeError, ValueError):
        issues.add("financial.minute_simulation_unsupported")
    return issues


__all__ = [
    "MINUTE_VERIFIER_ALGORITHM_VERSIONS",
    "recompute_minute_simulation_issues",
    "recompute_minute_validity_issues",
]
