"""分钟标签的时序拆分、HAC 下限、多重检验与可重算证据合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from collections import deque
import math
from statistics import NormalDist
from typing import Callable, Iterable, Mapping, Sequence

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.statistics_contracts import (
    MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID,
    MINUTE_STATISTICS_SPLIT_SCHEMA_ID,
)

from .contracts import StatisticsError
from .covariance import newey_west_mean
from .multiple_testing import TestFamilyManifest, adjust_p_values


MINUTE_STATISTICS_PROFILE_VERSION = "minute-statistics-profile-v1"
MINUTE_STATISTICS_ARTIFACT_VERSION = "minute-statistics-artifact-v2"
MINUTE_STATISTICS_CLAIM_CEILING = "historical_intraday_research_observation"
@dataclass(frozen=True, order=True)
class MinuteLabelReturn:
    candidate_id: str
    observation_id: str
    decision_at_ns: int
    entry_at_ns: int
    exit_at_ns: int
    return_value: float

    def __post_init__(self) -> None:
        if not self.candidate_id or not self.observation_id:
            raise StatisticsError("分钟标签 candidate/observation ID 不能为空")
        if not all(type(value) is int and value > 0 for value in (
            self.decision_at_ns, self.entry_at_ns, self.exit_at_ns,
        )):
            raise StatisticsError("分钟标签时间必须是正整数纳秒")
        if not self.decision_at_ns < self.entry_at_ns < self.exit_at_ns:
            raise StatisticsError("分钟标签必须满足 decision < entry < exit")
        if not math.isfinite(self.return_value):
            raise StatisticsError("分钟收益必须有限")

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "observation_id": self.observation_id,
            "decision_at_ns": self.decision_at_ns,
            "entry_at_ns": self.entry_at_ns,
            "exit_at_ns": self.exit_at_ns,
            "return_value": self.return_value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "MinuteLabelReturn":
        expected = {
            "candidate_id", "observation_id", "decision_at_ns", "entry_at_ns",
            "exit_at_ns", "return_value",
        }
        if set(payload) != expected:
            raise StatisticsError("分钟标签 schema 无效")
        if not all(type(payload[field]) is int for field in (
            "decision_at_ns", "entry_at_ns", "exit_at_ns",
        )) or isinstance(payload["return_value"], bool) or not isinstance(payload["return_value"], (int, float)):
            raise StatisticsError("分钟标签数值类型无效")
        return cls(
            str(payload["candidate_id"]),
            str(payload["observation_id"]),
            int(payload["decision_at_ns"]),
            int(payload["entry_at_ns"]),
            int(payload["exit_at_ns"]),
            float(payload["return_value"]),
        )


@dataclass(frozen=True)
class MinuteStatisticsProfile:
    trial_candidate_ids: tuple[str, ...]
    train_end_ns: int
    validation_end_ns: int
    test_end_ns: int
    hac_lag: int
    embargo_ns: int
    multiple_testing_method: str
    alpha: float
    min_test_samples: int
    fixed_clock: str
    claim_ceiling: str = MINUTE_STATISTICS_CLAIM_CEILING
    contract_version: str = MINUTE_STATISTICS_PROFILE_VERSION

    def __post_init__(self) -> None:
        if (
            len(self.trial_candidate_ids) < 1
            or tuple(sorted(set(self.trial_candidate_ids))) != self.trial_candidate_ids
        ):
            raise StatisticsError("分钟 trial universe 必须非空且唯一稳定排序")
        if not 0 < self.train_end_ns < self.validation_end_ns < self.test_end_ns:
            raise StatisticsError("分钟 train/validation/test 边界必须严格递增")
        if type(self.hac_lag) is not int or self.hac_lag < 0:
            raise StatisticsError("分钟 HAC lag 必须是非负整数")
        if type(self.embargo_ns) is not int or self.embargo_ns < 0:
            raise StatisticsError("分钟 embargo 必须是非负纳秒")
        if self.multiple_testing_method not in {"holm", "benjamini_yekutieli"}:
            raise StatisticsError("分钟多重检验方法不受支持")
        if not 0 < self.alpha < 1 or self.min_test_samples < 30:
            raise StatisticsError("分钟 alpha/min_test_samples 无效")
        clock = datetime.fromisoformat(self.fixed_clock)
        if clock.tzinfo is None or clock.utcoffset() is None:
            raise StatisticsError("分钟统计 fixed_clock 必须包含时区")
        if self.claim_ceiling != MINUTE_STATISTICS_CLAIM_CEILING:
            raise StatisticsError("分钟统计 claim 不得高于历史盘中研究观察")
        if self.contract_version != MINUTE_STATISTICS_PROFILE_VERSION:
            raise StatisticsError("分钟统计 profile 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "trial_candidate_ids": list(self.trial_candidate_ids),
            "train_end_ns": self.train_end_ns,
            "validation_end_ns": self.validation_end_ns,
            "test_end_ns": self.test_end_ns,
            "hac_lag": self.hac_lag,
            "embargo_ns": self.embargo_ns,
            "multiple_testing_method": self.multiple_testing_method,
            "alpha": self.alpha,
            "min_test_samples": self.min_test_samples,
            "fixed_clock": self.fixed_clock,
            "claim_ceiling": self.claim_ceiling,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "MinuteStatisticsProfile":
        expected = {
            "contract_version", "trial_candidate_ids", "train_end_ns", "validation_end_ns",
            "test_end_ns", "hac_lag", "embargo_ns", "multiple_testing_method", "alpha",
            "min_test_samples", "fixed_clock", "claim_ceiling",
        }
        if set(payload) != expected:
            raise StatisticsError("分钟统计 profile schema 无效")
        trials = payload["trial_candidate_ids"]
        if not isinstance(trials, list):
            raise StatisticsError("分钟统计 profile 容器字段无效")
        integer_fields = (
            "train_end_ns", "validation_end_ns", "test_end_ns", "hac_lag",
            "embargo_ns", "min_test_samples",
        )
        if (
            any(type(payload[field]) is not int for field in integer_fields)
            or isinstance(payload["alpha"], bool)
            or not isinstance(payload["alpha"], (int, float))
            or any(not isinstance(item, str) for item in trials)
        ):
            raise StatisticsError("分钟统计 profile 字段类型无效")
        return cls(
            tuple(str(item) for item in trials),
            int(payload["train_end_ns"]),
            int(payload["validation_end_ns"]),
            int(payload["test_end_ns"]),
            int(payload["hac_lag"]),
            int(payload["embargo_ns"]),
            str(payload["multiple_testing_method"]),
            float(payload["alpha"]),
            int(payload["min_test_samples"]),
            str(payload["fixed_clock"]),
            str(payload["claim_ceiling"]),
            str(payload["contract_version"]),
        )


def derive_minute_overlap_floors(observations: Sequence[MinuteLabelReturn]) -> tuple[int, int]:
    if not observations:
        raise StatisticsError("分钟统计没有标签")
    by_candidate: dict[str, list[MinuteLabelReturn]] = {}
    for item in observations:
        by_candidate.setdefault(item.candidate_id, []).append(item)
    lag_floor = 0
    embargo_floor = 0
    for items in by_candidate.values():
        ordered = sorted(items, key=lambda item: (item.entry_at_ns, item.observation_id))
        if len({item.observation_id for item in ordered}) != len(ordered):
            raise StatisticsError("分钟 observation_id 在候选内必须唯一")
        for index, item in enumerate(ordered):
            lag_floor = max(
                lag_floor,
                sum(candidate.entry_at_ns < item.exit_at_ns for candidate in ordered[index + 1:]),
            )
            embargo_floor = max(embargo_floor, item.exit_at_ns - item.entry_at_ns)
    return lag_floor, embargo_floor


def derive_minute_trial_universe_hash(
    *, candidate_ids: tuple[str, ...], alpha: float, fixed_clock: str,
) -> str:
    return TestFamilyManifest(
        "minute-trial-universe-v1",
        candidate_ids,
        "mean_minute_label_return",
        "two_sided",
        alpha,
        "arbitrary",
        datetime.fromisoformat(fixed_clock),
    ).manifest_hash


def _split_observation(item: MinuteLabelReturn, profile: MinuteStatisticsProfile) -> str:
    if item.entry_at_ns < profile.train_end_ns:
        return "train" if item.exit_at_ns <= profile.train_end_ns else "purged_train_boundary"
    if item.entry_at_ns < profile.train_end_ns + profile.embargo_ns:
        return "embargo_after_train"
    if item.entry_at_ns < profile.validation_end_ns:
        return "validation" if item.exit_at_ns <= profile.validation_end_ns else "purged_validation_boundary"
    if item.entry_at_ns < profile.validation_end_ns + profile.embargo_ns:
        return "embargo_after_validation"
    if item.entry_at_ns < profile.test_end_ns:
        return "test" if item.exit_at_ns <= profile.test_end_ns else "purged_test_boundary"
    return "outside"


@dataclass
class _HacStreamState:
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

    def inference(self) -> tuple[float, float]:
        if self.count < 3 or self.lag >= self.count:
            raise StatisticsError("推断样本至少 3 个且 HAC lag 必须小于样本数")
        mean = self.total / self.count
        centered_square = self.total_square - self.count * mean * mean
        if centered_square <= 0.0:
            raise StatisticsError("常数序列无法进行稳健推断")
        long_run = centered_square / self.count
        first_prefix = [0.0]
        for value in self.first:
            first_prefix.append(first_prefix[-1] + value)
        last_values = tuple(self.last)
        last_suffix = [0.0]
        for value in reversed(last_values):
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
            raise StatisticsError("HAC 方差退化，无法产生有效标准误")
        standard_error = math.sqrt(variance)
        p_value = 2.0 * (
            1.0 - NormalDist().cdf(abs(mean / standard_error))
        )
        return float(mean), float(max(0.0, min(1.0, p_value)))


def stream_minute_statistics_artifact(
    *,
    observations: Iterable[MinuteLabelReturn],
    profile: MinuteStatisticsProfile,
    emit_observation: Callable[[Mapping[str, object]], None],
    emit_assignment: Callable[[Mapping[str, object]], None],
) -> dict[str, object]:
    """单遍消费标签，只保留候选 HAC 和活动重叠窗口状态。"""

    candidates = profile.trial_candidate_ids
    states = {candidate: _HacStreamState(profile.hac_lag) for candidate in candidates}
    observed_candidates: set[str] = set()
    active_intervals: dict[str, list[list[int]]] = {
        candidate: [] for candidate in candidates
    }
    last_keys: dict[str, tuple[int, str]] = {}
    counts = {candidate: 0 for candidate in candidates}
    observation_count = 0
    lag_floor = 0
    embargo_floor = 0
    for item in observations:
        if item.candidate_id not in states:
            raise StatisticsError("分钟标签出现 trial universe 外候选")
        key = (item.entry_at_ns, item.observation_id)
        previous_key = last_keys.get(item.candidate_id)
        if previous_key is not None and key <= previous_key:
            raise StatisticsError("分钟标签必须按候选、entry 和 observation 稳定递增")
        last_keys[item.candidate_id] = key
        observed_candidates.add(item.candidate_id)
        counts[item.candidate_id] += 1
        observation_count += 1
        emit_observation(item.to_dict())
        split = _split_observation(item, profile)
        emit_assignment({
            "observation_id": item.observation_id,
            "candidate_id": item.candidate_id,
            "split": split,
        })
        active = active_intervals[item.candidate_id]
        retained: list[list[int]] = []
        for interval in active:
            if interval[0] > item.entry_at_ns:
                interval[1] += 1
                retained.append(interval)
            else:
                lag_floor = max(lag_floor, interval[1])
        retained.append([item.exit_at_ns, 0])
        active_intervals[item.candidate_id] = retained
        embargo_floor = max(
            embargo_floor,
            item.exit_at_ns - item.entry_at_ns,
        )
        if split == "test":
            states[item.candidate_id].add(item.return_value)
    if observation_count == 0:
        raise StatisticsError("分钟统计没有标签")
    for active in active_intervals.values():
        lag_floor = max(lag_floor, *(item[1] for item in active), 0)

    issues: set[str] = set()
    if profile.hac_lag < lag_floor:
        issues.add("statistics.hac_below_floor")
    if profile.embargo_ns < embargo_floor:
        issues.add("split.embargo_below_floor")
    if observed_candidates != set(candidates):
        issues.add("search.minute_trial_universe_incomplete")
    raw_p_values: list[float] = []
    estimates: list[float | None] = []
    sample_counts: list[int] = []
    for candidate in candidates:
        state = states[candidate]
        sample_counts.append(state.count)
        if state.count < profile.min_test_samples or profile.hac_lag >= state.count:
            issues.add("statistics.minute_sample_invalid")
            estimates.append(None)
            raw_p_values.append(1.0)
            continue
        try:
            estimate, p_value = state.inference()
        except StatisticsError:
            issues.add("statistics.minute_sample_invalid")
            estimates.append(None)
            raw_p_values.append(1.0)
        else:
            estimates.append(estimate)
            raw_p_values.append(p_value)
    family = TestFamilyManifest(
        "minute-trial-universe-v1", candidates,
        "mean_minute_label_return", "two_sided", profile.alpha,
        "arbitrary", datetime.fromisoformat(profile.fixed_clock),
    )
    family.require_complete_results(candidates)
    adjusted = adjust_p_values(
        raw_p_values,
        method=profile.multiple_testing_method,
        manifest=family,
    )
    trial_results = [
        {
            "candidate_id": candidate,
            "sample_count": sample_counts[index],
            "estimate": estimates[index],
            "raw_p_value": raw_p_values[index],
            "adjusted_p_value": float(adjusted[index]),
            "passes_alpha": bool(adjusted[index] <= profile.alpha),
        }
        for index, candidate in enumerate(candidates)
    ]
    reported = {
        "derived_hac_lag_floor": lag_floor,
        "derived_embargo_ns_floor": embargo_floor,
        "trial_results": trial_results,
        "trial_universe_hash": family.manifest_hash,
        "issue_codes": sorted(issues),
        "status": "pass" if not issues else "fail",
        "claim_ceiling": MINUTE_STATISTICS_CLAIM_CEILING,
    }
    payload: dict[str, object] = {
        "contract_version": MINUTE_STATISTICS_ARTIFACT_VERSION,
        "profile": profile.to_dict(),
        "observation_table": {
            "schema_id": MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID,
            "row_count": observation_count,
        },
        "split_assignment_table": {
            "schema_id": MINUTE_STATISTICS_SPLIT_SCHEMA_ID,
            "row_count": observation_count,
        },
        "reported": reported,
    }
    payload["artifact_hash"] = typed_canonical_hash(payload)
    return payload


def _reported_payload(
    observations: tuple[MinuteLabelReturn, ...],
    profile: MinuteStatisticsProfile,
) -> dict[str, object]:
    lag_floor, embargo_floor = derive_minute_overlap_floors(observations)
    issues: set[str] = set()
    if profile.hac_lag < lag_floor:
        issues.add("statistics.hac_below_floor")
    if profile.embargo_ns < embargo_floor:
        issues.add("split.embargo_below_floor")
    assignments = tuple(
        (item.observation_id, item.candidate_id, _split_observation(item, profile))
        for item in observations
    )
    observed_candidates = {item.candidate_id for item in observations}
    if observed_candidates != set(profile.trial_candidate_ids):
        issues.add("search.minute_trial_universe_incomplete")
    raw_p_values: list[float] = []
    estimates: list[float | None] = []
    sample_counts: list[int] = []
    for candidate_id in profile.trial_candidate_ids:
        test_rows = [
            item for item in observations
            if item.candidate_id == candidate_id and _split_observation(item, profile) == "test"
        ]
        sample_counts.append(len(test_rows))
        if len(test_rows) < profile.min_test_samples or profile.hac_lag >= len(test_rows):
            issues.add("statistics.minute_sample_invalid")
            estimates.append(None)
            raw_p_values.append(1.0)
            continue
        try:
            inference = newey_west_mean(
                [item.return_value for item in test_rows],
                lag=profile.hac_lag,
                overlap_source="minute_label_entry_exit_v1",
            )
        except StatisticsError:
            issues.add("statistics.minute_sample_invalid")
            estimates.append(None)
            raw_p_values.append(1.0)
        else:
            estimates.append(inference.estimate)
            raw_p_values.append(inference.p_value)
    family = TestFamilyManifest(
        "minute-trial-universe-v1", profile.trial_candidate_ids,
        "mean_minute_label_return", "two_sided", profile.alpha,
        "arbitrary", datetime.fromisoformat(profile.fixed_clock),
    )
    family.require_complete_results(profile.trial_candidate_ids)
    adjusted = adjust_p_values(
        raw_p_values,
        method=profile.multiple_testing_method,
        manifest=family,
    )
    trial_results = [
        {
            "candidate_id": candidate_id,
            "sample_count": sample_count,
            "estimate": estimate,
            "raw_p_value": raw_p,
            "adjusted_p_value": float(adjusted[index]),
            "passes_alpha": bool(adjusted[index] <= profile.alpha),
        }
        for index, (candidate_id, sample_count, estimate, raw_p) in enumerate(zip(
            profile.trial_candidate_ids, sample_counts, estimates, raw_p_values, strict=True,
        ))
    ]
    return {
        "derived_hac_lag_floor": lag_floor,
        "derived_embargo_ns_floor": embargo_floor,
        "split_assignments": [
            {"observation_id": observation_id, "candidate_id": candidate_id, "split": split}
            for observation_id, candidate_id, split in assignments
        ],
        "trial_results": trial_results,
        "trial_universe_hash": family.manifest_hash,
        "issue_codes": sorted(issues),
        "status": "pass" if not issues else "fail",
        "claim_ceiling": MINUTE_STATISTICS_CLAIM_CEILING,
    }


def build_minute_statistics_artifact(
    *,
    observations: Sequence[MinuteLabelReturn],
    profile: MinuteStatisticsProfile,
) -> dict[str, object]:
    ordered = tuple(sorted(observations))
    if len({(item.candidate_id, item.observation_id) for item in ordered}) != len(ordered):
        raise StatisticsError("分钟标签 candidate/observation 身份重复")
    return stream_minute_statistics_artifact(
        observations=ordered,
        profile=profile,
        emit_observation=lambda _item: None,
        emit_assignment=lambda _item: None,
    )


def recompute_minute_statistics_artifact(
    payload: Mapping[str, object],
    *,
    observations: Iterable[Mapping[str, object]],
    split_assignments: Iterable[Mapping[str, object]],
) -> dict[str, object]:
    expected = {
        "contract_version", "profile", "observation_table",
        "split_assignment_table", "reported", "artifact_hash",
    }
    if set(payload) != expected or payload.get("contract_version") != MINUTE_STATISTICS_ARTIFACT_VERSION:
        raise StatisticsError("分钟统计 artifact schema 或版本无效")
    original_hash = payload.get("artifact_hash")
    body = {key: value for key, value in payload.items() if key != "artifact_hash"}
    if original_hash != typed_canonical_hash(body):
        raise StatisticsError("分钟统计 artifact hash 漂移")
    raw_profile = payload.get("profile")
    if not isinstance(raw_profile, Mapping):
        raise StatisticsError("分钟统计 artifact 容器字段无效")
    declared_assignments = iter(split_assignments)
    assignment_count = 0

    def compare_assignment(expected_assignment: Mapping[str, object]) -> None:
        nonlocal assignment_count
        try:
            actual = next(declared_assignments)
        except StopIteration as exc:
            raise StatisticsError("分钟统计 split assignment 缺行") from exc
        if dict(actual) != dict(expected_assignment):
            raise StatisticsError("分钟统计 split assignment 与独立重算不一致")
        assignment_count += 1

    rebuilt = stream_minute_statistics_artifact(
        observations=(
            MinuteLabelReturn.from_dict(item)
            for item in observations
        ),
        profile=MinuteStatisticsProfile.from_dict(raw_profile),
        emit_observation=lambda _item: None,
        emit_assignment=compare_assignment,
    )
    try:
        next(declared_assignments)
    except StopIteration:
        pass
    else:
        raise StatisticsError("分钟统计 split assignment 多出行")
    if assignment_count != int(rebuilt["observation_table"]["row_count"]):
        raise StatisticsError("分钟统计 split assignment 行数不闭合")
    if rebuilt != dict(payload):
        raise StatisticsError("分钟统计上游 reported pass/split/trial 不能通过独立重算")
    return rebuilt


__all__ = [
    "MINUTE_STATISTICS_ARTIFACT_VERSION",
    "MINUTE_STATISTICS_CLAIM_CEILING",
    "MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID",
    "MINUTE_STATISTICS_PROFILE_VERSION",
    "MINUTE_STATISTICS_SPLIT_SCHEMA_ID",
    "MinuteLabelReturn",
    "MinuteStatisticsProfile",
    "build_minute_statistics_artifact",
    "derive_minute_overlap_floors",
    "derive_minute_trial_universe_hash",
    "recompute_minute_statistics_artifact",
    "stream_minute_statistics_artifact",
]
