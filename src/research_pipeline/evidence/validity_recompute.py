"""Verifier 从不可变 facts 工件独立重算研究有效性。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import re
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.validity_contracts import VALIDITY_FACTS_VERSION
from research_pipeline.platform.statistics_contracts import (
    DIAGNOSTIC_UPDATE,
    FAMILY_WIDE_CONFIRMATION,
    robust_statistics_conclusion_contract,
)
from .facets import CLAIM_LEVELS, weakest_claim_level

from .errors import EvidenceContractError
from .facets import strict_fields
from .holdout_verification import verify_holdout_access_payload
from .validity import (
    ClaimPolicy,
    VALIDITY_GATE_IDS,
    ValidityFinding,
    ValidityGateResult,
)
from .minute_validity import (
    recompute_minute_simulation_issues,
    recompute_minute_validity_issues,
)


VALIDITY_FACTS_ARTIFACT_TYPE = "research.validity-facts.v1"
VALIDITY_FACTS_PRODUCER_HASH = typed_canonical_hash({
    "producer_id": "research.validity-facts.verifier.v1",
    "contract_version": VALIDITY_FACTS_VERSION,
})
GATE_ALGORITHM_VERSIONS = {
    "data.pit": "verifier.data-pit.v1",
    "label.split": "verifier.label-split.v2",
    "search.holdout": "verifier.search-holdout.v1",
    "statistics": "verifier.statistics.v1",
    "financial.tradability": "verifier.financial-tradability.v2",
}


def build_validity_gate_input_hashes(
    *,
    facts_artifact_hash: str,
    catalog_hash: str,
    lineage_root: str,
    policy_hash: str,
) -> tuple[str, ...]:
    """构造 draft 与 Verifier 共享的门禁输入身份。"""
    return tuple(sorted({
        facts_artifact_hash,
        catalog_hash,
        lineage_root,
        policy_hash,
    }))


class _SimulationTimeSemanticsError(EvidenceContractError):
    """仿真时钟、可见性或 schema 不能独立复核。"""


class _SimulationCapacitySemanticsError(EvidenceContractError):
    """容量、成本模型或结论上限不能独立复核。"""


_SIMULATION_STABLE_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")


_POLICY_BUILDERS = {
    "research.observation.strict.v1": lambda: ClaimPolicy.build(
        requested_level="research_observation",
        allowed_not_applicable=(
            "label.split",
            "search.holdout",
            "statistics",
            "financial.tradability",
        ),
    ),
    "research.portfolio-simulation.strict.v1": lambda: ClaimPolicy.build(
        requested_level="portfolio_simulation_candidate",
    ),
    "research.tradable-simulation.strict.v1": lambda: ClaimPolicy.build(
        requested_level="tradable_simulation"
    ),
}


@dataclass(frozen=True)
class GateRecomputeRecord:
    gate_id: str
    algorithm_version: str
    input_hashes: tuple[str, ...]
    status: str
    findings: tuple[ValidityFinding, ...]
    result_hash: str

    def __post_init__(self) -> None:
        if self.gate_id not in VALIDITY_GATE_IDS:
            raise EvidenceContractError("GateRecomputeRecord gate_id 无效")
        if not self.algorithm_version or not self.input_hashes:
            raise EvidenceContractError("GateRecomputeRecord 算法或输入为空")
        for value in (*self.input_hashes, self.result_hash):
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
                raise EvidenceContractError("GateRecomputeRecord hash 无效")
        ValidityGateResult(
            self.gate_id,
            self.status,
            self.input_hashes,
            self.findings,
            self.result_hash,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id,
            "algorithm_version": self.algorithm_version,
            "input_hashes": list(self.input_hashes),
            "status": self.status,
            "findings": [item.to_dict() for item in self.findings],
            "result_hash": self.result_hash,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "GateRecomputeRecord":
        strict_fields(
            payload,
            {
                "gate_id", "algorithm_version", "input_hashes", "status",
                "findings", "result_hash",
            },
            "GateRecomputeRecord",
        )
        hashes = payload["input_hashes"]
        findings = payload["findings"]
        if (
            not isinstance(hashes, list)
            or not isinstance(findings, list)
            or any(not isinstance(item, Mapping) for item in findings)
            or any(
                set(item) != {"code", "severity", "message"}
                for item in findings
                if isinstance(item, Mapping)
            )
        ):
            raise EvidenceContractError(
                "GateRecomputeRecord input_hashes/findings 必须是列表"
            )
        return cls(
            str(payload["gate_id"]),
            str(payload["algorithm_version"]),
            tuple(str(item) for item in hashes),
            str(payload["status"]),
            tuple(
                ValidityFinding(
                    str(item["code"]),
                    str(item["severity"]),
                    str(item["message"]),
                )
                for item in findings
                if isinstance(item, Mapping)
            ),
            str(payload["result_hash"]),
        )


def load_claim_policy(policy_id: str) -> ClaimPolicy:
    builder = _POLICY_BUILDERS.get(policy_id)
    if builder is None:
        raise EvidenceContractError("Verifier claim policy 未登记")
    return builder()


def policy_id_for_claim(claim_level: str) -> str:
    matches = tuple(
        policy_id
        for policy_id, builder in _POLICY_BUILDERS.items()
        if builder().requested_level == claim_level
    )
    if len(matches) != 1:
        raise EvidenceContractError("claim level 没有唯一 Verifier policy")
    return matches[0]


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EvidenceContractError(f"validity facts {label} 必须是对象")
    return value


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise EvidenceContractError(f"validity facts {label} 必须是列表")
    return value


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _validate_analysis_semantics(payload: Mapping[str, object]) -> None:
    expected = {
        "decision_at", "features", "labels", "estimands", "hypotheses",
        "claim_ceiling", "semantics_hash", "contract_version",
    }
    if set(payload) != expected or payload.get("contract_version") != "research-semantics-v1":
        raise EvidenceContractError("analysis ResearchSemantics schema 无效")
    body = dict(payload)
    semantics_hash = body.pop("semantics_hash", None)
    if semantics_hash != typed_canonical_hash(body):
        raise EvidenceContractError("analysis ResearchSemantics hash 无效")
    try:
        decision_at = datetime.fromisoformat(str(payload["decision_at"]))
    except ValueError as exc:
        raise EvidenceContractError("analysis decision_at 无效") from exc
    if decision_at.utcoffset() is None:
        raise EvidenceContractError("analysis decision_at 缺少时区")
    collections = {
        name: _list(payload.get(name), name)
        for name in ("features", "labels", "estimands", "hypotheses")
    }
    if any(not values for values in collections.values()):
        raise EvidenceContractError("analysis semantics 集合不能为空")

    def verify_children(name: str, id_field: str, hash_field: str) -> dict[str, Mapping[str, object]]:
        result: dict[str, Mapping[str, object]] = {}
        observed_ids = []
        for raw in collections[name]:
            item = _mapping(raw, name)
            item_id = item.get(id_field)
            item_hash = item.get(hash_field)
            child = dict(item)
            child.pop(hash_field, None)
            if (
                not isinstance(item_id, str)
                or item_hash != typed_canonical_hash(child)
            ):
                raise EvidenceContractError(f"analysis {name} 子合同 hash 无效")
            result[item_id] = item
            observed_ids.append(item_id)
        if observed_ids != sorted(set(observed_ids)) or len(result) != len(observed_ids):
            raise EvidenceContractError(f"analysis {name} 身份不唯一或未排序")
        return result

    features = verify_children("features", "feature_id", "artifact_hash")
    labels = verify_children("labels", "label_id", "artifact_hash")
    estimands = verify_children("estimands", "estimand_id", "estimand_hash")
    hypotheses = verify_children("hypotheses", "hypothesis_id", "hypothesis_hash")
    entity_keys = set()
    for feature in features.values():
        window = _mapping(feature.get("time_window"), "feature.time_window")
        try:
            observation = datetime.fromisoformat(str(window["observation_at"]))
            available = datetime.fromisoformat(str(window["available_at"]))
            window_end = datetime.fromisoformat(str(window["window_end"]))
        except (KeyError, ValueError) as exc:
            raise EvidenceContractError("analysis Feature 时间无效") from exc
        if (
            any(value.utcoffset() is None for value in (observation, available, window_end))
            or window_end > observation
            or available > decision_at
        ):
            raise EvidenceContractError("analysis Feature 存在未来可见性")
        entity_keys.add(tuple(feature.get("entity_keys", ())))
    for label in labels.values():
        window = _mapping(label.get("time_window"), "label.time_window")
        try:
            observation = datetime.fromisoformat(str(window["observation_at"]))
            available = datetime.fromisoformat(str(window["available_at"]))
            window_start = datetime.fromisoformat(str(window["window_start"]))
            window_end = datetime.fromisoformat(str(window["window_end"]))
        except (KeyError, ValueError) as exc:
            raise EvidenceContractError("analysis Label 时间无效") from exc
        if (
            any(
                value.utcoffset() is None
                for value in (observation, available, window_start, window_end)
            )
            or not observation < window_start <= window_end <= available
            or observation > decision_at
            or label.get("revision_policy")
            not in {"as_published", "point_in_time", "current_snapshot"}
        ):
            raise EvidenceContractError("analysis Label 时间或修订政策无效")
        entity_keys.add(tuple(label.get("entity_keys", ())))
    if len(entity_keys) != 1:
        raise EvidenceContractError("analysis Feature/Label entity key 不一致")
    metric_refs = set()
    for estimand in estimands.values():
        label_id = estimand.get("label_id")
        refs = estimand.get("metric_refs")
        if (
            label_id not in labels
            or not isinstance(refs, list)
            or refs != sorted(set(str(item) for item in refs))
        ):
            raise EvidenceContractError("analysis Estimand 引用不闭合")
        metric_refs.update(str(item) for item in refs)
    primary_ceilings = []
    for hypothesis in hypotheses.values():
        estimand = estimands.get(str(hypothesis.get("estimand_id")))
        if estimand is None:
            raise EvidenceContractError("analysis Hypothesis 引用未知 Estimand")
        kind = hypothesis.get("hypothesis_kind")
        preregistration = hypothesis.get("preregistration_hash")
        primary_metric = hypothesis.get("primary_metric_ref")
        if kind == "primary":
            if (
                not _is_sha256(preregistration)
                or primary_metric not in estimand.get("metric_refs", ())
            ):
                raise EvidenceContractError("analysis primary Hypothesis 未闭合")
            primary_ceilings.append(str(hypothesis.get("claim_ceiling")))
        elif kind == "exploratory":
            if preregistration is not None or hypothesis.get("claim_ceiling") != "research_observation":
                raise EvidenceContractError("analysis exploratory Hypothesis 未降级")
        else:
            raise EvidenceContractError("analysis hypothesis_kind 无效")
    expected_ceiling = (
        "research_observation"
        if not primary_ceilings
        else weakest_claim_level(*primary_ceilings)
    )
    if payload.get("claim_ceiling") != expected_ceiling or not metric_refs:
        raise EvidenceContractError("analysis ResearchSemantics claim/metric 不闭合")


def _recompute_issue_map(
    facts: Mapping[str, object],
    *,
    bar_tca_expectations: Mapping[str, object] | None = None,
    require_bar_tca_oracle: bool = True,
    minute_statistics_observations: object = (),
    minute_statistics_split_assignments: object = (),
) -> dict[str, set[str]]:
    issues = {gate_id: set() for gate_id in VALIDITY_GATE_IDS}
    raw_analysis = facts.get("analysis_semantics")
    analysis_only = isinstance(raw_analysis, Mapping) and raw_analysis.get(
        "analysis_only"
    ) is True
    raw_observation = facts.get("observation_only")
    observation_mode = (
        raw_observation.get("mode")
        if isinstance(raw_observation, Mapping)
        else None
    )
    observation_only = (
        isinstance(raw_observation, Mapping)
        and observation_mode
        in {
            "dataset_manifest_observation_v1",
            "minute_manifest_observation_v1",
        }
    )
    raw_minute_intraday = facts.get("minute_intraday")
    minute_intraday = (
        isinstance(raw_minute_intraday, Mapping)
        and raw_minute_intraday.get("mode")
        == "minute_intraday_statistics_and_simulation_v1"
    )
    if minute_intraday and (
        set(raw_minute_intraday)
        != {
            "mode", "trial_universe_hash", "statistics_artifact_hash",
            "simulation_result_hash",
        }
        or any(
            not _is_sha256(raw_minute_intraday.get(field))
            for field in (
                "trial_universe_hash", "statistics_artifact_hash",
                "simulation_result_hash",
            )
        )
    ):
        issues["statistics"].add("statistics.minute_recompute_mismatch")
    if observation_only:
        common_invalid = (
            not isinstance(raw_observation.get("request_id"), str)
            or not raw_observation["request_id"]
            or not _is_sha256(raw_observation.get("dataset_reference_id"))
            or type(raw_observation.get("row_count")) is not int
            or raw_observation["row_count"] <= 0
        )
        if observation_mode == "minute_manifest_observation_v1":
            mode_invalid = (
                set(raw_observation)
                != {
                    "mode",
                    "request_id",
                    "dataset_reference_id",
                    "partition_count",
                    "row_count",
                    "metric_ref",
                    "observation_payload_hash",
                }
                or type(raw_observation.get("partition_count")) is not int
                or raw_observation["partition_count"] <= 0
                or raw_observation.get("metric_ref")
                != "minute.row_count@1.0.0"
                or not _is_sha256(
                    raw_observation.get("observation_payload_hash")
                )
            )
        else:
            mode_invalid = (
                set(raw_observation)
                != {
                    "mode",
                    "request_id",
                    "dataset_reference_id",
                    "manifest_hash",
                    "row_count",
                    "metric_ref",
                }
                or not _is_sha256(raw_observation.get("manifest_hash"))
                or raw_observation.get("metric_ref") != "data.row_count@1.0.0"
            )
        if common_invalid or mode_invalid:
            issues["statistics"].add("statistics.method_not_applicable")
    if analysis_only:
        raw_semantics = raw_analysis.get("research_semantics")
        if not isinstance(raw_semantics, Mapping):
            issues["label.split"].add("label.leakage")
        else:
            try:
                _validate_analysis_semantics(raw_semantics)
            except (EvidenceContractError, TypeError, ValueError):
                issues["label.split"].add("label.leakage")
            if (
                raw_analysis.get("semantics_hash")
                != raw_semantics.get("semantics_hash")
                or raw_analysis.get("effective_claim_ceiling")
                != "research_observation"
            ):
                issues["label.split"].add("label.leakage")
    pit = _mapping(facts["data_pit"], "data_pit")
    consumed_request_ids = pit.get("consumed_request_ids")
    input_ceilings = pit.get("input_claim_ceilings")
    effective_input_ceiling = pit.get("effective_claim_ceiling")
    if (
        not isinstance(consumed_request_ids, list)
        or not consumed_request_ids
        or any(
            not isinstance(request_id, str) or not request_id
            for request_id in consumed_request_ids
        )
        or consumed_request_ids != sorted(consumed_request_ids)
        or len(consumed_request_ids) != len(set(consumed_request_ids))
        or not isinstance(input_ceilings, Mapping)
        or not input_ceilings
        or set(consumed_request_ids) != set(input_ceilings)
        or any(
            not isinstance(request_id, str)
            or not request_id
            or value not in {
                "research_observation",
                "portfolio_simulation_candidate",
                "tradable_simulation",
            }
            for request_id, value in input_ceilings.items()
        )
        or effective_input_ceiling
        != weakest_claim_level(*(str(value) for value in input_ceilings.values()))
    ):
        issues["data.pit"].add("pit.availability_policy_missing")
    for raw in _list(pit.get("observations"), "observations"):
        item = _mapping(raw, "observation")
        if not all(type(item.get(field)) is int for field in ("available_at_ns", "decision_at_ns")) or item["available_at_ns"] > item["decision_at_ns"]:
            issues["data.pit"].add("pit.future_data")
        if not _is_sha256(item.get("source_revision_hash")):
            issues["data.pit"].add("pit.source_revision_missing")
        if not _is_sha256(item.get("availability_policy_hash")):
            issues["data.pit"].add("pit.availability_policy_missing")
    split = _mapping(facts["label_split"], "label_split")
    if observation_only:
        if split != {
            "applicability": "not_applicable",
            "reason": "manifest_observation_has_no_features_or_labels",
        }:
            issues["label.split"].add("label.leakage")
    elif minute_intraday:
        if (
            set(split)
            != {
                "mode", "train_end_ns", "validation_end_ns", "test_end_ns",
                "embargo_ns",
            }
            or split.get("mode")
            != "minute_intraday_observation_intervals_v1"
            or any(
                type(split.get(field)) is not int
                for field in (
                    "train_end_ns", "validation_end_ns", "test_end_ns",
                    "embargo_ns",
                )
            )
            or not 0 < split["train_end_ns"] < split["validation_end_ns"] < split["test_end_ns"]
            or split["embargo_ns"] < 0
        ):
            issues["label.split"].add("split.minute_recompute_mismatch")
    elif analysis_only:
        windows = _list(split.get("analysis_windows"), "analysis_windows")
        if not windows:
            issues["label.split"].add("label.leakage")
        fact_keys: set[tuple[str, str]] = set()
        for raw in windows:
            item = _mapping(raw, "analysis_window")
            feature_id = item.get("feature_id")
            label_id = item.get("label_id")
            key = (str(feature_id), str(label_id))
            values = tuple(
                item.get(field)
                for field in (
                    "feature_end_ns", "label_start_ns", "label_end_ns",
                    "label_available_at_ns",
                )
            )
            if (
                not isinstance(feature_id, str)
                or not feature_id
                or not isinstance(label_id, str)
                or not label_id
                or key in fact_keys
                or any(type(value) is not int for value in values)
                or not values[0] < values[1] <= values[2] <= values[3]
            ):
                issues["label.split"].add("label.leakage")
            fact_keys.add(key)
    else:
        for raw in _list(split.get("intervals"), "intervals"):
            item = _mapping(raw, "interval")
            values = tuple(item.get(field) for field in ("feature_end_ns", "label_start_ns", "train_label_end_ns", "evaluation_label_start_ns"))
            if any(type(value) is not int for value in values) or values[0] > values[1]:
                issues["label.split"].add("label.leakage")
            if any(type(value) is not int for value in values) or values[2] >= values[3]:
                issues["label.split"].add("split.overlap")
            if item.get("embargo_required") is True and item.get("embargo_applied") is not True:
                issues["label.split"].add("split.embargo_missing")

    statistics = _mapping(facts["statistics"], "statistics")
    search = _mapping(facts["search_holdout"], "search_holdout")
    planned = (
        []
        if observation_only
        else _list(search.get("planned_candidate_ids"), "planned_candidate_ids")
    )
    terminal = (
        []
        if observation_only
        else _list(search.get("terminal_candidate_ids"), "terminal_candidate_ids")
    )
    if not observation_only and (
        sorted(planned) != sorted(terminal) or len(planned) != len(set(planned))
    ):
        issues["search.holdout"].add("search.ledger_incomplete")
    if observation_only:
        if search != {
            "applicability": "not_applicable",
            "reason": "fixed_observation_has_no_candidates",
        }:
            issues["search.holdout"].add("search.ledger_incomplete")
    elif minute_intraday:
        if (
            set(search)
            != {
                "mode", "planned_candidate_ids", "terminal_candidate_ids",
                "trial_universe_hash",
            }
            or search.get("mode") != "minute_intraday_single_family_v1"
            or search.get("trial_universe_hash")
            != raw_minute_intraday.get("trial_universe_hash")
        ):
            issues["search.holdout"].add(
                "search.minute_trial_universe_incomplete"
            )
    elif analysis_only:
        metric_refs = statistics.get("metric_refs")
        metric_hashes = statistics.get("metric_artifact_hashes")
        if (
            not isinstance(metric_refs, list)
            or metric_refs != sorted(set(str(item) for item in metric_refs))
            or not isinstance(metric_hashes, Mapping)
            or set(metric_hashes) != set(metric_refs)
            or any(not _is_sha256(value) for value in metric_hashes.values())
            or not _is_sha256(statistics.get("artifact_semantic_hash"))
        ):
            issues["statistics"].add("statistics.method_not_applicable")
    else:
        accesses = _list(search.get("holdout_accesses"), "holdout_accesses")
        mode = search.get("mode")
        exposures = search.get("exposed_intervals")
        try:
            evaluation_start = date.fromisoformat(str(search["evaluation_start"]))
            evaluation_end = date.fromisoformat(str(search["evaluation_end"]))
            if evaluation_end < evaluation_start or not isinstance(exposures, list):
                raise EvidenceContractError("统计评价窗口或暴露区间无效")
            overlaps_exposure = False
            for raw in exposures:
                item = _mapping(raw, "exposed_interval")
                if set(item) != {"start", "end", "reason"} or not str(
                    item["reason"]
                ).strip():
                    raise EvidenceContractError("exposed interval schema 无效")
                exposed_start = date.fromisoformat(str(item["start"]))
                exposed_end = date.fromisoformat(str(item["end"]))
                if exposed_end < exposed_start:
                    raise EvidenceContractError("exposed interval 日期无效")
                overlaps_exposure = overlaps_exposure or (
                    max(evaluation_start, exposed_start)
                    <= min(evaluation_end, exposed_end)
                )
            if mode == DIAGNOSTIC_UPDATE:
                if accesses or not overlaps_exposure:
                    raise EvidenceContractError(
                        "diagnostic update 不得打开 holdout，且必须闭合已暴露重叠"
                    )
            elif mode == FAMILY_WIDE_CONFIRMATION:
                if len(accesses) != 1 or overlaps_exposure:
                    raise EvidenceContractError(
                        "family-wide confirmation 的 holdout 或暴露区间无效"
                    )
                _verify_holdout_access_payload(
                    _mapping(accesses[0], "holdout_access"),
                    statistics=statistics,
                    planned_candidate_ids=planned,
                )
            else:
                raise EvidenceContractError("统计评价模式无效")
        except (EvidenceContractError, KeyError, TypeError, ValueError):
            issues["search.holdout"].add("holdout.access_invalid")
        if len(accesses) > 1:
            issues["search.holdout"].add("holdout.reused")

    if not observation_only and (
        type(statistics.get("sample_count")) is not int
        or statistics["sample_count"] < 30
    ):
        issues["statistics"].add("statistics.small_sample")
    if observation_only:
        expected_statistics_fields = {
            "applicability",
            "reason",
            "sample_count",
            "metric_ref",
        }
        payload_hash_mismatch = False
        if observation_mode == "minute_manifest_observation_v1":
            expected_statistics_fields.add("observation_payload_hash")
            payload_hash_mismatch = (
                statistics.get("observation_payload_hash")
                != raw_observation.get("observation_payload_hash")
            )
        if (
            set(statistics) != expected_statistics_fields
            or statistics.get("applicability") != "not_applicable"
            or statistics.get("reason")
            != "descriptive_row_count_has_no_statistical_inference"
            or statistics.get("sample_count") != raw_observation.get("row_count")
            or statistics.get("metric_ref") != raw_observation.get("metric_ref")
            or payload_hash_mismatch
        ):
            issues["statistics"].add("statistics.method_not_applicable")
    elif minute_intraday:
        minute_statistics = statistics.get("minute_intraday")
        observation_table = (
            minute_statistics.get("observation_table")
            if isinstance(minute_statistics, Mapping)
            else None
        )
        if (
            statistics.get("method") != "minute_intraday_hac_v1"
            or not _is_sha256(statistics.get("artifact_manifest_hash"))
            or statistics.get("artifact_manifest_hash")
            != raw_minute_intraday.get("statistics_artifact_hash")
            or statistics.get("artifact_semantic_hash")
            != raw_minute_intraday.get("statistics_artifact_hash")
            or not isinstance(minute_statistics, Mapping)
            or statistics.get("sample_count")
            != (
                observation_table.get("row_count")
                if isinstance(observation_table, Mapping)
                else None
            )
        ):
            issues["statistics"].add("statistics.minute_recompute_mismatch")
    elif analysis_only:
        metric_refs = statistics.get("metric_refs")
        metric_hashes = statistics.get("metric_artifact_hashes")
        if (
            not isinstance(metric_refs, list)
            or metric_refs != sorted(set(str(item) for item in metric_refs))
            or not isinstance(metric_hashes, Mapping)
            or set(metric_hashes) != set(metric_refs)
            or any(not _is_sha256(value) for value in metric_hashes.values())
            or not _is_sha256(statistics.get("artifact_semantic_hash"))
        ):
            issues["statistics"].add("statistics.method_not_applicable")
    else:
        expected_conclusion = None
        try:
            expected_conclusion = robust_statistics_conclusion_contract(
                str(statistics.get("evaluation_mode"))
            )
        except ValueError:
            pass
        if (
            statistics.get("estimator_method") != "newey_west_mean_v1"
            or statistics.get("correlation_correction") != "hac"
            or statistics.get("cluster_dimensions") != 0
            or statistics.get("hac_kernel") != "bartlett"
            or type(statistics.get("sample_count")) is not int
            or type(statistics.get("hac_lag")) is not int
            or not 0 <= statistics["hac_lag"] < statistics["sample_count"]
            or statistics.get("confidence_interval_method")
            != "two_sided_normal_v1"
            or statistics.get("evaluation_mode") != search.get("mode")
            or expected_conclusion is None
            or any(
                statistics.get(field) != value
                for field, value in (expected_conclusion or {}).items()
            )
        ):
            issues["statistics"].add("statistics.method_not_applicable")
        if type(statistics.get("matrix_rank")) is not int or type(statistics.get("matrix_columns")) is not int or statistics["matrix_rank"] < statistics["matrix_columns"]:
            issues["statistics"].add("statistics.degenerate_matrix")
        if statistics.get("multiple_testing_method") not in {
            "holm",
            "benjamini-hochberg",
            "benjamini-yekutieli",
            "white-reality-check",
            "spa",
        }:
            issues["statistics"].add("multiple_testing.missing")
    minute_statistics = statistics.get("minute_intraday")
    if minute_statistics is not None:
        if not isinstance(minute_statistics, Mapping):
            issues["statistics"].add("statistics.minute_recompute_mismatch")
        else:
            minute_issues = recompute_minute_validity_issues(
                minute_statistics,
                observations=minute_statistics_observations,
                split_assignments=minute_statistics_split_assignments,
            )
            for gate_id, codes in minute_issues.items():
                issues[gate_id].update(codes)

    financial = _mapping(facts["financial_tradability"], "financial_tradability")
    if observation_only:
        if financial != {
            "applicability": "not_applicable",
            "reason": "observation_only_no_simulation",
        }:
            issues["financial.tradability"].add("tradability.missing")
        return issues
    if analysis_only:
        if financial != {
            "applicability": "not_applicable",
            "reason": "analysis_only_no_simulation",
        }:
            issues["financial.tradability"].add("tradability.missing")
        return issues
    for field, code in (
        ("rule_snapshot_hash", "financial.rule_missing"),
        ("order_audit_hash", "financial.order_audit_missing"),
        ("ledger_hash", "financial.ledger_missing"),
        ("tradability_hash", "tradability.missing"),
    ):
        if not _is_sha256(financial.get(field)):
            issues["financial.tradability"].add(code)
    minute_simulation = financial.get("minute_simulation")
    if minute_intraday:
        if not isinstance(minute_simulation, Mapping):
            issues["financial.tradability"].add(
                "financial.minute_simulation_unsupported"
            )
        else:
            if minute_simulation.get("result_hash") != raw_minute_intraday.get(
                "simulation_result_hash"
            ):
                issues["financial.tradability"].add(
                    "financial.minute_simulation_unsupported"
                )
            issues["financial.tradability"].update(
                recompute_minute_simulation_issues(minute_simulation)
            )
    bar_tca = financial.get("bar_tca")
    if bar_tca is not None:
        try:
            tca_mapping = _mapping(bar_tca, "bar_tca")
            required_tca_hashes = {
                "tca_result_hash", "tca_artifact_manifest_hash", "tca_policy_hash",
                "tca_input_hash", "tca_implementation_digest", "tca_rule_snapshot_hash",
                "tca_source_simulation_hash", "tca_source_ledger_hash",
                "tca_source_fill_manifest_hash",
            }
            expected_tca_fields = required_tca_hashes | {
                "claim_ceiling", "reconciliation_delta_units",
                "liquidity_attribution_status",
            }
            if set(tca_mapping) != expected_tca_fields:
                raise EvidenceContractError("Bar TCA facts schema 无效")
            if any(not _is_sha256(tca_mapping.get(field)) for field in required_tca_hashes):
                raise EvidenceContractError("Bar TCA hash 身份无效")
            if bar_tca_expectations is None and require_bar_tca_oracle:
                raise EvidenceContractError("Bar TCA 缺少 canonical ResultBundle 独立复验")
            if bar_tca_expectations is not None and any(
                tca_mapping.get(field) != bar_tca_expectations.get(field)
                for field in (
                    *sorted(required_tca_hashes),
                    "claim_ceiling",
                    "reconciliation_delta_units",
                    "liquidity_attribution_status",
                )
            ):
                raise EvidenceContractError("Bar TCA 自报事实与独立重算不一致")
            if tca_mapping.get("claim_ceiling") not in {"analysis_only", "sealed"}:
                raise EvidenceContractError("Bar TCA claim ceiling 无效")
            if tca_mapping.get("reconciliation_delta_units") != 0:
                raise EvidenceContractError("Bar TCA 费用恒等差额不为零")
            liquidity_status = tca_mapping.get("liquidity_attribution_status")
            if liquidity_status not in {"computed", "not_computable"}:
                raise EvidenceContractError("Bar TCA 流动性归因状态无效")
            if liquidity_status == "not_computable" and tca_mapping.get("claim_ceiling") != "analysis_only":
                raise EvidenceContractError("不可计算的 Bar TCA 流动性归因只能 analysis_only")
        except EvidenceContractError:
            issues["financial.tradability"].add("financial.bar_tca_invalid")
    elif bar_tca_expectations is not None:
        issues["financial.tradability"].add("financial.bar_tca_invalid")
    semantics = financial.get("simulation_semantics")
    if semantics is not None:
        try:
            semantics_mapping = _mapping(semantics, "simulation_semantics")
            summary = _mapping(semantics_mapping.get("summary"), "simulation_semantics.summary")
            if summary.get("cost_model_id") == "research-cost-assumption":
                _verify_research_cost_assumption(
                    financial.get("research_cost_assumption"),
                    financial.get("simulation_window"),
                )
            blocks = semantics_mapping.get("blocks")
            if not isinstance(blocks, list) or not blocks:
                raise EvidenceContractError("simulation semantics blocks 无效")
            observed_modes = set()
            observed_ceilings = set()
            observed_models = set()
            for block in blocks:
                block_mapping = _mapping(block, "simulation_semantics.block")
                if block_mapping.get("schema") != "research-simulation-semantics-v1":
                    raise EvidenceContractError("正式证据不接受 legacy simulation semantics")
                sessions = block_mapping.get("sessions")
                if not isinstance(sessions, list) or not sessions:
                    raise EvidenceContractError("simulation semantics sessions 无效")
                observed_modes.add(str(block_mapping.get("capacity_mode")))
                observed_ceilings.add(str(block_mapping.get("claim_ceiling")))
                block_sessions = []
                for item in sessions:
                    item_mapping = _mapping(item, "simulation_semantics.session")
                    body = {
                        key: value
                        for key, value in item_mapping.items()
                        if key not in {"session", "semantics_hash"}
                    }
                    _verify_simulation_semantics_payload(body)
                    if typed_canonical_hash(body) != item_mapping.get("semantics_hash"):
                        raise EvidenceContractError("simulation semantics hash 不一致")
                    session = str(item_mapping.get("session"))
                    try:
                        execution_session = datetime.fromisoformat(
                            str(body["execution_at"])
                        ).date().isoformat()
                    except ValueError as exc:
                        raise _SimulationTimeSemanticsError(
                            "simulation session 无效"
                        ) from exc
                    if session != execution_session:
                        raise _SimulationTimeSemanticsError(
                            "simulation session 与 execution_at 日期不一致"
                        )
                    block_sessions.append(session)
                    observed_models.add(tuple(
                        str(body[field])
                        for field in (
                            "capacity_model_id", "capacity_model_version",
                            "cost_model_id", "cost_model_version",
                        )
                    ))
                if block_sessions != sorted(set(block_sessions)):
                    raise _SimulationTimeSemanticsError(
                        "simulation sessions 必须唯一并规范排序"
                    )
            if observed_modes != {str(summary.get("capacity_mode"))}:
                issues["financial.tradability"].add("financial.capacity_semantics_invalid")
            if observed_ceilings != {str(summary.get("claim_ceiling"))}:
                issues["financial.tradability"].add("financial.capacity_semantics_invalid")
            summary_model = tuple(
                str(summary.get(field))
                for field in (
                    "capacity_model_id", "capacity_model_version",
                    "cost_model_id", "cost_model_version",
                )
            )
            if observed_models != {summary_model}:
                issues["financial.tradability"].add(
                    "financial.capacity_semantics_invalid"
                )
        except _SimulationCapacitySemanticsError:
            issues["financial.tradability"].add("financial.capacity_semantics_invalid")
        except (EvidenceContractError, TypeError, ValueError):
            issues["financial.tradability"].add("financial.time_semantics_invalid")
    return issues


def _verify_holdout_access_payload(
    payload: Mapping[str, object],
    *,
    statistics: Mapping[str, object],
    planned_candidate_ids: list[object],
) -> None:
    if (
        not _is_sha256(statistics.get("artifact_manifest_hash"))
        or not _is_sha256(statistics.get("artifact_semantic_hash"))
    ):
        raise EvidenceContractError("holdout 统计工件身份无效")
    verify_holdout_access_payload(
        payload,
        expected_result_hash=str(statistics["artifact_semantic_hash"]),
        expected_sample_count=int(statistics["sample_count"]),
        expected_candidate_ids=tuple(str(item) for item in planned_candidate_ids),
    )


def _verify_simulation_semantics_payload(payload: Mapping[str, object]) -> None:
    expected = {
        "input_availability", "signal_at", "decision_at", "order_submitted_at",
        "execution_at", "valuation_at", "return_start_at", "return_end_at",
        "timezone", "calendar_id", "capacity_mode", "capacity_model_id",
        "capacity_model_version", "cost_model_id", "cost_model_version",
        "claim_ceiling", "contract_version",
    }
    inputs = payload.get("input_availability")
    if set(payload) != expected or not isinstance(inputs, list) or not inputs:
        raise _SimulationTimeSemanticsError("simulation semantics schema 无效")
    if payload.get("timezone") != "Asia/Shanghai" or payload.get(
        "contract_version"
    ) != "research-simulation-semantics-v1":
        raise _SimulationTimeSemanticsError("simulation semantics timezone/version 无效")
    times = {
        field: datetime.fromisoformat(str(payload[field]))
        for field in (
            "signal_at", "decision_at", "order_submitted_at", "execution_at",
            "valuation_at", "return_start_at", "return_end_at",
        )
    }
    if any(value.utcoffset() != timedelta(hours=8) for value in times.values()):
        raise _SimulationTimeSemanticsError(
            "simulation semantics 时间必须使用 Asia/Shanghai 偏移"
        )
    if not (
        times["signal_at"] <= times["decision_at"]
        <= times["order_submitted_at"] <= times["execution_at"]
        <= times["valuation_at"] <= times["return_start_at"]
        < times["return_end_at"]
    ):
        raise _SimulationTimeSemanticsError("simulation semantics 时间顺序无效")
    stage_times = {
        "decision": times["decision_at"],
        "execution": times["execution_at"],
        "valuation": times["valuation_at"],
    }
    input_ids = []
    for item in inputs:
        item_mapping = _mapping(item, "simulation input availability")
        if set(item_mapping) != {
            "input_id", "source_hash", "available_at", "use_stage", "contract_version"
        }:
            raise _SimulationTimeSemanticsError(
                "simulation input availability schema 无效"
            )
        if item_mapping["contract_version"] != "research-simulation-input-availability-v1":
            raise _SimulationTimeSemanticsError(
                "simulation input availability 版本无效"
            )
        stage = str(item_mapping["use_stage"])
        available = datetime.fromisoformat(str(item_mapping["available_at"]))
        if (
            stage not in stage_times
            or available.utcoffset() != timedelta(hours=8)
            or available > stage_times[stage]
        ):
            raise _SimulationTimeSemanticsError("simulation input 在使用时尚不可见")
        if not _is_sha256(item_mapping.get("source_hash")):
            raise _SimulationTimeSemanticsError("simulation input source hash 无效")
        if not _SIMULATION_STABLE_ID.fullmatch(str(item_mapping["input_id"])):
            raise _SimulationTimeSemanticsError("simulation input_id 无效")
        input_ids.append(str(item_mapping["input_id"]))
    if input_ids != sorted(set(input_ids)):
        raise _SimulationTimeSemanticsError(
            "simulation input availability 未规范排序"
        )
    required_input_ids = {
        "execution.open", "market.rule", "signal.target", "valuation.open"
    }
    if not required_input_ids <= set(input_ids):
        raise _SimulationTimeSemanticsError(
            "simulation semantics 缺少基础输入可见性"
        )
    for field in (
        "calendar_id", "capacity_model_id", "capacity_model_version",
        "cost_model_id", "cost_model_version",
    ):
        if not _SIMULATION_STABLE_ID.fullmatch(str(payload[field])):
            raise _SimulationCapacitySemanticsError(
                f"simulation {field} 不是稳定 ID"
            )
    capacity_mode = str(payload["capacity_mode"])
    expected_ceiling = {
        "modeled": "liquidity_modeled",
        "assumed_unbounded": "research_only",
        "unknown": "no_liquidity_claim",
    }.get(capacity_mode)
    if expected_ceiling is None or payload["claim_ceiling"] != expected_ceiling:
        raise _SimulationCapacitySemanticsError(
            "simulation capacity claim ceiling 无效"
        )
    if capacity_mode == "modeled" and payload["capacity_model_id"] == "none":
        raise _SimulationCapacitySemanticsError("modeled capacity 缺少真实模型")
    if (
        capacity_mode == "assumed_unbounded"
        and payload["capacity_model_id"] == "none"
    ):
        raise _SimulationCapacitySemanticsError(
            "assumed_unbounded 缺少假设模型身份"
        )
    if capacity_mode == "unknown" and payload["capacity_model_id"] != "none":
        raise _SimulationCapacitySemanticsError("unknown capacity 不得伪装模型")
    if capacity_mode == "modeled" and "capacity.visible" not in input_ids:
        raise _SimulationCapacitySemanticsError(
            "modeled capacity 缺少可见性输入"
        )


def _verify_research_cost_assumption(
    raw_assumption: object,
    raw_window: object,
) -> None:
    assumption = _mapping(raw_assumption, "research_cost_assumption")
    window = _mapping(raw_window, "simulation_window")
    expected = {
        "contract_version", "assumption_id", "currency", "rate_unit",
        "minimum_fee_unit", "slippage_unit", "applicable_start", "applicable_end",
        "commission_ppm", "min_commission_units", "sell_tax_ppm",
        "transfer_fee_ppm", "slippage_per_share_units",
    }
    if set(assumption) != expected or set(window) != {"start", "end"}:
        raise _SimulationCapacitySemanticsError("研究费用假设 schema 无效")
    if (
        assumption.get("contract_version") != "research-cost-assumption-v1"
        or not isinstance(assumption.get("assumption_id"), str)
        or not str(assumption["assumption_id"]).strip()
        or assumption.get("currency") != "CNY"
        or assumption.get("rate_unit") != "ppm_of_notional"
        or assumption.get("minimum_fee_unit") != "CNY_cent"
        or assumption.get("slippage_unit") != "CNY_cent_per_share"
    ):
        raise _SimulationCapacitySemanticsError("研究费用假设身份或单位无效")
    if any(
        type(assumption.get(field)) is not int or int(assumption[field]) < 0
        for field in (
            "commission_ppm", "min_commission_units", "sell_tax_ppm",
            "transfer_fee_ppm", "slippage_per_share_units",
        )
    ):
        raise _SimulationCapacitySemanticsError("研究费用假设数值无效")
    try:
        applicable_start = date.fromisoformat(str(assumption["applicable_start"]))
        applicable_end = date.fromisoformat(str(assumption["applicable_end"]))
        simulation_start = date.fromisoformat(str(window["start"]))
        simulation_end = date.fromisoformat(str(window["end"]))
    except ValueError as exc:
        raise _SimulationCapacitySemanticsError("研究费用假设日期无效") from exc
    if (
        simulation_start > simulation_end
        or applicable_start > simulation_start
        or applicable_end < simulation_end
    ):
        raise _SimulationCapacitySemanticsError("研究费用假设未覆盖完整研究窗口")


def simulation_claim_ceiling_from_facts(facts: Mapping[str, object]) -> str:
    """容量语义缺失或未知时只允许最小研究观察结论。"""

    pit = facts.get("data_pit")
    input_ceiling = (
        pit.get("effective_claim_ceiling")
        if isinstance(pit, Mapping)
        else "research_observation"
    )
    if input_ceiling not in CLAIM_LEVELS:
        input_ceiling = "research_observation"

    analysis = facts.get("analysis_semantics")
    if isinstance(analysis, Mapping) and analysis.get("analysis_only") is True:
        ceiling = analysis.get("effective_claim_ceiling")
        domain_ceiling = ceiling if ceiling in CLAIM_LEVELS else "research_observation"
        return weakest_claim_level(str(input_ceiling), str(domain_ceiling))

    financial = facts.get("financial_tradability")
    if not isinstance(financial, Mapping):
        return weakest_claim_level(str(input_ceiling), "research_observation")
    semantics = financial.get("simulation_semantics")
    if not isinstance(semantics, Mapping):
        return weakest_claim_level(str(input_ceiling), "research_observation")
    summary = semantics.get("summary")
    if not isinstance(summary, Mapping):
        return weakest_claim_level(str(input_ceiling), "research_observation")
    mapping = {
        "no_liquidity_claim": "research_observation",
        "research_only": "portfolio_simulation_candidate",
        "liquidity_modeled": "tradable_simulation",
    }
    return weakest_claim_level(
        str(input_ceiling),
        mapping.get(str(summary.get("claim_ceiling")), "research_observation"),
    )


def recompute_gate_results(
    facts: Mapping[str, object],
    *,
    input_hashes: tuple[str, ...],
    bar_tca_expectations: Mapping[str, object] | None = None,
    require_bar_tca_oracle: bool = True,
    minute_statistics_observations: object = (),
    minute_statistics_split_assignments: object = (),
) -> tuple[ValidityGateResult, ...]:
    """从 facts 重算门禁；只有未签名 draft 阶段可延后独立 TCA oracle。"""
    normalized_inputs = tuple(sorted(input_hashes))
    issue_map = _recompute_issue_map(
        facts,
        bar_tca_expectations=bar_tca_expectations,
        require_bar_tca_oracle=require_bar_tca_oracle,
        minute_statistics_observations=minute_statistics_observations,
        minute_statistics_split_assignments=minute_statistics_split_assignments,
    )
    analysis_only = isinstance(facts.get("analysis_semantics"), Mapping) and facts[
        "analysis_semantics"
    ].get("analysis_only") is True
    observation_only = (
        isinstance(facts.get("observation_only"), Mapping)
        and facts["observation_only"].get("mode")
        in {
            "dataset_manifest_observation_v1",
            "minute_manifest_observation_v1",
        }
    )
    return tuple(
        ValidityGateResult.build(
            gate_id=gate_id,
            input_hashes=normalized_inputs,
            issue_codes=tuple(sorted(issue_map[gate_id])),
            not_applicable=(
                not issue_map[gate_id]
                and (
                    (analysis_only and gate_id == "financial.tradability")
                    or (
                        (
                            observation_only
                        )
                        and gate_id
                        in {
                            "label.split",
                            "search.holdout",
                            "statistics",
                            "financial.tradability",
                        }
                    )
                )
            ),
        )
        for gate_id in VALIDITY_GATE_IDS
    )


__all__ = [
    "GATE_ALGORITHM_VERSIONS", "VALIDITY_FACTS_ARTIFACT_TYPE",
    "VALIDITY_FACTS_PRODUCER_HASH", "VALIDITY_FACTS_VERSION",
    "GateRecomputeRecord",
    "load_claim_policy",
    "recompute_gate_results", "simulation_claim_ceiling_from_facts",
]
