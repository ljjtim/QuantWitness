"""正式算子图的 validity facts 构建。"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping

from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.validity_recompute import (
    VALIDITY_FACTS_VERSION,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.research.statistics import (
    derive_minute_trial_universe_hash,
    recompute_minute_statistics_artifact,
)

from .validity_facts_common import (
    _as_of_ns,
    _data_pit_facts,
    _datetime_ns,
    _sha256_text,
    _source_revision_hash,
)


def build_minute_observation_validity_facts(
    *,
    admitted_plans: Mapping[str, object],
    data_bundle: Mapping[str, object],
    observation: Mapping[str, object],
    fixed_clock: str,
) -> dict[str, object]:
    """为纯分钟观察生成可独立重算的事实，不虚构标签、搜索、推断或仿真。"""

    references = data_bundle.get("references")
    if not isinstance(references, Mapping) or not set(admitted_plans) <= set(references):
        raise EvidenceContractError("分钟观察 validity 缺少正式输入的 data reference")
    request_id = observation.get("request_id")
    row_count = observation.get("row_count")
    partition_count = observation.get("partition_count")
    dataset_reference_id = observation.get("dataset_reference_id")
    if (
        not isinstance(request_id, str)
        or request_id not in admitted_plans
        or type(row_count) is not int
        or row_count <= 0
        or type(partition_count) is not int
        or partition_count <= 0
        or not _sha256_text(dataset_reference_id)
    ):
        raise EvidenceContractError("分钟观察 validity 输入无效")
    decision_at_ns = _datetime_ns(datetime.fromisoformat(fixed_clock))
    observations = []
    for current_request_id, plan in sorted(admitted_plans.items()):
        query = getattr(plan, "query", None)
        as_of = getattr(query, "as_of", None)
        if as_of is None:
            raise EvidenceContractError("分钟观察 validity 缺少 QueryIR as_of")
        observations.append({
            "available_at_ns": _as_of_ns(str(as_of), fixed_clock=fixed_clock),
            "decision_at_ns": decision_at_ns,
            "source_revision_hash": _source_revision_hash(
                references[current_request_id]
            ),
            "availability_policy_hash": str(
                getattr(plan, "availability_policy_hash")
            ),
        })
    observation_payload_hash = typed_canonical_hash(dict(observation))
    return {
        "contract_version": VALIDITY_FACTS_VERSION,
        "observation_only": {
            "mode": "minute_manifest_observation_v1",
            "request_id": request_id,
            "dataset_reference_id": dataset_reference_id,
            "partition_count": partition_count,
            "row_count": row_count,
            "metric_ref": "minute.row_count@1.0.0",
            "observation_payload_hash": observation_payload_hash,
        },
        "data_pit": _data_pit_facts(admitted_plans, observations),
        "label_split": {
            "applicability": "not_applicable",
            "reason": "manifest_observation_has_no_features_or_labels",
        },
        "search_holdout": {
            "applicability": "not_applicable",
            "reason": "fixed_observation_has_no_candidates",
        },
        "statistics": {
            "applicability": "not_applicable",
            "reason": "descriptive_row_count_has_no_statistical_inference",
            "sample_count": row_count,
            "metric_ref": "minute.row_count@1.0.0",
            "observation_payload_hash": observation_payload_hash,
        },
        "financial_tradability": {
            "applicability": "not_applicable",
            "reason": "observation_only_no_simulation",
        },
    }


def build_dataset_observation_validity_facts(
    *,
    admitted_plans: Mapping[str, object],
    data_bundle: Mapping[str, object],
    observations: object,
    fixed_clock: str,
) -> dict[str, object]:
    """为单一普通列式请求生成描述性观察 facts，不聚合多个数据集。"""

    references = data_bundle.get("references")
    if (
        not isinstance(references, Mapping)
        or len(admitted_plans) != 1
        or not set(admitted_plans) <= set(references)
        or not isinstance(observations, list)
        or len(observations) != 1
        or not isinstance(observations[0], Mapping)
    ):
        raise EvidenceContractError("日频数据观察 validity 要求唯一闭合请求")
    observation = observations[0]
    expected_fields = {
        "contract_version",
        "request_id",
        "dataset_reference_id",
        "manifest_hash",
        "row_count",
        "metric_ref",
        "sample_start",
        "sample_end",
    }


    request_id = observation.get("request_id")
    row_count = observation.get("row_count")
    if (
        set(observation) != expected_fields
        or observation.get("contract_version")
        != "dataset-manifest-observation-v1"
        or not isinstance(request_id, str)
        or request_id not in admitted_plans
        or type(row_count) is not int
        or row_count <= 0
        or observation.get("metric_ref") != "data.row_count@1.0.0"
        or not _sha256_text(observation.get("dataset_reference_id"))
        or not _sha256_text(observation.get("manifest_hash"))
    ):
        raise EvidenceContractError("日频数据观察 validity 输入无效")
    reference = references[request_id]
    if (
        not isinstance(reference, Mapping)
        or reference.get("physical_snapshot_id")
        != observation.get("dataset_reference_id")
        or reference.get("manifest_hash") != observation.get("manifest_hash")
    ):
        raise EvidenceContractError("日频数据观察与 DatasetArtifactRef 不一致")
    plan = admitted_plans[request_id]
    query = getattr(plan, "query", None)
    time_range = getattr(query, "time_range", None)
    as_of = getattr(query, "as_of", None)
    if (
        time_range is None
        or as_of is None
        or observation.get("sample_start") != time_range.start.isoformat()
        or observation.get("sample_end") != time_range.end.isoformat()
    ):
        raise EvidenceContractError("日频数据观察与已准入 QueryIR 不一致")
    return {
        "contract_version": VALIDITY_FACTS_VERSION,
        "observation_only": {
            "mode": "dataset_manifest_observation_v1",
            "request_id": request_id,
            "dataset_reference_id": observation["dataset_reference_id"],
            "manifest_hash": observation["manifest_hash"],
            "row_count": row_count,
            "metric_ref": "data.row_count@1.0.0",
        },
        "data_pit": _data_pit_facts(admitted_plans, [{
            "available_at_ns": _as_of_ns(str(as_of), fixed_clock=fixed_clock),
            "decision_at_ns": _datetime_ns(datetime.fromisoformat(fixed_clock)),
            "source_revision_hash": _source_revision_hash(reference),
            "availability_policy_hash": str(
                getattr(plan, "availability_policy_hash")
            ),
        }]),
        "label_split": {
            "applicability": "not_applicable",
            "reason": "manifest_observation_has_no_features_or_labels",
        },
        "search_holdout": {
            "applicability": "not_applicable",
            "reason": "fixed_observation_has_no_candidates",
        },
        "statistics": {
            "applicability": "not_applicable",
            "reason": "descriptive_row_count_has_no_statistical_inference",
            "sample_count": row_count,
            "metric_ref": "data.row_count@1.0.0",
        },
        "financial_tradability": {
            "applicability": "not_applicable",
            "reason": "observation_only_no_simulation",
        },
    }


def build_minute_intraday_validity_facts(
    *,
    admitted_plans: Mapping[str, object],
    data_bundle: Mapping[str, object],
    simulation: Mapping[str, object],
    statistics: Mapping[str, object],
    statistic_observations: Iterable[Mapping[str, object]],
    statistic_split_assignments: Iterable[Mapping[str, object]],
    fixed_clock: str,
) -> dict[str, object]:
    """从真实分钟输入、统计和仿真载荷生成完整 validity facts。"""

    references = data_bundle.get("references")
    if not isinstance(references, Mapping) or not set(admitted_plans) <= set(references):
        raise EvidenceContractError("分钟 validity 缺少正式输入的 data reference")
    decision_at_ns = _datetime_ns(datetime.fromisoformat(fixed_clock))
    observations = []
    for request_id, plan in sorted(admitted_plans.items()):
        query = getattr(plan, "query", None)
        as_of = getattr(query, "as_of", None)
        if as_of is None:
            raise EvidenceContractError("分钟 validity 缺少 QueryIR as_of")
        observations.append({
            "available_at_ns": _as_of_ns(str(as_of), fixed_clock=fixed_clock),
            "decision_at_ns": decision_at_ns,
            "source_revision_hash": _source_revision_hash(references[request_id]),
            "availability_policy_hash": str(
                getattr(plan, "availability_policy_hash")
            ),
        })

    try:
        rebuilt_statistics = recompute_minute_statistics_artifact(
            statistics,
            observations=statistic_observations,
            split_assignments=statistic_split_assignments,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceContractError("分钟 validity 的 statistics 身份无效") from exc
    profile = rebuilt_statistics.get("profile")
    observation_table = rebuilt_statistics.get("observation_table")
    reported = rebuilt_statistics.get("reported")
    if (
        not isinstance(profile, Mapping)
        or not isinstance(observation_table, Mapping)
        or not isinstance(reported, Mapping)
    ):
        raise EvidenceContractError("分钟 validity 的 statistics 载荷不完整")
    raw_candidates = profile.get("trial_candidate_ids")
    if (
        not isinstance(raw_candidates, list)
        or any(not isinstance(item, str) for item in raw_candidates)
    ):
        raise EvidenceContractError("分钟 validity 的候选集合无效")
    if profile.get("fixed_clock") != fixed_clock:
        raise EvidenceContractError("分钟 validity 的统计时钟与运行时钟不一致")
    planned_candidates = sorted(set(raw_candidates))
    raw_trial_results = reported.get("trial_results")
    if not isinstance(raw_trial_results, list):
        raise EvidenceContractError("分钟 validity 缺少实际 trial 结果")
    terminal_candidates = sorted({
        str(item.get("candidate_id"))
        for item in raw_trial_results
        if isinstance(item, Mapping) and isinstance(item.get("candidate_id"), str)
    })
    if len(terminal_candidates) != len(raw_trial_results):
        raise EvidenceContractError("分钟 validity 的 terminal trial 身份无效")
    trial_universe_hash = derive_minute_trial_universe_hash(
        candidate_ids=tuple(planned_candidates),
        alpha=float(profile["alpha"]),
        fixed_clock=fixed_clock,
    )
    if reported.get("trial_universe_hash") != trial_universe_hash:
        raise EvidenceContractError("分钟 validity 的 trial universe 身份无效")

    if simulation.get("contract_version") != "minute-simulation-result-v2":
        raise EvidenceContractError("分钟 validity 的 simulation 合同无效")
    simulation_body = {
        key: value for key, value in simulation.items() if key != "result_hash"
    }
    if simulation.get("result_hash") != typed_canonical_hash(simulation_body):
        raise EvidenceContractError("分钟 validity 的 simulation 身份无效")
    outcomes = simulation.get("outcomes")
    ledger_hashes = simulation.get("ledger_hashes")
    if not isinstance(outcomes, list) or not isinstance(ledger_hashes, Mapping):
        raise EvidenceContractError("分钟 validity 的 simulation 载荷不完整")
    for field in ("rule_bundle_hash", "policy_hash", "result_hash"):
        if not _sha256_text(simulation.get(field)):
            raise EvidenceContractError(f"分钟 validity 缺少 simulation {field}")

    return {
        "contract_version": VALIDITY_FACTS_VERSION,
        "minute_intraday": {
            "mode": "minute_intraday_statistics_and_simulation_v1",
            "trial_universe_hash": str(trial_universe_hash),
            "statistics_artifact_hash": str(rebuilt_statistics["artifact_hash"]),
            "simulation_result_hash": str(simulation["result_hash"]),
        },
        "data_pit": _data_pit_facts(admitted_plans, observations),
        "label_split": {
            "mode": "minute_intraday_observation_intervals_v1",
            "train_end_ns": int(profile["train_end_ns"]),
            "validation_end_ns": int(profile["validation_end_ns"]),
            "test_end_ns": int(profile["test_end_ns"]),
            "embargo_ns": int(profile["embargo_ns"]),
        },
        "search_holdout": {
            "mode": "minute_intraday_single_family_v1",
            "planned_candidate_ids": planned_candidates,
            "terminal_candidate_ids": terminal_candidates,
            "trial_universe_hash": str(trial_universe_hash),
        },
        "statistics": {
            "sample_count": int(observation_table["row_count"]),
            "method": "minute_intraday_hac_v1",
            "artifact_manifest_hash": str(rebuilt_statistics["artifact_hash"]),
            "artifact_semantic_hash": str(rebuilt_statistics["artifact_hash"]),
            "minute_intraday": dict(rebuilt_statistics),
        },
        "financial_tradability": {
            "rule_snapshot_hash": str(simulation["rule_bundle_hash"]),
            "order_audit_hash": typed_canonical_hash(outcomes),
            "ledger_hash": typed_canonical_hash(dict(ledger_hashes)),
            "tradability_hash": str(simulation["result_hash"]),
            "minute_simulation": dict(simulation),
        },
    }


__all__ = [
    "build_dataset_observation_validity_facts",
    "build_minute_intraday_validity_facts",
    "build_minute_observation_validity_facts",
]
