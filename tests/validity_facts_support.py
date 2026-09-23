"""Validity Verifier 测试使用的完整通过样本。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.statistics_contracts import (
    FAMILY_WIDE_CONFIRMATION,
    robust_statistics_conclusion_contract,
)
from research_pipeline.platform.validity_contracts import VALIDITY_FACTS_VERSION


def default_passing_validity_facts(
    *,
    request_ids: tuple[str, ...] = ("prices",),
) -> dict[str, object]:
    """构造测试使用的完整通过 facts；生产 Verifier 仍从落盘工件重算。"""
    normalized_request_ids = tuple(sorted(request_ids))
    if (
        not normalized_request_ids
        or len(normalized_request_ids) != len(set(normalized_request_ids))
        or any(not request_id for request_id in normalized_request_ids)
    ):
        raise ValueError("默认 validity facts 的 request_ids 必须非空、唯一")
    inputs = [
        {
            "input_id": input_id,
            "source_hash": source_hash,
            "available_at": available_at,
            "use_stage": use_stage,
            "contract_version": "research-simulation-input-availability-v1",
        }
        for input_id, source_hash, available_at, use_stage in (
            ("capacity.visible", "a" * 64, "2024-01-02T09:30:00+08:00", "execution"),
            ("execution.open", "8" * 64, "2024-01-02T09:30:00+08:00", "execution"),
            ("market.rule", "9" * 64, "2020-01-01T00:00:00+08:00", "decision"),
            ("signal.target", "7" * 64, "2024-01-01T15:00:00+08:00", "decision"),
            ("valuation.open", "8" * 64, "2024-01-02T09:30:00+08:00", "valuation"),
        )
    ]
    semantics = {
        "input_availability": inputs,
        "signal_at": "2024-01-01T15:00:00+08:00",
        "decision_at": "2024-01-01T15:00:00+08:00",
        "order_submitted_at": "2024-01-02T09:25:00+08:00",
        "execution_at": "2024-01-02T09:30:00+08:00",
        "valuation_at": "2024-01-02T09:30:00+08:00",
        "return_start_at": "2024-01-02T09:30:00+08:00",
        "return_end_at": "2024-01-03T09:30:00+08:00",
        "timezone": "Asia/Shanghai",
        "calendar_id": "cn.exchange.calendar.v1",
        "capacity_mode": "modeled",
        "capacity_model_id": "fixture-visible-capacity",
        "capacity_model_version": "v1",
        "cost_model_id": "fixture-cost",
        "cost_model_version": "v1",
        "claim_ceiling": "liquidity_modeled",
        "contract_version": "research-simulation-semantics-v1",
    }
    block_semantics = {
        "schema": "research-simulation-semantics-v1",
        "capacity_mode": "modeled",
        "claim_ceiling": "liquidity_modeled",
        "sessions": [{
            "session": "2024-01-02",
            **semantics,
            "semantics_hash": typed_canonical_hash(semantics),
        }],
    }
    statistics = {
        "sample_count": 100,
        "estimator_method": "newey_west_mean_v1",
        "correlation_correction": "hac",
        "cluster_dimensions": 0,
        "matrix_rank": 3,
        "matrix_columns": 3,
        "confidence_level": 0.95,
        "confidence_interval_method": "two_sided_normal_v1",
        "hac_kernel": "bartlett",
        "hac_lag_policy": "floor_4_n100_pow_2_9_clamped_v1",
        "hac_lag": 4,
        "multiple_testing_policy": "benjamini_yekutieli_v1",
        "multiple_testing_method": "benjamini-yekutieli",
        "evaluation_mode": FAMILY_WIDE_CONFIRMATION,
        **robust_statistics_conclusion_contract(FAMILY_WIDE_CONFIRMATION),
        "selection_bias_methods": ["dsr", "pbo", "spa"],
        "bootstrap_method": "stationary_bootstrap_v1",
        "bootstrap_seed": 7,
        "bootstrap_derived_seed": 11,
        "bootstrap_repetitions": 1000,
        "bootstrap_block_length": 20,
        "pbo_blocks": 8,
        "artifact_manifest_hash": "b" * 64,
        "artifact_semantic_hash": "c" * 64,
    }
    return {
        "contract_version": VALIDITY_FACTS_VERSION,
        "data_pit": {
            "observations": [{
                "available_at_ns": 100,
                "decision_at_ns": 100,
                "source_revision_hash": "1" * 64,
                "availability_policy_hash": "2" * 64,
            }],
            "consumed_request_ids": list(normalized_request_ids),
            "input_claim_ceilings": {
                request_id: "tradable_simulation"
                for request_id in normalized_request_ids
            },
            "effective_claim_ceiling": "tradable_simulation",
        },
        "label_split": {
            "intervals": [{
                "feature_end_ns": 100,
                "label_start_ns": 101,
                "train_label_end_ns": 200,
                "evaluation_label_start_ns": 202,
                "embargo_required": True,
                "embargo_applied": True,
            }],
        },
        "search_holdout": {
            "mode": FAMILY_WIDE_CONFIRMATION,
            "planned_candidate_ids": ["candidate.a", "candidate.b"],
            "terminal_candidate_ids": ["candidate.a", "candidate.b"],
            "holdout_accesses": [_default_holdout_access_fixture(statistics)],
            "evaluation_start": "2024-01-01",
            "evaluation_end": "2024-04-09",
            "exposed_intervals": [],
        },
        "statistics": statistics,
        "financial_tradability": {
            "rule_snapshot_hash": "3" * 64,
            "order_audit_hash": "4" * 64,
            "ledger_hash": "5" * 64,
            "tradability_hash": "6" * 64,
            "simulation_semantics": {
                "summary": {
                    "capacity_mode": "modeled",
                    "claim_ceiling": "liquidity_modeled",
                    "capacity_model_id": "fixture-visible-capacity",
                    "capacity_model_version": "v1",
                    "cost_model_id": "fixture-cost",
                    "cost_model_version": "v1",
                },
                "blocks": [block_semantics],
            },
        },
    }


def _default_holdout_access_fixture(
    statistics: Mapping[str, object],
) -> dict[str, object]:
    sample_count = int(statistics["sample_count"])
    sample_ids = [
        (datetime(2024, 1, 1) + timedelta(days=offset)).date().isoformat()
        for offset in range(sample_count)
    ]
    unlock_at = "2024-04-10T00:00:00+08:00"
    freeze = {
        "parent_research_purpose": "d" * 64,
        "data_snapshot": "c" * 64,
        "holdout_split": {
            "split_id": "fixture-holdout",
            "start": sample_ids[0],
            "end": sample_ids[-1],
            "sample_ids": sample_ids,
        },
        "mode": "family_wide_confirmation",
        "candidates": ["candidate.a", "candidate.b"],
        "selection_rule": {"method": "fixture-selection-v1", "uses_validation": False},
        "validation": {"purpose": "diagnostic", "rule": None},
        "primary_estimand": "fixture-mean",
        "direction": "greater",
        "alpha": 0.05,
        "multiple_testing": {
            "method": "benjamini_yekutieli",
            "family_size": 2,
            "dependency": "arbitrary",
        },
        "random_protocol": {"combinations": [], "repetitions": 0, "seed": 7},
        "failure_policy": "opened_then_failure_is_consumed",
        "exposed_intervals": [],
        "package_plan_identity": "f" * 64,
        "implementation_identity": "1" * 64,
        "aliases": {"run": "fixture", "package": "fixture", "family": "fixture"},
    }
    identity_hash = typed_canonical_hash({
        "parent_research_purpose": freeze["parent_research_purpose"],
        "data_snapshot": freeze["data_snapshot"],
        "holdout_start": sample_ids[0],
        "holdout_end": sample_ids[-1],
        "sample_ids": sample_ids,
    })
    plan_hash = typed_canonical_hash({
        "freeze_payload": freeze,
        "actor": "research_pipeline",
        "reason": "locked_holdout_primary",
        "unlock_at": unlock_at,
        "holdout_identity_hash": identity_hash,
    })
    token_hash = typed_canonical_hash({
        "domain": "locked-holdout-token-v2",
        "plan_hash": plan_hash,
    })
    plan = {
        "contract_version": "research-persistent-holdout-plan-v2",
        "state": "frozen",
        "holdout_identity_hash": identity_hash,
        "freeze_payload": freeze,
        "actor": "research_pipeline",
        "reason": "locked_holdout_primary",
        "unlock_at": unlock_at,
        "plan_hash": plan_hash,
        "token_hash": token_hash,
    }
    prepared = {
        "contract_version": "research-persistent-holdout-prepared-v2",
        "state": "prepared",
        "plan_hash": plan_hash,
        "prepared_at": unlock_at,
        "preflight": {
            "format": "parquet",
            "schema": "fixture-v1",
            "candidate_ids": ["candidate.a", "candidate.b"],
            "family_size": 2,
        },
    }
    prepared["prepared_hash"] = typed_canonical_hash(prepared)
    opened = {
        "contract_version": "research-persistent-holdout-opened-v2",
        "state": "opened",
        "opening_id": "fixture-primary",
        "plan_hash": plan_hash,
        "prepared_hash": prepared["prepared_hash"],
        "token_hash": token_hash,
        "actor": "research_pipeline",
        "reason": "locked_holdout_primary",
        "opened_at": unlock_at,
        "sample_ids_hash": typed_canonical_hash(sample_ids),
    }
    opened["opened_hash"] = typed_canonical_hash(opened)
    terminal = {
        "contract_version": "research-persistent-holdout-terminal-v2",
        "opened_hash": opened["opened_hash"],
        "status": "committed",
        "result_hash": statistics["artifact_semantic_hash"],
        "reason": None,
    }
    terminal["terminal_hash"] = typed_canonical_hash(terminal)
    retired = {
        "contract_version": "research-persistent-holdout-retired-v2",
        "status": "retired",
        "holdout_identity_hash": identity_hash,
        "terminal_hash": terminal["terminal_hash"],
        "final_status": "committed",
    }
    retired["retired_hash"] = typed_canonical_hash(retired)
    return {
        "authorized": True,
        "plan": plan,
        "prepared": prepared,
        "opened": opened,
        "terminal": terminal,
        "retired": retired,
    }


__all__ = ["default_passing_validity_facts"]
