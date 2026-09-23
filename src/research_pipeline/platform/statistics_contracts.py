"""稳健统计评价模式与正式结论上限的纯合同。"""

from __future__ import annotations

from types import MappingProxyType


FAMILY_WIDE_CONFIRMATION = "family_wide_confirmation"
DIAGNOSTIC_UPDATE = "diagnostic_update"
MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID = (
    "research.minute-statistics.observations.v1"
)
MINUTE_STATISTICS_SPLIT_SCHEMA_ID = (
    "research.minute-statistics.split-assignments.v1"
)


ROBUST_STATISTICS_CONCLUSION_CONTRACTS = MappingProxyType({
    FAMILY_WIDE_CONFIRMATION: MappingProxyType({
        "family_claim_scope": "preregistered_family_corrected_only",
        "post_evaluation_winner_scope": "exploratory_only",
    }),
    DIAGNOSTIC_UPDATE: MappingProxyType({
        "family_claim_scope": "diagnostic_update_only",
        "post_evaluation_winner_scope": "diagnostic_only",
    }),
})


def robust_statistics_conclusion_contract(mode: str) -> dict[str, str]:
    try:
        return dict(ROBUST_STATISTICS_CONCLUSION_CONTRACTS[mode])
    except KeyError as exc:
        raise ValueError(f"未知稳健统计评价模式: {mode}") from exc


__all__ = [
    "DIAGNOSTIC_UPDATE",
    "FAMILY_WIDE_CONFIRMATION",
    "MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID",
    "MINUTE_STATISTICS_SPLIT_SCHEMA_ID",
    "ROBUST_STATISTICS_CONCLUSION_CONTRACTS",
    "robust_statistics_conclusion_contract",
]
