"""跨计划、Runtime、Result 与证据层共用的正式结论等级顺序。"""

from __future__ import annotations


CLAIM_LEVELS = (
    "research_observation",
    "portfolio_simulation_candidate",
    "tradable_simulation",
)


def weakest_claim_level(*levels: str) -> str:
    """返回一组正式结论等级中最保守的一个。"""

    if not levels or any(level not in CLAIM_LEVELS for level in levels):
        raise ValueError("claim level 集合无效")
    return min(levels, key=CLAIM_LEVELS.index)


__all__ = ["CLAIM_LEVELS", "weakest_claim_level"]
