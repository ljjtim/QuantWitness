"""确定性截面排序算法。"""

from __future__ import annotations

import pandas as pd


def stable_score_order(scores: pd.Series) -> pd.DataFrame:
    """按分数降序、证券代码升序稳定排列有效观测。"""
    valid = scores.dropna()
    frame = pd.DataFrame(
        {
            "code": valid.index.astype(str),
            "score": valid.astype("float64").to_numpy(),
        }
    )
    return frame.sort_values(
        ["score", "code"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def select_top_n_codes(scores: pd.Series, *, top_n: int) -> list[str]:
    return stable_score_order(scores).head(max(int(top_n), 1))["code"].tolist()


def assign_quantile_buckets(scores: pd.Series, *, quantiles: int) -> pd.Series:
    ordered = stable_score_order(scores)
    if len(ordered) < 2:
        return pd.Series(dtype="int64")
    bucket_count = min(max(int(quantiles), 1), len(ordered))
    ordered["position"] = range(len(ordered))
    ordered["quantile"] = (
        bucket_count - 1 - (ordered["position"] * bucket_count // len(ordered))
    ) + 1
    return ordered.set_index("code")["quantile"].astype("int64")


__all__ = [
    "assign_quantile_buckets",
    "select_top_n_codes",
    "stable_score_order",
]
