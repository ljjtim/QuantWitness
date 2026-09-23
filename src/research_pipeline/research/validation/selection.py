"""只允许 validation 指标参与候选选择。"""

from __future__ import annotations

import math

import pandas as pd

from .splits import ValidationError


def select_by_validation(metrics: pd.DataFrame, *, objective: str, direction: str, stage_column: str = "stage") -> dict[str, object]:
    required = {"candidate_id", stage_column, objective}
    missing = required - set(metrics.columns)
    if missing:
        raise ValidationError(f"候选选择缺少字段: {sorted(missing)}")
    stages = set(metrics[stage_column].astype(str))
    if stages - {"validation"}:
        raise ValidationError("候选选择只能接收 validation 指标，test/holdout 不可见")
    if direction not in {"maximize", "minimize"}:
        raise ValidationError("选择方向只支持 maximize/minimize")
    frame = metrics.copy()
    frame[objective] = pd.to_numeric(frame[objective], errors="coerce")
    if frame.empty or not frame[objective].map(math.isfinite).all():
        raise ValidationError("validation 指标必须非空且全部有限")
    ascending = direction == "minimize"
    selected = frame.sort_values([objective, "candidate_id"], ascending=[ascending, True], kind="mergesort").iloc[0]
    return selected.to_dict()


__all__ = ["select_by_validation"]
