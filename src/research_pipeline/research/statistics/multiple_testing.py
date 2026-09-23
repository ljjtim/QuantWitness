"""预声明检验族与 FWER/FDR 校正。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

import numpy as np

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import StatisticsError


@dataclass(frozen=True)
class TestFamilyManifest:
    __test__: ClassVar[bool] = False

    family_id: str
    candidate_ids: tuple[str, ...]
    metric: str
    direction: str
    alpha: float
    dependency_assumption: str
    frozen_at: datetime
    stage: str = "pre_test"

    def __post_init__(self) -> None:
        if not self.family_id.strip() or not self.metric.strip():
            raise StatisticsError("family_id/metric 不能为空")
        if not self.candidate_ids or len(set(self.candidate_ids)) != len(self.candidate_ids):
            raise StatisticsError("检验族必须非空且候选 ID 必须唯一")
        if self.direction not in {"greater", "less", "two_sided"}:
            raise StatisticsError("检验方向无效")
        if not 0 < self.alpha < 1:
            raise StatisticsError("alpha 必须位于 0 和 1 之间")
        if self.dependency_assumption not in {"arbitrary", "independent_or_positive"}:
            raise StatisticsError("依赖假设无效")
        if self.stage != "pre_test":
            raise StatisticsError("检验族必须在读取 test/holdout 前冻结")
        if self.frozen_at.tzinfo is None or self.frozen_at.utcoffset() is None:
            raise StatisticsError("frozen_at 必须包含时区")

    @property
    def manifest_hash(self) -> str:
        return typed_canonical_hash({"family_id": self.family_id, "candidate_ids": list(self.candidate_ids), "metric": self.metric, "direction": self.direction, "alpha": self.alpha, "dependency_assumption": self.dependency_assumption, "frozen_at": self.frozen_at.isoformat(), "stage": self.stage})

    def require_complete_results(self, result_candidate_ids: tuple[str, ...]) -> None:
        if set(result_candidate_ids) != set(self.candidate_ids) or len(result_candidate_ids) != len(self.candidate_ids):
            raise StatisticsError("结果必须保留完整检验族，包括失败或被剪枝候选")


def adjust_p_values(p_values: object, *, method: str, manifest: TestFamilyManifest) -> np.ndarray:
    values = np.asarray(p_values, dtype=float).reshape(-1)
    if len(values) != len(manifest.candidate_ids) or np.any(~np.isfinite(values)) or np.any((values < 0) | (values > 1)):
        raise StatisticsError("p 值必须与完整检验族一一对应且位于 [0,1]")
    count = len(values)
    if method == "bonferroni":
        return np.minimum(1.0, values * count)
    order = np.argsort(values, kind="mergesort")
    sorted_values = values[order]
    if method == "holm":
        sorted_adjusted = np.maximum.accumulate(sorted_values * (count - np.arange(count)))
    elif method in {"benjamini_hochberg", "benjamini_yekutieli"}:
        if method == "benjamini_hochberg" and manifest.dependency_assumption != "independent_or_positive":
            raise StatisticsError("BH 需要 independent_or_positive 依赖假设")
        harmonic = 1.0 if method == "benjamini_hochberg" else float(np.sum(1.0 / np.arange(1, count + 1)))
        raw = sorted_values * count * harmonic / np.arange(1, count + 1)
        sorted_adjusted = np.minimum.accumulate(raw[::-1])[::-1]
    else:
        raise StatisticsError("多重检验方法不受支持")
    output = np.empty(count, dtype=float)
    output[order] = np.minimum(1.0, sorted_adjusted)
    return output


__all__ = ["TestFamilyManifest", "adjust_p_values"]
