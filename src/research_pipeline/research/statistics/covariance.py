"""HAC 均值标准误估计。"""

from __future__ import annotations

import numpy as np

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import InferenceResult, StatisticsError


def _values(values: object) -> np.ndarray:
    array = np.asarray(values, dtype=float).reshape(-1)
    if len(array) < 3 or np.any(~np.isfinite(array)):
        raise StatisticsError("推断样本至少 3 个且必须全部有限")
    if float(np.std(array, ddof=0)) <= 0.0:
        raise StatisticsError("常数序列无法进行稳健推断")
    return array


def newey_west_mean(values: object, *, lag: int, kernel: str = "bartlett", confidence: float = 0.95, overlap_source: str | None = None) -> InferenceResult:
    array = _values(values)
    n = len(array)
    if isinstance(lag, bool) or not isinstance(lag, int) or lag < 0 or lag >= n:
        raise StatisticsError("HAC lag 必须是 0 到 n-1 的整数")
    if kernel != "bartlett":
        raise StatisticsError("当前 HAC 只支持 bartlett kernel")
    centered = array - float(array.mean())
    long_run = float(centered @ centered / n)
    for offset in range(1, lag + 1):
        gamma = float(centered[offset:] @ centered[:-offset] / n)
        long_run += 2.0 * (1.0 - offset / (lag + 1.0)) * gamma
    variance = max(long_run / n, 0.0)
    if variance <= 0:
        raise StatisticsError("HAC 方差退化，无法产生有效标准误")
    effective = min(float(n), float(np.var(array, ddof=0) / variance))
    input_hash = typed_canonical_hash({"values": array.tolist(), "lag": lag, "kernel": kernel, "overlap_source": overlap_source})
    return InferenceResult.build(
        method="newey_west_mean_v1",
        estimate=float(array.mean()),
        standard_error=float(np.sqrt(variance)),
        sample_size=n,
        effective_sample_size=max(1.0, effective),
        parameters={"lag": lag, "kernel": kernel, "overlap_source": overlap_source},
        assumptions=("弱平稳", "Bartlett 截断后的自协方差足以描述序列相关"),
        limitations=("正态近似 p 值",),
        input_hash=input_hash,
        confidence=confidence,
    )

__all__ = ["newey_west_mean"]
