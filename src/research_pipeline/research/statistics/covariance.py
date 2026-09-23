"""HAC、聚类稳健均值误差和重叠标签修正。"""

from __future__ import annotations

from typing import Protocol

import numpy as np

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import InferenceResult, StatisticsError

class LabelInterval(Protocol):
    observation_time: object
    exit_time: object


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


def infer_overlap_lag(intervals: tuple[LabelInterval, ...]) -> int:
    if len(intervals) < 2:
        raise StatisticsError("重叠标签修正至少需要两个标签区间")
    ordered = tuple(sorted(intervals, key=lambda item: item.observation_time))
    if len({item.observation_time for item in ordered}) != len(ordered):
        raise StatisticsError("标签 observation_time 必须唯一")
    lag = 0
    for index, interval in enumerate(ordered):
        overlaps = sum(candidate.observation_time < interval.exit_time for candidate in ordered[index + 1:])
        lag = max(lag, overlaps)
    return lag


def cluster_robust_mean(values: object, *, clusters: object, second_clusters: object | None = None, confidence: float = 0.95) -> InferenceResult:
    array = _values(values)
    first = np.asarray(clusters, dtype=object).reshape(-1)
    if len(first) != len(array):
        raise StatisticsError("cluster 长度必须与样本一致")
    variance_first, count_first = _cluster_variance(array, first)
    parameters: dict[str, object] = {"cluster_dimensions": 1, "clusters_first": count_first, "finite_sample_correction": True}
    variance = variance_first
    assumptions = ("不同簇之间近似独立",)
    if second_clusters is not None:
        second = np.asarray(second_clusters, dtype=object).reshape(-1)
        if len(second) != len(array):
            raise StatisticsError("second_clusters 长度必须与样本一致")
        variance_second, count_second = _cluster_variance(array, second)
        intersection = np.asarray([f"{a!r}|{b!r}" for a, b in zip(first, second, strict=True)], dtype=object)
        variance_intersection, count_intersection = _cluster_variance(array, intersection)
        variance = variance_first + variance_second - variance_intersection
        parameters.update({"cluster_dimensions": 2, "clusters_second": count_second, "clusters_intersection": count_intersection})
        assumptions = ("每个聚类维度之外近似独立", "二维方差使用 inclusion-exclusion")
    if variance <= 0 or not np.isfinite(variance):
        raise StatisticsError("聚类稳健方差退化")
    input_hash = typed_canonical_hash({"values": array.tolist(), "clusters": [str(v) for v in first], "second_clusters": None if second_clusters is None else [str(v) for v in second]})
    return InferenceResult.build(method="cluster_robust_mean_v1", estimate=float(array.mean()), standard_error=float(np.sqrt(variance)), sample_size=len(array), effective_sample_size=float(count_first), parameters=parameters, assumptions=assumptions, limitations=("簇数较少时正态近似可能偏乐观",), input_hash=input_hash, confidence=confidence)


def _cluster_variance(values: np.ndarray, clusters: np.ndarray) -> tuple[float, int]:
    unique = sorted(set(clusters.tolist()), key=str)
    count = len(unique)
    if count < 2:
        raise StatisticsError("聚类稳健误差至少需要两个簇")
    centered = values - values.mean()
    sums = np.asarray([centered[clusters == cluster].sum() for cluster in unique], dtype=float)
    variance = count / (count - 1.0) * float(sums @ sums) / (len(values) ** 2)
    return variance, count


__all__ = ["cluster_robust_mean", "infer_overlap_lag", "newey_west_mean"]
