"""PSR/DSR、PBO、White Reality Check 与 SPA 选择偏差诊断。"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from statistics import NormalDist

import numpy as np

from research_pipeline.platform.canonical import typed_canonical_hash

from .bootstrap import (
    bootstrap_batch_replications,
    derive_seed,
    iter_bootstrap_index_batches,
)
from .contracts import StatisticsError


@dataclass(frozen=True)
class SelectionBiasResult:
    method: str
    statistic: float
    p_value: float | None
    status: str
    sample_size: int
    trial_count: int
    effective_trial_count: float
    parameters: tuple[tuple[str, object], ...]
    assumptions: tuple[str, ...]
    limitations: tuple[str, ...]
    input_hash: str
    result_hash: str


def effective_trial_count(candidate_returns: object) -> float:
    raw = np.asarray(candidate_returns, dtype=float)
    if raw.ndim == 2 and raw.shape[1] < 2:
        raise StatisticsError("有效试验数至少需要两个候选")
    matrix = _matrix(raw)
    centered = matrix - matrix.mean(axis=0)
    scale = matrix.std(axis=0, ddof=1)
    standardized = centered / scale
    observations, trials = standardized.shape
    # trace(C^2) 等于相关矩阵所有元素平方和；在候选很多时改算较小的 ZZ'。
    if trials <= observations:
        gram = standardized.T @ standardized / (observations - 1)
    else:
        gram = standardized @ standardized.T / (observations - 1)
    denominator = float(np.square(gram).sum())
    if denominator <= 0:
        raise StatisticsError("候选相关矩阵无法计算有效试验数")
    return float(np.clip(trials**2 / denominator, 1.0, trials))


def probabilistic_sharpe_ratio(returns: object, *, benchmark_sharpe: float = 0.0) -> SelectionBiasResult:
    values = _vector(returns, minimum=8)
    mean = float(values.mean())
    std = float(values.std(ddof=1))
    if std <= 0:
        raise StatisticsError("PSR 收益标准差必须为正")
    sharpe = mean / std
    centered = (values - mean) / std
    skew = float(np.mean(centered ** 3))
    kurtosis = float(np.mean(centered ** 4))
    denominator = 1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe ** 2
    if denominator <= 0:
        raise StatisticsError("PSR 高阶矩条件导致分母退化")
    z = (sharpe - benchmark_sharpe) * np.sqrt(len(values) - 1.0) / np.sqrt(denominator)
    probability = float(NormalDist().cdf(z))
    return _result("probabilistic_sharpe_ratio_v1", sharpe, 1.0 - probability, len(values), 1, 1.0, {"benchmark_sharpe": benchmark_sharpe, "skew": skew, "kurtosis": kurtosis}, ("收益序列近似平稳", "高阶矩可稳定估计"), ("输出是超越基准 Sharpe 的概率诊断，不是交易授权",), values)


def deflated_sharpe_ratio(selected_returns: object, candidate_returns: object) -> SelectionBiasResult:
    selected = _vector(selected_returns, minimum=8)
    matrix = _matrix(candidate_returns)
    if matrix.shape[0] != len(selected):
        raise StatisticsError("DSR 候选与选中收益样本长度不一致")
    effective = effective_trial_count(matrix)
    candidate_sharpes = matrix.mean(axis=0) / matrix.std(axis=0, ddof=1)
    if np.any(~np.isfinite(candidate_sharpes)):
        raise StatisticsError("DSR 候选 Sharpe 退化")
    mean_sr = float(candidate_sharpes.mean())
    std_sr = float(candidate_sharpes.std(ddof=1))
    quantile = NormalDist().inv_cdf(max(0.5, 1.0 - 1.0 / effective))
    benchmark = mean_sr + std_sr * quantile
    base = probabilistic_sharpe_ratio(selected, benchmark_sharpe=benchmark)
    return _result("deflated_sharpe_ratio_v1", base.statistic, base.p_value or 0.0, len(selected), matrix.shape[1], effective, {"benchmark_sharpe": benchmark}, ("候选全集完整", "候选相关结构可代表有效试验数"), ("DSR 是选择偏差诊断，不等于策略有效",), np.column_stack([selected, matrix]))


def probability_of_backtest_overfitting(performance_by_split: object, *, max_combinations: int = 10_000) -> SelectionBiasResult:
    raw = np.asarray(performance_by_split, dtype=float)
    if raw.ndim != 2:
        raise StatisticsError("PBO 输入必须是二维切分矩阵")
    splits, trials = raw.shape
    if splits < 4 or splits % 2 or trials < 2:
        raise StatisticsError("PBO 需要偶数且至少 4 个切分、至少 2 个候选")
    matrix = _matrix(raw)
    combos = list(combinations(range(splits), splits // 2))
    if len(combos) > max_combinations:
        raise StatisticsError("PBO 组合数量超过预算")
    logits = []
    all_splits = set(range(splits))
    for train_indices in combos:
        test_indices = sorted(all_splits - set(train_indices))
        best = int(np.argmax(matrix[list(train_indices)].mean(axis=0)))
        test_scores = matrix[test_indices].mean(axis=0)
        rank = int(np.argsort(np.argsort(test_scores, kind="mergesort"), kind="mergesort")[best]) + 1
        relative = (rank - 0.5) / trials
        logits.append(float(np.log(relative / (1.0 - relative))))
    pbo = float(np.mean(np.asarray(logits) <= 0.0))
    effective = effective_trial_count(matrix)
    return _result("probability_of_backtest_overfitting_v1", pbo, None, splits, trials, effective, {"combinations": len(combos)}, ("切分可交换且样本外半区代表未来",), ("PBO 依赖切分定义和候选全集",), matrix)


def reality_check(candidate_returns: object, *, method: str, block_length: int, replications: int, root_seed: int, node_id: str) -> SelectionBiasResult:
    matrix = _matrix(candidate_returns)
    if matrix.shape[0] < 8 or matrix.shape[1] < 2:
        raise StatisticsError("Reality Check/SPA 至少需要 8 期和 2 个候选")
    input_hash = typed_canonical_hash({"matrix": matrix.tolist(), "method": method, "block_length": block_length, "replications": replications})
    seed = derive_seed(root_seed, node_id=node_id, method=method, input_hash=input_hash)
    means = matrix.mean(axis=0)
    centered = matrix - means
    bootstrap_means = np.empty((replications, matrix.shape[1]), dtype=float)
    batch_replications = bootstrap_batch_replications(
        sample_size=matrix.shape[0],
        replications=replications,
        persistent_bytes=int(
            matrix.nbytes + centered.nbytes + bootstrap_means.nbytes
        ),
        sample_copies_per_replication=1,
        extra_bytes_per_replication=matrix.shape[1] * np.dtype(float).itemsize,
    )
    offset = 0
    for indices in iter_bootstrap_index_batches(
        method="stationary",
        sample_size=matrix.shape[0],
        block_length=block_length,
        replications=replications,
        seed=seed,
        batch_replications=batch_replications,
    ):
        weights = np.stack(
            [
                np.bincount(row, minlength=matrix.shape[0])
                for row in indices
            ]
        ).astype(float, copy=False)
        stop = offset + len(indices)
        bootstrap_means[offset:stop] = weights @ centered / matrix.shape[0]
        offset = stop
    if method == "white_reality_check":
        observed = float(np.sqrt(len(matrix)) * max(0.0, means.max()))
        boot = np.sqrt(len(matrix)) * np.maximum(0.0, bootstrap_means.max(axis=1))
        assumptions = ("stationary bootstrap 可保留时序依赖", "完整候选全集均在零假设下无超额表现")
    elif method == "spa":
        scale = matrix.std(axis=0, ddof=1)
        if np.any(scale <= 0):
            raise StatisticsError("SPA 候选方差必须为正")
        observed = float(max(0.0, (np.sqrt(len(matrix)) * means / scale).max()))
        boot = np.maximum(0.0, (np.sqrt(len(matrix)) * bootstrap_means / scale).max(axis=1))
        assumptions = ("studentized 候选收益可比较", "stationary bootstrap 可保留时序依赖")
    else:
        raise StatisticsError("method 只支持 white_reality_check/spa")
    p_value = float((1 + np.sum(boot >= observed)) / (replications + 1))
    return _result(f"{method}_v1", observed, p_value, matrix.shape[0], matrix.shape[1], effective_trial_count(matrix), {"block_length": block_length, "replications": replications, "seed": seed}, assumptions, ("结果依赖候选全集、块长和重复次数",), matrix)


def _vector(values: object, *, minimum: int) -> np.ndarray:
    array = np.asarray(values, dtype=float).reshape(-1)
    if len(array) < minimum or np.any(~np.isfinite(array)):
        raise StatisticsError(f"样本至少 {minimum} 个且必须全部有限")
    return array


def _matrix(values: object) -> np.ndarray:
    matrix = np.asarray(values, dtype=float)
    if matrix.ndim != 2 or min(matrix.shape) < 2 or np.any(~np.isfinite(matrix)):
        raise StatisticsError("候选收益必须是至少 2x2 的有限矩阵")
    if np.any(matrix.std(axis=0, ddof=0) <= 0):
        raise StatisticsError("候选收益不能包含常数列")
    return matrix


def _result(method: str, statistic: float, p_value: float | None, sample_size: int, trial_count: int, effective: float, parameters: dict[str, object], assumptions: tuple[str, ...], limitations: tuple[str, ...], raw: np.ndarray) -> SelectionBiasResult:
    input_hash = typed_canonical_hash({"raw": np.asarray(raw).tolist(), "method": method, "parameters": parameters})
    payload = {"method": method, "statistic": statistic, "p_value": p_value, "sample_size": sample_size, "trial_count": trial_count, "effective_trial_count": effective, "parameters": parameters, "input_hash": input_hash}
    return SelectionBiasResult(method, float(statistic), None if p_value is None else float(p_value), "applicable", sample_size, trial_count, float(effective), tuple(sorted(parameters.items())), assumptions, limitations, input_hash, typed_canonical_hash(payload))


__all__ = ["SelectionBiasResult", "deflated_sharpe_ratio", "effective_trial_count", "probabilistic_sharpe_ratio", "probability_of_backtest_overfitting", "reality_check"]
