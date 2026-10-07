"""DSR 原论文数值、零假设门槛和公共统计合同的纯内存验收。"""

from __future__ import annotations

import math
from statistics import NormalDist

import numpy as np
import pytest

from research_pipeline.research.statistics import StatisticsError
from research_pipeline.research.statistics.selection_bias import (
    _expected_maximum_sharpe,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
)


def _extreme_coefficient(trials: float) -> float:
    if trials <= 1:
        return 0.0
    normal = NormalDist()
    gamma = 0.5772156649015329
    return max(0.0, (1 - gamma) * normal.inv_cdf(1 - 1 / trials)
               + gamma * normal.inv_cdf(1 - 1 / (math.e * trials)))


def _moment_probability(sharpe, threshold, observations, skew, kurtosis):
    error = math.sqrt((1 - skew * sharpe + (kurtosis - 1) * sharpe**2 / 4)
                      / (observations - 1))
    return NormalDist().cdf((sharpe - threshold) / error)


def _oracle(selected, family, *, effective_trials=None):
    sharpes = np.array([np.mean(column) / np.std(column, ddof=1)
                       for column in family.T])
    trials = family.shape[1] if effective_trials is None else effective_trials
    threshold = 0.0 if trials <= 1 else np.std(sharpes, ddof=1) * _extreme_coefficient(trials)
    scale = np.std(selected, ddof=1)
    standardized = (selected - np.mean(selected)) / scale
    probability = _moment_probability(
        np.mean(selected) / scale, threshold, len(selected),
        np.mean(standardized**3), np.mean(standardized**4),
    )
    return float(threshold), probability


def _family(sharpes=(0.02, 0.07, 0.11, 0.18), observations=40):
    noise = np.random.default_rng(871).normal(size=(observations, len(sharpes)))
    orthogonal, _ = np.linalg.qr(noise - noise.mean(axis=0))
    return (orthogonal * math.sqrt(observations - 1) + np.asarray(sharpes)) * 0.01


def test_original_paper_example_and_general_expected_maximum():
    # 原文第 9–10 页：候选年化 Sharpe 方差 1/2，每年 250 期。
    threshold = _expected_maximum_sharpe(0.0, math.sqrt(0.5 / 250), 100)
    assert threshold == pytest.approx(0.113172001865, abs=5e-13)
    probability = _moment_probability(2.5 / math.sqrt(250), threshold, 1250, -3, 10)
    assert probability == pytest.approx(0.900396834449, abs=5e-13)
    assert _expected_maximum_sharpe(0.3, 0.1, 10) == pytest.approx(0.457459830134575)
    assert _expected_maximum_sharpe(0.3, 0.1, 1) == 0.3
    assert _expected_maximum_sharpe(0.3, 0.0, 10) == 0.3


@pytest.mark.parametrize("offset", [0.0, 0.23, -0.17])
def test_public_zero_null_does_not_add_nonzero_candidate_mean(offset):
    matrix = _family() + offset * 0.01
    selected = matrix[:, -1]
    correlation = np.corrcoef(matrix, rowvar=False)
    trials = matrix.shape[1] ** 2 / np.square(correlation).sum()
    threshold, probability = _oracle(selected, matrix, effective_trials=trials)
    result = deflated_sharpe_ratio(selected, matrix)
    parameters = dict(result.parameters)
    assert parameters["benchmark_sharpe"] == pytest.approx(threshold, abs=1e-14)
    assert result.p_value == pytest.approx(1 - probability, abs=1e-14)
    assert result.method == "deflated_sharpe_ratio_v2"
    assert parameters["expected_maximum_formula"] == "bailey_lopez_de_prado_2014_eq2"
    assert parameters["null_mean_sharpe"] == 0.0
    assert parameters["effective_trials_method"] == "correlation_participation_ratio"


@pytest.mark.parametrize("spread", [0.25, 1.0, 3.0])
def test_candidate_sharpe_scale_controls_public_dsr(spread):
    matrix = _family(tuple(0.12 + spread * np.array([-0.09, -0.03, 0.03, 0.09])))
    selected = _family()[:, -1]
    threshold, probability = _oracle(selected, matrix)
    public = deflated_sharpe_ratio(selected, matrix)
    assert dict(public.parameters)["benchmark_sharpe"] == pytest.approx(threshold)
    assert 1 - public.p_value == pytest.approx(probability)


def test_zero_sharpe_dispersion_has_zero_threshold():
    matrix = _family((0.12, 0.12, 0.12, 0.12))
    selected = matrix[:, 0]
    psr = probabilistic_sharpe_ratio(selected)
    public = deflated_sharpe_ratio(selected, matrix)
    assert dict(public.parameters)["benchmark_sharpe"] == pytest.approx(0.0, abs=1e-14)
    assert public.p_value == pytest.approx(psr.p_value)


def test_public_rejects_single_candidate_and_supports_correlated_family():
    selected = _family()[:, 0]
    single = selected[:, None]
    with pytest.raises(StatisticsError, match="至少 2x2"):
        deflated_sharpe_ratio(selected, single)
    expected = 1 - probabilistic_sharpe_ratio(selected).p_value
    public = deflated_sharpe_ratio(selected, np.repeat(single, 3, axis=1))
    assert public.effective_trial_count == pytest.approx(1.0)
    assert 1 - public.p_value == pytest.approx(expected)


@pytest.mark.parametrize("constant", [0.0, 1.0, -1.0])
def test_public_rejects_constant_selected_series(constant):
    selected = np.full(12, constant)
    with pytest.raises(StatisticsError, match="标准差必须为正"):
        deflated_sharpe_ratio(selected, _family(observations=12))


@pytest.mark.parametrize("failure", ["constant", "nonfinite", "length", "short"])
def test_undefined_family_or_misaligned_samples_are_rejected(failure):
    family = _family()
    selected = family[:, -1].copy()
    if failure == "constant":
        family[:, 0] = 0
    elif failure == "nonfinite":
        family[0, 0] = np.nan
    elif failure == "length":
        family = family[:-1]
    else:
        family, selected = family[:2], selected[:2]
    with pytest.raises(StatisticsError):
        deflated_sharpe_ratio(selected, family)
