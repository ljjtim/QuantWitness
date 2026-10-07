"""风险和组合手算、真实求解状态及PIT专项。"""
from copy import deepcopy
import importlib.util
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1] / "project_extensions/qlib_portfolio_risk"
sys.path.insert(0, str(ROOT))
import optimizer as risk_optimizer
import oracle as risk_oracle


def covariance():
    return pd.DataFrame([[.04, 0.], [0., .09]], index=["A", "B"], columns=["A", "B"])


@pytest.mark.parametrize("method,expected", [("inv", [.6, .4]), ("gmv", [9/13, 4/13]), ("rp", [.6, .4]), ("mvo", [21/26, 5/26])])
def test_hand_calculated_qlib_portfolios_and_independent_oracle(method, expected):
    scores = pd.Series([.04, .01], index=["A", "B"])
    result = risk_optimizer.optimize_portfolio(covariance(), method=method, expected_returns=scores)
    assert result["status"] == "original_success"
    assert result["weights"] == pytest.approx(expected, abs=2e-5)
    risk_oracle.verify_optimization(result)
    from qlib.contrib.strategy.optimizer import PortfolioOptimizer
    direct = PortfolioOptimizer(method=method, lamb=1, scale_return=False, tol=1e-10)(covariance(), scores if method == "mvo" else None)
    assert result["weights"] == pytest.approx(direct.to_numpy(), abs=1e-12)


def risk_rows():
    return [{"session": f"2024-01-0{day}", "entity_id": code, "return": value,
             "available_at": f"2024-01-0{day}T15:00:00+08:00"}
            for day, values in [(2, [.01, -.02]), (3, [.03, .01]), (4, [-.02, .04])]
            for code, value in zip(["A", "B"], values)]


@pytest.mark.parametrize("estimator", ["empirical", "shrink"])
def test_covariance_units_and_future_input_isolation(estimator):
    rows = risk_rows()
    cov, inputs = risk_optimizer.estimate_covariance(rows, assets=["A", "B"], decision_time="2024-01-05T09:30:00+08:00", lookback=3, estimator=estimator)
    risk_oracle.verify_covariance(inputs, cov.to_numpy().tolist())
    future = rows + [{"session": "2024-01-05", "entity_id": code, "return": 999., "available_at": "2024-01-05T15:00:00+08:00"} for code in ["A", "B"]]
    changed, _ = risk_optimizer.estimate_covariance(future, assets=["A", "B"], decision_time="2024-01-05T09:30:00+08:00", lookback=3, estimator=estimator)
    pd.testing.assert_frame_equal(cov, changed)
    broken = cov.to_numpy().tolist()
    broken[0][0] += .01
    with pytest.raises(ValueError, match="独立复算"):
        risk_oracle.verify_covariance(inputs, broken)


def enhanced(**kwargs):
    args = dict(assets=["A", "B"], expected_returns=[0., 0.], factor=[[1.], [1.]],
        factor_covariance=[[.02]], residual_variance=[.04, .09], w0=[.5, .5], benchmark=[.5, .5],
        delta=.2, benchmark_deviation=None, epsilon=0.)
    args.update(kwargs)
    return risk_optimizer.optimize_enhanced(**args)


def test_success_equal_to_original_holdings_is_not_fallback():
    result = enhanced()
    assert result["status"] == "original_success" and result["fallback"] is False
    assert result["weights"] == pytest.approx([.5, .5], abs=1e-6)
    risk_oracle.verify_optimization(result)


def test_forced_sale_conflicts_with_turnover_without_silent_relaxation():
    result = enhanced(force_sell=["A"])
    assert result["status"] == "failed" and not result["accepted"]
    assert len(result["attempts"]) == 1
    assert result["attempts"][0]["status"] in {"infeasible", "infeasible_inaccurate"}
    relaxed = enhanced(force_sell=["A"], allow_turnover_relaxation=True)
    assert relaxed["status"] == "relaxed_success" and relaxed["accepted"]
    assert len(relaxed["attempts"]) == 2
    assert relaxed["attempts"][1]["relaxed_constraints"] == ["turnover_l1"]
    assert relaxed["constraint_residuals"]["turnover_l1"] == pytest.approx(.8, abs=1e-6)
    risk_oracle.verify_optimization(relaxed)


def test_retained_holdings_report_current_constraint_violation():
    result = enhanced(force_sell=["A"], retain_on_failure=True)
    assert result["status"] == "failed_retained" and not result["accepted"]
    assert result["fallback"] and result["weights"] == [.5, .5]
    assert result["constraint_residuals"]["upper_bound"] == .5


def test_solver_error_is_not_mathematical_infeasibility():
    result = enhanced(solver="UNAVAILABLE_SOLVER")
    assert result["attempts"][0]["status"] == "solver_error"
    assert result["status"] == "failed" and not result["accepted"]


def test_small_weight_postprocessing_cannot_break_benchmark_constraint():
    result = enhanced(w0=[.995, .005], benchmark=[.995, .005], benchmark_deviation=0., epsilon=.01)
    assert result["attempts"][0]["status"] == "optimal"
    assert result["status"] == "postprocessing_rejected" and not result["accepted"]
    assert result["constraint_residuals"]["lower_bound"] == pytest.approx(.005)


@pytest.mark.parametrize("kind", ["zero_volatility", "same_scores", "wrong_index", "inverse_turnover"])
def test_supported_bad_inputs_are_rejected(kind):
    cov = covariance()
    kwargs = dict(method="mvo", expected_returns=pd.Series([.1, .2], index=["A", "B"]))
    if kind == "zero_volatility":
        cov.iloc[0, 0] = 0
        kwargs = dict(method="inv")
    elif kind == "same_scores":
        kwargs.update(expected_returns=pd.Series([.1, .1], index=["A", "B"]), scale_return=True)
    elif kind == "wrong_index":
        kwargs["expected_returns"] = pd.Series([.1, .2], index=["B", "A"])
    else:
        kwargs = dict(method="inv", w0=pd.Series([.5, .5], index=["A", "B"]), delta=.2)
    with pytest.raises(ValueError):
        risk_optimizer.optimize_portfolio(cov, **kwargs)


def test_topk_uses_actual_holdings_and_respects_holding_sessions():
    scores = pd.Series([1., 2., 3.], index=["A", "B", "C"])
    old = pd.Series([.45, .45, 0.], index=scores.index)
    age = pd.Series([3, 3, 0], index=scores.index)
    result = risk_optimizer.topk_dropout(scores, current_weights=old, holding_sessions=age, topk=2, n_drop=1)
    assert result["sold"] == ["A"] and result["bought"] == ["C"]
    assert result["weights"] == pytest.approx([0., .45, .495])
    risk_oracle.verify_optimization(result)
    age.loc["A"] = 0
    blocked = risk_optimizer.topk_dropout(scores, current_weights=old, holding_sessions=age, topk=2, n_drop=1)
    assert blocked["weights"] == pytest.approx([.45, .45, .09])
    risk_oracle.verify_optimization(blocked)
    following = risk_optimizer.topk_dropout(scores, current_weights=pd.Series(blocked["weights"], index=scores.index),
        holding_sessions=pd.Series([3, 3, 1], index=scores.index), topk=2, n_drop=1)
    assert following["sold"] == ["A"] and len(following["selected"]) == 2
    risk_oracle.verify_optimization(following)


def test_independent_oracle_rejects_changed_weight_and_original_success_label():
    result = enhanced(force_sell=["A"], allow_turnover_relaxation=True)
    changed = deepcopy(result)
    changed["status"] = "original_success"
    with pytest.raises(ValueError, match="放宽"):
        risk_oracle.verify_optimization(changed)
    changed = deepcopy(result)
    changed["weights"] = [.1, .9]
    with pytest.raises(ValueError):
        risk_oracle.verify_optimization(changed)


def test_scipy_failed_result_is_preserved_even_when_weights_are_feasible(monkeypatch):
    from scipy.optimize import OptimizeResult
    monkeypatch.setattr(risk_optimizer.optimize, "minimize", lambda *a, **k: OptimizeResult(x=np.array([.5,.5]), success=False, status=9, message="Iteration limit reached", fun=.0325))
    result = risk_optimizer.optimize_portfolio(covariance(), method="gmv")
    assert result["status"] == "failed" and not result["accepted"]
    assert result["weights"] == [.5, .5]
    assert result["attempts"][0]["solver_status"] == 9


def test_enhanced_equations_match_direct_qlib_with_declared_solver(monkeypatch):
    import cvxpy as cp
    from qlib.contrib.strategy.optimizer.enhanced_indexing import EnhancedIndexingOptimizer
    solve = cp.Problem.solve
    def declared_solver(problem, *args, **kwargs):
        kwargs["solver"] = "CLARABEL"
        return solve(problem, *args, **kwargs)
    monkeypatch.setattr(cp.Problem, "solve", declared_solver)
    args = dict(expected_returns=[.01, -.01], factor=[[1.], [2.]], factor_covariance=[[.02]],
        residual_variance=[.04, .09], w0=[.5, .5], benchmark=[.5, .5], delta=.4, benchmark_deviation=.3, epsilon=0.)
    result = enhanced(**args)
    direct = EnhancedIndexingOptimizer(lamb=1., delta=.4, b_dev=.3, f_dev=None, scale_return=False, epsilon=0.)(
        r=np.array(args["expected_returns"]), F=np.array(args["factor"]), cov_b=np.array(args["factor_covariance"]),
        var_u=np.array(args["residual_variance"]), w0=np.array(args["w0"]), wb=np.array(args["benchmark"]))
    assert result["status"] == "original_success"
    assert result["weights"] == pytest.approx(direct, abs=1e-10)
    risk_oracle.verify_optimization(result)
