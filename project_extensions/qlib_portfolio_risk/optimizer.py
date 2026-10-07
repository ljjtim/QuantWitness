"""Qlib 0.9.7 风险估计与组合求解的状态适配。"""
from importlib.metadata import version
import math

import numpy as np
import pandas as pd
from scipy import optimize
from qlib.contrib.strategy.optimizer import PortfolioOptimizer
from qlib.model.riskmodel import RiskModel, ShrinkCovEstimator


def _finite(values, name):
    array = np.asarray(values, dtype=float)
    if not np.isfinite(array).all():
        raise ValueError(name + "必须是有限数值")
    return array


def _moment(value):
    value = pd.Timestamp(value)
    if value.tzinfo is None:
        raise ValueError("时点必须包含时区")
    return value


def estimate_covariance(rows, *, assets, decision_time, lookback=20, estimator="empirical", shrink_alpha=0.1):
    """仅用决策时已经可见的完整日收益，单位为小数收益平方。"""
    if len(assets) != len(set(assets)) or not assets:
        raise ValueError("证券集合必须非空且唯一")
    if type(lookback) is not int or lookback < 2:
        raise ValueError("风险窗口至少包含两个会话")
    decision = _moment(decision_time)
    frame = pd.DataFrame(rows)
    required = {"session", "entity_id", "return", "available_at"}
    if not required <= set(frame):
        raise ValueError("风险输入缺少会话、证券、收益或可见时点")
    frame["available_at"] = frame.available_at.map(_moment)
    frame = frame.loc[(frame.available_at <= decision) & frame.entity_id.isin(assets)].copy()
    if frame.duplicated(["session", "entity_id"]).any():
        raise ValueError("风险输入会话证券重复")
    frame["return"] = _finite(frame["return"], "风险收益")
    days = sorted(frame.session.astype(str).unique())[-lookback:]
    matrix = frame.pivot(index="session", columns="entity_id", values="return").reindex(index=days, columns=assets)
    if len(days) != lookback or matrix.isna().any().any():
        raise ValueError("风险窗口缺少完整证券会话")
    if estimator == "empirical":
        model = RiskModel(nan_option="ignore", scale_return=False)
    elif estimator == "shrink":
        model = ShrinkCovEstimator(alpha=shrink_alpha, target="const_var", nan_option="ignore", scale_return=False)
    else:
        raise ValueError("仅支持empirical和shrink协方差")
    covariance = model.predict(matrix.copy(), is_price=False)
    return covariance, {"estimator": estimator, "lookback": lookback, "shrink_alpha": shrink_alpha,
        "assets": list(assets), "sessions": days, "returns": matrix.to_numpy().tolist(),
        "available_at": frame.loc[frame.session.astype(str).isin(days), "available_at"].max().isoformat(),
        "decision_time": decision.isoformat(), "unit": "squared_decimal_return", "qlib_version": version("pyqlib")}


def constraint_residuals(weights, *, w0=None, delta=None, lower=None, upper=None, factor=None, benchmark=None, factor_deviation=None):
    """正残差表示越界，换手使用双边L1。"""
    w = _finite(weights, "权重")
    lb = np.zeros(len(w)) if lower is None else np.asarray(lower, dtype=float)
    ub = np.ones(len(w)) if upper is None else np.asarray(upper, dtype=float)
    residuals = {"full_investment": abs(float(w.sum()) - 1),
        "lower_bound": max(0., float(np.max(lb - w))), "upper_bound": max(0., float(np.max(w - ub)))}
    if delta is not None and w0 is not None:
        residuals["turnover_l1"] = max(0., float(np.abs(w - w0).sum()) - delta)
    if factor_deviation is not None:
        exposure = (w - benchmark) @ factor
        residuals["factor_deviation"] = max(0., float(np.max(np.abs(exposure) - factor_deviation)))
    return residuals


class ObservedPortfolioOptimizer(PortfolioOptimizer):
    """取得Qlib真实SciPy状态，不根据返回权重猜测成功。"""
    def _solve(self, n, obj, bounds, cons):
        wrapped = obj if self.alpha == 0 else lambda x: obj(x) + self.alpha * np.square(x).sum()
        self.solution = optimize.minimize(wrapped, np.ones(n) / n, bounds=bounds, constraints=cons, tol=self.tol)
        return self.solution.x


def optimize_portfolio(covariance, *, method, expected_returns=None, score_semantics="expected_return",
                       w0=None, delta=None, lamb=1., alpha=0., scale_return=False, tolerance=1e-6):
    """普通Qlib只做多满仓组合；逆波动不支持换手约束。"""
    if not isinstance(covariance, pd.DataFrame) or not covariance.index.equals(covariance.columns):
        raise ValueError("协方差行列必须绑定同一证券顺序")
    assets = covariance.index.astype(str).tolist()
    S = _finite(covariance.to_numpy(), "协方差")
    if not np.allclose(S, S.T, atol=1e-12) or np.linalg.eigvalsh(S).min() < -1e-12:
        raise ValueError("协方差必须对称且半正定")
    if method not in {"inv", "gmv", "rp", "mvo"}:
        raise ValueError("组合方法不受支持")
    if method in {"inv", "rp"} and (np.diag(S) <= 0).any():
        raise ValueError("逆波动和风险平价拒绝零波动证券")
    if w0 is not None:
        if not isinstance(w0, pd.Series) or not w0.index.equals(covariance.index):
            raise ValueError("原持仓证券索引不一致")
        w0 = _finite(w0.to_numpy(), "原持仓")
        if delta is None or delta < 0:
            raise ValueError("原持仓必须绑定非负双边换手预算")
    if method == "inv" and w0 is not None:
        raise ValueError("Qlib逆波动不执行换手约束")
    r = None
    if method == "mvo":
        if score_semantics not in {"expected_return", "standardized_score"}:
            raise ValueError("均值方差需要明确预期收益或标准化分数")
        if not isinstance(expected_returns, pd.Series) or not expected_returns.index.equals(covariance.index):
            raise ValueError("预期收益证券索引不一致")
        r = _finite(expected_returns.to_numpy(), "预期收益")
        if scale_return and np.std(r) <= 0:
            raise ValueError("全相同分数不能缩放")
    optimizer = ObservedPortfolioOptimizer(method=method, lamb=lamb, delta=0. if delta is None else delta,
                                          alpha=alpha, scale_return=scale_return, tol=1e-10)
    try:
        weights = np.asarray(optimizer(pd.DataFrame(S, index=assets, columns=assets),
                                      None if r is None else pd.Series(r, index=assets),
                                      None if w0 is None else pd.Series(w0, index=assets)))
        if method == "inv":
            success, code, message, objective = True, 0, "analytic", None
        else:
            sol = optimizer.solution
            success, code, message = bool(sol.success), int(sol.status), str(sol.message)
            objective = float(sol.fun) if math.isfinite(float(sol.fun)) else None
        residuals = constraint_residuals(weights, w0=w0, delta=delta)
        accepted = success and max(residuals.values()) <= tolerance
        attempt = {"attempt": 1, "status": "optimal" if success else "solver_failed", "solver_status": code,
            "message": message, "objective": objective, "relaxed_constraints": []}
    except (RuntimeError, ValueError, ArithmeticError) as exc:
        weights, residuals, accepted = None, {}, False
        attempt = {"attempt": 1, "status": "solver_error", "solver_status": None, "message": str(exc),
                   "objective": None, "relaxed_constraints": []}
    return {"assets": assets, "method": method, "status": "original_success" if accepted else "failed",
        "accepted": accepted, "weights": None if weights is None else weights.tolist(), "attempts": [attempt],
        "constraint_residuals": residuals, "tolerance": tolerance, "solver": "analytic" if method == "inv" else "scipy.optimize.minimize/SLSQP",
        "solver_version": version("scipy"), "qlib_version": version("pyqlib"), "fallback": False,
        "postprocessing": "none", "constraints": {"full_investment": True, "long_only": True, "delta_l1": delta},
        "scale_return": scale_return, "score_semantics": score_semantics,
        "parameters": {"lamb": lamb, "alpha": alpha}, "w0": None if w0 is None else w0.tolist(),
        "expected_returns": None if r is None else r.tolist(), "covariance": S.tolist()}


def optimize_enhanced(*, assets, expected_returns, factor, factor_covariance, residual_variance, w0, benchmark,
                      lamb=1., delta=0.2, benchmark_deviation=0.2, factor_deviation=None,
                      force_hold=(), force_sell=(), scale_return=False, epsilon=1e-4,
                      allow_turnover_relaxation=False, retain_on_failure=False, solver="CLARABEL", tolerance=1e-6):
    """按Qlib增强指数方程求解，每次状态和显式放宽分别封存。"""
    import cvxpy as cp
    assets = list(assets)
    if not assets or len(set(assets)) != len(assets):
        raise ValueError("证券身份重复或为空")
    r, F, B, u, old, wb = [_finite(x, name) for x, name in ((expected_returns, "收益"), (factor, "暴露"),
        (factor_covariance, "因子协方差"), (residual_variance, "残差方差"), (w0, "实际持仓"), (benchmark, "历史基准"))]
    n = len(assets)
    if r.shape != (n,) or old.shape != (n,) or wb.shape != (n,) or u.shape != (n,) or F.shape[0] != n or B.shape != (F.shape[1], F.shape[1]):
        raise ValueError("增强指数输入形状与证券或因子身份不一致")
    if (u < 0).any() or (old < 0).any() or old.sum() > 1 + tolerance or (wb < 0).any() or abs(wb.sum() - 1) > tolerance:
        raise ValueError("残差方差、实际持仓或基准权重无效")
    if not np.allclose(B, B.T, atol=1e-12) or np.linalg.eigvalsh(B).min() < -1e-12:
        raise ValueError("因子协方差必须对称半正定")
    if lamb < 0 or delta < 0 or epsilon < 0 or (benchmark_deviation is not None and benchmark_deviation < 0):
        raise ValueError("风险、换手、基准偏离和截断参数必须非负")
    raw_returns = r.copy()
    if scale_return:
        if np.std(r) == 0:
            raise ValueError("全相同分数不能缩放")
        r = r / r.std() * np.sqrt(np.mean(np.diag(F @ B @ F.T) + u))
    lb, ub = np.zeros(n), np.ones(n)
    if benchmark_deviation is not None:
        lb, ub = np.maximum(lb, wb - benchmark_deviation), np.minimum(ub, wb + benchmark_deviation)
    for code in force_hold:
        i = assets.index(code)
        lb[i] = ub[i] = old[i]
    for code in force_sell:
        i = assets.index(code)
        lb[i] = ub[i] = 0
    fdev = None if factor_deviation is None else np.broadcast_to(_finite(factor_deviation, "暴露预算"), F.shape[1])
    if fdev is not None and (fdev < 0).any():
        raise ValueError("因子偏离预算不能为负")
    w = cp.Variable(n, nonneg=True)
    d = w - wb
    objective = cp.Maximize(d @ r - lamb * (cp.quad_form(d @ F, B) + u @ cp.square(d)))
    base = [cp.sum(w) == 1, w >= lb, w <= ub]
    if fdev is not None:
        base += [d @ F >= -fdev, d @ F <= fdev]
    turnover = [cp.norm(w - old, 1) <= delta] if old.sum() > 0 else []
    attempts, chosen, relaxed = [], None, False
    for attempt, relax in enumerate((False, True) if allow_turnover_relaxation and turnover else (False,), 1):
        prob = cp.Problem(objective, base + ([] if relax else turnover))
        w.value = wb
        message = None
        try:
            prob.solve(solver=solver, warm_start=True)
            status = str(prob.status)
        except cp.error.SolverError as exc:
            status, message = "solver_error", str(exc)
        attempts.append({"attempt": attempt, "status": status, "message": message,
                         "objective": float(prob.value) if prob.value is not None and math.isfinite(float(prob.value)) else None,
                         "relaxed_constraints": ["turnover_l1"] if relax else []})
        if status in {"optimal", "optimal_inaccurate"} and w.value is not None:
            chosen, relaxed = np.asarray(w.value).copy(), relax
            break
    fallback = chosen is None and retain_on_failure
    if fallback:
        chosen = old.copy()
    before = None if chosen is None else chosen.copy()
    if chosen is not None and not fallback:
        chosen[chosen < epsilon] = 0
        if chosen.sum() > 0:
            chosen /= chosen.sum()
    residuals = {} if chosen is None else constraint_residuals(chosen, w0=old,
        delta=delta if turnover else None, lower=lb, upper=ub, factor=F, benchmark=wb, factor_deviation=fdev)
    active_residuals = {k: v for k, v in residuals.items() if not (relaxed and k == "turnover_l1")}
    accepted = chosen is not None and not fallback and max(active_residuals.values(), default=0) <= tolerance
    status = "failed_retained" if fallback else "failed" if chosen is None else "postprocessing_rejected" if not accepted else "approximate" if attempts[-1]["status"] == "optimal_inaccurate" else "relaxed_success" if relaxed else "original_success"
    return {"assets": assets, "method": "enhanced", "status": status, "accepted": accepted,
        "weights": None if chosen is None else chosen.tolist(), "preprocessing_weights": None if before is None else before.tolist(),
        "attempts": attempts, "constraint_residuals": residuals, "tolerance": tolerance,
        "solver": solver, "solver_version": version("clarabel") if solver == "CLARABEL" else None, "cvxpy_version": version("cvxpy"), "qlib_version": version("pyqlib"),
        "fallback": fallback, "postprocessing": {"epsilon": epsilon, "normalization": "sum_to_one"},
        "relaxed_constraints": ["turnover_l1"] if relaxed else [],
        "constraints": {"lower": lb.tolist(), "upper": ub.tolist(), "delta_l1": delta if turnover else None,
            "factor_deviation": None if fdev is None else fdev.tolist()}, "scale_return": scale_return,
        "inputs": {"expected_returns": r.tolist(), "expected_returns_raw": raw_returns.tolist(), "factor": F.tolist(), "factor_covariance": B.tolist(),
            "residual_variance": u.tolist(), "w0": old.tolist(), "benchmark": wb.tolist()}, "lamb": lamb}


def topk_dropout(scores, *, current_weights, holding_sessions, topk, n_drop, hold_threshold=1, risk_degree=0.9):
    """确定性bottom/top筛选；保留未卖持仓权重，新增证券均分可用权重。"""
    if not scores.index.equals(current_weights.index) or not scores.index.equals(holding_sessions.index):
        raise ValueError("Topk证券索引不一致")
    if type(topk) is not int or not 1 <= topk <= len(scores) or type(n_drop) is not int or not 0 <= n_drop <= topk:
        raise ValueError("Topk或drop数无效")
    s, old = _finite(scores.to_numpy(), "分数"), _finite(current_weights.to_numpy(), "持仓")
    if (old < 0).any() or old.sum() > 1 + 1e-8 or not 0 < risk_degree <= 1:
        raise ValueError("持仓或风险资金比例无效")
    age = _finite(holding_sessions.to_numpy(), "持仓会话数")
    if (age < 0).any() or hold_threshold < 0:
        raise ValueError("持仓会话数不能为负")
    assets = scores.index.astype(str).tolist()
    held = [code for code, weight in zip(assets, old) if weight > 0]
    if len(held) > topk + n_drop:
        raise ValueError("实际持仓数量超过Topk允许的暂时超额范围")
    rank = lambda code: (-float(scores.loc[code]), code)
    last = sorted(held, key=rank)
    today = sorted(set(assets) - set(held), key=rank)[:n_drop + topk - len(last)]
    combined = sorted(set(last) | set(today), key=rank)
    planned = [code for code in last if code in (combined[-n_drop:] if n_drop else [])]
    sell = [code for code in planned if holding_sessions.loc[code] >= hold_threshold]
    buy = today[:len(planned) + topk - len(last)]
    retained = sorted(set(held) - set(sell))
    weights = pd.Series(0., index=scores.index)
    weights.loc[retained] = current_weights.loc[retained]
    free = max(0., 1 - float(weights.sum()))
    if buy:
        weights.loc[buy] = free * risk_degree / len(buy)
    return {"assets": assets, "method": "topk_dropout", "status": "rule_success", "accepted": True,
        "weights": weights.tolist(), "cash_weight": 1 - float(weights.sum()), "selected": sorted(retained + buy),
        "sold": sell, "bought": buy, "retained": retained, "holding_sessions": age.tolist(), "w0": old.tolist(),
        "scores": s.tolist(), "topk": topk, "n_drop": n_drop, "hold_threshold": hold_threshold,
        "risk_degree": risk_degree, "qlib_version": version("pyqlib"), "postprocessing": "none"}
