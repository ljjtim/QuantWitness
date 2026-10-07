"""从封存风险输入独立复算协方差、目标约束和规则。"""
import json
import math
from datetime import datetime


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dot(a, b):
    return sum(x * y for x, y in zip(a, b))


def matrix_vector(matrix, vector):
    return [dot(row, vector) for row in matrix]


def equal(a, b, tolerance=1e-7):
    return math.isclose(a, b, rel_tol=tolerance, abs_tol=tolerance)


def verify_covariance(inputs, covariance):
    returns = inputs["returns"]
    n, count = len(inputs["assets"]), len(returns)
    require(count == inputs["lookback"] and count >= 2, "风险样本窗口不同")
    require(len(inputs["sessions"]) == count and inputs["sessions"] == sorted(set(inputs["sessions"])), "风险日历无序或重复")
    require(datetime.fromisoformat(inputs["available_at"]) <= datetime.fromisoformat(inputs["decision_time"]), "风险数据晚于决策")
    means = [sum(row[i] for row in returns) / count for i in range(n)]
    sample = [[sum((row[i] - means[i]) * (row[j] - means[j]) for row in returns) / count
               for j in range(n)] for i in range(n)]
    if inputs["estimator"] == "shrink":
        alpha = inputs["shrink_alpha"]
        require(isinstance(alpha, (int, float)) and 0 <= alpha <= 1, "收缩系数无效")
        diagonal_mean = sum(sample[i][i] for i in range(n)) / n
        sample = [[(1 - alpha) * sample[i][j] + alpha * diagonal_mean * (i == j) for j in range(n)] for i in range(n)]
    else:
        require(inputs["estimator"] == "empirical", "未知风险估计方法")
    require(len(covariance) == n and all(len(row) == n for row in covariance), "协方差形状不同")
    require(all(equal(covariance[i][j], sample[i][j], 1e-10) for i in range(n) for j in range(n)), "协方差无法从封存收益独立复算")


def verify_optimization(result):
    assets, w = result["assets"], result["weights"]
    require(len(set(assets)) == len(assets), "目标证券重复")
    if not result["accepted"]:
        require(result["status"] not in {"original_success", "relaxed_success", "rule_success"}, "失败标为成功")
        return
    require(w is not None and len(w) == len(assets) and all(math.isfinite(x) for x in w), "权重没有绑定证券身份")
    if result["method"] == "topk_dropout":
        verify_topk(result)
        return
    tol = result["tolerance"]
    require(not result["fallback"], "回退结果标为优化成功")
    require(equal(sum(w), 1, tol), "最终权重未满仓")
    residuals = {"full_investment": abs(sum(w) - 1)}
    constraints = result["constraints"]
    lower, upper = constraints.get("lower", [0.] * len(w)), constraints.get("upper", [1.] * len(w))
    residuals.update(lower_bound=max(0., max(a - b for a, b in zip(lower, w))),
                     upper_bound=max(0., max(a - b for a, b in zip(w, upper))))
    inputs = result.get("inputs", {})
    old = inputs.get("w0", result.get("w0"))
    delta = constraints["delta_l1"]
    if delta is not None and old is not None:
        residuals["turnover_l1"] = max(0., sum(abs(a - b) for a, b in zip(w, old)) - delta)
    fdev = constraints.get("factor_deviation")
    if fdev is not None:
        d = [a - b for a, b in zip(w, inputs["benchmark"])]
        F = inputs["factor"]
        exposure = [sum(d[i] * F[i][j] for i in range(len(w))) for j in range(len(F[0]))]
        residuals["factor_deviation"] = max(0., max(abs(x) - limit for x, limit in zip(exposure, fdev)))
    require(result["constraint_residuals"].keys() == residuals.keys(), "约束残差集合不同")
    require(all(equal(value, result["constraint_residuals"][key], 1e-10) for key, value in residuals.items()), "约束残差无法独立复算")
    relaxed = result.get("relaxed_constraints", [])
    require(all(value <= tol for key, value in residuals.items() if key not in relaxed), "最终目标违反生效约束")
    attempts = result["attempts"]
    require(attempts and attempts[-1]["status"] in {"optimal", "optimal_inaccurate"}, "目标没有真实成功状态")
    require(result["status"] != "original_success" or not relaxed, "放宽问题标为原约束成功")
    if result["method"] == "inv":
        inv = [1 / math.sqrt(result["covariance"][i][i]) for i in range(len(w))]
        require(all(equal(value, expected / sum(inv)) for value, expected in zip(w, inv)), "逆波动目标不符")
    if result["method"] in {"gmv", "mvo", "rp"}:
        S = result["covariance"]
        Sw = matrix_vector(S, w)
        variance = dot(w, Sw)
        if result["method"] == "gmv":
            objective = variance
        elif result["method"] == "mvo":
            returns = result["expected_returns"]
            if result["scale_return"]:
                mean = sum(returns) / len(returns)
                std = math.sqrt(sum((x - mean)**2 for x in returns) / len(returns))
                returns = [x / std * math.sqrt(sum(S[i][i] for i in range(len(w))) / len(w)) for x in returns]
            objective = -dot(w, returns) + result["parameters"]["lamb"] * variance
        else:
            objective = sum((w[i] - variance / Sw[i] / len(w))**2 for i in range(len(w)))
        objective += result["parameters"]["alpha"] * sum(x*x for x in w)
        require(equal(attempts[-1]["objective"], objective), "Qlib优化目标值无法独立复算")
    if result["method"] == "enhanced":
        raw = result["preprocessing_weights"]
        original_returns = inputs["expected_returns_raw"]
        expected_returns = list(original_returns)
        if result["scale_return"]:
            mean = sum(original_returns) / len(original_returns)
            std = math.sqrt(sum((x - mean)**2 for x in original_returns) / len(original_returns))
            F, B, u = inputs["factor"], inputs["factor_covariance"], inputs["residual_variance"]
            diagonal = [dot(F[i], matrix_vector(B, F[i])) + u[i] for i in range(len(raw))]
            expected_returns = [value / std * math.sqrt(sum(diagonal) / len(diagonal)) for value in original_returns]
        require(all(equal(a, b, 1e-12) for a, b in zip(expected_returns, inputs["expected_returns"])), "增强指数分数缩放不同")
        expected = [0. if x < result["postprocessing"]["epsilon"] else x for x in raw]
        require(sum(expected) > 0, "后处理没有剩余权重")
        expected = [x / sum(expected) for x in expected]
        require(all(equal(x, y, 1e-10) for x, y in zip(w, expected)), "目标与封存后处理不同")
        d = [a - b for a, b in zip(raw, inputs["benchmark"])]
        exposure = [sum(d[i] * inputs["factor"][i][j] for i in range(len(w))) for j in range(len(inputs["factor"][0]))]
        risk = dot(exposure, matrix_vector(inputs["factor_covariance"], exposure)) + dot(inputs["residual_variance"], [x * x for x in d])
        objective = dot(d, inputs["expected_returns"]) - result["lamb"] * risk
        require(equal(attempts[-1]["objective"], objective), "增强指数目标值不同")


def verify_topk(result):
    assets, old, score = result["assets"], result["w0"], result["scores"]
    scores = dict(zip(assets, score))
    age = dict(zip(assets, result["holding_sessions"]))
    held = [code for code, value in zip(assets, old) if value > 0]
    ordered = sorted(held, key=lambda code: (-scores[code], code))
    new = sorted(set(assets) - set(held), key=lambda code: (-scores[code], code))[:result["n_drop"] + result["topk"] - len(held)]
    universe = sorted(set(ordered) | set(new), key=lambda code: (-scores[code], code))
    bottom = universe[-result["n_drop"]:] if result["n_drop"] else []
    proposed_sell = [code for code in ordered if code in bottom]
    sell = [code for code in proposed_sell if age[code] >= result["hold_threshold"]]
    retained = sorted(set(held) - set(sell))
    buy = new[:len(proposed_sell) + result["topk"] - len(held)]
    expected = dict(zip(assets, old))
    expected = {code: expected[code] if code in retained else 0. for code in assets}
    free = 1 - sum(expected.values())
    for code in buy:
        expected[code] = free * result["risk_degree"] / len(buy)
    require(result["sold"] == sell and result["bought"] == buy and result["retained"] == retained, "Topk换仓或持仓期限不同")
    require(all(equal(expected[code], value, 1e-12) for code, value in zip(assets, result["weights"])), "Topk目标不能从实际持仓复算")
    require(equal(result["cash_weight"], 1 - sum(result["weights"])), "Topk现金权重不同")


def verify_portfolio(tables, design):
    results = tables["risk_optimization_results"]
    require(len(results) == 1, "单次调仓示例必须封存唯一目标")
    result = json.loads(results[0]["result_json"])
    risk = json.loads(tables["risk_inputs"][0]["input_json"])
    verify_covariance(risk, json.loads(tables["risk_inputs"][0]["covariance_json"]))
    verify_optimization(result)
    require(result["accepted"], "失败组合不得进入执行")
    attempts = result.get("attempts", [{"attempt": 1, "status": "rule_success", "objective": None, "message": None, "relaxed_constraints": []}])
    rows = tables["risk_optimization_attempts"]
    require(len(rows) == len(attempts), "独立求解尝试表缺失")
    for row, attempt in zip(rows, attempts):
        require(row["attempt"] == attempt["attempt"] and row["status"] == attempt["status"] and row["objective"] == attempt.get("objective") and row["message"] == attempt.get("message") and json.loads(row["relaxed_constraints_json"]) == attempt["relaxed_constraints"], "求解尝试表与目标证据不同")
    residuals = result["constraint_residuals"] if "constraint_residuals" in result else {"weights_and_cash": abs(sum(result["weights"]) + result["cash_weight"] - 1)}
    actual_residuals = {row["constraint"]: row["violation"] for row in tables["risk_constraint_residuals"]}
    require(actual_residuals == residuals, "约束残差表与目标证据不同")
    require(risk["assets"] == design["entities"] == result["assets"], "风险与目标证券身份不同")
    require(result["method"] == design["portfolio_risk"]["method"], "目标方法与冻结设计不同")
    covariance = json.loads(tables["risk_inputs"][0]["covariance_json"])
    if result["method"] in {"inv", "gmv", "rp", "mvo"}:
        require(result["covariance"] == covariance, "优化协方差不是正式风险估计")
    elif result["method"] == "enhanced":
        inputs = result["inputs"]
        n = len(result["assets"])
        require(inputs["factor"] == [[float(i == j) for j in range(n)] for i in range(n)] and inputs["factor_covariance"] == covariance and inputs["residual_variance"] == [0.] * n, "增强指数风险输入未绑定正式估计")
        require(inputs["benchmark"] == [1/n] * n, "合成基准权重与冻结定义不同")
        require(inputs["w0"] == risk["position_snapshot"]["weights"], "增强指数原持仓不是实际初始化状态")
        for name in ("factor_snapshot", "benchmark_snapshot"):
            require(datetime.fromisoformat(risk[name]["available_at"]) <= datetime.fromisoformat(risk["decision_time"]), "历史基准或风险暴露尚不可见")
    require(risk["position_snapshot"]["weights"] == [0.] * len(result["assets"]), "初始现金示例混入未初始化持仓")
    require(risk["position_snapshot"]["source"] == "simulation_initial_cash", "持仓未绑定实际初始化状态")
    target_row = tables["portfolio_targets"][0]
    target = json.loads(target_row["target_json"])
    entries = {item["instrument"]["instrument_id"]: item["value"] for item in target["entries"]}
    require(set(entries) == {code for code, value in zip(result["assets"], result["weights"]) if value > 0}, "目标证券和优化输出不同")
    require(all(equal(entries.get(code, 0.), value, 1e-10) for code, value in zip(result["assets"], result["weights"])), "执行目标权重不同")
    require(equal(target["cash_weight"], 1 - sum(result["weights"])), "执行目标现金比例不同")
    decision = target_row["decision_time"]
    require(decision.isoformat() == risk["decision_time"], "风险和目标时点不同")
    require(decision < datetime.fromisoformat(design["holdout_start"]), "最终holdout进入组合")
    day = decision.date().isoformat()
    scores = {row["entity_id"]: row["prediction"] for row in tables["test_predictions"] if str(row["observation_session"])[:10] == day}
    require(set(scores) == set(result["assets"]), "目标未绑定完整开发test预测")
    if result["method"] in {"mvo", "enhanced", "topk_dropout"}:
        supplied = result["scores"] if result["method"] == "topk_dropout" else result["expected_returns"] if result["method"] == "mvo" else result["inputs"]["expected_returns"]
        require(all(equal(scores[code], value, 1e-10) for code, value in zip(result["assets"], supplied)), "组合分数未绑定正式模型预测")
    benchmark = {row["code"]: row for row in tables["decision_benchmarks"]}
    prices = {(str(row["session"])[:10], row["entity_id"]): row["close"] for row in tables["raw_prices"]}
    previous = design["calendar_sessions"][design["calendar_sessions"].index(day)-1]
    calendar = design["calendar_sessions"]
    require(risk["sessions"] == calendar[calendar.index(day)-risk["lookback"]:calendar.index(day)], "风险窗口不是决策前的固定完整会话")
    for session, returns in zip(risk["sessions"], risk["returns"]):
        prior = calendar[calendar.index(session)-1]
        for code, value in zip(risk["assets"], returns):
            require(equal(value, prices[session, code] / prices[prior, code] - 1, 1e-12), "风险收益没有绑定封存原始行情")
    require(datetime.fromisoformat(risk["available_at"]).date().isoformat() == day, "风险末端成熟会话不同")
    for code in result["assets"]:
        require(benchmark[code]["price_units"] == int(round(prices[previous, code] * 1000)), "决策基准不是当时已知前收盘")
        require(benchmark[code]["available_at"] <= decision, "决策基准晚于决策")
    for order in tables["research.simulation.orders"]:
        order_decision = order["decision_time"]
        order_decision = datetime.fromisoformat(order_decision) if isinstance(order_decision, str) else order_decision
        require(order_decision == decision, "交易订单不能绑定目标时点")
    fills = tables["research.simulation.fills"]
    valuations = sorted(tables["research.simulation.valuations"], key=lambda row: row["valuation_time"])
    require(valuations, "现金模拟没有估值")
    initial = design.get("finance", {}).get("initial_cash_cny", 100000.)
    nav = [row["nav_units"] / 100 for row in valuations]
    peak, drawdown = nav[0], 0.
    for value in nav:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1)
    expected_metrics = {"portfolio.total_return@1.0.0": nav[-1] / initial - 1,
        "portfolio.max_drawdown@1.0.0": drawdown,
        "portfolio.turnover@1.0.0": sum(row["notional_units"] for row in fills) / 100 / initial,
        "portfolio.transaction_cost@1.0.0": sum(row["fee_units"] for row in fills) / 100}
    metrics = {row["metric_ref"]: row["value"] for row in tables["research.daily-simulation.metrics"]}
    require(set(metrics) == set(expected_metrics), "金融指标集合不同")
    require(all(equal(metrics[ref], value) for ref, value in expected_metrics.items()), "金融指标不能从正式交易账本复算")
