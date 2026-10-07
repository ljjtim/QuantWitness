"""把单次实际初始持仓调仓接到既有日频现金执行端口。"""
from datetime import datetime
from decimal import Decimal
import json
import math

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from optimizer import estimate_covariance, optimize_portfolio, optimize_enhanced, topk_dropout


def _read(value, name, columns=None):
    rows = []
    for path in value.file_paths:
        if path.startswith(name + "/") and path.endswith(".parquet"):
            rows.extend(pq.ParquetFile(pa.BufferReader(value.read_bytes(path))).read(columns=columns).to_pylist())
    if not rows:
        raise ValueError("组合输入为空：" + name)
    return rows


def _at(day, clock):
    return datetime.fromisoformat(str(day)[:10] + "T" + clock + "+08:00")


def _commit(output_root, port, artifact_type, tables):
    files = []
    for name, rows in tables.items():
        path = output_root / port / (name + "/part-00000.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), path)
        files.append(name + "/part-00000.parquet")
    return output_root.commit_directory(port=port, artifact_type=artifact_type, relative_path=port,
        files=tuple(files), publish_at_artifact_root=True)


def build_targets(predictions, prices, design, source_identity):
    assets, days = design["entities"], design["calendar_sessions"]
    config = design["portfolio_risk"]
    columns = design["price_fields"]
    indexed = {(str(row[columns[0]])[:10], row[columns[1]]): row[columns[2]] for row in prices}
    prediction_days = sorted({str(row["observation_session"])[:10] for row in predictions})
    if not prediction_days:
        raise ValueError("组合没有开发test预测")
    day = prediction_days[0]
    decision, execution = _at(day, "09:31:00"), _at(days[days.index(day)+1], "09:30:00")
    selected = [row for row in predictions if str(row["observation_session"])[:10] == day]
    if len(selected) != len(assets) or {row["entity_id"] for row in selected} != set(assets):
        raise ValueError("调仓预测证券没有完整且唯一覆盖")
    for row in selected:
        if row["stage"] != "test" or not math.isfinite(row["prediction"]):
            raise ValueError("组合只接受有限开发test预测")
        feature = pd.Timestamp(row["feature_available_time"])
        moment = pd.Timestamp(row["decision_time"])
        if feature.tzinfo is None or moment.tzinfo is None or feature > moment or moment > pd.Timestamp(decision):
            raise ValueError("预测晚于决策时点")
    if decision >= datetime.fromisoformat(design["holdout_start"]):
        raise ValueError("最终holdout不能生成开发组合")
    scores = pd.Series({row["entity_id"]: row["prediction"] for row in selected}).reindex(assets)
    returns = []
    for index, session in enumerate(days[:-1]):
        if index == 0:
            continue
        for code in assets:
            first, last = indexed.get((days[index-1], code)), indexed.get((session, code))
            if first is not None and last is not None:
                returns.append({"session": session, "entity_id": code, "return": last / first - 1,
                                "available_at": _at(days[index+1], "09:30:00")})
    covariance, risk = estimate_covariance(returns, assets=assets, decision_time=decision,
        lookback=config["lookback"], estimator=config["estimator"], shrink_alpha=config["shrink_alpha"])
    old = pd.Series(0., index=assets)
    risk["position_snapshot"] = {"source": "simulation_initial_cash", "weights": old.tolist(),
        "cash_weight": 1., "available_at": days[0]+"T00:00:00+08:00"}
    method = config["method"]
    if method == "enhanced":
        result = optimize_enhanced(assets=assets, expected_returns=scores.tolist(), factor=np.eye(len(assets)),
            factor_covariance=covariance.to_numpy(), residual_variance=np.zeros(len(assets)), w0=old.tolist(),
            benchmark=[1/len(assets)] * len(assets), lamb=100., delta=.2, benchmark_deviation=.2,
            factor_deviation=.2, scale_return=False, epsilon=0.)
        risk["benchmark_snapshot"] = {"source": "frozen_equal_weight_synthetic", "available_at": days[0]+"T00:00:00+08:00"}
        risk["factor_snapshot"] = {"source": "frozen_security_identity", "available_at": days[0]+"T00:00:00+08:00"}
    elif method == "topk_dropout":
        result = topk_dropout(scores, current_weights=old, holding_sessions=pd.Series(0, index=assets), topk=3, n_drop=1)
    else:
        result = optimize_portfolio(covariance, method=method, expected_returns=scores,
                                    scale_return=False, lamb=100.)
    if not result["accepted"]:
        raise ValueError("求解结果不能发布执行目标：" + result["status"])
    weights = result["weights"]
    target = {"decision_time": decision.isoformat(), "target_type": "weight",
        "entries": [{"instrument": {"instrument_id": code, "asset_class": "cn_etf", "venue": "XSHG",
            "currency": "CNY", "contract_kind": "etf", "contract_version": "research-instrument-key-v1"},
            "target_type": "weight", "value": weight} for code, weight in zip(assets, weights) if weight > 0],
        "base_currency": "CNY", "cash_weight": max(0., 1 - sum(weights)), "short_allowed": False,
        "leverage_limit": 1., "source_hashes": [source_identity], "contract_version": "research-portfolio-target-v1"}
    targets = [{"decision_time": decision, "order_time": execution,
        "target_json": json.dumps(target, ensure_ascii=False, sort_keys=True), "intent_plan_hash": source_identity}]
    benchmarks = []
    for code in assets:
        price = Decimal(str(indexed[(days[days.index(day)-1], code)])) * 1000
        if price != price.to_integral_value():
            raise ValueError("决策基准报价超过三位小数")
        benchmarks.append({"code": code, "decision_time": decision, "price_units": int(price), "price_scale": 3,
                           "available_at": _at(day, "09:30:00")})
    attempts = result.get("attempts", [{"attempt": 1, "status": "rule_success", "objective": None,
        "message": None, "relaxed_constraints": []}])
    residuals = result.get("constraint_residuals", {"weights_and_cash": abs(sum(weights) + target["cash_weight"] - 1)})
    return {"targets": targets, "benchmarks": benchmarks,
        "risk_inputs": [{"decision_time": decision, "input_json": json.dumps(risk, ensure_ascii=False),
                          "covariance_json": json.dumps(covariance.to_numpy().tolist())}],
        "risk_optimization_results": [{"decision_time": decision, "method": method, "status": result["status"],
            "accepted": result["accepted"], "result_json": json.dumps(result, ensure_ascii=False)}],
        "risk_optimization_attempts": [{"decision_time": decision, "attempt": item["attempt"], "status": item["status"],
            "objective": item.get("objective"), "message": item.get("message"),
            "relaxed_constraints_json": json.dumps(item["relaxed_constraints"])} for item in attempts],
        "risk_constraint_residuals": [{"decision_time": decision, "constraint": name, "violation": value} for name, value in residuals.items()]}


def targets(context, inputs, output_root):
    values = {value.port: value for value in inputs}
    predictions = _read(values["selection"], "test_predictions", ["entity_id", "observation_session", "prediction",
        "decision_time", "feature_available_time", "stage"])
    prices = [row for batch in values["data"].request(context.parameters["price_request_id"]).iter_batches(
        columns=context.parameters["design"]["price_fields"], batch_size=8192) for row in batch.to_pylist()]
    tables = build_targets(predictions, prices, context.parameters["design"], values["selection"].source_identity)
    return _commit(output_root, "targets", "research.portfolio-targets.v1", tables)
