"""风险目标、实际现金执行与独立金融oracle。"""
from copy import deepcopy
from datetime import datetime, time
import importlib.util
import json
import math
from pathlib import Path
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1] / "project_extensions/qlib_portfolio_risk"
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("risk_extension", ROOT/"extension.py")
extension = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extension)
import oracle as risk_oracle


def case(method="inv"):
    days = pd.bdate_range("2024-01-02", periods=32).strftime("%Y-%m-%d").tolist()
    assets = ["SYN000.XSHG", "SYN001.XSHG", "SYN002.XSHG"]
    design = {"calendar_sessions": days, "entities": assets, "holdout_start": days[-2]+"T00:00:00+08:00",
        "price_fields": ["date", "code", "close"], "finance": {"initial_cash_cny": 100000.},
        "portfolio_risk": {"method": method, "estimator": "empirical", "lookback": 20, "shrink_alpha": .1}}
    prices = [{"date": pd.Timestamp(day).date(), "code": code,
        "close": round(10 + number * 3 + .03 * i + .2 * math.sin(i/3 + number), 3)}
        for i, day in enumerate(days) for number, code in enumerate(assets)]
    predictions = [{"entity_id": code, "observation_session": pd.Timestamp(days[23]).date(), "prediction": .001 * (number-1),
        "stage": "test", "decision_time": extension._at(days[23], "09:30:00"),
        "feature_available_time": extension._at(days[23], "09:30:00")}
        for number, code in enumerate(assets)]
    return predictions, prices, design


@pytest.mark.parametrize("method,paused", [(name, False) for name in ("inv", "gmv", "rp", "mvo", "enhanced", "topk_dropout")] + [("inv", True)])
def test_visible_target_to_cash_engine_and_independent_finance_oracle(tmp_path, monkeypatch, method, paused):
    import duckdb
    import sqlite3
    from research_pipeline.runtime.qlib_portfolio_execution import execute_daily_cash_artifact
    from research_pipeline.evidence.financial_oracle.daily_etf import verify_daily_etf_financial_context
    from research_pipeline.evidence.financial_oracle.canonical import verify_canonical_tables
    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("风险组合验收不访问数据库"))
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: pytest.fail("风险组合验收不访问数据库"))
    predictions, prices, design = case(method)
    tables = extension.build_targets(predictions, prices, design, "a"*64)
    for name in ("targets", "benchmarks"):
        folder = tmp_path/name
        folder.mkdir()
        pq.write_table(pa.Table.from_pylist(tables[name]), folder/"part.parquet")
    day = tables["targets"][0]["order_time"].date()
    market = [{"date": day, "code": row["code"], "open": row["close"], "close": row["close"],
        "high_limit": round(row["close"]*1.1, 3), "low_limit": round(row["close"]*.9, 3), "paused": False}
        for row in prices if row["date"] == day]
    if paused:
        market[0]["paused"] = True
    (tmp_path/"market").mkdir()
    pq.write_table(pa.Table.from_pylist(market), tmp_path/"market/part.parquet")
    params = {"market_rule_profile_id": "cn_etf.daily.curated.v1", "instrument_codes": design["entities"],
        "bond_etf_codes": [], "equity_etf_codes": design["entities"], "commission_ppm": 300, "min_commission_units": 5,
        "corporate_actions": [], "initial_cash_cny": 100000., "calendar_id": "synthetic_weekdays",
        "policy_available_at": design["calendar_sessions"][0]+"T00:00:00+08:00"}
    result = execute_daily_cash_artifact(target_root=tmp_path, market_root=tmp_path, parameters=params,
        output_root=tmp_path/"output", max_memory_bytes=64*1024**2, market_artifact_hash="b"*64, target_artifact_hash="c"*64)
    sim = tmp_path/"output/simulation"
    canonical = {name: pq.read_table(sim/"result-contract"/name).to_pylist() for name in ("orders", "fills", "positions", "cash", "costs", "valuations")}
    context = json.loads((sim/"daily-context.json").read_text(encoding="utf-8"))
    manifest = json.loads((sim/"result-contract/manifest.json").read_text(encoding="utf-8"))
    verify_canonical_tables(canonical, frequency=manifest["semantics"]["frequency"])
    verify_daily_etf_financial_context(context=context, canonical=canonical, simulation_manifest=manifest,
        oracle_input={"source_ledger_hash": result["source_ledger_hash"], "policy": {"asset_class": "cn_etf", "bar_frequency": "daily", "rule_snapshot_hash": context["rule_bundle_hash"]}})
    verify_tables = {**tables, "portfolio_targets": tables["targets"], "decision_benchmarks": tables["benchmarks"],
        "test_predictions": predictions, "raw_prices": [{"session": row["date"], "entity_id": row["code"], "close": row["close"]} for row in prices],
        **{"research.simulation."+name: rows for name, rows in canonical.items()},
        "research.daily-simulation.metrics": pq.read_table(sim/"metrics").to_pylist()}
    risk_oracle.verify_portfolio(verify_tables, design)
    assert canonical["fills"]
    if paused:
        assert not any(row["instrument_id"] == market[0]["code"] for row in canonical["fills"])
        assert tables["risk_optimization_results"][0]["status"] == "original_success"
    assert sum(row["fee_units"] for row in canonical["fills"]) > 0
    tampered = deepcopy(verify_tables)
    tampered["risk_optimization_results"][0]["result_json"] = tampered["risk_optimization_results"][0]["result_json"].replace('"accepted": true', '"accepted": false')
    with pytest.raises(ValueError):
        risk_oracle.verify_portfolio(tampered, design)


@pytest.mark.parametrize("attack", ["future_prediction", "holdout", "duplicate", "nan"])
def test_target_rejects_ineligible_prediction(attack):
    predictions, prices, design = case()
    if attack == "future_prediction":
        predictions[0]["decision_time"] = extension._at(design["calendar_sessions"][24], "09:30:00")
    elif attack == "holdout":
        predictions[0]["stage"] = "holdout"
    elif attack == "duplicate":
        predictions.append(predictions[0])
    else:
        predictions[0]["prediction"] = float("nan")
    with pytest.raises(ValueError):
        extension.build_targets(predictions, prices, design, "a"*64)


def test_future_prices_cannot_change_earlier_risk_or_target():
    predictions, prices, design = case()
    baseline = extension.build_targets(predictions, prices, design, "a"*64)
    later = deepcopy(prices)
    for row in later:
        if row["date"] >= predictions[0]["observation_session"]:
            row["close"] *= 3
    assert extension.build_targets(predictions, later, design, "a"*64) == baseline
