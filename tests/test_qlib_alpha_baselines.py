"""固定因子的逐项手算、原生窗口、缺失会话和未来扰动验收。"""
from __future__ import annotations

from copy import deepcopy
import importlib
import json
from pathlib import Path
import sqlite3
import sys

import numpy as np
import pandas as pd
import pytest

_EXTENSION = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio/extension"
sys.path.insert(0, str(_EXTENSION))
try:
    _baselines = importlib.import_module("factor_baselines")
    _oracle = importlib.import_module("factor_baseline_oracle")
finally:
    sys.path.remove(str(_EXTENSION))

factor_suite = _baselines.factor_suite
evaluate_factor_suite = _baselines.evaluate_factor_suite
independent_factor_values = _oracle.independent_factor_values
validate_factor_suite = _oracle.validate_factor_suite


@pytest.fixture(autouse=True)
def _forbid_database_connections(monkeypatch):
    import duckdb

    def reject(*args, **kwargs):
        pytest.fail("固定因子测试不得连接数据库")

    monkeypatch.setattr(sqlite3, "connect", reject)
    monkeypatch.setattr(duckdb, "connect", reject)


def _frame():
    sessions = pd.bdate_range("2024-01-02", periods=18)
    rows = []
    for day, session in enumerate(sessions):
        for asset, instrument in enumerate(("A", "B")):
            close = 10 + asset * 2 + day * 0.13 + np.sin(day / 2 + asset) * 0.5
            opening = close + np.cos(day / 3) * 0.2
            rows.append({"datetime": session, "instrument": instrument, "close": close,
                         "open": opening, "high": max(close, opening) + 0.4,
                         "low": min(close, opening) - 0.3, "vwap": (opening + close) / 2,
                         "volume": 1000 + asset * 150 + day * 12 + np.cos(day) * 110})
    return pd.DataFrame(rows).set_index(["datetime", "instrument"]), sessions


def _assert_oracle(frame, sessions, suite):
    actual = evaluate_factor_suite(frame, suite, sessions=sessions)
    for instrument in frame.index.get_level_values("instrument").unique():
        history = frame.xs(instrument, level="instrument").reindex(sessions).to_dict("records")
        for number, session in enumerate(sessions):
            if (session, instrument) not in frame.index:
                continue
            expected = independent_factor_values(suite, history[:number + 1])
            for feature, value in expected.items():
                observed = actual.loc[(session, instrument), feature]
                if value is None:
                    assert pd.isna(observed), (suite["name"], feature, session, observed)
                else:
                    assert observed == pytest.approx(value, rel=1e-10, abs=1e-12), (suite["name"], feature, session)
    return actual


@pytest.mark.parametrize("name,count,fields", [
    ("volume_price_v1", 8, ["close", "volume"]),
    ("alpha158_selected_v1", 22, ["close", "high", "low", "open", "volume", "vwap"]),
    ("alpha360_selected_v1", 18, ["close", "high", "low", "open", "volume", "vwap"]),
])
def test_fixed_suite_declares_native_definition_and_finite_history(name, count, fields):
    pytest.importorskip("qlib")
    suite = factor_suite(name)
    validate_factor_suite(suite)
    assert len(suite["features"]) == count and suite["fields"] == fields
    assert suite["lookback"] == 5
    assert suite["min_periods"] == ("full" if name == "volume_price_v1" else "qlib")
    assert all(spec["window_sessions"] == spec["lookback"] + 1 for spec in suite["features"].values())
    assert json.loads(json.dumps(suite)) == suite
    if suite["baseline"]:
        from qlib.contrib.data.loader import Alpha158DL, Alpha360DL
        definitions, names = {"Alpha158": Alpha158DL, "Alpha360": Alpha360DL}[suite["baseline"]].get_feature_config()
        native = dict(zip(names, definitions))
        assert all(spec["formula"] == native[name] for name, spec in suite["features"].items())


@pytest.mark.parametrize("name", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"])
@pytest.mark.parametrize("policy", ["full", "qlib"])
def test_every_selected_feature_matches_independent_scalar_arithmetic(name, policy):
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    result = _assert_oracle(frame, sessions, factor_suite(name, min_periods=policy))
    assert result.iloc[-2:].notna().all().all()


@pytest.mark.parametrize("name", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"])
@pytest.mark.parametrize("policy", ["full", "qlib"])
def test_missing_sessions_and_fields_keep_native_window_semantics(name, policy):
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    frame = frame.drop(index=(sessions[7], "A"))
    frame.loc[(sessions[10], "B"), "volume"] = np.nan
    frame.loc[(sessions[12], "B"), "close"] = np.nan
    _assert_oracle(frame, sessions, factor_suite(name, min_periods=policy))


@pytest.mark.parametrize("name", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"])
@pytest.mark.parametrize("policy", ["full", "qlib"])
def test_future_price_and_volume_changes_do_not_change_visible_past(name, policy):
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    suite = factor_suite(name, min_periods=policy)
    original = evaluate_factor_suite(frame, suite, sessions=sessions)
    changed = frame.copy()
    future = changed.index.get_level_values("datetime") > sessions[9]
    changed.loc[future, ["open", "high", "low", "close", "vwap"]] *= 1.7
    changed.loc[future, "volume"] *= 0.43
    altered = evaluate_factor_suite(changed, suite, sessions=sessions)
    past = original.index.get_level_values("datetime") <= sessions[9]
    pd.testing.assert_frame_equal(original.loc[past], altered.loc[past])
    cropped = evaluate_factor_suite(frame, suite, sessions=sessions, output_start=sessions[8], output_end=sessions[13])
    pd.testing.assert_frame_equal(original.loc[(slice(sessions[8], sessions[13]), slice(None)), :], cropped)


@pytest.mark.parametrize("name", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"])
def test_flat_prices_and_zero_volume_are_explicit_native_values(name):
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    for field in ("open", "high", "low", "close", "vwap"):
        frame[field] = 10.0
    frame["volume"] = 0.0
    result = _assert_oracle(frame, sessions, factor_suite(name))
    assert not np.isinf(result.to_numpy()).any()
    if "CORR5" in result:
        assert result["CORR5"].isna().all()
    if "price_volume_correlation5" in result:
        assert result["price_volume_correlation5"].isna().all()
    if "VOLUME0" in result:
        assert result["VOLUME0"].eq(0).all()


def test_known_numeric_samples_have_direct_expected_values():
    pytest.importorskip("qlib")
    history = [{"open": 10.0, "high": 14.0, "low": 8.0, "close": close,
                "vwap": 11.0, "volume": volume}
               for close, volume in zip([10., 11., 12., 10., 13., 12.], [100., 200., 300., 400., 500., 600.])]
    volume = independent_factor_values(factor_suite("volume_price_v1"), history)
    assert volume["volume_ratio5"] == pytest.approx(600 / 400)
    assert volume["volume_change1"] == pytest.approx(0.2)
    assert volume["volume_change_balance5"] == pytest.approx(1)
    alpha = independent_factor_values(factor_suite("alpha158_selected_v1"), history)
    assert alpha["KMID"] == pytest.approx(0.2)
    assert alpha["ROC5"] == pytest.approx(10 / 12)
    assert alpha["MA5"] == pytest.approx((11 + 12 + 10 + 13 + 12) / 5 / 12)
    assert alpha["VMA5"] == pytest.approx(400 / 600)
    alpha360 = independent_factor_values(factor_suite("alpha360_selected_v1"), history)
    assert alpha360["CLOSE5"] == pytest.approx(10 / 12)
    assert alpha360["VOLUME5"] == pytest.approx(100 / 600)


@pytest.mark.parametrize("mutation", ["formula", "fields", "lookback", "operators", "missing_feature", "version"])
def test_independent_validator_rejects_formula_or_history_drift(mutation):
    suite = deepcopy(factor_suite("volume_price_v1"))
    if mutation == "missing_feature":
        suite["features"].pop("volume_ratio5")
    elif mutation == "version":
        suite["qlib_version"] = "0.9.8"
    else:
        spec = suite["features"]["volume_ratio5"]
        spec[mutation] = {"formula": "$volume/(Mean($volume,4)+1e-12)", "fields": ["close"],
                          "lookback": 3, "operators": ["Mean"]}[mutation]
    with pytest.raises(ValueError):
        validate_factor_suite(suite)


def test_invalid_market_signs_are_rejected_without_zero_filling():
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    suite = factor_suite("volume_price_v1")
    frame.iloc[0, frame.columns.get_loc("volume")] = -1
    with pytest.raises(ValueError, match="非负"):
        evaluate_factor_suite(frame, suite, sessions=sessions)
    with pytest.raises(ValueError, match="非负"):
        independent_factor_values(suite, frame.xs("A", level="instrument").to_dict("records"))


def test_market_wide_missing_session_is_not_compressed():
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    frame = frame.drop(index=sessions[7], level="datetime")
    suite = factor_suite("alpha360_selected_v1")
    actual = _assert_oracle(frame, sessions, suite)
    assert pd.isna(actual.loc[(sessions[8], "A"), "CLOSE1"])


def test_infinite_market_input_is_rejected_by_calculation_and_oracle():
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    suite = factor_suite("volume_price_v1")
    frame.iloc[0, frame.columns.get_loc("volume")] = np.inf
    with pytest.raises(ValueError, match="无穷值"):
        evaluate_factor_suite(frame, suite, sessions=sessions)
    with pytest.raises(ValueError, match="无穷值"):
        independent_factor_values(suite, frame.xs("A", level="instrument").to_dict("records"))


def test_actual_qlib_version_must_match_frozen_suite(monkeypatch):
    pytest.importorskip("qlib")
    frame, sessions = _frame()
    suite = factor_suite("volume_price_v1")
    monkeypatch.setattr(_baselines, "version", lambda name: "0.9.8")
    with pytest.raises(ValueError, match="Qlib0.9.7"):
        evaluate_factor_suite(frame, suite, sessions=sessions)


def test_worker_frozen_suite_preserves_dataframe_attributes_on_selection():
    pytest.importorskip("qlib")
    from research_pipeline.extensions.project_bundle import _freeze_payload
    frame, sessions = _frame()
    suite = _freeze_payload(factor_suite("volume_price_v1"))
    calculated = evaluate_factor_suite(frame, suite, sessions=sessions)
    assert np.isfinite(calculated.loc[(sessions[-1], "A"), "volume_ratio5"])
    assert isinstance(calculated.attrs["factor_suite"]["features"]["volume_ratio5"]["fields"], list)
