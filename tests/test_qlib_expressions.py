"""有限 Qlib 表达式的手算、因果方向、局部缓存与冻结处理器检查。"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

SOURCE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio/extension/expressions.py"
_spec = importlib.util.spec_from_file_location("public_qlib_expressions", SOURCE)
_expressions = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _expressions
_spec.loader.exec_module(_expressions)
QlibExpressionError = _expressions.QlibExpressionError
baseline_expressions = _expressions.baseline_expressions
evaluate_expressions = _expressions.evaluate_expressions
validate_expression = _expressions.validate_expression
from research_pipeline.research.modeling.qlib import QlibModelError, fit_bundle, normalize_candidates, predict_bundle


def _frame():
    index = pd.MultiIndex.from_product([pd.date_range("2024-01-02", periods=6), ["A", "B"]], names=["datetime", "instrument"])
    return pd.DataFrame({"close": [1, 10, 2, 20, 4, 40, 8, 80, 16, 160, 32, 320],
                         "volume": [2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6]}, index=index)


@pytest.mark.parametrize("expression", [
    "Ref($close, -1)", "Ref(Ref($close,-1), 2)", "Ref($close, 0)",
    "Mean($close,0)", "EMA($close,5)", "Mean($close,1.5)",
    "Mean($close,$volume)", "Mask($close, 'OTHER')", "$close.iloc[-1]",
    "__import__('os')", "Mean($unknown,5)", "Mean($close, N=3)",
    "1", "Mean($close, 252) + Ref($close,253)", "Ref(Ref($close,200),100)",
])
def test_reject_future_unknown_or_unbounded_expression(expression):
    with pytest.raises(QlibExpressionError):
        validate_expression(expression, ["close", "volume"])


def test_static_dependency_accumulates_nested_windows():
    spec = validate_expression("Mean($close / Ref($close, 5) - 1, 20)", ["close"])
    assert spec.lookback == 24
    assert spec.fields == ("close",)
    assert set(spec.operators) == {"Mean", "Div", "Ref", "Sub"}
    assert spec.to_dict()["lookback"] == 24


def test_manual_returns_and_rolling_full_window():
    pytest.importorskip("qlib")
    frame = _frame().iloc[::-1]
    result = evaluate_expressions(frame, {"return2": "$close/Ref($close,2)-1", "mean": "Mean($close,3)"}, fields=["close"])
    assert result.index.equals(frame.index)
    values = result.xs("A", level="instrument").sort_index()
    assert values["return2"].iloc[:2].isna().all()
    np.testing.assert_allclose(values["return2"].iloc[2:], [3, 3, 3, 3])
    np.testing.assert_allclose(values["mean"].iloc[2:], [7/3, 14/3, 28/3, 56/3])
    assert result.attrs["qlib_expressions"]["features"]["return2"]["lookback"] == 2


def test_full_and_qlib_partial_windows_are_explicit():
    pytest.importorskip("qlib")
    frame = _frame()
    full = evaluate_expressions(frame, {"mean": "Mean($close,3)"}, fields=["close"], min_periods="full")
    partial = evaluate_expressions(frame, {"mean": "Mean($close,3)"}, fields=["close"], min_periods="qlib")
    assert full.iloc[:4]["mean"].isna().all()
    np.testing.assert_allclose(partial.xs("A", level="instrument")["mean"].iloc[:3], [1, 1.5, 7/3])
    with pytest.raises(QlibExpressionError, match="min_periods"):
        evaluate_expressions(frame, {"mean": "Mean($close,3)"}, fields=["close"], min_periods="automatic")


def test_missing_instrument_session_is_not_skipped_by_ref():
    pytest.importorskip("qlib")
    frame = _frame().drop((pd.Timestamp("2024-01-03"), "A"))
    result = evaluate_expressions(frame, {"lag": "Ref($close,1)", "mean": "Mean($close,2)"}, fields=["close"])
    assert np.isnan(result.loc[("2024-01-04", "A"), "lag"])
    assert np.isnan(result.loc[("2024-01-04", "A"), "mean"])
    assert result.loc[("2024-01-04", "B"), "lag"] == 20


def test_explicit_calendar_keeps_market_wide_missing_session():
    pytest.importorskip("qlib")
    frame = _frame().drop(pd.Timestamp("2024-01-03"), level="datetime")
    result = evaluate_expressions(frame, {"lag": "Ref($close,1)"}, fields=["close"], sessions=pd.date_range("2024-01-02", periods=6))
    assert result.loc[pd.Timestamp("2024-01-04"), "lag"].isna().all()


def test_future_perturbation_does_not_change_past_and_output_is_sliced_after_warmup():
    pytest.importorskip("qlib")
    frame = _frame()
    expressions = {"relative_mean": "$close/Mean($close,3)-1", "return": "$close/Ref($close,2)-1"}
    before = evaluate_expressions(frame, expressions, fields=["close"])
    future = frame.copy()
    future.loc[future.index.get_level_values("datetime") > "2024-01-05", "close"] *= 100
    after = evaluate_expressions(future, expressions, fields=["close"])
    pd.testing.assert_frame_equal(before.loc[:"2024-01-05"], after.loc[:"2024-01-05"])
    sliced = evaluate_expressions(frame, expressions, fields=["close"], output_start="2024-01-05", output_end="2024-01-06")
    pd.testing.assert_frame_equal(sliced, before.loc["2024-01-05":"2024-01-06"])


def test_no_global_provider_or_expression_cache(monkeypatch):
    qlib = pytest.importorskip("qlib")
    from qlib.data.cache import H
    monkeypatch.setattr(qlib, "init", lambda *args, **kwargs: pytest.fail("不得配置全局 provider"))
    global_cache = H["f"]
    original = list(global_cache.od)
    first = evaluate_expressions(_frame(), {"mean": "Mean($close,2)"}, fields=["close"])
    second = evaluate_expressions(_frame() * 10, {"mean": "Mean($close,2)"}, fields=["close"])
    np.testing.assert_allclose(second["mean"], first["mean"] * 10, equal_nan=True)
    assert H["f"] is global_cache
    assert list(global_cache.od) == original


def test_corr_and_nested_windows_use_complete_inputs():
    pytest.importorskip("qlib")
    result = evaluate_expressions(_frame(), {"corr": "Corr($close,$close,3)", "nested": "Mean(Ref($close,2),3)"}, fields=["close"])
    np.testing.assert_allclose(result["corr"].iloc[4:], 1)
    assert result["nested"].iloc[:8].isna().all()
    assert result.loc[("2024-01-06", "A"), "nested"] == pytest.approx(7/3)


def test_native_baselines_load_definitions_without_labels_or_data():
    pytest.importorskip("qlib")
    alpha158 = baseline_expressions("Alpha158")
    alpha360 = baseline_expressions("Alpha360")
    assert len(alpha158) == 158
    assert len(alpha360) == 360
    assert baseline_expressions("Alpha158", selected=["MA5"], fields=["close"]) == {"MA5": "Mean($close, 5)/$close"}
    assert validate_expression(alpha360["CLOSE59"], ["close"]).lookback == 59
    with pytest.raises(QlibExpressionError, match="不存在"):
        baseline_expressions("Alpha158", selected=["LABEL0"])
    with pytest.raises(QlibExpressionError, match="未声明"):
        baseline_expressions("Alpha360", fields=["close"])


def test_native_selected_baseline_manual_value():
    pytest.importorskip("qlib")
    result = evaluate_expressions(_frame(), baseline_expressions("Alpha158", selected=["MA5"], fields=["close"]), fields=["close"], min_periods="qlib")
    assert result.loc[("2024-01-06", "A"), "MA5"] == pytest.approx(31 / 5 / 16)


def test_input_identity_and_nonfinite_values_are_rejected():
    pytest.importorskip("qlib")
    duplicated = pd.concat([_frame(), _frame().iloc[:1]])
    with pytest.raises(QlibExpressionError, match="重复"):
        evaluate_expressions(duplicated, {"x": "$close"}, fields=["close"])
    with pytest.raises(QlibExpressionError, match="无穷值"):
        evaluate_expressions(_frame(), {"x": "$close/($close-$close)"}, fields=["close"])


def _candidate(processor="ZScoreNorm"):
    return {"candidate_id": "ridge", "model": {"class": "LinearModel", "module_path": "qlib.contrib.model.linear", "kwargs": {"estimator": "ridge", "alpha": 0.1}},
            "processors": {"infer": [{"class": processor, "kwargs": {"fields_group": "feature"}}], "learn": [{"class": "DropnaLabel", "kwargs": {}}]}, "fit": {}}


def test_new_processors_preserve_scope_constraints():
    for name in ("ZScoreNorm", "CSZScoreNorm"):
        candidate = _candidate(name)
        assert normalize_candidates([candidate])[0]["processors"]["infer"][0]["class"] == name
        candidate["processors"]["infer"][0]["kwargs"]["fit_end_time"] = "2099-01-01"
        with pytest.raises(QlibModelError, match="拟合区间"):
            normalize_candidates([candidate])
    candidate = _candidate("CSZScoreNorm")
    candidate["processors"]["infer"][0]["kwargs"]["method"] = "future"
    with pytest.raises(QlibModelError, match="method"):
        normalize_candidates([candidate])


def test_zscore_bundle_is_fit_on_train_only_and_restored(tmp_path, monkeypatch):
    pytest.importorskip("qlib")
    import sqlite3
    from qlib.utils.serial import Serializable
    monkeypatch.setattr(sqlite3, "connect", lambda *args, **kwargs: pytest.fail("此测试不得创建数据库"))
    rows = []
    for day, date in enumerate(pd.date_range("2024-01-02", periods=8, tz="UTC")):
        for asset in range(2):
            x = day * 2 + asset if day < 4 else 100 + day * 2 + asset
            rows.append({"sample_id": f"{day}-{asset}", "entity_id": str(asset), "observation_session": date.date(),
                         "decision_time": date + pd.Timedelta(hours=16), "feature_available_time": date + pd.Timedelta(hours=15),
                         "label_available_time": date + pd.Timedelta(hours=17), "label_end_time": date + pd.Timedelta(hours=17),
                         "target": 0.2 * x + 1, "x": float(x)})
    frame = pd.DataFrame(rows)
    train, valid = frame.iloc[:8], frame.iloc[8:12]
    row = fit_bundle(train, valid, candidate=_candidate(), feature_columns=["x"], output_root=tmp_path,
                     bundle_path="bundle", root_seed=1, fit_scope_ref="train-only")
    config = json.loads((tmp_path / row["config_path"]).read_text(encoding="utf-8"))
    processor = Serializable.load(tmp_path / config["processor_files"]["infer"][0])
    assert processor.mean_train[0] == pytest.approx(3.5)
    assert processor.std_train[0] == pytest.approx(np.std(np.arange(8)))
    assert pd.Timestamp(processor.fit_end_time).date() == pd.Timestamp("2024-01-05").date()
    prediction = predict_bundle(tmp_path, row, frame.iloc[12:])
    assert np.isfinite(prediction).all()
    np.testing.assert_allclose(prediction, predict_bundle(tmp_path, row, frame.iloc[12:]))


@pytest.mark.parametrize("name,count", [("Alpha158", 158), ("Alpha360", 360)])
def test_all_native_feature_definitions_execute_on_complete_synthetic_bars(name, count):
    pytest.importorskip("qlib")
    sessions = pd.date_range("2024-01-02", periods=70)
    index = pd.MultiIndex.from_product([sessions, ["A", "B"]], names=["datetime", "instrument"])
    day = np.repeat(np.arange(70), 2)
    asset = np.tile([0, 1], 70)
    close = 10 + day * 0.07 + asset + np.sin(day / 4) * 0.3
    frame = pd.DataFrame({"close": close, "open": close * 0.99, "high": close * 1.02,
                          "low": close * 0.98, "vwap": close * 1.001,
                          "volume": 1000 + day * 3 + np.cos(day / 3) * 20 + asset * 100}, index=index)
    result = evaluate_expressions(frame, baseline_expressions(name), fields=list(frame.columns),
                                  min_periods="qlib", sessions=sessions)
    assert result.shape == (140, count)
    assert np.isfinite(result.iloc[-2:].to_numpy()).all()
