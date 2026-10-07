"""公开 Qlib 多期限标签、冻结身份和开发归档边界。"""
from __future__ import annotations

from datetime import date, timedelta
import importlib.util
import json
from pathlib import Path
from statistics import pstdev
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from research_pipeline.platform import typed_canonical_hash

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"
WINDOWS = {
    "train_sessions": 20, "validation_sessions": 10, "test_sessions": 8,
    "step_sessions": 8, "embargo_sessions": 1,
}


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check = load_module("qlib_horizon_example_verifier", EXAMPLE / "verifier/check.py")
operator = load_module("qlib_horizon_example_operator", EXAMPLE / "extension/operator.py")


@pytest.fixture(autouse=True)
def reject_database_connections(monkeypatch):
    import duckdb
    import sqlite3

    def reject(*args, **kwargs):
        pytest.fail("多期限公开示例测试不得连接数据库")

    monkeypatch.setattr(duckdb, "connect", reject)
    monkeypatch.setattr(sqlite3, "connect", reject)


@pytest.fixture(scope="module")
def prepared_research(tmp_path_factory):
    roots = {}
    with pytest.MonkeyPatch.context() as patch:
        import duckdb
        import sqlite3
        patch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("准备研究不得连接数据库"))
        patch.setattr(sqlite3, "connect", lambda *a, **k: pytest.fail("准备研究不得连接数据库"))
        patch.syspath_prepend(str(EXAMPLE))
        module = load_module("qlib_horizon_example_prepare", EXAMPLE / "prepare.py")
        parent = tmp_path_factory.mktemp("ex")
        for mode in ("development", "model"):
            for expanding in (False, True):
                for horizon in (1, 5):
                    root = parent / f"{mode[0]}{horizon}{int(expanding)}"
                    module.prepare(
                        root, mode=mode, horizon_sessions=horizon,
                        window_config={**WINDOWS, "expanding": expanding},
                        model_candidates=[module.candidate(0.1)],
                    )
                    request = json.loads((root / "request.json").read_text(encoding="utf-8"))
                    graph = yaml.safe_load((root / "package/spec/research.yaml").read_text(encoding="utf-8"))["graph"]
                    roots[(mode, expanding, horizon)] = (root, request, {node["node_id"]: node for node in graph["nodes"]})
    return roots


@pytest.mark.parametrize("mode", ["development", "model"])
@pytest.mark.parametrize("expanding", [False, True])
def test_horizons_freeze_separate_identities_and_share_holdout_boundary(prepared_research, mode, expanding):
    identities = []
    boundaries = []
    for horizon in (1, 5):
        root, request, nodes = prepared_research[(mode, expanding, horizon)]
        design = request["design"]
        days = design["calendar_sessions"]
        identity = request["model_parameters"]["research_identity_hash"]
        assert identity == typed_canonical_hash(design)
        identities.append(identity)
        boundaries.append(design["holdout_start"])
        assert design["horizon_sessions"] == horizon
        assert design["expanding"] is expanding
        assert all(design[name] == value for name, value in WINDOWS.items())
        split = nodes["model_split"]["parameters"]
        assert split["horizon_sessions"] == horizon
        assert split["expanding"] is expanding
        assert all(split[name] == value for name, value in WINDOWS.items())
        assert nodes["model_fit"]["parameters"]["research_identity_hash"] == identity
        if mode == "model":
            assert nodes["model_holdout"]["parameters"]["research_identity_hash"] == identity
        else:
            assert "model_holdout" not in nodes
        label = nodes["label"]["parameters"]
        assert label["lineage_ref"] == identity
        assert label["design"]["horizon_sessions"] == horizon
        plan = label["causal_plan"]
        assert plan["key_columns"] == ["entity_id", "observation_session", "horizon_sessions"]
        for work in plan["work_items"]:
            session = work["key_rows"][0][1]
            index = days.index(session)
            assert {row[2] for row in work["key_rows"]} == {horizon}
            assert work["window_end"] == days[index + horizon] + "T15:00:00+08:00"
            assert index + horizon + 1 < len(days)
        if mode == "model":
            assert design["research_sessions"] == days[11:-(horizon + 2)]
            last = days.index(design["research_sessions"][-1])
            assert last + horizon == 99
            assert last + horizon + 1 == 100
    assert boundaries[0] == boundaries[1]
    assert boundaries[0] == days[-23] + "T00:00:00+08:00"
    assert identities[0] != identities[1]


@pytest.mark.parametrize("mode", ["development", "model"])
@pytest.mark.parametrize("horizon", [1, 5])
def test_rolling_and_expanding_have_distinct_frozen_research_identity(prepared_research, mode, horizon):
    rolling = prepared_research[(mode, False, horizon)][1]
    expanding = prepared_research[(mode, True, horizon)][1]
    assert rolling["model_parameters"]["research_identity_hash"] != expanding["model_parameters"]["research_identity_hash"]
    assert rolling["design"]["holdout_start"] == expanding["design"]["holdout_start"]


@pytest.mark.parametrize("expanding", [False, True])
def test_development_horizon_five_archive_has_no_holdout_prices(prepared_research, expanding):
    root, request, nodes = prepared_research[("development", expanding, 5)]
    design = request["design"]
    days = design["calendar_sessions"]
    boundary = date.fromisoformat(design["holdout_start"][:10])
    cutoff = date.fromisoformat(days[days.index(boundary.isoformat()) - 1])
    for kind in ("feature", "label"):
        parts = sorted((root / "inputs" / kind).rglob("*.parquet"))
        assert parts
        archived_days = [value for path in parts for value in pq.ParquetFile(path).read(columns=[design["price_fields"][0]]).column(0).to_pylist()]
        assert max(archived_days) == cutoff
        assert all(day < boundary for day in archived_days)
    last = days.index(design["research_sessions"][-1])
    assert last + 5 == days.index(cutoff.isoformat())
    assert all(days.index(day) + 5 <= days.index(cutoff.isoformat()) for day in design["research_sessions"])
    assert "model_selection" not in nodes and "model_holdout" not in nodes


def label_case(horizon=5):
    days = [date(2024, 1, 2) + timedelta(days=i) for i in range(23)]
    prices = [100.0 + i + (i % 4) * 0.5 for i in range(len(days))]
    sessions = days[11:14]
    design = {
        "calendar_sessions": [str(day) for day in days],
        "research_sessions": [str(day) for day in sessions],
        "entities": ["SYN"], "horizon_sessions": horizon,
    }
    tables = {
        "raw_prices": [{"entity_id": "SYN", "session": day, "close": close} for day, close in zip(days, prices)],
        "features": [], "labels": [],
    }
    for session in sessions:
        index = days.index(session)
        for window in (5, 10):
            history = prices[index - window - 1:index]
            returns = [right / left - 1 for left, right in zip(history, history[1:])]
            for name, value in (("historical_return", history[-1] / history[0] - 1), ("volatility", pstdev(returns))):
                tables["features"].append({
                    "entity_id": "SYN", "observation_session": session,
                    "window_sessions": window, "feature_id": name,
                    "value": value, "status": "ok",
                    "observation_time": check._at(days[index - 1], 15),
                    "available_time": check._at(session, 9, 30),
                })
        tables["labels"].append({
            "entity_id": "SYN", "observation_session": session,
            "horizon_sessions": horizon,
            "forward_return": prices[index + horizon] / prices[index] - 1,
            "decision_time": check._at(session, 9, 30),
            "label_start_time": check._at(session, 15),
            "label_end_time": check._at(days[index + horizon], 15),
            "available_time": check._at(days[index + horizon + 1], 9, 30),
        })
    return tables, design, days, prices


def test_independent_horizon_five_labels_match_hand_calculated_prices():
    tables, design, days, prices = label_case()
    samples = check._inputs(tables, design)
    assert set(samples) == {f"SYN:{day}:h5" for day in days[11:14]}
    for sid, sample in samples.items():
        index = days.index(sample["observation_session"])
        assert sample["target"] == prices[index + 5] / prices[index] - 1
        assert sample["label_end_time"] == check._at(days[index + 5], 15)
        assert sample["label_available_time"] == check._at(days[index + 6], 9, 30)


@pytest.mark.parametrize("change", ["wrong_horizon", "late_available", "early_available", "horizon_one_value", "wrong_end", "design_horizon"])
def test_independent_inputs_reject_changed_horizon_or_label_fact(change):
    tables, design, days, prices = label_case()
    row = tables["labels"][0]
    if change == "wrong_horizon":
        row["horizon_sessions"] = 1
    elif change == "late_available":
        row["available_time"] += timedelta(hours=1)
    elif change == "early_available":
        row["available_time"] -= timedelta(days=1)
    elif change == "horizon_one_value":
        row["forward_return"] = prices[12] / prices[11] - 1
    elif change == "wrong_end":
        row["label_end_time"] = check._at(days[12], 15)
    else:
        design["horizon_sessions"] = 1
    with pytest.raises(ValueError, match="标签"):
        check._inputs(tables, design)


def prediction_case():
    tables, design, _, _ = label_case()
    sid, sample = next(iter(check._inputs(tables, design).items()))
    row = {
        "sample_id": sid, "stage": "validation", "horizon_sessions": 5,
        "entity_id": sample["entity_id"], "observation_session": sample["observation_session"],
        "prediction": 0.0, "actual": sample["target"], "raw_label": sample["target"],
        "evaluation_label": sample["target"], "score_semantics": "raw_return_prediction",
        "feature_available_time": sample["decision_time"],
        **{name: sample[name] for name in ("decision_time", "label_start_time", "label_end_time", "label_available_time")},
    }
    return row, sample


def test_independent_prediction_accepts_raw_horizon_five_label():
    row, sample = prediction_case()
    assert check._prediction(row, sample, "validation") == sample["target"] ** 2


@pytest.mark.parametrize("change", ["horizon", "late_available", "horizon_one_value"])
def test_independent_prediction_rejects_wrong_horizon_value_or_time(change):
    row, sample = prediction_case()
    if change == "horizon":
        row["horizon_sessions"] = 1
    elif change == "late_available":
        row["label_available_time"] += timedelta(hours=1)
    else:
        _, _, _, prices = label_case()
        row["actual"] = prices[12] / prices[11] - 1
    with pytest.raises(ValueError):
        check._prediction(row, sample, "validation")


def test_label_operator_uses_declared_horizon_end_price():
    tables, design, days, prices = label_case()
    design["price_fields"] = ["session", "entity_id", "close"]
    work = {"key_rows": [["SYN", str(days[11]), 5]], "decision_time": check._at(days[11], 9, 30).isoformat()}
    plan = {"kind": "label", "output_port": "labels", "key_columns": ["entity_id", "observation_session", "horizon_sessions"], "work_items": [work]}
    context = SimpleNamespace(parameters={"causal_plan": plan, "design": design, "lineage_ref": "1" * 64})
    source = SimpleNamespace(iter_batches=lambda **kwargs: pa.Table.from_pylist(tables["raw_prices"]).to_batches())
    saved = []

    def write_batches(**kwargs):
        saved.extend(pa.Table.from_batches(kwargs["batches"]).to_pylist())
        return "labels"

    assert operator.run(context, [source], SimpleNamespace(write_batches=write_batches)) == "labels"
    assert saved[0]["forward_return"] == prices[16] / prices[11] - 1
    assert saved[0]["label_end_time"] == check._at(days[16], 15)
    assert saved[0]["horizon_sessions"] == 5
    plan["work_items"][0]["key_rows"][0][2] = 1
    with pytest.raises(ValueError, match="期限"):
        operator.run(context, [source], SimpleNamespace(write_batches=write_batches))


@pytest.mark.parametrize("kwargs", [{"horizon_sessions": 5}, {"window_config": {"expanding": False}}])
def test_portfolio_rejects_changed_horizon_or_window(tmp_path, monkeypatch, kwargs):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    prepare = load_module("qlib_portfolio_horizon_prepare", EXAMPLE / "prepare.py")
    with pytest.raises(ValueError, match="组合示例"):
        prepare.prepare(tmp_path / "portfolio", mode="portfolio", **kwargs)
