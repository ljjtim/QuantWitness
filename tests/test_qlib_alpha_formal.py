"""固定因子正式声明、归档字段、可见时间与生产算法手算验收。"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import date
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest
import yaml

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"


@contextmanager
def example_extension_scope():
    """为本例绑定 extension 命名空间，退出后恢复同进程其他测试的模块。"""
    def belongs(name):
        return name == "extension" or name.startswith("extension.")

    previous = {name: module for name, module in sys.modules.copy().items() if belongs(name)}
    spec = importlib.machinery.ModuleSpec("extension", loader=None, is_package=True)
    spec.submodule_search_locations = [str(EXAMPLE / "extension")]
    try:
        for name in previous:
            sys.modules.pop(name, None)
        sys.modules["extension"] = importlib.util.module_from_spec(spec)
        yield
    finally:
        for name in tuple(sys.modules):
            if belongs(name):
                sys.modules.pop(name)
        sys.modules.update(previous)


@pytest.fixture(autouse=True)
def example_imports(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    with example_extension_scope():
        yield


def test_example_extension_scope_restores_foreign_module_and_children():
    foreign = ModuleType("extension")
    child = ModuleType("extension.foreign")
    with pytest.MonkeyPatch.context() as modules:
        modules.setitem(sys.modules, "extension", foreign)
        modules.setitem(sys.modules, "extension.foreign", child)
        with example_extension_scope():
            from extension import factor_baselines
            assert Path(factor_baselines.__file__).resolve() == EXAMPLE / "extension/factor_baselines.py"
            assert "extension.foreign" not in sys.modules
        assert sys.modules["extension"] is foreign
        assert sys.modules["extension.foreign"] is child
        assert "extension.factor_baselines" not in sys.modules


@pytest.fixture(autouse=True)
def database_boundary(monkeypatch):
    import duckdb
    import sqlite3
    def reject(*args, **kwargs):
        pytest.fail("因子接线测试不得连接数据库")
    monkeypatch.setattr(duckdb, "connect", reject)
    monkeypatch.setattr(sqlite3, "connect", reject)


@pytest.fixture
def prepared(tmp_path, monkeypatch, request):
    pytest.importorskip("qlib")
    monkeypatch.syspath_prepend(str(EXAMPLE))
    from prepare import prepare
    name = getattr(request, "param", "volume_price_v1")
    root = tmp_path / name
    bundle = prepare(root, "model", feature_suite=name, research_series={
        "series_id": "factor-confirmation", "root": str(tmp_path / "series"),
        "purpose": "量价因子接线", "legacy_ledger_roots": []})
    design = json.loads((root / "request.json").read_text(encoding="utf-8"))["design"]
    graph = yaml.safe_load((root / "package/spec/research.yaml").read_text(encoding="utf-8"))["graph"]
    return root, bundle, design, graph


@pytest.mark.parametrize("prepared", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"], indirect=True)
def test_formal_prepare_freezes_factor_keys_fields_and_metric_definition(prepared):
    root, bundle, design, graph = prepared
    from run import execute
    assert execute(root, "lint", "model")["status"] == "linted"
    suite = design["feature_suite"]
    nodes = {node["node_id"]: node for node in graph["nodes"]}
    causal = nodes["feature"]["parameters"]["causal_plan"]
    item = causal["work_items"][0]
    expected = {(code, design["research_sessions"][0], spec["window_sessions"], name)
                for code in design["entities"] for name, spec in suite["features"].items()}
    assert {tuple(key) for key in item["key_rows"]} == expected
    assert set(design["factor_fields"].values()) <= set(causal["sources"][0]["columns"])
    index = design["calendar_sessions"].index(design["research_sessions"][0])
    assert item["window_start"].startswith(design["calendar_sessions"][index-suite["lookback"]-1])
    assert design["feature_columns"] == sorted(f"{name}__w{spec['window_sessions']}" for name, spec in suite["features"].items())
    specification = yaml.safe_load((root / "package/spec/research.yaml").read_text(encoding="utf-8"))
    definition, = specification["metric_definitions"]
    assert definition["implementation_ref"] == "project.qlib_demo.operator.prediction_metric"
    metric_table = next(item for item in specification["result"]["tables"] if item["table_id"] == "metrics")
    assert metric_table["source_node_id"] == "summary"
    config = json.loads((root / "input-config.json").read_text(encoding="utf-8"))
    from input_config import load_input_config
    own, _, _ = load_input_config(root / "input-config.json", mode="model", feature_fields=suite["fields"])
    assert own["columns"] == config["columns"]


def fixture_tables(design):
    sys.path.insert(0, str(EXAMPLE / "extension"))
    try:
        from factor_baseline_oracle import independent_factor_values
    finally:
        sys.path.remove(str(EXAMPLE / "extension"))
    import synthetic
    design = deepcopy(design)
    calendar = design["calendar_sessions"]
    raw, features, labels = [], [], []
    for source in synthetic.rows(include_factor_fields=True):
        raw.append({"entity_id": source["fld_equity_daily_code"], "session": source["fld_equity_daily_date"],
                    **{role: source[field] for role, field in design["factor_fields"].items()}})
    indexed = {(row["entity_id"], row["session"].isoformat()): row for row in raw}
    for entity in design["entities"]:
        for day in design["research_sessions"]:
            index = calendar.index(day)
            history = [indexed[(entity, session)] for session in calendar[index-design["feature_suite"]["lookback"]-1:index]]
            values = independent_factor_values(design["feature_suite"], history)
            for name, value in values.items():
                features.append({"entity_id": entity, "observation_session": day, "feature_id": name,
                    "window_sessions": design["feature_suite"]["features"][name]["window_sessions"],
                    "value": value, "status": "ok" if value is not None else "missing",
                    "observation_time": calendar[index-1] + "T15:00:00+08:00", "available_time": day + "T09:30:00+08:00"})
            close = indexed[(entity, day)]["close"]
            future = indexed[(entity, calendar[index+1])]["close"]
            labels.append({"entity_id": entity, "observation_session": day, "horizon_sessions": 1,
                "forward_return": future / close - 1, "decision_time": day + "T09:30:00+08:00",
                "label_start_time": day + "T15:00:00+08:00", "label_end_time": calendar[index+1] + "T15:00:00+08:00",
                "available_time": calendar[index+2] + "T09:30:00+08:00"})
    return {"raw_prices": raw, "features": features, "labels": labels}, design


@pytest.mark.parametrize("prepared", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"], indirect=True)
def test_formal_factor_output_matches_hand_calculation_and_previous_close(prepared, monkeypatch):
    import pyarrow as pa
    from types import SimpleNamespace
    from extension import operator
    import synthetic
    monkeypatch.syspath_prepend(str(EXAMPLE / "extension"))
    _, _, design, graph = prepared
    expected, _ = fixture_tables({**design, "research_sessions": design["research_sessions"][:1]})
    node = next(item for item in graph["nodes"] if item["node_id"] == "feature")
    parameters = deepcopy(node["parameters"])
    parameters["causal_plan"]["work_items"] = parameters["causal_plan"]["work_items"][:1]
    day = design["research_sessions"][0]
    source = SimpleNamespace(iter_batches=lambda **kwargs: pa.Table.from_pylist(
        list(synthetic.rows(include_factor_fields=True))).select(kwargs["columns"]).to_batches())
    actual = []
    def write_batches(**kwargs):
        actual.extend(pa.Table.from_batches(kwargs["batches"]).to_pylist())
        return "features"
    assert operator.run(SimpleNamespace(parameters=parameters), [source], SimpleNamespace(write_batches=write_batches)) == "features"
    wanted = {(row["entity_id"], row["feature_id"]): row
              for row in expected["features"] if row["observation_session"] == day}
    assert len(actual) == len(wanted)
    for row in actual:
        reference = wanted[row["entity_id"], row["feature_id"]]
        assert row["status"] == reference["status"]
        if reference["value"] is None:
            assert row["value"] is None
        else:
            tolerance = 1e-8 if row["feature_id"] in {"price_volume_correlation5", "CORR5"} else 1e-12
            assert row["value"] == pytest.approx(reference["value"], abs=tolerance, rel=0)
        assert row["observation_time"].isoformat() == reference["observation_time"]
        assert row["available_time"].isoformat() == reference["available_time"]


@pytest.mark.parametrize("mutation", ["definition", "omitted_feature"])
def test_fixed_factor_production_rejects_changed_frozen_definitions(prepared, mutation):
    from extension.factor_baseline_oracle import validate_factor_suite
    suite = deepcopy(prepared[2]["feature_suite"])
    if mutation == "definition":
        suite["features"]["volume_ratio5"]["formula"] = "$volume/Mean($volume,4)"
    else:
        suite["features"].pop("volume_ratio5")
    with pytest.raises(ValueError):
        validate_factor_suite(suite)


def test_fixed_factor_suite_accepts_worker_frozen_sequences(prepared):
    from research_pipeline.extensions.project_bundle import _freeze_payload
    from extension.factor_baseline_oracle import validate_factor_suite
    _, _, design, _ = prepared
    validate_factor_suite(_freeze_payload(design["feature_suite"]))


@pytest.mark.parametrize("suite_name", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"])
def test_formal_operator_never_consumes_decision_day_or_future_bars(monkeypatch, suite_name):
    pytest.importorskip("qlib")
    monkeypatch.syspath_prepend(str(EXAMPLE))
    monkeypatch.syspath_prepend(str(EXAMPLE / "extension"))
    from extension.factor_baselines import factor_suite
    import synthetic
    spec = importlib.util.spec_from_file_location("alpha_operator_time", EXAMPLE / "extension/operator.py")
    operator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(operator)
    suite = factor_suite(suite_name)
    calendar = [day.isoformat() for day in synthetic.sessions()]
    field_map = {"close": "fld_equity_daily_close", "open": "fld_demo_open", "high": "fld_demo_high",
                 "low": "fld_demo_low", "vwap": "fld_demo_vwap", "volume": "fld_demo_volume"}
    design = {"feature_suite": suite, "calendar_sessions": calendar,
              "factor_fields": {field: field_map[field] for field in suite["fields"]}}
    bars = {(row["fld_equity_daily_code"], row["fld_equity_daily_date"].isoformat()): row
            for row in synthetic.rows(include_factor_fields=True)}
    entity, day = synthetic.instruments()[0], calendar[15]
    keys = [[entity, day, item["window_sessions"], name] for name, item in suite["features"].items()]
    columns = ["entity_id", "observation_session", "window_sessions", "feature_id"]
    original = operator._factor_values(design, bars, keys, columns)
    changed = deepcopy(bars)
    for (code, session), row in changed.items():
        if session >= day:
            for field in field_map.values():
                row[field] *= 2.0
    assert operator._factor_values(design, changed, keys, columns) == original


def test_public_resume_rejects_redeclaring_frozen_factor_suite(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    import run
    with pytest.raises(SystemExit):
        run.main(["--output", str(tmp_path), "--stage", "resume", "--factor-suite", "alpha158_selected_v1"])


@pytest.mark.parametrize("suite_name,factor", [("volume_price_v1", "price_volume_correlation5"), ("alpha158_selected_v1", "CORR5")])
def test_low_variance_native_correlation_matches_hand_calculation(monkeypatch, suite_name, factor):
    pytest.importorskip("qlib")
    import pandas as pd
    monkeypatch.syspath_prepend(str(EXAMPLE))
    monkeypatch.syspath_prepend(str(EXAMPLE / "extension"))
    from extension.factor_baselines import factor_suite, evaluate_factor_suite
    from extension.factor_baseline_oracle import independent_factor_values
    import synthetic
    suite = factor_suite(suite_name)
    calendar = [day.isoformat() for day in synthetic.sessions()]
    day = calendar[12]
    raw = []
    history_closes = [10.34, 10.34, 10.34, 10.34, 10.34, 10.35]
    history_volumes = [14421., 14336., 14267., 14231., 14239., 14301.]
    for index, session in enumerate(calendar[:15]):
        close = history_closes[index-6] if 6 <= index < 12 else 10.34 + index * 0.01
        volume = history_volumes[index-6] if 6 <= index < 12 else 14000. + index * 31
        raw.append({"session": date.fromisoformat(session), "entity_id": "A", "close": close,
                    "volume": volume, "open": close - 0.01, "high": close + 0.1, "low": close - 0.1, "vwap": close + 0.01})
    history = raw[6:12]
    frame = pd.DataFrame([{**row, "datetime": pd.Timestamp(row["session"]), "instrument": "A"} for row in history]).set_index(["datetime", "instrument"])
    observed = evaluate_factor_suite(frame, suite, sessions=pd.to_datetime(calendar[6:12])).iloc[-1]
    expected = independent_factor_values(suite, history)
    assert abs(observed[factor] - expected[factor]) <= 1e-8
    assert abs(observed[factor] - expected[factor]) > 1e-12


@pytest.mark.parametrize("expanding", [False, True])
def test_fixed_factor_split_uses_previous_observation_and_purges_unavailable_labels(prepared, expanding):
    import pandas as pd
    from research_pipeline.research.modeling import assemble_daily_model_samples
    from research_pipeline.research.validation.splits import build_walk_forward
    _, _, original, _ = prepared
    design = deepcopy(original)
    design["research_sessions"] = design["calendar_sessions"][12:40]
    tables, _ = fixture_tables(design)
    for row in tables["features"] + tables["labels"]:
        row["lineage_hash"] = "factor-contract"
    samples, _ = assemble_daily_model_samples(pd.DataFrame(tables["features"]), pd.DataFrame(tables["labels"]), horizon_sessions=1)
    frame = samples.rename(columns={"label_start_time": "label_start", "label_end_time": "label_end"})
    calendar = design["calendar_sessions"]
    for row in samples.itertuples():
        assert row.observation_time.date().isoformat() == calendar[calendar.index(str(row.observation_session)) - 1]
    split = build_walk_forward(frame, calendar=calendar[11:39], train_sessions=8,
        validation_sessions=4, test_sessions=4, step_sessions=4, embargo_sessions=1, expanding=expanding)
    assert len(split.folds) >= 2
    indexed = frame.set_index("sample_id")
    for fold in split.folds:
        train, valid, test = (indexed.loc[list(ids)] for ids in (fold.train_ids, fold.validation_ids, fold.test_ids))
        assert (train.label_end < valid.decision_time.min()).all()
        assert (train.label_available_time <= valid.decision_time.min()).all()
        assert (valid.label_end < test.decision_time.min()).all()
        assert (valid.label_available_time <= test.decision_time.min()).all()
        assert fold.purged_ids
        assert set(fold.train_ids).isdisjoint(fold.validation_ids)
    if expanding:
        assert set(split.folds[0].train_ids) <= set(split.folds[1].train_ids)
    else:
        assert not set(split.folds[0].train_ids) <= set(split.folds[1].train_ids)


@pytest.mark.parametrize("suite_name", ["volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"])
def test_public_factor_all_prepares_in_separate_process(tmp_path, monkeypatch, suite_name):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    import run
    prepared_calls, stages = [], []
    monkeypatch.setattr(run.subprocess, "run", lambda args, **kwargs: prepared_calls.append((args, kwargs)))
    monkeypatch.setattr(run, "execute", lambda root, stage, *args, **kwargs: stages.append(stage) or {})
    run.main(["--output", str(tmp_path), "--factor-suite", suite_name,
        "--sequence-step-len", "3", "--horizon-sessions", "5",
        "--window-mode", "rolling", "--train-sessions", "20",
        "--research-series-id", "factor-study", "--research-series-root", str(tmp_path / "series"),
        "--research-series-purpose", "因子研究接线"])
    assert len(prepared_calls) == 2
    arguments, options = prepared_calls[0]
    assert options == {"check": True}
    assert arguments[:3] == [sys.executable, "-B", str(EXAMPLE / "run.py")]
    frozen = dict(zip(arguments[3::2], arguments[4::2]))
    assert frozen == {"--output": str(tmp_path.resolve()), "--mode": "model",
        "--stage": "prepare", "--factor-suite": suite_name,
        "--sequence-step-len": "3", "--horizon-sessions": "5",
        "--window-mode": "rolling", "--train-sessions": "20",
        "--research-series-id": "factor-study", "--research-series-root": str(tmp_path / "series"),
        "--research-series-purpose": "因子研究接线"}
    assert stages == ["lint", "admit", "run"]
    for (arguments, options), stage in zip(prepared_calls[1:], ("report",)):
        assert options == {"check": True}
        assert arguments == [sys.executable, "-B", str(EXAMPLE / "run.py"),
            "--output", str(tmp_path.resolve()), "--mode", "model", "--stage", stage]


def test_public_factor_prepare_failure_stops_formal_run(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    import run
    stages = []
    def fail_prepare(arguments, **kwargs):
        raise run.subprocess.CalledProcessError(2, arguments)
    monkeypatch.setattr(run.subprocess, "run", fail_prepare)
    monkeypatch.setattr(run, "execute", lambda root, stage, *args, **kwargs: stages.append(stage))
    with pytest.raises(run.subprocess.CalledProcessError):
        run.main(["--output", str(tmp_path), "--factor-suite", "alpha158_selected_v1",
            "--research-series-id", "factor-study", "--research-series-root", str(tmp_path / "series"),
            "--research-series-purpose", "因子研究接线"])
    assert stages == []
