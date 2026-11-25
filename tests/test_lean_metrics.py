"""LEAN正式指标的期初基线、部分成交、定义证明和历史读取。"""
from collections import Counter
from copy import deepcopy
from decimal import Decimal
import json
import math
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from research_pipeline.platform.metric_contracts import compose_metric_registry
from research_pipeline.research.lean_metrics import lean_metric_definitions, lean_metric_rows, lean_metrics_table


@pytest.mark.parametrize("frequency,semantics", [
    ("1d", "latest_visible_mark_not_session_close"), ("1m", "session_close_mark"),
    ("1m", "execution_window_mark"),
])
def test_plan_metric_definition_matches_actual_account(frequency, semantics):
    from types import SimpleNamespace
    from research_pipeline.research.lean_metrics import lean_metric_definitions_for_nodes

    nodes = [SimpleNamespace(parameters={"execution_backend": "lean", "frequency": frequency})]
    planned = {item.metric_ref: item.to_dict() for item in lean_metric_definitions_for_nodes(nodes)}
    rows = lean_metric_rows([{"date": "2024-01-08", "equity": 100, "cash": 100,
                              "equity_semantics": semantics}], [], initial_equity=100)
    assert {row["metric_ref"]: json.loads(row["definition_json"]) for row in rows} == planned


def test_complete_and_partial_minutes_share_declared_window_metric_semantics():
    daily = [{"date": "2024-01-08", "equity": 100, "cash": 100, "equity_semantics": "session_close_mark"},
             {"date": "2024-01-09", "equity": 110, "cash": 110, "equity_semantics": "execution_window_mark"}]
    rows = lean_metric_rows(daily, [], initial_equity=100)
    total = next(row for row in rows if row["metric_ref"].startswith("project.lean.total_return@"))
    assert total["value"] == pytest.approx(0.1)
    assert json.loads(total["definition_json"])["frequency"] == "daily_execution_window"


@pytest.mark.parametrize("terminal_cash", [0, 10])
def test_delisted_terminal_observation_projects_value_cash_and_actual_time(terminal_cash):
    from research_pipeline.platform.lean_result_projection import lean_research_daily

    daily = [{"date": "2024-01-08", "equity": 100, "cash": 0,
              "valuation_at": "2024-01-08T15:00:00+08:00", "equity_semantics": "session_close_mark"}]
    original = deepcopy(daily)
    terminal = dict(equity=terminal_cash, cash=terminal_cash,
        terminal_observation_at="2024-01-09T15:00:00+08:00", valuation_at="2024-01-09T15:00:00+08:00",
        valuation_marks=[dict(instrument_id="000001.XSHE", source="delisted_zero", price=0)])
    projected = lean_research_daily(daily, final_account=terminal)
    assert daily == original
    assert len(projected) == 1 and projected[0]["equity"] == 100 and projected[0]["cash"] == 0
    assert projected[0]["research_equity"] == terminal_cash
    assert projected[0]["research_cash"] == terminal_cash
    assert projected[0]["research_valuation_at"] == terminal["valuation_at"]
    rows = lean_metric_rows(daily, [], initial_equity=100, expected_dates=["2024-01-08"], final_account=terminal)
    metrics = {row["metric_ref"].split("@", 1)[0].removeprefix("project.lean."): row["value"] for row in rows}
    assert metrics["final_equity"] == terminal_cash
    assert metrics["total_return"] == pytest.approx(terminal_cash / 100 - 1)
    assert metrics["capital_utilization_mean"] == 0
    assert all(row["sample_size"] == 1 and row["sample_end"] == "2024-01-08" for row in rows)
from research_pipeline.results import ResultSnapshot
from research_pipeline.platform.lean_contract import (
    LEAN_ARTIFACT_TYPE, LEAN_JSONL_FILES, LEAN_METRICS_PATH, LEAN_METRICS_SCHEMA_ID, LEAN_PATHS,
)
from research_pipeline.results.metrics import metrics_from_snapshot


def observations():
    daily = [{"date": "2024-01-08", "equity": 90, "cash": 50},
             {"date": "2024-01-09", "equity": 99, "cash": 60}]
    orders = [dict(order_id=1, quantity=50, price=1, fee=0.5, status="PartiallyFilled", at="2024-01-08T09:32:00+08:00"),
              dict(order_id=1, quantity=50, price=1, fee=0.5, status="Filled", at="2024-01-08T09:33:00+08:00"),
              dict(order_id=2, quantity=-20, price=1, fee=0.2, status="Filled", at="2024-01-09T01:32:00+00:00"),
              dict(order_id=3, quantity=0, price=0, fee=0, status="Submitted", at="2024-01-09T09:31:00+08:00")]
    return daily, orders


def metric_snapshot(table=None):
    daily, orders = observations()
    table = lean_metrics_table(daily, orders, initial_equity=100) if table is None else table
    manifest = SimpleNamespace(table_id="lean_metrics", schema_id=LEAN_METRICS_SCHEMA_ID,
        source_node_id="simulation", source_port="simulation", artifact_type=LEAN_ARTIFACT_TYPE,
        path_prefix=LEAN_METRICS_PATH, artifact_key="simulation-artifact")
    definitions = lean_metric_definitions()
    proofs = compose_metric_registry(definitions).prove(tuple(d.metric_ref for d in definitions),
        (("simulation", "simulation", LEAN_ARTIFACT_TYPE),), (manifest,))
    bundle = SimpleNamespace(tables=[manifest], metric_proofs=proofs, result_id="result.lean",
                             metric_definitions=())
    return ResultSnapshot(bundle, Path("I:/temp/quantwitness-lean-integration-20261029"),
                          {LEAN_METRICS_SCHEMA_ID: table}, {})


def test_first_day_loss_partial_fills_and_population_risk_are_computed_from_facts():
    daily, orders = observations()
    rows = lean_metric_rows(daily, orders, initial_equity=100, expected_dates=[r["date"] for r in daily])
    values = {row["metric_ref"].split("@", 1)[0].removeprefix("project.lean."): row["value"] for row in rows}
    assert values["total_return"] == pytest.approx(-0.01)
    assert values["annualized_return"] == pytest.approx(0.99 ** 126 - 1)
    assert values["max_drawdown"] == pytest.approx(0.1)
    assert values["annualized_volatility"] == pytest.approx(0.1 * math.sqrt(252))
    assert values["sharpe"] == pytest.approx(0)
    assert values["fee_total"] == pytest.approx(1.2)
    assert values["gross_traded_notional"] == 120
    assert values["fill_count"] == 3
    assert values["filled_order_count"] == 2
    assert values["order_count"] == 3
    assert values["turnover_mean"] == pytest.approx((0.5 + 20 / 180) / 2)
    assert values["capital_utilization_mean"] == pytest.approx(((90-50)/90 + (99-60)/99)/2)
    assert all(row["sample_size"] == 2 and row["status"] == "computed" for row in rows)


def test_no_trade_flat_returns_are_finite_and_initial_drawdown_is_preserved():
    rows = lean_metric_rows([{"date": "2024-01-08", "equity": 100, "cash": 100}], [], initial_equity=100)
    values = {row["metric_ref"]: row["value"] for row in rows}
    assert values["project.lean.sharpe@2.0.0"] == 0
    assert values["project.lean.fee_total@2.0.0"] == 0
    assert values["project.lean.fill_count@2.0.0"] == 0
    assert all(math.isfinite(value) for value in values.values())


@pytest.mark.parametrize("mutation", ["missing_day", "duplicate_day", "zero_equity", "nan", "non_fill", "outside_fill"])
def test_incomplete_daily_series_and_invalid_execution_facts_fail(mutation):
    daily, orders = observations()
    expected_dates = [r["date"] for r in daily]
    if mutation == "missing_day":
        daily.pop()
    if mutation == "duplicate_day":
        daily.append(deepcopy(daily[-1]))
    if mutation == "zero_equity":
        daily[0]["equity"] = -1
    if mutation == "nan":
        daily[0]["equity"] = float("nan")
    if mutation == "non_fill":
        orders[0]["status"] = "Submitted"
    if mutation == "outside_fill":
        orders[0]["at"] = "2024-01-10T09:31:00+08:00"
    with pytest.raises(ValueError):
        lean_metric_rows(daily, orders, initial_equity=100, expected_dates=expected_dates)


def test_formal_proofs_read_frozen_definitions_without_requiring_current_production_code(monkeypatch):
    snapshot = metric_snapshot()
    import research_pipeline.research.lean_metrics as production
    monkeypatch.setattr(production, "lean_metric_definitions", lambda: (_ for _ in ()).throw(AssertionError("不得重算历史定义")))
    metrics = metrics_from_snapshot(snapshot)
    assert len(metrics) == 14
    assert next(m.value for m in metrics if m.metric_ref == "project.lean.total_return@2.0.0") == pytest.approx(-0.01)


def test_latest_visible_valuation_is_explicit_in_metric_definition():
    daily, orders = observations()
    for row in daily:
        row["equity_semantics"] = "latest_visible_mark_not_session_close"
    rows = lean_metric_rows(daily, orders, initial_equity=100)
    for row in rows:
        definition = json.loads(row["definition_json"])
        assert definition["frequency"] == "daily_latest_visible_mark"
        assert "latest_visible_mark" in definition["measurement_semantics"]["observation_timing"]
    daily[-1]["equity_semantics"] = "session_close_mark"
    with pytest.raises(ValueError, match="混用权益"):
        lean_metric_rows(daily, orders, initial_equity=100)


def test_changed_frozen_definition_is_rejected_by_its_digest():
    snapshot = metric_snapshot()
    rows = snapshot.tables[LEAN_METRICS_SCHEMA_ID].to_pylist()
    definition = json.loads(rows[0]["definition_json"])
    definition["unit"] = "wrong"
    rows[0]["definition_json"] = json.dumps(definition)
    table = pa.Table.from_pylist(rows, schema=snapshot.tables[LEAN_METRICS_SCHEMA_ID].schema)
    with pytest.raises(ValueError, match="digest"):
        metrics_from_snapshot(metric_snapshot(table))


def test_runtime_publishes_typed_metrics_and_preserves_stage_timings(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    from research_pipeline.platform.lean_contract import LEAN_SUPPORT_PATHS
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from research_pipeline.runtime.lean_strategy_execution import _publish_lean_output

    root = tmp_path / "lean"
    root.mkdir()
    for relative in LEAN_SUPPORT_PATHS:
        target = root / Path(relative).relative_to("lean")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}", encoding="utf-8")
    daily, orders = observations()
    for row in daily:
        row.update(account_id="CNY", valuation_at=f"{row['date']}T15:00:00+08:00",
                   equity_semantics="session_close_mark")
    for event_id, row in enumerate(orders, 1):
        row.update(event_id=event_id, instrument_id="000001.XSHE")
    (root / "strategy-input.json").write_text(json.dumps({"initial_cash_units": 10000,
        "dates": [r["date"] for r in daily]}), encoding="utf-8")
    trace = [dict(event_seq=index, kind="order_event", at="2024-01-08T09:32:00+08:00",
                  payload={"order_id": 1, "event_id": index}) for index in range(1, 4098)]
    for name, rows in (("daily", daily), ("orders", orders), ("intents", []), ("strategy-trace", trace),
                       ("fills", []), ("positions", []), ("account_events", []), ("lifecycle", [])):
        (root / (name+".jsonl")).write_text("".join(json.dumps(r)+"\n" for r in rows), encoding="utf-8")
    for name in ("bars", "rules", "actions", "signals"):
        target = root / "evidence" / name
        target.mkdir(parents=True)
        pq.write_table(pa.table({"record": [1]}), target / "part-00000.parquet")
    store = ExternalArtifactStore(tmp_path / "external")
    context = SimpleNamespace(external_store=store)
    opened = Counter()
    original_open = Path.open

    def track_open(path, *args, **kwargs):
        if path.parent == root and path.name in LEAN_JSONL_FILES.values():
            opened[path.name] += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", track_open)
    output = _publish_lean_output(context, root, {"stage_seconds": {"prepare": 1.25, "execute": 2.5}}, {},
                                  json.loads((root / "strategy-input.json").read_text()))
    assert opened == dict.fromkeys(LEAN_JSONL_FILES.values(), 1)
    commit = output.values["simulation"].external_commit
    verified = store.verify(commit.semantic_hash)
    assert verified == commit
    location = store.objects_root / commit.semantic_hash
    with pq.ParquetFile(location / LEAN_PATHS["trace"] / "part-00000.parquet") as trace_file:
        assert trace_file.num_row_groups == 2
        assert trace_file.read(columns=["event_seq"])["event_seq"].to_pylist() == list(range(1, 4098))
    metrics = pq.read_table(location / LEAN_METRICS_PATH / "part-00000.parquet").to_pylist()
    assert len(metrics) == 14
    assert metrics == lean_metric_rows(daily, orders, initial_equity=100)
    assert all("definition_json" in row and row["status"] == "computed" for row in metrics)
    summary = json.loads((location / "result.json").read_text(encoding="utf-8"))
    assert summary["preparation"]["stage_seconds"]["prepare"] == 1.25
    assert summary["preparation"]["stage_seconds"]["execute"] == 2.5
    assert summary["preparation"]["stage_seconds"]["output_write_before_commit"] >= 0

    # 后读的流水损坏时，整批执行表及指标都不得发布。
    (root / "positions.jsonl").write_text("{", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        _publish_lean_output(context, root, {}, {}, json.loads((root / "strategy-input.json").read_text()))
    assert not list(store.staging_root.iterdir())
    assert store.verify(commit.semantic_hash) == commit


@pytest.mark.parametrize("across_batches", [False, True])
def test_publication_decimal_width_preserves_nested_and_top_level_values(tmp_path, monkeypatch, across_batches):
    import duckdb
    import pyarrow.parquet as pq
    from research_pipeline.runtime.lean_strategy_execution import _publish_execution_rows

    small = "0.1234567890123456789012345678"
    large = "12345678901234567890123456789"
    exact = "123456789012.125"
    # 同列需要容纳大整数与很多小数位，其他列仍应使用 DuckDB 可精确读取的窄型。
    rows = [f'{{"equity":{small},"margin":{exact},"positions":[{{"price":{small}}}]}}'] * (
        4096 if across_batches else 1
    )
    rows.append(f'{{"equity":{large},"margin":10.5,"positions":[{{"price":{large}}}]}}')
    source = tmp_path / "daily.jsonl"
    source.write_text("\n".join(rows), encoding="utf-8")
    target = tmp_path / "daily" / "part-00000.parquet"
    original_open = Path.open
    source_opens = 0

    def track_open(path, *args, **kwargs):
        nonlocal source_opens
        if path == source:
            source_opens += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", track_open)
    counts = {}
    for _ in _publish_execution_rows("daily", source, target, counts):
        pass
    assert source_opens == 1
    assert counts == {"daily": len(rows)}
    table = pq.read_table(target)
    assert table.schema.field("equity").type == pa.decimal256(57, 28)
    assert table.schema.field("margin").type == pa.decimal128(38, 26)
    positions = table.schema.field("positions").type.value_type
    assert positions.field("price").type == pa.decimal256(57, 28)
    for index, value in ((0, small), (len(rows) - 1, large)):
        assert table["equity"][index].as_py() == Decimal(value)
        assert table["positions"][index].as_py()[0]["price"] == Decimal(value)
    assert table["margin"][0].as_py() == Decimal(exact)
    with duckdb.connect(":memory:") as connection:
        values = connection.execute("SELECT margin, typeof(margin) FROM read_parquet(?) LIMIT 1",
                                    [str(target)]).fetchone()
    assert values == (Decimal(exact), "DECIMAL(38,26)")
    assert list(target.parent.iterdir()) == [target]


def test_zero_terminal_equity_preserves_complete_sample_and_full_loss():
    rows = lean_metric_rows([{"date": "2024-01-08", "equity": 0, "cash": 0},
                            {"date": "2024-01-09", "equity": 0, "cash": 0}], [], initial_equity=100)
    values = {r["metric_ref"]: r["value"] for r in rows}
    assert values["project.lean.total_return@2.0.0"] == -1
    assert values["project.lean.annualized_return@2.0.0"] == -1
    assert values["project.lean.max_drawdown@2.0.0"] == 1
    assert all(r["sample_size"] == 2 for r in rows)




def test_prepare_rejects_zero_initial_cash_before_runtime_freeze(tmp_path, monkeypatch):
    import importlib.util
    from research_pipeline.packages.store import PACKAGE_FILES
    import research_pipeline.platform.lean_runtime as runtime
    path = Path(__file__).resolve().parents[1] / "integrations/lean/prepare.py"
    spec = importlib.util.spec_from_file_location("lean_prepare_test", path)
    prepare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prepare)
    research = {"graph": {"nodes": [{"node_id": "simulation", "parameters": {
        "strategy_declaration": {}, "initial_cash_units": 0}}]}}
    for name in PACKAGE_FILES:
        target = tmp_path / "source" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(research if name == "spec/research.yaml" else {}), encoding="utf-8")
    import research_pipeline.packages as packages
    monkeypatch.setattr(packages, "load_research_package", lambda *a, **k: None)
    monkeypatch.setattr(runtime, "freeze_lean_runtime", lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应冻结安装")))
    with pytest.raises(ValueError, match="初始资金"):
        prepare.convert(tmp_path / "source", tmp_path / "output", tmp_path)


