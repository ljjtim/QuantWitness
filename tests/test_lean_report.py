"""LEAN报告直接读取Result的正式指标、日终及订单表。"""
import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.evidence.lean_report import LEAN_REPORT_VERSION, read_lean_report_data
from research_pipeline.evidence.qlib_report import validate_report_request, render_qlib_report
from research_pipeline.results import ResultSnapshot
from research_pipeline.platform.lean_contract import LEAN_SCHEMA_IDS
from research_pipeline.platform.lean_execution_schema import LEAN_EXECUTION_SCHEMAS, encode_lean_execution_row
from research_pipeline.results.metrics import metrics_from_snapshot
from test_lean_metrics import metric_snapshot, observations


def report_case():
    snapshot = metric_snapshot()
    daily, orders = observations()
    tables = dict(snapshot.tables)
    manifests = list(snapshot.bundle.tables)
    for name in LEAN_SCHEMA_IDS:
        rows = daily if name == "daily" else orders if name == "orders" else []
        schema = LEAN_SCHEMA_IDS[name]
        tables[schema] = execution_table(name, rows)
        manifests.append(SimpleNamespace(table_id="lean_"+name, schema_id=schema, artifact_key="simulation-artifact"))
    snapshot.bundle.tables = manifests
    snapshot.bundle.support_files = ()
    snapshot = ResultSnapshot(snapshot.bundle, snapshot.directory, tables, {})
    return SimpleNamespace(snapshot=snapshot, metrics=metrics_from_snapshot(snapshot))


def execution_table(name, rows):
    return pa.Table.from_pylist(
        [encode_lean_execution_row(name, row) for row in rows], schema=LEAN_EXECUTION_SCHEMAS[name],
    )


def request():
    return {"contract_version": LEAN_REPORT_VERSION, "result_id": "result.lean",
            "tables": {**{name: "lean_" + name for name in LEAN_SCHEMA_IDS}, "metrics": "lean_metrics"},
            "budget": {"max_rows": 1000, "memory_bytes": 64*1024**2}}


def test_public_report_request_routes_to_lean_and_reads_formal_metrics():
    context = report_case()
    assert validate_report_request(request()) == request()
    rows, metrics = read_lean_report_data(context, request())
    assert len(rows["daily"]) == 2
    assert metrics["project.lean.fee_total@2.0.0"].value == pytest.approx(1.2)


def test_disk_account_rejects_changed_content(tmp_path):
    import hashlib
    from test_lean_futures_results import futures_report_context
    from research_pipeline.results.errors import ResultContractError

    context, req = futures_report_context()
    original = context.snapshot.support_bytes["lean/account.json"]
    table = next(item for item in context.snapshot.bundle.tables if item.table_id == req["tables"]["daily"])
    account = tmp_path / "account.json"
    account.write_bytes(original)
    context.snapshot.bundle.support_files = (SimpleNamespace(artifact_key=table.artifact_key,
        source_path="lean/account.json", relative_path=account.name,
        content_hash=hashlib.sha256(original).hexdigest()),)
    context.snapshot = ResultSnapshot(context.snapshot.bundle, tmp_path, context.snapshot.tables, {})
    read_lean_report_data(context, req)
    account.write_bytes(original + b" ")
    with pytest.raises(ResultContractError, match="hash"):
        read_lean_report_data(context, req)


@pytest.mark.parametrize("has_terminal_event", [False, True])
def test_spot_terminal_report_uses_formal_zero_marks_and_actual_valuation_time(has_terminal_event):
    from research_pipeline.evidence.lean_report import render_lean_report
    from research_pipeline.platform.lean_contract import LEAN_METRICS_SCHEMA_ID
    from research_pipeline.research.lean_metrics import lean_metrics_table

    context = report_case()
    daily, orders = observations()
    for row in daily:
        row["valuation_at"] = row["date"] + "T15:00:00+08:00"
    terminal = dict(equity=10, cash=10, terminal_observation_at="2024-01-10T15:00:00+08:00",
        valuation_at="2024-01-10T15:00:00+08:00",
        valuation_marks=[dict(instrument_id="000001.XSHE", source="delisted_zero", price=0)])
    tables = dict(context.snapshot.tables)
    tables[LEAN_METRICS_SCHEMA_ID] = lean_metrics_table(daily, orders, initial_equity=100, final_account=terminal)
    tables[LEAN_SCHEMA_IDS["daily"]] = execution_table("daily", daily)
    if has_terminal_event:
        event = {key: value for key, value in terminal.items() if key != "terminal_observation_at"}
        tables[LEAN_SCHEMA_IDS["account_events"]] = execution_table("account_events", [dict(event, reason="terminal")])
    context.snapshot.bundle.support_files = (SimpleNamespace(artifact_key="simulation-artifact",
        source_path="lean/account.json", relative_path="support/account.json"),)
    snapshot = ResultSnapshot(context.snapshot.bundle, context.snapshot.directory, tables,
        {"lean/account.json": json.dumps(terminal).encode("utf-8")})
    context = SimpleNamespace(snapshot=snapshot, metrics=metrics_from_snapshot(snapshot))
    rows, metrics = read_lean_report_data(context, request())
    assert rows["daily"][-1]["equity"] == 99
    assert rows["research_daily"][-1]["research_equity"] == 10
    assert rows["research_daily"][-1]["research_cash"] == 10
    assert metrics["project.lean.total_return@2.0.0"].value == pytest.approx(-0.9)
    output = render_lean_report(context, request(), result_summary="退市账户")
    assert "2024-01-10T15:00:00+08:00" in output
    assert "观察尾部不增加交易日" in output
    assert "研究现金" in output and "实际估值时间" in output
    assert "期货研究权益净值" not in output


@pytest.mark.parametrize("failure", ["wrong_result", "budget_rows", "budget_bytes", "wrong_schema", "other_artifact", "unproved_metrics", "intents_schema", "trace_schema"])
def test_report_rejects_unbound_tables_or_unproved_metrics(failure):
    context, req = report_case(), request()
    if failure == "wrong_result":
        req["result_id"] = "other"
    if failure == "budget_rows":
        req["budget"]["max_rows"] = 1
    if failure == "budget_bytes":
        req["budget"]["memory_bytes"] = 1
    if failure == "wrong_schema":
        context.snapshot.bundle.tables[-1].schema_id = "wrong"
    if failure in {"intents_schema", "trace_schema"}:
        name = failure.removesuffix("_schema")
        next(table for table in context.snapshot.bundle.tables if table.table_id == "lean_" + name).schema_id = "wrong"
    if failure == "other_artifact":
        context.snapshot.bundle.tables[-1].artifact_key = "other"
    if failure == "unproved_metrics":
        context.metrics = ()
    with pytest.raises(ValueError):
        read_lean_report_data(context, req)


def test_html_is_self_contained_and_displays_execution_summary():
    pytest.importorskip("plotly")
    context = report_case()
    result = render_qlib_report(context, request(), result_summary="执行成功；研究观察")
    assert "LEAN研究报告" in result and "累计收益率" in result
    assert "执行成功；研究观察" in result
    assert "plotly.js" in result
    assert '<script src="https://' not in result
    title = "逐日成交与费用"
    assert title in result or json.dumps(title, ensure_ascii=True)[1:-1] in result


@pytest.mark.parametrize("market", ["stock", "futures"])
def test_html_projects_parquet_columns_and_preserves_display(monkeypatch, tmp_path, market):
    pytest.importorskip("plotly")
    import plotly.io
    from research_pipeline.evidence.lean_report import render_lean_report

    if market == "futures":
        from test_lean_futures_results import futures_report_context
        context, req = futures_report_context()
    else:
        context, req = report_case(), request()
    monkeypatch.setattr(plotly.io, "to_html", lambda figure, **kwargs: figure.to_json())
    expected = render_lean_report(context, req, result_summary="正式研究结果")
    tables = dict(context.snapshot.tables)
    skipped = {"intents", "trace"}
    if market == "stock":
        skipped.add("fills")
        tables[LEAN_SCHEMA_IDS["account_events"]] = execution_table("account_events", [{"reason": "fill"}] * 1500)
    for name in skipped:
        tables[LEAN_SCHEMA_IDS[name]] = execution_table(name, [{}] * 1500)
    for manifest in context.snapshot.bundle.tables:
        path = f"{manifest.table_id}.parquet"
        pq.write_table(tables[manifest.schema_id], tmp_path / path)
        manifest.files = (path,)
    context.snapshot = ResultSnapshot(
        context.snapshot.bundle, tmp_path, {}, context.snapshot.support_bytes,
    )
    reads = []
    original = ResultSnapshot.iter_table_batches
    def read(snapshot, schema_id, **kwargs):
        assert schema_id not in {LEAN_SCHEMA_IDS[name] for name in skipped}
        assert kwargs["columns"]
        reads.append((schema_id, kwargs["columns"], kwargs["filters"]))
        yield from original(snapshot, schema_id, **kwargs)
    monkeypatch.setattr(ResultSnapshot, "iter_table_batches", read)
    actual = render_lean_report(context, req, result_summary="正式研究结果")
    rows, _ = read_lean_report_data(context, req)
    assert "intents" not in rows and "trace" not in rows
    assert reads and actual == expected
    if market == "stock":
        assert rows["account_events"] == []
        assert all(filters for schema, _, filters in reads if schema == LEAN_SCHEMA_IDS["account_events"])
    else:
        assert all(columns == ("order_id",) for schema, columns, _ in reads if schema == LEAN_SCHEMA_IDS["orders"])


def test_standard_markdown_and_json_report_include_formal_lean_metrics(tmp_path):
    from research_pipeline.application.report_output import write_result_report
    from research_pipeline.results import ResultContext, ResultRunSummary, build_result_report, render_result_report

    case = report_case()
    case.snapshot.bundle.run = ResultRunSummary(
        "research.test", "a" * 64, None, "b" * 64, "succeeded", "deterministic_serial",
        "2024-01-10T00:00:00+08:00", {"simulation": "succeeded"},
    )
    context = ResultContext(case.snapshot, case.metrics, "research_observation", (), None)
    markdown = render_result_report(context)
    model = build_result_report(context)
    assert "project.lean.total_return@2.0.0" in markdown
    assert "project.lean.fee_total@2.0.0" in markdown
    output = tmp_path / "report.json"
    write_result_report(output, output_format="json", markdown=markdown, report=model.to_dict())
    document = json.loads(output.read_text(encoding="utf-8"))
    assert len(document["report"]["metrics"]) == 14
    assert "verification_hash" not in document["report"]
    assert document["report"]["claim_level"] == "research_observation"
