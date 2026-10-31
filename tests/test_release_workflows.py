"""发布工作流的失败收据、安装来源、恢复点与 Gate C 接口测试。"""

from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import os
import site
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import run_release_workflows as workflows  # noqa: E402
import release_evidence_binding as binding  # noqa: E402


@pytest.fixture
def commands(tmp_path):
    return workflows.Commands(Path(sys.executable), tmp_path)


@pytest.mark.parametrize("code, stdout", [(7, '{"status":"pass"}'), (0, '{"status":"fail"}'), (0, "invalid json")])
def test_command_failure_preserves_output_and_never_passes(commands, code, stdout):
    program = f"import sys; print({stdout!r}); print('diagnostic', file=sys.stderr); sys.exit({code})"
    with pytest.raises(ValueError):
        commands.run("negative", ["-c", program])
    record = workflows.read_json(Path(commands.records[0]["path"]))
    assert record["exit_code"] == code
    assert stdout in record["stdout"]
    assert "diagnostic" in record["stderr"]
    assert Path(record["cwd"]) == commands.cwd


def test_timeout_is_a_failed_command_with_partial_output(commands, monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 0.1, output=b"partial", stderr=b"pending")

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(ValueError, match="退出码"):
        commands.run("timeout", ["-c", "pass"])
    record = workflows.read_json(Path(commands.records[0]["path"]))
    assert record["timed_out"] is True
    assert record["exit_code"] is None
    assert record["stdout"] == "partial" and record["stderr"] == "pending"


def test_subprocess_isolated_from_parent_pythonpath(commands, tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "unwanted_local_module.py").write_text("value = 1", encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(source))
    commands.environment["PYTHONPATH"] = str(source)
    result = commands.run("isolated", ["-c", (
        "import importlib.util,json; "
        "print(json.dumps({'status':'pass', 'visible':importlib.util.find_spec('unwanted_local_module') is not None}))"
    )])
    assert result["visible"] is False


@pytest.mark.parametrize("failure", ["global_python", "source_import", "editable"])
def test_invalid_installation_is_rejected(tmp_path, failure):
    prefix = tmp_path / "venv"
    source = tmp_path / "source"
    payload = {"prefix": str(prefix), "base_prefix": str(tmp_path / "base"), "editable": False,
               "imports": {"research_pipeline": str(prefix / "Lib/site-packages/research_pipeline/__init__.py")}}
    workflows.validate_installation(payload, source)
    if failure == "global_python":
        payload["base_prefix"] = payload["prefix"]
    elif failure == "source_import":
        payload["imports"]["research_pipeline"] = str(source / "src/research_pipeline/__init__.py")
    else:
        payload["editable"] = True
    with pytest.raises(ValueError):
        workflows.validate_installation(payload, source)


def test_discovery_uses_current_public_commands_and_stable_draft_diagnostics(tmp_path):
    from research_pipeline.cli import main

    class PublicCommands:
        def cli(self, label, arguments, *, expected=0):
            stream = io.StringIO()
            with redirect_stdout(stream):
                code = main(list(map(str, arguments)))
            assert code == expected, stream.getvalue()
            return json.loads(stream.getvalue())

    discovered = workflows.discover(PublicCommands(), tmp_path)
    assert discovered["diagnostics"]["error_code"] == "research_package_invalid"
    assert discovered["operator"]["data"]["kind"] == "operator.describe"
    assert discovered["capabilities"]["capabilities"]
    draft = Path(discovered["draft"])
    assert (draft / "spec/research.yaml").is_file()


@pytest.mark.parametrize("failure", ["accepted", "other_error", "no_issues", "no_action"])
def test_draft_failure_must_be_specific_and_actionable(failure):
    # 参数解析错误和通用失败都不能替代中性草稿诊断。
    payload = {"status": "fail", "error_code": "research_package_invalid", "data": {
        "execution_ready": False, "issues": [
            {"file": file, "field": field, "code": "required", "message": "未填写", "action": "补齐"}
            for file, field in (("package.yaml", "package_slug"), ("sources/sources.yaml", "sources"),
                                ("spec/research.yaml", "requests"))]}}
    if failure == "accepted":
        payload["status"] = "pass"
    elif failure == "other_error":
        payload["error_code"] = "internal_error"
    elif failure == "no_issues":
        payload["data"]["issues"] = []
    else:
        payload["data"]["issues"][0].pop("action")
    with pytest.raises(ValueError):
        workflows.require_draft_rejection(payload)


def _workflow_result(tmp_path):
    directory = tmp_path / "results" / "result-1"
    workflows.write_json(directory / "result.json", {"result_id": "result-1"})
    report = tmp_path / "report.md"
    report.write_text("研究结果", encoding="utf-8")
    return {"data": {"execution_status": "succeeded", "result_id": "result-1",
                     "result_directory": str(directory), "report_path": str(report),
                     "stages": {name: "pass" for name in ("prepare", "allocate", "admit", "run", "report")}}}


def test_workflow_accepts_result_and_report(tmp_path):
    payload = _workflow_result(tmp_path)
    assert workflows.require_workflow(payload) is payload["data"]


@pytest.mark.parametrize("failure", ["execution", "stage", "report", "identity", "duplicate"])
def test_workflow_rejects_incomplete_result_delivery(tmp_path, failure):
    payload = _workflow_result(tmp_path)
    data = payload["data"]
    if failure == "execution":
        data["execution_status"] = "failed"
    elif failure == "stage":
        data["stages"]["report"] = "not_run"
    elif failure == "report":
        Path(data["report_path"]).unlink()
    elif failure == "identity":
        data["result_id"] = "another-result"
    else:
        (Path(data["result_directory"]).parent / "extra").mkdir()
    with pytest.raises(ValueError):
        workflows.require_workflow(payload)


def test_workspace_arguments_use_operator_bundle_only(tmp_path):
    argv = workflows.workspace_flow({"operator_bundle": "operator"},
                                    {"catalog_lock": "catalog", "database": "database"}, tmp_path)
    assert argv == ["workspace", "execute", "--workspace", tmp_path,
                    "--catalog-lock", "catalog", "--data-db", "database",
                    "--extension-bundle", "operator", "--json"]


@pytest.mark.parametrize("change", ["size", "mtime", "wal"])
def test_database_changes_cannot_be_reported_as_unchanged(tmp_path, change):
    database = tmp_path / "synthetic.duckdb"
    database.write_bytes(b"fixture")
    before = workflows.database_state(database)
    assert workflows.database_evidence(database, before)["unchanged"]
    if change == "size":
        database.write_bytes(b"different-size")
    elif change == "mtime":
        os.utime(database, ns=(before["mtime_ns"], before["mtime_ns"] + 1_000_000_000))
    else:
        Path(str(database) + ".wal").write_bytes(b"wal")
    result = workflows.database_evidence(database, before)
    assert result["status"] == "fail" and result["unchanged"] is False


def _result_tables(root, *, changed=False, omitted=False, metadata=None):
    root.mkdir()
    tables = []
    for name in ("metric", "observations"):
        if omitted and name == "observations":
            continue
        values = [1.0, 2.0 if not changed or name == "metric" else 3.0]
        table = pa.table({"value": values}).replace_schema_metadata(metadata)
        pq.write_table(table, root / f"{name}.parquet")
        tables.append({"table_id": name, "files": [f"{name}.parquet"]})
    workflows.write_json(root / "result.json", {"tables": tables})
    return root


@pytest.mark.parametrize("change", ["diagnostic_values", "missing_table", "metadata"])
def test_recovery_comparison_includes_every_table_and_schema(tmp_path, change):
    left = _result_tables(tmp_path / "baseline")
    same = _result_tables(tmp_path / "same")
    assert len(workflows.compare_tables(left, same)["tables"]) == 2
    right = _result_tables(tmp_path / "recovered", changed=change == "diagnostic_values",
                           omitted=change == "missing_table", metadata={b"unit": b"CNY"} if change == "metadata" else None)
    with pytest.raises(ValueError, match="结果表"):
        workflows.compare_tables(left, right)


@pytest.mark.parametrize("node_id", workflows.CHECKPOINTS.values())
def test_forced_exit_occurs_after_a_real_committed_checkpoint(commands, tmp_path, node_id):
    from research_pipeline.runtime import CheckpointStore

    run_root, receipt, finished = tmp_path / "run", tmp_path / "interrupt.json", tmp_path / "finished"
    program = f'''
import hashlib
import sys
from pathlib import Path
sys.path[:0] = [{str(ROOT / "src")!r}, {str(ROOT / "tools")!r}, {site.getusersitepackages()!r}]
import research_pipeline.cli as cli
from research_pipeline.runtime import ArtifactRef, CheckpointExpectation, CheckpointStore
import run_release_workflows as workflows

def execute(arguments):
    store = CheckpointStore({str(run_root)!r})
    store.commit_bytes(
        expectation=CheckpointExpectation("a" * 64, (), "fixture.v1", "b" * 64, "c" * 64, "d" * 64),
        attempt_id={node_id!r} + "-attempt-1", content=b"verified checkpoint",
        outputs={{"result": ArtifactRef("result", "fixture.v1", "a" * 64,
            hashlib.sha256(b"verified checkpoint").hexdigest())}}, audit_environment_digest="e" * 64,
        execution_identity_digest="f" * 64, root_seed=1, fixed_clock="2024-01-09T00:00:00+08:00")
    Path({str(finished)!r}).write_text("finished")
    return 0

cli.main = execute
workflows.worker("interrupt", [{node_id!r}, {str(receipt)!r}])
'''
    commands.run("interrupt", ["-c", program], expected=95, parse=False)
    assert not finished.exists()
    manifest = CheckpointStore(run_root, create=False).verify_stored("a" * 64)
    assert manifest.content_size == len(b"verified checkpoint")
    assert workflows.read_json(receipt)["node_id"] == node_id


def test_dirty_candidate_fails_before_any_cli_or_database_creation(tmp_path, monkeypatch):
    def rejected(**kwargs):
        raise ValueError("候选不是 clean commit")

    monkeypatch.setattr(binding, "release_evidence_binding", rejected)
    output = tmp_path / "release"
    receipt = workflows.run_workflows(python=Path(sys.executable), project=ROOT, output=output,
                                     release_candidate_id="candidate", build_manifest=tmp_path / "manifest.json")
    assert receipt["status"] == "fail"
    assert receipt["commands"] == []
    assert not (output / "environment").exists()
    assert not (output / "gate-c-input.json").exists()
    for gate in ("gate-a", "gate-i-b"):
        assert workflows.read_json(output / f"{gate}.json")["status"] == "fail"


def test_existing_output_is_never_reused(tmp_path):
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError, match="新目录"):
        workflows.run_workflows(python=Path(sys.executable), project=ROOT, output=tmp_path,
                                release_candidate_id="candidate", build_manifest=tmp_path / "manifest.json")
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_gate_c_projection_preserves_database_evidence_and_rejects_wrong_result(tmp_path):

    database = tmp_path / "synthetic.duckdb"
    database.write_bytes(b"synthetic fixture")
    measured = workflows.database_evidence(database, workflows.database_state(database))
    projects = []
    for index, name in enumerate(workflows.PROJECTS):
        root = tmp_path / name
        workflows.write_json(root / "result.json", {"result_id": f"result-{index}"})
        projects.append({"name": name, "package": str(root / "package"), "plan": str(root / "plan.json"),
                         "result_directory": str(root), "result_store": str(tmp_path), "result_id": f"result-{index}",
                         "graph_id": f"quantwitness.{name}",
                         "data_range": [{"request_id": f"request-{index}"}], "database_evidence": measured})
    payload = workflows.gate_c_input(projects)
    assert len(payload["references"]) == 4
    for reference in payload["references"]:
        assert "verification_result" not in reference
        assert "oracle_script" not in reference
        assert reference["expected_metrics"] == [workflows.EXPECTED_METRICS[reference["reference"]]]
        assert reference["expected_metrics"][0] is not workflows.EXPECTED_METRICS[reference["reference"]]
        assert Path(reference["result_directory"]).is_dir()
        evidence = reference["database_evidence"]
        assert evidence["data_scope"] == "synthetic"
        assert evidence["result_id"] == reference["expected_result_id"]
        assert evidence["read_only"] and evidence["database_unchanged"]
        assert evidence["before"] == evidence["after"]
    projects[0]["result_id"] = "another-result"
    with pytest.raises(ValueError, match="Result 身份"):
        workflows.gate_c_input(projects)


@pytest.mark.parametrize("name,value,unit,sample_size", [
    ("equity_cross_section", (12.3 / 12.0 - 1) - (8.8 / 9.0 - 1), "decimal_return", 4),
    ("etf_time_series", 0.025, "decimal_return", 2),
    ("event_study", 0.02, "decimal_return", 2),
    ("futures_term_structure", 0.03 * 365 / 29, "annualized_decimal_slope", 2),
])
def test_gate_c_expected_metrics_use_frozen_synthetic_values(name, value, unit, sample_size):
    expected = workflows.EXPECTED_METRICS[name]
    assert expected["value"] == pytest.approx(value, rel=0, abs=1e-12)
    assert expected["unit"] == unit
    assert expected["sample_size"] == sample_size


@pytest.mark.parametrize("checkpoint", workflows.CHECKPOINTS)
def test_resume_must_reuse_committed_nodes_instead_of_recomputing(checkpoint):
    nodes = list(workflows.CHECKPOINTS.values())
    required = nodes[:list(workflows.CHECKPOINTS).index(checkpoint) + 1]
    runtime = {"status": "succeeded", "reused_nodes": required}
    assert workflows.require_reused_checkpoints(runtime, checkpoint) == required
    runtime["reused_nodes"] = required[:-1]
    with pytest.raises(ValueError, match="复用必需"):
        workflows.require_reused_checkpoints(runtime, checkpoint)


def test_tampered_result_is_rejected_by_report(tmp_path):
    store = tmp_path / "results"
    result = _result_tables(store)
    baseline = {"result_store": str(store), "result_directory": str(result)}

    class ReportCommands:
        def cli(self, label, arguments, *, expected=0):
            assert label == "tamper-report"
            assert arguments[0] == "report" and "--result" in arguments
            assert expected == 1
            target = Path(arguments[arguments.index("--result") + 1]) / "metric.parquet"
            assert target.read_bytes() != (store / "metric.parquet").read_bytes()
            return {"status": "fail", "error_code": "result_contract_invalid"}

    receipt = workflows.tamper(ReportCommands(), baseline, tmp_path)
    assert receipt["error_code"] == "result_contract_invalid"
    assert receipt["modified_table"] == "metric"


def test_recovery_reports_result_and_compares_all_tables(tmp_path):
    workspace = tmp_path / "workspace"
    baseline = {"workspace": str(workspace), "result_directory": "baseline"}
    result = tmp_path / "result"
    workflows.write_json(result / "result.json", {"result_id": "recovered"})
    calls = []

    class RecoveryCommands:
        def worker(self, label, action, *arguments, **kwargs):
            if action == "interrupt":
                node, receipt, *_ = arguments
                run_root = tmp_path / label / "run"
                marker = run_root / "COMMITTED"
                marker.parent.mkdir(parents=True)
                marker.write_text("", encoding="utf-8")
                workflows.write_json(Path(receipt), {"node_id": node, "phase": "committed_before_event",
                    "run_root": str(run_root), "committed_marker": str(marker)})
                workflows.write_json(run_root / "operator-dag-run.json", {
                    "status": "succeeded", "reused_nodes": list(workflows.CHECKPOINTS.values())})
                return {}
            assert action == "compare" and arguments == ("baseline", str(result))
            return {"tables": {"metric": {"status": "pass"}}}

        def cli(self, label, arguments, **kwargs):
            calls.append(arguments)
            if arguments[0] == "inspect":
                return {"summary": {"recommended_action": "resume"}}
            if arguments[0] == "workspace":
                return {"summary": {"execution_status": "succeeded"},
                        "data": {"result_directory": str(result), "result_id": "recovered"}}
            assert arguments[0] == "report" and arguments[1:3] == ["--result", str(result)]
            Path(arguments[arguments.index("--output") + 1]).write_text("报告", encoding="utf-8")
            return {"status": "pass"}

    receipt = workflows.recover(RecoveryCommands(), {"operator_bundle": "operator"},
        {"catalog_lock": "catalog", "database": "database"}, baseline, tmp_path)
    assert set(receipt) == set(workflows.CHECKPOINTS)
    assert all(item["result_id"] == "recovered" and Path(item["report"]).is_file()
               for item in receipt.values())
    assert all(call[0] in {"inspect", "workspace", "report"} for call in calls)
