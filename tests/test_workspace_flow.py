from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_pipeline.cli import main
from research_pipeline.cli.commands import workspace_flow
from research_pipeline.workspace import WorkspaceError, initialize_workspace


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "examples" / "equity_cross_section" / "package"


@pytest.fixture(autouse=True)
def forbid_database(monkeypatch):
    import duckdb

    def forbidden(*args, **kwargs):
        raise AssertionError("Workspace 编排测试不得访问数据库")

    monkeypatch.setattr(duckdb, "connect", forbidden)


def test_workspace_copies_complete_package_and_preserves_source(tmp_path):
    before = {str(path.relative_to(PACKAGE)): path.read_bytes() for path in PACKAGE.rglob("*.yaml")}
    root = tmp_path / "copied"
    initialize_workspace(root, from_package=PACKAGE)
    for relative, content in before.items():
        assert (root / "package" / relative).read_bytes() == content
        assert (PACKAGE / relative).read_bytes() == content
    assert len(list((root / "package").rglob("*.yaml"))) == 4


def test_workspace_copy_rejects_existing_target_and_invalid_source(tmp_path):
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(WorkspaceError, match="必须不存在"):
        initialize_workspace(existing, from_package=PACKAGE, allow_existing=True)
    draft = initialize_workspace(tmp_path / "draft")
    target = tmp_path / "invalid-copy"
    with pytest.raises(ValueError):
        initialize_workspace(target, from_package=draft.package_root)
    assert not target.exists()


def _install_services(monkeypatch, *, fail=None, verdict="pass", finalized=True):
    calls = []
    arguments = {}

    def capture(stage, args):
        calls.append(stage)
        arguments[stage] = vars(args).copy()
        if stage == fail:
            raise ValueError("测试阶段失败：" + stage)

    def package(args):
        stage = args.package_command
        capture(stage, args)
        if stage == "lint":
            return {"checks": {"resources": {"nodes": []}}, "status": "linted"}
        return {"output": args.output, "execution_ready": True}

    def run(args):
        capture("run", args)
        return {
            "status": "result_finalized" if finalized else "succeeded",
            "result_id": "a" * 64,
            "runtime_run_id": "b" * 64,
            "result_directory": str(Path(args.result_store) / ("a" * 64)),
        }

    def evidence(args):
        capture(args.command, args)
        if args.command == "verify":
            return {"status": verdict, "output": args.output, "claim_level": "local_only"}
        return {"output": args.output}

    monkeypatch.setattr(workspace_flow.research_package, "_execute", package)
    monkeypatch.setattr(workspace_flow.research_run, "_execute", run)
    monkeypatch.setattr(workspace_flow.evidence_lifecycle, "_execute", evidence)
    return calls, arguments


def _command(tmp_path):
    workspace = tmp_path / "workspace"
    initialize_workspace(workspace, from_package=PACKAGE)
    return [
        "workspace", "execute", "--workspace", str(workspace),
        "--catalog-lock", str(tmp_path / "catalog"), "--data-db", str(tmp_path / "readonly.duckdb"),
        "--extension-bundle", "operator-bundle", "--verifier-bundle", "verifier-bundle",
        "--verification-process-slots", "3", "--json",
    ]


def test_workspace_execute_delegates_same_inputs_and_result(monkeypatch, tmp_path, capsys):
    calls, args = _install_services(monkeypatch)
    argv = _command(tmp_path)
    assert main(argv) == 0
    payload = json.loads(capsys.readouterr().out)
    data = payload["data"]
    assert calls == ["lint", "admit", "run", "verify", "report"]
    assert all(status == "pass" for status in data["stages"].values())
    assert data["verification_status"] == "pass"
    assert data["execution_status"] == "succeeded"
    assert args["run"]["clock"] == "2024-01-09T00:00:00+08:00"
    assert args["run"]["root_seed"] == 1701
    assert args["run"]["plan"] == args["admit"]["output"]
    assert args["lint"]["extension_bundle"] == args["admit"]["extension_bundle"] == ["operator-bundle"]
    assert args["verify"]["result"] == data["result_directory"]
    assert args["verify"]["verification_process_slots"] == 3
    assert args["report"]["verification_result"] == data["verification_result"]
    assert args["report"]["result_store"] == data["result_store"]
    assert Path(args["verify"]["verification_scratch_root"]).is_relative_to(Path(data["execution_root"]))
    assert not (tmp_path / "readonly.duckdb").exists()
    first_execution = data["execution_id"]
    assert main(argv) == 0
    assert json.loads(capsys.readouterr().out)["data"]["execution_id"] != first_execution


@pytest.mark.parametrize("stage", ["lint", "admit", "run", "verify", "report"])
def test_workspace_failure_stops_later_services(monkeypatch, tmp_path, capsys, stage):
    calls, args = _install_services(monkeypatch, fail=stage)
    assert main(_command(tmp_path)) == 1
    payload = json.loads(capsys.readouterr().out)
    data = payload["data"]
    sequence = ["lint", "admit", "run", "verify", "report"]
    assert calls == sequence[:sequence.index(stage) + 1]
    assert data["failed_stage"] == stage
    assert data["stages"][stage] == "fail"
    if stage in {"verify", "report"}:
        assert data["result_id"] == "a" * 64
        assert data["execution_status"] == "succeeded"
    if stage == "lint":
        assert not (tmp_path / "workspace" / ".research" / "executions").exists()


def test_workspace_failed_verdict_reports_without_claiming_success(monkeypatch, tmp_path, capsys):
    calls, _ = _install_services(monkeypatch, verdict="fail")
    assert main(_command(tmp_path)) == 1
    payload = json.loads(capsys.readouterr().out)
    assert calls[-1] == "report"
    assert payload["error_code"] == "workspace_verification_failed"
    data = payload["data"]
    assert data["execution_status"] == "succeeded"
    assert data["verification_status"] == "fail"
    assert data["failed_stage"] == "verify"
    assert data["stages"]["report"] == "pass"
    assert data["report_path"].endswith("report.md")


def test_workspace_requires_finalized_result(monkeypatch, tmp_path, capsys):
    calls, _ = _install_services(monkeypatch, finalized=False)
    assert main(_command(tmp_path)) == 1
    data = json.loads(capsys.readouterr().out)["data"]
    assert calls[-1] == "run"
    assert data["verification_status"] is None
    assert data["stages"]["verify"] == "not_run"


@pytest.mark.parametrize("override", [["--clock", "2025-01-01T00:00:00+08:00"], ["--root-seed", "2"]])
def test_workspace_rejects_clock_or_seed_changes_before_allocation(monkeypatch, tmp_path, capsys, override):
    calls, _ = _install_services(monkeypatch)
    assert main(_command(tmp_path) + override) == 1
    data = json.loads(capsys.readouterr().out)["data"]
    assert calls == ["lint"]
    assert data["failed_stage"] == "prepare"
    assert data["stages"]["allocate"] == "not_run"
    assert not (tmp_path / "workspace" / ".research" / "executions").exists()


def test_cli_summary_is_mutually_exclusive_with_full_json(tmp_path):
    assert main(["workspace", "inspect", "--workspace", str(tmp_path), "--json", "--summary"]) == 2


def test_resource_advice_only_counts_project_processes(monkeypatch):
    from types import SimpleNamespace
    from research_pipeline.platform import process_budget_advice as budget

    monkeypatch.setattr(budget.sys, "platform", "linux")
    lint = {"checks": {"resources": {"nodes": [
        {"node_id": "core", "implementation_scope": "core", "resource_profile": {"process_slots": 1}},
        {"node_id": "project", "implementation_scope": "project", "resource_profile": {"process_slots": 1}},
    ]}}}
    args = SimpleNamespace(verifier_bundle=None, verification_process_slots=2)
    advice = workspace_flow._resource_advice(lint, args)
    assert len(advice) == 1
    assert advice[0]["node_id"] == "project"
    assert advice[0]["status"] == "configuration_insufficient"
    assert advice[0]["declared_slots"] == 1


def test_workspace_cannot_be_created_inside_source(tmp_path):
    source = initialize_workspace(tmp_path / "source", from_package=PACKAGE).package_root
    before = {path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()}
    with pytest.raises(WorkspaceError, match="不能位于来源研究包内"):
        initialize_workspace(source / "nested", from_package=source)
    assert not (source / "nested").exists()
    assert before == {path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()}


@pytest.mark.parametrize("stage", ["verify", "report"])
def test_workspace_failure_suggests_result_bound_followup(monkeypatch, tmp_path, capsys, stage):
    from research_pipeline.cli.parser import build_parser

    calls, args = _install_services(monkeypatch, fail=stage)
    assert main(_command(tmp_path)) == 1
    data = json.loads(capsys.readouterr().out)["data"]
    followup = build_parser().parse_args(data["next_command_argv"][3:])
    assert followup.command == stage
    assert followup.result_store == data["result_store"]
    if stage == "verify":
        assert followup.result == data["result_directory"]
        assert followup.verifier_bundle == "verifier-bundle"
        assert followup.verification_process_slots == 3
        assert Path(followup.verification_scratch_root).is_dir()
    else:
        assert followup.verification_result == data["verification_result"]
        assert followup.output is None
