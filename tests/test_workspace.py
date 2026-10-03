from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from research_pipeline.cli import main
from research_pipeline.workspace import (
    WorkspaceError,
    allocate_execution,
    export_dashboard_manifest,
    initialize_workspace,
    inspect_workspace,
    load_workspace,
    rebuild_workspace_index,
    resume_workspace_execution,
    run_workspace_execution,
    validate_workspace,
)


def test_workspace_init_validate_and_allocate_never_opens_database(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialized = initialize_workspace(root, workspace_id="demo-v1")
    assert initialized.root == root.absolute()
    result = validate_workspace(root)
    assert result["status"] == "pass"
    assert (root / "package" / "sources" / "sources.yaml").read_text(encoding="utf-8").endswith("sources: []\n")
    assert result["database_binding"] == {"configured": False, "path": None, "opened": False}
    first = allocate_execution(root, clock="2026-08-06T09:00:00+08:00", root_seed=7)
    second = allocate_execution(root, clock="2026-08-06T09:00:00+08:00", root_seed=7)
    assert first["execution_id"] != second["execution_id"]
    assert (root / ".research" / "executions" / first["execution_id"] / "execution.json").is_file()
    assert (
        root / ".research" / "executions" / first["execution_id"] / "verification"
    ).is_dir()
    assert not (root / ".research" / "stores" / "sealed").exists()
    assert not (root / "data.duckdb").exists()


def test_workspace_allocate_accepts_any_valid_iso_utc_offset(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)

    allocation = allocate_execution(
        root,
        clock="2026-08-24T09:00:00-04:00",
        root_seed=7,
    )

    assert allocation["clock"] == "2026-08-24T09:00:00-04:00"


@pytest.mark.parametrize("clock", ("2026-08-24T09:00:00", "not-a-clock"))
def test_workspace_allocate_rejects_clock_without_valid_utc_offset(tmp_path, clock) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)

    with pytest.raises(WorkspaceError, match="clock"):
        allocate_execution(root, clock=clock, root_seed=7)


def test_workspace_init_rejects_non_empty_directory(tmp_path) -> None:
    root = tmp_path / "existing"
    root.mkdir()
    (root / "keep.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(WorkspaceError, match="非空"):
        initialize_workspace(root)
    assert (root / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_workspace_validate_rejects_forbidden_package_file(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    (root / "package" / "bad.py").write_text("print(1)", encoding="utf-8")
    with pytest.raises(WorkspaceError, match="ResearchPackage 合同无效"):
        validate_workspace(root)


def test_workspace_cli_init_validate_and_inspect(tmp_path, capsys) -> None:
    root = tmp_path / "cli-workspace"
    assert main(["workspace", "init", str(root), "--workspace-id", "cli-v1", "--json"]) == 0
    initialized = json.loads(capsys.readouterr().out)
    assert initialized["status"] == "pass"
    assert main(["workspace", "validate", "--workspace", str(root), "--json"]) == 0
    validated = json.loads(capsys.readouterr().out)
    assert validated["data"]["database_binding"]["opened"] is False
    assert main(["workspace", "allocate", "--workspace", str(root), "--clock", "2026-08-06T09:00:00+08:00", "--root-seed", "9", "--json"]) == 0
    allocation = json.loads(capsys.readouterr().out)
    execution_id = allocation["data"]["execution_id"]
    assert main(["workspace", "inspect", "--workspace", str(root), "--execution", execution_id, "--json"]) == 0
    inspected = json.loads(capsys.readouterr().out)
    assert inspected["data"]["execution_id"] == execution_id


def test_workspace_run_maps_paths_without_opening_database(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=17, label="样例"
    )
    captured = {}

    def fake_handler(args):
        captured.update(vars(args))
        return {
            "status": "result_finalized",
            "package_plan_hash": "a" * 64,
            "run_id": "b" * 64,
            "result_id": "c" * 64,
            "evidence_semantic_hash": "d" * 64,
        }

    result = run_workspace_execution(
        root,
        execution_id=allocation["execution_id"],
        plan=tmp_path / "plan",
        data_db=tmp_path / "does-not-open.duckdb",
        clock="2026-08-06T09:00:00+08:00",
        root_seed=17,
        handler=fake_handler,
    )
    execution_root = root / ".research" / "executions" / allocation["execution_id"]
    assert captured["run_root"] == str(execution_root / "run")
    assert captured["result_store"] == str(execution_root / "results")
    assert captured["handoff_out"] == str(execution_root / "handoff.json")
    assert "draft_out" not in captured
    assert not (execution_root / "handoff.json").exists()
    assert result["result_id"] == "c" * 64
    indexed = json.loads((root / ".research" / "index.json").read_text(encoding="utf-8"))
    assert indexed["executions"][0]["run_id"] == "b" * 64
    rebuilt = rebuild_workspace_index(root)
    assert rebuilt["executions"][0]["run_id"] == "b" * 64
    assert rebuild_workspace_index(root)["index_hash"] == rebuilt["index_hash"]


def test_workspace_run_rejects_execution_clock_or_seed_drift_before_handler(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=17
    )
    called = False

    def fake_handler(_args):
        nonlocal called
        called = True
        return {}

    with pytest.raises(WorkspaceError, match="clock/root_seed"):
        run_workspace_execution(
            root,
            execution_id=allocation["execution_id"],
            plan=tmp_path / "plan",
            data_db=tmp_path / "data.duckdb",
            clock="2026-08-06T09:00:01+08:00",
            root_seed=17,
            handler=fake_handler,
        )
    assert called is False


def test_workspace_resume_delegates_existing_execution_run_root(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=17
    )
    execution_root = root / ".research" / "executions" / allocation["execution_id"]
    (execution_root / "run" / "operator-dag-invocation.json").write_text(
        "{}", encoding="utf-8"
    )
    captured = {}

    def fake_handler(**kwargs):
        captured.update(kwargs)
        return {"status": "succeeded", "run_id": "a" * 64}

    result = resume_workspace_execution(
        root,
        execution_id=allocation["execution_id"],
        retry_node_id="analysis",
        handler=fake_handler,
    )
    assert captured == {
        "run_root": execution_root / "run",
        "retry_node_id": "analysis",
    }
    assert result["run_id"] == "a" * 64


def test_workspace_resume_rejects_empty_preallocated_run_before_handler(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=17
    )
    called = False

    def fake_handler(**_kwargs):
        nonlocal called
        called = True
        return {}

    with pytest.raises(WorkspaceError, match="Runtime invocation"):
        resume_workspace_execution(
            root,
            execution_id=allocation["execution_id"],
            handler=fake_handler,
        )
    assert called is False


def test_workspace_completion_rejects_canonical_identity_drift(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=17
    )

    def first_handler(_args):
        return {"status": "result_finalized", "run_id": "a" * 64}

    run_workspace_execution(
        root,
        execution_id=allocation["execution_id"],
        plan=tmp_path / "plan",
        data_db=tmp_path / "unused.duckdb",
        clock="2026-08-06T09:00:00+08:00",
        root_seed=17,
        handler=first_handler,
    )
    with pytest.raises(WorkspaceError, match="不可变身份发生漂移"):
        run_workspace_execution(
            root,
            execution_id=allocation["execution_id"],
            plan=tmp_path / "plan",
            data_db=tmp_path / "unused.duckdb",
            clock="2026-08-06T09:00:00+08:00",
            root_seed=17,
            handler=lambda _args: {"status": "result_finalized", "run_id": "b" * 64},
        )


def test_workspace_dashboard_fails_closed_without_verification_result(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    with pytest.raises(WorkspaceError, match="VerificationResult"):
        export_dashboard_manifest(root)
    assert not (root / ".research" / "workspace.json").exists()


@pytest.mark.parametrize("run_identity_key", ["run_id", "runtime_run_id"])
def test_workspace_dashboard_exports_only_identity_bound_v3_entry(
    tmp_path, monkeypatch, run_identity_key
) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    formal = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=1, label="正式结果"
    )
    unverified = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=2, label="草稿"
    )
    mismatched = allocate_execution(
        root, clock="2026-08-06T09:00:00+08:00", root_seed=3, label="错绑结果"
    )
    run_id = "a" * 64
    result_id = "b" * 64
    run_workspace_execution(
        root,
        execution_id=formal["execution_id"],
        plan=tmp_path / "plan",
        data_db=tmp_path / "unused.duckdb",
        clock="2026-08-06T09:00:00+08:00",
        root_seed=1,
        handler=lambda _args: {
            "status": "result_finalized",
            run_identity_key: run_id,
            "result_id": result_id,
        },
    )
    execution_root = root / ".research" / "executions" / formal["execution_id"]
    verification = execution_root / "verification" / "result.json"
    verification.write_text("{}", encoding="utf-8")
    run_workspace_execution(
        root,
        execution_id=mismatched["execution_id"],
        plan=tmp_path / "plan",
        data_db=tmp_path / "unused.duckdb",
        clock="2026-08-06T09:00:00+08:00",
        root_seed=3,
        handler=lambda _args: {
            "status": "result_finalized",
            "run_id": run_id,
            "runtime_run_id": "c" * 64,
            "result_id": result_id,
        },
    )
    mismatched_root = (
        root / ".research" / "executions" / mismatched["execution_id"]
    )
    (mismatched_root / "verification" / "result.json").write_text(
        "{}", encoding="utf-8"
    )
    import research_pipeline.evidence as evidence

    context = SimpleNamespace(
        verification=SimpleNamespace(
            result_reference=SimpleNamespace(run_id=run_id, result_id=result_id),
            status="pass",
        )
    )
    monkeypatch.setattr(
        evidence,
        "load_verified_result_context",
        lambda *args, **kwargs: context,
    )
    result = export_dashboard_manifest(root)
    payload = json.loads((root / ".research" / "workspace.json").read_text(encoding="utf-8"))
    assert result["entry_count"] == 1
    assert payload["contract_version"] == "research-dashboard-workspace-v3"
    assert payload["entries"][0]["entry_id"] == formal["execution_id"]
    assert payload["entries"][0]["verification_result"].endswith(
        "/verification/result.json"
    )
    assert set(payload["entries"][0]) == {
        "entry_id",
        "label",
        "verification_result",
        "result_store",
    }
    assert unverified["execution_id"] not in json.dumps(payload)
    assert mismatched["execution_id"] not in json.dumps(payload)
    assert ".." not in json.dumps(payload)


def test_workspace_dashboard_exports_pass_and_skips_fail(tmp_path, monkeypatch) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    identities = {}
    for seed, status in ((1, "pass"), (2, "fail")):
        allocation = allocate_execution(
            root,
            clock="2026-08-06T09:00:00+08:00",
            root_seed=seed,
            label=status,
        )
        run_id = str(seed) * 64
        result_id = str(seed + 2) * 64
        identities[allocation["execution_id"]] = (run_id, result_id, status)
        run_workspace_execution(
            root,
            execution_id=allocation["execution_id"],
            plan=tmp_path / "plan",
            data_db=tmp_path / "unused.duckdb",
            clock="2026-08-06T09:00:00+08:00",
            root_seed=seed,
            handler=lambda _args, run_id=run_id, result_id=result_id: {
                "status": "result_finalized",
                "run_id": run_id,
                "result_id": result_id,
            },
        )
        execution_root = root / ".research" / "executions" / allocation["execution_id"]
        (execution_root / "verification" / "result.json").write_text(
            "{}", encoding="utf-8"
        )

    import research_pipeline.evidence as evidence

    def fake_load(verification_result, **_kwargs):
        entry_id = verification_result.parents[1].name
        run_id, result_id, status = identities[entry_id]
        return SimpleNamespace(verification=SimpleNamespace(
            result_reference=SimpleNamespace(run_id=run_id, result_id=result_id),
            status=status,
        ))

    monkeypatch.setattr(evidence, "load_verified_result_context", fake_load)
    result = export_dashboard_manifest(root)
    payload = json.loads((root / ".research" / "workspace.json").read_text(encoding="utf-8"))

    assert result["entry_count"] == 1
    assert [item["label"] for item in payload["entries"]] == ["pass"]


def test_workspace_dashboard_all_fail_removes_stale_manifest(
    tmp_path, monkeypatch
) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(
        root,
        clock="2026-08-06T09:00:00+08:00",
        root_seed=1,
        label="fail",
    )
    run_id = "a" * 64
    result_id = "b" * 64
    run_workspace_execution(
        root,
        execution_id=allocation["execution_id"],
        plan=tmp_path / "plan",
        data_db=tmp_path / "unused.duckdb",
        clock="2026-08-06T09:00:00+08:00",
        root_seed=1,
        handler=lambda _args: {
            "status": "result_finalized",
            "run_id": run_id,
            "result_id": result_id,
        },
    )
    verification = (
        root
        / ".research"
        / "executions"
        / allocation["execution_id"]
        / "verification"
        / "result.json"
    )
    verification.write_text("{}", encoding="utf-8")

    import research_pipeline.evidence as evidence

    verification_status = {"value": "pass"}
    monkeypatch.setattr(
        evidence,
        "load_verified_result_context",
        lambda *_args, **_kwargs: SimpleNamespace(verification=SimpleNamespace(
            result_reference=SimpleNamespace(run_id=run_id, result_id=result_id),
            status=verification_status["value"],
        )),
    )

    export_dashboard_manifest(root)
    manifest = root / ".research" / "workspace.json"
    assert manifest.is_file()

    verification_status["value"] = "fail"
    with pytest.raises(WorkspaceError, match="status=pass"):
        export_dashboard_manifest(root)
    assert not manifest.exists()


def test_workspace_dashboard_cli_help_has_no_historical_trust_store(capsys) -> None:
    assert main(["workspace", "dashboard", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "VerificationResult" in help_text
    assert "--trust-store" not in help_text


def test_workspace_relative_paths_survive_move_and_reject_nested_roles(tmp_path) -> None:
    original = tmp_path / "original"
    initialize_workspace(original)
    moved = tmp_path / "moved"
    original.rename(moved)
    assert load_workspace(moved).package_root == moved / "package"
    declaration = moved / "research-workspace.yaml"
    text = declaration.read_text(encoding="utf-8").replace(
        "operators_path: operators", "operators_path: package/operators"
    )
    declaration.write_text(text, encoding="utf-8")
    (moved / "package" / "operators").mkdir()
    with pytest.raises(WorkspaceError, match="冲突"):
        load_workspace(moved)


def test_workspace_validate_rejects_modified_gitignore_managed_block(tmp_path) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    gitignore = root / ".gitignore"
    gitignore.write_text(
        gitignore.read_text(encoding="utf-8").replace("**/*.duckdb\n", ""),
        encoding="utf-8",
    )
    with pytest.raises(WorkspaceError, match="危险文件保护规则"):
        validate_workspace(root)


def test_workspace_cli_validate_fails_for_tracked_generated_file(tmp_path, capsys) -> None:
    root = tmp_path / "workspace"
    initialize_workspace(root)
    subprocess.run(("git", "init", str(root)), check=True, capture_output=True)
    subprocess.run(
        ("git", "-C", str(root), "add", "-f", ".research/index.json"),
        check=True,
        capture_output=True,
    )
    assert main(["workspace", "validate", "--workspace", str(root), "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "fail"
    assert payload["data"]["git"]["tracked_dangerous"] == [".research/index.json"]


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ([], ([], [], None)),
        (["--reuse-run-root", "first-run", "--reuse-run-root", "second-run",
          "--require-reused-node", "panel", "--require-reused-node", "statistics"],
         (["first-run", "second-run"], ["panel", "statistics"], None)),
        (["--reuse-failed-run-root", "failed-run"], ([], [], "failed-run")),
        (["--reuse-failed-run-root", "failed-run", "--require-reused-node", "panel"],
         ([], ["panel"], "failed-run")),
    ],
)
def test_workspace_cli_forwards_reuse_options_to_run_service(
    tmp_path, capsys, monkeypatch, options, expected,
) -> None:
    from research_pipeline.cli.commands import research_run

    root = tmp_path / "workspace"
    initialize_workspace(root)
    clock = "2026-08-06T09:00:00+08:00"
    allocation = allocate_execution(root, clock=clock, root_seed=17)
    captured = {}

    def capture(args):
        captured.update(vars(args))
        return {"status": "result_finalized", "run_id": "a" * 64}

    monkeypatch.setattr(research_run, "_execute", capture)
    assert main([
        "workspace", "run", "--workspace", str(root),
        "--execution", allocation["execution_id"],
        "--plan", str(tmp_path / "plan"),
        "--data-db", str(tmp_path / "unused.duckdb"),
        "--clock", clock, "--root-seed", "17", *options, "--json",
    ]) == 0
    assert (captured["reuse_run_root"], captured["require_reused_node"],
            captured["reuse_failed_run_root"]) == expected
    execution_root = root / ".research/executions" / allocation["execution_id"]
    assert captured["run_root"] == str(execution_root / "run")
    assert captured["clock"] == clock
    assert captured["root_seed"] == 17
    assert json.loads(capsys.readouterr().out)["data"]["run_id"] == "a" * 64
    assert not (tmp_path / "unused.duckdb").exists()


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (["--require-reused-node", "panel"], "必须提供 --reuse-run-root"),
        (["--reuse-run-root", "first-run", "--require-reused-node", "panel",
          "--require-reused-node", "panel"], "不得重复"),
        (["--reuse-run-root", "first-run", "--reuse-failed-run-root", "failed-run"],
         "不能与完成态跨运行复用同时启用"),
    ],
)
def test_workspace_cli_reuses_run_rejection_before_engine_start(
    tmp_path, capsys, monkeypatch, options, message,
) -> None:
    from research_pipeline.cli.commands import research_run

    root = tmp_path / "workspace"
    initialize_workspace(root)
    clock = "2026-08-06T09:00:00+08:00"
    allocation = allocate_execution(root, clock=clock, root_seed=17)

    def unexpected_engine(*args, **kwargs):
        pytest.fail("无效复用选项不应启动运行引擎")

    monkeypatch.setattr(research_run, "_execute_owned_operator_graph", unexpected_engine)
    assert main([
        "workspace", "run", "--workspace", str(root),
        "--execution", allocation["execution_id"],
        "--plan", str(tmp_path / "absent-plan"),
        "--data-db", str(tmp_path / "absent.duckdb"),
        "--clock", clock, "--root-seed", "17", *options, "--json",
    ]) != 0
    payload = json.loads(capsys.readouterr().out)
    assert message in payload["message"]
    assert inspect_workspace(root, allocation["execution_id"])["status"] == "allocated"
    assert not (tmp_path / "absent.duckdb").exists()


@pytest.mark.parametrize("command", ["resume", "retry-node"])
def test_workspace_cli_recovery_preserves_result_and_completion(
    tmp_path, capsys, monkeypatch, command,
) -> None:
    from pathlib import Path

    from research_pipeline.cli.commands import research_run

    root = tmp_path / "workspace"
    initialize_workspace(root)
    allocation = allocate_execution(root, clock="2026-08-06T09:00:00+08:00", root_seed=17)
    execution_root = Path(allocation["execution_path"])
    (execution_root / "run" / "operator-dag-invocation.json").write_text(
        "{}", encoding="utf-8",
    )
    result = {"status": "result_finalized", "run_id": "a" * 64,
              "result_directory": str(execution_root / "results" / "sealed-result")}

    def recover(*, run_root, retry_node_id):
        assert run_root == execution_root / "run"
        assert retry_node_id == ("analysis" if command == "retry-node" else None)
        return result

    monkeypatch.setattr(research_run, "resume_operator_graph", recover)
    argv = [
        "workspace", command, "--workspace", str(root),
        "--execution", allocation["execution_id"], "--json",
    ]
    if command == "retry-node":
        argv += ["--node", "analysis"]
    assert main(argv) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"] == {
        **result, "execution_id": allocation["execution_id"],
        "execution_root": str(execution_root),
    }
    indexed = inspect_workspace(root)["executions"][0]
    assert indexed["run_id"] == result["run_id"]
    assert indexed["status"] == result["status"]
