from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_pipeline.cli.result import execute_guarded
from research_pipeline.runtime.errors import RuntimeWorkerError
from research_pipeline.runtime.store import EventStore


def test_runtime_worker_failure_json_includes_root_error_and_inspect_command(
    capfd, tmp_path: Path,
) -> None:
    run_root = tmp_path / "run-v1"
    EventStore(run_root).append(
        "run-1",
        "diagnostic",
        {
            "error_code": "TypeError",
            "exception_type": "TypeError",
            "message": "nested must be dict",
        },
        command_id="analysis:diagnostic",
        node_id="analysis",
        attempt_id="attempt-1",
    )
    args = SimpleNamespace(json=True, run_root=str(run_root))

    def fail(_args):
        raise RuntimeWorkerError(
            "operator 节点执行失败: analysis",
        )

    assert execute_guarded(args, fail) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["data"]["failed_node"] == "analysis"
    assert payload["data"]["root_error"]["message"] == "nested must be dict"
    assert payload["data"]["next_command_argv"] == [
        "python",
        "-m",
        "research_pipeline",
        "inspect",
        "--run-root",
        str(run_root),
        "--json",
    ]


def test_non_runtime_failure_does_not_project_stale_runtime_diagnostic(
    capfd,
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "run-v1"
    EventStore(run_root).append(
        "run-1",
        "diagnostic",
        {
            "error_code": "TypeError",
            "exception_type": "TypeError",
            "message": "stale runtime error",
        },
        command_id="analysis:diagnostic",
        node_id="analysis",
        attempt_id="attempt-1",
    )
    args = SimpleNamespace(json=True, run_root=str(run_root))

    def fail(_args):
        raise ValueError("package input invalid")

    assert execute_guarded(args, fail) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["data"] is None
    assert payload["message"] == "package input invalid"


def test_verify_cli_forwards_default_and_explicit_process_slots(monkeypatch, capfd, tmp_path):
    from research_pipeline.cli import main
    from research_pipeline.cli.commands import evidence_lifecycle

    observed = []

    def capture(*args, **kwargs):
        observed.append(kwargs["project_verifier_process_slots"])
        raise ValueError("预算已转发")

    monkeypatch.setattr(evidence_lifecycle, "verify_result", capture)
    for options, expected in (([], 2), (["--verification-process-slots", "3"], 3)):
        assert main([
            "verify", "--result", str(tmp_path / "result"),
            "--result-store", str(tmp_path / "store"),
            "--output", str(tmp_path / "verification.json"),
            *options, "--json",
        ]) == 1
        assert json.loads(capfd.readouterr().out)["message"] == "预算已转发"
        assert observed[-1] == expected


@pytest.fixture
def workspace_failure_cli(tmp_path, monkeypatch):
    import duckdb

    from research_pipeline.cli.commands import research_run
    from research_pipeline.workspace import allocate_execution, initialize_workspace

    def reject_database(*args, **kwargs):
        pytest.fail("Workspace CLI 诊断测试不得连接数据库")

    monkeypatch.setattr(duckdb, "connect", reject_database)
    root = tmp_path / "workspace"
    initialize_workspace(root)
    clock = "2026-08-06T09:00:00+08:00"
    allocation = allocate_execution(root, clock=clock, root_seed=17)
    execution_root = Path(allocation["execution_path"])
    run_root = execution_root / "run"

    def prepare(command, handler, *, with_invocation=True, execution_id=None):
        argv = [
            "workspace", command, "--workspace", str(root),
            "--execution", execution_id or allocation["execution_id"], "--json",
        ]
        if command == "run":
            monkeypatch.setattr(research_run, "_execute", handler)
            argv += [
                "--plan", str(tmp_path / "unused-plan"),
                "--data-db", str(tmp_path / "unused.duckdb"),
                "--clock", clock, "--root-seed", "17",
            ]
        else:
            monkeypatch.setattr(research_run, "resume_operator_graph", handler)
            if with_invocation:
                (run_root / "operator-dag-invocation.json").write_text(
                    "{}", encoding="utf-8",
                )
            if command == "retry-node":
                argv += ["--node", "analysis"]
        return argv

    return prepare, run_root


@pytest.mark.parametrize("command", ["run", "resume", "retry-node"])
@pytest.mark.parametrize("failure", ["worker", "worker_without_events", "non_worker"])
def test_workspace_cli_failure_uses_existing_runtime_projection(
    workspace_failure_cli, capfd, command, failure,
):
    from research_pipeline.cli import main

    prepare, run_root = workspace_failure_cli
    root_error = {
        "error_code": "TypeError",
        "exception_type": "TypeError",
        "message": "输入字段类型不匹配",
    }
    if failure != "worker_without_events":
        EventStore(run_root).append(
            "run-1", "diagnostic", root_error,
            command_id="analysis:diagnostic", node_id="analysis", attempt_id="attempt-1",
        )
    events_path = run_root / "events.jsonl"
    original_events = events_path.read_bytes() if events_path.exists() else None

    def fail(*args, **kwargs):
        actual_root = args[0].run_root if command == "run" else kwargs["run_root"]
        assert Path(actual_root) == run_root
        if command != "run":
            assert kwargs["retry_node_id"] == ("analysis" if command == "retry-node" else None)
        if failure == "non_worker":
            raise ValueError("研究计划未通过校验")
        raise RuntimeWorkerError(
            "operator 节点执行失败: analysis", error_code="TypeError",
            failure_payload={"detail": "保留原异常载荷"},
        )

    assert main(prepare(command, fail)) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["status"] == "fail"
    assert payload["contract_version"] == "research-cli-result-v1"
    if failure == "non_worker":
        assert payload["message"] == "研究计划未通过校验"
        assert payload["data"] is None
    elif failure == "worker_without_events":
        assert payload["data"] == {"detail": "保留原异常载荷"}
    else:
        assert payload["error_code"] == "TypeError"
        data = payload["data"]
        assert data["detail"] == "保留原异常载荷"
        assert data["failed_node"] == "analysis"
        assert data["root_error"] == root_error
        assert Path(data["run_root"]) == run_root
        assert data["next_command_argv"] == [
            "python", "-m", "research_pipeline", "inspect",
            "--run-root", str(run_root), "--json",
        ]
        assert "inspect" in data["next_command"]
        assert str(run_root) in data["next_command"]
    assert (events_path.read_bytes() if events_path.exists() else None) == original_events


@pytest.mark.parametrize("command", ["run", "resume", "retry-node"])
@pytest.mark.parametrize("invalid_execution", ["unknown", "declaration"])
def test_workspace_cli_invalid_execution_keeps_original_error(
    workspace_failure_cli, capfd, command, invalid_execution,
):
    from research_pipeline.cli import main

    prepare, run_root = workspace_failure_cli

    def unexpected(*args, **kwargs):
        pytest.fail("非法 execution 不得调用运行或恢复服务")

    argv = prepare(
        command, unexpected,
        execution_id="../unknown" if invalid_execution == "unknown" else None,
    )
    if invalid_execution == "declaration":
        declaration = run_root.parent / "execution.json"
        payload = json.loads(declaration.read_text(encoding="utf-8"))
        payload["workspace_id"] = "another-workspace"
        declaration.write_text(json.dumps(payload), encoding="utf-8")
    assert main(argv) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["message"] == (
        "execution_id 不存在" if invalid_execution == "unknown" else "execution 声明身份不匹配"
    )
    assert payload["error_code"] == "value_error"
    assert payload["data"] is None
    assert not (run_root / "events.jsonl").exists()


@pytest.mark.parametrize("command", ["resume", "retry-node"])
def test_workspace_cli_without_invocation_keeps_original_error(
    workspace_failure_cli, capfd, command,
):
    from research_pipeline.cli import main

    prepare, run_root = workspace_failure_cli

    def unexpected(*args, **kwargs):
        pytest.fail("缺少 invocation 时不得调用恢复服务")

    assert main(prepare(command, unexpected, with_invocation=False)) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["message"] == "execution 尚无可验证的正式 Runtime invocation"
    assert payload["error_code"] == "value_error"
    assert payload["data"] is None
    assert not (run_root / "events.jsonl").exists()
