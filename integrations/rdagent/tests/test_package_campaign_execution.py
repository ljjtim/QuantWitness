"""RD-Agent 委托 Workspace 并读取正式 Result 的完成与中断合同。"""
from pathlib import Path
from types import SimpleNamespace

import pytest
from quantwitness_rdagent import package_execution


@pytest.fixture
def published(tmp_path, monkeypatch):
    import research_pipeline.results as results
    from research_pipeline.application import workspace_flow
    root = tmp_path / "candidate"
    (root / "workspace").mkdir(parents=True)
    execution = root / "workspace/.research/executions/existing"
    result = execution / "results/result"
    context = SimpleNamespace(snapshot=SimpleNamespace(bundle=SimpleNamespace(run=SimpleNamespace(status="succeeded"))),
        claim_level="research_observation", limitations=("lean_next_completed_close_v1", "input_claim_ceiling:prices:research_observation"))
    calls = []
    def execute(args, *, allocation_handler):
        calls.append(args)
        return {"status": "succeeded", "execution_status": "succeeded", "result_directory": str(result),
                "result_id": "result-id", "result_store": str(execution / "results"), "report_path": str(execution / "report.md")}
    monkeypatch.setattr(workspace_flow, "execute_workspace", execute)
    monkeypatch.setattr(results, "load_result_context", lambda *a, **kw: context)
    kwargs = {"root": root, "workspace_id": "campaign", "allocation_label": "candidate", "base_package": "package",
              "source_archive_root": "archive", "input_snapshot_manifest": "snapshot.json",
              "binding": {"catalog_lock": "catalog", "extension_bundles": [], "runtime_options": {}}}
    return kwargs, context, result, calls


def test_completed_workspace_returns_result_and_limitations(published):
    kwargs, context, result, calls = published
    feedback = package_execution._execute_package(**kwargs)
    assert feedback["command_status"] == feedback["execution_status"] == "succeeded"
    assert feedback["result_ref"] == str(result)
    assert feedback["result_id"] == "result-id"
    assert feedback["diagnostics"] == list(context.limitations)
    assert len(calls) == 1


def test_incomplete_result_cannot_be_successful_candidate(published):
    kwargs, context, _, _ = published
    context.snapshot.bundle.run.status = "failed"
    with pytest.raises(ValueError, match="尚未完成"):
        package_execution._execute_package(**kwargs)


def test_runtime_interruption_keeps_execution_and_stage(published, monkeypatch):
    kwargs, _, result, _ = published
    from research_pipeline.application import workspace_flow
    def interrupted(*a, **kw):
        error = RuntimeError("运行等待资源")
        error.failure_payload = {"failed_stage": "run", "execution_status": "failed", "execution_root": str(result.parents[1])}
        raise error
    monkeypatch.setattr(workspace_flow, "execute_workspace", interrupted)
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution._execute_package(**kwargs)
    feedback = captured.value.package_execution_feedback
    assert feedback["failed_stage"] == "run"
    assert feedback["execution_status"] == "failed"
    assert feedback["result_ref"] is None
    assert (kwargs["root"] / "execution-diagnostic.json").is_file()


def test_real_isolated_rd_package_reuses_workspace_execution(tmp_path):
    from archived_observation_support import build_archived_observation_fixture
    paths = build_archived_observation_fixture(tmp_path / "fixture")
    arguments = dict(root=tmp_path / "candidate", workspace_id="formal-orchestration", allocation_label="candidate-1",
        base_package=str(paths["package"]), source_archive_root=None,
        input_snapshot_manifest=str(paths["input_snapshot_manifest"]),
        binding={"catalog_lock": str(paths["catalog_lock"]), "extension_bundles": [], "runtime_options": {}})
    first = package_execution.execute_package(**arguments)
    assert first["command_status"] == first["execution_status"] == "succeeded"
    execution = Path(first["execution_ref"])
    frozen = (execution / "run/operator-dag-invocation.json").read_bytes()
    assert package_execution.execute_package(**arguments) == first
    assert (execution / "run/operator-dag-invocation.json").read_bytes() == frozen
    assert len(list((arguments["root"] / "workspace/.research/executions").iterdir())) == 1
