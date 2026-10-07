"""共用执行桥只补齐缺失验证/报告，不能重新启动已有运行。"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from quantwitness_rdagent import package_execution, worker


@pytest.fixture
def published(tmp_path, monkeypatch):
    import research_pipeline.workspace as workspace
    import research_pipeline.packages as packages
    import research_pipeline.results as results
    import research_pipeline.evidence as evidence
    root = tmp_path / "candidate"
    execution = root / "workspace/.research/executions/run1"
    run = execution / "run"
    run.mkdir(parents=True)
    (execution / "plan/admitted").mkdir(parents=True)
    result = execution / "results/result1"
    result.mkdir(parents=True)
    (run / "result-ref.json").write_text(json.dumps({"project_id": "p", "run_id": "r", "result_id": "x"}), encoding="utf-8")
    monkeypatch.setattr(workspace, "load_workspace", lambda root: SimpleNamespace(package_root=tmp_path / "package"))
    monkeypatch.setattr(packages, "load_research_package", lambda root: SimpleNamespace(spec_payload={"fixed_clock": "2025-01-13T16:00:00+08:00", "root_seed": 17}))
    monkeypatch.setattr(worker, "find_or_allocate", lambda *a, **kw: {"execution_id": "run1", "execution_path": str(execution)})
    def forbidden(*args, **kwargs):
        raise AssertionError("已发布Result不能重新启动或恢复Runtime")
    monkeypatch.setattr(workspace, "run_workspace_execution", forbidden)
    monkeypatch.setattr(workspace, "resume_workspace_execution", forbidden)
    class Store:
        def __init__(self, path):
            pass
        def load_by_identity(self, **kwargs):
            return object()
        def result_directory(self, bundle):
            return result
        def open_snapshot(self, path):
            return SimpleNamespace(bundle=object())
    monkeypatch.setattr(results, "ResultStore", Store)
    monkeypatch.setattr(evidence, "load_verified_result_context", lambda *a, **kw: SimpleNamespace(
        verification=SimpleNamespace(status="pass", claim_level="research_observation", limitations=[])))
    calls = []
    def command(argv):
        calls.append(argv[0])
        if argv[0] == "inspect":
            return {"recommended_action": "verify", "finalize": {"result_published": True, "result_directory": str(result)}}
        if argv[0] in {"verify", "report"}:
            path = Path(argv[argv.index("--output") + 1])
            path.parent.mkdir(exist_ok=True)
            path.write_text("{}", encoding="utf-8")
        return {}
    monkeypatch.setattr(worker, "_command", command)
    kwargs = {"root": root, "workspace_id": "campaign-candidate", "allocation_label": "candidate", "base_package": "base",
              "source_archive_root": "archive", "input_snapshot_manifest": "snapshot", "verifier_bundle": "verifier",
              "binding": {"catalog_lock": "catalog", "extension_bundles": [], "runtime_options": {}, "verification_process_slots": 2}}
    return kwargs, execution, result, calls


def test_existing_result_only_runs_missing_verification_and_report(published):
    kwargs, execution, result, calls = published
    feedback = package_execution._execute_package(**kwargs)
    assert feedback["verification_status"] == "pass"
    assert feedback["result_ref"] == str(result)
    assert calls == ["package", "verify", "report"]
    calls.clear()
    repeated = package_execution._execute_package(**kwargs)
    assert repeated == feedback
    assert calls == ["package"]


def test_published_result_without_reference_uses_inspect_not_runtime(published):
    kwargs, execution, result, calls = published
    (execution / "run/result-ref.json").rename(execution / "result-reference.retained")
    (execution / "run/operator-dag-invocation.json").write_text("{}", encoding="utf-8")
    feedback = package_execution._execute_package(**kwargs)
    assert feedback["result_ref"] == str(result)
    assert calls == ["package", "inspect", "verify", "report"]


def test_unknown_execution_error_preserves_stage_and_propagates(published, monkeypatch):
    kwargs, execution, result, calls = published
    original = worker._command
    def command(argv):
        if argv[0] == "verify":
            raise RuntimeError("verifier transport interrupted")
        return original(argv)
    monkeypatch.setattr(worker, "_command", command)
    with pytest.raises(package_execution.PackageExecutionInterrupted, match="Runtime诊断") as captured:
        package_execution._execute_package(**kwargs)
    evidence = captured.value.package_execution_feedback
    assert evidence["failed_stage"] == "verify"
    assert evidence["execution_status"] == "succeeded"
    assert evidence["result_ref"] == str(result)


def test_runtime_wait_keeps_existing_execution_for_manual_resume(published, monkeypatch):
    kwargs, execution, result, calls = published
    (execution / "run/result-ref.json").rename(execution / "result-reference.retained")
    (execution / "run/operator-dag-invocation.json").write_text("{}", encoding="utf-8")
    original = worker._command
    def command(argv):
        if argv[0] == "inspect":
            return {"recommended_action": "wait", "recommendation_reason": "owner_live", "nodes": {}, "finalize": {"result_published": False}}
        return original(argv)
    monkeypatch.setattr(worker, "_command", command)
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution._execute_package(**kwargs)
    assert captured.value.package_execution_feedback["failed_stage"] == "run"
    assert (kwargs["root"] / "execution-diagnostic.json").exists()
    assert (execution / "run/operator-dag-invocation.json").exists()


def test_verification_memory_bytes_is_forwarded_to_verify(published, monkeypatch):
    kwargs, execution, result, calls = published
    kwargs["binding"]["verification_memory_bytes"] = 4 * 1024 ** 3
    seen = []
    original = worker._command
    def command(argv):
        seen.append(list(argv))
        return original(argv)
    monkeypatch.setattr(worker, "_command", command)
    package_execution._execute_package(**kwargs)
    verify = next(argv for argv in seen if argv[0] == "verify")
    assert verify[verify.index("--verification-memory-bytes") + 1] == str(4 * 1024 ** 3)
