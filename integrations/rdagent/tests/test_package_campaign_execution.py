"""共用执行桥只补齐缺失验证/报告，不能重新启动已有运行。"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from quantwitness_rdagent import package_execution


@pytest.fixture
def published(tmp_path, monkeypatch):
    from research_pipeline.workspace import initialize_workspace, allocate_execution
    from research_pipeline.application import workspace_flow, research_package, evidence_lifecycle
    import research_pipeline.evidence as evidence
    from test_result_bundle import _finalize
    from archived_observation_support import build_archived_observation_fixture
    paths = build_archived_observation_fixture(tmp_path / "fixture")
    root = tmp_path / "candidate"
    package = Path(__file__).resolve().parents[3] / "examples/equity_cross_section/package"
    initialize_workspace(root / "workspace", workspace_id="campaign-candidate", from_package=package)
    allocation = allocate_execution(root / "workspace", clock="2024-01-09T00:00:00+08:00", root_seed=1701, label="candidate")
    execution = Path(allocation["execution_path"])
    (execution / "plan/admitted").mkdir(parents=True)
    _, _, _, bundle, result, _ = _finalize(execution)
    def forbidden(*args, **kwargs):
        raise AssertionError("已发布Result不能重新启动或恢复Runtime")
    monkeypatch.setattr(workspace_flow, "run_workspace_execution", forbidden)
    monkeypatch.setattr(workspace_flow, "resume_workspace_execution", forbidden)
    monkeypatch.setattr(workspace_flow, "_show_verification_resources", lambda *a: None)
    monkeypatch.setattr(workspace_flow, "_resource_forecast", lambda *a: {})
    monkeypatch.setattr(evidence, "load_verified_result_context", lambda *a, **kw: SimpleNamespace(
        verification=SimpleNamespace(status="pass", claim_level="research_observation", limitations=[]),
        snapshot=SimpleNamespace(bundle=bundle)))
    calls = []
    def package_command(args, *, documents=None):
        calls.append("package")
        return {}
    def evidence_command(args):
        calls.append(args.command)
        path = Path(args.output)
        path.parent.mkdir(exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        return {"output": str(path), "status": "pass", "claim_level": "research_observation"}
    monkeypatch.setattr(research_package, "_execute", package_command)
    monkeypatch.setattr(evidence_lifecycle, "_execute", evidence_command)
    kwargs = {"root": root, "workspace_id": "campaign-candidate", "allocation_label": "candidate", "base_package": str(package),
              "source_archive_root": "archive", "input_snapshot_manifest": str(paths["input_snapshot_manifest"]), "verifier_bundle": "verifier",
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


@pytest.mark.parametrize("other_failure", [None, "statistics", "gate", "integrity", "reproducibility"])
def test_completed_negative_verification_remains_candidate_feedback(published, monkeypatch, other_failure):
    kwargs, execution, result, calls = published
    from research_pipeline.application import evidence_lifecycle
    import research_pipeline.evidence as evidence
    verified = evidence.load_verified_result_context()
    findings = ["formula.value_mismatch", "simulation_claim_ceiling:research_observation"]
    verified.verification.status = "fail"
    verified.verification.limitations = findings
    verified.verification.integrity_status = "fail" if other_failure == "integrity" else "pass"
    verified.verification.reproducibility_status = "fail" if other_failure == "reproducibility" else "pass"
    verified.verification.gate_records = (SimpleNamespace(status="fail" if other_failure == "gate" else "pass"),)
    if other_failure == "statistics":
        findings.append("statistics.mismatch")
    monkeypatch.setattr(evidence, "load_verified_result_context", lambda *a, **kw: verified)
    original = evidence_lifecycle._execute

    def command(args):
        outcome = original(args)
        if args.command == "verify":
            outcome["status"] = "fail"
        return outcome

    monkeypatch.setattr(evidence_lifecycle, "_execute", command)
    if other_failure is not None:
        with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
            package_execution._execute_package(**kwargs)
        assert captured.value.package_execution_feedback["verification_status"] == "fail"
        return
    first = package_execution._execute_package(**kwargs)
    assert first["command_status"] == first["execution_status"] == "succeeded"
    assert first["verification_status"] == "fail"
    assert first["diagnostics"] == findings
    assert first["result_ref"] == str(result)
    assert calls == ["package", "verify", "report"]
    calls.clear()
    assert package_execution._execute_package(**kwargs) == first
    assert calls == ["package"]


def test_published_result_without_reference_uses_inspect_not_runtime(published, monkeypatch):
    kwargs, execution, result, calls = published
    (execution / "run/result-ref.json").rename(execution / "result-reference.retained")
    (execution / "run/operator-dag-invocation.json").write_text("{}", encoding="utf-8")
    from research_pipeline.application import runtime_lifecycle
    from research_pipeline.results import ResultStore
    bundle = ResultStore(execution / "results", create=False).inspect_directory(result)
    monkeypatch.setattr(runtime_lifecycle, "_execute", lambda args: {"recommended_action": "verify",
        "finalize": {"result_published": True, "result_directory": str(result), "result_id": bundle.result_id}})
    feedback = package_execution._execute_package(**kwargs)
    assert feedback["result_ref"] == str(result)
    assert calls == ["package", "verify", "report"]


def test_unknown_execution_error_preserves_stage_and_propagates(published, monkeypatch):
    kwargs, execution, result, calls = published
    from research_pipeline.application import evidence_lifecycle
    original = evidence_lifecycle._execute
    def command(args):
        if args.command == "verify":
            raise RuntimeError("verifier transport interrupted")
        return original(args)
    monkeypatch.setattr(evidence_lifecycle, "_execute", command)
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
    (execution / "run/result-finalize.json").unlink(missing_ok=True)
    from research_pipeline.application import runtime_lifecycle
    monkeypatch.setattr(runtime_lifecycle, "_execute", lambda args: {
        "recommended_action": "wait", "recommendation_reason": "owner_live", "nodes": {}, "finalize": {"result_published": False}})
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution._execute_package(**kwargs)
    assert captured.value.package_execution_feedback["failed_stage"] == "run"
    assert (kwargs["root"] / "execution-diagnostic.json").exists()
    assert (execution / "run/operator-dag-invocation.json").exists()


def test_verification_memory_bytes_is_forwarded_to_verify(published, monkeypatch):
    kwargs, execution, result, calls = published
    kwargs["binding"]["verification_memory_bytes"] = 4 * 1024 ** 3
    seen = []
    from research_pipeline.application import evidence_lifecycle
    original = evidence_lifecycle._execute
    def command(args):
        seen.append(args)
        return original(args)
    monkeypatch.setattr(evidence_lifecycle, "_execute", command)
    package_execution._execute_package(**kwargs)
    verify = next(args for args in seen if args.command == "verify")
    assert verify.verification_memory_bytes == 4 * 1024 ** 3

def test_real_isolated_rd_package_uses_workspace_flow(tmp_path):
    from archived_observation_support import build_archived_observation_fixture
    from quantwitness_rdagent.package_execution import execute_package
    paths = build_archived_observation_fixture(tmp_path / "fixture")
    root = tmp_path / "candidate"
    arguments = dict(root=root, workspace_id="formal-orchestration", allocation_label="candidate-1",
        base_package=str(paths["package"]), source_archive_root=None,
        input_snapshot_manifest=str(paths["input_snapshot_manifest"]), verifier_bundle=None,
        binding={"catalog_lock": str(paths["catalog_lock"]), "extension_bundles": [], "runtime_options": {},
            "verification_process_slots": 2})
    first = execute_package(**arguments)
    assert first["command_status"] == "succeeded" and first["verification_status"] == "pass"
    execution = Path(first["execution_ref"])
    frozen = (execution / "run/operator-dag-invocation.json").read_bytes()
    repeated = execute_package(**arguments)
    assert repeated == first
    assert (execution / "run/operator-dag-invocation.json").read_bytes() == frozen
    assert len(list((root / "workspace/.research/executions").iterdir())) == 1
    continuation = json.loads((execution / "continuation.json").read_text(encoding="utf-8"))
    assert continuation["outcome"]["verification_status"] == "pass"
    assert (execution / "completion.json").is_file()


def test_failed_verification_keeps_result_and_failed_rd_feedback(published, monkeypatch):
    from research_pipeline.application import evidence_lifecycle
    import research_pipeline.evidence as evidence
    kwargs, execution, result, calls = published
    original = evidence_lifecycle._execute

    def failed(args):
        response = original(args)
        if args.command == "verify":
            response["status"] = "fail"
        return response

    monkeypatch.setattr(evidence_lifecycle, "_execute", failed)
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution._execute_package(**kwargs)
    feedback = captured.value.package_execution_feedback
    assert feedback["command_status"] == "failed"
    assert feedback["execution_status"] == "succeeded"
    assert feedback["verification_status"] == "fail"
    assert feedback["result_ref"] == str(result)
    assert feedback["failed_stage"] == "verify"
    assert (execution / "report.md").is_file()
    original_context = evidence.load_verified_result_context

    def failed_context(*args, **kwargs):
        context = original_context(*args, **kwargs)
        context.verification.status = "fail"
        return context

    monkeypatch.setattr(evidence, "load_verified_result_context", failed_context)
    calls.clear()
    with pytest.raises(package_execution.PackageExecutionInterrupted) as repeated:
        package_execution._execute_package(**kwargs)
    assert repeated.value.package_execution_feedback["verification_status"] == "fail"
    assert calls == ["package"]
