"""正式执行进程与替代 Workspace 关联；测试仅使用文件工件。"""
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from quantwitness_rdagent import package_execution, worker


def arguments(root):
    return {"root": root, "workspace_id": "campaign-candidate", "allocation_label": "candidate",
            "base_package": str(root / "base"), "source_archive_root": str(root / "archive"),
            "input_snapshot_manifest": str(root / "snapshot.json"), "verifier_bundle": str(root / "verifier"),
            "binding": {"catalog_lock": str(root / "catalog"), "extension_bundles": [], "runtime_options": {},
                        "verification_process_slots": 2}}


def test_public_process_receives_only_execution_inputs(tmp_path, monkeypatch):
    kwargs = arguments(tmp_path)
    kwargs["binding"]["model_env_path"] = "private-model.env"
    kwargs["binding"]["api_key"] = "private-value"
    monkeypatch.setenv("OPENAI_API_KEY", "private-value")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / "mpl"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    seen = []
    def run(argv, **options):
        seen.append((argv, options))
        return SimpleNamespace(returncode=0, stdout=json.dumps({"feedback": {"command_status": "succeeded"}}))
    monkeypatch.setattr(package_execution.subprocess, "run", run)
    assert package_execution.execute_package(**kwargs) == {"command_status": "succeeded"}
    argv, options = seen[0]
    assert argv[0] == sys.executable and argv[-1] == "quantwitness_rdagent.package_execution"
    assert "OPENAI_API_KEY" not in options["env"]
    assert options["env"]["MPLCONFIGDIR"] == str(tmp_path / "mpl")
    assert options["env"]["XDG_CACHE_HOME"] == str(tmp_path / "cache")
    assert "private-" not in options["input"]
    assert json.loads(options["input"])["arguments"]["binding"]["verification_process_slots"] == 2


def test_formal_interruption_survives_process_boundary(tmp_path, monkeypatch):
    feedback = {"failed_stage": "run", "execution_ref": "execution", "diagnostics": [
        {"error_code": "project_worker_resource_exceeded", "recommended_action": "readmit-new-run"}]}
    response = {"failure": {"error_type": "PackageExecutionInterrupted", "message": "正式执行中断",
                           "interrupted": True, "package_execution_feedback": feedback}}
    monkeypatch.setattr(package_execution.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout=json.dumps(response)))
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution.execute_package(**arguments(tmp_path))
    assert captured.value.package_execution_feedback == feedback


def test_real_execution_child_excludes_rd_and_model_environment(tmp_path, monkeypatch):
    probe = tmp_path / "process-probe"
    probe.mkdir()
    evidence = probe / "evidence.json"
    (probe / "sitecustomize.py").write_text(
        "import atexit,json,os,sys\n"
        "from pathlib import Path\n"
        "@atexit.register\n"
        "def save():\n"
        f"    Path({str(evidence)!r}).write_text(json.dumps({{'pid':os.getpid(),"
        "'rd_modules':[name for name in sys.modules if name == 'rdagent' or name.startswith('rdagent.')],"
        "'model_env_present':'OPENAI_API_KEY' in os.environ}),encoding='utf-8')\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(probe))
    monkeypatch.setenv("OPENAI_API_KEY", "unused-secret")
    with pytest.raises((RuntimeError, ValueError, FileNotFoundError)) as captured:
        package_execution.execute_package(**arguments(tmp_path / "candidate"))
    assert captured.value.package_execution_feedback["failed_stage"] == "prepare"
    recorded = json.loads(evidence.read_text(encoding="utf-8"))
    assert recorded["pid"] != os.getpid()
    assert recorded["rd_modules"] == [] and recorded["model_env_present"] is False


def test_active_calculation_child_blocks_duplicate_then_allows_recovery(tmp_path):
    root = tmp_path / "candidate"
    lock_path = root / ".package-execution.lock"
    script = ("import sys;from pathlib import Path;"
              "from research_pipeline.runtime.store import _StoreLock;"
              "lock=_StoreLock(Path(sys.argv[1]));lock.__enter__();"
              "print('ready',flush=True);sys.stdin.readline();lock.__exit__()")
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join(str(Path(p).resolve()) for p in sys.path if p))
    holder = subprocess.Popen([sys.executable, "-c", script, str(lock_path)], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", env=environment)
    try:
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
            package_execution.execute_package(**arguments(root))
        assert captured.value.package_execution_feedback["diagnostics"][0]["recommended_action"] == "wait"
        assert not (root / "workspace").exists()
        holder.communicate("finish\n", timeout=30)
        assert holder.returncode == 0 and not lock_path.exists()
        with pytest.raises((ValueError, RuntimeError, FileNotFoundError)) as recovered:
            package_execution.execute_package(**arguments(root))
        assert recovered.value.package_execution_feedback["failed_stage"] == "prepare"
    finally:
        if holder.poll() is None:
            holder.kill()
        holder.communicate(timeout=30)


@pytest.fixture
def replacement(tmp_path, monkeypatch):
    import research_pipeline.workspace as workspace
    import research_pipeline.packages as packages
    import research_pipeline.cli.research_plan_store as plan_store
    import research_pipeline.data_plane.archived_inputs as archived
    import research_pipeline.extensions as extensions
    import research_pipeline.results as results
    import research_pipeline.evidence as evidence

    root = tmp_path / "candidate"
    kwargs = arguments(root)
    config = SimpleNamespace(root=root / "workspace", package_root=root / "workspace/package",
                             generated_root=root / "workspace/.research", workspace_id=kwargs["workspace_id"])
    package = SimpleNamespace(package_hash="package", spec_payload={"fixed_clock": "2025-01-13T16:00:00+08:00", "root_seed": 17})
    allocations = {identifier: {"execution_id": identifier, "workspace_id": config.workspace_id,
                    "clock": package.spec_payload["fixed_clock"], "root_seed": 17, "label": label}
                   for identifier, label in (("old", "candidate"), ("new", "candidate-readmit-1"))}
    manifests = {identifier: {"manifest_hash": "manifest", "package_hash": "package", "package_plan_hash": "plan",
                 "fixed_clock": package.spec_payload["fixed_clock"], "root_seed": 17,
                 "input_snapshot_manifest": {"requests": {"r": {"root": "archive"}}},
                 "verifier_admission": {"bundle_hash": "verifier"}} for identifier in allocations}
    for identifier in allocations:
        execution = config.generated_root / "executions" / identifier
        (execution / "plan/admitted").mkdir(parents=True)
        (execution / "run").mkdir()
        invocation = {"clock": package.spec_payload["fixed_clock"], "root_seed": 17,
                      "plan": str(execution / "plan/admitted"), "run_root": str(execution / "run"),
                      "artifact_root": str(execution / "artifacts"), "handoff_out": str(execution / "handoff.json"),
                      "result_store": str(execution / "results"), "input_snapshot_manifest": kwargs["input_snapshot_manifest"],
                      "data_db": None, "source_dbs": [], "mode": "deterministic_serial", "resource_capacity": {"memory_bytes": 1024},
                      "resource_governance": {"process_slots": 3}, "reuse_run_roots": []}
        (execution / "run/operator-dag-invocation.json").write_text(json.dumps(invocation), encoding="utf-8")
    new = config.generated_root / "executions/new"
    (new / "run/result-ref.json").write_text(json.dumps({"project_id": "p", "run_id": "r", "result_id": "result"}), encoding="utf-8")
    previous = {"execution_id": "old", "execution_root": str(config.generated_root / "executions/old")}
    (root / "execution-ref.json").write_text(json.dumps(previous), encoding="utf-8")
    (root / "execution-diagnostic.json").write_text('{"error_code":"project_worker_resource_exceeded"}', encoding="utf-8")
    bundle = SimpleNamespace(result_id="result", package_hash="package", plan_hash="plan")
    verification = SimpleNamespace(status="pass", claim_level="research_observation", limitations=[])
    monkeypatch.setattr(workspace, "load_workspace", lambda p: config)
    def inspect(p, identifier):
        if identifier not in allocations:
            raise ValueError("execution_id 不存在")
        return allocations[identifier]
    monkeypatch.setattr(workspace, "inspect_workspace", inspect)
    monkeypatch.setattr(packages, "load_research_package", lambda p: package)
    monkeypatch.setattr(plan_store, "load_operator_graph_research_plan", lambda *, target: (
        manifests[Path(target).parent.parent.name], {"r": SimpleNamespace(catalog_hash="catalog")}, None, None, None))
    monkeypatch.setattr(archived, "load_archived_input_manifest", lambda p: {"requests": {"r": {"root": "archive"}}})
    monkeypatch.setattr(extensions, "verify_project_verifier_bundle", lambda p: SimpleNamespace(bundle_hash="verifier"))
    def command(argv):
        if argv[:2] == ["package", "lint"]:
            return {"checks": {"fields": {"catalog_hash": "catalog"}}}
        if argv[0] in {"verify", "report"}:
            path = Path(argv[argv.index("--output") + 1])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
        return {}
    monkeypatch.setattr(worker, "_command", command)
    class Store:
        def __init__(self, *a, **kw):
            pass
        def load_by_identity(self, **kw):
            return bundle
        def result_directory(self, value):
            return new / "results/result"
    monkeypatch.setattr(results, "ResultStore", Store)
    monkeypatch.setattr(evidence, "load_verified_result_context", lambda *a, **kw: SimpleNamespace(
        verification=verification, snapshot=SimpleNamespace(bundle=bundle)))
    return kwargs, config, package, allocations, manifests, new, bundle, verification


def test_adopted_execution_stably_reuses_formal_result(replacement, monkeypatch):
    kwargs, config, package, allocations, manifests, new, bundle, verification = replacement
    adoption = package_execution._adopt_package_execution(execution_id="new", **kwargs)
    root = Path(kwargs["root"])
    assert json.loads((root / "execution-ref.json").read_text())["execution_id"] == "new"
    assert json.loads((root / "execution-history/old/execution-ref.json").read_text())["execution_id"] == "old"
    assert (root / "execution-history/old/execution-diagnostic.json").read_bytes() == (root / "execution-diagnostic.json").read_bytes()
    selected = []
    def select(*a, execution_id=None, **kw):
        selected.append(execution_id)
        assert execution_id == "new"
        return {"execution_id": "new", "execution_path": str(new)}
    monkeypatch.setattr(worker, "find_or_allocate", select)
    def forbidden(*a, **kw):
        raise AssertionError("采用后不得重算已有正式 Result")
    import research_pipeline.workspace as workspace
    monkeypatch.setattr(workspace, "run_workspace_execution", forbidden)
    monkeypatch.setattr(workspace, "resume_workspace_execution", forbidden)
    first = package_execution._execute_package(**kwargs)
    repeated = package_execution._execute_package(**kwargs)
    assert first == repeated and first["result_ref"] == str(new / "results/result")
    assert selected == ["new", "new"]
    assert package_execution._adopt_package_execution(execution_id="new", **kwargs) == adoption


@pytest.mark.parametrize("drift", ["workspace", "package", "input", "catalog", "bundle", "clock", "seed", "plan", "invocation", "verification"])
def test_replacement_identity_drift_is_rejected(replacement, monkeypatch, drift):
    kwargs, config, package, allocations, manifests, new, bundle, verification = replacement
    if drift == "workspace":
        config.workspace_id = "other-workspace"
    elif drift == "package":
        manifests["new"]["package_hash"] = "other-package"
    elif drift == "input":
        manifests["new"]["input_snapshot_manifest"] = {"requests": {"r": {"root": "other"}}}
    elif drift == "catalog":
        monkeypatch.setattr(worker, "_command", lambda argv: {"checks": {"fields": {"catalog_hash": "other"}}})
    elif drift == "bundle":
        manifests["new"]["project_admission"] = {"bundle_hashes": ["other"]}
    elif drift == "clock":
        allocations["new"]["clock"] = "2025-01-14T16:00:00+08:00"
    elif drift == "seed":
        allocations["new"]["root_seed"] = 18
    elif drift == "plan":
        manifests["new"]["package_plan_hash"] = "other-plan"
    elif drift == "invocation":
        path = new / "run/operator-dag-invocation.json"
        invocation = json.loads(path.read_text())
        invocation["plan"] = str(new.parent / "old/plan/admitted")
        path.write_text(json.dumps(invocation), encoding="utf-8")
    else:
        verification.status = "fail"
    with pytest.raises(ValueError):
        package_execution._adopt_package_execution(execution_id="new", **kwargs)
    assert not (Path(kwargs["root"]) / "execution-adoption.json").exists()
    assert json.loads((Path(kwargs["root"]) / "execution-ref.json").read_text())["execution_id"] == "old"


def test_adopt_rejects_execution_from_other_workspace(replacement):
    kwargs, *_ = replacement
    with pytest.raises(ValueError, match="execution_id"):
        package_execution._adopt_package_execution(execution_id="foreign", **kwargs)


def test_public_adoption_uses_fixed_process_operation(tmp_path, monkeypatch):
    seen = []
    def run(argv, **options):
        seen.append(json.loads(options["input"]))
        return SimpleNamespace(returncode=0, stdout=json.dumps({"feedback": {"execution_id": "new"}}))
    monkeypatch.setattr(package_execution.subprocess, "run", run)
    assert package_execution.adopt_package_execution(execution_id="new", **arguments(tmp_path)) == {"execution_id": "new"}
    assert seen[0]["operation"] == "adopt" and seen[0]["arguments"]["execution_id"] == "new"


def test_explicit_execution_selection_ignores_original_label(replacement, monkeypatch):
    kwargs, config, package, allocations, *_ = replacement
    import research_pipeline.workspace as workspace
    monkeypatch.setattr(workspace, "rebuild_workspace_index", lambda p: {"executions": [
        {**allocations["old"], "label": "candidate"}, {**allocations["new"], "label": "candidate"}]})
    selected = worker.find_or_allocate(config.root, "candidate", clock=package.spec_payload["fixed_clock"],
                                      root_seed=17, execution_id="new")
    assert selected["execution_id"] == "new" and selected["execution_path"] == str(config.generated_root / "executions/new")
    with pytest.raises(ValueError, match="时钟或种子"):
        worker.find_or_allocate(config.root, "candidate", clock=package.spec_payload["fixed_clock"],
                                root_seed=18, execution_id="new")


def test_worker_preserves_carried_formal_failure(tmp_path):
    error = package_execution.PackageExecutionInterrupted("正式执行中断")
    error.package_execution_feedback = {"diagnostics": [{"stage": "run", "error_type": "RuntimeWorkerError",
        "error_code": "project_worker_resource_exceeded", "recommended_action": "readmit-new-run"}]}
    assert worker.failure_diagnostics(error, stage="run", execution=tmp_path) == error.package_execution_feedback["diagnostics"]


def test_adopted_result_identity_cannot_change(replacement):
    kwargs, _, _, _, _, new, _, _ = replacement
    package_execution._adopt_package_execution(execution_id="new", **kwargs)
    path = new / "run/result-ref.json"
    reference = json.loads(path.read_text())
    reference["result_id"] = "different-result"
    path.write_text(json.dumps(reference), encoding="utf-8")
    with pytest.raises(ValueError, match="Result 身份"):
        package_execution._adopt_package_execution(execution_id="new", **kwargs)


def test_process_exit_preserves_diagnostic_for_current_execution(tmp_path, monkeypatch):
    root = tmp_path / "candidate"
    root.mkdir()
    execution = root / "workspace/.research/executions/old"
    (root / "execution-ref.json").write_text(json.dumps({"execution_id": "old", "execution_root": str(execution)}), encoding="utf-8")
    feedback = {"execution_ref": str(execution), "failed_stage": "run", "diagnostics": [
        {"error_code": "project_worker_resource_exceeded", "recommended_action": "readmit-new-run"}]}
    (root / "execution-diagnostic.json").write_text(json.dumps(feedback), encoding="utf-8")
    monkeypatch.setattr(package_execution.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=3, stderr="interrupted"))
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution.execute_package(**arguments(root))
    assert captured.value.package_execution_feedback == feedback


def test_process_exit_does_not_reuse_diagnostic_from_previous_execution(tmp_path, monkeypatch):
    root = tmp_path / "candidate"
    execution = root / "workspace/.research/executions/new"
    (execution / "run").mkdir(parents=True)
    (execution / "run/operator-dag-invocation.json").write_text("{}", encoding="utf-8")
    (root / "execution-ref.json").write_text(json.dumps({"execution_id": "new", "execution_root": str(execution)}), encoding="utf-8")
    (root / "execution-diagnostic.json").write_text(json.dumps({"execution_ref": "old", "failed_stage": "verify"}), encoding="utf-8")
    monkeypatch.setattr(package_execution.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=3, stderr="interrupted"))
    with pytest.raises(package_execution.PackageExecutionInterrupted) as captured:
        package_execution.execute_package(**arguments(root))
    assert captured.value.package_execution_feedback["execution_ref"] == str(execution)
    assert captured.value.package_execution_feedback["failed_stage"] == "run"


@pytest.mark.parametrize("field,new_value", [("resource_governance", {"process_slots": 4}), ("reuse_run_roots", ["other-run"])])
def test_replacement_resource_governance_and_reuse_source_must_match(replacement, field, new_value):
    kwargs, _, _, _, _, new, _, _ = replacement
    path = new / "run/operator-dag-invocation.json"
    invocation = json.loads(path.read_text())
    invocation[field] = new_value
    path.write_text(json.dumps(invocation), encoding="utf-8")
    with pytest.raises(ValueError, match="冻结来源或运行资源"):
        package_execution._adopt_package_execution(execution_id="new", **kwargs)
