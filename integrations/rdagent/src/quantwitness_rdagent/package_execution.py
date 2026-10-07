"""正式研究包执行：共用Workspace分配、恢复、独立验证与报告。"""
import contextlib
import json
import os
import subprocess
import sys
import shutil
from pathlib import Path

from .contracts import write_json


class PackageExecutionInterrupted(RuntimeError):
    """保留已有运行位置，由Runtime诊断决定后续恢复动作。"""


_BINDING_FIELDS = ("catalog_lock", "extension_bundles", "runtime_options",
                   "verification_process_slots", "verification_memory_bytes")
_PROCESS_ENV = frozenset({
    "PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "HOME", "USERPROFILE",
    "HOMEDRIVE", "HOMEPATH", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE",
    "VIRTUAL_ENV", "CONDA_PREFIX", "LD_LIBRARY_PATH", "OMP_NUM_THREADS",
    "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS",
    "MPLCONFIGDIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME",
})


def _execution_arguments(arguments):
    values = dict(arguments)
    values["binding"] = {key: arguments["binding"][key] for key in _BINDING_FIELDS
                         if key in arguments["binding"]}
    return json.loads(json.dumps(values, default=str, allow_nan=False))


def _isolated_operation(operation, arguments):
    """固定子进程仅接收正式执行声明，模型环境与凭据留在研究进程。"""
    environment = {key: value for key, value in os.environ.items() if key.upper() in _PROCESS_ENV}
    environment["PYTHONPATH"] = os.pathsep.join(str(Path(path).resolve()) for path in sys.path
                                              if path and Path(path).is_dir())
    environment["PYTHONUTF8"] = "1"
    completed = subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-m", "quantwitness_rdagent.package_execution"],
        input=json.dumps({"operation": operation, "arguments": _execution_arguments(arguments)},
                         ensure_ascii=False, allow_nan=False),
        text=True, encoding="utf-8", capture_output=True, check=False, env=environment,
    )
    if completed.returncode != 0:
        raise _process_interruption(arguments, f"正式执行进程中断，退出码 {completed.returncode}: {completed.stderr[-2000:]}")
    try:
        response = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise _process_interruption(arguments, "正式执行进程未返回完整反馈") from exc
    if "feedback" in response:
        return response["feedback"]
    failure = response["failure"]
    error_types = {"ValueError": ValueError, "ArithmeticError": ArithmeticError,
                   "TypeError": TypeError, "FileNotFoundError": FileNotFoundError}
    error_type = PackageExecutionInterrupted if failure["interrupted"] else error_types.get(failure["error_type"], error_types.get(failure.get("category"), RuntimeError))
    error = error_type(failure["message"])
    if failure.get("package_execution_feedback") is not None:
        error.package_execution_feedback = failure["package_execution_feedback"]
    raise error



def _process_interruption(arguments, message):
    root = Path(arguments["root"])
    reference = root / "execution-ref.json"
    execution = json.loads(reference.read_text(encoding="utf-8")).get("execution_root") if reference.exists() else None
    diagnostic = root / "execution-diagnostic.json"
    feedback = json.loads(diagnostic.read_text(encoding="utf-8")) if diagnostic.exists() else None
    if feedback is None or feedback.get("execution_ref") != execution:
        stage = "run" if execution and (Path(execution) / "run/operator-dag-invocation.json").exists() else "prepare"
        feedback = {"command_status": "failed", "execution_status": "not_run", "verification_status": "not_run",
                    "execution_ref": execution, "result_ref": None, "verification_ref": None, "failed_stage": stage,
                    "diagnostics": [{"stage": stage, "error_code": "package_execution_process_interrupted", "message": message}]}
    error = PackageExecutionInterrupted(message)
    error.package_execution_feedback = feedback
    return error


def execute_package(*, root, workspace_id, allocation_label, base_package,
                    source_archive_root, input_snapshot_manifest, verifier_bundle, binding):
    """在独立轻量进程内运行正式 Workspace，并返回既有评价反馈。"""
    return _isolated_operation("execute", locals())


def adopt_package_execution(*, execution_id, root, workspace_id, allocation_label, base_package,
                            source_archive_root, input_snapshot_manifest, verifier_bundle, binding):
    """显式采用同候选已完成且通过独立验证的替代 Workspace execution。"""
    return _isolated_operation("adopt", locals())


def _execute_package(*, root, workspace_id, allocation_label, base_package,
                    source_archive_root, input_snapshot_manifest, verifier_bundle, binding):
    from research_pipeline.workspace import initialize_workspace, load_workspace
    from research_pipeline.workspace import run_workspace_execution, resume_workspace_execution
    from research_pipeline.packages import load_research_package
    from .worker import _command, find_or_allocate, runtime_options, failure_diagnostics
    root = Path(root)
    feedback = dict(command_status="failed", execution_status="not_run",
                    verification_status="not_run", result_ref=None, verification_ref=None)
    stage = "prepare"
    try:
        workspace = root / "workspace"
        if not workspace.exists():
            initialize_workspace(workspace, workspace_id=workspace_id,
                                 from_package=base_package)
        config = load_workspace(workspace)
        package = load_research_package(config.package_root)
        frozen = package.spec_payload
        extensions = []
        for bundle in binding["extension_bundles"]:
            extensions.extend(["--extension-bundle", bundle])
        common = ["--package", str(config.package_root), "--catalog-lock", binding["catalog_lock"],
                  "--source-archive-root", source_archive_root,
                  "--verifier-bundle", verifier_bundle, *extensions]
        stage = "lint"
        lint = _command(["package", "lint", *common, "--json"])
        adoption = _read_adoption(root)
        if adoption is not None:
            _validate_adopted_execution(adoption, locals())
        allocation = find_or_allocate(workspace, allocation_label,
                                      clock=frozen["fixed_clock"], root_seed=frozen["root_seed"],
                                      **({"execution_id": adoption["execution_id"]} if adoption else {}))
        execution = Path(allocation["execution_path"])
        feedback["execution_ref"] = str(execution)
        write_json(root / "execution-ref.json", {"execution_id": allocation["execution_id"], "execution_root": str(execution)})
        stage = "admit"
        plan = execution / "plan" / "admitted"
        if not plan.exists():
            pending_plan = plan.with_name("." + plan.name + ".tmp")
            if pending_plan.exists():
                if not pending_plan.resolve().is_relative_to(execution.resolve()):
                    raise ValueError("未发布准入目录不属于当前 execution")
                retained = execution / "interrupted-admission"
                retained.mkdir(exist_ok=True)
                index = len(tuple(retained.iterdir()))
                pending_plan.rename(retained / f"attempt-{index:04d}")
            _command(["package", "admit", *common, "--input-snapshot-manifest", input_snapshot_manifest,
                      "--output", str(plan), "--json"])
        stage = "run"
        run_root = execution / "run"
        reference = run_root / "result-ref.json"
        if reference.is_file():
            from research_pipeline.results import ResultStore
            identity = json.loads(reference.read_text(encoding="utf-8"))
            store = ResultStore(execution / "results")
            bundle = store.load_by_identity(project_id=identity["project_id"], run_id=identity["run_id"], result_id=identity["result_id"])
            result = {"status": "result_finalized", "result_directory": str(store.result_directory(bundle))}
        elif (run_root / "operator-dag-invocation.json").exists():
            inspection = _command(["inspect", "--run-root", str(run_root), "--json"])
            action = inspection["recommended_action"]
            if action == "verify" and inspection["finalize"].get("result_published") is True:
                from research_pipeline.results import ResultStore
                store = ResultStore(execution / "results")
                published = Path(inspection["finalize"]["result_directory"]).resolve()
                if not published.is_relative_to((execution / "results").resolve()):
                    raise ValueError("已发布 Result 不属于当前 execution")
                snapshot = store.open_snapshot(published)
                result = {"status": "result_finalized", "result_directory": str(store.result_directory(snapshot.bundle))}
            else:
                if action not in {"resume", "retry-node"}:
                    raise ValueError(f"Runtime 要求 {action}: {inspection['recommendation_reason']}")
                retry = None
                if action == "retry-node":
                    argv = inspection["next_command_argv"]
                    retry = argv[argv.index("--node") + 1]
                result = resume_workspace_execution(workspace, execution_id=allocation["execution_id"], retry_node_id=retry)
        else:
            options = runtime_options(binding["runtime_options"], plan=plan, execution=execution,
                                      clock=frozen["fixed_clock"])
            result = run_workspace_execution(workspace, execution_id=allocation["execution_id"], plan=plan,
                data_db=None, clock=frozen["fixed_clock"], root_seed=frozen["root_seed"],
                input_snapshot_manifest=input_snapshot_manifest, **options)
        if result.get("status") != "result_finalized":
            raise ValueError("RP 尚未生成正式 Result")
        feedback.update(execution_status="succeeded", result_ref=result["result_directory"])
        stage = "verify"
        verification_path = execution / "verification" / "result.json"
        if not verification_path.exists():
            verification_options = []
            if "verification_memory_bytes" in binding:
                verification_options = ["--verification-memory-bytes", str(binding["verification_memory_bytes"])]
            _command(["verify", "--result", result["result_directory"], "--result-store", str(execution / "results"),
                      "--output", str(verification_path), "--verification-process-slots", str(binding.get("verification_process_slots", 3)),
                      *verification_options, "--json"])
        from research_pipeline.evidence import load_verified_result_context
        verified = load_verified_result_context(verification_path, result_store=execution / "results")
        verdict = verified.verification
        feedback.update(verification_status=verdict.status, verification_ref=str(verification_path),
                        claim_level=verdict.claim_level, diagnostics=list(verdict.limitations))
        stage = "report"
        report = execution / "report.md"
        if not report.exists():
            _command(["report", "--verification-result", str(verification_path), "--result-store", str(execution / "results"),
                      "--output", str(report), "--format", "markdown", "--json"])
        feedback["command_status"] = "succeeded"
        return feedback
    except Exception as exc:
        diagnostic = {**feedback, "failed_stage": stage,
            "diagnostics": failure_diagnostics(exc, stage=stage, execution=feedback.get("execution_ref"))}
        if stage in {"admit", "run", "verify", "report"}:
            interruption = PackageExecutionInterrupted("正式执行尚未完成，请按Runtime诊断恢复同一execution")
            interruption.package_execution_feedback = diagnostic
            write_json(root / "execution-diagnostic.json", diagnostic)
            raise interruption from exc
        exc.package_execution_feedback = diagnostic
        raise

_ARGUMENT_FIELDS = ("root", "workspace_id", "allocation_label", "base_package",
                    "source_archive_root", "input_snapshot_manifest", "verifier_bundle", "binding")


def _adoption_inputs(arguments):
    return _execution_arguments({key: arguments[key] for key in _ARGUMENT_FIELDS})


def _read_adoption(root):
    path = Path(root) / "execution-adoption.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _validate_execution(arguments, execution_id):
    from research_pipeline.workspace import load_workspace, inspect_workspace
    from research_pipeline.packages import load_research_package
    from research_pipeline.cli.research_plan_store import load_operator_graph_research_plan
    from research_pipeline.data_plane.archived_inputs import load_archived_input_manifest
    from research_pipeline.extensions import verify_project_operator_bundle, verify_project_verifier_bundle
    from .worker import _command

    root = Path(arguments["root"])
    config = load_workspace(root / "workspace")
    if config.workspace_id != arguments["workspace_id"]:
        raise ValueError("替代 execution 的 Workspace 与候选不一致")
    allocation = inspect_workspace(config.root, execution_id)
    execution = config.generated_root / "executions" / allocation["execution_id"]
    package = load_research_package(config.package_root)
    if load_research_package(arguments["base_package"]).package_hash != package.package_hash:
        raise ValueError("候选 Workspace 的冻结研究包发生变化")
    manifest, queries, _, _, _ = load_operator_graph_research_plan(target=execution / "plan/admitted")
    if manifest["package_hash"] != package.package_hash:
        raise ValueError("替代 execution 的研究包与候选不一致")
    for field, declaration in (("fixed_clock", "clock"), ("root_seed", "root_seed")):
        if manifest[field] != package.spec_payload[field] or allocation[declaration] != package.spec_payload[field]:
            raise ValueError("替代 execution 的时钟或种子与冻结候选不一致")
    if manifest.get("input_snapshot_manifest") != load_archived_input_manifest(arguments["input_snapshot_manifest"]):
        raise ValueError("替代 execution 的冻结输入与候选不一致")
    binding = arguments["binding"]
    extensions = [part for bundle in binding["extension_bundles"] for part in ("--extension-bundle", bundle)]
    lint = arguments.get("lint") or _command([
        "package", "lint", "--package", str(config.package_root), "--catalog-lock", binding["catalog_lock"],
        "--source-archive-root", arguments["source_archive_root"],
        "--verifier-bundle", arguments["verifier_bundle"], *extensions, "--json",
    ])
    catalog_hash = lint["checks"]["fields"]["catalog_hash"]
    if any(query.catalog_hash != catalog_hash for query in queries.values()):
        raise ValueError("替代 execution 的 Catalog 与冻结候选不一致")
    bundle_hashes = sorted(verify_project_operator_bundle(path).bundle_hash for path in binding["extension_bundles"])
    if bundle_hashes != sorted(manifest.get("project_admission", {}).get("bundle_hashes", [])):
        raise ValueError("替代 execution 的算子 bundle 与候选不一致")
    verifier = verify_project_verifier_bundle(arguments["verifier_bundle"])
    if verifier.bundle_hash != manifest.get("verifier_admission", {}).get("bundle_hash"):
        raise ValueError("替代 execution 的 Verifier bundle 与候选不一致")
    invocation = json.loads((execution / "run/operator-dag-invocation.json").read_text(encoding="utf-8"))
    paths = {"plan": execution / "plan/admitted", "run_root": execution / "run",
             "artifact_root": execution / "artifacts", "handoff_out": execution / "handoff.json",
             "result_store": execution / "results", "input_snapshot_manifest": arguments["input_snapshot_manifest"]}
    if any(Path(invocation[key]).resolve() != Path(value).resolve() for key, value in paths.items()):
        raise ValueError("替代 execution 的正式运行位置或来源与候选不一致")
    if invocation["clock"] != allocation["clock"] or invocation["root_seed"] != allocation["root_seed"]:
        raise ValueError("替代 execution 的 invocation 时钟或种子不一致")
    if invocation["data_db"] is not None or invocation["source_dbs"]:
        raise ValueError("替代 execution 必须使用冻结归档输入")
    return execution, manifest, invocation


def _validate_adopted_execution(adoption, arguments):
    if adoption["inputs"] != _adoption_inputs(arguments):
        raise ValueError("采用记录与当前冻结执行声明不一致")
    execution, manifest, _ = _validate_execution(arguments, adoption["execution_id"])
    if manifest["manifest_hash"] != adoption["plan_manifest_hash"]:
        raise ValueError("已采用 execution 的正式 Plan 发生变化")
    reference = json.loads((execution / "run/result-ref.json").read_text(encoding="utf-8"))
    if reference["result_id"] != adoption["result_id"]:
        raise ValueError("已采用 execution 的正式 Result 身份发生变化")
    return execution


def _adopt_package_execution(*, execution_id, **arguments):
    from research_pipeline.cli.research_plan_store import load_operator_graph_research_plan
    from research_pipeline.evidence import load_verified_result_context
    from research_pipeline.results import ResultStore

    root = Path(arguments["root"])
    adoption = _read_adoption(root)
    if adoption is not None and adoption["execution_id"] == execution_id:
        execution = _validate_adopted_execution(adoption, arguments)
        write_json(root / "execution-ref.json", {"execution_id": execution_id, "execution_root": str(execution)})
        return adoption
    previous = json.loads((root / "execution-ref.json").read_text(encoding="utf-8"))
    if previous["execution_id"] == execution_id:
        raise ValueError("替代 execution 必须不同于当前执行")
    execution, manifest, invocation = _validate_execution(arguments, execution_id)
    workspace = execution.parent
    previous_execution = workspace / previous["execution_id"]
    if Path(previous["execution_root"]).resolve() != previous_execution.resolve():
        raise ValueError("原 execution 引用不属于候选 Workspace")
    from research_pipeline.workspace import inspect_workspace
    old_allocation = inspect_workspace(root / "workspace", previous["execution_id"])
    if (old_allocation["clock"], old_allocation["root_seed"]) != (invocation["clock"], invocation["root_seed"]):
        raise ValueError("替代 execution 与原执行的时钟或种子不一致")
    old_manifest, _, _, _, _ = load_operator_graph_research_plan(target=previous_execution / "plan/admitted")
    if old_manifest != manifest:
        raise ValueError("替代 execution 与原执行的正式 Plan 不一致")
    old_invocation = json.loads((previous_execution / "run/operator-dag-invocation.json").read_text(encoding="utf-8"))
    for field in ("mode", "minute_data_root", "resource_capacity", "resource_governance",
                  "reuse_run_roots", "input_snapshot_manifest"):
        if old_invocation.get(field) != invocation.get(field):
            raise ValueError("替代 execution 与原执行的冻结来源或运行资源不一致")
    store = ResultStore(execution / "results", create=False)
    reference = json.loads((execution / "run/result-ref.json").read_text(encoding="utf-8"))
    bundle = store.load_by_identity(project_id=reference["project_id"], run_id=reference["run_id"], result_id=reference["result_id"])
    context = load_verified_result_context(execution / "verification/result.json", result_store=execution / "results")
    if context.verification.status != "pass" or context.snapshot.bundle.result_id != bundle.result_id:
        raise ValueError("替代 execution 尚未通过同一 Result 的独立验证")
    if bundle.package_hash != manifest["package_hash"] or bundle.plan_hash != manifest["package_plan_hash"]:
        raise ValueError("替代 execution 的 Result 与正式 Plan 不一致")
    retained = root / "execution-history" / previous["execution_id"]
    retained.mkdir(parents=True, exist_ok=True)
    for name in ("execution-ref.json", "execution-diagnostic.json", "execution-adoption.json"):
        source = root / name
        target = retained / name
        if source.exists() and not target.exists():
            shutil.copyfile(source, target)
    adoption = {"execution_id": execution_id, "previous_execution_id": previous["execution_id"],
                "inputs": _adoption_inputs(arguments), "plan_manifest_hash": manifest["manifest_hash"],
                "result_id": bundle.result_id, "verification_ref": str(execution / "verification/result.json")}
    write_json(root / "execution-adoption.json", adoption)
    write_json(root / "execution-ref.json", {"execution_id": execution_id, "execution_root": str(execution)})
    return adoption


def _process_main():
    from research_pipeline.runtime.store import _StoreLock, process_identity_alive
    from research_pipeline.runtime.errors import RuntimeStateError

    payload = json.load(sys.stdin)
    root = Path(payload["arguments"]["root"])
    lock_path = root / ".package-execution.lock"
    try:
        with contextlib.redirect_stdout(sys.stderr), _StoreLock(lock_path):
            operation = {"execute": _execute_package, "adopt": _adopt_package_execution}[payload["operation"]]
            feedback = operation(**payload["arguments"])
        response = {"feedback": feedback}
    except Exception as exc:
        if isinstance(exc, RuntimeStateError) and lock_path.exists():
            owner = json.loads(lock_path.read_text(encoding="utf-8"))
            if process_identity_alive(owner["pid"], owner["process_started_at"]):
                exc = PackageExecutionInterrupted("正式执行进程仍存活，请等待原进程退出后恢复")
                reference = root / "execution-ref.json"
                execution = json.loads(reference.read_text(encoding="utf-8")).get("execution_root") if reference.exists() else None
                exc.package_execution_feedback = {"command_status": "failed", "execution_status": "not_run",
                    "verification_status": "not_run", "execution_ref": execution, "failed_stage": "run",
                    "result_ref": None, "verification_ref": None,
                    "diagnostics": [{"stage": "run", "error_code": "package_execution_owner_live",
                                     "message": str(exc), "recommended_action": "wait"}]}
        response = {"failure": {"error_type": type(exc).__name__, "message": str(exc),
                    "interrupted": isinstance(exc, PackageExecutionInterrupted),
                    "category": "ValueError" if isinstance(exc, ValueError) else "ArithmeticError" if isinstance(exc, ArithmeticError) else "RuntimeError",
                    "package_execution_feedback": getattr(exc, "package_execution_feedback", None)}}
    print(json.dumps(response, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    _process_main()
