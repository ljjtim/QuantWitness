"""正式研究包执行：共用Workspace分配、恢复、独立验证与报告。"""
import json
from pathlib import Path

from .contracts import write_json


class PackageExecutionInterrupted(RuntimeError):
    """保留已有运行位置，由Runtime诊断决定后续恢复动作。"""


def execute_package(*, root, workspace_id, allocation_label, base_package,
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
        _command(["package", "lint", *common, "--json"])
        allocation = find_or_allocate(workspace, allocation_label,
                                      clock=frozen["fixed_clock"], root_seed=frozen["root_seed"])
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
            _command(["verify", "--result", result["result_directory"], "--result-store", str(execution / "results"),
                      "--output", str(verification_path), "--verification-process-slots", str(binding.get("verification_process_slots", 3)), "--json"])
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
