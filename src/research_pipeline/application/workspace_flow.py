"""复用现有服务完成工作区研究，不持有独立运行状态。"""

from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
import sys

from research_pipeline.packages import load_research_package, read_package_documents
from research_pipeline.platform import MainlineError
from research_pipeline.workspace import (
    WorkspaceError,
    allocate_execution,
    load_workspace,
    run_workspace_execution,
    resume_workspace_execution,
    workspace_execution_context,
    record_workspace_continuation,
    record_workspace_result_completion,
)

from .command_suggestion import command_suggestion
from . import evidence_lifecycle, research_package, research_run, runtime_lifecycle


class WorkspaceVerificationFailedError(MainlineError):
    """完整流程已生成验证报告，但研究未通过独立验证。"""

    error_code = "workspace_verification_failed"


_RUNTIME_OPTIONS = (
    "mode", "workers", "resource_state_dir", "resource_memory_bytes",
    "resource_cpu_slots", "resource_scratch_bytes", "resource_process_slots",
    "resource_timeout_seconds", "resource_stale_seconds", "source_db",
    "input_snapshot_manifest", "minute_data_root", "reuse_run_root", "require_reused_node", "reuse_failed_run_root",
)


def workspace_arguments(**values) -> Namespace:
    """为程序调用提供与正式入口相同的可选参数默认值。"""
    defaults = {name: None for name in _RUNTIME_OPTIONS}
    defaults.update(
        mode="deterministic_serial", workers=None, resource_stale_seconds=30.0,
        source_db=[], reuse_run_root=[], require_reused_node=[],
        clock=None, root_seed=None, label=None, data_db=None, execution_id=None,
        verification_memory_bytes=None, verification_temp_bytes=None,
        verification_scratch_root=None, verification_process_slots=2,
        verifier_bundle=None, extension_bundle=[], source_archive_root=None,
    )
    return Namespace(**{**defaults, **values})


def normalize_runtime_options(mapping) -> dict[str, object]:
    """解析程序传入的运行选项；来源和输出位置由正式编排冻结。"""
    allowed = set(_RUNTIME_OPTIONS) - {"source_db", "input_snapshot_manifest"}
    if set(mapping) - allowed:
        raise ValueError("runtime_options 含非运行参数或覆盖冻结来源")
    defaults = vars(workspace_arguments())
    result = {name: defaults[name] for name in sorted(allowed)}
    integers = {"workers", "resource_memory_bytes", "resource_cpu_slots", "resource_scratch_bytes", "resource_process_slots"}
    floats = {"resource_timeout_seconds", "resource_stale_seconds"}
    repeated = {"reuse_run_root", "require_reused_node"}
    for name, value in mapping.items():
        if value is None:
            continue
        if name in integers:
            value = int(str(value))
        elif name in floats:
            value = float(str(value))
        elif name in repeated:
            value = [str(item) for item in (value if isinstance(value, list) else [value])]
        else:
            value = str(value)
        result[name] = value
    if result["mode"] not in {"deterministic_serial", "bounded_parallel", "partitioned_batch"}:
        raise ValueError("未知正式执行模式")
    if result["workers"] is not None and result["workers"] < 1:
        raise ValueError("workers 必须是正整数")
    return result


def _arguments(args, **updates) -> Namespace:
    return Namespace(**{**vars(args), **updates})


def execute_workspace(args, *, documents=None, allocation_handler=None) -> dict[str, object]:
    """逐阶段委托主链服务，返回阶段事实及当前结果位置。"""
    if getattr(args, "execution_id", None):
        return _continue_workspace(args)
    if args.verification_process_slots is None:
        args.verification_process_slots = 2
    stages = {name: "not_run" for name in ("lint", "prepare", "allocate", "admit", "run", "verify", "report")}
    outcome: dict[str, object] = {
        "stages": stages,
        "execution_status": None,
        "verification_status": None,
    }
    stage = "lint"
    execution_root = None
    try:
        config = load_workspace(args.workspace)
        package_root = str(config.package_root)
        if documents is None:
            documents = read_package_documents(config.package_root)
        lint = research_package._execute(_arguments(
            args, command="package", package_command="lint", package=package_root,
        ), documents=documents)
        stages[stage] = "pass"
        stage = "prepare"
        package = load_research_package(config.package_root, documents=documents)
        clock = package.spec_payload["fixed_clock"]
        root_seed = package.spec_payload["root_seed"]
        if args.clock is not None and args.clock != clock:
            raise WorkspaceError("execute 的 clock 必须与研究包 fixed_clock 一致")
        if args.root_seed is not None and args.root_seed != root_seed:
            raise WorkspaceError("execute 的 root_seed 必须与研究包一致")
        if getattr(args, "input_snapshot_manifest", None):
            from research_pipeline.data_plane.archived_inputs import load_archived_input_manifest
            from research_pipeline.data_plane import PathRolePolicy

            archived = load_archived_input_manifest(args.input_snapshot_manifest)
            sources = {f"archive_{key}_input": value["root"]
                       for key, value in archived["requests"].items()}
            PathRolePolicy().validate(
                {"workspace_generated_output": config.generated_root, **sources},
                read_only_roles=tuple(sources),
            )
        outcome["resource_advice"] = _resource_advice(lint, args)
        outcome["resource_forecast"] = _resource_forecast(lint, args)
        print("资源建议: " + json.dumps(outcome["resource_forecast"], ensure_ascii=False), file=sys.stderr)
        for advice in outcome["resource_advice"]:
            if advice["status"] == "configuration_insufficient":
                print(f"资源提示 [{advice.get('node_id', advice['purpose'])}]: {advice['message']} 参数: {advice['parameter']}", file=sys.stderr)
        stages[stage] = "pass"
        stage = "allocate"
        allocator = allocation_handler or allocate_execution
        allocation = allocator(config.root, clock=clock, root_seed=root_seed, label=args.label)
        execution_root = Path(allocation["execution_path"])
        outcome.update(
            execution_id=allocation["execution_id"],
            execution_root=str(execution_root),
        )
        stages[stage] = "pass"
        outcome["plan_directory"] = str(execution_root / "plan" / "admitted")
        if (execution_root / "plan" / "admitted").is_dir() or (execution_root / "run" / "operator-dag-invocation.json").is_file():
            return _continue_workspace(_arguments(args, execution_id=allocation["execution_id"]), initial_outcome=outcome)
        _save_continuation(execution_root, outcome, args)
        stage = "admit"
        pending_plan = execution_root / "plan" / ".admitted.tmp"
        if pending_plan.exists():
            retained = execution_root / "interrupted-admission"
            retained.mkdir(exist_ok=True)
            pending_plan.rename(retained / f"attempt-{len(tuple(retained.iterdir())):04d}")
        admission = research_package._execute(_arguments(
            args, command="package", package_command="admit", package=package_root,
            output=str(execution_root / "plan" / "admitted"),
        ), documents=documents)
        outcome["plan_directory"] = admission["output"]
        stages[stage] = "pass"
        _save_continuation(execution_root, outcome, args)
        stage = "run"

        def run_with_diagnostics(runtime_args):
            args.run_root = runtime_args.run_root
            return research_run._execute(runtime_args)

        run = run_workspace_execution(
            config.root, execution_id=allocation["execution_id"],
            plan=admission["output"], data_db=args.data_db, clock=clock,
            root_seed=root_seed, handler=run_with_diagnostics,
            **{name: getattr(args, name, None) for name in _RUNTIME_OPTIONS},
        )
        _accept_run(outcome, run, execution_root)
        stages[stage] = "pass"
        _save_continuation(execution_root, outcome, args)
        stage = "verify"
        return _finish_execution(args, outcome, execution_root)
    except Exception as exc:
        stage = outcome.get("failed_stage", stage)
        if stages[stage] != "pass":
            stages[stage] = "fail"
        outcome["failed_stage"] = stage
        if stage == "run":
            outcome["execution_status"] = "failed"
        payload = getattr(exc, "failure_payload", None)
        outcome.update(payload if isinstance(payload, dict) else {})
        if execution_root is not None:
            run_root = execution_root / "run"
            if stage == "run" and (run_root / "operator-dag-invocation.json").is_file():
                outcome.update(command_suggestion(
                    "python", "-m", "research_pipeline", "inspect",
                    "--run-root", str(run_root), "--json",
                ))
        if stage in {"verify", "report"}:
            outcome.update(_continuation(outcome, args, execution_root))
            outcome["next_action"] = "保留本次 Result，检查独立验证诊断或报告路径；不要重新运行研究。"
        elif "next_command_argv" not in outcome:
            outcome["next_action"] = "根据本次错误修正声明或显式参数，再执行 workspace execute。"
        exc.failure_payload = outcome
        raise


def _resource_advice(lint, args) -> list[dict[str, object]]:
    from research_pipeline.platform.process_budget_advice import process_budget_advice

    nodes = lint.get("checks", {}).get("resources", {}).get("nodes", [])
    advice = []
    for node in nodes:
        if node.get("implementation_scope") != "project":
            continue
        slots = node["resource_profile"]["process_slots"]
        item = process_budget_advice(slots, purpose="worker")
        advice.append({**item, "node_id": node["node_id"]})
    if args.verifier_bundle is not None:
        advice.append(process_budget_advice(args.verification_process_slots, purpose="verifier"))
    return advice


def _continuation(outcome, args, execution_root) -> dict[str, object]:
    """后续命令只消费本次 Result，已生成的验证结论直接交给报告。"""
    if outcome.get("verification_result") is not None:
        return command_suggestion(
            "python", "-m", "research_pipeline", "report",
            "--verification-result", outcome["verification_result"],
            "--result-store", outcome["result_store"], "--json",
        )
    output = execution_root / "verification" / "result.json"
    suffix = 2
    while output.exists():
        output = execution_root / "verification" / f"result-{suffix}.json"
        suffix += 1
    argv = [
        "python", "-m", "research_pipeline", "verify",
        "--result", outcome["result_directory"],
        "--result-store", outcome["result_store"], "--output", str(output),
        "--verification-process-slots", str(args.verification_process_slots),
        "--verification-scratch-root", str(args.verification_scratch_root or execution_root / "verification" / "scratch"),
    ]
    for field, option in (
        ("verifier_bundle", "--verifier-bundle"),
        ("verification_memory_bytes", "--verification-memory-bytes"),
        ("verification_temp_bytes", "--verification-temp-bytes"),
    ):
        value = getattr(args, field)
        if value is not None:
            argv.extend([option, str(value)])
    return command_suggestion(*argv, "--json")


def _save_continuation(execution_root, outcome, args):
    record_workspace_continuation(execution_root, {
        "execution_id": outcome["execution_id"],
        "arguments": vars(args),
        "outcome": outcome,
    })


def _accept_run(outcome, run, execution_root):
    if run.get("status") != "result_finalized":
        raise WorkspaceError("run 未返回已封存 Result，不能进入独立验证")
    if outcome.get("result_id") is not None and outcome["result_id"] != run["result_id"]:
        raise WorkspaceError("正式 Result 身份与原 execution 接续记录不一致")
    outcome.update({name: run[name] for name in ("result_id", "result_directory")})
    outcome["execution_status"] = "succeeded"
    outcome["result_store"] = str(execution_root / "results")


def _validate_result(outcome):
    from research_pipeline.results import ResultStore

    bundle = ResultStore(outcome["result_store"], create=False).inspect_directory(outcome["result_directory"])
    if bundle.result_id != outcome["result_id"]:
        raise WorkspaceError("接续 Result 身份与原执行不一致")


def _continue_workspace(args, *, initial_outcome=None):
    """只按显式 execution 和正式主链产物接续。"""
    context = workspace_execution_context(args.workspace, args.execution_id)
    execution_root = Path(context["execution_root"])
    pointer = execution_root / "continuation.json"
    if pointer.is_file():
        saved = json.loads(pointer.read_text(encoding="utf-8"))
        if saved.get("execution_id") != args.execution_id:
            raise WorkspaceError("接续记录 execution 身份不一致")
        outcome = saved["outcome"]
        outcome.pop("failed_stage", None)
        outcome.pop("status", None)
        outcome.pop("next_command", None)
        outcome.pop("next_command_argv", None)
        frozen = _arguments(args, **saved["arguments"])
        # 复用原预算；更换预算须显式给出当前调用中的覆盖值。
        for field in ("verifier_bundle", "verification_memory_bytes", "verification_temp_bytes", "verification_scratch_root"):
            value = getattr(args, field, None)
            if value is not None:
                setattr(frozen, field, value)
        if args.verification_process_slots is not None:
            frozen.verification_process_slots = args.verification_process_slots
        frozen.workspace = args.workspace
    else:
        outcome = {
            "execution_id": args.execution_id, "execution_root": str(execution_root),
            "stages": {name: "not_run" for name in ("lint", "prepare", "allocate", "admit", "run", "verify", "report")},
            "execution_status": None, "verification_status": None,
        }
        if initial_outcome is not None:
            outcome.update(initial_outcome)
        plan = execution_root / "plan" / "admitted"
        if plan.is_dir():
            outcome["plan_directory"] = str(plan)
        frozen = args
        if frozen.verification_process_slots is None:
            frozen.verification_process_slots = 2
    stage = "run"
    try:
        if outcome.get("resource_forecast"):
            print("资源建议: " + json.dumps(outcome["resource_forecast"], ensure_ascii=False), file=sys.stderr)
        # Result 发布与导航记录之间中断时，以正式 finalizer 记录恢复显式位置。
        from research_pipeline.runtime.diagnostics import read_finalize_status
        finalized = read_finalize_status(execution_root / "run")
        if finalized.get("result_published"):
            _accept_run(outcome, {**finalized, "status": "result_finalized"}, execution_root)
        if not outcome.get("result_directory"):
            reference = execution_root / "run" / "result-ref.json"
            if reference.is_file():
                from research_pipeline.results import ResultStore
                identity = json.loads(reference.read_text(encoding="utf-8"))
                store = ResultStore(execution_root / "results", create=False)
                bundle = store.load_by_identity(project_id=identity["project_id"], run_id=identity["run_id"], result_id=identity["result_id"])
                _accept_run(outcome, {"status": "result_finalized", "result_id": bundle.result_id,
                    "result_directory": str(store.result_directory(bundle))}, execution_root)
        if outcome.get("result_directory"):
            _validate_result(outcome)
            record_workspace_result_completion(
                args.workspace, execution_id=args.execution_id,
                result_directory=outcome["result_directory"], result_id=outcome["result_id"],
            )
        else:
            invocation = execution_root / "run" / "operator-dag-invocation.json"
            if invocation.is_file():
                args.run_root = str(execution_root / "run")
                inspection = runtime_lifecycle._execute(_arguments(args, command="inspect", run_root=str(execution_root / "run")))
                action = inspection["recommended_action"]
                published = inspection.get("finalize", {})
                if action == "verify" and published.get("result_published") is True:
                    run = {**published, "status": "result_finalized"}
                    _accept_run(outcome, run, execution_root)
                    _validate_result(outcome)
                    record_workspace_result_completion(
                        args.workspace, execution_id=args.execution_id,
                        result_directory=outcome["result_directory"], result_id=outcome["result_id"],
                    )
                else:
                    if action not in {"resume", "retry-node"}:
                        error = WorkspaceError(f"Runtime 要求 {action}: {inspection['recommendation_reason']}")
                        error.failure_payload = inspection
                        raise error
                    retry_node = None
                    if action == "retry-node":
                        argv = inspection["next_command_argv"]
                        retry_node = argv[argv.index("--node") + 1]
                    run = resume_workspace_execution(args.workspace, execution_id=args.execution_id, retry_node_id=retry_node)
            elif outcome.get("plan_directory"):
                if (execution_root / "run").exists() and any((execution_root / "run").iterdir()):
                    raise WorkspaceError("运行目录非空但缺少正式 invocation；拒绝重新执行研究")
                from .research_plan_store import load_operator_graph_research_plan
                load_operator_graph_research_plan(target=outcome["plan_directory"])
                run = run_workspace_execution(
                    args.workspace, execution_id=args.execution_id, plan=outcome["plan_directory"],
                    data_db=frozen.data_db, clock=context["clock"], root_seed=context["root_seed"],
                    **{name: getattr(frozen, name, None) for name in _RUNTIME_OPTIONS},
                )
            else:
                raise WorkspaceError("execution 尚无正式 plan、invocation 或 Result；拒绝猜测运行位置")
            _accept_run(outcome, run, execution_root)
            _validate_result(outcome)
        outcome.pop("failed_stage", None)
        outcome["stages"]["run"] = "pass"
        _save_continuation(execution_root, outcome, frozen)
        stage = "verify"
        return _finish_execution(frozen, outcome, execution_root, continuing=True)
    except Exception as exc:
        stage = outcome.get("failed_stage", stage)
        outcome["failed_stage"] = stage
        outcome["stages"][stage] = "fail"
        outcome.update(getattr(exc, "failure_payload", {}) or {})
        outcome.update(command_suggestion(
            "python", "-m", "research_pipeline", "workspace", "execute",
            "--workspace", str(args.workspace), "--execution-id", args.execution_id,
            "--catalog-lock", str(args.catalog_lock), "--json",
        ))
        exc.failure_payload = outcome
        raise


def _finish_execution(args, outcome, execution_root, *, continuing=False):
    stages = outcome["stages"]
    stage = "verify"
    try:
        _show_verification_resources(outcome, args)
        verification_path = outcome.get("verification_result")
        pending = execution_root / "verification" / "result.json"
        if continuing and verification_path is None and pending.is_file():
            verification_path = str(pending)
        if continuing and verification_path is not None:
            from research_pipeline.evidence import load_verified_result_context
            context = load_verified_result_context(verification_path, result_store=outcome["result_store"])
            if context.snapshot.bundle.result_id != outcome["result_id"]:
                raise WorkspaceError("VerificationResult 未绑定本 execution 的 Result")
            verification = {
                "output": verification_path, "status": context.verification.status,
                "claim_level": context.verification.claim_level,
            }
        else:
            scratch = args.verification_scratch_root
            if scratch is None:
                scratch_root = execution_root / "verification" / "scratch"
                scratch_root.mkdir(exist_ok=True)
                scratch = str(scratch_root)
            verification = evidence_lifecycle._execute(_arguments(
                args, command="verify", result=outcome["result_directory"],
                result_store=outcome["result_store"], output=str(pending),
                verification_scratch_root=scratch,
            ))
        verdict = verification["status"]
        if verdict not in {"pass", "fail"}:
            raise WorkspaceError("verify 未返回明确的独立验证结论")
        outcome.update(verification_status=verdict, verification_result=verification["output"], claim_level=verification.get("claim_level"))
        stages[stage] = verdict
        _save_continuation(execution_root, outcome, args)
        stage = "report"
        if not (continuing and stages[stage] == "pass" and Path(outcome.get("report_path", "")).is_file()):
            report_path = execution_root / "report.md"
            suffix = 2
            while report_path.exists():
                report_path = execution_root / f"report-{suffix}.md"
                suffix += 1
            report = evidence_lifecycle._execute(_arguments(
                args, command="report", verification_result=verification["output"],
                result_store=outcome["result_store"], output=str(report_path), format="markdown",
            ))
            outcome["report_path"] = report["output"]
            stages[stage] = "pass"
            _save_continuation(execution_root, outcome, args)
        if verdict != "pass":
            stage = "verify"
            raise WorkspaceVerificationFailedError("研究未通过独立验证，请阅读本次验证报告")
        outcome["status"] = "verified"
        outcome["next_action"] = "阅读本次 report_path；比较研究时使用本次 verification_result 与 result_store。"
        _save_continuation(execution_root, outcome, args)
        return outcome
    except Exception as exc:
        outcome["failed_stage"] = stage
        if stages[stage] != "pass":
            stages[stage] = "fail"
        outcome.update(getattr(exc, "failure_payload", {}) or {})
        outcome.update(_continuation(outcome, args, execution_root))
        exc.failure_payload = outcome
        raise


def _resource_forecast(lint, args):
    """记录可观察事实和预算差额，不将声明上限冒充峰值估计。"""
    nodes = lint.get("checks", {}).get("resources", {}).get("nodes", [])
    capacity = research_run._runtime_resource_capacity(args)
    limits = {
        "memory_bytes": capacity.memory_bytes,
        "temp_bytes": capacity.temp_bytes,
        "cpu_slots": capacity.cpu_slots,
        "process_slots": getattr(args, "resource_process_slots", None),
    }
    shortages = []
    for node in nodes:
        for dimension, limit in limits.items():
            declared = node["resource_profile"].get(dimension)
            if limit is not None and declared is not None and declared > limit:
                shortages.append({"node_id": node["node_id"], "dimension": dimension, "declared": declared, "capacity": limit})
    archive = {"status": "unknown", "bytes": None, "reason": "未提供输入归档；数据库规模不作猜测"}
    if getattr(args, "input_snapshot_manifest", None):
        from research_pipeline.data_plane.archived_inputs import load_archived_input_manifest
        manifest = load_archived_input_manifest(args.input_snapshot_manifest)
        files = set()
        for entry in manifest["requests"].values():
            reference = entry["reference"]
            base = Path(entry["root"])
            if entry["kind"] == "dataset":
                files.update(base / reference["relative_path"] / part for part in reference["partitions"])
            else:
                files.update(base / part["relative_path"] for part in reference["partitions"])
        archive = {"status": "known", "bytes": sum(path.stat().st_size for path in files), "file_count": len(files), "basis": "manifest_referenced_files"}
    from research_pipeline.evidence import FinancialOracleBudget
    verification_defaults = FinancialOracleBudget()
    verification_capacity = {"process_slots": args.verification_process_slots}
    for dimension in ("memory_bytes", "temp_bytes"):
        value = getattr(args, "verification_" + dimension, None)
        verification_capacity[dimension] = getattr(verification_defaults, dimension) if value is None else value
    startup = _resource_advice(lint, args)
    insufficient = shortages or any(item["status"] == "configuration_insufficient" for item in startup)
    return {
        "status": "configuration_insufficient" if insufficient else "advisory",
        "nodes": nodes, "capacity": limits, "shortages": shortages,
        "input_archive": archive,
        "verification_copy_lower_bound": {
            "status": "unknown", "bytes": None,
            "reason": "Result 尚未发布；验证暂存还需重编码，输入归档尺寸不能替代暂存需求",
        },
        "startup_processes": startup,
        "verification_capacity": verification_capacity,
        "peak_memory_bytes": {"status": "unknown", "bytes": None},
        "budgets_changed": False,
    }


def _show_verification_resources(outcome, args):
    """区分确定复制字节和授权列重编码估计，不将原表体积当投影下界。"""
    from research_pipeline.results import ResultStore
    from research_pipeline.evidence import FinancialOracleBudget
    from research_pipeline.extensions import verify_project_verifier_bundle
    from research_pipeline.evidence.project_verifier_runtime import estimate_project_verifier_input_size

    bundle = ResultStore(outcome["result_store"], create=False).inspect_directory(outcome["result_directory"])
    identity = bundle.verification.verifier_identity
    lower_bound = {"status": "unknown", "bytes": None, "reason": "无项目验证授权复制清单；金融验证重编码大小未知"}
    estimate = {"status": "unknown", "bytes": None}
    forecast = outcome.setdefault("resource_forecast", {})
    if identity is not None:
        root = Path(outcome["result_directory"])
        tables = [table for table in bundle.tables if table.schema_id in identity["authorized_schema_ids"]]
        support_paths = {item.relative_path for item in bundle.support_files if item.artifact_type in identity["authorized_support_artifact_types"]}
        support_bytes = sum((root / path).stat().st_size for path in support_paths)
        source_bytes = sum((root / path).stat().st_size for table in tables for path in table.files) + support_bytes
        forecast["verification_authorized_source_bytes"] = source_bytes
        embedded = getattr(bundle.verification, "verifier_bundle_path", None)
        bundle_path = root / embedded if embedded is not None else getattr(args, "verifier_bundle", None)
        if bundle_path is None:
            lower_bound = {"status": "unknown", "bytes": None, "reason": "缺少冻结 Verifier bundle 的授权列声明，不能将完整源文件体积当复制下界"}
        else:
            manifest = verify_project_verifier_bundle(bundle_path)
            if manifest.identity() != dict(identity):
                raise WorkspaceError("资源建议的 Verifier bundle 与 Result 冻结身份不一致")
            projected = False
            known_bytes = support_bytes
            estimated_bytes = support_bytes
            for table in tables:
                columns = manifest.authorized_columns.get(table.schema_id)
                projected = projected or columns is not None
                for relative in table.files:
                    path = root / relative
                    item = estimate_project_verifier_input_size(path, columns=columns)
                    estimated_bytes += item["bytes"]
                    if columns is None:
                        known_bytes += path.stat().st_size
            lower_bound = {
                "status": "known", "bytes": known_bytes,
                "basis": "unprojected_authorized_files_and_support",
                "excludes_projected_tables": projected,
            }
            estimate = {
                "status": "estimated" if projected else "measured",
                "bytes": estimated_bytes,
                "basis": "project_verifier_input_size",
            }
    forecast["verification_copy_lower_bound"] = lower_bound
    forecast["verification_input_size"] = estimate
    limit = args.verification_temp_bytes
    if limit is None:
        limit = FinancialOracleBudget().temp_bytes
    forecast["verification_temp_bytes"] = limit
    if lower_bound["bytes"] is not None and lower_bound["bytes"] > limit:
        forecast["status"] = "configuration_insufficient"
    elif estimate["bytes"] is not None and estimate["bytes"] > limit and forecast.get("status") != "configuration_insufficient":
        forecast["status"] = "estimated_configuration_insufficient"
    forecast["budgets_changed"] = False
    print("验证资源建议: " + json.dumps(forecast, ensure_ascii=False), file=sys.stderr)
