"""复用现有服务完成工作区研究，不持有独立运行状态。"""

from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
import sys

from research_pipeline.packages import load_research_package, read_package_documents
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
from . import result_delivery, research_package, research_plan, research_run, runtime_lifecycle


_RUNTIME_OPTIONS = (
    "mode", "workers", "resource_state_dir", "resource_memory_bytes",
    "resource_cpu_slots", "resource_scratch_bytes", "resource_process_slots",
    "resource_timeout_seconds", "resource_stale_seconds", "source_db",
    "input_snapshot_manifest", "minute_data_root", "reuse_run_root", "require_reused_node", "reuse_failed_run_root", "persist_inputs",
)


def workspace_arguments(**values) -> Namespace:
    """为程序调用提供与正式入口相同的可选参数默认值。"""
    defaults = {name: None for name in _RUNTIME_OPTIONS}
    defaults.update(
        mode="deterministic_serial", workers=None, resource_stale_seconds=30.0,
        source_db=[], reuse_run_root=[], require_reused_node=[],
        clock=None, root_seed=None, label=None, data_db=None, execution_id=None,
        extension_bundle=[], source_archive_root=None, persist_inputs=False,
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
        if name == "persist_inputs":
            if not isinstance(value, bool):
                raise ValueError("persist_inputs必须为布尔值")
        elif name in integers:
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
    stages = {name: "not_run" for name in ("prepare", "allocate", "admit", "run", "report")}
    outcome: dict[str, object] = {
        "stages": stages,
        "execution_status": None,
    }
    stage = "prepare"
    execution_root = None
    try:
        config = load_workspace(args.workspace)
        package_root = str(config.package_root)
        if documents is None:
            documents = read_package_documents(config.package_root)
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
        admission, prepared = research_plan.prepare_package(_arguments(
            args, command="package", package_command="admit", package=package_root,
            output=str(execution_root / "plan" / "admitted"),
        ), package=package)
        resource_summary = research_package._declared_resource_summary(prepared, prepared.registry)
        resource_facts = {"checks": {"resources": resource_summary}}
        outcome["resource_advice"] = _resource_advice(resource_facts, args)
        outcome["resource_forecast"] = _resource_forecast(resource_facts, args)
        print("资源建议: " + json.dumps(outcome["resource_forecast"], ensure_ascii=False), file=sys.stderr)
        outcome["plan_directory"] = admission["output"]
        stages[stage] = "pass"
        _save_continuation(execution_root, outcome, args)
        stage = "run"

        def run_with_diagnostics(runtime_args):
            args.run_root = runtime_args.run_root
            return research_run._execute(runtime_args, prepared_plan=prepared)

        run = run_workspace_execution(
            config.root, execution_id=allocation["execution_id"],
            plan=admission["output"], data_db=args.data_db, clock=clock,
            root_seed=root_seed, handler=run_with_diagnostics,
            **{name: getattr(args, name, None) for name in _RUNTIME_OPTIONS},
        )
        _accept_run(outcome, run, execution_root)
        stages[stage] = "pass"
        _save_continuation(execution_root, outcome, args)
        stage = "report"
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
        if stage == "report":
            outcome.update(_continuation(outcome, args, execution_root))
            outcome["next_action"] = "保留本次 Result，检查报告诊断或输出路径；不要重新运行研究。"
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
    return advice


def _continuation(outcome, args, execution_root) -> dict[str, object]:
    """后续报告只消费本次已发布 Result。"""
    return command_suggestion(
        "python", "-m", "research_pipeline", "report",
        "--result", outcome["result_directory"],
        "--result-store", outcome["result_store"], "--json",
    )


def _save_continuation(execution_root, outcome, args):
    record_workspace_continuation(execution_root, {
        "execution_id": outcome["execution_id"],
        "arguments": vars(args),
        "outcome": outcome,
    })


def _accept_run(outcome, run, execution_root):
    if run.get("status") != "result_finalized":
        raise WorkspaceError("run 未返回已封存 Result，不能生成报告")
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
        frozen.workspace = args.workspace
    else:
        outcome = {
            "execution_id": args.execution_id, "execution_root": str(execution_root),
            "stages": {name: "not_run" for name in ("prepare", "allocate", "admit", "run", "report")},
            "execution_status": None,
        }
        if initial_outcome is not None:
            outcome.update(initial_outcome)
        plan = execution_root / "plan" / "admitted"
        if plan.is_dir():
            outcome["plan_directory"] = str(plan)
        frozen = args
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
                if action == "report" and published.get("result_published") is True:
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
        stage = "report"
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
    stage = "report"
    try:
        if not (continuing and stages[stage] == "pass" and Path(outcome.get("report_path", "")).is_file()):
            report_path = execution_root / "report.md"
            suffix = 2
            while report_path.exists():
                report_path = execution_root / f"report-{suffix}.md"
                suffix += 1
            report = result_delivery._execute(_arguments(
                args, command="report", result=outcome["result_directory"],
                result_store=outcome["result_store"], output=str(report_path), format="markdown",
            ))
            outcome["report_path"] = report["output"]
            if "claim_level" in report:
                outcome["claim_level"] = report["claim_level"]
            stages[stage] = "pass"
        outcome["status"] = "succeeded"
        outcome["next_action"] = "阅读本次 report_path；比较研究时使用本次 result_directory 与 result_store。"
        _save_continuation(execution_root, outcome, args)
        return outcome
    except Exception as exc:
        outcome["failed_stage"] = stage
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
    startup = _resource_advice(lint, args)
    insufficient = shortages or any(item["status"] == "configuration_insufficient" for item in startup)
    return {
        "status": "configuration_insufficient" if insufficient else "advisory",
        "nodes": nodes, "capacity": limits, "shortages": shortages,
        "input_archive": archive,
        "startup_processes": startup,
        "peak_memory_bytes": {"status": "unknown", "bytes": None},
        "budgets_changed": False,
    }

