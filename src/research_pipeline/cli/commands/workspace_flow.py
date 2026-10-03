"""复用现有服务完成工作区研究，不持有独立运行状态。"""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import sys

from research_pipeline.packages import load_research_package
from research_pipeline.platform import MainlineError
from research_pipeline.workspace import (
    WorkspaceError,
    allocate_execution,
    load_workspace,
    run_workspace_execution,
)

from ..command_suggestion import command_suggestion
from . import evidence_lifecycle, research_package, research_run


class WorkspaceVerificationFailedError(MainlineError):
    """完整流程已生成验证报告，但研究未通过独立验证。"""

    error_code = "workspace_verification_failed"


_RUNTIME_OPTIONS = (
    "mode", "workers", "resource_state_dir", "resource_memory_bytes",
    "resource_cpu_slots", "resource_scratch_bytes", "resource_process_slots",
    "resource_timeout_seconds", "resource_stale_seconds", "source_db",
    "input_snapshot_manifest", "minute_data_root", "reuse_run_root", "require_reused_node", "reuse_failed_run_root",
)


def _arguments(args, **updates) -> Namespace:
    return Namespace(**{**vars(args), **updates})


def execute_workspace(args) -> dict[str, object]:
    """逐阶段委托主链服务，返回阶段事实及当前结果位置。"""
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
        lint = research_package._execute(_arguments(
            args, command="package", package_command="lint", package=package_root,
        ))
        stages[stage] = "pass"
        stage = "prepare"
        package = load_research_package(config.package_root)
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
        for advice in outcome["resource_advice"]:
            if advice["status"] == "configuration_insufficient":
                print(f"资源提示 [{advice.get('node_id', advice['purpose'])}]: {advice['message']} 参数: {advice['parameter']}", file=sys.stderr)
        stages[stage] = "pass"
        stage = "allocate"
        allocation = allocate_execution(
            config.root, clock=clock, root_seed=root_seed, label=args.label,
        )
        execution_root = Path(allocation["execution_path"])
        outcome.update(
            execution_id=allocation["execution_id"],
            execution_root=str(execution_root),
        )
        stages[stage] = "pass"
        stage = "admit"
        admission = research_package._execute(_arguments(
            args, command="package", package_command="admit", package=package_root,
            output=str(execution_root / "plan" / "admitted"),
        ))
        outcome["plan_directory"] = admission["output"]
        stages[stage] = "pass"
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
        if run.get("status") != "result_finalized":
            raise WorkspaceError("run 未返回已封存 Result，不能进入独立验证")
        outcome.update({name: run[name] for name in ("result_id", "result_directory")})
        outcome["execution_status"] = "succeeded"
        outcome["result_store"] = str(execution_root / "results")
        stages[stage] = "pass"
        stage = "verify"
        scratch = args.verification_scratch_root
        if scratch is None:
            scratch_root = execution_root / "verification" / "scratch"
            scratch_root.mkdir()
            scratch = str(scratch_root)
        verification = evidence_lifecycle._execute(_arguments(
            args, command="verify", result=run["result_directory"],
            result_store=outcome["result_store"],
            output=str(execution_root / "verification" / "result.json"),
            verification_scratch_root=scratch,
        ))
        verdict = verification["status"]
        if verdict not in {"pass", "fail"}:
            raise WorkspaceError("verify 未返回明确的独立验证结论")
        outcome["verification_status"] = verdict
        outcome["verification_result"] = verification["output"]
        outcome["claim_level"] = verification.get("claim_level")
        stages[stage] = verdict
        stage = "report"
        report = evidence_lifecycle._execute(_arguments(
            args, command="report", verification_result=verification["output"],
            result_store=outcome["result_store"], output=str(execution_root / "report.md"),
            format="markdown",
        ))
        outcome["report_path"] = report["output"]
        stages[stage] = "pass"
        if verdict != "pass":
            stage = "verify"
            raise WorkspaceVerificationFailedError("研究未通过独立验证，请阅读本次验证报告")
        outcome["status"] = "verified"
        outcome["next_action"] = "阅读本次 report_path；比较研究时使用本次 verification_result 与 result_store。"
        return outcome
    except Exception as exc:
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
