"""个人 Research Workspace v1 命令。"""

from __future__ import annotations

from research_pipeline.workspace import (
    WorkspaceError,
    allocate_execution,
    export_dashboard_manifest,
    initialize_workspace,
    inspect_workspace,
    rebuild_workspace_index,
    resume_workspace_execution,
    run_workspace_execution,
    validate_workspace,
)

from ..result import execute_guarded


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    if args.workspace_command == "init":
        config = initialize_workspace(
            args.root,
            workspace_id=args.workspace_id,
            allow_existing=args.allow_existing,
            from_package=getattr(args, "from_package", None),
        )
        return {"workspace_id": config.workspace_id, "root": str(config.root), "status": "initialized"}
    if args.workspace_command == "execute":
        from .workspace_flow import execute_workspace

        return execute_workspace(args)
    if args.workspace_command == "validate":
        result = validate_workspace(args.workspace)
        if result["status"] == "fail":
            error = WorkspaceError("Git 索引含数据库、密钥或机器生成文件；请先取消跟踪或暂存")
            error.failure_payload = result
            raise error
        return result
    if args.workspace_command == "allocate":
        return allocate_execution(
            args.workspace,
            clock=args.clock,
            root_seed=args.root_seed,
            label=args.label,
        )
    if args.workspace_command == "inspect":
        return inspect_workspace(args.workspace, args.execution)
    if args.workspace_command == "rebuild-index":
        return rebuild_workspace_index(args.workspace)
    if args.workspace_command == "run":
        def run_with_diagnostics(runtime_args):
            from . import research_run

            # 服务已校验 execution；将实际运行目录交给统一异常投影。
            args.run_root = runtime_args.run_root
            return research_run._execute(runtime_args)

        source_dbs = list(getattr(args, "source_db", []))
        return run_workspace_execution(
            args.workspace,
            execution_id=args.execution,
            plan=args.plan,
            data_db=args.data_db,
            clock=args.clock,
            root_seed=args.root_seed,
            mode=args.mode,
            workers=args.workers,
            resource_state_dir=args.resource_state_dir,
            resource_memory_bytes=args.resource_memory_bytes,
            resource_cpu_slots=args.resource_cpu_slots,
            resource_scratch_bytes=args.resource_scratch_bytes,
            resource_process_slots=args.resource_process_slots,
            resource_timeout_seconds=args.resource_timeout_seconds,
            resource_stale_seconds=args.resource_stale_seconds,
            source_db=source_dbs,
            input_snapshot_manifest=getattr(args, "input_snapshot_manifest", None),
            minute_data_root=args.minute_data_root,
            reuse_run_root=args.reuse_run_root,
            require_reused_node=args.require_reused_node,
            reuse_failed_run_root=args.reuse_failed_run_root,
            handler=run_with_diagnostics,
        )
    if args.workspace_command in {"resume", "retry-node"}:
        def resume_with_diagnostics(*, run_root, retry_node_id):
            from . import research_run

            args.run_root = run_root
            return research_run.resume_operator_graph(
                run_root=run_root, retry_node_id=retry_node_id,
            )

        return resume_workspace_execution(
            args.workspace,
            execution_id=args.execution,
            retry_node_id=(args.node if args.workspace_command == "retry-node" else None),
            handler=resume_with_diagnostics,
        )
    if args.workspace_command == "dashboard":
        return export_dashboard_manifest(args.workspace)
    raise ValueError(f"不支持的 workspace 命令: {args.workspace_command}")


__all__ = ["execute"]
