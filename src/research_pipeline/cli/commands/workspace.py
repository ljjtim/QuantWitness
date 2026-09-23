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
        )
        return {"workspace_id": config.workspace_id, "root": str(config.root), "status": "initialized"}
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
            minute_data_root=args.minute_data_root,
            acceptance_proof=args.acceptance_proof,
        )
    if args.workspace_command in {"resume", "retry-node"}:
        return resume_workspace_execution(
            args.workspace,
            execution_id=args.execution,
            retry_node_id=(args.node if args.workspace_command == "retry-node" else None),
        )
    if args.workspace_command == "dashboard":
        return export_dashboard_manifest(args.workspace)
    raise ValueError(f"不支持的 workspace 命令: {args.workspace_command}")


__all__ = ["execute"]
