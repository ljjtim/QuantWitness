"""当前 operator DAG 的检查、恢复、节点重试和子运行命令。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from research_pipeline.runtime import (
    DagSpec,
    EventStore,
    ResourceGovernorConfig,
    ResourceVector,
    RuntimeIntegrityError,
)
from research_pipeline.runtime.liveness import (
    process_identity_alive,
    read_runtime_liveness,
    runtime_owner_alive,
)
from research_pipeline.runtime.resource_governor import read_resource_governor_state
from research_pipeline.runtime.diagnostics import (
    read_finalize_status,
    safe_error_summary,
)

from ..result import execute_guarded
from ..command_suggestion import render_powershell_command


INSPECTION_CONTRACT_VERSION = "research-operator-dag-inspection-v3"


class _SuggestedCommand(str):
    def __new__(cls, argv: tuple[str, ...]):
        value = str.__new__(cls, render_powershell_command(argv))
        value.argv = argv
        return value


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    root = Path(args.run_root).resolve()
    operator_record = root / "operator-dag-run.json"
    operator_invocation = root / "operator-dag-invocation.json"
    if args.command == "inspect":
        if operator_record.is_file():
            record = json.loads(operator_record.read_text(encoding="utf-8"))
            if not isinstance(record, Mapping) or not isinstance(
                record.get("dag"), Mapping
            ):
                raise RuntimeIntegrityError("operator DAG run record schema 无效")
            return _inspect_started_run(root, dict(record), operator_invocation)
        if operator_invocation.is_file():
            return _inspect_not_started_run(root)
        raise ValueError("run-root 不是当前 operator DAG 运行目录")

    if not (operator_record.is_file() or operator_invocation.is_file()):
        raise ValueError("run-root 缺少当前 operator DAG invocation")

    from .research_run import rerun_operator_graph, resume_operator_graph

    if args.command == "rerun-from":
        return rerun_operator_graph(
            parent_run_root=root,
            child_run_root=args.output_run_root,
            node_id=args.node,
        )
    return resume_operator_graph(
        run_root=root,
        retry_node_id=args.node if args.command == "retry-node" else None,
    )


def _inspect_not_started_run(root: Path) -> dict[str, object]:
    identity_issue = None
    try:
        from .research_run import _load_operator_invocation

        _load_operator_invocation(root)
    except Exception as exc:
        identity_issue = safe_error_summary(
            exc,
            default_error_code="runtime_recovery_identity_invalid",
        )
    if identity_issue is None:
        action = "resume"
        reason = "invocation 已复验，但 Runtime 尚未开始"
        next_command = _command(
            "python",
            "-m",
            "research_pipeline",
            "resume",
            "--run-root",
            str(root),
            "--json",
        )
    else:
        action, reason, next_command = _readmit_action(
            "当前 invocation 身份无效，不能开始或恢复 Runtime"
        )
    liveness = read_runtime_liveness(root)
    if runtime_owner_alive(liveness):
        action, reason, next_command = (
            "wait", "Runtime owner 仍存活，等待当前进程完成后再恢复",
            _command("python", "-m", "research_pipeline", "inspect", "--run-root", str(root), "--json"),
        )
    resource_pool = _resource_pool_projection(
        root / "operator-dag-invocation.json",
        run_id="",
    )
    return {
        "contract_version": INSPECTION_CONTRACT_VERSION,
        "run_id": None,
        "run_status": "not_started",
        "node_statuses": {},
        "attempt_statuses": {},
        "nodes": {},
        "runtime_phase": "not_started",
        "liveness": _liveness_projection(liveness),
        "resource_pool": resource_pool,
        "event_chain_head": None,
        "record_status": None,
        "finalize": read_finalize_status(root),
        "identity_issue": identity_issue,
        "recommended_action": action,
        "recommendation_reason": reason,
        "next_command": next_command,
        "next_command_argv": list(next_command.argv),
        "required_inputs": (
            _required_inputs(action, next_command)
        ),
    }


def _inspect_started_run(
    root: Path,
    record: dict[str, object],
    operator_invocation: Path,
) -> dict[str, object]:
    event_store = EventStore(root)
    events = event_store.read_events()
    projection = event_store.replay()
    if projection.run_id != record.get("run_id"):
        raise RuntimeIntegrityError("operator DAG record 与事件 run identity 不一致")
    if projection.run_status in {"paused", "succeeded", "failed", "cancelled"} and (
        projection.run_status != record.get("status")
    ):
        raise RuntimeIntegrityError("operator DAG record 与事件终态不一致")
    dag = DagSpec.from_dict(dict(record["dag"]))
    checkpoint_states: dict[str, dict[str, object]]
    identity_issue = None
    if operator_invocation.is_file():
        try:
            from .research_run import inspect_operator_graph

            inspected = inspect_operator_graph(root)
            inspected_dag = inspected.get("dag")
            raw_checkpoints = inspected.get("checkpoints")
            if inspected_dag != dag or not isinstance(raw_checkpoints, Mapping):
                raise RuntimeIntegrityError("当前 invocation 的 DAG 与 run record 不一致")
            checkpoint_states = {
                str(node_id): dict(state)
                for node_id, state in raw_checkpoints.items()
                if isinstance(state, Mapping)
            }
        except Exception as exc:
            identity_issue = safe_error_summary(
                exc,
                default_error_code="runtime_recovery_identity_invalid",
            )
            checkpoint_states = {
                node.node_id: {
                    "status": "unavailable",
                    "reusable": False,
                    "reason": identity_issue["message"],
                }
                for node in dag.nodes
            }
    else:
        identity_issue = {
            "error_code": "runtime_invocation_missing",
            "exception_type": "RuntimeIntegrityError",
            "message": "run-root 缺少当前 invocation，无法复验恢复身份",
        }
        checkpoint_states = {
            node.node_id: {
                "status": "unavailable",
                "reusable": False,
                "reason": identity_issue["message"],
            }
            for node in dag.nodes
        }

    diagnostics = {
        event.node_id: dict(event.payload)
        for event in events
        if event.kind == "diagnostic" and event.node_id is not None
    }
    liveness = read_runtime_liveness(root)
    if (
        liveness is not None and liveness.get("run_id") is not None
        and liveness.get("run_id") != projection.run_id
    ):
        raise RuntimeIntegrityError("Runtime 存活投影与事件 run identity 不一致")
    nodes = {}
    for node in dag.nodes:
        node_status = projection.node_statuses.get(node.node_id, "not_started")
        attempts_used = sum(
            event.kind == "attempt_status_changed"
            and event.node_id == node.node_id
            and event.payload.get("status") == "pending"
            for event in events
        )
        last_error = diagnostics.get(node.node_id)
        if last_error is not None:
            last_error = {
                "error_code": last_error.get("error_code"),
                "exception_type": last_error.get("exception_type"),
                "message": last_error.get("message"),
                **(
                    {"failure_context": last_error.get("failure_context")}
                    if last_error.get("failure_context") is not None
                    else {}
                ),
            }
        elif node_status == "waiting_for_resources":
            last_error = {
                "error_code": "runtime_resource_wait_interrupted",
                "exception_type": "RuntimeInterruption",
                "message": "节点最后一次 attempt 在等待资源时中断，可按当前 invocation 恢复",
            }
        elif node_status == "running":
            last_error = {
                "error_code": "runtime_attempt_interrupted",
                "exception_type": "RuntimeInterruption",
                "message": "节点最后一次 attempt 未形成终态，可按当前 invocation 恢复",
            }
        runtime_diagnostics = _node_runtime_diagnostics(
            node_id=node.node_id,
            node_status=node_status,
            dag=dag,
            node_statuses=projection.node_statuses,
            events=events,
            liveness=liveness,
        )
        nodes[node.node_id] = {
            "status": node_status,
            **runtime_diagnostics,
            "attempts_used": attempts_used,
            "max_attempts": node.retry_policy.max_attempts,
            "attempts_remaining": max(
                0, node.retry_policy.max_attempts - attempts_used
            ),
            "retryable_codes": list(node.retry_policy.retryable_codes),
            "last_error": last_error,
            "checkpoint": checkpoint_states.get(
                node.node_id,
                {
                    "status": "unavailable",
                    "reusable": False,
                    "reason": "checkpoint 复验没有返回节点结果",
                },
            ),
        }

    finalize = read_finalize_status(root)
    action, reason, next_command = _recommend_action(
        root=root,
        dag=dag,
        run_status=str(projection.run_status),
        nodes=nodes,
        finalize=finalize,
        identity_issue=identity_issue,
        result_store=(
            None
            if identity_issue is not None
            else _invocation_result_store(operator_invocation)
        ),
    )
    if runtime_owner_alive(liveness):
        action, reason, next_command = (
            "wait", "Runtime owner 仍存活，等待当前进程完成后再恢复",
            _command("python", "-m", "research_pipeline", "inspect", "--run-root", str(root), "--json"),
        )
    resource_pool = _resource_pool_projection(
        operator_invocation,
        run_id=str(projection.run_id),
    )
    runtime_phase = _runtime_phase(
        run_status=str(projection.run_status),
        finalize=finalize,
        nodes=nodes,
    )
    return {
        "contract_version": INSPECTION_CONTRACT_VERSION,
        "run_id": projection.run_id,
        "run_status": projection.run_status,
        "node_statuses": dict(sorted(projection.node_statuses.items())),
        "attempt_statuses": dict(sorted(projection.attempt_statuses.items())),
        "nodes": dict(sorted(nodes.items())),
        "runtime_phase": runtime_phase,
        "liveness": _liveness_projection(liveness),
        "resource_pool": resource_pool,
        "event_chain_head": projection.chain_head,
        "record_status": record.get("status"),
        "finalize": finalize,
        "identity_issue": identity_issue,
        "recommended_action": action,
        "recommendation_reason": reason,
        "next_command": next_command,
        "next_command_argv": list(next_command.argv),
        "required_inputs": (
            _required_inputs(action, next_command)
        ),
    }


def _recommend_action(
    *,
    root: Path,
    dag: DagSpec,
    run_status: str,
    nodes: Mapping[str, Mapping[str, object]],
    finalize: Mapping[str, object],
    identity_issue: Mapping[str, str] | None,
    result_store: str | None,
) -> tuple[str, str, str]:
    finalize_status = finalize.get("status")
    if run_status == "succeeded" and finalize_status == "succeeded":
        return _verify_action(
            root=root,
            finalize=finalize,
            result_store=result_store,
            reason="Runtime 与 Result finalize 均已成功",
        )
    if (
        run_status == "succeeded"
        and finalize_status == "failed"
        and finalize.get("result_published") is True
    ):
        return _verify_action(
            root=root,
            finalize=finalize,
            result_store=result_store,
            reason="Result 已原子发布；保留 finalize 后续错误并进入独立 verify",
        )
    if run_status == "succeeded" and finalize_status == "failed":
        return _readmit_action("Result finalize 已失败，需修正 Result/package 合同后新建运行")
    if identity_issue is not None or any(
        node.get("status") == "succeeded"
        and isinstance(node.get("checkpoint"), Mapping)
        and node["checkpoint"].get("status") != "reusable"
        for node in nodes.values()
    ):
        return _readmit_action("当前计划闭包或已成功 checkpoint 身份不可复用")
    if run_status in {"created", "planned", "running"}:
        return (
            "resume",
            "Runtime 尚未进入终态，可复用的 checkpoint 已通过复验",
            _command(
                "python", "-m", "research_pipeline", "resume",
                "--run-root", str(root), "--json",
            ),
        )
    if run_status == "paused":
        candidates = []
        for node_id, node in nodes.items():
            error = node.get("last_error")
            error_code = error.get("error_code") if isinstance(error, Mapping) else None
            if (
                node.get("status") == "retryable_failed"
                and isinstance(node.get("attempts_remaining"), int)
                and node["attempts_remaining"] > 0
                and error_code in node.get("retryable_codes", ())
            ):
                candidates.append(node_id)
        if len(candidates) == 1:
            node_id = candidates[0]
            return (
                "retry-node",
                f"节点 {node_id} 的错误码受 retry policy 允许且仍有尝试余额",
                _command(
                    "python", "-m", "research_pipeline", "retry-node",
                    "--run-root", str(root), "--node", node_id, "--json",
                ),
            )
        return _readmit_action("暂停节点没有实际可执行的重试资格")
    if run_status == "failed":
        target = next(
            (
                node_id
                for node_id in dag.topological_order()
                if nodes[node_id].get("status") != "succeeded"
            ),
            dag.topological_order()[-1],
        )
        target_error = nodes[target].get("last_error")
        target_error_code = (
            target_error.get("error_code")
            if isinstance(target_error, Mapping)
            else None
        )
        if target_error_code not in nodes[target].get("retryable_codes", ()):
            failure_context = (
                target_error.get("failure_context")
                if isinstance(target_error, Mapping)
                else None
            )
            if isinstance(failure_context, Mapping):
                return _readmit_action(
                    "data-plane request "
                    f"{failure_context.get('request_id')} / "
                    f"{failure_context.get('object_name')} 执行失败；"
                    "请修对象合同、缩小查询范围或调整节点 execution "
                    "memory/temp 后重新 lint/admit/new run。"
                    "max_bytes 仅是最终输出上限"
                )
            return _readmit_action(
                f"终态节点 {target} 的错误不属于可重试故障，需修正 package/算子合同后新建运行"
            )
        return _rerun_action(root=root, node_id=target)
    if run_status == "cancelled":
        target = next(
            (
                node_id
                for node_id in dag.topological_order()
                if nodes[node_id].get("status") != "succeeded"
            ),
            dag.topological_order()[-1],
        )
        return _rerun_action(root=root, node_id=target)
    if run_status != "succeeded":
        return _readmit_action("Runtime 状态不支持当前恢复命令")

    if finalize_status in {"unknown", "pending"}:
        return (
            "resume",
            "Runtime 已成功，但 Result finalize 尚无可确认终态",
            _command(
                "python", "-m", "research_pipeline", "resume",
                "--run-root", str(root), "--json",
            ),
        )
    return _readmit_action("Result finalize 状态不支持继续消费")


def _rerun_action(*, root: Path, node_id: str) -> tuple[str, str, str]:
    child_root = _available_output_path(root.with_name(f"{root.name}-rerun"))
    return (
        "rerun-from",
        f"Runtime 已是终态，需从节点 {node_id} 创建独立 child run",
        _command(
            "python", "-m", "research_pipeline", "rerun-from",
            "--run-root", str(root), "--output-run-root", str(child_root),
            "--node", node_id, "--json",
        ),
    )


def _verify_action(
    *,
    root: Path,
    finalize: Mapping[str, object],
    result_store: str | None,
    reason: str,
) -> tuple[str, str, str]:
    result_directory = str(finalize["result_directory"])
    resolved_result = Path(result_directory).resolve()
    try:
        inferred_store = resolved_result.parents[2]
    except IndexError as exc:
        raise RuntimeIntegrityError("Result 路径无法推导 ResultStore") from exc
    store = result_store or str(inferred_store)
    verification_output = _available_output_path(
        root / "verification-result.json", suffix_with_counter=True
    )
    command_parts = [
        "python", "-m", "research_pipeline", "verify",
        "--result", result_directory, "--result-store", store,
        "--output", str(verification_output),
    ]
    legacy_bundle = _legacy_verifier_bundle(
        root=root,
        result_directory=result_directory,
        result_store=store,
    )
    if legacy_bundle is False:
        return (
            "verify",
            f"{reason}；历史 Result 仍需操作者提供冻结身份一致的 Verifier bundle",
            _command("python", "-m", "research_pipeline", "verify", "--help"),
        )
    if isinstance(legacy_bundle, Path):
        command_parts.extend(("--verifier-bundle", str(legacy_bundle)))
    command_parts.append("--json")
    return (
        "verify",
        reason,
        _command(*command_parts),
    )


def _readmit_action(reason: str) -> tuple[str, str, str]:
    return (
        "readmit-new-run",
        f"{reason}；需人工修改原 ResearchPackage 后重新 lint/admit 并新建 run",
        _command(
            "python", "-m", "research_pipeline", "package", "lint",
            "--help",
        ),
    )


def _legacy_verifier_bundle(
    *,
    root: Path,
    result_directory: str,
    result_store: str,
) -> Path | bool | None:
    """返回历史 Result 的精确 Plan Verifier；False 表示确实需要外部选择。"""

    from research_pipeline.results import ResultStore

    if not Path(result_store).is_dir() or not Path(result_directory).is_dir():
        return None
    bundle = ResultStore(result_store, create=False).inspect_directory(
        result_directory
    )
    verification = bundle.verification
    if verification.verifier_identity is None:
        return None
    if verification.verifier_bundle_path is not None:
        return None
    invocation_path = root / "operator-dag-invocation.json"
    try:
        invocation = json.loads(invocation_path.read_text(encoding="utf-8"))
        plan = invocation.get("plan") if isinstance(invocation, Mapping) else None
        if not isinstance(plan, str) or not plan:
            return False
        from ..research_plan_store import resolve_plan_verifier_bundle

        return resolve_plan_verifier_bundle(
            plan,
            verification.verifier_identity,
        ) or False
    except Exception:
        return False


def _required_inputs(
    action: str,
    next_command: _SuggestedCommand,
) -> list[str]:
    if action == "readmit-new-run":
        return ["package_changes", "admission_inputs"]
    if action == "verify" and next_command.argv[-1:] == ("--help",):
        return ["verifier_bundle"]
    return []


def _invocation_result_store(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    value = payload.get("result_store") if isinstance(payload, Mapping) else None
    return value if isinstance(value, str) else None


def _available_output_path(path: Path, *, suffix_with_counter: bool = False) -> Path:
    if not path.exists():
        return path
    for index in range(2, 10_000):
        candidate = (
            path.with_name(f"{path.stem}-{index}{path.suffix}")
            if suffix_with_counter
            else path.with_name(f"{path.name}-{index}")
        )
        if not candidate.exists():
            return candidate
    raise RuntimeIntegrityError("无法为建议命令选择未占用输出路径")


def _command(*parts: str) -> _SuggestedCommand:
    return _SuggestedCommand(tuple(parts))


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _node_runtime_diagnostics(
    *,
    node_id: str,
    node_status: str,
    dag: DagSpec,
    node_statuses: Mapping[str, str],
    events,
    liveness: Mapping[str, object] | None,
) -> dict[str, object]:
    node_events = [event for event in events if event.node_id == node_id]
    if node_status == "not_started":
        dependencies = {
            edge.source_node for edge in dag.edges if edge.target_node == node_id
        }
        phase = (
            "ready"
            if all(node_statuses.get(item) == "succeeded" for item in dependencies)
            else "waiting_for_dependencies"
        )
        phase_event = None
    elif node_status == "waiting_for_resources":
        phase = "waiting_for_resources"
        phase_event = next(
            (
                event for event in reversed(node_events)
                if event.kind == "node_status_changed"
                and event.payload.get("status") == "waiting_for_resources"
            ),
            None,
        )
    elif node_status == "running":
        last_running_index = max(
            (
                index for index, event in enumerate(node_events)
                if event.kind == "node_status_changed"
                and event.payload.get("status") == "running"
            ),
            default=-1,
        )
        tail = node_events[last_running_index + 1 :]
        completed = next(
            (event for event in reversed(tail) if event.kind == "execution_completed"),
            None,
        )
        phase = "checkpointing" if completed is not None else "executing"
        phase_event = completed or (
            node_events[last_running_index] if last_running_index >= 0 else None
        )
    else:
        phase = node_status
        phase_event = node_events[-1] if node_events else None
    started_event = next(
        (
            event for event in node_events
            if event.kind == "attempt_status_changed"
            and event.payload.get("status") == "pending"
        ),
        None,
    )
    now = datetime.now(timezone.utc)
    started_at = None if started_event is None else started_event.occurred_at
    phase_started_at = None if phase_event is None else phase_event.occurred_at
    terminal_time = (
        _parse_time(node_events[-1].occurred_at)
        if node_events and node_status not in {"waiting_for_resources", "running"}
        else now
    )
    start_time = _parse_time(started_at)
    duration = (
        None
        if start_time is None or terminal_time is None
        else max(0.0, round((terminal_time - start_time).total_seconds(), 3))
    )
    node_liveness = None
    if liveness is not None and liveness.get("node_id") == node_id:
        node_liveness = _liveness_projection(liveness)
        if node_liveness is not None and phase in {
            "waiting_for_resources", "executing", "checkpointing"
        }:
            phase_started_at = str(liveness["phase_started_at"])
    return {
        "phase": phase,
        "started_at": started_at,
        "phase_started_at": phase_started_at,
        "duration_seconds": duration,
        "liveness": node_liveness,
    }


def _liveness_projection(
    liveness: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if liveness is None:
        return None
    heartbeat = _parse_time(liveness.get("heartbeat_at"))
    age = (
        None
        if heartbeat is None
        else max(
            0.0,
            round((datetime.now(timezone.utc) - heartbeat).total_seconds(), 3),
        )
    )
    alive = process_identity_alive(
        int(liveness["pid"]),
        float(liveness["process_started_at"]),
    )
    healthy = alive and age is not None and age <= 10.0
    return {
        **dict(liveness),
        "heartbeat_age_seconds": age,
        "process_alive": alive,
        "health": "healthy" if healthy else "stale",
    }


def _runtime_phase(
    *,
    run_status: str,
    finalize: Mapping[str, object],
    nodes: Mapping[str, Mapping[str, object]],
) -> str:
    if run_status == "succeeded" and finalize.get("status") in {"unknown", "pending"}:
        return "finalizing"
    active = next(
        (
            str(node["phase"])
            for node in nodes.values()
            if node.get("phase") in {
                "waiting_for_resources", "executing", "checkpointing"
            }
        ),
        None,
    )
    return active or run_status


def _resource_pool_projection(
    invocation_path: Path,
    *,
    run_id: str,
) -> dict[str, object] | None:
    if not invocation_path.is_file():
        return None
    try:
        invocation = json.loads(invocation_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeIntegrityError("正式 invocation 无法读取资源治理身份") from exc
    governance = invocation.get("resource_governance")
    capacity = invocation.get("resource_capacity")
    if governance is None:
        return None
    if not isinstance(governance, Mapping) or not isinstance(capacity, Mapping):
        raise RuntimeIntegrityError("正式 invocation 资源治理身份无效")
    config = ResourceGovernorConfig(
        state_dir=Path(str(governance["state_dir"])),
        capacity=ResourceVector(
            memory_bytes=int(capacity["memory_bytes"]),
            cpu_slots=int(capacity["cpu_slots"]),
            scratch_bytes=int(capacity["temp_bytes"]),
            process_slots=int(governance["process_slots"]),
        ),
        stale_after_seconds=float(governance["stale_seconds"]),
    )
    state = read_resource_governor_state(config)
    if state is None:
        return None
    now_epoch = datetime.now(timezone.utc).timestamp()

    def enrich(item: Mapping[str, object], *, request: bool) -> dict[str, object]:
        owner = str(item.get("owner_id", ""))
        parts = owner.split("/", 3)
        alive = process_identity_alive(
            int(item["pid"]),
            float(item["process_started_at"]),
        )
        result = {
            **dict(item),
            "owner": {
                "project_id": parts[0] if len(parts) > 0 else None,
                "run_id": parts[1] if len(parts) > 1 else None,
                "node_id": parts[2] if len(parts) > 2 else None,
                "attempt_id": parts[3] if len(parts) > 3 else None,
            },
            "current_run": len(parts) > 1 and parts[1] == run_id,
            "process_alive": alive,
        }
        if request:
            created = _parse_time(item.get("created_at"))
            result["queue_age_seconds"] = (
                None
                if created is None
                else max(0.0, round((datetime.now(timezone.utc) - created).total_seconds(), 3))
            )
            result["health"] = "waiting" if alive else "stale"
        else:
            heartbeat_age = max(
                0.0,
                round(now_epoch - float(item.get("heartbeat_epoch", 0.0)), 3),
            )
            result["heartbeat_age_seconds"] = heartbeat_age
            result["health"] = (
                "healthy"
                if alive and heartbeat_age <= config.stale_after_seconds
                else "stale"
            )
        return result

    return {
        "contract_version": state["contract_version"],
        "capacity": state["capacity"],
        "requests": [enrich(item, request=True) for item in state["requests"]],
        "leases": [
            enrich(item, request=False)
            for item in state["leases"]
            if item.get("status") == "active"
        ],
    }


__all__ = ["execute"]
