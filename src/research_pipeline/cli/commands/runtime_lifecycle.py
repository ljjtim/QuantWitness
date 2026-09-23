"""当前 operator DAG 的检查、恢复、节点重试和子运行命令。"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Mapping

from research_pipeline.runtime import DagSpec, EventStore, RuntimeIntegrityError
from research_pipeline.runtime.diagnostics import (
    read_finalize_status,
    safe_error_summary,
)

from ..result import execute_guarded


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
    return {
        "contract_version": "research-operator-dag-inspection-v2",
        "run_id": None,
        "run_status": "not_started",
        "node_statuses": {},
        "attempt_statuses": {},
        "nodes": {},
        "event_chain_head": None,
        "record_status": None,
        "finalize": read_finalize_status(root),
        "identity_issue": identity_issue,
        "recommended_action": action,
        "recommendation_reason": reason,
        "next_command": next_command,
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
        elif node_status == "running":
            last_error = {
                "error_code": "runtime_attempt_interrupted",
                "exception_type": "RuntimeInterruption",
                "message": "节点最后一次 attempt 未形成终态，可按当前 invocation 恢复",
            }
        nodes[node.node_id] = {
            "status": node_status,
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
    return {
        "contract_version": "research-operator-dag-inspection-v2",
        "run_id": projection.run_id,
        "run_status": projection.run_status,
        "node_statuses": dict(sorted(projection.node_statuses.items())),
        "attempt_statuses": dict(sorted(projection.attempt_statuses.items())),
        "nodes": dict(sorted(nodes.items())),
        "event_chain_head": projection.chain_head,
        "record_status": record.get("status"),
        "finalize": finalize,
        "identity_issue": identity_issue,
        "recommended_action": action,
        "recommendation_reason": reason,
        "next_command": next_command,
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
    return (
        "verify",
        reason,
        _command(
            "python", "-m", "research_pipeline", "verify",
            "--result", result_directory, "--result-store", store,
            "--output", str(verification_output), "--json",
        ),
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


def _command(*parts: str) -> str:
    return subprocess.list2cmdline(list(parts))


__all__ = ["execute"]
