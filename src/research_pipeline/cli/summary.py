"""只从服务返回值生成 CLI 状态摘要，不读取研究工件。"""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from research_pipeline.platform.process_budget_advice import resource_failure_advice

from .command_suggestion import render_powershell_command


_DETAIL_FIELDS = (
    "result_id", "result_directory", "run_id", "runtime_run_id", "run_root",
    "execution_id", "execution_root", "execution_path", "output", "report_path",
    "verification_path", "verification_result", "result_store", "package_path",
    "plan_path", "plan_directory", "failed_stage", "failed_node",
    "root_error", "identity_issue", "issues", "stages", "resource_advice",
    "resource_measurement_status", "process_cleanup_status", "validity_status",
    "claim_level", "next_action", "recommended_action", "recommendation_reason",
)
_RUN_COMMANDS = {"run", "resume", "retry-node", "rerun-from"}


def build_summary(
    args: Any,
    *,
    status: str,
    data: object,
    error_code: str | None = None,
) -> dict[str, object]:
    """命令完成、Result 封存和验证结论分别投影，缺少证据不记作成功。"""

    details = data if isinstance(data, Mapping) else {}
    command = getattr(args, "command", None)
    workspace = command == "workspace"
    action = getattr(args, "workspace_command", None) if workspace else command
    summary: dict[str, object] = {
        "command_status": status,
        "execution_status": None,
        "verification_status": None,
    }
    summary.update({key: details[key] for key in _DETAIL_FIELDS if key in details})
    if action in _RUN_COMMANDS or (workspace and action == "execute"):
        summary["execution_status"] = _run_status(details, status, workspace and action == "execute")
    elif action == "inspect" and "run_status" in details:
        summary["execution_status"] = _inspection_status(details)
    if command == "inspect":
        summary["node_errors"] = _node_errors(details)
    finalize = details.get("finalize")
    if isinstance(finalize, Mapping):
        summary["finalize_status"] = finalize.get("status")
        for key in ("result_id", "result_directory", "result_published"):
            if key in finalize:
                summary[key] = finalize[key]
        if finalize.get("error"):
            summary["finalize_error"] = finalize["error"]
    if command == "verify":
        summary["verification_status"] = details.get("status")
    elif "verification_status" in details:
        summary["verification_status"] = details["verification_status"]
    advice = resource_failure_advice(
        details,
        error_code=error_code,
        purpose="verifier" if command == "verify" or details.get("failed_stage") == "verify" else "worker",
    )
    if advice is not None:
        summary["resource_advice"] = [*details.get("resource_advice", []), advice]
    return summary



def _node_errors(details: Mapping[str, object]) -> list[dict[str, object]]:
    """只保留服务已提供的节点错误，后续动作沿用整条运行的唯一建议。"""

    nodes = details.get("nodes", {})
    if not isinstance(nodes, Mapping):
        return []
    errors = []
    for node_id, node in sorted(nodes.items()):
        error = node.get("last_error")
        if not isinstance(error, Mapping) or not error:
            continue
        item = {
            "node_id": node_id,
            "status": node.get("status"),
            **{key: error.get(key) for key in ("error_code", "message", "exception_type")},
        }
        if error.get("failure_context") is not None:
            item["failure_context"] = error["failure_context"]
        errors.append(item)
    return errors


def _inspection_status(details: Mapping[str, object]) -> object:
    runtime = details.get("run_status")
    if runtime != "succeeded":
        return runtime
    finalize = details.get("finalize")
    final_status = finalize.get("status") if isinstance(finalize, Mapping) else None
    if final_status == "succeeded":
        return "succeeded"
    return "finalize_failed" if final_status == "failed" else "finalize_pending"


def _run_status(details: Mapping[str, object], command_status: str, execute: bool) -> object:
    # Workspace 服务仅在确认 result_finalized 后投影执行成功。
    if execute and "execution_status" in details:
        return details["execution_status"]
    if details.get("status") == "result_finalized":
        return "succeeded"
    if execute:
        stages = details.get("stages", {})
        run = stages.get("run") if isinstance(stages, Mapping) else None
        if run == "result_finalized":
            return "succeeded"
        if run in {None, "not_started", "not_run", "skipped", "pending"}:
            return None
        if run in {"fail", "failed"}:
            return "failed"
        return "finalize_pending"
    return "failed" if command_status == "fail" else "finalize_pending"


def summary_payload(
    *, contract_version: str, summary: dict[str, object], data: object,
    error_code: str | None, message: str | None,
) -> dict[str, object]:
    """紧凑 JSON 保留缺失输入和可执行命令，不重复完整 data。"""

    payload: dict[str, object] = {"contract_version": contract_version, "summary": summary}
    if error_code is not None:
        payload["error_code"] = error_code
    if message is not None:
        payload["message"] = message
    if isinstance(data, Mapping):
        for key in ("next_command_argv", "required_inputs"):
            if key in data:
                payload[key] = data[key]
    return payload


def render_summary_text(
    summary: Mapping[str, object], *, data: object,
    error_code: str | None, message: str | None,
) -> str:
    """展示状态、主要路径、问题和下一步，不倾倒完整服务返回值。"""

    lines = [f"命令状态: {summary['command_status']}"]
    for key, label in (("execution_status", "执行状态"), ("verification_status", "验证状态")):
        if summary.get(key) is not None:
            lines.append(f"{label}: {summary[key]}")
    for key in (
        "execution_id", "execution_root", "execution_path", "result_id", "result_directory",
        "run_root", "output", "report_path", "verification_path", "verification_result",
        "package_path", "plan_path", "plan_directory", "failed_stage", "failed_node",
    ):
        if summary.get(key) is not None:
            lines.append(f"{key}: {summary[key]}")
    if error_code:
        lines.append(f"错误: {error_code}")
    if message:
        lines.append(f"问题: {message}")
    for key, label in (("root_error", "首错"), ("finalize_error", "封存错误"), ("identity_issue", "身份问题")):
        error = summary.get(key)
        if isinstance(error, Mapping):
            lines.append(f"{label}: {error.get('error_code')}: {error.get('message')}")
    for error in summary.get("node_errors", []):
        lines.append(
            f"节点错误: {error['node_id']} | {error['status']} | "
            f"{error['error_code']} | {error['exception_type']} | {error['message']}"
        )
        if error.get("failure_context") is not None:
            context = json.dumps(error["failure_context"], ensure_ascii=False, sort_keys=True)
            lines.append(f"  失败上下文: {context}")
    for issue in summary.get("issues", []):
        if isinstance(issue, Mapping):
            lines.append("问题: " + " | ".join(
                str(issue[key]) for key in ("code", "file", "field", "message", "action") if issue.get(key)
            ))
        else:
            lines.append(f"问题: {issue}")
    for advice in summary.get("resource_advice", []):
        if isinstance(advice, Mapping):
            target = advice.get("node_id") or advice.get("purpose")
            label = f"资源提示 ({target})" if target else "资源提示"
            lines.append(f"{label}: {advice.get('message', advice.get('status'))}")
            if advice.get("parameter"):
                lines.append(f"  预算位置: {advice['parameter']}")
            for name, values in advice.get("exceeded", {}).items():
                lines.append(f"  {name}: actual={values['actual']}, limit={values['limit']}, 建议={values['suggested_limit']}")
    for key in ("next_action", "recommended_action", "recommendation_reason"):
        if summary.get(key):
            lines.append(f"后续动作: {summary[key]}")
    if isinstance(data, Mapping):
        if data.get("required_inputs"):
            lines.append(f"需要补充: {data['required_inputs']}")
        argv = data.get("next_command_argv")
        if argv:
            lines.append(f"下一条命令: {render_powershell_command(argv)}")
        elif data.get("next_command"):
            lines.append(f"下一条命令: {data['next_command']}")
    return "\n".join(lines)
