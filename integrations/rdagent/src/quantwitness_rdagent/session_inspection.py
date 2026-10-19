"""只读查看研究会话已有收据，不构建研究对象或加载RD运行依赖。"""
import json
from pathlib import Path


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def changed_request_fields(saved, current, prefix=""):
    """只返回字段位置，不把模型响应或用户配置值放入差异提示。"""
    if isinstance(saved, dict) and isinstance(current, dict):
        result = []
        for key in sorted(saved.keys() | current.keys()):
            field = prefix + "." + key if prefix else key
            if key not in saved or key not in current:
                result.append(field)
            else:
                result.extend(changed_request_fields(saved[key], current[key], field))
        return result
    return [] if saved == current else [prefix]



def check_frozen_request(payload):
    frozen = Path(payload["session_root"]) / "request.json"
    if frozen.exists():
        changed = changed_request_fields(_read(frozen), payload)
        if changed:
            raise ValueError("研究请求与已保存会话不一致：" + ", ".join(changed)
                             + "；请使用原请求恢复，改变研究范围须新会话")

def validate_campaign_request(payload):
    if payload.get("research_kind") == "package":
        from .package_campaign import validate_package_campaign
        return validate_package_campaign(payload)
    from .campaign import validate_campaign
    return validate_campaign(payload)


def _inspection_request(payload):
    # 查看冻结声明只比较事实，不运行模型结构编译或重新读取研究来源。
    value = json.loads(json.dumps(payload, allow_nan=False))
    if value.get("research_kind") == "factor_research" and "baseline" in value:
        from .factor_research import canonical_expression, factor_contract
        value["baseline"]["expression"] = canonical_expression(
            value["baseline"]["expression"], contract=factor_contract(value))
    if value.get("research_kind") == "joint_research" and "factor_request" in value:
        value["factor_request"] = _inspection_request(value["factor_request"])
    return value


def _execution_receipts(root):
    # 只走已知研究层级，避免递归扫描Workspace、模型和正式Result载荷。
    sessions = [root, *sorted((root / "experiments").glob("*"))]
    for branch in sorted((root / "branches").glob("*")):
        sessions.append(branch)
        sessions.extend(sorted((branch / "experiments").glob("*")))
    for session in sessions:
        for candidate in sorted((session / "candidates").glob("*")):
            if candidate.is_dir():
                yield candidate


def inspect_session(payload, *, command):
    resume = command.removesuffix("inspect") + "resume"
    if command == "inspect":
        root = Path(payload["runtime_binding"]["linux_session_root"])
    else:
        if not isinstance(payload.get("session_root"), str) or not payload["session_root"]:
            raise ValueError("查看研究会话须提供session_root")
        root = Path(payload["session_root"])
    frozen = root / "request.json"
    result = {"status": "not_started", "session_root": str(root), "completed_rounds": 0,
              "request_consistent": None, "next_action": command.removesuffix("inspect") + "run"}
    if frozen.exists():
        saved = _read(frozen)
        # 公式按冻结时的表达式规范比较；查看不依赖模型编译和来源材料。
        try:
            current = _inspection_request(payload)
        except ValueError as exc:
            result.update(status="request_mismatch", request_consistent=False,
                          changed_fields=changed_request_fields(saved, payload),
                          request_error=str(exc), next_action="use_frozen_request_or_new_session")
            return result
        changed = changed_request_fields(saved, current)
        if changed:
            result.update(status="request_mismatch", request_consistent=False,
                          changed_fields=changed, next_action="use_frozen_request_or_new_session")
            return result
        result["request_consistent"] = True
    else:
        _inspection_request(payload)
    records = [_read(p) for p in sorted((root / "rounds").glob("*/record.json"))]
    result["completed_rounds"] = len(records)
    outcome = root / "outcome.json"
    if outcome.exists():
        return {**_read(outcome), **{key: value for key, value in result.items()
                                  if key not in {"status", "next_action", "completed_rounds"}},
                "completed_rounds": len(records), "next_action": None}
    if frozen.exists():
        result.update(status="incomplete", next_action=resume)
    pending_rounds = []
    for round_root in sorted((root / "rounds").glob("*")):
        if round_root.is_dir() and not (round_root / "record.json").exists():
            stages = [name for name in ("proposal", "evaluation", "reflection")
                      if (round_root / (name + ".json")).exists()]
            if stages:
                pending_rounds.append({"round": round_root.name, "saved_stages": stages})
    if pending_rounds:
        result.update(status="interrupted", pending_rounds=pending_rounds, next_action=resume)
    completed_executions = {r.get("metrics", {}).get("execution_ref") for r in records}
    executions = []
    running = False
    for candidate in _execution_receipts(root):
        reference = candidate / "execution-ref.json"
        diagnostic = candidate / "execution-diagnostic.json"
        feedback = candidate / "feedback.json"
        if not reference.exists() and not diagnostic.exists() and not feedback.exists():
            continue
        entry = {"candidate_root": str(candidate)}
        if reference.exists():
            entry.update(_read(reference))
        if entry.get("execution_root") is not None and entry["execution_root"] in completed_executions:
            continue
        if diagnostic.exists():
            entry["diagnostic"] = _read(diagnostic)
        if feedback.exists():
            entry["feedback"] = _read(feedback)
        owner = candidate / ".package-execution.lock"
        if owner.exists():
            from research_pipeline.runtime.store import process_identity_alive
            identity = _read(owner)
            entry["owner_alive"] = process_identity_alive(identity["pid"], identity["process_started_at"])
            running |= entry["owner_alive"]
        executions.append(entry)
    if executions:
        result.update(status="running" if running else "interrupted", executions=executions,
                      next_action="wait" if running else resume)
    unresolved_calls = []
    for path in sorted((root / "calls").glob("*.json")):
        call = _read(path)
        if call.get("status") in {"reserved", "failed"}:
            unresolved_calls.append({"path": str(path), "status": call["status"],
                                     "transport": call.get("transport"),
                                     "error_code": call.get("error_code", "model_call_outcome_unknown")})
    if unresolved_calls:
        result["unresolved_model_calls"] = unresolved_calls
        if not running:
            result.update(status="interrupted", next_action="review_model_calls")
    # 仅展示现有收据，不能把诊断或失败状态提升为可反馈的开发指标。
    return result
