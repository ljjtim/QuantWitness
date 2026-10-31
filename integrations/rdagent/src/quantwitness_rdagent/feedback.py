"""从 RP 正式事实投影技术反馈，不按收益筛选公式。"""

def acceptable(feedback):
    return all(feedback.get(key) == value for key, value in (
        ("command_status", "succeeded"), ("execution_status", "succeeded"),
    )) and bool(feedback.get("result_ref"))


def candidate_needs_repair(evidence):
    """仅明确属于候选代码的失败允许触发付费修复。"""
    diagnostics = [item for item in evidence.get("diagnostics", []) if isinstance(item, dict)]
    if any(item.get("stage") in {"report", "lint", "admit", "build", "inspect"} for item in diagnostics):
        return False
    if any(item.get("recommended_action") in {"wait", "readmit"} or
           str(item.get("error_code", "")).startswith(("resource_", "runtime_resource_", "project_worker_")) or
           item.get("error_code") in {"worker_crash", "heartbeat_timeout"} for item in diagnostics):
        return False
    for item in diagnostics:
        if item.get("stage") == "code_validation":
            if item.get("error_type") in {"SyntaxError", "IndentationError", "TabError"}:
                return True
            if str(item.get("message", "")).startswith("code."):
                return True
        if item.get("stage") == "run" and item.get("candidate_operator") is True:
            code = item.get("error_code")
            if code in {"SyntaxError", "IndentationError", "NameError", "UnboundLocalError", "TypeError",
                        "AttributeError", "KeyError", "IndexError", "ZeroDivisionError", "OverflowError",
                        "compute.daily_value 返回合同不符", "compute.rolling_value 返回合同不符"}:
                return True
    return False


def project_feedback(request, candidate_id, evidence):
    fields = ("bundle_ref", "execution_ref", "command_status", "execution_status", "claim_level", "diagnostics", "result_ref")
    missing = set(fields) - set(evidence)
    if missing:
        raise ValueError(f"RP 执行反馈缺少字段: {sorted(missing)}")
    return {"request_id": request.payload["request_id"], "candidate_id": candidate_id,
            "evaluation_scope": request.payload["development_scope"], "allowed_metrics": {},
            **{key: evidence[key] for key in fields}}
