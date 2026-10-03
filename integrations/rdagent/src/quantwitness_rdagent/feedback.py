"""从 RP 正式事实投影技术反馈，不按收益筛选公式。"""

def acceptable(feedback):
    return all(feedback.get(key) == value for key, value in (
        ("command_status", "succeeded"), ("execution_status", "succeeded"),
        ("verification_status", "pass"), ("formula_status", "pass"),
    ))


def candidate_needs_repair(evidence):
    """仅明确属于候选代码的失败允许触发付费修复。"""
    diagnostics = [item for item in evidence.get("diagnostics", []) if isinstance(item, dict)]
    if any(item.get("stage") in {"report", "verify", "lint", "admit", "build", "inspect"} for item in diagnostics):
        return False
    if any(item.get("recommended_action") in {"wait", "readmit"} or
           str(item.get("error_code", "")).startswith(("resource_", "runtime_resource_", "project_worker_")) or
           item.get("error_code") in {"worker_crash", "heartbeat_timeout"} for item in diagnostics):
        return False
    if evidence.get("command_status") == "succeeded" and evidence.get("formula_status") == "fail":
        return True
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
    fields = ("bundle_ref", "execution_ref", "command_status", "execution_status", "verification_status",
              "formula_status", "formula_coverage", "claim_level", "diagnostics", "result_ref", "verification_ref")
    missing = set(fields) - set(evidence)
    if missing:
        raise ValueError(f"RP 执行反馈缺少字段: {sorted(missing)}")
    if evidence["formula_status"] not in {"pass", "fail", "not_run"}:
        raise ValueError("公式状态无效")
    return {"request_id": request.payload["request_id"], "candidate_id": candidate_id,
            "evaluation_scope": request.payload["development_scope"], "allowed_metrics": {},
            **{key: evidence[key] for key in fields}}


def project_formula_feedback(verified, result_store, evaluation=None):
    """仅从已复验上下文投影冻结公式验证器的结论。"""
    verdict = verified.verification
    identity = verdict.project_verifier_identity or {}
    accepted = {
        ("dai-zhu-er-jiu-fixed-session-formula-check", "2.0.0"),
        ("dai-zhu-er-jiu-formula-and-statistics-check", "2.0.0"),
        ("dai-zhu-er-jiu-formula-and-statistics-check", "3.0.0"),
    }
    schema = "project.dai-zhu-er-jiu.formula.coverage.v2"
    if evaluation is not None:
        accepted = {(evaluation["verifier_id"], evaluation["verifier_version"])}
        schema = evaluation["coverage_schema_id"]
    if ((identity.get("verifier_id"), identity.get("verifier_version")) not in accepted
            or not verdict.project_verifier_outcome_hash
            or not any(item.schema_id == schema for item in verified.snapshot.bundle.tables)):
        return {"formula_status": "not_run", "formula_coverage": {}}
    coverage = result_store.read_table_by_schema_id(verified.snapshot.bundle, schema_id=schema).to_pylist()
    if not coverage:
        return {"formula_status": "not_run", "formula_coverage": {}}
    findings = [item for item in verdict.limitations if str(item).startswith("formula.")]
    status = "fail" if findings else ("pass" if verdict.status == "pass" else "not_run")
    return {"formula_status": status, "formula_coverage": {
        "entity_sessions": len(coverage), "entities": sorted({row["entity"] for row in coverage}),
        "sessions": sorted({row["session"] for row in coverage}),
        "raw_rows": sum(row["raw_rows"] for row in coverage),
    }}
