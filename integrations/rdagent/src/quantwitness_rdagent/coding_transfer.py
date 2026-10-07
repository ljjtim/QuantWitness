"""按接口与实际代码结构检索技术经验，不读取金融指标。"""
import ast
import json
import re


_TECHNICAL_ERRORS = {"SyntaxError", "IndentationError", "TabError", "NameError", "UnboundLocalError",
                     "TypeError", "AttributeError", "KeyError", "IndexError", "ZeroDivisionError",
                     "OverflowError", "ValueError"}
_FORMAL_REFS = ("execution_ref", "result_ref", "verification_ref", "bundle_ref")


def technical_profile(source, objective=""):
    """从完整源码和声明公式提取可解释的计算结构。"""
    operations, libraries = set(), set()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        tree = None
    if tree is not None:
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                libraries.update(item.name.split(".")[0] for item in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                libraries.add(node.module.split(".")[0])
            elif isinstance(node, (ast.For, ast.While, ast.ListComp, ast.GeneratorExp)):
                operations.add("iteration")
            elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Slice):
                operations.add("window_slice")
            elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
                operations.add("division")
            elif isinstance(node, ast.If):
                operations.add("conditional_guard")
            elif isinstance(node, ast.Constant) and node.value is None:
                operations.add("missing_value")
            elif isinstance(node, ast.Call):
                name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
                name = name.lower()
                if name in {"mean", "fmean", "average", "sum"}:
                    operations.add("aggregation")
                elif name in {"stdev", "std", "variance", "var"}:
                    operations.add("dispersion")
                elif name in {"min", "max", "minimum", "maximum"}:
                    operations.add("extrema")
                elif name in {"isfinite", "isnan", "dropna", "fillna"}:
                    operations.add("missing_value")
                elif name in {"array", "asarray", "reshape", "tensor"}:
                    operations.add("array_shape")
                elif name in {"fit", "predict"}:
                    operations.add("model_fit_predict")
                elif name in {"lstm", "gru", "transformer", "transformerencoder"}:
                    operations.add("sequence_model")
                elif name in {"linear", "relu", "tanh", "gelu"}:
                    operations.add("feedforward_model")
    text = objective.lower()
    for pattern, operation in ((r"\b(ref|rolling|window)\b|滚动|窗口", "window_slice"),
                               (r"\b(mean|average)\b|均值|均价", "aggregation"),
                               (r"\b(std|stdev|variance)\b|标准差|波动", "dispersion"),
                               (r"\b(min|max)\b|最高|最低|区间", "extrema"),
                               (r"lstm|gru|transformer", "sequence_model"),
                               (r"linear|ridge|lgb|xgb|ensemble", "model_fit_predict")):
        if re.search(pattern, text):
            operations.add(operation)
    return {"operations": sorted(operations), "libraries": sorted(libraries)}


def matching_reason(source_task, target_task, source, target_source=""):
    """先按合同筛选，再按相同目标或共同结构决定是否迁移。"""
    if source_task.get("kind") != target_task.get("kind") or source_task.get("interface") != target_task.get("interface"):
        return None
    objective_key = "formula" if target_task["kind"] == "formula" else "model_family"
    source_profile = technical_profile(source, source_task.get(objective_key, ""))
    target_profile = technical_profile(target_source, target_task.get(objective_key, ""))
    for task, profile in ((source_task, source_profile), (target_task, target_profile)):
        if "definition" in task:
            profile["operations"] = sorted(set(profile["operations"]) | {"feedforward_model"})
    if (source_task.get(objective_key) == target_task.get(objective_key)
            and source_task.get("definition") == target_task.get("definition")):
        kind = "exact_task"
    else:
        operations = sorted(set(source_profile["operations"]) & set(target_profile["operations"]))
        libraries = sorted(set(source_profile["libraries"]) & set(target_profile["libraries"]) - {"__future__", "math"})
        if not operations and not libraries:
            return None
        kind = "technical_transfer"
    return {"kind": kind, "shared_operations": sorted(set(source_profile["operations"]) & set(target_profile["operations"])),
            "shared_libraries": sorted(set(source_profile["libraries"]) & set(target_profile["libraries"]) - {"__future__", "math"})}


def technical_feedback(evidence):
    """只保留状态与明确技术错误，不发送诊断正文或实测值。"""
    statuses = {"succeeded", "failed", "pass", "fail", "not_run"}
    result = {key: evidence[key] for key in ("command_status", "execution_status", "verification_status", "formula_status", "model_status")
              if isinstance(evidence.get(key), str) and evidence[key] in statuses}
    codes = set()
    for item in evidence.get("diagnostics", []):
        if isinstance(item, dict):
            stage = item.get("stage")
            if stage in {"code_validation", "run", "model_validation"}:
                codes.add("stage:" + stage)
            error = item.get("error_type", item.get("error_code"))
            if isinstance(error, str) and error in _TECHNICAL_ERRORS:
                codes.add("error_type:" + error)
            text = str(item.get("error_code", "")) + " " + str(item.get("message", ""))
        else:
            text = str(item)
        codes.update(match.group() for match in re.finditer(r"(?<![A-Za-z0-9_])(?:formula|code|model)\.[a-z_]+", text))
    result["diagnostic_codes"] = sorted(codes)
    return result



def stored_technical_feedback(feedback):
    statuses = {"succeeded", "failed", "pass", "fail", "not_run"}
    result = {key: feedback[key] for key in ("command_status", "execution_status", "verification_status", "formula_status", "model_status")
              if isinstance(feedback.get(key), str) and feedback[key] in statuses}
    result["diagnostic_codes"] = [code for code in feedback.get("diagnostic_codes", []) if isinstance(code, str) and (
        re.fullmatch(r"(?:formula|code|model)\.[a-z_]+", code)
        or code in {"stage:code_validation", "stage:run", "stage:model_validation"}
        or code.startswith("error_type:") and code.split(":", 1)[1] in _TECHNICAL_ERRORS)]
    return result


def model_needs_repair(evidence):
    """执行资源或研究链失败不能成为模型代码的修复经验。"""
    for item in evidence.get("diagnostics", []):
        if isinstance(item, dict):
            if item.get("stage") in {"report", "verify", "lint", "admit", "build", "inspect"}:
                return False
            code = str(item.get("error_code", ""))
            if (item.get("recommended_action") in {"wait", "readmit"}
                    or code.startswith(("resource_", "runtime_resource_", "project_worker_"))
                    or code in {"worker_crash", "heartbeat_timeout"}):
                return False
    feedback = technical_feedback(evidence)
    return bool(feedback["diagnostic_codes"] and any(
        code.startswith(("code.", "model.", "error_type:")) for code in feedback["diagnostic_codes"]))


def formal_refs(evidence):
    return {key: evidence[key] for key in _FORMAL_REFS if isinstance(evidence.get(key), str) and evidence[key]}


def prompt_record(record, target_task, target_source=""):
    """提示保留来源身份与代码，磁盘正式引用不进入提示。"""
    reason = matching_reason(record["task"], target_task, record["source"], target_source)
    if reason is None:
        return None
    return {"record_id": record["record_id"], "source_task": record["task"], "source": record["source"],
            "technical_feedback": stored_technical_feedback(record["technical_feedback"]), "success": record["success"],
            "technical_profile": technical_profile(record["source"]), "match_reason": reason}


def select_records(records, target_task, target_source, *, max_records, max_source_chars):
    """保留成功与其失败链，同目标优先，源码预算不截断文件。"""
    groups = {}
    for record in records:
        key = record.get("repair_group") or (record["record_id"].rsplit("/", 1)[0], json.dumps(record["task"], sort_keys=True, ensure_ascii=False))
        groups.setdefault(key, []).append(record)
    ranked = []
    for group in groups.values():
        if all("sequence" in item for item in group):
            group.sort(key=lambda item: item["sequence"])
        successes = [item for item in group if item["success"]]
        if not successes:
            continue
        success = successes[-1]
        view = prompt_record(success, target_task, target_source)
        if view is None:
            continue
        success_index = group.index(success)
        failed = [item for item in group[:success_index] if not item["success"]]
        view["repair_record_id"] = success["record_id"]
        chain = [view] + [{**view,
                          "record_id": item["record_id"], "source_task": item["task"], "source": item["source"],
                          "technical_profile": technical_profile(item["source"]),
                          "technical_feedback": stored_technical_feedback(item["technical_feedback"]), "success": False}
                         for item in reversed(failed)]
        ranked.append((0 if view["match_reason"]["kind"] == "exact_task" else 1, chain))
    ranked.sort(key=lambda item: item[0])
    result, used = [], 0
    for _, chain in ranked:
        for view in chain:
            if len(result) >= max_records:
                return result
            if used + len(view["source"]) > max_source_chars:
                if view["success"]:
                    break
                continue
            used += len(view["source"])
            result.append(view)
    return result
