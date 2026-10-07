"""模型代码生成、最小技术提示与调用预约；不读取行情或独立参考源码。"""
import ast
from contextlib import contextmanager
from contextvars import ContextVar
import json
from pathlib import Path
import re

from .contracts import write_json

_MODEL_ENV_PATH = ContextVar("rp_model_env_path", default=None)


@contextmanager
def model_environment(path):
    """路径只属于当前调用上下文，不进入Loop或CoSTEER序列化对象。"""
    token = _MODEL_ENV_PATH.set(None if path is None else str(Path(path).resolve()))
    try:
        yield
    finally:
        _MODEL_ENV_PATH.reset(token)


def validate_initial_stub(source):
    tree = ast.parse(source)
    functions = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            continue
        if not isinstance(node, ast.FunctionDef) or node.decorator_list:
            raise ValueError("initial_stub只能包含函数签名、说明和未实现函数体")
        body = [part for part in node.body if not (isinstance(part, ast.Expr)
                and isinstance(part.value, ast.Constant) and isinstance(part.value.value, str))]
        if len(body) != 1 or not (isinstance(body[0], ast.Pass) or
            isinstance(body[0], ast.Raise) and isinstance(body[0].exc, (ast.Name, ast.Call))
            and isinstance(body[0].exc if isinstance(body[0].exc, ast.Name) else body[0].exc.func, ast.Name)
            and (body[0].exc if isinstance(body[0].exc, ast.Name) else body[0].exc.func).id == "NotImplementedError"):
            raise ValueError("initial_stub不得包含已实现公式")
        functions.append(node.name)
    if set(functions) != {"daily_value", "rolling_value"} or len(functions) != 2:
        raise ValueError("initial_stub必须声明daily_value和rolling_value")



def validate_generated_source(source):
    """公式源码只接受纯计算语法；不提供通用Python执行环境。"""
    tree = ast.parse(source)
    prohibited = {"open", "exec", "eval", "compile", "__import__", "globals", "locals",
                  "getattr", "setattr", "delattr", "vars", "dir", "input", "breakpoint",
                  "type", "object", "super", "help", "exit", "quit"}
    modules = {"math", "statistics", "__future__"}
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imported = {item.name for item in node.names} if isinstance(node, ast.Import) else {node.module}
            if not imported <= modules:
                raise ValueError("code.unsupported_import")
        elif isinstance(node, ast.FunctionDef):
            if node.decorator_list:
                raise ValueError("code.unsupported_decorator")
            names.add(node.name)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            pass
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            try:
                ast.literal_eval(node.value)
            except (ValueError, TypeError):
                raise ValueError("code.unsupported_module_expression") from None
        else:
            raise ValueError("code.unsupported_module_statement")
    if not {"daily_value", "rolling_value"} <= names:
        raise ValueError("code.missing_formula_interface")
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)) and node not in tree.body:
            raise ValueError("code.unsupported_nested_import")
        if isinstance(node, (ast.ClassDef, ast.AsyncFunctionDef, ast.With, ast.AsyncWith)):
            raise ValueError("code.unsupported_statement")
        if isinstance(node, ast.Name) and (node.id in prohibited or "__" in node.id):
            raise ValueError("code.unsupported_name")
        if isinstance(node, ast.Attribute) and "__" in node.attr:
            raise ValueError("code.unsupported_attribute")
    compile(tree, "compute.py", "exec")

def _technical_feedback(evidence):
    """只发送状态和错误类别；完整错误正文可能含行情值、路径或标签。"""
    result = {key: evidence[key] for key in ("command_status", "execution_status", "verification_status", "formula_status")
              if evidence.get(key) in {"succeeded", "failed", "pass", "fail", "not_run"}}
    codes = set()
    for item in evidence.get("diagnostics", []):
        if isinstance(item, dict):
            for key in ("stage", "error_type", "error_code"):
                value = item.get(key)
                if isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,100}", value):
                    codes.add(key + ":" + value)
            text = str(item.get("message", ""))
        else:
            text = str(item)
        for match in re.finditer(r"(?<![A-Za-z0-9_])(?:formula|code)\.[a-z_]+", text):
            codes.add(match.group())
    result["diagnostic_codes"] = sorted(codes)
    return result


def build_prompt(request, previous_source=None, evidence=None, coding_knowledge=None):
    spec = request.payload["code_generation"]
    payload = {"interface": spec["interface"], "formula": spec["formula"],
               "source": spec["initial_stub"] if previous_source is None else previous_source}
    if coding_knowledge:
        payload["coding_knowledge"] = coding_knowledge
    if evidence is not None:
        payload["technical_feedback"] = _technical_feedback(evidence)
    return ("请只返回可直接保存为compute.py的完整Python源码，不加Markdown代码围栏。"
            "只实现声明的daily_value和rolling_value接口；保持缺失位置与固定交易窗口，禁止未来信息。"
            "只允许math、statistics、__future__导入以及公式函数和纯计算helper。"
            "不得访问文件、网络、数据库或独立Verifier，禁止动态exec/eval和反射；不优化收益。"
            "修复时依据接口、公式与技术诊断。\n"
            + json.dumps(payload, ensure_ascii=False))


def generate_source(request, attempt, *, previous_source=None, evidence=None, coding_knowledge=None):
    """每次编码尝试最多一次调用；预约中断或失败必须显式处理，不自动重付费。"""
    folder = request.session_root / "model-calls"
    folder.mkdir(exist_ok=True)
    path = folder / f"call-{attempt:04d}.json"
    if path.exists():
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if receipt["status"] == "completed":
            return receipt["text"]
        raise RuntimeError("模型调用已预约或失败，预算已占用；请检查调用记录，不自动重试付费")
    env_path = _MODEL_ENV_PATH.get()
    if env_path is None:
        raise ValueError("live生成必须显式提供--model-env-file")
    from .model_client import generate, public_config, ModelCallError
    spec = request.payload["code_generation"]
    if public_config(env_path) != {"model": spec["model"], "base_url": spec["base_url"].rstrip("/")}:
        raise ValueError(".env的模型或接口地址与冻结请求不一致")
    budget = request.payload["budget"]
    calls = [json.loads(item.read_text(encoding="utf-8")) for item in sorted(folder.glob("call-*.json"))]
    remaining = budget["max_output_tokens"] - sum(item["reserved_output_tokens"] for item in calls)
    if len(calls) >= budget["live_llm_calls"] or remaining <= 0:
        raise RuntimeError("持久模型调用或输出token预约预算已耗尽")
    maximum = min(remaining, request.payload["code_generation"]["max_output_tokens_per_call"])
    prompt = build_prompt(request, previous_source, evidence, coding_knowledge)
    receipt = {"attempt": attempt, "status": "reserved", "model": request.payload["code_generation"]["model"],
               "reserved_output_tokens": maximum, "prompt": prompt}
    write_json(path, receipt)
    try:
        response = generate(env_path, prompt, maximum)
        if (not isinstance(response, dict) or response.get("model") != receipt["model"]
                or not isinstance(response.get("text"), str) or not response["text"].strip()):
            raise ValueError("模型响应合同无效")
        usage = response.get("usage", {})
        if isinstance(usage, dict) and type(usage.get("output_tokens")) is int and usage["output_tokens"] > maximum:
            raise ValueError("模型响应输出用量超过预约上限")
        receipt.update(status="completed", text=response["text"], usage={
            key: value for key, value in usage.items() if key in ("input_tokens", "output_tokens", "total_tokens")
            and type(value) is int and value >= 0} if isinstance(usage, dict) else {})
        write_json(path, receipt)
    except Exception as exc:
        code = str(exc) if isinstance(exc, ModelCallError) else "model_call_failed_or_response_unusable"
        receipt.update(status="failed", error_code=code)
        write_json(path, receipt)
        raise RuntimeError("模型调用失败或响应不可用；详情仅保留安全错误码，调用预算已占用") from None
    return receipt["text"]
