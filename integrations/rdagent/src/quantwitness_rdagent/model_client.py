"""显式读取.env，通过指定代理调用Responses接口；凭据不进入环境或会话。"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


_CODE_INSTRUCTIONS = "按研究任务实现Python公式。仅输出完整compute.py代码，不输出解释。"


class ModelCallError(RuntimeError):
    """仅包含可公开诊断，不带请求头或服务端原始正文。"""


def _config(env_path):
    from dotenv import dotenv_values
    path = Path(env_path)
    if not path.is_file():
        raise ModelCallError("model_env_file_missing")
    values = dotenv_values(path, interpolate=False, encoding="utf-8-sig")
    required = ("MODEL", "API_KEY", "BASE_URL", "PROXY_URL")
    if any(not values.get(key) for key in required):
        raise ModelCallError("model_env_fields_missing")
    from urllib.parse import urlsplit
    endpoint = urlsplit(values["BASE_URL"])
    proxy = urlsplit(values["PROXY_URL"])
    if endpoint.scheme != "https" or not endpoint.netloc or endpoint.username or endpoint.password:
        raise ModelCallError("model_endpoint_invalid")
    if proxy.scheme not in {"http", "https"} or not proxy.hostname:
        raise ModelCallError("model_proxy_required")
    return values


def public_config(env_path):
    """只返回冻结研究运行需要的非敏感模型身份。"""
    values = _config(env_path)
    return {"model": values["MODEL"], "base_url": values["BASE_URL"].rstrip("/")}


def _windows_path(path):
    text = str(path)
    if text.startswith("/mnt/") and len(text) > 7:
        return text[5].upper() + ":/" + text[7:]
    return text.replace("\\", "/")


def _linux_path(path):
    text = str(path).replace("\\", "/")
    if len(text) > 2 and text[1] == ":":
        return "/mnt/" + text[0].lower() + text[2:]
    return text


def _response(payload, model):
    if payload.get("status") != "completed":
        raise ModelCallError("model_response_incomplete")
    text = "".join(part.get("text", "") for item in payload.get("output", [])
                   if item.get("type") == "message" for part in item.get("content", [])
                   if part.get("type") == "output_text")
    if not text.strip():
        raise ModelCallError("model_response_empty")
    return {"text": text, "usage": payload.get("usage") or {}, "model": payload.get("model", model)}


def _http_generate(values, prompt, max_output_tokens, *, instructions=_CODE_INSTRUCTIONS):
    import httpx
    body = {"model": values["MODEL"], "instructions": instructions,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "store": False, "stream": True, "max_output_tokens": max_output_tokens}
    headers = {"Authorization": "Bearer " + values["API_KEY"], "Content-Type": "application/json"}
    try:
        timeout = float(values.get("TIMEOUT_SECONDS", "180"))
        with httpx.Client(proxy=values["PROXY_URL"], trust_env=False, timeout=timeout, follow_redirects=False) as client:
            with client.stream("POST", values["BASE_URL"].rstrip("/") + "/responses", json=body, headers=headers) as response:
                if response.status_code != 200:
                    raise ModelCallError("model_http_status_" + str(response.status_code))
                if "text/event-stream" not in response.headers.get("content-type", ""):
                    response.read()
                    return _response(response.json(), values["MODEL"])
                data = []
                for line in response.iter_lines():
                    if line.startswith("data:"):
                        data.append(line[5:].strip())
                    elif not line and data:
                        raw = "\n".join(data)
                        data = []
                        if raw == "[DONE]":
                            continue
                        event = json.loads(raw)
                        if event.get("type") == "response.completed":
                            return _response(event["response"], values["MODEL"])
                        if event.get("type") in {"response.failed", "response.incomplete", "error"}:
                            raise ModelCallError("model_response_failed")
        raise ModelCallError("model_stream_interrupted")
    except ModelCallError:
        raise
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        raise ModelCallError("model_transport_or_protocol_error") from None


def generate(env_path, prompt, max_output_tokens):
    """沿用公式代码生成指令与返回合同。"""
    return request_text(env_path, prompt, max_output_tokens, instructions=_CODE_INSTRUCTIONS)


def request_text(env_path, prompt, max_output_tokens, *, instructions):
    """任务指令由调用方显式提供，不从.env读取。"""
    if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 8192:
        raise ModelCallError("model_output_budget_invalid")
    if not isinstance(instructions, str) or not instructions.strip():
        raise ModelCallError("model_instructions_invalid")
    values = _config(env_path)
    if os.name == "nt":
        return _http_generate(values, prompt, max_output_tokens, instructions=instructions)
    # WSL中的RD通过Windows使用其仅监听回环地址的本机代理。
    if not values.get("WINDOWS_PYTHON") or not values.get("WINDOWS_REPO"):
        raise ModelCallError("model_windows_bridge_config_missing")
    bootstrap = ("import sys,runpy;from pathlib import Path;root=Path(sys.argv[1]);"
                 "root=root if (root/'integrations/rdagent/src').is_dir() else root/'research_pipeline';"
                 "sys.path.insert(0,str(root/'integrations/rdagent/src'));"
                 "sys.argv=['quantwitness_rdagent.model_client'];"
                 "runpy.run_module('quantwitness_rdagent.model_client',run_name='__main__')")
    argv = [_linux_path(values["WINDOWS_PYTHON"]), "-B", "-X", "utf8", "-c", bootstrap,
            values["WINDOWS_REPO"].rstrip("/\\")]
    request = {"env_path": _windows_path(Path(env_path).resolve()), "prompt": prompt,
               "max_output_tokens": max_output_tokens, "instructions": instructions}
    try:
        completed = subprocess.run(argv, input=json.dumps(request, ensure_ascii=False), text=True,
            encoding="utf-8", capture_output=True, check=False, timeout=float(values.get("TIMEOUT_SECONDS", "180")) + 30)
        result = json.loads(completed.stdout)
    except (subprocess.SubprocessError, OSError, ValueError):
        raise ModelCallError("model_windows_bridge_failed") from None
    if completed.returncode != 0 or "error" in result:
        raise ModelCallError(result.get("error", "model_windows_bridge_failed"))
    return result


def main():
    try:
        request = json.load(sys.stdin)
        result = request_text(request["env_path"], request["prompt"], request["max_output_tokens"],
                              instructions=request.get("instructions", _CODE_INSTRUCTIONS))
        print(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        message = str(exc) if isinstance(exc, ModelCallError) else "model_client_failed"
        print(json.dumps({"error": message}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
