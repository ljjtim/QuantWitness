"""统一 CLI 成功、失败与文本输出合同。"""

from __future__ import annotations

from collections.abc import Callable
import json
import sys
from typing import Any

from research_pipeline.platform import error_code_for_exception


CLI_RESULT_VERSION = "research-cli-result-v1"


def write_machine_json(payload: object) -> None:
    """把机器 JSON 明确写成 UTF-8 字节，不依赖终端默认代码页。"""

    text = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is not None:
        buffer.write(text.encode("utf-8"))
        buffer.flush()
        return
    sys.stdout.write(text)
    sys.stdout.flush()


def execute_guarded(args: Any, handler: Callable[[Any], dict[str, object]]) -> int:
    try:
        return emit(args, status="pass", data=handler(args), code=0)
    except Exception as exc:
        failure_payload = getattr(exc, "failure_payload", None)
        return emit(
            args,
            status="fail",
            error_code=error_code_for_exception(exc),
            message=str(exc),
            data=failure_payload if isinstance(failure_payload, dict) else None,
            code=1,
        )


def emit(
    args: Any,
    *,
    status: str,
    data: object,
    code: int,
    error_code: str | None = None,
    message: str | None = None,
) -> int:
    payload = {
        "contract_version": CLI_RESULT_VERSION,
        "status": status,
        "error_code": error_code,
        "message": message,
        "data": data,
    }
    if getattr(args, "json", False):
        write_machine_json(payload)
    else:
        print(f"status: {status}")
        if error_code:
            print(f"error_code: {error_code}")
        if message:
            print(f"message: {message}")
        if data is not None:
            print(json.dumps(data, ensure_ascii=False, sort_keys=True))
    return code


__all__ = [
    "CLI_RESULT_VERSION",
    "emit",
    "execute_guarded",
    "write_machine_json",
]
