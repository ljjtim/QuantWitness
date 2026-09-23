"""Runtime 错误摘要与 Result finalize 轻量状态投影。"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping

from research_pipeline.platform.canonical import canonical_json
from research_pipeline.platform.redaction import redact_text

from .errors import RuntimeIntegrityError


FINALIZE_STATUS_VERSION = "research-result-finalize-status-v1"
_FINALIZE_STATUSES = frozenset({"pending", "succeeded", "failed"})
_ERROR_FIELDS = frozenset({"error_code", "exception_type", "message"})
_FAILURE_CONTEXT_FIELDS = frozenset(
    {
        "contract_version",
        "request_status",
        "request_id",
        "dataset_id",
        "binding_id",
        "object_name",
        "provider",
        "output_budget",
        "execution_budget",
        "completed_request_ids",
        "underlying_exception_type",
    }
)


def _valid_failure_context(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != _FAILURE_CONTEXT_FIELDS:
        return False
    output = value.get("output_budget")
    execution = value.get("execution_budget")
    completed = value.get("completed_request_ids")
    return (
        value.get("contract_version") == "data-plane-request-failure-v1"
        and value.get("request_status") in {"not_opened", "opened"}
        and all(
            isinstance(value.get(field), str) and value.get(field)
            for field in (
                "request_id",
                "dataset_id",
                "binding_id",
                "object_name",
                "provider",
                "underlying_exception_type",
            )
        )
        and isinstance(output, Mapping)
        and set(output) == {"max_rows", "max_bytes", "batch_size"}
        and all(type(item) is int and item > 0 for item in output.values())
        and isinstance(execution, Mapping)
        and set(execution) == {"memory_bytes", "temp_bytes", "cpu_slots"}
        and type(execution.get("memory_bytes")) is int
        and execution["memory_bytes"] > 0
        and type(execution.get("temp_bytes")) is int
        and execution["temp_bytes"] >= 0
        and type(execution.get("cpu_slots")) is int
        and execution["cpu_slots"] > 0
        and isinstance(completed, list)
        and completed == sorted(set(completed))
        and all(isinstance(item, str) and item for item in completed)
    )


def _valid_error_summary(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and _ERROR_FIELDS <= set(value) <= (_ERROR_FIELDS | {"failure_context"})
        and all(isinstance(value[field], str) and value[field] for field in _ERROR_FIELDS)
        and (
            "failure_context" not in value
            or _valid_failure_context(value["failure_context"])
        )
    )


def safe_error_summary(
    error: BaseException,
    *,
    default_error_code: str,
    limit: int = 500,
) -> dict[str, object]:
    """只保留可显示、可定位且有界的错误摘要，不持久化 traceback。"""

    error_code = getattr(error, "error_code", default_error_code)
    if not isinstance(error_code, str) or not error_code:
        error_code = default_error_code
    message = redact_text(" ".join(str(error).split())) or type(error).__name__
    if len(message) > limit:
        message = f"{message[: limit - 1]}…"
    result: dict[str, object] = {
        "error_code": error_code,
        "exception_type": type(error).__name__,
        "message": message,
    }
    failure_context = getattr(error, "failure_payload", None)
    if _valid_failure_context(failure_context):
        result["failure_context"] = dict(failure_context)
    return result


def write_finalize_status(
    run_root: str | Path,
    *,
    status: str,
    result_id: str | None = None,
    result_directory: str | None = None,
    result_published: bool = False,
    error: Mapping[str, str] | None = None,
) -> None:
    """原子写入 inspect 直接消费的 finalize 状态，不把它当作 Result。"""

    if status not in _FINALIZE_STATUSES:
        raise ValueError("Result finalize 状态无效")
    if status == "succeeded":
        if not result_id or not result_directory or not result_published or error is not None:
            raise ValueError("成功 finalize 必须绑定已发布 Result")
    elif status == "pending" and (
        result_id is not None or result_directory is not None or result_published
    ):
        raise ValueError("pending finalize 不得声明已发布 Result")
    elif status == "failed":
        if result_published and (not result_id or not result_directory):
            raise ValueError("已发布但失败的 finalize 必须绑定真实 Result")
        if not result_published and (
            result_id is not None or result_directory is not None
        ):
            raise ValueError("未发布 Result 的失败 finalize 不得保留 Result 引用")
    if status == "failed" and error is None:
        raise ValueError("失败 finalize 必须包含错误摘要")
    if error is not None and not _valid_error_summary(error):
        raise ValueError("Result finalize 错误摘要 schema 无效")
    if status != "failed" and error is not None:
        raise ValueError("非失败 finalize 不得包含错误摘要")
    payload = {
        "contract_version": FINALIZE_STATUS_VERSION,
        "status": status,
        "result_id": result_id,
        "result_directory": result_directory,
        "result_published": result_published,
        "error": None if error is None else dict(error),
    }
    target = Path(run_root).resolve() / "result-finalize.json"
    if target.is_file():
        current = read_finalize_status(run_root)
        if current == payload:
            return
        if current["status"] != "pending":
            raise RuntimeIntegrityError("Result finalize 终态不可覆盖")
    elif status != "pending":
        raise RuntimeIntegrityError("Result finalize 必须先进入 pending")
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(canonical_json(payload), encoding="utf-8")
    os.replace(temporary, target)


def read_finalize_status(run_root: str | Path) -> dict[str, object]:
    """只读 finalize 投影；旧 run 缺文件时明确返回 unknown。"""

    target = Path(run_root).resolve() / "result-finalize.json"
    if not target.is_file():
        return {
            "contract_version": FINALIZE_STATUS_VERSION,
            "status": "unknown",
            "result_id": None,
            "result_directory": None,
            "result_published": None,
            "error": None,
        }
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeIntegrityError("Result finalize 状态无法读取") from exc
    expected = {
        "contract_version",
        "status",
        "result_id",
        "result_directory",
        "result_published",
        "error",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise RuntimeIntegrityError("Result finalize 状态 schema 无效")
    status = payload.get("status")
    error = payload.get("error")
    if (
        payload.get("contract_version") != FINALIZE_STATUS_VERSION
        or status not in _FINALIZE_STATUSES
        or type(payload.get("result_published")) is not bool
        or (error is not None and not _valid_error_summary(error))
    ):
        raise RuntimeIntegrityError("Result finalize 状态内容无效")
    if status == "succeeded":
        if (
            not isinstance(payload.get("result_id"), str)
            or not isinstance(payload.get("result_directory"), str)
            or payload["result_published"] is not True
            or error is not None
        ):
            raise RuntimeIntegrityError("成功 finalize 状态未绑定已发布 Result")
    elif status == "pending":
        if (
            payload.get("result_id") is not None
            or payload.get("result_directory") is not None
            or payload["result_published"] is not False
            or error is not None
        ):
            raise RuntimeIntegrityError("pending finalize 状态字段矛盾")
    elif (
        error is None
        or (
            payload["result_published"] is True
            and (
                not isinstance(payload.get("result_id"), str)
                or not isinstance(payload.get("result_directory"), str)
            )
        )
        or (
            payload["result_published"] is False
            and (
                payload.get("result_id") is not None
                or payload.get("result_directory") is not None
            )
        )
    ):
        raise RuntimeIntegrityError("失败 finalize 状态字段矛盾")
    return payload


__all__ = [
    "FINALIZE_STATUS_VERSION",
    "read_finalize_status",
    "safe_error_summary",
    "write_finalize_status",
]
