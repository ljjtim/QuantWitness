"""依据已知启动方式和失败载荷提供预算建议，不执行探测或调整额度。"""

from __future__ import annotations

from collections.abc import Mapping
import sys


def process_budget_advice(declared_slots: int, *, purpose: str) -> dict[str, object]:
    """投影当前解释器的已知进程开销；建议不代表项目代码的进程上限。"""

    parameter = (
        "--verification-process-slots"
        if purpose == "verifier"
        else "OperatorDefinition.resource_profile.process_slots"
    )
    advice: dict[str, object] = {
        "purpose": purpose,
        "declared_slots": declared_slots,
        "suggested_slots": None,
        "parameter": parameter,
        "sources": [],
    }
    if purpose not in {"worker", "verifier"}:
        return {**advice, "status": "invalid_purpose", "message": "用途须为 worker 或 verifier。"}
    if type(declared_slots) is not int or declared_slots <= 0:
        return {
            **advice, "status": "invalid_declared_slots",
            "message": "进程槽须为正整数，请检查声明。",
        }
    sources = ["supervisor", "worker"]
    if sys.platform == "win32":
        if sys.prefix != sys.base_prefix:
            sources.append("windows_venv_launcher")
        if purpose == "worker" and sys.version_info[:2] == (3, 10):
            sources.append("windows_python310_version_query")
    startup_slots = len(sources)
    insufficient = declared_slots < startup_slots
    return {
        **advice,
        "status": "configuration_insufficient" if insufficient else "startup_budget_sufficient",
        "suggested_slots": max(declared_slots, startup_slots),
        "startup_slots": startup_slots,
        "sources": sources,
        "message": (
            f"已声明 {declared_slots} 个进程槽，已知启动方式建议至少 {startup_slots} 个。"
            if insufficient else f"已声明 {declared_slots} 个进程槽，可覆盖已知启动开销。"
        ) + "项目自行启动的进程仍需计入；建议不是任意项目的硬上限，最终以运行时测量为准。",
    }


def resource_failure_advice(
    failure_payload: Mapping[str, object],
    *,
    error_code: str | None = None,
    purpose: str = "worker",
) -> dict[str, object] | None:
    """保留实测超限的 actual/limit；测量失败和未给出数值的错误不推算峰值。"""

    code = error_code or str(failure_payload.get("code", ""))
    measurement = failure_payload.get("resource_measurement_status")
    exceeded = failure_payload.get("exceeded")
    dimensions: dict[str, object] = {}
    if isinstance(exceeded, Mapping):
        for name, values in exceeded.items():
            if not isinstance(values, Mapping):
                continue
            actual, limit = values.get("actual"), values.get("limit")
            if isinstance(actual, (int, float)) and isinstance(limit, (int, float)) and actual > limit:
                dimensions[str(name)] = {"actual": actual, "limit": limit, "suggested_limit": actual}
    if measurement == "measurement_unavailable" or "measurement_unavailable" in code:
        status = "measurement_unavailable"
        message = "资源测量不可用；检查测量失败原因，不能据此推算所需额度。"
    elif dimensions:
        status = "measured_exceeded"
        message = "资源实际用量超过已声明额度；建议按实测值检查对应预算，重跑仍以运行时测量为准。"
    elif code.endswith("process_slots_exceeded"):
        status = "process_slots_exceeded"
        message = "进程槽超限，但失败载荷未提供 actual/limit；请核对进程树与声明，不推算实测峰值。"
    else:
        return None
    parameter = (
        "--verification-process-slots" if purpose == "verifier"
        else "OperatorDefinition.resource_profile.process_slots / --resource-process-slots"
    )
    if dimensions and "process_slots" not in dimensions:
        parameter = (
            "--verification-memory-bytes / --verification-temp-bytes"
            if purpose == "verifier" else "OperatorDefinition.resource_profile / --resource-memory-bytes / --resource-scratch-bytes"
        )
    return {
        "purpose": purpose,
        "status": status,
        "source": "failure_payload",
        "error_code": code or None,
        "exceeded": dimensions,
        "resource_measurement_status": measurement,
        "process_cleanup_status": failure_payload.get("process_cleanup_status"),
        "parameter": parameter,
        "message": message,
    }
