"""新主线只读诊断、观测和安全运维。"""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "METADATA_FIELDS",
    "OBSERVABILITY_VERSION",
    "ArtifactGcError",
    "ArtifactGcPlan",
    "DoctorFinding",
    "DoctorReport",
    "GcCandidate",
    "ObservabilityIndexRecord",
    "StructuredLogRecord",
    "apply_artifact_gc",
    "build_structured_log",
    "plan_artifact_gc",
    "purge_artifact_quarantine",
    "rebuild_observability_index",
    "redact_text",
    "run_research_doctor",
    "safe_metadata",
]


_LAZY_EXPORTS = {
    "ArtifactGcError": ".gc",
    "ArtifactGcPlan": ".gc",
    "GcCandidate": ".gc",
    "apply_artifact_gc": ".gc",
    "plan_artifact_gc": ".gc",
    "purge_artifact_quarantine": ".gc",
    "METADATA_FIELDS": ".observability",
    "OBSERVABILITY_VERSION": ".observability",
    "ObservabilityIndexRecord": ".observability",
    "StructuredLogRecord": ".observability",
    "build_structured_log": ".observability",
    "rebuild_observability_index": ".observability",
    "redact_text": ".observability",
    "safe_metadata": ".observability",
    "DoctorFinding": ".doctor",
    "DoctorReport": ".doctor",
    "run_research_doctor": ".doctor",
}


def __getattr__(name: str):
    """只在真实消费者访问公共入口时加载对应运维实现。"""
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
