"""新主线只读诊断、观测和安全运维。"""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "ArtifactGcError",
    "ArtifactGcPlan",
    "DoctorFinding",
    "DoctorReport",
    "GcCandidate",
    "apply_artifact_gc",
    "plan_artifact_gc",
    "purge_artifact_quarantine",
    "run_research_doctor",
]


_LAZY_EXPORTS = {
    "ArtifactGcError": ".gc",
    "ArtifactGcPlan": ".gc",
    "GcCandidate": ".gc",
    "apply_artifact_gc": ".gc",
    "plan_artifact_gc": ".gc",
    "purge_artifact_quarantine": ".gc",
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
