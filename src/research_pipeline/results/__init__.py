"""集中、不可变且可验证的研究结果合同。"""

from .compiler import ResultOperatorResolver, compile_result_spec
from .contracts import (
    BAR_TCA_SCHEMA_IDS,
    CANONICAL_SIMULATION_SCHEMA_IDS,
    MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS,
    RESULT_BUNDLE_VERSION,
    RESULT_INPUT_REVISION_VERSION,
    RESULT_REF_VERSION,
    RESULT_RUN_SUMMARY_VERSION,
    RESULT_SPEC_VERSION,
    RESULT_SUPPORT_FILE_VERSION,
    RESULT_TABLE_MANIFEST_VERSION,
    RESULT_VERIFICATION_CLOSURE_VERSION,
    ResultBundle,
    ResultInputRevision,
    ResultReference,
    ResultRunSummary,
    ResultSpec,
    ResultSupportFile,
    ResultTableManifest,
    ResultTableSpec,
    ResultVerificationClosure,
)
from .errors import ResultContractError


def __getattr__(name: str) -> object:
    """延迟加载依赖 Runtime/Data Plane 的发布实现，保持纯合同可独立导入。"""
    if name in {"RESULT_REFERENCE_FILE", "ResultAssembler"}:
        from .assembler import RESULT_REFERENCE_FILE, ResultAssembler

        values = {
            "RESULT_REFERENCE_FILE": RESULT_REFERENCE_FILE,
            "ResultAssembler": ResultAssembler,
        }
    elif name in {"RESULT_COMMITTED_NAME", "RESULT_MANIFEST_NAME", "ResultSnapshot", "ResultStore"}:
        from .store import RESULT_COMMITTED_NAME, RESULT_MANIFEST_NAME, ResultSnapshot, ResultStore

        values = {
            "RESULT_COMMITTED_NAME": RESULT_COMMITTED_NAME,
            "RESULT_MANIFEST_NAME": RESULT_MANIFEST_NAME,
            "ResultSnapshot": ResultSnapshot,
            "ResultStore": ResultStore,
        }
    elif name in {"ResultMetric", "load_result_metrics", "metrics_from_snapshot"}:
        from .metrics import ResultMetric, load_result_metrics, metrics_from_snapshot

        values = {
            "ResultMetric": ResultMetric,
            "load_result_metrics": load_result_metrics,
            "metrics_from_snapshot": metrics_from_snapshot,
        }
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals().update(values)
    return values[name]

__all__ = [
    "BAR_TCA_SCHEMA_IDS", "CANONICAL_SIMULATION_SCHEMA_IDS",
    "MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS",
    "RESULT_BUNDLE_VERSION",
    "RESULT_COMMITTED_NAME", "RESULT_INPUT_REVISION_VERSION",
    "RESULT_MANIFEST_NAME", "RESULT_REFERENCE_FILE", "RESULT_REF_VERSION",
    "RESULT_RUN_SUMMARY_VERSION",
    "RESULT_SPEC_VERSION", "RESULT_SUPPORT_FILE_VERSION", "RESULT_TABLE_MANIFEST_VERSION", "ResultAssembler", "ResultBundle",
    "ResultContractError", "ResultInputRevision", "ResultOperatorResolver",
    "RESULT_VERIFICATION_CLOSURE_VERSION", "ResultReference", "ResultRunSummary",
    "ResultSnapshot", "ResultSpec", "ResultStore", "ResultSupportFile",
    "ResultTableManifest", "ResultTableSpec", "ResultVerificationClosure",
    "compile_result_spec", "load_result_metrics", "metrics_from_snapshot", "ResultMetric",
]
