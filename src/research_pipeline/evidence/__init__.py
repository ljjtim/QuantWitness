"""当前 Result/VerificationResult 主线的公共合同。"""

from importlib import import_module

__all__ = [
    "CLAIM_LEVELS",
    "weakest_claim_level",
    "FACET_CONTRACT_VERSION",
    "GATE_STATUSES",
    "VALIDITY_CONTRACT_VERSION",
    "VALIDITY_FACTS_ARTIFACT_TYPE",
    "VALIDITY_FACTS_VERSION",
    "VALIDITY_GATE_IDS",
    "VALIDITY_GATE_REASON_CODES",
    "VALIDITY_REASON_MESSAGES",
    "VERIFICATION_RESULT_VERSION",
    "ArtifactIntegrityFacet",
    "ClaimAssessment",
    "ClaimFacet",
    "ClaimPolicy",
    "EvidenceContractError",
    "FinancialOracleBudget",
    "GateRecomputeRecord",
    "ResearchValidityFacet",
    "ReproducibilityFacet",
    "ValidityFinding",
    "ValidityGateResult",
    "VerificationComparison",
    "VerificationIssue",
    "VerificationReportModel",
    "VerificationResult",
    "VerifiedResultContext",
    "assess_claim_policy",
    "build_verification_report",
    "compare_verification_results",
    "load_verified_result_context",
    "render_verification_report",
    "export_verified_result",
    "verify_result",
    "write_verification_result",
]


_LAZY_EXPORTS = {
    "ClaimFacet": ".claims",
    "EvidenceContractError": ".errors",
    "FinancialOracleBudget": ".result_financial_oracle",
    "CLAIM_LEVELS": ".facets",
    "weakest_claim_level": ".facets",
    "FACET_CONTRACT_VERSION": ".facets",
    "ArtifactIntegrityFacet": ".facets",
    "ResearchValidityFacet": ".facets",
    "ReproducibilityFacet": ".facets",
    "VerificationIssue": ".facets",
    "GATE_STATUSES": ".validity",
    "VALIDITY_CONTRACT_VERSION": ".validity",
    "VALIDITY_GATE_IDS": ".validity",
    "VALIDITY_GATE_REASON_CODES": ".validity",
    "VALIDITY_REASON_MESSAGES": ".validity",
    "ClaimAssessment": ".validity",
    "ClaimPolicy": ".validity",
    "ValidityFinding": ".validity",
    "ValidityGateResult": ".validity",
    "assess_claim_policy": ".validity",
    "VALIDITY_FACTS_ARTIFACT_TYPE": ".validity_recompute",
    "VALIDITY_FACTS_VERSION": ".validity_recompute",
    "GateRecomputeRecord": ".validity_recompute",
    "VERIFICATION_RESULT_VERSION": ".verification_result",
    "VerificationComparison": ".verification_result",
    "VerificationReportModel": ".verification_result",
    "VerificationResult": ".verification_result",
    "VerifiedResultContext": ".verification_result",
    "build_verification_report": ".verification_result",
    "compare_verification_results": ".verification_result",
    "load_verified_result_context": ".verification_result",
    "render_verification_report": ".verification_result",
    "export_verified_result": ".verification_result",
    "verify_result": ".verification_result",
    "write_verification_result": ".verification_result",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
