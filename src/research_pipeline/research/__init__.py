"""新研究主链的纯研究算法。"""

from importlib import import_module


__all__ = [
    "ESTIMAND_SPEC_VERSION",
    "FEATURE_SET_ARTIFACT_VERSION",
    "HYPOTHESIS_SPEC_VERSION",
    "LABEL_ARTIFACT_VERSION",
    "RESEARCH_SEMANTICS_VERSION",
    "RESEARCH_TIME_WINDOW_VERSION",
    "EstimandSpec",
    "FeatureSetArtifact",
    "HypothesisSpec",
    "LabelArtifact",
    "ResearchSemantics",
    "ResearchSemanticsError",
    "ResearchTimeWindow",
    "assign_quantile_buckets",
    "portfolio_metric_values",
    "select_top_n_codes",
    "stable_score_order",
]


_LAZY_EXPORTS = {
    "portfolio_metric_values": ".portfolio_metrics",
    "assign_quantile_buckets": ".ranking",
    "select_top_n_codes": ".ranking",
    "stable_score_order": ".ranking",
    "ESTIMAND_SPEC_VERSION": ".semantics",
    "FEATURE_SET_ARTIFACT_VERSION": ".semantics",
    "HYPOTHESIS_SPEC_VERSION": ".semantics",
    "LABEL_ARTIFACT_VERSION": ".semantics",
    "RESEARCH_SEMANTICS_VERSION": ".semantics",
    "RESEARCH_TIME_WINDOW_VERSION": ".semantics",
    "EstimandSpec": ".semantics",
    "FeatureSetArtifact": ".semantics",
    "HypothesisSpec": ".semantics",
    "LabelArtifact": ".semantics",
    "ResearchSemantics": ".semantics",
    "ResearchSemanticsError": ".semantics",
    "ResearchTimeWindow": ".semantics",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
