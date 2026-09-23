"""无版本 ResearchPackage 公共 API。"""

from importlib import import_module

from .constants import OPERATOR_GRAPH_BUILDER_ID

__all__ = [
    "LocalizationDecision", "MetricContract", "OPERATOR_GRAPH_BUILDER_ID",
    "OPERATOR_GRAPH_PACKAGE_VERSION", "OperatorGraphAdmission", "OperatorGraphPlan",
    "PACKAGE_CLAIM_LEVELS",
    "PACKAGE_FILES", "PackageClaimContract",
    "PackageSource", "QueryCompileResult", "RESEARCH_PACKAGE_VERSION", "ResearchPackage", "ResearchPackageError",
    "SOURCE_PROVENANCE_MODES", "SOURCE_PROVENANCE_VERSION", "SourceProvenance",
    "SOURCE_SNAPSHOT_VERSION", "ingest_source_snapshot", "verify_package_source_provenance", "verify_source_snapshot",
    "compare_research_package_results", "compile_research_package", "default_research_package_template",
    "compile_package_queries",
    "initialize_research_package", "load_research_package", "render_research_package_report",
    "registered_package_builders", "RecipeCompileResult", "export_research_package_result", "validate_package_result",
]


_LAZY_EXPORTS = {
    "compile_research_package": ".compilation",
    "registered_package_builders": ".compilation",
    "compile_package_queries": ".compiler",
    "compare_research_package_results": ".delivery",
    "render_research_package_report": ".delivery",
    "export_research_package_result": ".delivery",
    "validate_package_result": ".delivery",
    "LocalizationDecision": ".models",
    "MetricContract": ".models",
    "PACKAGE_CLAIM_LEVELS": ".models",
    "PackageClaimContract": ".models",
    "PackageSource": ".models",
    "RESEARCH_PACKAGE_VERSION": ".models",
    "ResearchPackage": ".models",
    "ResearchPackageError": ".models",
    "SOURCE_PROVENANCE_MODES": ".models",
    "SOURCE_PROVENANCE_VERSION": ".models",
    "SourceProvenance": ".models",
    "OPERATOR_GRAPH_PACKAGE_VERSION": ".operator_graph",
    "OperatorGraphAdmission": ".operator_graph",
    "OperatorGraphPlan": ".plan_contracts",
    "QueryCompileResult": ".plan_contracts",
    "RecipeCompileResult": ".plan_contracts",
    "SOURCE_SNAPSHOT_VERSION": ".source_provenance",
    "ingest_source_snapshot": ".source_provenance",
    "verify_package_source_provenance": ".source_provenance",
    "verify_source_snapshot": ".source_provenance",
    "PACKAGE_FILES": ".store",
    "default_research_package_template": ".store",
    "initialize_research_package": ".store",
    "load_research_package": ".store",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
