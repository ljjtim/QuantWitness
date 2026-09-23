"""可编译数据目录的稳定错误分类。"""

from research_pipeline.platform.errors import MainlineError


class CatalogError(MainlineError):
    error_code = "catalog_invalid"


class CatalogParseError(CatalogError):
    error_code = "catalog_parse_invalid"


class CatalogReferenceError(CatalogError):
    error_code = "catalog_reference_invalid"


class CatalogFinancialSemanticsError(CatalogError):
    error_code = "catalog_financial_semantics_invalid"


class CatalogDriftError(CatalogError):
    error_code = "catalog_schema_drift"


__all__ = [
    "CatalogDriftError",
    "CatalogError",
    "CatalogFinancialSemanticsError",
    "CatalogParseError",
    "CatalogReferenceError",
]
