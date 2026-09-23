"""新主链可编译数据目录公共 API。"""

from .errors import CatalogDriftError, CatalogError, CatalogFinancialSemanticsError, CatalogParseError, CatalogReferenceError
from .discovery import (
    DriftAttestation,
    DriftFinding,
    DuckDBSourceInspector,
    ParquetSourceInspector,
    PhysicalColumn,
    PhysicalInventory,
    evaluate_current_drift,
    require_approval,
)
from .declarative import DeclarativeCatalog, load_declarative_catalog, load_default_declarative_catalog
from .models import (
    ApprovalDecision,
    CATALOG_CONTRACT_VERSION,
    CatalogContract,
    CatalogCoverageBaseline,
    CatalogSourceManifest,
    DatasetContract,
    FieldContract,
    FieldFamilySource,
    PhysicalBindingContract,
    PolicyContract,
    TransformContract,
)
from .minute import (
    ADJUSTMENT_FACTOR_SNAPSHOT_VERSION,
    MINUTE_DATASET_SEMANTICS_VERSION,
    MINUTE_SOURCE_SEMANTICS_VERSION,
    AdjustmentDecisionAnchor,
    AdjustmentFactorSegment,
    AdjustmentFactorSnapshot,
    MinuteDatasetSemantics,
    MinuteSourceSemantics,
    register_minute_policy_validators,
)
from .families import expand_field_family
from .finance import require_financial_observation_visible
from .docs import render_catalog_docs
from .compiler import COMPILER_VERSION, LOCK_FORMAT_VERSION, CatalogPreflight, CompiledCatalog, compile_catalog
from .schema import catalog_schema_bundle, catalog_source_schema
from .source_loader import load_contract_file, load_contract_payload
from .validation import validate_contract_set
from .transforms import TransformDescriptor, build_transform_manifest, code_hash_for_transform, describe_transform, validate_transform_manifest, verify_transform_implementation

register_minute_policy_validators()

__all__ = [
    "ApprovalDecision",
    "ADJUSTMENT_FACTOR_SNAPSHOT_VERSION",
    "CATALOG_CONTRACT_VERSION",
    "CatalogContract",
    "CatalogPreflight",
    "CompiledCatalog",
    "COMPILER_VERSION",
    "CatalogCoverageBaseline",
    "CatalogDriftError",
    "CatalogError",
    "CatalogFinancialSemanticsError",
    "CatalogParseError",
    "CatalogReferenceError",
    "CatalogSourceManifest",
    "DatasetContract",
    "DeclarativeCatalog",
    "DriftAttestation",
    "DriftFinding",
    "DuckDBSourceInspector",
    "FieldContract",
    "FieldFamilySource",
    "LOCK_FORMAT_VERSION",
    "MINUTE_DATASET_SEMANTICS_VERSION",
    "MINUTE_SOURCE_SEMANTICS_VERSION",
    "MinuteDatasetSemantics",
    "MinuteSourceSemantics",
    "AdjustmentDecisionAnchor",
    "AdjustmentFactorSegment",
    "AdjustmentFactorSnapshot",
    "PhysicalBindingContract",
    "PhysicalColumn",
    "PhysicalInventory",
    "ParquetSourceInspector",
    "PolicyContract",
    "TransformContract",
    "TransformDescriptor",
    "build_transform_manifest",
    "catalog_schema_bundle",
    "catalog_source_schema",
    "code_hash_for_transform",
    "compile_catalog",
    "describe_transform",
    "expand_field_family",
    "load_contract_file",
    "load_contract_payload",
    "load_declarative_catalog",
    "load_default_declarative_catalog",
    "evaluate_current_drift",
    "require_approval",
    "require_financial_observation_visible",
    "render_catalog_docs",
    "validate_contract_set",
    "validate_transform_manifest",
    "verify_transform_implementation",
]
