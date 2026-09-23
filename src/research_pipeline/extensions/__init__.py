"""受控扩展清单公共 API。"""

from .contracts import EXTENSION_CONTRACT_VERSION, DeterminismPolicy, ExtensionDescriptor, ExtensionKind, ResourceProfile
from .errors import ExtensionError
from .manifest import EXTENSION_MANIFEST_VERSION, CompiledExtensionManifest, compile_extension_manifest
from .registry import ControlledExtensionRegistry, RegisteredExtension
from .operators import (
    CompiledOperatorManifest,
    OperatorDefinition,
    OperatorImplementationRef,
    RegisteredOperatorBinding,
    TrustedOperatorRegistry,
    compile_operator_manifest,
)
from .projections import ExtensionProjection, project_extensions
from .project_bundle import (
    PROJECT_OPERATOR_ABI_VERSION,
    PROJECT_OPERATOR_BUNDLE_VERSION,
    PROJECT_OPERATOR_DECLARATION_VERSION,
    ProjectArtifactCommit,
    ProjectDirectoryCommit,
    ProjectArtifactInput,
    ProjectOperatorBundleManifest,
    ProjectOperatorContext,
    ProjectOperatorDeclaration,
    ProjectOperatorPermissionProfile,
    compile_project_operator_bundle,
    load_project_operator_declaration,
    project_source_hash,
    verify_project_operator_bundle,
)
from .project_admission import (
    AdmittedProjectOperatorRegistry,
    ProjectOperatorImplementationToken,
    build_admitted_operator_registry,
)
from .verifier_bundle import (
    PROJECT_VERIFIER_ABI_VERSION,
    PROJECT_VERIFIER_BUNDLE_VERSION,
    AdmittedProjectVerifier,
    ProjectVerifierBundleManifest,
    admit_project_verifier_bundle,
    compile_project_verifier_bundle,
    verify_project_verifier_bundle,
)
from research_pipeline.platform.operator_contracts import (
    OPERATOR_CONTRACT_VERSION,
    OPERATOR_GRAPH_ADMISSION_VERSION,
    OPERATOR_GRAPH_RECIPE_VERSION,
    STRATEGY_CONTRACT_VERSION,
    AdmittedOperatorGraph,
    InputBinding,
    OperatorContractError,
    OperatorGraphRecipe,
    OperatorNodeRecipe,
    OperatorSpec,
    ParameterSpec,
    ParameterType,
    PortSpec,
    StrategyRole,
    StrategySelection,
    StrategySpec,
)

__all__ = [
    "EXTENSION_CONTRACT_VERSION", "EXTENSION_MANIFEST_VERSION", "OPERATOR_CONTRACT_VERSION",
    "OPERATOR_GRAPH_ADMISSION_VERSION", "OPERATOR_GRAPH_RECIPE_VERSION", "STRATEGY_CONTRACT_VERSION",
    "PROJECT_OPERATOR_ABI_VERSION", "PROJECT_OPERATOR_BUNDLE_VERSION",
    "PROJECT_OPERATOR_DECLARATION_VERSION",
    "AdmittedOperatorGraph", "AdmittedProjectOperatorRegistry", "CompiledExtensionManifest", "CompiledOperatorManifest", "ControlledExtensionRegistry",
    "DeterminismPolicy", "ExtensionDescriptor", "ExtensionError", "ExtensionKind",
    "ExtensionProjection", "InputBinding", "OperatorContractError", "OperatorDefinition", "OperatorGraphRecipe",
    "OperatorImplementationRef",
    "OperatorNodeRecipe", "OperatorSpec", "ParameterSpec", "ParameterType", "PortSpec",
    "ProjectArtifactCommit", "ProjectArtifactInput", "ProjectDirectoryCommit", "ProjectOperatorBundleManifest",
    "PROJECT_VERIFIER_ABI_VERSION", "PROJECT_VERIFIER_BUNDLE_VERSION",
    "AdmittedProjectVerifier", "ProjectVerifierBundleManifest",
    "admit_project_verifier_bundle", "compile_project_verifier_bundle",
    "verify_project_verifier_bundle",
    "ProjectOperatorContext", "ProjectOperatorDeclaration", "ProjectOperatorImplementationToken", "ProjectOperatorPermissionProfile",
    "RegisteredExtension", "RegisteredOperatorBinding", "ResourceProfile", "StrategyRole",
    "StrategySelection", "StrategySpec", "TrustedOperatorRegistry",
    "build_admitted_operator_registry", "compile_extension_manifest", "compile_operator_manifest", "compile_project_operator_bundle", "project_extensions",
    "load_project_operator_declaration", "project_source_hash", "verify_project_operator_bundle",
]
