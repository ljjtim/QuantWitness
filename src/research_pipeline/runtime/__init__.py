"""可恢复统一研究运行时。"""

from importlib import import_module

from .contracts import ArtifactRef, CheckpointPolicy, DeterminismContext, NodeSpec, PartitionSpec, ResourceBudget, RetryPolicy
from .errors import RuntimeAdmissionError, RuntimeContractError, RuntimeErrorBase, RuntimeGraphError, RuntimeIntegrityError, RuntimeRegistryError, RuntimeStateError, RuntimeWorkerError
from .graph import DagSpec, Edge
from .identity import (
    AUDIT_ENVIRONMENT_VERSION,
    CACHE_COMPATIBILITY_VERSION,
    EXECUTION_IDENTITY_VERSION,
    AuditEnvironmentManifest,
    CacheCompatibilityMode,
    CacheCompatibilityProfile,
    ExecutionIdentity,
    derive_node_execution_id,
    derive_partition_seed,
    derive_run_id,
    environment_fingerprint,
)
from .registry import ImplementationDescriptor, ImplementationRegistry
from .events import RuntimeEvent
from .state import RuntimeProjection, apply_event
from .store import EventStore
from .artifacts import CheckpointExpectation, CheckpointManifest
from .checkpoint import CheckpointStore

__all__ = [
    "AUDIT_ENVIRONMENT_VERSION", "CACHE_COMPATIBILITY_VERSION", "EXECUTION_IDENTITY_VERSION", "EXTERNAL_ARTIFACT_COMMIT_VERSION", "MINUTE_EXECUTION_PROFILE_VERSION", "MINUTE_PARTITION_ALGORITHM_VERSION", "MINUTE_PARTITION_CHECKPOINT_VERSION", "MINUTE_PARTITION_MANIFEST_VERSION", "MINUTE_PARTITION_METRICS_VERSION", "MINUTE_RUNTIME_RESULT_VERSION", "OPERATOR_DAG_RUN_VERSION", "RUNTIME_COMPLETION_METADATA_VERSION",
    "ArtifactRef", "AuditEnvironmentManifest", "CacheCompatibilityMode", "CacheCompatibilityProfile",
    "CheckpointExpectation", "CheckpointManifest", "CheckpointPolicy", "CheckpointStore", "DagSpec", "DeterminismContext", "Edge", "EventStore", "ExecutionIdentity", "ExternalArtifactCommit", "ExternalArtifactStore",
    "ExecutionMode", "ImplementationDescriptor", "ImplementationRegistry", "MemoryGovernor", "NodeSpec", "PartitionSpec", "ProjectFairQueue", "ProjectReadyCandidate", "ReadyCandidate", "ReservationLedger", "ResourceCapacity", "ResourceToken",
    "RESOURCE_CALIBRATION_VERSION", "RESOURCE_GOVERNOR_VERSION", "RESOURCE_OBSERVATION_VERSION", "ResourceGovernor", "ResourceGovernorConfig", "ResourceLease", "ResourceObservation", "ResourceUsageSampler", "ResourceVector",
    "PROJECT_RUNTIME_IDENTITY_VERSION", "ProjectRuntimeIdentity", "ProjectRuntimeInput",
    "ResourceBudget", "RetryPolicy", "RuntimeAdmissionError", "RuntimeContractError",
    "RuntimeErrorBase", "RuntimeEvent", "RuntimeGraphError", "RuntimeIntegrityError", "RuntimeProjection", "RuntimeRegistryError",
    "RuntimeStateError", "RuntimeWorkerError", "RecoveryPlan", "apply_event", "derive_node_execution_id",
    "RuntimeCompletionMetadata", "RuntimeExecutionService", "RuntimeNodeContext", "RuntimeNodeOutputs", "RuntimeNodeValue",
    "MinuteExecutionIdentity", "MinutePartitionExecutionResult", "MinutePartitionKey", "MinutePartitionManifest", "MinutePartitionMetrics", "MinutePartitionRuntime", "MinutePartitionSpec", "MinutePartitionWorkResult", "MinuteRuntimeBudget", "MinuteRuntimeBudgetError", "compile_minute_partition_manifest", "stable_minute_instrument_bucket",
    "derive_partition_seed", "derive_run_id",
    "environment_fingerprint",
    "plan_rerun_from", "plan_resume", "plan_retry_node",
    "stable_partition_keys", "stable_partition_merge",
    "build_production_registry", "compile_production_dag", "project_runtime_identity",
]


_LAZY_EXPORTS = {
    "EXTERNAL_ARTIFACT_COMMIT_VERSION": ".external_artifact",
    "ExternalArtifactCommit": ".external_artifact",
    "ExternalArtifactStore": ".external_artifact",
    "MINUTE_EXECUTION_PROFILE_VERSION": ".minute_profile",
    "MINUTE_PARTITION_ALGORITHM_VERSION": ".minute_profile",
    "MINUTE_PARTITION_CHECKPOINT_VERSION": ".minute_profile",
    "MINUTE_PARTITION_MANIFEST_VERSION": ".minute_profile",
    "MINUTE_PARTITION_METRICS_VERSION": ".minute_profile",
    "MINUTE_RUNTIME_RESULT_VERSION": ".minute_profile",
    "MinuteExecutionIdentity": ".minute_profile",
    "MinutePartitionExecutionResult": ".minute_profile",
    "MinutePartitionKey": ".minute_profile",
    "MinutePartitionManifest": ".minute_profile",
    "MinutePartitionMetrics": ".minute_profile",
    "MinutePartitionRuntime": ".minute_profile",
    "MinutePartitionSpec": ".minute_profile",
    "MinutePartitionWorkResult": ".minute_profile",
    "MinuteRuntimeBudget": ".minute_profile",
    "MinuteRuntimeBudgetError": ".minute_profile",
    "compile_minute_partition_manifest": ".minute_profile",
    "stable_minute_instrument_bucket": ".minute_profile",
    "OPERATOR_DAG_RUN_VERSION": ".execution_service",
    "RuntimeExecutionService": ".execution_service",
    "RuntimeNodeContext": ".execution_service",
    "RuntimeNodeOutputs": ".execution_service",
    "RuntimeNodeValue": ".execution_service",
    "RUNTIME_COMPLETION_METADATA_VERSION": ".operator_runtime",
    "RuntimeCompletionMetadata": ".operator_runtime",
    "RecoveryPlan": ".recovery",
    "plan_rerun_from": ".recovery",
    "plan_resume": ".recovery",
    "plan_retry_node": ".recovery",
    "ExecutionMode": ".scheduler",
    "MemoryGovernor": ".scheduler",
    "ProjectFairQueue": ".scheduler",
    "ProjectReadyCandidate": ".scheduler",
    "ReadyCandidate": ".scheduler",
    "ReservationLedger": ".scheduler",
    "ResourceCapacity": ".scheduler",
    "ResourceToken": ".scheduler",
    "stable_partition_keys": ".scheduler",
    "stable_partition_merge": ".scheduler",
    "RESOURCE_CALIBRATION_VERSION": ".resource_governor",
    "RESOURCE_GOVERNOR_VERSION": ".resource_governor",
    "RESOURCE_OBSERVATION_VERSION": ".resource_governor",
    "ResourceGovernor": ".resource_governor",
    "ResourceGovernorConfig": ".resource_governor",
    "ResourceLease": ".resource_governor",
    "ResourceObservation": ".resource_governor",
    "ResourceUsageSampler": ".resource_governor",
    "ResourceVector": ".resource_governor",
    "build_production_registry": ".compiler",
    "compile_production_dag": ".compiler",
    "PROJECT_RUNTIME_IDENTITY_VERSION": ".project_operator_runtime",
    "ProjectRuntimeIdentity": ".project_operator_runtime",
    "ProjectRuntimeInput": ".project_operator_runtime",
    "project_runtime_identity": ".project_operator_runtime",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
