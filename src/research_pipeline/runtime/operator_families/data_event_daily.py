"""数据准入与描述性观察算子定义。"""

from __future__ import annotations

from research_pipeline.extensions import OperatorDefinition, ParameterType

from ..operator_definition_factory import _GIB, _MIB, _definition, _parameter


def build_data_event_daily_operator_definitions() -> tuple[OperatorDefinition, ...]:
    return (
        _definition(
            "data.catalog.admission",
            "data.catalog.admission.v1",
            inputs=(),
            outputs=(("admission", "data.catalog-admission.v1"),),
            parameters=(
                _parameter("request_ids", ParameterType.STRING_LIST),
                _parameter("catalog_hash", ParameterType.STRING),
                _parameter("pit_contract_hash", ParameterType.STRING),
                _parameter("drift_proof_hash", ParameterType.STRING),
                _parameter("execution_estimates_hash", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 256 * _MIB,
                "cpu_slots": 1,
                "temp_bytes": 64 * _MIB,
                "wall_seconds": 300,
            },
            code_fingerprint="compiled-catalog-pit-admission-v1",
            capability="data.catalog.admission.v1",
            module_name="research_pipeline.catalog.compiler",
            symbol_name="CatalogPreflight",
            implementation_scope="core",
        ),
        _definition(
            "data.columnar.materialize",
            "data.columnar.materialize.v1",
            inputs=(("admission", "data.catalog-admission.v1"),),
            outputs=(("data", "data.columnar-bundle.v1"),),
            parameters=(),
            resource_profile={
                "memory_bytes": 8 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 32 * _GIB,
                "wall_seconds": 21_600,
            },
            code_fingerprint="columnar-data-plane-snapshot-observation-v2",
            capability="data.columnar.materialize.v1",
            module_name="research_pipeline.data_plane.service",
            symbol_name="materialize_dataset_plan",
            implementation_scope="core",
        ),
        _definition(
            "research.validity.data-observation",
            "research.validity.data-observation.v1",
            inputs=(("data", "data.columnar-bundle.v1"),),
            outputs=(("validity", "research.validity-facts.v1"),),
            parameters=(),
            resource_profile={
                "memory_bytes": 256 * _MIB,
                "cpu_slots": 1,
                "temp_bytes": 256 * _MIB,
                "wall_seconds": 300,
            },
            code_fingerprint="dataset-manifest-observation-validity-v1",
            capability="research.validity-facts.v1",
            module_name="research_pipeline.runtime.operator_graph_evidence",
            symbol_name="build_dataset_observation_validity_facts",
            implementation_scope="core",
            dependency_modules=(
                "research_pipeline.runtime.validity_facts_common",
            ),
            cache_compatibility_mode="byte_exact",
        ),
    )
