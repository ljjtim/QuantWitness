from __future__ import annotations

import pytest

from research_pipeline.extensions import ExtensionError, OperatorDefinition, compile_operator_manifest
from research_pipeline.platform.semantic_governance import (
    APPROVED_BUILTIN_SEMANTIC_IDENTITIES,
    SemanticGovernanceError,
    validate_public_semantic_inventory,
)
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest
from research_pipeline.runtime.operator_promotion import (
    BUILTIN_OPERATOR_IDENTITIES,
    LEGACY_OPERATOR_DISPOSITIONS,
    operator_identity,
    validate_mainline_operator_promotions,
)


def test_public_operator_inventory_is_only_approved_builtins() -> None:
    manifest = build_mainline_operator_manifest()
    assert LEGACY_OPERATOR_DISPOSITIONS == {}
    from research_pipeline.runtime.operator_promotion import (
        DAILY_CASH_LOCAL_APPROVAL, SHARED_FUTURES_LOCAL_APPROVALS,
    )
    assert {operator_identity(item) for item in manifest.definitions} == (
        BUILTIN_OPERATOR_IDENTITIES | {DAILY_CASH_LOCAL_APPROVAL["operator_identity"]}
        | {item["operator_identity"] for item in SHARED_FUTURES_LOCAL_APPROVALS}
    )
    validate_mainline_operator_promotions(manifest)


def test_changed_builtin_definition_requires_review() -> None:
    manifest = build_mainline_operator_manifest()
    original = manifest.definitions[0]
    changed = OperatorDefinition.build(
        operator_spec=original.operator_spec,
        implementation_ref=original.implementation_ref,
        runtime_adapter_ref=original.runtime_adapter_ref,
        cache_profile_ref=original.cache_profile_ref,
        cache_compatibility_mode=original.cache_compatibility_mode,
        resource_hint_ref=f"{original.resource_hint_ref}.changed",
        partition_keys=original.partition_keys,
    )
    with pytest.raises(ExtensionError, match="内建公共算子定义已漂移"):
        validate_mainline_operator_promotions(
            compile_operator_manifest((changed, *manifest.definitions[1:]))
        )


def test_public_semantic_identity_cannot_self_approve() -> None:
    approved = APPROVED_BUILTIN_SEMANTIC_IDENTITIES["metric"]
    expanded = (*approved, "project.specialized_metric@1.0.0")
    with pytest.raises(SemanticGovernanceError, match="缺少逐项批准"):
        validate_public_semantic_inventory(
            "metric", expanded, builtin_identities=expanded
        )


def test_daily_cash_local_admission_cannot_cover_changed_definition() -> None:
    from research_pipeline.extensions import OperatorSpec
    from research_pipeline.runtime.operator_promotion import operator_mainline_governance
    original = build_mainline_operator_manifest().require_operator("finance.simulation.daily-cash", "1.0.0")
    governance = operator_mainline_governance(original)
    assert governance["maximum_state"] == "local_only"
    assert governance["heterogeneous_reuse_confirmed"] is False
    changed = OperatorDefinition.build(
        operator_spec=original.operator_spec, implementation_ref=original.implementation_ref,
        runtime_adapter_ref=original.runtime_adapter_ref, cache_profile_ref=original.cache_profile_ref,
        cache_compatibility_mode=original.cache_compatibility_mode,
        resource_hint_ref=original.resource_hint_ref + ".changed", partition_keys=original.partition_keys,
    )
    with pytest.raises(ExtensionError, match="local_only 人工准入定义已漂移"):
        operator_mainline_governance(changed)
    spec = original.operator_spec
    renamed_spec = OperatorSpec.build(
        operator_id="finance.simulation.another", operator_version=spec.operator_version,
        input_ports=spec.input_ports, output_ports=spec.output_ports, parameters=spec.parameters,
        strategy_roles=spec.strategy_roles, resource_profile=dict(spec.resource_profile),
        determinism_mode=spec.determinism_mode, seed_policy=spec.seed_policy,
        code_hash=spec.code_hash, pit_capabilities=spec.pit_capabilities,
    )
    renamed = OperatorDefinition.build(
        operator_spec=renamed_spec, implementation_ref=original.implementation_ref,
        runtime_adapter_ref=original.runtime_adapter_ref, cache_profile_ref=original.cache_profile_ref,
        cache_compatibility_mode=original.cache_compatibility_mode,
        resource_hint_ref=original.resource_hint_ref, partition_keys=original.partition_keys,
    )
    with pytest.raises(ExtensionError, match="缺少 approved promotion record"):
        operator_mainline_governance(renamed)


def test_daily_cash_identity_covers_cash_financial_semantics() -> None:
    definition = build_mainline_operator_manifest().require_operator("finance.simulation.daily-cash", "1.0.0")
    required = {
        "research_pipeline.simulation.cn_etf",
        "research_pipeline.simulation.corporate_actions",
        "research_pipeline.domain.values",
        "research_pipeline.domain.time",
        "research_pipeline.domain.simulation_result",
    }
    assert required <= set(definition.implementation_ref.dependency_modules)
    assert required <= set(definition.runtime_adapter_ref.dependency_modules)
