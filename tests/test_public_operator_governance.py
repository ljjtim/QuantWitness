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
    assert {operator_identity(item) for item in manifest.definitions} == BUILTIN_OPERATOR_IDENTITIES
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
