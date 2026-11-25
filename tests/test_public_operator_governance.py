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
    operator_identity,
    validate_mainline_operator_promotions,
)


def test_public_operator_inventory_is_only_approved_builtins() -> None:
    manifest = build_mainline_operator_manifest()
    assert {operator_identity(item) for item in manifest.definitions} == BUILTIN_OPERATOR_IDENTITIES
    validate_mainline_operator_promotions(manifest)


def test_duckdb_date_pushdown_is_in_executing_operator_closures() -> None:
    definitions = build_mainline_operator_manifest().definitions
    module = "research_pipeline.data_plane.providers.epoch_date"
    assert {
        item.operator_spec.operator_id
        for item in definitions
        if module in item.runtime_adapter_ref.dependency_modules
    } == {"data.columnar.materialize", "data.minute.adjustment_snapshot", "research.bars.minute_adjust"}
    materialize = next(
        item for item in definitions if item.operator_spec.operator_id == "data.columnar.materialize"
    )
    assert module in materialize.implementation_ref.dependency_modules


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
    assert changed.definition_hash != original.definition_hash
    recompiled = compile_operator_manifest((changed, *manifest.definitions[1:]))
    assert recompiled.manifest_hash != manifest.manifest_hash


def test_public_semantic_identity_cannot_self_approve() -> None:
    approved = APPROVED_BUILTIN_SEMANTIC_IDENTITIES["metric"]
    expanded = (*approved, "project.specialized_metric@1.0.0")
    with pytest.raises(SemanticGovernanceError, match="缺少逐项批准"):
        validate_public_semantic_inventory(
            "metric", expanded, builtin_identities=expanded
        )
