"""公共 OperatorDefinition 的内建、待迁出与正式晋级门禁。"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Iterable, Mapping

from research_pipeline.extensions import (
    CompiledOperatorManifest,
    ExtensionError,
    OperatorDefinition,
)
from research_pipeline.extensions.governance import (
    OperatorPromotionReview,
    validate_framework_implementation_boundary,
)
from research_pipeline.platform.semantic_governance import (
    MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    validate_public_semantic_inventory,
)


BUILTIN_CLASSIFICATION = "builtin"
LEGACY_EXIT_CLASSIFICATION = "legacy_exit"
PROMOTED_CLASSIFICATION = "promoted"
OperatorIdentity = tuple[str, str, str, str]
BUILTIN_ARTIFACT_TYPE_IDENTITIES = frozenset({
    "data.adjustment-factor-snapshot.v1",
    "data.catalog-admission.v1",
    "data.columnar-bundle.v1",
    "data.minute-bars.1m.v1",
    "data.minute-bars.v1",
    "research.feature-set.v1",
    "research.label.v1",
    "research.minute-features.v1",
    "research.minute-labels.v1",
    "research.minute-observation.v1",
    "research.minute-signals.v1",
    "research.minute-simulation.v1",
    "research.minute-statistics.v1",
    "research.minute-targets.v1",
    "research.model-fits.v1",
    "research.model-fold-metrics.v1",
    "research.model-locked-holdout.v1",
    "research.model-preprocessed-folds.v1",
    "research.model-selection.v1",
    "research.model-split-manifest.v1",
    "research.model-validation-predictions.v1",
    "research.validity-facts.v1",
})


# 公共定义集合经审查固定；有意修改实现需同步复核本常量。
BUILTIN_OPERATOR_DEFINITION_SET_HASH = (
    "fd7b40753676a7f3fa767ef421452f962886e787365f3a6e5b032843e0694568"
)


@dataclass(frozen=True)
class LegacyOperatorDisposition:
    remediation_item: str
    target: str


# 该清单固定 00A 验收后的既有公共合同，不从 manifest 或 baseline 自动生成。
BUILTIN_OPERATOR_IDENTITIES: frozenset[OperatorIdentity] = frozenset(
    {
        (
            "data.catalog.admission",
            "1.0.0",
            "data.catalog.admission.v1",
            "research_pipeline.catalog.compiler",
        ),
        (
            "data.columnar.materialize",
            "1.0.0",
            "data.columnar.materialize.v1",
            "research_pipeline.data_plane.service",
        ),
        (
            "data.minute.adjustment_snapshot",
            "1.0.0",
            "data.minute.adjustment_snapshot.v1",
            "research_pipeline.data_plane.minute_adjustment",
        ),
        (
            "data.minute.scan",
            "1.0.0",
            "data.minute.scan.v1",
            "research_pipeline.data_plane.minute_scan",
        ),
        (
            "finance.simulation.intraday",
            "3.0.0",
            "finance.simulation.intraday.v3",
            "research_pipeline.simulation.minute_execution",
        ),
        (
            "research.bars.minute_adjust",
            "1.0.0",
            "research.bars.minute_adjust.v1",
            "research_pipeline.data_plane.minute_adjustment",
        ),
        (
            "research.bars.minute_resample",
            "1.0.0",
            "research.bars.minute_resample.v1",
            "research_pipeline.data_plane.minute_resampling",
        ),
        (
            "research.features.intraday",
            "1.0.0",
            "research.features.intraday.v1",
            "research_pipeline.research.minute_operators",
        ),
        (
            "research.labels.intraday",
            "1.0.0",
            "research.labels.intraday.v1",
            "research_pipeline.research.minute_operators",
        ),
        (
            "research.model.fit",
            "1.0.0",
            "research.model.fit.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.model.fold-metrics",
            "1.0.0",
            "research.model.fold-metrics.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.model.locked-holdout",
            "1.0.0",
            "research.model.locked-holdout.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.model.predict",
            "1.0.0",
            "research.model.predict.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.model.preprocess-fit",
            "1.0.0",
            "research.model.preprocess-fit.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.model.selection",
            "1.0.0",
            "research.model.selection.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.model.split-manifest",
            "1.0.0",
            "research.model.split-manifest.v1",
            "research_pipeline.runtime.walk_forward_model_execution",
        ),
        (
            "research.observation.minute-bars",
            "1.0.0",
            "research.observation.minute-bars.v1",
            "research_pipeline.runtime.adapters.minute_data",
        ),
        (
            "research.signals.intraday",
            "1.0.0",
            "research.signals.intraday.v1",
            "research_pipeline.research.minute_operators",
        ),
        (
            "research.statistics.minute",
            "1.0.0",
            "research.statistics.minute.v1",
            "research_pipeline.research.statistics.minute_profile",
        ),
        (
            "research.targets.intraday",
            "1.0.0",
            "research.targets.intraday.v1",
            "research_pipeline.runtime.adapters.minute_research",
        ),
        (
            "research.validity.adjusted-minute-observation",
            "1.0.0",
            "research.validity.adjusted-minute-observation.v1",
            "research_pipeline.runtime.adapters.minute_data",
        ),
        (
            "research.validity.data-observation",
            "1.0.0",
            "research.validity.data-observation.v1",
            "research_pipeline.runtime.operator_graph_evidence",
        ),
        (
            "research.validity.minute",
            "1.0.0",
            "research.validity.minute.v1",
            "research_pipeline.runtime.operator_graph_evidence",
        ),
        (
            "research.validity.minute-observation",
            "1.0.0",
            "research.validity.minute-observation.v1",
            "research_pipeline.runtime.adapters.minute_data",
        ),
    }
)

LEGACY_OPERATOR_DISPOSITIONS: Mapping[OperatorIdentity, LegacyOperatorDisposition] = (
    MappingProxyType({})
)

LEGACY_OPERATOR_DEFINITION_HASHES: Mapping[OperatorIdentity, str] = MappingProxyType(
    {}
)

# 新增公共业务算子只能在这里持有已批准的 OperatorPromotionReview；当前没有。
MAINLINE_OPERATOR_PROMOTION_REVIEWS: tuple[OperatorPromotionReview, ...] = ()


def operator_identity(definition: OperatorDefinition) -> OperatorIdentity:
    reference = definition.implementation_ref
    return (
        definition.name,
        definition.version,
        reference.implementation_id,
        reference.module_name,
    )


def operator_mainline_governance(
    definition: OperatorDefinition,
    *,
    reviews: Iterable[OperatorPromotionReview] = MAINLINE_OPERATOR_PROMOTION_REVIEWS,
) -> dict[str, object]:
    identity = operator_identity(definition)
    if identity in BUILTIN_OPERATOR_IDENTITIES:
        return {
            "mainline_classification": BUILTIN_CLASSIFICATION,
            "promotion_status": "not_required",
        }
    disposition = LEGACY_OPERATOR_DISPOSITIONS.get(identity)
    if disposition is not None:
        return {
            "mainline_classification": LEGACY_EXIT_CLASSIFICATION,
            "promotion_status": "pending_removal",
            "remediation_item": disposition.remediation_item,
            "target": disposition.target,
        }
    matches = tuple(
        review
        for review in reviews
        if (review.operator_id, review.operator_version)
        == (definition.name, definition.version)
    )
    if len(matches) != 1:
        if matches:
            raise ExtensionError(
                f"公共算子晋级记录重复: {definition.name}@{definition.version}"
            )
        raise ExtensionError(
            f"新增公共算子缺少 approved promotion record: "
            f"{definition.name}@{definition.version}"
        )
    review = matches[0]
    validate_operator_promotion_identity(review, definition)
    if not review.is_public:
        raise ExtensionError(
            f"candidate/rejected 算子不得进入公共 manifest: "
            f"{definition.name}@{definition.version}"
        )
    return {
        "mainline_classification": PROMOTED_CLASSIFICATION,
        "promotion_status": "approved",
    }


def validate_mainline_operator_promotions(
    manifest: CompiledOperatorManifest,
    *,
    reviews: Iterable[OperatorPromotionReview] = MAINLINE_OPERATOR_PROMOTION_REVIEWS,
) -> None:
    review_items = tuple(reviews)
    review_keys = tuple(
        (item.operator_id, item.operator_version) for item in review_items
    )
    if len(set(review_keys)) != len(review_keys):
        raise ExtensionError("公共算子晋级记录的 operator/version 重复")
    overlap = BUILTIN_OPERATOR_IDENTITIES.intersection(LEGACY_OPERATOR_DISPOSITIONS)
    if overlap:
        raise ExtensionError("内建算子与待迁出存量分类重叠")
    if set(LEGACY_OPERATOR_DEFINITION_HASHES) != set(LEGACY_OPERATOR_DISPOSITIONS):
        raise ExtensionError("待迁出存量定义摘要与责任清单不一致")

    definitions_by_identity = {
        operator_identity(item): item for item in manifest.definitions
    }
    missing_builtins = BUILTIN_OPERATOR_IDENTITIES.difference(definitions_by_identity)
    if missing_builtins:
        first = min(missing_builtins)
        raise ExtensionError(f"内建公共算子不可静默删除: {first[0]}@{first[1]}")
    builtin_definition_hashes = sorted(
        definitions_by_identity[item].definition_hash
        for item in BUILTIN_OPERATOR_IDENTITIES
    )
    builtin_definition_set_hash = sha256(
        "\n".join(builtin_definition_hashes).encode("utf-8")
    ).hexdigest()
    if builtin_definition_set_hash != BUILTIN_OPERATOR_DEFINITION_SET_HASH:
        raise ExtensionError(
            "内建公共算子定义已漂移，必须重新评审固定清单: "
            f"expected={BUILTIN_OPERATOR_DEFINITION_SET_HASH}, "
            f"actual={builtin_definition_set_hash}"
        )
    for definition in manifest.definitions:
        operator_mainline_governance(definition, reviews=review_items)
        reference = definition.implementation_ref
        validate_framework_implementation_boundary(
            implementation_scope=reference.implementation_scope,
            implementation_id=reference.implementation_id,
            module_name=reference.module_name,
            dependency_modules=reference.dependency_modules,
        )
        identity = operator_identity(definition)
        expected_legacy_hash = LEGACY_OPERATOR_DEFINITION_HASHES.get(identity)
        if (
            expected_legacy_hash is not None
            and definition.definition_hash != expected_legacy_hash
        ):
            raise ExtensionError(
                f"待迁出公共算子定义已漂移: {definition.name}@{definition.version}"
            )

    approved = tuple(item for item in review_items if item.is_public)
    for review in approved:
        identity = (
            review.operator_id,
            review.operator_version,
            review.implementation_id,
            review.module_name,
        )
        if (
            identity in BUILTIN_OPERATOR_IDENTITIES
            or identity in LEGACY_OPERATOR_DISPOSITIONS
        ):
            raise ExtensionError(
                f"既有内建或待迁出算子不得重复伪造晋级: "
                f"{review.operator_id}@{review.operator_version}"
            )
        definition = definitions_by_identity.get(identity)
        if definition is None:
            raise ExtensionError(
                f"approved promotion record 没有对应公共定义: "
                f"{review.operator_id}@{review.operator_version}"
            )
    validate_public_semantic_inventory(
        "artifact_type",
        (
            port.artifact_type
            for definition in manifest.definitions
            for port in (*definition.input_schema, *definition.output_schema)
        ),
        builtin_identities=BUILTIN_ARTIFACT_TYPE_IDENTITIES,
        reviews=MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    )


def validate_operator_promotion_identity(
    review: OperatorPromotionReview,
    definition: OperatorDefinition,
) -> None:
    reference = definition.implementation_ref
    actual = (
        definition.name,
        definition.version,
        reference.implementation_id,
        reference.module_name,
        definition.definition_hash,
    )
    expected = (
        review.operator_id,
        review.operator_version,
        review.implementation_id,
        review.module_name,
        review.definition_hash,
    )
    if actual != expected:
        raise ExtensionError(
            f"promotion record 与 OperatorDefinition identity 不一致: "
            f"{definition.name}@{definition.version}"
        )
    validate_framework_implementation_boundary(
        implementation_scope=reference.implementation_scope,
        implementation_id=reference.implementation_id,
        module_name=reference.module_name,
        dependency_modules=reference.dependency_modules,
    )


__all__ = [
    "BUILTIN_ARTIFACT_TYPE_IDENTITIES",
    "BUILTIN_CLASSIFICATION",
    "BUILTIN_OPERATOR_DEFINITION_SET_HASH",
    "BUILTIN_OPERATOR_IDENTITIES",
    "LEGACY_EXIT_CLASSIFICATION",
    "LEGACY_OPERATOR_DISPOSITIONS",
    "LEGACY_OPERATOR_DEFINITION_HASHES",
    "MAINLINE_OPERATOR_PROMOTION_REVIEWS",
    "PROMOTED_CLASSIFICATION",
    "LegacyOperatorDisposition",
    "operator_identity",
    "operator_mainline_governance",
    "validate_mainline_operator_promotions",
    "validate_operator_promotion_identity",
]
