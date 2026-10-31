"""公共语义身份的内建归属与晋级门禁。"""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Iterable


SEMANTIC_PROMOTION_VERSION = "research-semantic-promotion-v1"
PUBLIC_SEMANTIC_KINDS = frozenset({
    "artifact_type",
    "metric",
    "result_schema",
    "workflow_profile",
})
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/,=-]{0,511}$")


# 独立于各注册表的已审查内建合同。新增公共身份不能只修改注册表或刷新快照。
APPROVED_BUILTIN_SEMANTIC_IDENTITIES = MappingProxyType({
    "artifact_type": frozenset({
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
        "research.minute-statistics.v1",
        "research.minute-targets.v1",
        "research.model-fits.v2",
        "research.model-fold-metrics.v2",
        "research.model-locked-holdout.v2",
        "research.model-selection.v2",
        "research.model-split-manifest.v2",
        "research.model-validation-predictions.v2",
    }),
    "metric": frozenset({
        "data.row_count@1.0.0",
        "minute.row_count@1.0.0",
        "statistics.adjusted_p@1.0.0",
    }),
    "result_schema": frozenset({
        "research.strategy.decisions.v1",
        "research.strategy.events.v2", "research.strategy.state.v2", "research.strategy.market.v2",
        "research.shared-futures.context.v1",
        "research.shared-futures.cash.v1",
        "research.shared-futures.positions.v1",
        "research.shared-futures.orders.v1",
        "research.shared-futures.fills.v1",
        "research.shared-futures.costs.v1",
        "research.shared-futures.valuations.v1",
        "research.shared-futures.reservations.v1",
        "research.shared-futures.risks.v1",
        "research.shared-futures.rolls.v1",
        "research.futures-daily-context.v1",
        "research.order-lifecycle.v1",
        "research.qlib-model-inventory.v1",
        "data.adjustment-factor-snapshot.payload.v1",
        "data.adjustment-factor-snapshot.payload.v2",
        "data.columnar-bundle.metrics.v1",
        "research.bar-tca.daily.v1",
        "research.bar-tca.fills.v1",
        "research.bar-tca.orders.v1",
        "research.bar-tca.research.v1",
        "research.minute-features.pre-anchor.v1",
        "research.minute-labels.pre-anchor.v1",
        "research.minute-observation.metrics.v1",
        "research.minute-targets.payload.v1",
        "research.minute-financial-context.execution-bars.v2",
        "research.minute-financial-context.decision-benchmarks.v2",
        "research.minute-financial-context.execution-observations.v1",
        "research.minute-financial-context.settlement-events.v1",
        "research.minute-statistics.observations.v1",
        "research.minute-statistics.split-assignments.v1",
        "research.minute-statistics.v1",
        "research.simulation.cash.v1",
        "research.simulation.costs.v1",
        "research.simulation.fills.v1",
        "research.simulation.orders.v1",
        "research.simulation.positions.v1",
        "research.simulation.valuations.v1",
    }),
    "workflow_profile": frozenset(),
})


class SemanticGovernanceError(ValueError):
    """公共语义身份没有明确归属或晋级依据。"""


# 晋级记录只能经评审后显式加入；能力 baseline 和发现快照不能生成该授权。
MAINLINE_SEMANTIC_PROMOTION_REVIEWS: tuple["SemanticPromotionReview", ...] = ()

# LEAN仅登记本地项目结果类型，不进入公共算子或能力晋级记录。
LEAN_LOCAL_SEMANTIC_IDENTITIES = MappingProxyType({
    "artifact_type": frozenset({"research.lean-simulation.v1"}),
    "result_schema": frozenset(
        {f"research.lean.{name}.v1"
         for name in ("orders", "intents", "daily", "trace")}
        | {"research.lean-signal-source.v1"}
    ),
})


@dataclass(frozen=True)
class SemanticPromotionReview:
    semantic_kind: str
    identity: str
    status: str
    reuse_projects: tuple[str, ...]
    oracle_ids: tuple[str, ...]
    attack_ids: tuple[str, ...]
    heterogeneous_reuse_confirmed: bool
    generic_semantics_confirmed: bool
    contract_version: str = SEMANTIC_PROMOTION_VERSION

    def __post_init__(self) -> None:
        if self.semantic_kind not in PUBLIC_SEMANTIC_KINDS:
            raise SemanticGovernanceError("公共语义类型无效")
        if not isinstance(self.identity, str) or not _ID.fullmatch(self.identity):
            raise SemanticGovernanceError("公共语义 identity 无效")
        if self.status not in {"candidate", "approved", "rejected"}:
            raise SemanticGovernanceError("公共语义晋级状态无效")
        for field in ("reuse_projects", "oracle_ids", "attack_ids"):
            values = getattr(self, field)
            if tuple(sorted(values)) != values or len(set(values)) != len(values):
                raise SemanticGovernanceError(f"{field} 必须唯一并规范排序")
            if any(not isinstance(value, str) or not value for value in values):
                raise SemanticGovernanceError(f"{field} 包含无效身份")
        if type(self.heterogeneous_reuse_confirmed) is not bool:
            raise SemanticGovernanceError(
                "heterogeneous_reuse_confirmed 必须是 bool"
            )
        if type(self.generic_semantics_confirmed) is not bool:
            raise SemanticGovernanceError("generic_semantics_confirmed 必须是 bool")
        if self.contract_version != SEMANTIC_PROMOTION_VERSION:
            raise SemanticGovernanceError("公共语义晋级合同版本不受支持")
        if self.status == "approved" and (
            len(self.reuse_projects) < 2
            or not self.oracle_ids
            or not self.attack_ids
            or not self.heterogeneous_reuse_confirmed
            or not self.generic_semantics_confirmed
        ):
            raise SemanticGovernanceError(
                "公共语义晋级缺少异构复用、独立 oracle、攻击测试或人工结论"
            )

    @property
    def is_public(self) -> bool:
        return self.status == "approved"


def validate_public_semantic_inventory(
    semantic_kind: str,
    actual_identities: Iterable[str],
    *,
    builtin_identities: Iterable[str],
    reviews: Iterable[SemanticPromotionReview] = (),
) -> None:
    """公共注册面只能包含固定内建身份或已批准晋级身份。"""

    if semantic_kind not in PUBLIC_SEMANTIC_KINDS:
        raise SemanticGovernanceError("公共语义类型无效")
    actual = tuple(sorted(set(str(item) for item in actual_identities)))
    builtins = tuple(sorted(str(item) for item in builtin_identities))
    if len(builtins) != len(set(builtins)):
        raise SemanticGovernanceError("内建公共语义身份不得重复")
    approved = set(APPROVED_BUILTIN_SEMANTIC_IDENTITIES[semantic_kind])
    unapproved = set(builtins) - approved
    if unapproved:
        raise SemanticGovernanceError(
            f"内建公共语义缺少逐项批准: {semantic_kind}:{min(unapproved)}"
        )
    withdrawn = approved - set(builtins)
    if withdrawn:
        raise SemanticGovernanceError(
            f"已批准内建公共语义不可静默移除: {semantic_kind}:{min(withdrawn)}"
        )
    missing = set(builtins) - set(actual)
    if missing:
        raise SemanticGovernanceError(
            f"内建公共语义不可静默删除: {semantic_kind}:{min(missing)}"
        )
    review_items = tuple(
        item for item in reviews if item.semantic_kind == semantic_kind
    )
    review_by_identity = {item.identity: item for item in review_items}
    if len(review_by_identity) != len(review_items):
        raise SemanticGovernanceError("公共语义晋级记录重复")
    for identity in sorted(set(actual) - set(builtins)):
        if identity in LEAN_LOCAL_SEMANTIC_IDENTITIES.get(semantic_kind, ()):
            continue
        review = review_by_identity.get(identity)
        if review is None:
            raise SemanticGovernanceError(
                f"新增公共语义缺少 approved promotion record: "
                f"{semantic_kind}:{identity}"
            )
        if not review.is_public:
            raise SemanticGovernanceError(
                f"candidate/rejected 语义不得进入公共注册面: "
                f"{semantic_kind}:{identity}"
            )
    unused = set(review_by_identity) - set(actual)
    if unused:
        raise SemanticGovernanceError(
            f"公共语义晋级记录没有对应身份: {semantic_kind}:{min(unused)}"
        )


__all__ = [
    "APPROVED_BUILTIN_SEMANTIC_IDENTITIES",
    "PUBLIC_SEMANTIC_KINDS",
    "MAINLINE_SEMANTIC_PROMOTION_REVIEWS",
    "SEMANTIC_PROMOTION_VERSION",
    "SemanticGovernanceError",
    "SemanticPromotionReview",
    "validate_public_semantic_inventory",
]
