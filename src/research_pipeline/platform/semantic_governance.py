"""公共语义身份的内建归属与晋级门禁。"""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Iterable


SEMANTIC_PROMOTION_VERSION = "research-semantic-promotion-v1"
BUILTIN_SEMANTIC_REVISION_VERSION = "research-builtin-semantic-revision-v1"
PUBLIC_SEMANTIC_KINDS = frozenset({
    "artifact_type",
    "metric",
    "result_schema",
    "result_schema_set",
    "verifier",
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
        "research.minute-simulation.v1",
        "research.minute-statistics.v1",
        "research.minute-targets.v1",
        "research.model-fits.v2",
        "research.model-fold-metrics.v2",
        "research.model-locked-holdout.v2",
        "research.model-selection.v2",
        "research.model-split-manifest.v2",
        "research.model-validation-predictions.v2",
        "research.validity-facts.v1",
    }),
    "metric": frozenset({
        "data.row_count@1.0.0",
        "minute.row_count@1.0.0",
        "statistics.adjusted_p@1.0.0",
    }),
    "result_schema": frozenset({
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
        "data.columnar-bundle.metrics.v1",
        "research.bar-tca.daily.v1",
        "research.bar-tca.fills.v1",
        "research.bar-tca.orders.v1",
        "research.bar-tca.research.v1",
        "research.minute-features.pre-anchor.v1",
        "research.minute-labels.pre-anchor.v1",
        "research.minute-observation.metrics.v1",
        "research.minute-targets.payload.v1",
        "research.minute-financial-context.execution-bars.v1",
        "research.minute-financial-context.decision-benchmarks.v1",
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
    "result_schema_set": frozenset({
        "shared_futures:research.shared-futures.cash.v1@shared_futures/cash,research.shared-futures.context.v1@shared_futures/context,research.shared-futures.costs.v1@shared_futures/costs,research.shared-futures.fills.v1@shared_futures/fills,research.shared-futures.orders.v1@shared_futures/orders,research.shared-futures.positions.v1@shared_futures/positions,research.shared-futures.reservations.v1@shared_futures/reservations,research.shared-futures.risks.v1@shared_futures/risks,research.shared-futures.rolls.v1@shared_futures/rolls,research.shared-futures.valuations.v1@shared_futures/valuations",
        "futures_daily_context:research.futures-daily-context.v1@simulation/futures-context-tables",
        "order_lifecycle:research.order-lifecycle.v1@simulation/context-tables/order-lifecycle",
        "adjustment_snapshot:data.adjustment-factor-snapshot.payload.v1,"
        "research.minute-features.pre-anchor.v1,research.minute-labels.pre-anchor.v1",
        "bar_tca:research.bar-tca.daily.v1@simulation/tca/daily,"
        "research.bar-tca.fills.v1@simulation/tca/fills,"
        "research.bar-tca.orders.v1@simulation/tca/orders,"
        "research.bar-tca.research.v1@simulation/tca/research",
        "canonical_simulation:research.simulation.cash.v1,research.simulation.costs.v1,"
        "research.simulation.fills.v1,research.simulation.orders.v1,"
        "research.simulation.positions.v1,research.simulation.valuations.v1",
        "minute_statistics:research.minute-statistics.observations.v1,"
        "research.minute-statistics.split-assignments.v1",
        "minute_financial_context:research.minute-financial-context.decision-benchmarks.v1,"
        "research.minute-financial-context.execution-bars.v1,"
        "research.minute-financial-context.execution-observations.v1,"
        "research.minute-financial-context.settlement-events.v1,"
        "research.minute-targets.payload.v1",
    }),
    "verifier": frozenset({
        "result-semantic:shared_futures:shared-futures-financial-oracle-v1",
        "default:data.pit:verifier.data-pit.v1",
        "default:financial.tradability:verifier.financial-tradability.v2",
        "default:label.split:verifier.label-split.v2",
        "default:search.holdout:verifier.search-holdout.v1",
        "default:statistics:verifier.statistics.v2",
        "model:data.pit:verifier.data-pit.v1",
        "model:label.split:verifier.model-label-split.v1",
        "model:search.holdout:verifier.model-search-holdout.v1",
        "model:statistics:verifier.model-statistics.v1",
        "model:financial.tradability:verifier.model-prediction-scope.v1",
        "minute:data.pit:verifier.minute-data-pit.v2",
        "minute:financial.tradability:verifier.minute-financial.v2",
        "minute:label.split:verifier.minute-label-split.v2",
        "minute:search.holdout:verifier.minute-trial-universe.v2",
        "minute:statistics:verifier.minute-statistics.v2",
        "result-semantic:adjustment_snapshot:verifier.adjustment-snapshot.v1",
        "result-semantic:financial_oracle:result-bundle-financial-oracle-v5",
    }),
    "workflow_profile": frozenset(),
})


class SemanticGovernanceError(ValueError):
    """公共语义身份没有明确归属或晋级依据。"""


# 晋级记录只能经评审后显式加入；能力 baseline 和发现快照不能生成该授权。
MAINLINE_SEMANTIC_PROMOTION_REVIEWS: tuple["SemanticPromotionReview", ...] = ()

# 2026-10-03 维护者批准日频现金接口有界接入，通用语义晋级仍按原合同。
DAILY_CASH_LOCAL_ARTIFACT_IDENTITIES = frozenset({
    "data.daily-market.v1",
    "research.portfolio-targets.v1",
    "research.daily-simulation.v1",
})


# P8 共享期货账户仅获本地执行批准；发现身份不构成公共能力晋级。
SHARED_FUTURES_LOCAL_ARTIFACT_IDENTITIES = frozenset({
    "research.shared-futures-simulation.v1",
})


@dataclass(frozen=True)
class BuiltinSemanticRevision:
    semantic_kind: str
    previous_identity: str
    replacement_identity: str
    reason: str
    regression_test_ids: tuple[str, ...]
    contract_version: str = BUILTIN_SEMANTIC_REVISION_VERSION

    def __post_init__(self) -> None:
        if self.semantic_kind not in PUBLIC_SEMANTIC_KINDS:
            raise SemanticGovernanceError("内建公共语义修订类型无效")
        for field in ("previous_identity", "replacement_identity"):
            value = getattr(self, field)
            if not isinstance(value, str) or not _ID.fullmatch(value):
                raise SemanticGovernanceError(f"{field} 无效")
        if self.previous_identity == self.replacement_identity:
            raise SemanticGovernanceError("内建公共语义修订必须改变身份")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise SemanticGovernanceError("内建公共语义修订必须说明原因")
        if (
            tuple(sorted(self.regression_test_ids)) != self.regression_test_ids
            or len(set(self.regression_test_ids)) != len(self.regression_test_ids)
            or not self.regression_test_ids
        ):
            raise SemanticGovernanceError("内建公共语义修订测试必须非空、唯一并排序")
        if self.contract_version != BUILTIN_SEMANTIC_REVISION_VERSION:
            raise SemanticGovernanceError("内建公共语义修订合同版本不受支持")


MAINLINE_BUILTIN_SEMANTIC_REVISIONS = (
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="model:label.split:verifier.model-label-split.v1",
        replacement_identity="model:label.split:verifier.model-label-split.v2",
        reason="序列研究按原始特征和标签重建窗口资格并复核模型训练子集与GRU配置",
        regression_test_ids=(
            "test_real_gru_window_and_model_facts_pass_without_production_sampling",
            "test_sequence_facts_reject_changed_evidence",
            "test_window_file_fact_must_match_result_snapshot",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v5",
        replacement_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v6",
        reason="P3 生命周期支持分区独立复核，六表保持原成交摘要语义",
        regression_test_ids=(
            "test_formal_result_oracle_uses_sealed_lifecycle_after_run_root_removed",
            "test_independent_oracle_rejects_lifecycle_corruption",
            "test_new_result_cannot_drop_or_corrupt_lifecycle_support",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="model:search.holdout:verifier.model-search-holdout.v1",
        replacement_identity="model:search.holdout:verifier.model-search-holdout.v2",
        reason="序列研究按原始特征和标签重建窗口资格并复核模型训练子集与GRU配置",
        regression_test_ids=(
            "test_real_gru_window_and_model_facts_pass_without_production_sampling",
            "test_sequence_facts_reject_changed_evidence",
            "test_window_file_fact_must_match_result_snapshot",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v6",
        replacement_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v7",
        reason="分钟现货按当时可见的最新行情事件独立复核估值；日频期货按会话账本事实复核",
        regression_test_ids=(
            "test_futures_result_verifies_without_run_root",
            "test_late_old_price_cannot_replace_latest_visible_event",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v7",
        replacement_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v8",
        reason="P6 显式分钟数量与价格限制、历史状态、可见修订及实际 PIT 消费绑定的独立复核",
        regression_test_ids=(
            "test_oracle_selects_pit_rule_at_consumption_time",
            "test_p6_minute_public_package_chain",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:adjustment_snapshot:verifier.adjustment-snapshot.v1",
        replacement_identity="result-semantic:adjustment_snapshot:verifier.adjustment-snapshot.v2",
        reason="PIT 快照同时封存完整金融行动，独立校验 v2 合同和研究时钟可见性",
        regression_test_ids=("test_verifier_consumes_complete_snapshot_payload_and_rejects_tampering",),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="minute:label.split:verifier.minute-label-split.v2",
        replacement_identity="minute:label.split:verifier.minute-label-split.v3",
        reason="分钟标签经济起点可已知，独立要求决策和起价均早于未来终价",
        regression_test_ids=("test_minute_oracle_accepts_known_economic_entry", "test_minute_oracle_rejects_nonfuture_exit"),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="minute:statistics:verifier.minute-statistics.v2",
        replacement_identity="minute:statistics:verifier.minute-statistics.v3",
        reason="分钟标签经济起点可已知，独立要求决策和起价均早于未来终价",
        regression_test_ids=("test_minute_oracle_accepts_known_economic_entry", "test_minute_oracle_rejects_nonfuture_exit"),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="minute:financial.tradability:verifier.minute-financial.v2",
        replacement_identity="minute:financial.tradability:verifier.minute-financial.v3",
        reason="分钟金融事实从正式六表与 TCA 身份重建，并绑定 Result 独立金融复核",
        regression_test_ids=("test_p6_minute_public_package_chain",),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="model:financial.tradability:verifier.model-prediction-scope.v1",
        replacement_identity="model:financial.tradability:verifier.model-financial-scope.v2",
        reason="模型组合金融事实绑定正式 Result 的独立日频现金及 TCA 复核",
        regression_test_ids=(
            "test_model_financial_gate_consumes_independent_oracle",
            "test_portfolio_cannot_hide_simulation_as_prediction_only",
            "test_portfolio_cannot_pass_with_only_self_reported_facts",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity=(
            "default:financial.tradability:verifier.financial-tradability.v2"
        ),
        replacement_identity=(
            "default:financial.tradability:verifier.financial-tradability.v3"
        ),
        reason="支持上一收盘到当前收盘的账户日收益，并保留输入可见性门禁",
        regression_test_ids=(
            "test_close_to_close_account_return_window_is_supported",
            "test_verifier_recomputes_semantics_and_capacity_claim_ceiling",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v8",
        replacement_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v9",
        reason="P7 显式订单流独立复核提交可见性、持续订单、累计费用与预占事实",
        regression_test_ids=(
            "test_p7_minute_runtime_seals_explicit_context_and_support",
            "test_p7_semantic_revisions_register_current_financial_verifiers",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v9",
        replacement_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v10",
        reason="P7A 独立重建外部资金流、提款预占、可见估值及时间加权收益",
        regression_test_ids=(
            "test_cashflows_complete_formal_package_and_recovery",
            "test_compile_rejects_invalid_flow_before_worker_start",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v10",
        replacement_identity="result-semantic:financial_oracle:result-bundle-financial-oracle-v11",
        reason="P7B 独立重建融资债务、自然日利息、担保约束及信用净资产",
        regression_test_ids=(
            "test_credit_report_uses_net_opening_assets_and_does_not_deduct_interest_twice",
            "test_credit_selection_is_rejected_before_execution",
        ),
    ),
    BuiltinSemanticRevision(
        semantic_kind="verifier",
        previous_identity="minute:financial.tradability:verifier.minute-financial.v3",
        replacement_identity="minute:financial.tradability:verifier.minute-financial.v4",
        reason="分钟金融门禁接入显式订单决策语义与逐订单累计费用，旧目标上下文继续可读",
        regression_test_ids=(
            "test_p7_minute_runtime_preserves_target_context_version",
            "test_p7_semantic_revisions_register_current_financial_verifiers",
        ),
    ),
)


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
    builtin_revisions: Iterable[BuiltinSemanticRevision] = (),
) -> None:
    """公共注册面只能包含固定内建身份或已批准晋级身份。"""

    if semantic_kind not in PUBLIC_SEMANTIC_KINDS:
        raise SemanticGovernanceError("公共语义类型无效")
    actual = tuple(sorted(set(str(item) for item in actual_identities)))
    builtins = tuple(sorted(str(item) for item in builtin_identities))
    if len(builtins) != len(set(builtins)):
        raise SemanticGovernanceError("内建公共语义身份不得重复")
    approved = set(APPROVED_BUILTIN_SEMANTIC_IDENTITIES[semantic_kind])
    revisions = tuple(
        item for item in builtin_revisions if item.semantic_kind == semantic_kind
    )
    previous_identities = [item.previous_identity for item in revisions]
    replacement_identities = [item.replacement_identity for item in revisions]
    if (
        len(set(previous_identities)) != len(previous_identities)
        or len(set(replacement_identities)) != len(replacement_identities)
    ):
        raise SemanticGovernanceError("内建公共语义修订身份重复")
    for revision in revisions:
        if revision.previous_identity not in approved:
            raise SemanticGovernanceError(
                f"内建公共语义修订来源未获批准: "
                f"{semantic_kind}:{revision.previous_identity}"
            )
        if revision.replacement_identity in approved:
            raise SemanticGovernanceError(
                f"内建公共语义修订目标已存在: "
                f"{semantic_kind}:{revision.replacement_identity}"
            )
        approved.remove(revision.previous_identity)
        approved.add(revision.replacement_identity)
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
        if semantic_kind == "artifact_type" and identity in (
            DAILY_CASH_LOCAL_ARTIFACT_IDENTITIES | SHARED_FUTURES_LOCAL_ARTIFACT_IDENTITIES
        ):
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
    "BUILTIN_SEMANTIC_REVISION_VERSION",
    "BuiltinSemanticRevision",
    "MAINLINE_BUILTIN_SEMANTIC_REVISIONS",
    "PUBLIC_SEMANTIC_KINDS",
    "MAINLINE_SEMANTIC_PROMOTION_REVIEWS",
    "SEMANTIC_PROMOTION_VERSION",
    "SemanticGovernanceError",
    "SemanticPromotionReview",
    "validate_public_semantic_inventory",
]
