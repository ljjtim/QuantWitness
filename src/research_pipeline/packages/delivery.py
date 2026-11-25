"""ResearchPackage 对当前 Result 的报告、比较和导出。"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from research_pipeline.results import (
    ResultContext,
    compare_results,
    export_result,
)

from .models import ResearchPackage, ResearchPackageError
from .plan_contracts import OperatorGraphPlan


def validate_package_result(
    package: ResearchPackage,
    plan: OperatorGraphPlan | None,
    context: ResultContext,
) -> None:
    """确认结果确实属于当前包、计划、指标和结论合同。"""

    if type(context) is not ResultContext:
        raise ResearchPackageError(
            "ResearchPackage 只接受 ResultContext"
        )
    if plan is not None and package.package_hash != plan.package_hash:
        raise ResearchPackageError("研究计划不属于当前 ResearchPackage")
    if any(item.claim_effect == "not_sealable" for item in package.localizations):
        raise ResearchPackageError("ResearchPackage 禁止可信结果消费")

    bundle = context.snapshot.bundle
    if bundle.package_hash != package.package_hash:
        raise ResearchPackageError("Result 未绑定当前 ResearchPackage")
    if plan is not None and bundle.plan_hash != plan.plan_hash:
        raise ResearchPackageError("Result plan 与 ResearchPackage 不一致")
    if plan is not None and bundle.metric_proofs != plan.metric_proofs:
        raise ResearchPackageError("Result metric proof 与研究计划不一致")
    if plan is not None and bundle.result_spec != plan.result_spec:
        raise ResearchPackageError("ResultSpec 与研究计划不一致")
    if {item.metric_ref for item in bundle.metric_proofs} != set(
        package.metric_contract.metrics
    ):
        raise ResearchPackageError("Result metric 合同与 ResearchPackage 不一致")
    if context.claim_level not in package.claim_contract.allowed_claim_levels:
        raise ResearchPackageError("Result claim 超过 ResearchPackage 允许范围")


def compare_research_package_results(
    package: ResearchPackage,
    plan: OperatorGraphPlan | None,
    left: ResultContext,
    right: ResultContext,
    *,
    right_package: ResearchPackage | None = None,
    right_plan: OperatorGraphPlan | None = None,
):
    checked_package = right_package or package
    checked_plan = right_plan or plan
    if package.metric_contract.contract_hash != checked_package.metric_contract.contract_hash:
        raise ResearchPackageError("metric 合同不同，禁止比较")
    if package.claim_contract.contract_hash != checked_package.claim_contract.contract_hash:
        raise ResearchPackageError("claim 合同不同，禁止比较")
    validate_package_result(package, plan, left)
    validate_package_result(checked_package, checked_plan, right)
    comparison = replace(
        compare_results(left, right),
        comparison_scope="package_contract_and_metric_facts",
    )
    if not comparison.comparable:
        raise ResearchPackageError(
            f"结果不可比较: {','.join(comparison.reason_codes)}"
        )
    return comparison


__all__ = [
    "compare_research_package_results",
    "validate_package_result",
]
