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
    plan: OperatorGraphPlan,
    context: ResultContext,
) -> None:
    """确认结果确实属于当前包、计划、指标和结论合同。"""

    if type(context) is not ResultContext:
        raise ResearchPackageError(
            "ResearchPackage 只接受 ResultContext"
        )
    if package.package_hash != plan.package_hash:
        raise ResearchPackageError("研究计划不属于当前 ResearchPackage")
    if any(item.claim_effect == "not_sealable" for item in package.localizations):
        raise ResearchPackageError("ResearchPackage 禁止可信结果消费")

    bundle = context.snapshot.bundle
    if bundle.package_hash != package.package_hash:
        raise ResearchPackageError("Result 未绑定当前 ResearchPackage")
    if bundle.plan_hash != plan.plan_hash:
        raise ResearchPackageError("Result plan 与 ResearchPackage 不一致")
    if bundle.metric_proofs != plan.metric_proofs:
        raise ResearchPackageError("Result metric proof 与研究计划不一致")
    if bundle.result_spec != plan.result_spec:
        raise ResearchPackageError("ResultSpec 与研究计划不一致")
    if {item.metric_ref for item in bundle.metric_proofs} != set(
        package.metric_contract.metrics
    ):
        raise ResearchPackageError("Result metric 合同与 ResearchPackage 不一致")
    if context.claim_level not in package.claim_contract.allowed_claim_levels:
        raise ResearchPackageError("Result claim 超过 ResearchPackage 允许范围")


def render_research_package_report(
    package: ResearchPackage,
    plan: OperatorGraphPlan,
    context: ResultContext,
) -> str:
    validate_package_result(package, plan, context)
    bundle = context.snapshot.bundle
    lines = [
        f"# {package.display_name}",
        "",
        f"- ResearchPackage：`{package.package_id}`",
        f"- Result：`{bundle.result_id}`",
        f"- 结论等级：`{context.claim_level}`",
        f"- Metric 合同：`{package.metric_contract.contract_hash}`",
        f"- Claim 合同：`{package.claim_contract.contract_hash}`",
        "- 结果导出：先加载 Result，再调用 "
        "`export_research_package_result`；该操作不重新执行研究。",
        "",
        "## 来源",
    ]
    for item in package.sources:
        if item.provenance.mode == "citation_only":
            offline = "不支持（仅引用元数据；content_hash 不证明来源正文）"
        else:
            offline = (
                "支持（需保留并重新校验来源归档）；"
                f"正文摘要={item.provenance.content_digest}；"
                f"归档清单={item.provenance.snapshot_manifest_hash}"
            )
        lines.append(
            f"- [{item.title}]({item.url})；类型={item.source_type}；"
            f"状态={item.status}；许可={item.license_id}；"
            f"离线内容复现={offline}；限制={item.limitation or '无额外声明'}"
        )
    lines.extend(("", "## 本土化差异"))
    lines.extend(
        f"- {item.original_assumption} → {item.local_adaptation}；"
        f"状态={item.status}；claim影响={item.claim_effect}"
        for item in package.localizations
    )
    return "\n".join(lines) + "\n"


def compare_research_package_results(
    package: ResearchPackage,
    plan: OperatorGraphPlan,
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


def export_research_package_result(
    package: ResearchPackage,
    plan: OperatorGraphPlan,
    context: ResultContext,
    destination: str | Path,
) -> Path:
    validate_package_result(package, plan, context)
    return export_result(context, destination)


__all__ = [
    "compare_research_package_results",
    "render_research_package_report",
    "export_research_package_result",
    "validate_package_result",
]
