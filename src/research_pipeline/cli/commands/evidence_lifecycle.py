"""结构化 VerificationResult 的验证、报告、比较与结果导出命令。"""

from __future__ import annotations

from dataclasses import asdict

from research_pipeline.data_plane import PathRolePolicy
from research_pipeline.evidence import (
    FinancialOracleBudget,
    compare_verification_results,
    load_verified_result_context,
    render_verification_report,
    export_verified_result,
    verify_result,
)

from ..result import execute_guarded


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    if args.command == "verify":
        roles = {
            "result_input": args.result,
            "result_store_input": args.result_store,
            "verification_output": args.output,
        }
        if args.verification_scratch_root is not None:
            roles["verification_scratch"] = args.verification_scratch_root
        PathRolePolicy().validate(
            roles,
            read_only_roles=("result_input", "result_store_input"),
        )
        budget_kwargs = {}
        if args.verification_memory_bytes is not None:
            budget_kwargs["memory_bytes"] = args.verification_memory_bytes
        if args.verification_temp_bytes is not None:
            budget_kwargs["temp_bytes"] = args.verification_temp_bytes
        if args.verification_scratch_root is not None:
            budget_kwargs["scratch_root"] = args.verification_scratch_root
        context = verify_result(
            args.result,
            result_store=args.result_store,
            output=args.output,
            financial_oracle_budget=FinancialOracleBudget(**budget_kwargs),
            verifier_bundle=getattr(args, "verifier_bundle", None),
        )
        return {
            "verification_hash": context.verification.verification_hash,
            "result_id": context.snapshot.bundle.result_id,
            "status": context.verification.status,
            "validity_status": context.verification.validity_status,
            "claim_level": context.verification.claim_level,
            "metric_count": len(context.metrics),
            "output": args.output,
            "next_action": "可使用 report、compare 或 export-result 消费该 VerificationResult。",
        }
    if args.command == "compare":
        PathRolePolicy().validate(
            {
                "left_verification_input": args.left_verification_result,
                "left_result_store_input": args.left_result_store,
                "right_verification_input": args.right_verification_result,
                "right_result_store_input": args.right_result_store,
            },
            read_only_roles=(
                "left_verification_input",
                "left_result_store_input",
                "right_verification_input",
                "right_result_store_input",
            ),
        )
        comparison = compare_verification_results(
            load_verified_result_context(
                args.left_verification_result,
                result_store=args.left_result_store,
            ),
            load_verified_result_context(
                args.right_verification_result,
                result_store=args.right_result_store,
            ),
        )
        return {
            "comparison": asdict(comparison),
            "next_action": (
                "如需比较完整 ResearchPackage 的 metric/claim 合同，"
                "请使用 package compare。"
            ),
        }
    roles = {
        "verification_input": args.verification_result,
        "result_store_input": args.result_store,
    }
    if args.command == "export-result":
        roles["export_result_output"] = args.output
    PathRolePolicy().validate(
        roles,
        read_only_roles=("verification_input", "result_store_input"),
    )
    context = load_verified_result_context(
        args.verification_result,
        result_store=args.result_store,
    )
    if args.command == "report":
        return {"report": render_verification_report(context)}
    return {"output": str(export_verified_result(context, args.output))}


__all__ = ["execute"]
