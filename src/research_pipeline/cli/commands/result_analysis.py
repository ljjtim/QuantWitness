"""Result 的通用时间序列分析命令。"""

from __future__ import annotations

from research_pipeline.data_plane import PathRolePolicy
from research_pipeline.evidence import (
    analyze_result,
    compare_analysis_results,
    load_analysis_request,
    load_analysis_result,
    write_analysis_comparison,
    write_analysis_result,
)
from research_pipeline.evidence.errors import EvidenceContractError

from ..result import execute_guarded


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    if args.analysis_command == "run":
        roles = {
            "result_input": args.result,
            "analysis_request_input": args.request,
            "analysis_result_output": args.output,
        }
        if args.result_store is not None:
            roles["result_store_input"] = args.result_store
        PathRolePolicy().validate(
            roles,
            read_only_roles=tuple(role for role in roles if role.endswith("_input")),
        )
        request = load_analysis_request(args.request)
        result = analyze_result(
            args.result,
            result_store=args.result_store,
            request=request,
            memory_bytes=args.analysis_memory_bytes,
        )
        output = write_analysis_result(result, args.output)
        return {
            "output": str(output),
            "analysis_hash": result.analysis_hash,
            "result_id": result.source.result_reference.result_id,
            "selected_rows": result.selected_rows,
            "actual_start": result.actual_start,
            "actual_end": result.actual_end,
            "claim_level": result.source.claim_level,
            "next_action": (
                "可使用 analysis compare 对两份或多份同口径 AnalysisResult 排名。"
            ),
        }

    if len(args.analysis_result) < 2:
        raise EvidenceContractError("analysis compare 至少需要两个 --analysis-result")
    roles = {
        f"analysis_result_input_{index}": path
        for index, path in enumerate(args.analysis_result, start=1)
    }
    roles["analysis_comparison_output"] = args.output
    PathRolePolicy().validate(
        roles,
        read_only_roles=tuple(
            role for role in roles if role.startswith("analysis_result_input_")
        ),
    )
    results = tuple(load_analysis_result(path) for path in args.analysis_result)
    comparison = compare_analysis_results(
        results,
        measure=args.measure,
        direction=args.direction,
    )
    output = write_analysis_comparison(comparison, args.output)
    return {
        "output": str(output),
        "comparison_hash": comparison.comparison_hash,
        "comparable": comparison.comparable,
        "reason_codes": list(comparison.reason_codes),
        "ranking_count": len(comparison.rankings),
    }


__all__ = ["execute"]
