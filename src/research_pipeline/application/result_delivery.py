"""Result 的报告、比较与结果导出服务。"""

from __future__ import annotations

from research_pipeline.data_plane import PathRolePolicy
from research_pipeline.results import (
    build_result_report, compare_results, load_result_context,
    render_result_report, export_result,
)

from .report_output import prepare_qlib_request, report_table_ids, write_qlib_report, write_result_report


def _execute(args) -> dict[str, object]:
    if args.command == "compare":
        roles = {"left_result_input": args.left_result, "right_result_input": args.right_result}
        for side in ("left", "right"):
            store = getattr(args, side + "_result_store", None)
            if store is not None:
                roles[side + "_result_store_input"] = store
        PathRolePolicy().validate(roles, read_only_roles=tuple(roles))
        comparison = compare_results(
            load_result_context(args.left_result, result_store=args.left_result_store),
            load_result_context(args.right_result, result_store=args.right_result_store),
        )
        return {
            "comparison": {
                **comparison.to_dict(),
                "scope_note": "直接比较正式指标事实；未检查 package 合同。",
            },
            "next_action": "如需比较 ResearchPackage 的 metric/claim 合同，请使用 package compare。",
        }
    roles = {"result_input": args.result}
    if args.result_store is not None:
        roles["result_store_input"] = args.result_store
    if args.command == "export-result":
        roles["export_result_output"] = args.output
    elif args.command == "report" and args.output is not None:
        roles["report_output"] = args.output
    PathRolePolicy().validate(roles, read_only_roles=tuple(role for role in roles if role.endswith("_input")))
    request = prepare_qlib_request(args) if args.command == "report" else None
    context = load_result_context(
        args.result, result_store=args.result_store,
        additional_table_ids=report_table_ids(request),
    )
    if args.command == "report":
        markdown = render_result_report(context)
        if args.output is None:
            return {"report": markdown}
        if request is not None:
            output = write_qlib_report(args.output, context=context, request=request, markdown=markdown)
        else:
            output = write_result_report(
                args.output, output_format=args.format, markdown=markdown,
                report=build_result_report(context).to_dict(),
            )
        return {
            "output": str(output), "format": args.format,
            "result_id": context.snapshot.bundle.result_id,
            "claim_level": context.claim_level,
            "limitations": list(context.limitations),
        }
    return {"output": str(export_result(context, args.output))}


__all__ = ["_execute"]
