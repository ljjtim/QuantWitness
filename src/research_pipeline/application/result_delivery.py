"""Result 的报告、比较与结果导出服务。"""

from __future__ import annotations

from research_pipeline.data_plane import PathRolePolicy
from research_pipeline.results import (
    build_result_report, compare_results, load_result_context,
    render_result_report, export_result,
)

from .report_output import prepare_qlib_request, report_table_ids, write_qlib_report, write_result_report


def _load_package(path, archive_root):
    from research_pipeline.packages import load_research_package, verify_package_source_provenance

    package = load_research_package(path)
    facts = {
        "package_id": package.package_id,
        "package_hash": package.package_hash,
        "metric_contract_hash": package.metric_contract.contract_hash,
        "claim_contract_hash": package.claim_contract.contract_hash,
        "source_provenance_hash": package.source_provenance_hash,
        "source_verification": verify_package_source_provenance(package, archive_root),
    }
    return package, facts


def _execute(args) -> dict[str, object]:
    package_path = getattr(args, "package", None)
    archive_root = getattr(args, "source_archive_root", None)
    right_path = getattr(args, "right_package", None)
    right_archive = getattr(args, "right_source_archive_root", None)
    if not package_path and any((archive_root, right_path, right_archive)):
        raise ValueError("核对研究包来源或右侧研究包需要 --package")
    package_roles = {role: value for role, value in (
        ("package_input", package_path), ("source_archive_input", archive_root),
        ("right_package_input", right_path), ("right_source_archive_input", right_archive),
    ) if value}

    if args.command == "compare":
        roles = {**package_roles, "left_result_input": args.left_result, "right_result_input": args.right_result}
        for side in ("left", "right"):
            store = getattr(args, side + "_result_store", None)
            if store is not None:
                roles[side + "_result_store_input"] = store
        PathRolePolicy().validate(roles, read_only_roles=tuple(roles))
        left = load_result_context(args.left_result, result_store=args.left_result_store)
        right = load_result_context(args.right_result, result_store=args.right_result_store)
        if package_path:
            from research_pipeline.packages.delivery import compare_research_package_results

            package, facts = _load_package(package_path, archive_root)
            right_package = package
            if right_path or right_archive:
                right_package, _ = _load_package(right_path or package_path, right_archive or archive_root)
            comparison = compare_research_package_results(package, None, left, right, right_package=right_package)
            return {**facts, "package_plan_hash": left.snapshot.bundle.plan_hash, "comparison": comparison.to_dict()}
        comparison = compare_results(left, right)
        return {
            "comparison": {**comparison.to_dict(), "scope_note": "直接比较正式指标事实；未检查 package 合同。"},
            "next_action": "需要核对研究包时，使用 compare --package。",
        }
    roles = {**package_roles, "result_input": args.result}
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
    facts = {}
    if package_path:
        from research_pipeline.packages import validate_package_result

        package, facts = _load_package(package_path, archive_root)
        validate_package_result(package, None, context)
        facts["package_plan_hash"] = context.snapshot.bundle.plan_hash
    if args.command == "report":
        markdown = render_result_report(context)
        if package_path:
            markdown = f"# {package.display_name}\n\n{markdown}"
        if args.output is None:
            return {**facts, "report": markdown}
        if request is not None:
            output = write_qlib_report(args.output, context=context, request=request, markdown=markdown)
        else:
            report = build_result_report(context).to_dict()
            output = write_result_report(
                args.output, output_format=args.format, markdown=markdown,
                report={"package": facts, "result": report} if facts else report,
            )
        return {
            **facts, "output": str(output), "format": args.format,
            "result_id": context.snapshot.bundle.result_id,
            "claim_level": context.claim_level,
            "limitations": list(context.limitations),
        }
    return {**facts, "output": str(export_result(context, args.output))}


__all__ = ["_execute"]
