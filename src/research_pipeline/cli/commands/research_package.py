"""无版本 ResearchPackage 命令。"""

from __future__ import annotations

from dataclasses import asdict

from ..result import execute_guarded


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    if args.package_command == "init":
        from research_pipeline.packages import initialize_research_package

        path = initialize_research_package(args.destination)
        return {"package_path": str(path), "next_action": "填写来源、本土化和研究规格后运行 package lint。"}
    if args.package_command == "source-import":
        from research_pipeline.packages import ingest_source_snapshot

        result = ingest_source_snapshot(
            package_root=args.package,
            source_id=args.source_id,
            input_root=args.input_root,
            input_file=args.input_file,
            archive_root=args.archive_root,
            media_type=args.media_type,
            importer_id=args.importer_id,
            imported_at=args.imported_at,
        )
        return {**result, "next_action": "运行 package lint --source-archive-root 重新核验归档正文。"}
    if args.package_command == "admit":
        from .research_plan import admit_package

        return admit_package(args)
    if args.package_command == "lint":
        from research_pipeline.data_plane import PathRolePolicy

        roles = {"package_input": args.package}
        if getattr(args, "catalog_lock", None):
            roles["catalog_input"] = args.catalog_lock
        if getattr(args, "source_archive_root", None):
            roles["source_archive_input"] = args.source_archive_root
        PathRolePolicy().validate(
            roles,
            read_only_roles=tuple(role for role in roles if role.endswith("_input")),
        )
    if args.package_command in {"report", "compare", "export-result"}:
        from research_pipeline.data_plane import PathRolePolicy

        roles = {
            "package_input": args.package,
        }
        if args.package_command == "compare":
            roles.update({
                "left_verification_result_input": args.left_verification_result,
                "left_result_store_input": args.left_result_store,
                "right_verification_result_input": args.right_verification_result,
                "right_result_store_input": args.right_result_store,
            })
            if args.right_package:
                roles["right_package_input"] = args.right_package
            if getattr(args, "right_source_archive_root", None):
                roles["right_source_archive_input"] = args.right_source_archive_root
        else:
            roles.update({
                "verification_result_input": args.verification_result,
                "result_store_input": args.result_store,
            })
        if args.package_command == "export-result":
            roles["export_result_output"] = args.output
        if getattr(args, "source_archive_root", None):
            roles["source_archive_input"] = args.source_archive_root
        PathRolePolicy().validate(
            roles,
            read_only_roles=tuple(role for role in roles if role.endswith("_input")),
        )
    from research_pipeline.packages import (
        load_research_package,
        verify_package_source_provenance,
    )

    package = load_research_package(args.package)
    source_verification = verify_package_source_provenance(
        package,
        getattr(args, "source_archive_root", None),
    )
    if args.package_command == "lint":
        from research_pipeline.packages.lint import build_lint_report

        plan, registry = _compile_lint(package, args)
        report = build_lint_report(
            package,
            plan,
            catalog_lock=getattr(args, "catalog_lock", None),
            source_verification=source_verification,
            resource_summary=_declared_resource_summary(plan, registry),
        )
        return report
    plan = _compile_trusted_context(package, args)
    from research_pipeline.evidence import (
        load_verified_result_context,
        render_verification_report,
        export_verified_result,
    )
    base = {
        "package_id": package.package_id,
        "package_hash": package.package_hash,
        "package_plan_hash": plan.plan_hash,
        "query_count": len(plan.queries),
        "metric_contract_hash": package.metric_contract.contract_hash,
        "claim_contract_hash": package.claim_contract.contract_hash,
        "source_provenance_hash": package.source_provenance_hash,
        "source_verification": source_verification,
    }
    if args.package_command == "compare":
        right_package = package if not args.right_package else load_research_package(args.right_package)
        verify_package_source_provenance(
            right_package,
            getattr(args, "right_source_archive_root", None) or getattr(args, "source_archive_root", None),
        )
        right_plan = plan if not args.right_package else _compile_trusted_context(right_package, args)
        left = load_verified_result_context(
            args.left_verification_result,
            result_store=args.left_result_store,
        )
        right = load_verified_result_context(
            args.right_verification_result,
            result_store=args.right_result_store,
        )
        from research_pipeline.packages.delivery import (
            compare_research_package_results,
        )

        comparison = compare_research_package_results(
            package,
            plan,
            left,
            right,
            right_package=right_package,
            right_plan=right_plan,
        )
        return {**base, "comparison": asdict(comparison)}
    if args.package_command == "report":
        context = load_verified_result_context(
            args.verification_result,
            result_store=args.result_store,
        )
        _validate_package_result(package, plan, context)
        return {
            **base,
            "report": f"# {package.display_name}\n\n{render_verification_report(context)}",
        }
    context = load_verified_result_context(
        args.verification_result,
        result_store=args.result_store,
    )
    _validate_package_result(package, plan, context)
    output = export_verified_result(context, args.output)
    return {**base, "output": str(output)}


def _compile_lint(package, args):
    from research_pipeline.extensions import build_admitted_operator_registry
    from research_pipeline.packages import (
        OPERATOR_GRAPH_BUILDER_ID,
        compile_research_package,
    )
    from research_pipeline.runtime.operator_registry import (
        build_mainline_operator_registry,
    )

    admission = None
    if package.builder_id == OPERATOR_GRAPH_BUILDER_ID:
        admission = build_admitted_operator_registry(
            getattr(args, "extension_bundle", ()),
            builtin_registry=build_mainline_operator_registry(),
        )
    verifier = None
    if getattr(args, "verifier_bundle", None):
        from research_pipeline.extensions import admit_project_verifier_bundle

        verifier = admit_project_verifier_bundle(
            args.verifier_bundle,
            expected_project_id=(
                getattr(admission, "project_id", None)
                or package.spec_payload["research_id"]
            ),
        )
    return compile_research_package(
        package,
        admission=admission,
        verifier_admission=verifier,
    ), admission


def _compile_trusted_context(package, args):
    return _compile_lint(package, args)[0]


def _declared_resource_summary(plan, registry) -> dict[str, object]:
    """只汇总 OperatorDefinition 已声明预算，不猜运行规模或修改研究语义。"""
    if registry is None:
        raise ValueError("package lint 缺少算子注册表")
    specifications = {
        (item.operator_id, item.operator_version): item
        for item in registry.operator_specs
    }
    nodes = []
    for node in plan.recipe.nodes:
        specification = specifications.get((node.operator_id, node.operator_version))
        if specification is None:
            raise ValueError(
                f"package lint 缺少算子资源声明: {node.operator_id}@{node.operator_version}"
            )
        nodes.append(
            {
                "node_id": node.node_id,
                "operator_id": node.operator_id,
                "operator_version": node.operator_version,
                "resource_profile": dict(specification.resource_profile),
            }
        )
    profiles = [item["resource_profile"] for item in nodes]
    return {
        "status": "declared",
        "basis": "operator_definition.resource_profile",
        "nodes": nodes,
        "summary": {
            "max_memory_bytes": max(int(item["memory_bytes"]) for item in profiles),
            "total_temp_bytes": sum(int(item["temp_bytes"]) for item in profiles),
            "max_cpu_slots": max(int(item["cpu_slots"]) for item in profiles),
            "max_wall_seconds": max(int(item["wall_seconds"]) for item in profiles),
        },
        "changed_research_semantics": False,
    }


def _validate_package_result(package, plan, context) -> None:
    bundle = context.snapshot.bundle
    if bundle.package_hash != package.package_hash or bundle.plan_hash != plan.plan_hash:
        raise ValueError("VerificationResult 不属于当前 ResearchPackage/plan")
    claim = context.verification.claim_level
    if claim not in package.claim_contract.allowed_claim_levels:
        raise ValueError("VerificationResult claim 超过 ResearchPackage 允许范围")


__all__ = ["execute"]
