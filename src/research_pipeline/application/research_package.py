"""无版本 ResearchPackage 命令。"""

from __future__ import annotations

from .command_suggestion import command_suggestion


def _execute(args, *, documents=None) -> dict[str, object]:
    if args.package_command == "init":
        from research_pipeline.packages import initialize_research_package

        path = initialize_research_package(args.destination)
        return {
            "package_path": str(path),
            "next_action": "填写来源、本土化和研究规格后运行 package lint。",
            **command_suggestion(
                "python", "-m", "research_pipeline", "package", "lint",
                "--package", str(path), "--json",
            ),
        }
    if args.package_command == "expand-variants":
        from research_pipeline.packages.variants import (
            expand_research_package_variants,
        )

        return expand_research_package_variants(
            base_root=args.base,
            manifest_path=args.manifest,
            output_root=args.output_root,
        )
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
        return {
            **result,
            "next_action": "运行 package lint --source-archive-root 重新核验归档正文。",
            **command_suggestion(
                "python", "-m", "research_pipeline", "package", "lint",
                "--package", args.package,
                "--source-archive-root", args.archive_root,
                "--json",
            ),
        }
    if args.package_command == "lint" and documents is None:
        from research_pipeline.packages import read_package_documents
        documents = read_package_documents(args.package)
    if args.package_command == "admit":
        from .research_plan import admit_package

        return admit_package(args, documents=documents)
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
    from research_pipeline.packages import verify_package_source_provenance
    from research_pipeline.packages.lint import build_lint_report, validate_lint_declarations

    package, queries, recipe, semantics = validate_lint_declarations(args.package, documents=documents)
    source_verification = verify_package_source_provenance(package, getattr(args, "source_archive_root", None))
    plan, registry = _compile_lint(package, args, query_result=queries, recipe=recipe, research_semantics=semantics)
    return build_lint_report(
        package, plan, catalog_lock=getattr(args, "catalog_lock", None),
        source_verification=source_verification, resource_summary=_declared_resource_summary(plan, registry),
    )


def _compile_lint(package, args, *, query_result=None, recipe=None, research_semantics=None):
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
    if query_result is not None and package.builder_id == OPERATOR_GRAPH_BUILDER_ID:
        from research_pipeline.packages.operator_graph import compile_operator_graph_package

        return compile_operator_graph_package(
            package, query_result=query_result, admission=admission,
            recipe=recipe, research_semantics=research_semantics,
        ), admission
    return compile_research_package(
        package,
        admission=admission,
    ), admission


def _declared_resource_summary(plan, registry) -> dict[str, object]:
    """准备后使用冻结 DAG，lint 使用算子声明；不猜规模或改预算。"""
    if registry is None:
        raise ValueError("package lint 缺少算子注册表")
    from research_pipeline.extensions.project_admission import ProjectOperatorImplementationToken
    from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest

    from research_pipeline.runtime.operator_registry import effective_node_resource_profile
    manifest = build_mainline_operator_manifest()
    specifications = {
        (item.operator_id, item.operator_version): item
        for item in registry.operator_specs
    }
    dag = getattr(plan, "dag", None)
    frozen_budgets = {} if dag is None else {
        node.node_id: node.resource_budget.to_dict() for node in dag.nodes
    }
    nodes = []
    for node in plan.recipe.nodes:
        specification = specifications.get((node.operator_id, node.operator_version))
        if specification is None:
            raise ValueError(
                f"package lint 缺少算子资源声明: {node.operator_id}@{node.operator_version}"
            )
        binding = registry.binding(node.operator_id, node.operator_version)
        if isinstance(binding.implementation_token, ProjectOperatorImplementationToken):
            implementation_scope = "project"
        else:
            implementation_scope = manifest.require_operator(
                node.operator_id, node.operator_version,
            ).implementation_ref.implementation_scope
        nodes.append(
            {
                "node_id": node.node_id,
                "operator_id": node.operator_id,
                "operator_version": node.operator_version,
                "implementation_scope": implementation_scope,
                "resource_profile": {
                    **effective_node_resource_profile(node, specification),
                    **(frozen_budgets[node.node_id] if dag is not None else {}),
                },
            }
        )
    profiles = [item["resource_profile"] for item in nodes]
    return {
        "status": "declared",
        "basis": "frozen_dag.resource_budget" if dag is not None else "operator_definition.resource_profile",
        "nodes": nodes,
        "summary": {
            "max_memory_bytes": max(int(item["memory_bytes"]) for item in profiles),
            "total_temp_bytes": sum(int(item["temp_bytes"]) for item in profiles),
            "max_cpu_slots": max(int(item["cpu_slots"]) for item in profiles),
            "max_process_slots": max(
                int(item["process_slots"]) for item in profiles
            ),
            "max_wall_seconds": max(int(item["wall_seconds"]) for item in profiles),
        },
        "changed_research_semantics": False,
    }


__all__ = ["_execute"]
