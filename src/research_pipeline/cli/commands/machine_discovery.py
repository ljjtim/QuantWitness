"""operator、artifact 与 recipe 的稳定机器发现命令。"""

from __future__ import annotations

from research_pipeline.cli.discovery import (
    artifact_describe,
    operator_describe,
    operator_list,
    recipe_describe,
    recipe_list,
    recipe_scaffold,
)
from ..result import execute_guarded


def execute(args) -> int:
    args.json = args.format == "json"
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    if args.command == "operator":
        if args.operator_command == "list":
            return operator_list()
        if args.operator_command == "describe":
            return operator_describe(args.operator_id)
        if args.operator_command == "scaffold":
            return _project_operator_scaffold(args)
        return _project_operator_bundle(args)
    if args.command == "artifact":
        return artifact_describe(args.artifact_type)
    if args.recipe_command == "list":
        return recipe_list()
    if args.recipe_command == "describe":
        return recipe_describe(args.recipe_id)
    return recipe_scaffold(
        args.recipe_id,
        output=args.output,
        catalog_lock=args.catalog_lock,
        assignments=tuple(args.set or ()),
        answers_file=args.answers,
    )


def _project_operator_bundle(args) -> dict[str, object]:
    import tempfile

    from research_pipeline.extensions import (
        compile_project_operator_bundle,
        load_project_operator_declaration,
        verify_project_operator_bundle,
    )
    from research_pipeline.runtime.operator_definitions import (
        build_mainline_operator_manifest,
    )

    declaration = load_project_operator_declaration(
        args.spec,
        source_root=args.source,
    )
    registered_operator_specs = tuple(
        definition.operator_spec
        for definition in build_mainline_operator_manifest().definitions
    )
    output = getattr(args, "output", None)

    def compile_bundle(output_root):
        return compile_project_operator_bundle(
            source_root=args.source,
            output_root=output_root,
            project_id=declaration.project_id,
            operator_spec=declaration.operator_spec,
            entry_module=declaration.entry_module,
            entry_function=declaration.entry_function,
            dependency_lock=declaration.dependency_lock,
            registered_operator_specs=registered_operator_specs,
            project_artifact_types=declaration.project_artifact_types,
            permissions=declaration.permissions,
        )

    if args.operator_command == "validate":
        with tempfile.TemporaryDirectory(prefix="research-operator-validate-") as temporary:
            bundle = compile_bundle(temporary)
            manifest = verify_project_operator_bundle(bundle)
        bundle_path = None
    else:
        bundle = compile_bundle(output)
        manifest = verify_project_operator_bundle(bundle)
        bundle_path = str(bundle)
    return {
        "kind": f"operator.{args.operator_command}",
        "project_id": manifest.project_id,
        "operator_id": manifest.operator_spec.operator_id,
        "operator_version": manifest.operator_spec.operator_version,
        "operator_spec_hash": manifest.operator_spec.spec_hash,
        "implementation_kind": "python_worker",
        "source_tree_hash": getattr(manifest, "source_tree_hash", None),
        "bundle_hash": manifest.bundle_hash,
        "bundle_path": bundle_path,
        "artifact_types": sorted({
            item.artifact_type
            for item in (*manifest.operator_spec.input_ports, *manifest.operator_spec.output_ports)
        }),
        "next_action": (
            "使用 operator build --spec <声明> --output <目录> 生成 bundle；Python 项目算子另传 --source。"
            if args.operator_command == "validate"
            else "使用 --extension-bundle 显式传入 package lint/admit；Plan 会封存已准入 bundle，运行和恢复不再重复传入。"
        ),
    }


def _project_operator_scaffold(args) -> dict[str, object]:
    from research_pipeline.extensions.project_scaffold import scaffold_project_operator
    from research_pipeline.runtime.operator_definitions import (
        build_mainline_operator_manifest,
    )

    registered_operator_specs = tuple(
        definition.operator_spec
        for definition in build_mainline_operator_manifest().definitions
    )
    scaffold = scaffold_project_operator(
        args.output,
        project_id=args.project_id,
        operator_id=args.operator_id,
        operator_version=args.operator_version,
        registered_operator_specs=registered_operator_specs,
    )
    payload = scaffold.to_dict()
    payload["kind"] = "operator.scaffold"
    payload["next_commands"] = [
        "python -m research_pipeline operator validate "
        f'--spec "{scaffold.declaration_path}" '
        f'--source "{scaffold.source_root}" --format json',
        "python -m research_pipeline operator build "
        f'--spec "{scaffold.declaration_path}" '
        f'--source "{scaffold.source_root}" --output <bundle父目录> --format json',
    ]
    return payload


__all__ = ["execute"]
