from __future__ import annotations

import argparse
from pathlib import Path

from research_pipeline.catalog import (
    CatalogPreflight,
    CompiledCatalog,
    DuckDBSourceInspector,
    ParquetSourceInspector,
    compile_catalog,
    load_declarative_catalog,
    render_catalog_docs,
    validate_contract_set,
)
from research_pipeline.platform import canonical_json
from research_pipeline.cli.discovery import catalog_search
from research_pipeline.data_plane.path_policy import PathRolePolicy

from ..result import execute_guarded


def execute(args: argparse.Namespace) -> int:
    if args.catalog_command in {"dataset", "field"}:
        args.json = args.format == "json"
        return execute_guarded(
            args,
            lambda _: catalog_search(
                args.catalog_command,
                args.query,
                catalog_lock=args.catalog_lock,
            ),
        )
    handler = {
        "discover": _discover,
        "validate": _validate,
        "compile": _compile,
        "drift": _drift,
        "docs": _docs,
    }[args.catalog_command]
    return execute_guarded(args, handler)


def _catalog(args: argparse.Namespace):
    return load_declarative_catalog(
        args.definition,
        approval_path=getattr(args, "approvals", None),
    )


def _inspector(args: argparse.Namespace):
    if args.source_kind == "duckdb":
        return DuckDBSourceInspector(
            args.source,
            source_profile=args.profile,
            environment=args.environment,
        )
    return ParquetSourceInspector(
        args.source,
        source_profile=args.profile,
        environment=args.environment,
    )


def _discover(args: argparse.Namespace) -> dict[str, object]:
    output = Path(args.output)
    if output.resolve() == Path(args.source).resolve():
        raise ValueError("discover 输出不能覆盖输入数据源")
    PathRolePolicy().validate(
        {"source_input": args.source, "inventory_output": output},
        read_only_roles=("source_input",),
    )
    inventory = _inspector(args).observe_current_schema(args.object)
    payload = {
        "source_kind": inventory.source_kind,
        "source_profile": inventory.source_profile,
        "environment": inventory.environment,
        "object_name": inventory.object_name,
        "schema_revision": inventory.schema_revision,
        "derived": inventory.derived,
        "columns": [item.to_dict() for item in inventory.columns],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(canonical_json(payload), encoding="utf-8")
    return {"inventory": payload, "output": str(output)}


def _validate(args: argparse.Namespace) -> dict[str, object]:
    catalog = _catalog(args)
    validate_contract_set(catalog.contracts)
    decision_targets = {(item.target_kind, item.target_id) for item in catalog.decisions}
    missing_slots = [
        slot
        for slot in catalog.baseline.required_slots
        if ("coverage_slot", slot) not in decision_targets
    ]
    if missing_slots:
        raise ValueError(f"coverage slot 缺少决定: {missing_slots}")
    return {
        "contract_count": len(catalog.contracts),
        "decision_count": len(catalog.decisions),
        "coverage_slot_count": len(catalog.baseline.required_slots),
        "baseline_hash": catalog.baseline.content_hash,
    }


def _compile(args: argparse.Namespace) -> dict[str, object]:
    roles = {f"definition_{index}_input": path for index, path in enumerate(args.definition)}
    if args.approvals:
        roles["approvals_input"] = args.approvals
    PathRolePolicy().validate(
        {**roles, "release_output": args.output},
        read_only_roles=tuple(roles),
    )
    catalog = _catalog(args)
    if not catalog.decisions:
        raise ValueError("compile 必须提供独立 --approvals 审批文件")
    compiled = compile_catalog(
        baseline=catalog.baseline,
        expected_baseline_hash=catalog.baseline.content_hash,
        manifest=catalog.manifest,
        contracts=catalog.contracts,
        decisions=catalog.decisions,
        release_root=args.output,
    )
    return {
        "compile_id": compiled.payload["compile_id"],
        "catalog_hash": compiled.catalog_hash,
        "release": str(Path(args.output)),
    }


def _drift(args: argparse.Namespace) -> dict[str, object]:
    catalog = CompiledCatalog.load(args.release)
    binding, attestation = CatalogPreflight(catalog).resolve_current_binding(
        inspector=_inspector(args),
        dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
        source_profile=args.profile,
        environment=args.environment,
        binding_version=args.binding_version,
    )
    return {
        "binding_id": binding["binding_id"],
        "attestation": attestation.to_dict(),
        "attestation_hash": attestation.attestation_hash,
    }


def _docs(args: argparse.Namespace) -> dict[str, object]:
    release = Path(args.release).resolve()
    output = Path(args.output).resolve()
    if output == release or release in output.parents:
        raise ValueError("docs 输出不能写入不可变 release 目录")
    PathRolePolicy().validate(
        {"release_input": args.release, "docs_output": args.output},
        read_only_roles=("release_input",),
    )
    return render_catalog_docs(args.release, args.output)
