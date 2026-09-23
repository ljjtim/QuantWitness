"""在 CLI 编排层从正式 registry、Catalog Lock 与 compiler 派生发现结果。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from research_pipeline.operations.capabilities import require_available_capability
from research_pipeline.platform import (
    MainlineError,
    canonical_json,
    typed_canonical_hash,
)

if TYPE_CHECKING:
    from research_pipeline.catalog import CompiledCatalog


MACHINE_DISCOVERY_VERSION = "research-machine-discovery-v2"
DISCOVERY_FAILURE_VERSION = "research-machine-discovery-failure-v1"


class DiscoveryRequestError(MainlineError):
    """机器发现请求无法由正式 registry 闭合。"""

    error_code = "discovery_request_invalid"

    def __init__(
        self,
        message: str,
        *,
        missing_requirements: tuple[str, ...],
        next_commands: tuple[str, ...],
    ) -> None:
        super().__init__(message)
        if not missing_requirements or not next_commands:
            raise ValueError("discovery failure 必须给出缺失项和下一命令")
        self.failure_payload = {
            "contract_version": DISCOVERY_FAILURE_VERSION,
            "code": self.error_code,
            "missing_requirements": sorted(set(missing_requirements)),
            "next_commands": list(dict.fromkeys(next_commands)),
        }


class DiscoveryNotFoundError(DiscoveryRequestError):
    error_code = "discovery_not_found"


def operator_list() -> dict[str, object]:
    manifest = _build_operator_manifest()
    availability_state, sealed = _discovery_availability()
    items = [
        {
            "operator_id": definition.name,
            "operator_version": definition.version,
            "capability": definition.implementation_ref.capability,
            "input_artifact_types": [
                item.artifact_type for item in definition.input_schema
            ],
            "output_artifact_types": [
                item.artifact_type for item in definition.output_schema
            ],
            "availability_state": availability_state,
            "sealed": sealed,
            "definition_hash": definition.definition_hash,
            **_operator_governance(definition),
        }
        for definition in manifest.definitions
    ]
    return _payload(
        "operator.list",
        source_identity=manifest.manifest_hash,
        items=items,
        next_commands=(
            "python -m research_pipeline operator describe <operator-id> --format json",
        ),
    )


def operator_describe(operator_id: str) -> dict[str, object]:
    manifest = _build_operator_manifest()
    availability_state, sealed = _discovery_availability()
    matches = [item for item in manifest.definitions if item.name == operator_id]
    if not matches:
        raise DiscoveryNotFoundError(
            f"未找到 operator: {operator_id}",
            missing_requirements=("registered_operator_id",),
            next_commands=("python -m research_pipeline operator list --format json",),
        )
    items = []
    for definition in matches:
        items.append(
            {
                "operator_id": definition.name,
                "operator_version": definition.version,
                "operator_spec": definition.operator_spec.to_dict(),
                "implementation": {
                    "implementation_id": definition.implementation_ref.implementation_id,
                    "capability": definition.implementation_ref.capability,
                    "code_hash": definition.implementation_ref.code_hash,
                    "implementation_scope": definition.implementation_ref.implementation_scope,
                },
                "cache_compatibility_mode": definition.cache_compatibility_mode,
                "partition_keys": list(definition.partition_keys),
                "availability_state": availability_state,
                "sealed": sealed,
                "definition_hash": definition.definition_hash,
                **_operator_governance(definition),
            }
        )
    return _payload(
        "operator.describe",
        source_identity=manifest.manifest_hash,
        items=items,
        next_commands=(
            "python -m research_pipeline artifact describe <artifact-type> --format json",
            "python -m research_pipeline recipe list --format json",
        ),
    )


def artifact_describe(artifact_type: str) -> dict[str, object]:
    manifest = _build_operator_manifest()
    availability_state, sealed = _discovery_availability()
    producers = []
    consumers = []
    for definition in manifest.definitions:
        for port in definition.output_schema:
            if port.artifact_type == artifact_type:
                producers.append(_port_binding(definition, port.port))
        for port in definition.input_schema:
            if port.artifact_type == artifact_type:
                consumers.append(_port_binding(definition, port.port))
    if not producers and not consumers:
        raise DiscoveryNotFoundError(
            f"未找到 artifact type: {artifact_type}",
            missing_requirements=("registered_artifact_type",),
            next_commands=("python -m research_pipeline operator list --format json",),
        )
    pit_capabilities = sorted(
        {
            capability
            for definition in manifest.definitions
            if any(
                port.artifact_type == artifact_type for port in definition.output_schema
            )
            for capability in definition.operator_spec.pit_capabilities
        }
    )
    item = {
        "artifact_type": artifact_type,
        "artifact_version": artifact_type.rsplit(".", 1)[-1],
        "producers": producers,
        "consumers": consumers,
        "schema": {
            "field_source": (
                "catalog.field-contract-v2"
                if artifact_type == "data.columnar-bundle.v1"
                else "producer_operator_output_contract"
            ),
            "unit_source": (
                "catalog_field.unit"
                if artifact_type == "data.columnar-bundle.v1"
                else "producer_artifact_schema"
            ),
            "time_semantics_source": (
                "catalog_field.availability_policy"
                if artifact_type == "data.columnar-bundle.v1"
                else "producer_operator.pit_capabilities"
            ),
            "pit_capabilities": pit_capabilities,
            "fields": [],
            "fields_status": "resolve_from_declared_schema_source",
        },
        "availability_state": availability_state,
        "sealed": sealed,
    }
    return _payload(
        "artifact.describe",
        source_identity=manifest.manifest_hash,
        items=(item,),
        next_commands=(
            "python -m research_pipeline catalog field search <query> --format json",
        ),
    )


def recipe_list() -> dict[str, object]:
    from research_pipeline.packages.reference_transform_recipes import (
        build_reference_transform_recipes,
    )

    recipes = build_reference_transform_recipes()
    identity = typed_canonical_hash([_recipe_item(item) for item in recipes])
    next_commands = (
        ("python -m research_pipeline recipe describe <recipe-id> --format json",)
        if recipes
        else (
            "python -m research_pipeline package init <new-package-dir> --json",
            "python -m research_pipeline operator scaffold --project-id <project-id> "
            "--operator-id <operator-id> --output <new-extension-dir> --format json",
        )
    )
    return _payload(
        "recipe.list",
        source_identity=identity,
        items=[
            {
                "recipe_id": item.reference_id,
                "research_kind": item.research_kind,
                "execution_ready": item.execution_ready,
                "availability_state": _recipe_availability(item),
                "sealed": False,
                "graph_hash": _recipe_hash(item),
            }
            for item in recipes
        ],
        next_commands=next_commands,
    )


def recipe_describe(recipe_id: str) -> dict[str, object]:
    recipe = _require_recipe(recipe_id)
    item = _recipe_item(recipe)
    item.update(
        {
            "availability_state": _recipe_availability(recipe),
            "sealed": False,
            "missing_requirements": [
                item.key
                for item in recipe.questions
                if item.required and item.default is None
            ],
        }
    )
    return _payload(
        "recipe.describe",
        source_identity=_recipe_hash(recipe),
        items=(item,),
        next_commands=(
            f"python -m research_pipeline recipe scaffold {recipe.reference_id} --answers <answers.json> --output <new-package-dir> --format json",
        ),
    )


def recipe_scaffold(
    recipe_id: str,
    *,
    output: str | Path | None = None,
    catalog_lock: str | Path,
    assignments: tuple[str, ...] = (),
    answers_file: str | Path | None = None,
) -> dict[str, object]:
    from research_pipeline.catalog import CompiledCatalog
    from research_pipeline.packages.reference_transform_recipes import (
        RecipeScaffoldError,
        scaffold_research_package,
    )
    from research_pipeline.runtime.operator_registry import (
        build_mainline_operator_registry,
    )

    recipe = _require_recipe(recipe_id)
    if output is None:
        raise DiscoveryRequestError(
            "recipe scaffold 必须指定新的 ResearchPackage 目录",
            missing_requirements=("output",),
            next_commands=(
                f"python -m research_pipeline recipe scaffold {recipe_id} --answers <answers.json> --output <new-package-dir> --format json",
            ),
        )
    answers: dict[str, object] = {}
    if answers_file is not None:
        try:
            loaded = json.loads(Path(answers_file).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DiscoveryRequestError(
                f"recipe answers 文件无法读取: {exc}",
                missing_requirements=("valid_answers_json",),
                next_commands=(
                    f"python -m research_pipeline recipe describe {recipe_id} --format json",
                ),
            ) from exc
        if not isinstance(loaded, dict):
            raise DiscoveryRequestError(
                "recipe answers 必须是 JSON 对象",
                missing_requirements=("answers_json_object",),
                next_commands=(
                    f"python -m research_pipeline recipe describe {recipe_id} --format json",
                ),
            )
        answers.update(loaded)
    for assignment in assignments:
        key, separator, raw = assignment.partition("=")
        if not separator or not key.strip() or not raw.strip():
            raise DiscoveryRequestError(
                f"recipe --set 无效: {assignment}",
                missing_requirements=("key_value_assignment",),
                next_commands=(
                    f"python -m research_pipeline recipe describe {recipe_id} --format json",
                ),
            )
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = raw
        answers[key.strip()] = value
    try:
        scaffold = scaffold_research_package(
            recipe,
            answers=answers,
            output=output,
            admission=build_mainline_operator_registry(),
            dataset_versions={
                dataset_id: int(dataset["dataset_version"])
                for dataset_id, dataset in sorted(
                    CompiledCatalog.load(catalog_lock).datasets.items()
                )
            },
        )
    except RecipeScaffoldError as exc:
        missing = exc.missing_requirements or ("valid_recipe_answers",)
        raise DiscoveryRequestError(
            str(exc),
            missing_requirements=missing,
            next_commands=(
                f"python -m research_pipeline recipe describe {recipe_id} --format json",
                f"python -m research_pipeline recipe scaffold {recipe_id} --answers <answers.json> --output <new-package-dir> --format json",
            ),
        ) from exc
    return _payload(
        "recipe.scaffold",
        source_identity=_recipe_hash(recipe),
        items=(scaffold,),
        next_commands=tuple(scaffold["next_commands"]),
    )


def catalog_search(
    kind: str,
    query: str,
    *,
    catalog_lock: str | Path | None = None,
) -> dict[str, object]:
    normalized = query.strip().casefold()
    if not normalized:
        raise DiscoveryRequestError(
            "catalog search query 不能为空",
            missing_requirements=("non_empty_query",),
            next_commands=(
                f"python -m research_pipeline catalog {kind} search <query> --format json",
            ),
        )
    resolved_lock, catalog = _load_catalog_for_search(
        catalog_lock,
        kind=kind,
    )
    blocked_items = _blocked_catalog_items(resolved_lock, kind)
    tokens = tuple(normalized.split())

    def matches(item) -> bool:
        searchable = canonical_json(dict(item)).casefold()
        return all(token in searchable for token in tokens)

    relevant_bindings: list[dict[str, object]] = []
    if kind == "dataset":
        source = catalog.datasets.values()
        items = []
        for item in source:
            if not matches(item):
                continue
            dataset = dict(item)
            bindings = _dataset_binding_summaries(catalog, dataset)
            dataset["bindings"] = bindings
            relevant_bindings.extend(bindings)
            items.append(dataset)
    elif kind == "field":
        source = catalog.fields.values()
        items = [dict(item) for item in source if matches(item)]
        matched_field_ids = {
            str(item["field_id"]) for item in items if item.get("status") != "blocked"
        }
        for dataset in catalog.datasets.values():
            if not matched_field_ids.intersection(dataset["fields"]):
                continue
            relevant_bindings.extend(_dataset_binding_summaries(catalog, dict(dataset)))
    else:
        raise DiscoveryRequestError(
            f"未知 catalog search kind: {kind}",
            missing_requirements=("dataset_or_field",),
            next_commands=(
                "python -m research_pipeline catalog dataset search <query> --format json",
                "python -m research_pipeline catalog field search <query> --format json",
            ),
        )
    for item in blocked_items:
        if matches(item):
            if kind == "dataset":
                item["bindings"] = []
            items.append(item)
    items.sort(key=lambda item: str(item.get(f"{kind}_id", item.get("target_id", ""))))
    catalog_context = _catalog_context(catalog, resolved_lock)
    return _payload(
        f"catalog.{kind}.search",
        source_identity=catalog.catalog_hash,
        items=items,
        next_commands=_catalog_next_commands(
            catalog_context,
            relevant_bindings,
        ),
        catalog_context=catalog_context,
    )


def _load_catalog_for_search(
    catalog_lock: str | Path | None,
    *,
    kind: str,
) -> tuple[Path, CompiledCatalog]:
    from research_pipeline.catalog import CompiledCatalog

    if catalog_lock is None:
        raise DiscoveryRequestError(
            "Catalog 搜索必须显式提供持久 Catalog Lock",
            missing_requirements=("stable_catalog_lock_reference",),
            next_commands=(
                f"python -m research_pipeline catalog {kind} search <query> "
                "--catalog-lock <持久Catalog-Lock目录> --format json",
            ),
        )
    resolved_lock = Path(catalog_lock).resolve()
    return resolved_lock, CompiledCatalog.load(resolved_lock)


def _dataset_binding_summaries(
    catalog: CompiledCatalog,
    dataset: dict[str, object],
) -> list[dict[str, object]]:
    summaries = [
        {
            "binding_id": str(binding["binding_id"]),
            "binding_version": int(binding["binding_version"]),
            "dataset_id": str(binding["dataset_id"]),
            "dataset_version": int(binding["dataset_version"]),
            "source_profile": str(binding["source_profile"]),
            "environment": str(binding["environment"]),
            "object_name": str(binding["object_name"]),
            "status": str(binding["status"]),
        }
        for binding in catalog.bindings.values()
        if binding["dataset_id"] == dataset["dataset_id"]
        and binding["dataset_version"] == dataset["dataset_version"]
        and binding["status"] == "approved"
    ]
    return sorted(
        summaries,
        key=lambda item: (
            str(item["source_profile"]),
            str(item["environment"]),
            str(item["binding_id"]),
            int(item["binding_version"]),
        ),
    )


def _catalog_context(
    catalog: CompiledCatalog,
    resolved_lock: Path,
) -> dict[str, object]:
    source_pairs = sorted(
        {
            (str(binding["source_profile"]), str(binding["environment"]))
            for binding in catalog.bindings.values()
            if binding["status"] == "approved"
        }
    )
    return {
        "lock_reference": str(resolved_lock),
        "compile_id": str(catalog.payload["compile_id"]),
        "catalog_hash": catalog.catalog_hash,
        "source_profiles": [
            {
                "source_profile": profile,
                "environment": environment,
                "cli_option": "--data-db" if profile == "source" else "--source-db",
                "cli_value_template": (
                    f"<{profile}:{environment}只读DuckDB>"
                    if profile == "source"
                    else f"{profile}=<{profile}:{environment}只读DuckDB>"
                ),
            }
            for profile, environment in source_pairs
        ],
    }


def _catalog_next_commands(
    catalog_context: dict[str, object],
    relevant_bindings: list[dict[str, object]],
) -> tuple[str, ...]:
    lock_argument = f'--catalog-lock "{catalog_context["lock_reference"]}"'
    commands = [
        "python -m research_pipeline recipe list --format json",
        "python -m research_pipeline package lint --package <package-dir> "
        f"{lock_argument} --json",
    ]
    source_pairs = sorted(
        {
            (str(binding["source_profile"]), str(binding["environment"]))
            for binding in relevant_bindings
        }
    )
    if source_pairs:
        source_arguments = []
        for profile, environment in source_pairs:
            placeholder = f"<{profile}:{environment}只读DuckDB>"
            if profile == "source":
                source_arguments.append(f"--data-db {placeholder}")
            else:
                source_arguments.append(f'--source-db "{profile}={placeholder}"')
        commands.append(
            "python -m research_pipeline package admit --package <package-dir> "
            f"{lock_argument} {' '.join(source_arguments)} "
            "--output <new-plan-dir> --json"
        )
    return tuple(commands)


def _blocked_catalog_items(root: Path, kind: str) -> tuple[dict[str, object], ...]:
    compile_id = (root / "CURRENT").read_text(encoding="utf-8").strip()
    audit = json.loads(
        (root / compile_id / "catalog.audit.json").read_text(encoding="utf-8")
    )
    target_kind = "dataset" if kind == "dataset" else "field"
    return tuple(
        {
            f"{target_kind}_id": item["target_id"],
            "status": "blocked",
            "reason": item["reason"],
            "evidence_refs": list(item.get("evidence_refs", ())),
        }
        for item in audit.get("blocked_entries", ())
        if item.get("target_kind") == target_kind
    )


def _require_recipe(recipe_id: str):
    from research_pipeline.packages.reference_transform_recipes import (
        build_reference_transform_recipes,
    )

    matches = [
        item
        for item in build_reference_transform_recipes()
        if item.reference_id == recipe_id
    ]
    if len(matches) != 1:
        raise DiscoveryNotFoundError(
            f"未找到 recipe: {recipe_id}",
            missing_requirements=("registered_recipe_id",),
            next_commands=(
                "python -m research_pipeline recipe list --format json",
                "python -m research_pipeline package init <new-package-dir> --json",
            ),
        )
    return matches[0]


def _recipe_item(recipe) -> dict[str, object]:
    return {
        "recipe_id": recipe.reference_id,
        "research_kind": recipe.research_kind,
        "execution_ready": recipe.execution_ready,
        "contract_version": recipe.contract_version,
        "frequency": recipe.frequency,
        "supported_asset_profiles": list(recipe.supported_asset_profiles),
        "questions": [item.to_dict() for item in recipe.questions],
        "graph": dict(recipe.graph),
        "graph_hash": _recipe_hash(recipe),
    }


def _recipe_hash(recipe) -> str:
    from research_pipeline.packages.reference_transform_recipes import recipe_identity

    return recipe_identity(recipe)


def _recipe_availability(recipe) -> str:
    return "local_only" if recipe.execution_ready else "planned"


def _discovery_availability() -> tuple[str, bool]:
    descriptor = require_available_capability("capability.discovery")
    state = str(descriptor["state"])
    return state, state == "sealed"


def _port_binding(definition, port: str) -> dict[str, object]:
    return {
        "operator_id": definition.name,
        "operator_version": definition.version,
        "port": port,
        "definition_hash": definition.definition_hash,
    }


def _payload(
    kind: str,
    *,
    source_identity: str,
    items,
    next_commands: tuple[str, ...],
    catalog_context: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_items = list(items)
    values = {
        "contract_version": MACHINE_DISCOVERY_VERSION,
        "kind": kind,
        "source_identity": source_identity,
        "items": normalized_items,
        "next_commands": list(next_commands),
    }
    if catalog_context is not None:
        values["catalog_context"] = catalog_context
    return {**values, "payload_hash": typed_canonical_hash(values)}


def _operator_governance(definition) -> dict[str, object]:
    from research_pipeline.runtime.operator_promotion import (
        operator_mainline_governance,
    )

    return operator_mainline_governance(definition)


def _build_operator_manifest():
    from research_pipeline.runtime.operator_definitions import (
        build_mainline_operator_manifest,
    )
    return build_mainline_operator_manifest()


__all__ = [
    "DISCOVERY_FAILURE_VERSION",
    "MACHINE_DISCOVERY_VERSION",
    "DiscoveryNotFoundError",
    "DiscoveryRequestError",
    "artifact_describe",
    "catalog_search",
    "operator_describe",
    "operator_list",
    "recipe_describe",
    "recipe_list",
    "recipe_scaffold",
]
