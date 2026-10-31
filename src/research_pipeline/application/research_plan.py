"""ResearchPackage 到 Catalog 准入计划的唯一命令。"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

from research_pipeline.catalog import CatalogPreflight, DuckDBSourceInspector
from research_pipeline.data_plane import (
    DataPlaneExecutionBudget,
    InstantRangeV2,
    PathRolePolicy,
    QueryPurpose,
    admit_query,
)
from research_pipeline.data_plane.execution_budget import (
    bind_data_plane_request_budgets,
    validate_provider_execution_budget,
)
from research_pipeline.data_plane.execution_estimate import build_execution_estimate
from research_pipeline.data_plane.factor_publication import (
    observe_factor_publication,
    require_factor_query_covered,
)
from research_pipeline.data_plane.query_ir import source_local_naive
from research_pipeline.data_plane.providers.sql import (
    compile_duckdb_scope_statistics,
)
from research_pipeline.data_plane.service import load_compiled_catalog
from research_pipeline.packages import (
    OPERATOR_GRAPH_BUILDER_ID,
    OperatorGraphPlan,
    compile_package_queries,
    load_research_package,
    verify_package_source_provenance,
)
from research_pipeline.platform import MainlineError, typed_canonical_hash
from research_pipeline.platform.operator_contracts import OperatorGraphRecipe
from research_pipeline.platform.metric_contracts import MetricDefinition
from research_pipeline.runtime import ResourceBudget
from research_pipeline.packages.operator_graph import (
    _validate_operator_graph_spec, compile_operator_graph_package,
)
from research_pipeline.runtime.operator_registry import (
    build_mainline_operator_registry,
    compile_operator_graph_dag,
)
from research_pipeline.extensions import build_admitted_operator_registry

from .research_plan_store import publish_operator_graph_research_plan


PLAN_FAILURE_VERSION = "research-plan-failure-v1"


class PlanRequirementsMissingError(MainlineError):
    """计划门禁不变，只为机器消费者补充稳定缺失项。"""

    error_code = "plan_requirements_missing"

    def __init__(
        self,
        message: str,
        *missing_requirements: str,
    ) -> None:
        super().__init__(message)
        normalized = tuple(sorted(set(missing_requirements)))
        if not normalized:
            raise ValueError("PlanFailure missing_requirements 不能为空")
        self.failure_payload = {
            "contract_version": PLAN_FAILURE_VERSION,
            "code": self.error_code,
            "missing_requirements": list(normalized),
            "required_inputs": list(normalized),
        }


def admit_package(
    args: argparse.Namespace,
    *, documents=None,
) -> dict[str, object]:
    """执行当前算子图 ResearchPackage 的正式准入。"""
    return prepare_package(args, documents=documents)[0]


def prepare_package(args, *, documents=None, package=None):
    """编译并发布计划，同次执行直接使用返回的准入对象。"""
    if package is None:
        package = load_research_package(args.package, documents=documents)
    if package.builder_id != OPERATOR_GRAPH_BUILDER_ID:
        raise ValueError("ResearchPackage builder 不受支持；请使用当前 operator graph 模板")
    missing = [
        name for name in ("catalog_lock", "output")
        if not getattr(args, name, None)
    ]
    archive = getattr(args, "input_snapshot_manifest", None)
    if archive and (getattr(args, "data_db", None) or getattr(args, "source_db", None)):
        raise ValueError("--input-snapshot-manifest 与 --data-db/--source-db 互斥")
    if not archive and not getattr(args, "data_db", None) and not getattr(args, "source_db", None):
        missing.append("data_source")
    if (
        any(
            item.provenance.mode == "archived_snapshot"
            for item in getattr(package, "sources", ())
        )
        and not getattr(args, "source_archive_root", None)
    ):
        missing.append("source_archive_root")
    if missing:
        raise PlanRequirementsMissingError(
            "正式 plan 缺少必需输入",
            *missing,
        )
    roles = {
        "package_input": args.package,
        "catalog_input": args.catalog_lock,
        "plan_output": args.output,
    }
    if archive:
        roles["input_snapshot_manifest_input"] = archive
    if getattr(args, "data_db", None):
        roles["source_database_input"] = args.data_db
    for index, binding in enumerate(getattr(args, "source_db", ())):
        _, path = _parse_source_db_binding(binding)
        roles[f"source_database_{index}_input"] = path
    if args.source_archive_root:
        roles["source_archive_input"] = args.source_archive_root
    PathRolePolicy().validate(
        roles,
        read_only_roles=tuple(role for role in roles if role.endswith("_input")),
    )
    source_verification = verify_package_source_provenance(package, args.source_archive_root)
    registry = build_admitted_operator_registry(
        getattr(args, "extension_bundle", ()),
        builtin_registry=build_mainline_operator_registry(),
    )
    return _execute_operator_graph(
        args,
        package,
        registry,
        source_verification,
    )


def _execute_operator_graph(
    args,
    package,
    registry,
    source_verification,
):
    query_result = compile_package_queries(package)
    _reject_audit_query_formal_consumers(package, query_result=query_result)
    catalog = load_compiled_catalog(args.catalog_lock)
    _validate_operator_graph_spec(package.spec_payload)
    recipe = OperatorGraphRecipe.from_dict(package.spec_payload["graph"])
    # 数据预算与最终 DAG 同取 OperatorDefinition，其他节点只在正式编译时构建。
    data_nodes = []
    for node in recipe.nodes:
        if node.operator_id not in {"data.columnar.materialize", "data.minute.scan"}:
            continue
        spec = registry.require_operator(node.operator_id, node.operator_version)
        profile = dict(spec.resource_profile)
        profile.pop("process_slots")
        data_nodes.append(SimpleNamespace(node_id=node.node_id, resource_budget=ResourceBudget(**profile)))
    execution_budgets = bind_data_plane_request_budgets(
        request_queries=dict(zip(query_result.request_ids, query_result.queries, strict=True)),
        recipe_nodes=recipe.nodes,
        dag=SimpleNamespace(nodes=tuple(data_nodes)),
    )
    input_snapshot_manifest = None
    archive_path = getattr(args, "input_snapshot_manifest", None)
    if archive_path:
        from research_pipeline.data_plane.archived_inputs import (
            load_archived_input_manifest, admit_archived_queries,
        )

        input_snapshot_manifest = load_archived_input_manifest(archive_path)
        source_roles = {f"archive_{key}_input": item["root"]
                        for key, item in input_snapshot_manifest["requests"].items()}
        PathRolePolicy().validate(
            {"plan_output": args.output, **source_roles},
            read_only_roles=tuple(source_roles),
        )
        admitted, execution_estimates, platform_admission = admit_archived_queries(
            dict(zip(query_result.request_ids, query_result.queries, strict=True)), catalog=catalog,
            manifest=input_snapshot_manifest, execution_budgets=execution_budgets,
        )
        source_status = {"input_source": "archived_snapshot", "database_opened": False}
    else:
        admitted, execution_estimates, platform_admission, database_unchanged = _auto_admit_queries(
            package, catalog=catalog, data_db=getattr(args, "data_db", None),
            source_db=getattr(args, "source_db", ()), execution_budgets=execution_budgets,
            query_result=query_result,
        )
        source_status = {"database_unchanged": database_unchanged}
    package_plan = compile_operator_graph_package(
        package, query_result=query_result, recipe=recipe,
        admission=registry,
        platform_admission=platform_admission,
    )
    if not isinstance(package_plan, OperatorGraphPlan):
        raise ValueError("operator graph builder 返回了错误计划类型")
    dag = compile_operator_graph_dag(package_plan, registry)
    metric_definitions = tuple(
        MetricDefinition.from_dict(item)
        for item in package.spec_payload.get("metric_definitions", ())
    )
    if any(node.parameters.get("execution_backend") == "lean" for node in recipe.nodes):
        from research_pipeline.research.lean_metrics import lean_metric_definitions_for_nodes
        metric_definitions += tuple(
            item for item in lean_metric_definitions_for_nodes(recipe.nodes)
            if item.metric_ref in package.metric_contract.metrics
        )
    metric = {"contract_hash": package.metric_contract.contract_hash, **package.metric_contract.payload()}
    claim = {"contract_hash": package.claim_contract.contract_hash, **package.claim_contract.payload()}
    output = publish_operator_graph_research_plan(
        args.output,
        package_id=package.package_id,
        minute_scope_inputs=package.minute_scope_inputs,
        plan=package_plan,
        research_identity=package.research_identity_payload(plan_hash=package_plan.plan_hash),
        metric_contract=metric,
        claim_contract=claim,
        admitted_plans=admitted,
        execution_estimates=execution_estimates,
        dag=dag,
        registry=registry,
        input_snapshot_manifest=input_snapshot_manifest,
        catalog=catalog,
        metric_definitions=metric_definitions,
    )
    return {
        "package_id": package.package_id,
        "package_hash": package.package_hash,
        "package_plan_hash": package_plan.plan_hash,
        "registry_hash": package_plan.registry_hash,
        "admission_hash": package_plan.admission_hash,
        "dag_hash": dag.dag_id,
        "query_count": len(admitted),
        **source_status,
        "source_verification": source_verification,
        "output": str(output.directory),
        "execution_ready": True,
        "next_action": "使用 run，提供相同来源、产物目录、固定时钟和 seed。",
    }, output


def _reject_audit_query_formal_consumers(package, *, query_result=None) -> None:
    """AUDIT 查询只能做数据观察，不能成为正式研究或交易结论的祖先。"""
    query_result = query_result if query_result is not None else compile_package_queries(package)
    audit_request_ids = {
        request_id
        for request_id, query in zip(
            query_result.request_ids,
            query_result.queries,
            strict=True,
        )
        if query.purpose == QueryPurpose.AUDIT
    }
    if not audit_request_ids:
        return

    graph = package.spec_payload["graph"]
    nodes = {str(item["node_id"]): item for item in graph["nodes"]}
    descendants: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    for node_id, node in nodes.items():
        for binding in node.get("inputs", ()):
            source_node_id = str(binding["source_node_id"])
            if source_node_id in descendants:
                descendants[source_node_id].append(node_id)

    tainted: dict[str, frozenset[str]] = {}
    for node_id, node in nodes.items():
        if node.get("operator_id") != "data.minute.scan":
            continue
        request_ids = {
            str(item) for item in node.get("parameters", {}).get("request_ids", ())
        }
        matched = frozenset(request_ids & audit_request_ids)
        if matched:
            tainted[node_id] = matched

    formal_prefixes = (
        "finance.",
        "research.features.",
        "research.labels.",
        "research.signals.",
        "research.statistics.",
        "research.validity.",
    )
    pending = list(tainted)
    while pending:
        source_node_id = pending.pop()
        for consumer_id in descendants[source_node_id]:
            request_ids = tainted[source_node_id]
            operator_id = str(nodes[consumer_id]["operator_id"])
            if operator_id.startswith(formal_prefixes):
                raise ValueError(
                    "分钟 AUDIT QueryIR 只能用于数据观察，不能进入正式研究或交易节点；"
                    f"request_ids={sorted(request_ids)}，node_id={consumer_id}，"
                    f"operator_id={operator_id}"
                )
            combined = tainted.get(consumer_id, frozenset()) | request_ids
            if combined != tainted.get(consumer_id):
                tainted[consumer_id] = combined
                pending.append(consumer_id)


def _auto_admit_queries(
    package,
    *,
    catalog,
    data_db,
    source_db,
    execution_budgets: dict[str, DataPlaneExecutionBudget],
    query_result=None,
):
    sources = _source_database_map(data_db=data_db, source_db=source_db)
    query_result = query_result if query_result is not None else compile_package_queries(package)
    database_paths = tuple(sorted({path.resolve() for path in sources.values()}, key=str))
    before = {str(path): _database_fingerprint(path) for path in database_paths}
    attestation_by_binding = {}
    inspectors = {}
    factor_publication_id = None
    admitted = {}
    execution_estimates = {}
    with ExitStack() as sessions:
        for request_id, query in zip(query_result.request_ids, query_result.queries, strict=True):
            execution_budget = execution_budgets.get(request_id)
            if execution_budget is None:
                raise PlanRequirementsMissingError(
                    f"request={request_id} 缺少实际数据节点 ResourceBudget",
                    f"data_plane_resource_budget:{request_id}",
                )
            validate_provider_execution_budget(execution_budget)
            candidates = [
                (key, binding)
                for key, binding in catalog.binding_resolutions.items()
                if key[0] == query.dataset_id
                and key[1] == query.dataset_version
                and key[2] in sources
            ]
            if len(candidates) != 1:
                raise PlanRequirementsMissingError(
                    f"Catalog Lock 无法从显式只读数据源唯一解析 binding: {query.dataset_id}",
                    f"source_profile:{query.dataset_id}",
                )
            key, binding = candidates[0]
            binding_id = str(binding["binding_id"])
            inspector_key = (sources[str(key[2])], str(key[2]), str(key[3]))
            inspector = inspectors.get(inspector_key)
            if inspector is None:
                inspector = sessions.enter_context(DuckDBSourceInspector(
                    inspector_key[0], source_profile=inspector_key[1], environment=inspector_key[2],
                ))
                inspectors[inspector_key] = inspector
            cached = attestation_by_binding.get(binding_id)
            if cached is None:
                resolved, attestation = CatalogPreflight(catalog).resolve_current_binding(
                    inspector=inspector,
                    dataset_id=query.dataset_id,
                    dataset_version=query.dataset_version,
                    source_profile=str(key[2]),
                    environment=str(key[3]),
                    binding_version=int(key[4]),
                )
                cached = (resolved, attestation)
                attestation_by_binding[binding_id] = cached
            resolved, attestation = cached
            factor_publication = (
                observe_factor_publication(
                    sources[str(key[2])],
                    storage_table=str(resolved["object_name"]),
                )
                if str(key[2]) == "factor"
                else None
            )
            if factor_publication is not None:
                if factor_publication_id not in {None, factor_publication.publication_id}:
                    raise PlanRequirementsMissingError(
                        "同一研究计划不能混用不同因子publication",
                        "factor_publication_consistency",
                    )
                factor_publication_id = factor_publication.publication_id
                require_factor_query_covered(
                    factor_publication,
                    storage_table=str(resolved["object_name"]),
                    start=query.time_range.start,
                    end=query.time_range.end,
                )
            plan = admit_query(
                query,
                catalog=catalog,
                binding=resolved,
                attestation=attestation,
                factor_publication=factor_publication,
            )
            scan_fields = plan.temporal_selection.required_scan_fields
            field_types = dict(plan.field_types)
            variable_fields = tuple(
                field_id
                for field_id in scan_fields
                if field_types[field_id].lower() == "string"
            )
            statistics_sql, statistics_parameters = compile_duckdb_scope_statistics(
                plan,
                variable_fields=variable_fields,
            )
            time_range = plan.query.time_range
            if isinstance(time_range, InstantRangeV2):
                range_start = source_local_naive(time_range.start_at)
                range_end = source_local_naive(time_range.end_at)
                range_end_inclusive = False
            else:
                range_start = time_range.start
                range_end = time_range.end
                range_end_inclusive = True
            columns = dict(plan.columns)
            evidence = inspector.observe_execution_evidence(
                plan.object_name,
                projected_columns=columns,
                variable_fields=variable_fields,
                query_scope_hash=typed_canonical_hash(plan.query.to_dict()),
                statistics_sql=statistics_sql,
                statistics_parameters=tuple(statistics_parameters),
                event_column=columns[plan.event_time_field],
                range_start=range_start,
                range_end=range_end,
                range_end_inclusive=range_end_inclusive,
                scope_filters=tuple(
                    (
                        columns[predicate.field_id],
                        predicate.operator.value,
                        predicate.values,
                    )
                    for predicate in plan.query.filters
                ),
            )
            estimate = build_execution_estimate(
                plan,
                evidence=evidence,
                execution_budget=execution_budget,
            )
            admitted[request_id] = plan
            execution_estimates[request_id] = estimate
    after = {str(path): _database_fingerprint(path) for path in database_paths}
    if before != after:
        raise ValueError("只读 Catalog/PIT 准入前后数据库 size/mtime 发生变化")
    drift_payload = {
        "catalog_hash": catalog.catalog_hash,
        "attestations": [
            attestation.to_dict()
            for _, attestation in sorted(attestation_by_binding.values(), key=lambda item: item[1].binding_id)
        ],
        "database_fingerprints": before,
    }
    pit_payload = {}
    for request_id, plan in sorted(admitted.items()):
        item = {
            "availability_policy_hash": plan.availability_policy_hash,
            "revision_policy_hash": plan.revision_policy_hash,
            "query": plan.query.to_dict(),
        }
        pit_payload[request_id] = item
    execution_estimates_payload = {
        request_id: estimate.to_dict()
        for request_id, estimate in sorted(execution_estimates.items())
    }
    return admitted, execution_estimates, {
        "catalog_hash": catalog.catalog_hash,
        "pit_contract_hash": typed_canonical_hash(pit_payload),
        "drift_proof_hash": typed_canonical_hash(drift_payload),
        "execution_estimates_hash": typed_canonical_hash(
            execution_estimates_payload
        ),
    }, before == after


def _parse_source_db_binding(value: object) -> tuple[str, Path]:
    if not isinstance(value, str) or "=" not in value:
        raise ValueError("--source-db 必须使用 PROFILE=PATH")
    profile, raw_path = value.split("=", 1)
    if not profile.strip() or not raw_path.strip():
        raise ValueError("--source-db 必须使用非空 PROFILE=PATH")
    return profile.strip(), Path(raw_path).resolve()


def _source_database_map(*, data_db, source_db) -> dict[str, Path]:
    result = {}
    if data_db:
        result["source"] = Path(data_db).resolve()
    for value in source_db:
        profile, path = _parse_source_db_binding(value)
        if profile in result:
            raise ValueError(f"重复 source_profile: {profile}")
        result[profile] = path
    if not result:
        raise ValueError("package admit 必须显式提供至少一个只读数据源")
    missing = [str(path) for path in result.values() if not path.is_file()]
    if missing:
        raise ValueError(f"只读数据源不存在: {missing}")
    return result


def _database_fingerprint(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


__all__ = ["admit_package"]
