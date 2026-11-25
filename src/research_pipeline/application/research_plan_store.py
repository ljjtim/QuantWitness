"""已准入研究计划的原子目录合同。"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from copy import copy
from datetime import datetime
from pathlib import Path
import shutil
from types import MappingProxyType, SimpleNamespace
from typing import Mapping

from research_pipeline.data_plane.admission import AdmittedQueryPlan
from research_pipeline.data_plane.minute_scan import MinuteScanPlan, MinuteScanBudget
from research_pipeline.data_plane.execution_estimate import (
    ExecutionEstimate,
    load_execution_estimates,
)
from research_pipeline.data_plane.execution_budget import (
    bind_data_plane_request_budgets,
)
from research_pipeline.data_plane.admitted_plan_codec import admitted_plan_from_dict
from research_pipeline.platform.claim_levels import weakest_claim_level
from research_pipeline.platform.lean_runtime import validate_lean_runtime
from research_pipeline.platform.audit_mode import strict_audit_enabled
from research_pipeline.platform.immutable_files import remove_tree
from research_pipeline.packages.plan_contracts import OperatorGraphPlan
from research_pipeline.packages.project_causal_admission import (
    project_causal_request_ids,
    validate_project_causal_admitted_sources,
    validate_project_causal_recipe,
)
from research_pipeline.platform.operator_contracts import OperatorGraphRecipe
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.metric_contracts import MetricReachabilityProof
from research_pipeline.research.semantics import ResearchSemantics
from research_pipeline.results import ResultSpec
from research_pipeline.runtime.graph import DagSpec
from research_pipeline.runtime.operator_registry import (
    NODE_IDENTITY_PROJECTION_CURRENT,
    build_mainline_operator_registry,
    compile_operator_graph_dag,
)
from research_pipeline.extensions import (
    AdmittedProjectOperatorRegistry,
    build_admitted_operator_registry,
    verify_project_operator_bundle,
)


PLAN_MANIFEST = "research-plan.json"
QUERY_DIRECTORY = "queries"
DAG_PLAN = "dag.json"
OPERATOR_GRAPH_PLAN_VERSION = "research-cli-operator-graph-plan-v10"
OPERATOR_GRAPH_PLAN = "operator-graph-plan.json"
PROJECT_BUNDLE_DIRECTORY = "extensions"


@dataclass(frozen=True)
class PreparedResearchPlan:
    """同次准入直接交给执行的对象；持久目录仍用于独立运行与恢复。"""

    directory: Path
    manifest: Mapping[str, object]
    admitted: Mapping[str, AdmittedQueryPlan]
    dag: DagSpec
    recipe: OperatorGraphRecipe
    registry: object
    minute_scope_inputs: Mapping[str, object]
    minute_scan_plans: Mapping[str, MinuteScanPlan]
    result_spec: ResultSpec
    metric_proofs: tuple[MetricReachabilityProof, ...]


def minute_scan_parameters(recipe):
    """按请求归集扫描节点，保留每个节点的预算与能力声明。"""
    selected = {}
    for node in recipe.nodes:
        if node.operator_id != "data.minute.scan":
            continue
        ids = node.parameters["request_ids"]
        if len(ids) != 1:
            raise ValueError("每个分钟 scan 须绑定一个 request")
        selected.setdefault(ids[0], []).append(node.parameters)
    return selected


def minute_scan_budget(parameters):
    """共享扫描计划采用各节点都能接受的最严格预算。"""
    return MinuteScanBudget(
        max_selected_file_bytes=min(int(item["max_source_bytes"]) for item in parameters),
        max_batch_bytes=min(int(item["max_batch_bytes"]) for item in parameters),
    )


def validate_minute_scan_plans(recipe, admitted, scan_plans):
    from research_pipeline.packages.minute_scope_admission import minute_scan_scope_bindings

    parameters = minute_scan_parameters(recipe)
    scopes = minute_scan_scope_bindings(recipe, {
        key for key, query in admitted.items() if query.minute_asset_class == "cn_future"
    })
    if set(parameters) != set(scan_plans):
        raise ValueError("冻结分钟扫描计划与 scan 请求集合不一致")
    for request_id, values in parameters.items():
        plan = scan_plans[request_id]
        query = admitted[request_id]
        expected_budget = minute_scan_budget(values)
        if (plan.minute_scope != scopes.get(request_id)
                or plan.minute_timezone != query.minute_timezone
                or plan.admitted_plan_hash != query.plan_hash or plan.budget != expected_budget
                or plan.start_at != query.query.time_range.start_at
                or plan.end_at != query.query.time_range.end_at
                or plan.as_of != query.query.as_of_instant
                or plan.instruments != query.query.universe.instruments
                or plan.universe_snapshot_id != query.query.universe.snapshot_id
                or plan.query_max_rows != query.query.budget.max_rows
                or plan.query_max_bytes != query.query.budget.max_bytes
                or plan.batch_size != query.query.budget.batch_size):
            raise ValueError(f"冻结分钟扫描计划与准入请求或预算不一致: {request_id}")
        for node_parameters in values:
            if plan.query_max_rows > int(node_parameters["max_returned_rows"]):
                raise ValueError("分钟 QueryIR 返回行预算超过算子声明")
            if plan.minute_capability_manifest_hash != node_parameters["scope_binding_hash"]:
                raise ValueError("分钟 scan 能力 manifest 绑定与 QueryIR 不一致")


def _requires_causal_catalog(recipe) -> bool:
    """同日来源恢复时使用原可见性合同，不跟随当前默认目录变化。"""
    return any("strategy_declaration" in node.parameters and (
        node.parameters.get("frequency") == "1d" or "open_request_id" in node.parameters)
        for node in recipe.nodes) or any(
        (source.get("daily_time") or {}).get("rule") in {"session_open", "business_effective_date", "next_session_open_following_publication_date"}
        for node in recipe.nodes
        for source in node.parameters.get("causal_plan", {}).get("sources", ())
    )


def _used_contracts(recipe, registry):
    """冻结当前图实际调用的算子、策略合同及其执行入口。"""
    operators, strategies = {}, {}
    for node in recipe.nodes:
        key = f"{node.operator_id}@{node.operator_version}"
        if key not in operators:
            binding = registry.binding(node.operator_id, node.operator_version)
            spec = registry.require_operator(node.operator_id, node.operator_version)
            operators[key] = [binding.implementation_id, spec.spec_hash]
        for selected in node.strategies:
            strategy = registry.require_strategy(selected.role, selected.strategy_id, selected.strategy_version)
            strategies[f"{selected.role.value}/{selected.strategy_id}@{selected.strategy_version}"] = strategy.spec_hash
    return {"operators": operators, "strategies": strategies}


def publish_operator_graph_research_plan(
    destination: str | Path,
    *,
    package_id: str,
    plan: OperatorGraphPlan,
    research_identity: Mapping[str, object],
    metric_contract: Mapping[str, object],
    claim_contract: Mapping[str, object],
    admitted_plans: Mapping[str, AdmittedQueryPlan],
    execution_estimates: Mapping[str, ExecutionEstimate],
    dag: DagSpec,
    registry=None,
    input_snapshot_manifest: Mapping[str, object] | None = None,
    catalog=None,
    minute_scope_inputs=None,
    minute_scan_plans=None,
    minute_data_root=None,
    metric_definitions=(),
) -> PreparedResearchPlan:
    """原子发布声明式多查询候选计划；G 阶段不执行它。"""
    scope_inputs = minute_scope_inputs or {}
    scan_plans = minute_scan_plans or {}
    validate_minute_scan_plans(plan.recipe, admitted_plans, scan_plans)
    if scan_plans and minute_data_root is None:
        raise ValueError("分钟扫描计划缺少实际 minute-data-root")
    scope_references = tuple(node.parameters["minute_scope"] for node in plan.recipe.nodes
                             if "minute_scope" in node.parameters)
    if set(scope_inputs) != {reference["name"] for reference in scope_references} or any(
        scope_inputs[reference["name"]].reference != reference for reference in scope_references
    ):
        raise ValueError("计划发布的分钟输入与节点声明不一致")
    target = Path(destination).resolve()
    if target.exists():
        raise ValueError("plan 输出目录必须不存在")
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        raise ValueError("plan 临时目录已存在")
    query_root = temporary / QUERY_DIRECTORY
    if package_id != f"research_package_{plan.package_hash[:16]}":
        raise ValueError("operator graph package_id 与 package_hash 不一致")
    identity = _validate_research_identity(
        dict(research_identity), package_hash=plan.package_hash, package_plan_hash=plan.plan_hash,
    )
    for field, contract in (("metric_contract", metric_contract), ("claim_contract", claim_contract)):
        contract_payload = dict(contract)
        contract_hash = contract_payload.pop("contract_hash", None)
        if contract_hash != typed_canonical_hash(contract_payload):
            raise ValueError(f"operator graph {field} 内容 hash 不一致")
        if identity[f"{field}_hash"] != contract_hash:
            raise ValueError("operator graph research identity 合同 hash 不一致")
    if set(plan.request_ids) != set(admitted_plans):
        raise ValueError("operator graph QueryIR 集合不一致")
    if any(query.to_dict() != admitted_plans[key].query.to_dict()
           for key, query in zip(plan.request_ids, plan.queries, strict=True)):
        raise ValueError("operator graph QueryIR 内容不一致")
    validate_project_causal_admitted_sources(plan.recipe, admitted_plans, catalog=catalog)
    from research_pipeline.packages.project_strategy_admission import validate_daily_strategy_plans
    validate_daily_strategy_plans(plan.recipe, admitted_plans, catalog=catalog)
    admitted_hashes = {key: admitted_plans[key].plan_hash for key in sorted(admitted_plans)}
    if set(execution_estimates) != set(admitted_plans):
        raise ValueError("ExecutionEstimate 与 admitted request 集合不闭合")
    execution_estimate_payload = {
        request_id: execution_estimates[request_id].to_dict()
        for request_id in sorted(execution_estimates)
    }
    execution_estimates_hash = typed_canonical_hash(execution_estimate_payload)
    admission_nodes = tuple(
        node
        for node in plan.recipe.nodes
        if node.operator_id == "data.catalog.admission"
    )
    if len(admission_nodes) != 1:
        raise ValueError("正式 plan 必须恰好包含一个平台 Catalog admission 节点")
    admission_parameters = admission_nodes[0].parameters
    if (
        admission_parameters.get("request_ids")
        != tuple(sorted(admitted_plans))
        or admission_parameters.get("execution_estimates_hash")
        != execution_estimates_hash
    ):
        raise ValueError("正式 plan 冻结的 ExecutionEstimate 身份与发布内容不一致")
    _validate_execution_estimate_budget(
        plan=plan,
        dag=dag,
        estimates=execution_estimates,
        request_queries={
            request_id: admitted_plan.query
            for request_id, admitted_plan in admitted_plans.items()
        },
    )
    formal_ancestor_node_ids, consumed_request_ids = _formal_input_lineage(
        recipe=plan.recipe,
        result_spec=plan.result_spec,
        admitted_request_ids=frozenset(admitted_plans),
    )
    input_claim_ceilings = {
        request_id: admitted_plans[request_id].input_claim_ceiling
        for request_id in consumed_request_ids
    }
    requested_claim = str(claim_contract.get("max_claim_level"))
    semantic_ceiling = requested_claim
    if plan.research_semantics is not None:
        semantic_ceiling = plan.research_semantics.claim_ceiling
        operator_ids = {node.operator_id for node in plan.recipe.nodes}
        if (
            "research.factor.statistics" in operator_ids
            and not any("simulation" in operator_id for operator_id in operator_ids)
        ):
            semantic_ceiling = "research_observation"
    effective_claim_level = weakest_claim_level(
        requested_claim,
        semantic_ceiling,
        *input_claim_ceilings.values(),
    )
    payload = {
        "contract_version": OPERATOR_GRAPH_PLAN_VERSION,
        "plan_kind": "operator_graph_research_run_candidate",
        "package_id": package_id,
        "package_hash": plan.package_hash,
        "package_plan_hash": plan.plan_hash,
        "research_id": plan.research_id,
        "registry_hash": plan.registry_hash,
        "admission_hash": plan.admission_hash,
        "dag_hash": dag.dag_id,
        "root_seed": plan.root_seed,
        "fixed_clock": plan.fixed_clock,
        "research_identity": dict(research_identity),
        "metric_contract": dict(metric_contract),
        "claim_contract": dict(claim_contract),
        "metric_proofs": [item.to_dict() for item in plan.metric_proofs],
        "result_spec": plan.result_spec.to_dict(),
        "admitted_plan_hashes": admitted_hashes,
        "execution_estimates": execution_estimate_payload,
        "minute_scan_plans": {key: value.to_dict() for key, value in sorted(scan_plans.items())},
        "minute_data_root": None if not scan_plans else str(Path(minute_data_root).resolve()),
        "metric_definitions": [item.to_dict() for item in metric_definitions],
        "formal_ancestor_node_ids": list(formal_ancestor_node_ids),
        "consumed_request_ids": list(consumed_request_ids),
        "input_claim_ceilings": input_claim_ceilings,
        "effective_claim_level": effective_claim_level,
        "node_identity_projection": NODE_IDENTITY_PROJECTION_CURRENT,
    }
    if input_snapshot_manifest is not None:
        payload["input_snapshot_manifest"] = dict(input_snapshot_manifest)
    if _requires_causal_catalog(plan.recipe):
        payload["causal_catalog_lock"] = dict(catalog.payload)
    if plan.research_semantics is not None:
        payload["research_semantics"] = plan.research_semantics.to_dict()
    if plan.research_cost_assumption is not None:
        payload["research_cost_assumption"] = dict(plan.research_cost_assumption)
    if plan.project_bundle_hashes:
        payload["project_admission"] = {
            "project_id": plan.project_id,
            "bundle_hashes": list(plan.project_bundle_hashes),
            "implementation_hashes": dict(plan.project_implementation_hashes),
        }
    _validate_admitted_query_facts(payload, admitted_plans)
    graph_plan = {**plan.payload(), "plan_hash": plan.plan_hash}
    frozen_scopes = {}
    try:
        temporary.mkdir(parents=True)
        from research_pipeline.domain.minute_scope_input import freeze_minute_scope_input

        if scope_inputs:
            scope_manifests = {}
            for name, source in sorted(scope_inputs.items()):
                frozen_scopes[name], scope_manifests[name] = freeze_minute_scope_input(
                    source, temporary, final_root=target)
            payload["minute_scope_manifests"] = scope_manifests
        frozen_files = {}
        def write_frozen(name, value):
            content = canonical_json(value).encode("utf-8")
            (temporary / name).write_bytes(content)
            frozen_files[name] = hashlib.sha256(content).hexdigest()

        query_root.mkdir()
        universe_refs = {r["request_id"]: r["query"]["universe_ref"] for r in graph_plan["requests"]}
        for request_id, admitted in sorted(admitted_plans.items()):
            if not request_id.replace("_", "").isalnum():
                raise ValueError("request_id 不能用于安全文件名")
            frozen = admitted.to_dict()
            del frozen["query"]["universe"]
            frozen["query"]["universe_ref"] = universe_refs[request_id]
            write_frozen(f"{QUERY_DIRECTORY}/{request_id}.json", frozen)
        write_frozen(OPERATOR_GRAPH_PLAN, graph_plan)
        write_frozen(DAG_PLAN, {**dag.to_dict(), "dag_id": dag.dag_id})
        _copy_project_bundle_closure(
            target=temporary,
            plan=plan,
            registry=registry,
        )
        payload["frozen_files"] = frozen_files
        payload["used_contracts"] = _used_contracts(plan.recipe, registry or build_mainline_operator_registry())
        manifest = {**payload, "manifest_hash": typed_canonical_hash(payload)}
        (temporary / PLAN_MANIFEST).write_text(canonical_json(manifest), encoding="utf-8")
        os.replace(temporary, target)
    except Exception:
        remove_tree(temporary, ignore_errors=True)
        raise
    execution_registry = registry
    if isinstance(registry, AdmittedProjectOperatorRegistry):
        execution_registry = copy(registry)
        execution_registry.bundle_paths = MappingProxyType({
            key: target / PROJECT_BUNDLE_DIRECTORY / registry.project_token_by_implementation(key).manifest.bundle_hash
            for key in registry.bundle_paths
        })
    return PreparedResearchPlan(
        target, manifest, dict(admitted_plans), dag, plan.recipe, execution_registry,
        frozen_scopes,
        dict(scan_plans), plan.result_spec, plan.metric_proofs,
    )


def load_operator_graph_research_plan(
    *, target: str | Path, loaded_minute_scopes: dict | None = None,
    loaded_minute_scan_plans: dict | None = None,
    published_source_node_id: str | None = None, strict: bool | None = None,
) -> tuple[
    dict[str, object],
    dict[str, AdmittedQueryPlan],
    DagSpec,
    OperatorGraphRecipe,
    object,
]:
    prepared = read_prepared_research_plan(target=target, published_source_node_id=published_source_node_id, strict=strict)
    if loaded_minute_scopes is not None:
        loaded_minute_scopes.update(prepared.minute_scope_inputs)
    if loaded_minute_scan_plans is not None:
        loaded_minute_scan_plans.update(prepared.minute_scan_plans)
    return prepared.manifest, prepared.admitted, prepared.dag, prepared.recipe, prepared.registry


def read_prepared_research_plan(*, target, published_source_node_id=None, strict=None) -> PreparedResearchPlan:
    root = Path(target).resolve()
    for_published_input = published_source_node_id is not None
    if strict is None:
        strict = strict_audit_enabled()
    manifest = json.loads((root / PLAN_MANIFEST).read_text(encoding="utf-8"))
    expected_base = {
        "contract_version", "plan_kind", "package_id", "package_hash", "package_plan_hash",
        "research_id", "registry_hash", "admission_hash", "dag_hash", "root_seed", "fixed_clock",
        "research_identity", "metric_contract", "claim_contract", "admitted_plan_hashes", "manifest_hash",
        "formal_ancestor_node_ids", "consumed_request_ids", "input_claim_ceilings",
        "effective_claim_level",
        "execution_estimates", "metric_definitions",
        "minute_scan_plans", "minute_data_root",
    }
    if not isinstance(manifest, dict):
        raise ValueError("operator graph plan manifest schema 无效")
    version = manifest.get("contract_version")
    if version != OPERATOR_GRAPH_PLAN_VERSION:
        raise ValueError("operator graph plan manifest hash 或版本无效")
    expected_base.add("node_identity_projection")
    for name in ("input_snapshot_manifest", "causal_catalog_lock", "minute_scope_manifests", "research_cost_assumption"):
        if name in manifest:
            expected_base.add(name)
            if not isinstance(manifest[name], dict):
                raise ValueError(f"计划 {name} 必须是映射")
    has_files = "frozen_files" in manifest
    if has_files:
        expected_base.update(("frozen_files", "used_contracts"))
    elif not for_published_input:
        raise ValueError("冻结计划缺少文件清单，请重新准入研究包")
    expected_v2 = {
        *expected_base,
        "metric_proofs", "result_spec",
    }
    expected_v2_semantic = {*expected_v2, "research_semantics"}
    expected = expected_v2_semantic if "research_semantics" in manifest else expected_v2
    has_project_admission = "project_admission" in manifest
    if has_project_admission:
        expected.add("project_admission")
    if set(manifest) != expected:
        raise ValueError("operator graph plan manifest schema 无效")
    if manifest["node_identity_projection"] != NODE_IDENTITY_PROJECTION_CURRENT:
        raise ValueError("operator graph node identity projection 版本无效")
    manifest_hash = manifest.pop("manifest_hash")
    if (
        manifest["plan_kind"] != "operator_graph_research_run_candidate"
        or manifest_hash != typed_canonical_hash(manifest)
    ):
        raise ValueError("operator graph plan manifest hash 或版本无效")
    manifest["manifest_hash"] = manifest_hash
    def read_frozen(name):
        content = (root / name).read_bytes()
        if has_files and hashlib.sha256(content).hexdigest() != manifest["frozen_files"].get(name):
            raise ValueError(f"冻结计划文件内容不一致: {name}")
        return json.loads(content)

    graph_plan = read_frozen(OPERATOR_GRAPH_PLAN)
    if not isinstance(graph_plan, dict) or graph_plan.pop("plan_hash", None) != manifest["package_plan_hash"]:
        raise ValueError("operator graph package plan 身份无效")
    if (
        graph_plan.get("package_hash") != manifest["package_hash"]
        or graph_plan.get("research_id") != manifest["research_id"]
        or graph_plan.get("registry_hash") != manifest["registry_hash"]
        or graph_plan.get("admission_hash") != manifest["admission_hash"]
        or graph_plan.get("root_seed") != manifest["root_seed"]
        or graph_plan.get("fixed_clock") != manifest["fixed_clock"]
    ):
        raise ValueError("operator graph package plan 与 manifest 不一致")
    graph_project_admission = graph_plan.get("project_admission")
    if graph_project_admission != manifest.get("project_admission"):
        raise ValueError("operator graph 项目准入身份与 manifest 不一致")
    expected_package_id = f"research_package_{str(manifest['package_hash'])[:16]}"
    if manifest["package_id"] != expected_package_id:
        raise ValueError("operator graph package_id 与 package_hash 不一致")
    raw_hashes = manifest["admitted_plan_hashes"]
    if not isinstance(raw_hashes, dict) or not raw_hashes:
        raise ValueError("operator graph admitted plan 集合无效")
    if has_files:
        expected_files = {DAG_PLAN, OPERATOR_GRAPH_PLAN, *(f"{QUERY_DIRECTORY}/{key}.json" for key in raw_hashes)}
        if set(manifest["frozen_files"]) != expected_files:
            raise ValueError("冻结计划文件清单与准入请求不一致")
    universes = graph_plan.get("universes")
    if not isinstance(universes, dict):
        raise ValueError("冻结计划缺少股票池声明")
    authenticated = has_files and not strict
    from research_pipeline.data_plane.admitted_plan_codec import _decode_plan
    plans = {}
    for key in raw_hashes:
        frozen = read_frozen(f"{QUERY_DIRECTORY}/{key}.json")
        query = frozen["query"]
        ref = query.pop("universe_ref", None)
        if not isinstance(ref, str) or ref not in universes or "universe" in query:
            raise ValueError("冻结查询的股票池引用无效")
        query["universe"] = universes[ref]
        if frozen.get("plan_hash") != raw_hashes[key]:
            raise ValueError("operator graph admitted plan hash 不一致")
        plans[key] = _decode_plan(frozen) if authenticated else admitted_plan_from_dict(frozen)
    estimates = _validate_admitted_query_facts(manifest, plans, authenticated=authenticated)
    graph_requests = graph_plan.get("requests")
    if (
        not isinstance(graph_requests, list)
        or {item.get("request_id") for item in graph_requests if isinstance(item, dict)} != set(raw_hashes)
        or len(graph_requests) != len(raw_hashes)
    ):
        raise ValueError("operator graph QueryIR 集合不一致")
    for request in graph_requests:
        query = request["query"]
        if not isinstance(query, dict) or query.get("universe_ref") not in universes:
            raise ValueError("冻结计划股票池引用无效")
        if strict or not has_files:
            declared = dict(query)
            declared["universe"] = universes[declared.pop("universe_ref")]
            if plans[request["request_id"]].query.to_dict() != declared:
                raise ValueError("operator graph QueryIR 内容不一致")
    dag_payload = read_frozen(DAG_PLAN)
    if not isinstance(dag_payload, dict) or dag_payload.pop("dag_id", None) != manifest["dag_hash"]:
        raise ValueError("operator graph DAG 身份无效")
    dag = DagSpec.from_dict(dag_payload)
    if dag.dag_id != manifest["dag_hash"]:
        raise ValueError("operator graph DAG 内容 hash 不一致")
    raw_recipe = graph_plan.get("recipe")
    raw_order = graph_plan.get("topological_order")
    if not isinstance(raw_recipe, dict) or not isinstance(raw_order, list):
        raise ValueError("operator graph recipe 或拓扑序无效")
    recipe_payload = dict(raw_recipe)
    declared_recipe_hash = recipe_payload.pop("recipe_hash", None)
    recipe = OperatorGraphRecipe._decode(recipe_payload, declared_recipe_hash if authenticated else None)
    selected_nodes = (set(_recipe_ancestors(recipe, (published_source_node_id,)))
                      if for_published_input else {node.node_id for node in recipe.nodes})
    verified_recipe = (OperatorGraphRecipe.build(
        graph_id=recipe.graph_id, nodes=tuple(node for node in recipe.nodes if node.node_id in selected_nodes),
    ) if for_published_input else recipe)
    from .model_import_preparation import verify_model_import_sources
    verify_model_import_sources(verified_recipe)
    scan_plans = {key: MinuteScanPlan.from_dict(value)
                  for key, value in manifest["minute_scan_plans"].items()}
    validate_minute_scan_plans(recipe, plans, scan_plans)
    if bool(scan_plans) != bool(manifest["minute_data_root"]):
        raise ValueError("分钟扫描计划与实际来源根不闭合")
    if not authenticated and recipe.recipe_hash != declared_recipe_hash:
        raise ValueError("operator graph recipe hash 不一致")
    if strict or not has_files:
        plan_identity = {**graph_plan, "recipe": recipe.recipe_hash,
                         "identity_projection": "validated-recipe-ref-v1"}
        if typed_canonical_hash(plan_identity) != manifest["package_plan_hash"]:
            raise ValueError("operator graph package plan 内容 hash 不一致")
    for node in verified_recipe.nodes:
        if node.parameters.get("execution_backend") == "lean":
            validate_lean_runtime(node.parameters.get("lean_runtime"), strict=strict)
    admission_nodes = tuple(
        node
        for node in recipe.nodes
        if node.operator_id == "data.catalog.admission"
    )
    execution_estimates_hash = typed_canonical_hash(
        {
            request_id: estimate.to_dict()
            for request_id, estimate in sorted(estimates.items())
        }
    )
    if len(admission_nodes) != 1 or (
        admission_nodes[0].parameters.get("request_ids")
        != tuple(sorted(plans))
        or admission_nodes[0].parameters.get("execution_estimates_hash")
        != execution_estimates_hash
    ):
        raise ValueError("operator graph 平台准入与 ExecutionEstimate 不一致")
    _validate_execution_estimate_budget(
        plan=SimpleNamespace(recipe=recipe),
        dag=dag,
        estimates=estimates,
        request_queries={
            request_id: admitted_plan.query
            for request_id, admitted_plan in plans.items()
        },
    )
    raw_metric_proofs = graph_plan.get("metric_proofs")
    if not isinstance(raw_metric_proofs, list) or not raw_metric_proofs:
        raise ValueError("operator graph metric proofs 缺失")
    try:
        metric_proofs = tuple(
            MetricReachabilityProof.from_dict(item)
            for item in raw_metric_proofs
            if isinstance(item, Mapping)
        )
    except Exception as exc:
        raise ValueError(f"operator graph metric proofs 无效: {exc}") from exc
    if len(metric_proofs) != len(raw_metric_proofs):
        raise ValueError("operator graph metric proofs schema 无效")
    raw_result_spec = graph_plan.get("result_spec")
    if not isinstance(raw_result_spec, Mapping):
        raise ValueError("operator graph ResultSpec 缺失")
    try:
        result_spec = ResultSpec.from_dict(raw_result_spec)
    except Exception as exc:
        raise ValueError(f"operator graph ResultSpec 无效: {exc}") from exc
    if (
        manifest.get("metric_proofs") != raw_metric_proofs
        or manifest.get("result_spec") != raw_result_spec
    ):
        raise ValueError("operator graph manifest 与 ResultSpec/metric proofs 不一致")
    raw_research_semantics = graph_plan.get("research_semantics")
    research_semantics = None
    if raw_research_semantics is not None:
        if not isinstance(raw_research_semantics, Mapping):
            raise ValueError("operator graph research_semantics schema 无效")
        try:
            research_semantics = ResearchSemantics.from_dict(raw_research_semantics)
        except Exception as exc:
            raise ValueError(f"operator graph research_semantics 无效: {exc}") from exc
        if manifest.get("research_semantics") != raw_research_semantics:
            raise ValueError("operator graph manifest 与 research_semantics 不一致")
    elif "research_semantics" in manifest:
        raise ValueError("operator graph research_semantics 字段不闭合")
    raw_cost = graph_plan.get("research_cost_assumption")
    if raw_cost is not None:
        if not isinstance(raw_cost, Mapping):
            raise ValueError("operator graph research_cost_assumption schema 无效")
        if manifest.get("research_cost_assumption") != raw_cost:
            raise ValueError("operator graph manifest 与 research_cost_assumption 不一致")
    elif "research_cost_assumption" in manifest:
        raise ValueError("operator graph research_cost_assumption 字段不闭合")
    raw_claim_contract = manifest.get("claim_contract")
    if not isinstance(raw_claim_contract, Mapping):
        raise ValueError("operator graph claim_contract 必须是对象")
    requested_claim = str(raw_claim_contract.get("max_claim_level"))
    semantic_ceiling = (
        requested_claim
        if research_semantics is None
        else research_semantics.claim_ceiling
    )
    operator_ids = {node.operator_id for node in recipe.nodes}
    if (
        "research.factor.statistics" in operator_ids
        and not any("simulation" in operator_id for operator_id in operator_ids)
    ):
        semantic_ceiling = "research_observation"
    formal_node_ids, consumed_request_ids = _formal_input_lineage(
        recipe=recipe,
        result_spec=result_spec,
        admitted_request_ids=frozenset(plans),
    )
    input_claim_ceilings = {
        request_id: plans[request_id].input_claim_ceiling
        for request_id in consumed_request_ids
    }
    if (
        manifest.get("formal_ancestor_node_ids") != list(formal_node_ids)
        or manifest.get("consumed_request_ids") != list(consumed_request_ids)
        or manifest.get("input_claim_ceilings") != input_claim_ceilings
        or manifest.get("effective_claim_level")
        != weakest_claim_level(
            requested_claim,
            semantic_ceiling,
            *input_claim_ceilings.values(),
        )
    ):
        raise ValueError("operator graph formal input claim lineage 不一致")
    registry = _load_plan_operator_registry(
        root=root,
        project_admission=graph_project_admission,
        implementation_ids=({node.implementation_id for node in dag.nodes if node.node_id in selected_nodes}
                            if for_published_input else None),
    )
    if has_files:
        actual_contracts = _used_contracts(verified_recipe, registry)
        frozen_contracts = manifest["used_contracts"]
        if for_published_input:
            frozen_contracts = {
                kind: {key: frozen_contracts[kind].get(key) for key in contracts}
                for kind, contracts in actual_contracts.items()
            }
        if actual_contracts != frozen_contracts:
            raise ValueError("冻结计划实际使用的算子或策略实现改变，请重新准入")
    needs_catalog = _requires_causal_catalog(verified_recipe)
    locked_catalog = manifest.get("causal_catalog_lock")
    if needs_catalog:
        if not isinstance(locked_catalog, dict) or any(
            plan.catalog_hash != locked_catalog.get("catalog_hash") for plan in plans.values()
        ):
            raise ValueError("日期 causal 计划缺少相应的冻结 Catalog Lock")
    elif not for_published_input and locked_catalog is not None:
        raise ValueError("计划没有消费已封存的日期 causal Catalog Lock")
    if strict or not has_files:
        validate_project_causal_recipe(
            verified_recipe,
            request_queries={request_id: value.query for request_id, value in plans.items()},
            admission=registry,
            fixed_clock=datetime.fromisoformat(graph_plan["fixed_clock"]),
        )
        causal_catalog = None
        if needs_catalog:
            from research_pipeline.catalog import CompiledCatalog

            causal_catalog = CompiledCatalog.from_payload(locked_catalog)
        validate_project_causal_admitted_sources(verified_recipe, plans, catalog=causal_catalog)
        from research_pipeline.packages.project_strategy_admission import validate_daily_strategy_plans
        validate_daily_strategy_plans(verified_recipe, plans, catalog=causal_catalog)
        # 已发布工件只使用原计划证明来源。重新编译其节点时，全局注册表可新增无关算子。
        graph_admission = registry.admit(verified_recipe) if has_files or for_published_input else None
        expected_dag = compile_operator_graph_dag(
            SimpleNamespace(
                recipe=verified_recipe,
                topological_order=tuple(node_id for node_id in raw_order if node_id in selected_nodes),
                registry_hash=graph_plan["registry_hash"] if graph_admission is None else graph_admission.registry_hash,
                admission_hash=graph_plan["admission_hash"] if graph_admission is None else graph_admission.admission_hash,
                root_seed=graph_plan["root_seed"],
                fixed_clock=graph_plan["fixed_clock"],
                queries=tuple(
                    plans[str(item["request_id"])].query for item in graph_requests
                ),
                request_ids=tuple(str(item["request_id"]) for item in graph_requests),
                metric_proofs=metric_proofs,
                result_spec=result_spec,
                research_semantics=research_semantics,
            ),
            registry,
            identity_projection=str(manifest["node_identity_projection"]),
        )
        verified_dag = (DagSpec(
            dag.name, tuple(node for node in dag.nodes if node.node_id in selected_nodes),
            tuple(edge for edge in dag.edges if edge.target_node in selected_nodes),
        ) if for_published_input else dag)
        if expected_dag.to_dict() != verified_dag.to_dict():
            raise ValueError("operator graph DAG 不是可信 recipe 的唯一编译结果")
    identity = _validate_research_identity(
        manifest["research_identity"],
        package_hash=manifest["package_hash"],
        package_plan_hash=manifest["package_plan_hash"],
    )
    for field in ("metric_contract", "claim_contract"):
        contract = manifest[field]
        if not isinstance(contract, dict):
            raise ValueError(f"operator graph {field} 必须是对象")
        contract_payload = dict(contract)
        contract_hash = contract_payload.pop("contract_hash", None)
        if contract_hash != typed_canonical_hash(contract_payload):
            raise ValueError(f"operator graph {field} 内容 hash 不一致")
    if (
        identity.get("metric_contract_hash") != manifest["metric_contract"].get("contract_hash")
        or identity.get("claim_contract_hash") != manifest["claim_contract"].get("contract_hash")
    ):
        raise ValueError("operator graph research identity 合同 hash 不一致")

    # 计划与实现通过检查后核对冻结文件。完整历史由消费节点按需读取。
    from research_pipeline.packages.minute_scope_admission import load_graph_minute_scopes

    scope_inputs = load_graph_minute_scopes(
        root, (node.parameters for node in verified_recipe.nodes),
        frozen_manifest_digests=manifest.get("minute_scope_manifests", {}), strict=strict,
        allow_unreferenced=for_published_input)
    return PreparedResearchPlan(root, manifest, plans, dag, recipe, registry,
                                scope_inputs, scan_plans, result_spec, metric_proofs)


def _validate_admitted_query_facts(
    manifest, admitted_plans, *, authenticated=False,
) -> dict[str, ExecutionEstimate]:
    """校验准入事实，返回本次调用已解析的估算供预算检查复用。"""
    from research_pipeline.data_plane import resolve_as_of_cutoff

    clock = datetime.fromisoformat(manifest["fixed_clock"])
    if clock.tzinfo is None or type(manifest["root_seed"]) is not int or manifest["root_seed"] < 0:
        raise ValueError("平台准入 clock/root_seed 无效")
    estimates = load_execution_estimates(manifest.get("execution_estimates"), request_ids=tuple(sorted(admitted_plans)))
    for request_id, plan in admitted_plans.items():
        for field in ("catalog_hash", "attestation_hash", "availability_policy_hash", "revision_policy_hash"):
            value = getattr(plan, field, None)
            if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise ValueError(f"平台准入 {field} 无效: {request_id}")
        if plan.query.as_of is None:
            raise ValueError(f"平台准入缺少 PIT as_of: {request_id}")
        if resolve_as_of_cutoff(plan.query.as_of, reference_clock=clock) > clock:
            raise ValueError(f"平台准入 as_of 晚于固定时钟: {request_id}")
        estimate = estimates[request_id]
        if estimate.object_name != plan.object_name or (not authenticated and
                estimate.query_scope_hash != typed_canonical_hash(plan.query.to_dict())):
            raise ValueError(f"平台准入 ExecutionEstimate 与 QueryPlan 不一致: {request_id}")
    return estimates


def _validate_execution_estimate_budget(
    *,
    plan,
    dag: DagSpec,
    estimates: Mapping[str, ExecutionEstimate],
    request_queries: Mapping[str, object],
) -> None:
    execution_budgets = bind_data_plane_request_budgets(
        request_queries=request_queries,
        recipe_nodes=plan.recipe.nodes,
        dag=dag,
    )
    mismatches = tuple(
        request_id
        for request_id, estimate in sorted(estimates.items())
        if request_id not in execution_budgets
        or estimate.execution_budget != execution_budgets[request_id].to_dict()
    )
    if mismatches:
        raise ValueError(
            "ExecutionEstimate 与 request 实际数据节点 ResourceBudget 不一致: "
            + ",".join(mismatches)
        )


def _formal_input_lineage(
    *,
    recipe: OperatorGraphRecipe,
    result_spec: ResultSpec,
    admitted_request_ids: frozenset[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """从非诊断 Result 表反向求正式祖先及其真实数据请求。"""

    formal_sources = {
        table.source_node_id
        for table in result_spec.tables
        if table.role != "diagnostic"
    }
    ancestors = _recipe_ancestors(recipe, formal_sources)
    nodes = {node.node_id: node for node in recipe.nodes}

    consumed: set[str] = set()
    for node_id in sorted(ancestors):
        node = nodes[node_id]
        if node.operator_id == "data.catalog.admission":
            continue
        declared = _request_ids_from_parameters(node.parameters)
        unknown = declared - admitted_request_ids
        if unknown:
            raise ValueError(
                f"正式祖先声明未知 request_id: {sorted(unknown)}"
            )
        consumed.update(declared)
    if not consumed:
        consumed = set(admitted_request_ids)
    if not consumed:
        raise ValueError("正式 Result 没有可传播的输入 request_id")
    return tuple(sorted(ancestors)), tuple(sorted(consumed))


def _recipe_ancestors(recipe, source_node_ids):
    nodes = {node.node_id: node for node in recipe.nodes}
    pending = list(source_node_ids)
    ancestors = set()
    while pending:
        node_id = pending.pop()
        if node_id in ancestors:
            continue
        node = nodes.get(node_id)
        if node is None:
            raise ValueError(f"结果表或输入引用未知节点: {node_id}")
        ancestors.add(node_id)
        pending.extend(binding.source_node_id for binding in node.inputs)
    return ancestors


def _request_ids_from_parameters(parameters: Mapping[str, object]) -> set[str]:
    result = project_causal_request_ids(parameters)
    for name, raw_value in parameters.items():
        if not (
            name == "request_id"
            or name.endswith("_request_id")
            or name == "request_ids"
            or name.endswith("_request_ids")
        ):
            continue
        values = raw_value if isinstance(raw_value, (list, tuple)) else (raw_value,)
        for value in values:
            if value in {None, ""}:
                continue
            if not isinstance(value, str):
                raise ValueError(f"{name} 必须是 request_id 字符串或列表")
            result.add(value)
    return result


def _copy_project_bundle_closure(
    *,
    target: Path,
    plan: OperatorGraphPlan,
    registry: object,
) -> None:
    """把准入时复验过的项目 bundle 复制进不可变 Plan。"""

    if not plan.project_bundle_hashes:
        if isinstance(registry, AdmittedProjectOperatorRegistry):
            raise ValueError("核心计划不得携带项目 extension registry")
        return
    if not isinstance(registry, AdmittedProjectOperatorRegistry):
        raise ValueError("项目计划发布必须提供已准入项目 registry")
    actual_implementation_hashes = {
        key: typed_canonical_hash(dict(value))
        for key, value in registry.implementation_identities.items()
    }
    if (
        registry.project_id != plan.project_id
        or registry.bundle_hashes != plan.project_bundle_hashes
        or actual_implementation_hashes != dict(plan.project_implementation_hashes)
    ):
        raise ValueError("项目计划与准入 registry 身份不一致")

    sources: dict[str, Path] = {}
    for path in registry.bundle_paths.values():
        manifest = verify_project_operator_bundle(path)
        if manifest.bundle_hash in sources:
            raise ValueError("项目准入 registry 重复引用同一 bundle")
        sources[manifest.bundle_hash] = path
    if set(sources) != set(plan.project_bundle_hashes):
        raise ValueError("项目准入 registry 未提供完整 bundle 闭包")

    extension_root = target / PROJECT_BUNDLE_DIRECTORY
    extension_root.mkdir()
    for bundle_hash in plan.project_bundle_hashes:
        destination = extension_root / bundle_hash
        shutil.copytree(sources[bundle_hash], destination)
        # Windows 目录监听会占用句柄，计划发布前须允许整个暂存目录原子重命名。
        copied = verify_project_operator_bundle(destination, cache=False)
        if copied.project_id != plan.project_id:
            raise ValueError("Plan 内项目 bundle 与 project_id 不一致")








def _load_plan_operator_registry(
    *,
    root: Path,
    project_admission: object,
    implementation_ids: set[str] | None = None,
) -> object:
    """只从 Plan 内闭包重建本次运行唯一的组合 registry。"""

    extension_root = root / PROJECT_BUNDLE_DIRECTORY
    builtin = build_mainline_operator_registry()
    if project_admission is None:
        if extension_root.exists() and implementation_ids is None:
            raise ValueError("核心计划不得包含项目 extension 闭包")
        return builtin
    if not isinstance(project_admission, Mapping) or set(project_admission) != {
        "project_id",
        "bundle_hashes",
        "implementation_hashes",
    }:
        raise ValueError("operator graph 项目准入 schema 无效")
    bundle_hashes = project_admission["bundle_hashes"]
    implementation_hashes = project_admission["implementation_hashes"]
    if (
        not isinstance(bundle_hashes, list)
        or not bundle_hashes
        or any(not isinstance(item, str) for item in bundle_hashes)
        or bundle_hashes != sorted(set(bundle_hashes))
        or not isinstance(implementation_hashes, Mapping)
        or (implementation_ids is None and not extension_root.is_dir())
    ):
        raise ValueError("operator graph 项目准入身份无效")
    if implementation_ids is None:
        actual_entries = sorted(item.name for item in extension_root.iterdir())
        if actual_entries != bundle_hashes:
            raise ValueError("Plan 内项目 bundle 闭包缺失或包含额外条目")
    else:
        selected = {key for key in implementation_ids if key.startswith("project.operator.")}
        bundle_hashes = [value for value in bundle_hashes if f"project.operator.{value[:32]}" in selected]
        if (len(bundle_hashes) != len(selected) or not selected <= implementation_hashes.keys()
                or any(not (extension_root / value).is_dir() for value in bundle_hashes)):
            raise ValueError("Plan 内缺少发布输入所用项目 bundle")
        implementation_hashes = {key: implementation_hashes[key] for key in selected}
        if not selected:
            return builtin
    registry = build_admitted_operator_registry(
        tuple(extension_root / bundle_hash for bundle_hash in bundle_hashes),
        builtin_registry=builtin,
        expected_project_id=str(project_admission["project_id"]),
    )
    actual_hashes = {
        key: typed_canonical_hash(dict(value))
        for key, value in getattr(registry, "implementation_identities", {}).items()
    }
    if (
        list(getattr(registry, "bundle_hashes", ())) != bundle_hashes
        or actual_hashes != dict(implementation_hashes)
    ):
        raise ValueError("Plan 内项目 bundle 与准入身份不一致")
    return registry


def _validate_research_identity(
    value: object,
    *,
    package_hash: object,
    package_plan_hash: object,
) -> dict[str, object]:
    expected = {
        "package_id", "package_hash", "plan_hash", "metric_contract_hash",
        "claim_contract_hash", "source_provenance_hash", "contract_version",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("research identity schema 无效")
    hashes = (
        value["package_hash"], value["plan_hash"], value["metric_contract_hash"],
        value["claim_contract_hash"], value["source_provenance_hash"],
    )
    if any(
        not isinstance(item, str)
        or len(item) != 64
        or any(char not in "0123456789abcdef" for char in item)
        for item in hashes
    ):
        raise ValueError("research identity hash 无效")
    if (
        value["package_hash"] != package_hash
        or value["plan_hash"] != package_plan_hash
        or value["package_id"] != f"research_package_{str(package_hash)[:16]}"
        or value["contract_version"] != "research-package-v1"
    ):
        raise ValueError("research identity 与 package plan 不一致")
    return value



__all__ = [
    "PLAN_MANIFEST", "load_operator_graph_research_plan",
    "publish_operator_graph_research_plan",
]
