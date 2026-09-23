"""CLI 已准入研究计划的原子目录合同。"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
import shutil
from types import SimpleNamespace
from typing import Mapping

from research_pipeline.data_plane.admission import AdmittedQueryPlan
from research_pipeline.data_plane.execution_estimate import (
    ExecutionEstimate,
    load_execution_estimates,
)
from research_pipeline.data_plane.execution_budget import (
    bind_data_plane_request_budgets,
)
from research_pipeline.data_plane.service import load_plan, save_plan
from research_pipeline.platform.claim_levels import weakest_claim_level
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
    build_mainline_operator_registry,
    compile_operator_graph_dag,
)
from research_pipeline.extensions import (
    AdmittedProjectOperatorRegistry,
    build_admitted_operator_registry,
    verify_project_operator_bundle,
    verify_project_verifier_bundle,
)


PLAN_MANIFEST = "research-plan.json"
QUERY_DIRECTORY = "queries"
DAG_PLAN = "dag.json"
OPERATOR_GRAPH_PLAN_VERSION = "research-cli-operator-graph-plan-v4"
OPERATOR_GRAPH_PLAN = "operator-graph-plan.json"
PROJECT_BUNDLE_DIRECTORY = "extensions"
VERIFIER_BUNDLE_DIRECTORY = "verifiers"


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
    verifier=None,
) -> Path:
    """原子发布声明式多查询候选计划；G 阶段不执行它。"""
    target = Path(destination).resolve()
    if target.exists():
        raise ValueError("plan 输出目录必须不存在")
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        raise ValueError("plan 临时目录已存在")
    query_root = temporary / QUERY_DIRECTORY
    validate_project_causal_admitted_sources(plan.recipe, admitted_plans)
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
        "formal_ancestor_node_ids": list(formal_ancestor_node_ids),
        "consumed_request_ids": list(consumed_request_ids),
        "input_claim_ceilings": input_claim_ceilings,
        "effective_claim_level": effective_claim_level,
    }
    if plan.research_semantics is not None:
        payload["research_semantics"] = plan.research_semantics.to_dict()
    if plan.project_bundle_hashes:
        payload["project_admission"] = {
            "project_id": plan.project_id,
            "bundle_hashes": list(plan.project_bundle_hashes),
            "implementation_hashes": dict(plan.project_implementation_hashes),
        }
    if plan.verifier_identity is not None:
        payload["verifier_admission"] = dict(plan.verifier_identity)
    _validate_admitted_query_facts(payload, admitted_plans)
    manifest = {**payload, "manifest_hash": typed_canonical_hash(payload)}
    graph_plan = {**plan.payload(), "plan_hash": plan.plan_hash}
    try:
        temporary.mkdir(parents=True)
        query_root.mkdir()
        for request_id, admitted in sorted(admitted_plans.items()):
            if not request_id.replace("_", "").isalnum():
                raise ValueError("request_id 不能用于安全文件名")
            save_plan(query_root / f"{request_id}.json", admitted)
        (temporary / OPERATOR_GRAPH_PLAN).write_text(canonical_json(graph_plan), encoding="utf-8")
        (temporary / DAG_PLAN).write_text(canonical_json({**dag.to_dict(), "dag_id": dag.dag_id}), encoding="utf-8")
        _copy_project_bundle_closure(
            target=temporary,
            plan=plan,
            registry=registry,
        )
        _copy_verifier_bundle_closure(
            target=temporary,
            plan=plan,
            verifier=verifier,
        )
        (temporary / PLAN_MANIFEST).write_text(canonical_json(manifest), encoding="utf-8")
        load_operator_graph_research_plan(target=temporary)
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return target


def load_operator_graph_research_plan(
    *, target: str | Path,
) -> tuple[dict[str, object], dict[str, AdmittedQueryPlan], DagSpec, object]:
    root = Path(target).resolve()
    manifest = json.loads((root / PLAN_MANIFEST).read_text(encoding="utf-8"))
    expected_base = {
        "contract_version", "plan_kind", "package_id", "package_hash", "package_plan_hash",
        "research_id", "registry_hash", "admission_hash", "dag_hash", "root_seed", "fixed_clock",
        "research_identity", "metric_contract", "claim_contract", "admitted_plan_hashes", "manifest_hash",
        "formal_ancestor_node_ids", "consumed_request_ids", "input_claim_ceilings",
        "effective_claim_level",
        "execution_estimates",
    }
    if not isinstance(manifest, dict):
        raise ValueError("operator graph plan manifest schema 无效")
    version = manifest.get("contract_version")
    expected_v2 = {
        *expected_base,
        "metric_proofs", "result_spec",
    }
    expected_v2_semantic = {*expected_v2, "research_semantics"}
    expected = expected_v2_semantic if "research_semantics" in manifest else expected_v2
    has_project_admission = "project_admission" in manifest
    if has_project_admission:
        expected.add("project_admission")
    if "verifier_admission" in manifest:
        expected.add("verifier_admission")
    if set(manifest) != expected:
        raise ValueError("operator graph plan manifest schema 无效")
    manifest_hash = manifest.pop("manifest_hash")
    if (
        version != OPERATOR_GRAPH_PLAN_VERSION
        or manifest["plan_kind"] != "operator_graph_research_run_candidate"
        or manifest_hash != typed_canonical_hash(manifest)
    ):
        raise ValueError("operator graph plan manifest hash 或版本无效")
    manifest["manifest_hash"] = manifest_hash
    graph_plan = json.loads((root / OPERATOR_GRAPH_PLAN).read_text(encoding="utf-8"))
    if not isinstance(graph_plan, dict) or graph_plan.pop("plan_hash", None) != manifest["package_plan_hash"]:
        raise ValueError("operator graph package plan 身份无效")
    if typed_canonical_hash(graph_plan) != manifest["package_plan_hash"]:
        raise ValueError("operator graph package plan 内容 hash 不一致")
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
    graph_verifier_admission = graph_plan.get("verifier_admission")
    if graph_verifier_admission != manifest.get("verifier_admission"):
        raise ValueError("operator graph Verifier 准入身份与 manifest 不一致")
    _verify_plan_verifier_bundle(
        root=root,
        verifier_admission=graph_verifier_admission,
    )
    expected_package_id = f"research_package_{str(manifest['package_hash'])[:16]}"
    if manifest["package_id"] != expected_package_id:
        raise ValueError("operator graph package_id 与 package_hash 不一致")
    raw_hashes = manifest["admitted_plan_hashes"]
    if not isinstance(raw_hashes, dict) or not raw_hashes:
        raise ValueError("operator graph admitted plan 集合无效")
    plans = {
        request_id: load_plan(root / QUERY_DIRECTORY / f"{request_id}.json")
        for request_id in raw_hashes
    }
    if any(plans[key].plan_hash != raw_hashes[key] for key in plans):
        raise ValueError("operator graph admitted plan hash 不一致")
    estimates = load_execution_estimates(
        manifest.get("execution_estimates"),
        request_ids=tuple(sorted(plans)),
    )
    for request_id, estimate in estimates.items():
        plan = plans[request_id]
        if (
            estimate.object_name != plan.object_name
            or estimate.query_scope_hash
            != typed_canonical_hash(plan.query.to_dict())
        ):
            raise ValueError(
                f"request={request_id} ExecutionEstimate 与 QueryPlan 不一致"
            )
    graph_requests = graph_plan.get("requests")
    if (
        not isinstance(graph_requests, list)
        or {item.get("request_id") for item in graph_requests if isinstance(item, dict)} != set(raw_hashes)
        or len(graph_requests) != len(raw_hashes)
    ):
        raise ValueError("operator graph QueryIR 集合不一致")
    request_queries = {item["request_id"]: item.get("query") for item in graph_requests}
    if any(plans[key].query.to_dict() != request_queries[key] for key in plans):
        raise ValueError("operator graph QueryIR 内容不一致")
    dag_payload = json.loads((root / DAG_PLAN).read_text(encoding="utf-8"))
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
    recipe = OperatorGraphRecipe.from_dict(recipe_payload)
    if recipe.recipe_hash != declared_recipe_hash:
        raise ValueError("operator graph recipe hash 不一致")
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
        manifest.get("metric_proofs") != [item.to_dict() for item in metric_proofs]
        or manifest.get("result_spec") != result_spec.to_dict()
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
        if manifest.get("research_semantics") != research_semantics.to_dict():
            raise ValueError("operator graph manifest 与 research_semantics 不一致")
    elif "research_semantics" in manifest:
        raise ValueError("operator graph research_semantics 字段不闭合")
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
    )
    validate_project_causal_recipe(
        recipe,
        request_queries={request_id: value.query for request_id, value in plans.items()},
        admission=registry,
        fixed_clock=datetime.fromisoformat(graph_plan["fixed_clock"]),
    )
    validate_project_causal_admitted_sources(recipe, plans)
    if isinstance(graph_project_admission, Mapping):
        actual_bundle_hashes = tuple(getattr(registry, "bundle_hashes", ()))
        actual_identities = getattr(registry, "implementation_identities", {})
        actual_implementation_hashes = {
            key: typed_canonical_hash(dict(value)) for key, value in actual_identities.items()
        }
        if (
            list(actual_bundle_hashes) != graph_project_admission.get("bundle_hashes")
            or actual_implementation_hashes != graph_project_admission.get("implementation_hashes")
        ):
            raise ValueError("operator graph extension bundle 与计划身份不一致")
    elif getattr(registry, "bundle_hashes", ()):
        raise ValueError("核心计划不得注入项目 extension bundle")
    expected_dag = compile_operator_graph_dag(
        SimpleNamespace(
            recipe=recipe,
            topological_order=tuple(raw_order),
            registry_hash=graph_plan["registry_hash"],
            admission_hash=graph_plan["admission_hash"],
            root_seed=graph_plan["root_seed"],
            fixed_clock=graph_plan["fixed_clock"],
            queries=tuple(
                plans[str(item["request_id"])].query for item in graph_requests
            ),
            metric_proofs=metric_proofs,
            result_spec=result_spec,
            research_semantics=research_semantics,
        ),
        registry,
    )
    if expected_dag.to_dict() != dag.to_dict():
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
    _validate_admitted_query_facts(manifest, plans)
    return manifest, plans, dag, registry


def _validate_admitted_query_facts(manifest, admitted_plans) -> None:
    """独有准入检查属于计划边界，不再另写重复证明。"""
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
        if estimate.object_name != plan.object_name or estimate.query_scope_hash != typed_canonical_hash(plan.query.to_dict()):
            raise ValueError(f"平台准入 ExecutionEstimate 与 QueryPlan 不一致: {request_id}")


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

    nodes = {node.node_id: node for node in recipe.nodes}
    formal_sources = {
        table.source_node_id
        for table in result_spec.tables
        if table.role != "diagnostic"
    }
    pending = list(formal_sources)
    ancestors: set[str] = set()
    while pending:
        node_id = pending.pop()
        if node_id in ancestors:
            continue
        node = nodes.get(node_id)
        if node is None:
            raise ValueError(f"ResultSpec 正式表引用未知节点: {node_id}")
        ancestors.add(node_id)
        pending.extend(binding.source_node_id for binding in node.inputs)

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
        copied = verify_project_operator_bundle(destination)
        if copied.project_id != plan.project_id:
            raise ValueError("Plan 内项目 bundle 与 project_id 不一致")


def _copy_verifier_bundle_closure(*, target: Path, plan: OperatorGraphPlan, verifier) -> None:
    if plan.verifier_identity is None:
        if verifier is not None:
            raise ValueError("未冻结 Verifier identity 的计划不得携带 Verifier bundle")
        return
    if verifier is None:
        raise ValueError("计划声明了 Verifier identity 但未提供 bundle")
    manifest = verify_project_verifier_bundle(verifier.path)
    if manifest.identity() != dict(plan.verifier_identity):
        raise ValueError("计划与 Verifier bundle 身份不一致")
    verifier_root = target / VERIFIER_BUNDLE_DIRECTORY
    verifier_root.mkdir()
    destination = verifier_root / manifest.bundle_hash
    shutil.copytree(verifier.path, destination)
    verify_project_verifier_bundle(destination)


def _verify_plan_verifier_bundle(*, root: Path, verifier_admission: object) -> None:
    verifier_root = root / VERIFIER_BUNDLE_DIRECTORY
    if verifier_admission is None:
        if verifier_root.exists():
            raise ValueError("核心计划不得包含未声明的 Verifier bundle")
        return
    if not isinstance(verifier_admission, Mapping):
        raise ValueError("operator graph Verifier 准入 schema 无效")
    bundle_hash = verifier_admission.get("bundle_hash")
    if (
        not isinstance(bundle_hash, str)
        or not verifier_root.is_dir()
        or sorted(item.name for item in verifier_root.iterdir()) != [bundle_hash]
    ):
        raise ValueError("Plan 内 Verifier bundle 闭包无效")
    manifest = verify_project_verifier_bundle(verifier_root / bundle_hash)
    if manifest.identity() != dict(verifier_admission):
        raise ValueError("Plan 内 Verifier bundle 与准入身份不一致")


def _load_plan_operator_registry(
    *,
    root: Path,
    project_admission: object,
) -> object:
    """只从 Plan 内闭包重建本次运行唯一的组合 registry。"""

    extension_root = root / PROJECT_BUNDLE_DIRECTORY
    builtin = build_mainline_operator_registry()
    if project_admission is None:
        if extension_root.exists():
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
        or not extension_root.is_dir()
    ):
        raise ValueError("operator graph 项目准入身份无效")
    actual_entries = sorted(item.name for item in extension_root.iterdir())
    if actual_entries != bundle_hashes:
        raise ValueError("Plan 内项目 bundle 闭包缺失或包含额外条目")
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
