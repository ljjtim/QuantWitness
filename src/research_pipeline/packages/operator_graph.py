"""从纯查询编译结果构建声明式通用算子图计划。"""

from __future__ import annotations

from datetime import datetime
from typing import Mapping, Protocol, runtime_checkable

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.asset_taxonomy import MINUTE_TARGET_ASSET_CLASSES
from research_pipeline.platform.operator_contracts import (
    AdmittedOperatorGraph,
    OperatorGraphRecipe,
)
from research_pipeline.platform.metric_contracts import compose_metric_registry
from research_pipeline.research.semantics import ResearchSemantics
from research_pipeline.results.compiler import compile_result_spec
from research_pipeline.data_plane import (
    QueryIRInvalidError,
    parse_aware_datetime,
    resolve_as_of_cutoff,
)

from .constants import OPERATOR_GRAPH_BUILDER_ID
from .models import ResearchPackage, ResearchPackageError
from .plan_contracts import (
    OPERATOR_GRAPH_PLAN_VERSION,
    OperatorGraphPlan,
    QueryCompileResult,
    RecipeCompileResult,
)


OPERATOR_GRAPH_PACKAGE_VERSION = "research-operator-graph-package-v2"


@runtime_checkable
class OperatorGraphAdmission(Protocol):
    registry_hash: str

    def admit(self, recipe: OperatorGraphRecipe) -> AdmittedOperatorGraph:
        """验证 recipe 引用的可信算子、策略和端口闭包。"""

    def require_operator(self, operator_id: str, operator_version: str) -> object:
        """返回带 output_ports 的受信算子合同。"""


def compile_operator_graph_package(
    package: ResearchPackage,
    *,
    query_result: QueryCompileResult,
    admission: object | None = None,
    platform_admission: object | None = None,
    verifier_admission: object | None = None,
) -> OperatorGraphPlan:
    if not isinstance(admission, OperatorGraphAdmission):
        raise ResearchPackageError("operator_graph_plan_v1 必须提供可信算子注册表准入")
    payload = package.spec_payload
    required_fields = {
        "contract_version", "research_id", "as_of", "requests", "root_seed", "fixed_clock",
        "graph", "result",
    }
    optional_fields = {"reproduction", "research_semantics"}
    if not required_fields.issubset(payload) or set(payload) - required_fields - optional_fields:
        expected = required_fields | optional_fields
        raise ResearchPackageError(
            f"spec schema 不匹配；缺失={sorted(required_fields - set(payload))}，"
            f"未知={sorted(set(payload) - expected)}"
        )
    if payload["contract_version"] != OPERATOR_GRAPH_PACKAGE_VERSION:
        raise ResearchPackageError("operator graph package contract_version 不受支持")
    research_id = payload["research_id"]
    as_of = payload["as_of"]
    graph = payload["graph"]
    reproduction = payload.get("reproduction", {"study_proof_required": False})
    raw_research_semantics = payload.get("research_semantics")
    if not isinstance(research_id, str) or not research_id.strip():
        raise ResearchPackageError("spec.research_id 必须是非空字符串")
    if not isinstance(graph, Mapping):
        raise ResearchPackageError("spec.graph 必须是映射")
    if not isinstance(reproduction, Mapping) or set(reproduction) != {"study_proof_required"}:
        raise ResearchPackageError("spec.reproduction schema 无效")
    if type(reproduction["study_proof_required"]) is not bool:
        raise ResearchPackageError("spec.reproduction.study_proof_required 必须是 bool")
    research_semantics = None
    if raw_research_semantics is not None:
        if not isinstance(raw_research_semantics, Mapping):
            raise ResearchPackageError("spec.research_semantics 必须是映射")
        try:
            research_semantics = ResearchSemantics.from_dict(raw_research_semantics)
        except Exception as exc:
            raise ResearchPackageError(f"研究语义合同无效: {exc}") from exc
    if not isinstance(as_of, str):
        raise ResearchPackageError("spec.as_of 必须是规范 ISO 日期或带时区时间")
    versions_by_dataset: dict[str, set[int]] = {}
    for query in query_result.queries:
        versions_by_dataset.setdefault(query.dataset_id, set()).add(query.dataset_version)
    mixed_versions = sorted(dataset_id for dataset_id, versions in versions_by_dataset.items() if len(versions) != 1)
    if mixed_versions:
        raise ResearchPackageError(
            f"同一算子图内相同 dataset_id 必须使用同一 dataset_version: {mixed_versions}"
        )
    root_seed = payload["root_seed"]
    fixed_clock = payload["fixed_clock"]
    if type(root_seed) is not int or not isinstance(fixed_clock, str):
        raise ResearchPackageError("spec.root_seed/fixed_clock 类型无效")
    try:
        run_clock = parse_aware_datetime(fixed_clock, "spec.fixed_clock")
    except QueryIRInvalidError as exc:
        raise ResearchPackageError(str(exc)) from exc
    _validate_query_clock_contract(
        query_result,
        fixed_clock=run_clock,
    )
    if research_semantics is not None:
        if research_semantics.decision_at > run_clock:
            raise ResearchPackageError("研究 decision_at 不能晚于固定运行时钟")
        undeclared_metrics = set(research_semantics.metric_refs) - set(
            package.metric_contract.metrics
        )
        if undeclared_metrics:
            raise ResearchPackageError(
                f"Estimand 指标未进入 Package MetricContract: {sorted(undeclared_metrics)}"
            )
    try:
        recipe = OperatorGraphRecipe.from_dict(
            _inject_platform_catalog_admission(
                graph,
                request_ids=query_result.request_ids,
                platform_admission=platform_admission,
            )
        )
        admitted = admission.admit(recipe)
        _validate_operator_graph_special_contracts(
            recipe,
            admission=admission,
            request_ids=frozenset(query_result.request_ids),
        )
        from .project_causal_admission import validate_project_causal_recipe

        validate_project_causal_recipe(
            recipe,
            request_queries=dict(zip(query_result.request_ids, query_result.queries, strict=True)),
            admission=admission,
            fixed_clock=run_clock,
        )
        _validate_research_semantic_graph(
            recipe,
            admission=admission,
            semantics=research_semantics,
        )
    except ResearchPackageError:
        raise
    except Exception as exc:
        raise ResearchPackageError(f"operator graph 准入失败: {exc}") from exc
    try:
        result_spec = compile_result_spec(
            payload["result"],
            recipe=recipe,
            resolver=admission,
        )
        _validate_adjustment_result_closure(recipe, result_spec)
    except Exception as exc:
        raise ResearchPackageError(f"ResultSpec 编译失败: {exc}") from exc
    producers = []
    for node in recipe.nodes:
        operator = admission.require_operator(node.operator_id, node.operator_version)
        for output in operator.output_ports:
            producers.append((node.node_id, output.port, output.artifact_type))
    project_metrics = (
        () if verifier_admission is None
        else verifier_admission.manifest.metric_definitions
    )
    if project_metrics and {item.metric_ref for item in project_metrics} - set(package.metric_contract.metrics):
        raise ResearchPackageError("项目 Verifier Metric 定义未被当前 Package 声明")
    metric_proofs = compose_metric_registry(project_metrics).prove(
        package.metric_contract.metrics,
        producers,
        result_spec.tables,
    )
    compiled_recipe = RecipeCompileResult(
        recipe,
        admitted.topological_order,
        admitted.registry_hash,
        admitted.admission_hash,
        metric_proofs,
    )
    project_id = getattr(admission, "project_id", None)
    project_bundle_hashes = tuple(getattr(admission, "bundle_hashes", ()))
    implementation_identities = getattr(admission, "implementation_identities", {})
    project_implementation_hashes = {
        implementation_id: typed_canonical_hash(dict(identity))
        for implementation_id, identity in implementation_identities.items()
    }
    verifier_identity = None
    if verifier_admission is not None:
        raw_identity = getattr(verifier_admission, "identity", None)
        if not isinstance(raw_identity, Mapping):
            raise ResearchPackageError("Verifier bundle 未形成可冻结身份")
        verifier_identity = dict(raw_identity)
        expected_project_id = project_id or research_id
        if verifier_identity.get("project_id") != expected_project_id:
            raise ResearchPackageError("Verifier bundle project_id 与 Package 不一致")
    values = {
        "research_id": research_id,
        "package_hash": package.package_hash,
        "requests": [{"request_id": request_id, "query": query.to_dict()} for request_id, query in zip(query_result.request_ids, query_result.queries, strict=True)],
        "recipe": compiled_recipe.recipe.to_dict(),
        "topological_order": list(compiled_recipe.topological_order),
        "registry_hash": compiled_recipe.registry_hash,
        "admission_hash": compiled_recipe.admission_hash,
        "metric_proofs": [item.to_dict() for item in compiled_recipe.metric_proofs],
        "root_seed": root_seed,
        "fixed_clock": fixed_clock,
        "contract_version": OPERATOR_GRAPH_PLAN_VERSION,
    }
    if project_bundle_hashes:
        values["project_admission"] = {
            "project_id": project_id,
            "bundle_hashes": list(project_bundle_hashes),
            "implementation_hashes": dict(sorted(project_implementation_hashes.items())),
        }
    if verifier_identity is not None:
        values["verifier_admission"] = verifier_identity
    values["result_spec"] = result_spec.to_dict()
    if research_semantics is not None:
        values["research_semantics"] = research_semantics.to_dict()
    return OperatorGraphPlan(
        research_id=research_id,
        package_hash=package.package_hash,
        request_ids=query_result.request_ids,
        queries=query_result.queries,
        recipe=compiled_recipe.recipe,
        topological_order=compiled_recipe.topological_order,
        registry_hash=compiled_recipe.registry_hash,
        admission_hash=compiled_recipe.admission_hash,
        metric_proofs=compiled_recipe.metric_proofs,
        result_spec=result_spec,
        root_seed=root_seed,
        fixed_clock=fixed_clock,
        plan_hash=typed_canonical_hash(values),
        research_semantics=research_semantics,
        project_id=project_id,
        project_bundle_hashes=project_bundle_hashes,
        project_implementation_hashes=project_implementation_hashes,
        verifier_identity=verifier_identity,
    )


def _validate_query_clock_contract(
    query_result: QueryCompileResult,
    *,
    fixed_clock: datetime,
) -> None:
    """拒绝在固定时钟之后才结束的 QueryIR 可见性截止。"""

    future = []
    for request_id, query in zip(
        query_result.request_ids,
        query_result.queries,
        strict=True,
    ):
        if query.as_of is None:
            raise ResearchPackageError(f"QueryIR 缺少 as_of: {request_id}")
        try:
            cutoff = resolve_as_of_cutoff(
                query.as_of,
                reference_clock=fixed_clock,
                field=f"spec.requests[{request_id}].as_of",
            )
        except QueryIRInvalidError as exc:
            raise ResearchPackageError(str(exc)) from exc
        if cutoff > fixed_clock:
            future.append((request_id, cutoff.isoformat()))
    if future:
        raise ResearchPackageError(
            "QueryIR as_of 晚于 spec.fixed_clock；日期型 as_of 表示该本地日的"
            f"排他结束。请将 fixed_clock 调整到不早于 {future} 后重新 lint/admit"
        )


def _validate_adjustment_result_closure(recipe, result_spec) -> None:
    """复权必须封存实际快照；pre 还必须封存逐决策锚点。"""

    expected = {
        "research.features.intraday": (
            "features", "research.minute-features.pre-anchor.v1"
        ),
        "research.labels.intraday": (
            "labels", "research.minute-labels.pre-anchor.v1"
        ),
    }
    selected = {
        (item.source_node_id, item.source_port, item.artifact_type, item.schema_id)
        for item in result_spec.tables
    }
    missing = []
    snapshot_sources = set()
    for node in recipe.nodes:
        if node.operator_id != "research.bars.minute_adjust":
            continue
        bindings = tuple(item for item in node.inputs if item.input_port == "snapshot")
        if len(bindings) != 1:
            raise ResearchPackageError("分钟复权节点必须绑定唯一 snapshot 输入")
        binding = bindings[0]
        snapshot_sources.add((binding.source_node_id, binding.source_output_port))
    if len(snapshot_sources) > 1:
        raise ResearchPackageError("单个 ResultSpec 不能封存多套分钟复权快照")
    for source_node_id, source_port in snapshot_sources:
        identity = (
            source_node_id,
            source_port,
            "data.adjustment-factor-snapshot.v1",
            "data.adjustment-factor-snapshot.payload.v1",
        )
        if identity not in selected:
            missing.append("/".join(identity))
    for node in recipe.nodes:
        if (
            node.operator_id not in expected
            or node.parameters.get("adjustment_mode") != "pre"
        ):
            continue
        source_port, schema_id = expected[node.operator_id]
        identity = (
            node.node_id,
            source_port,
            "research.minute-features.v1"
            if node.operator_id == "research.features.intraday"
            else "research.minute-labels.v1",
            schema_id,
        )
        if identity not in selected:
            missing.append("/".join(identity))
    if missing:
        raise ResearchPackageError(
            f"分钟复权 ResultSpec 未封存实际快照或逐决策锚点行: {sorted(missing)}"
        )


def _inject_platform_catalog_admission(
    graph: Mapping[str, object],
    *,
    request_ids: tuple[str, ...],
    platform_admission: object | None,
) -> dict[str, object]:
    """把数据准入作为平台输入注入，ResearchPackage 只声明研究图。"""
    if set(graph) != {"contract_version", "graph_id", "nodes"}:
        raise ResearchPackageError("spec.graph schema 无效")
    raw_nodes = graph["nodes"]
    if not isinstance(raw_nodes, (list, tuple)) or not raw_nodes:
        raise ResearchPackageError("spec.graph.nodes 必须是非空序列")
    nodes = [_thaw_mapping(item, "spec.graph.nodes") for item in raw_nodes]
    declared = [item for item in nodes if item.get("operator_id") == "data.catalog.admission"]
    if declared:
        raise ResearchPackageError("ResearchPackage 不得声明平台 Catalog admission 节点")
    columnar_nodes = [
        item for item in nodes
        if item.get("operator_id") == "data.columnar.materialize"
    ]
    if len(columnar_nodes) > 1:
        raise ResearchPackageError("当前算子图最多包含一个 data.columnar.materialize 节点")
    data_nodes = [
        item
        for item in nodes
        if item.get("operator_id") in {
            "data.columnar.materialize",
            "data.minute.scan",
        }
    ]
    if not data_nodes:
        return {
            "contract_version": graph["contract_version"],
            "graph_id": graph["graph_id"],
            "nodes": nodes,
        }
    values = _platform_admission_values(
        request_ids=request_ids,
        platform_admission=platform_admission,
    )
    admission_id = "platform.catalog.admission"
    if any(item.get("node_id") == admission_id for item in nodes):
        raise ResearchPackageError("研究图 node_id 与平台保留节点冲突")
    for data_node in data_nodes:
        operator_id = str(data_node["operator_id"])
        inputs = data_node.get("inputs")
        if not isinstance(inputs, list):
            raise ResearchPackageError(f"{operator_id} inputs 必须是序列")
        if inputs:
            raise ResearchPackageError(f"{operator_id} 的 admission 输入由平台注入")
        data_node["inputs"] = [{
            "input_port": "admission",
            "source_node_id": admission_id,
            "source_output_port": "admission",
        }]
    nodes.append({
        "node_id": admission_id,
        "operator_id": "data.catalog.admission",
        "operator_version": "1.0.0",
        "inputs": [],
        "parameters": values,
        "strategies": [],
    })
    return {
        "contract_version": graph["contract_version"],
        "graph_id": graph["graph_id"],
        "nodes": nodes,
    }


def _platform_admission_values(
    *,
    request_ids: tuple[str, ...],
    platform_admission: object | None,
) -> dict[str, object]:
    if platform_admission is None:
        placeholder = "0" * 64
        return {
            "request_ids": list(sorted(request_ids)),
            "catalog_hash": placeholder,
            "pit_contract_hash": placeholder,
            "drift_proof_hash": placeholder,
            "execution_estimates_hash": placeholder,
        }
    if not isinstance(platform_admission, Mapping):
        raise ResearchPackageError("平台 Catalog admission 必须是映射")
    expected = {
        "catalog_hash",
        "pit_contract_hash",
        "drift_proof_hash",
        "execution_estimates_hash",
    }
    if set(platform_admission) != expected:
        raise ResearchPackageError("平台 Catalog admission schema 无效")
    values = {key: platform_admission[key] for key in sorted(expected)}
    for key, value in values.items():
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ResearchPackageError(f"平台 Catalog admission {key} 必须是 sha256")
    return {"request_ids": list(sorted(request_ids)), **values}


def _thaw_mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ResearchPackageError(f"{field} 必须是映射")

    def thaw(item: object) -> object:
        if isinstance(item, Mapping):
            return {str(key): thaw(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [thaw(child) for child in item]
        return item

    return {str(key): thaw(item) for key, item in value.items()}


def _validate_operator_graph_special_contracts(
    recipe: OperatorGraphRecipe,
    *,
    admission: OperatorGraphAdmission | None = None,
    request_ids: frozenset[str] | None = None,
) -> None:
    """执行通用类型系统无法表达的分钟与交易能力约束。"""
    from research_pipeline.platform.minute_operator_contracts import MINUTE_FEATURE_IDS

    node_by_id = {item.node_id: item for item in recipe.nodes}
    for node in recipe.nodes:
        parameters = node.parameters
        if node.operator_id in {
            "research.bars.minute_resample", "research.features.intraday",
            "research.labels.intraday", "research.signals.intraday",
            "research.targets.intraday",
        }:
            availability = parameters.get("availability_policy_ref")
            if not isinstance(availability, str) or not availability.strip():
                raise ResearchPackageError("分钟算子 availability_policy_ref 必须是非空稳定引用")
        if node.operator_id == "research.features.intraday":
            feature_ids = parameters.get("feature_ids")
            if (
                not isinstance(feature_ids, tuple)
                or tuple(sorted(set(feature_ids))) != feature_ids
                or not set(feature_ids).issubset(MINUTE_FEATURE_IDS)
            ):
                raise ResearchPackageError("分钟 feature_ids 必须来自小型白名单并规范排序")
            lookback = parameters.get("lookback_bars")
            warmup = parameters.get("warmup_bars")
            if type(lookback) is not int or type(warmup) is not int or lookback < 2 or warmup < lookback:
                raise ResearchPackageError("分钟 feature lookback/warmup 必须为正且 warmup >= lookback >= 2")
        elif node.operator_id == "research.labels.intraday":
            horizon = parameters.get("horizon_bars")
            if type(horizon) is not int or horizon < 1:
                raise ResearchPackageError("分钟 label horizon_bars 必须为正整数")
        elif node.operator_id == "research.signals.intraday":
            if parameters.get("feature_id") not in MINUTE_FEATURE_IDS:
                raise ResearchPackageError("分钟 signal feature_id 不在白名单")
        elif node.operator_id == "research.targets.intraday":
            asset_class = parameters.get("asset_class")
            quantity = parameters.get("target_quantity_per_signal")
            leverage = parameters.get("leverage_limit")
            if asset_class not in MINUTE_TARGET_ASSET_CLASSES:
                raise ResearchPackageError("分钟 target asset_class 不受支持")
            if type(quantity) is not int or quantity <= 0:
                raise ResearchPackageError("分钟 target_quantity_per_signal 必须是正整数")
            if asset_class in {"cn_stock", "cn_etf"} and quantity % 100:
                raise ResearchPackageError("股票和 ETF 分钟 target 必须按 100 股/份声明")
            if type(leverage) not in {int, float} or float(leverage) < 1.0:
                raise ResearchPackageError("分钟 target leverage_limit 无效")
        elif node.operator_id == "finance.simulation.intraday":
            participation = parameters.get("participation_ppm")
            initial_cash = parameters.get("initial_cash_units")
            rule_hash = parameters.get("rule_bundle_hash")
            if type(participation) is not int or not 1 <= participation <= 1_000_000:
                raise ResearchPackageError("分钟 simulation participation_ppm 必须在 (0,100%] 内")
            if type(initial_cash) is not int or initial_cash <= 0:
                raise ResearchPackageError("分钟 simulation initial_cash_units 必须是正整数")
            if (
                not isinstance(rule_hash, str)
                or len(rule_hash) != 64
                or any(char not in "0123456789abcdef" for char in rule_hash)
            ):
                raise ResearchPackageError("分钟 simulation rule_bundle_hash 必须是 sha256")
        elif node.operator_id in {
            "research.model.fit",
            "research.model.locked-holdout",
        }:
            if parameters.get("thread_count") != 1:
                raise ResearchPackageError(
                    f"{node.operator_id} 只允许 thread_count=1，以保证确定性"
                )
        if node.operator_id in {"research.features.intraday", "research.signals.intraday"}:
            ancestors = [binding.source_node_id for binding in node.inputs]
            visited: set[str] = set()
            while ancestors:
                source_id = ancestors.pop()
                if source_id in visited:
                    continue
                visited.add(source_id)
                source = node_by_id.get(source_id)
                if source is None:
                    continue
                if source.operator_id == "research.labels.intraday":
                    raise ResearchPackageError("label 不得成为 feature 或 signal 的直接或间接依赖")
                ancestors.extend(binding.source_node_id for binding in source.inputs)


def _validate_research_semantic_graph(
    recipe: OperatorGraphRecipe,
    *,
    admission: OperatorGraphAdmission,
    semantics: ResearchSemantics | None,
) -> None:
    """禁止 label 成为 feature 祖先，并要求分析图显式绑定研究语义。"""
    nodes = {item.node_id: item for item in recipe.nodes}
    output_types = {
        node.node_id: {
            item.port: item.artifact_type
            for item in admission.require_operator(
                node.operator_id,
                node.operator_version,
            ).output_ports
        }
        for node in recipe.nodes
    }
    feature_nodes = {
        node_id
        for node_id, outputs in output_types.items()
        if "research.feature-set.v1" in outputs.values()
    }
    label_nodes = {
        node_id
        for node_id, outputs in output_types.items()
        if {"research.label.v1", "research.event-window.v1"}.intersection(
            outputs.values()
        )
    }
    analysis_graph = bool(feature_nodes or label_nodes)
    if analysis_graph and semantics is None:
        raise ResearchPackageError("Feature/Label 分析图必须声明 research_semantics")
    if semantics is not None and (not feature_nodes or not label_nodes):
        raise ResearchPackageError(
            "research_semantics 必须由 Feature 与 Label/事件结果节点共同实现"
        )
    for feature_node in feature_nodes:
        ancestors = [item.source_node_id for item in nodes[feature_node].inputs]
        visited: set[str] = set()
        while ancestors:
            source_id = ancestors.pop()
            if source_id in visited:
                continue
            visited.add(source_id)
            if source_id in label_nodes:
                raise ResearchPackageError("Label 不得成为 Feature 的直接或间接祖先")
            source = nodes.get(source_id)
            if source is not None:
                ancestors.extend(item.source_node_id for item in source.inputs)
    if semantics is None:
        return
    features_by_id = {item.feature_id: item for item in semantics.features}
    labels_by_id = {item.label_id: item for item in semantics.labels}
    estimands_by_hash = {item.estimand_hash: item for item in semantics.estimands}
    hypotheses_by_hash = {
        item.hypothesis_hash: item for item in semantics.hypotheses
    }
    for node_id, node in nodes.items():
        parameters = node.parameters
        if node_id in feature_nodes:
            feature_ref = parameters.get("feature_set_id", parameters.get("feature_id"))
            if feature_ref is not None:
                feature = features_by_id.get(feature_ref)
                if feature is None:
                    raise ResearchPackageError(
                        f"Feature 节点引用未声明的 ResearchSemantics 身份: {feature_ref}"
                    )
                if (
                    "preprocessing_order" in parameters
                    and tuple(parameters["preprocessing_order"])
                    != feature.preprocessing_order
                ):
                    raise ResearchPackageError(
                        "Feature preprocessing_order 与 ResearchSemantics 不一致"
                    )
        if node_id in label_nodes:
            label_ref = parameters.get("label_set_id", parameters.get("label_id"))
            if label_ref is not None:
                label = labels_by_id.get(label_ref)
                if label is None:
                    raise ResearchPackageError(
                        f"Label 节点引用未声明的 ResearchSemantics 身份: {label_ref}"
                    )
                is_formal_label = "research.label.v1" in output_types[node_id].values()
                if (
                    is_formal_label
                    and "revision_policy" in parameters
                    and parameters["revision_policy"] != label.revision_policy
                ):
                    raise ResearchPackageError(
                        "Label revision_policy 与 ResearchSemantics 不一致"
                    )
                if (
                    is_formal_label
                    and "visibility_policy_hash" in parameters
                    and parameters["visibility_policy_hash"]
                    != label.visibility_policy_hash
                ):
                    raise ResearchPackageError(
                        "Label visibility_policy_hash 与 ResearchSemantics 不一致"
                    )
        if (
            "estimand_hash" in parameters
            and parameters["estimand_hash"] not in estimands_by_hash
        ):
            raise ResearchPackageError("节点引用未声明的 Estimand 身份")
        if (
            "hypothesis_hash" in parameters
            and parameters["hypothesis_hash"] not in hypotheses_by_hash
        ):
            raise ResearchPackageError("节点引用未声明的 Hypothesis 身份")
        if "estimand_hash" in parameters and "hypothesis_hash" in parameters:
            estimand = estimands_by_hash.get(parameters["estimand_hash"])
            hypothesis = hypotheses_by_hash.get(parameters["hypothesis_hash"])
            if (
                estimand is not None
                and hypothesis is not None
                and hypothesis.estimand_id != estimand.estimand_id
            ):
                raise ResearchPackageError(
                    "节点的 Estimand/Hypothesis 引用关系与 ResearchSemantics 不一致"
                )
        if (
            "semantics_hash" in parameters
            and parameters["semantics_hash"] != semantics.semantics_hash
        ):
            raise ResearchPackageError("节点 research_semantics 身份发生漂移")
        if "metric_refs" in parameters and not set(parameters["metric_refs"]).issubset(
            semantics.metric_refs
        ):
            raise ResearchPackageError("节点 metric_refs 未在 ResearchSemantics 中声明")


__all__ = [
    "OPERATOR_GRAPH_BUILDER_ID",
    "OPERATOR_GRAPH_PACKAGE_VERSION",
    "OperatorGraphAdmission",
    "OperatorGraphPlan",
    "compile_operator_graph_package",
]
