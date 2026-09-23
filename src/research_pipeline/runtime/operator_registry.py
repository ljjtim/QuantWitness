"""从唯一 OperatorDefinition 清单派生准入注册表与 Runtime DAG。"""

from __future__ import annotations

from research_pipeline.extensions import (
    ProjectOperatorImplementationToken,
    TrustedOperatorRegistry,
)
from research_pipeline.data_plane.query_ir import QUERY_IR_V2_VERSION
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.metric_contracts import MetricReachabilityProof

from .compiler import build_production_registry, compile_production_dag
from .contracts import (
    CheckpointPolicy,
    NodeSpec,
    PartitionSpec,
    ResourceBudget,
    RetryPolicy,
)
from .graph import DagSpec, Edge
from .operator_definitions import build_mainline_operator_manifest
from .operator_registry_builder import build_operator_registry_from_manifest


def _node_configuration_hash(plan: object, node_payload: dict[str, object]) -> str:
    queries = getattr(plan, "queries", ())
    minute_queries = tuple(
        query
        for query in queries
        if getattr(query, "ir_version", None) == QUERY_IR_V2_VERSION
    )
    payload: dict[str, object] = {
        "node": node_payload,
        "root_seed": getattr(plan, "root_seed", None),
        "fixed_clock": getattr(plan, "fixed_clock", None),
    }
    metric_proofs = getattr(plan, "metric_proofs", ())
    if metric_proofs:
        if not isinstance(metric_proofs, tuple) or any(
            not isinstance(item, MetricReachabilityProof) for item in metric_proofs
        ):
            raise ValueError("operator graph metric reachability proof 无效")
        payload["metric_proof_digests"] = [
            item.proof_digest
            for item in sorted(metric_proofs, key=lambda item: item.metric_ref)
        ]
    research_semantics = getattr(plan, "research_semantics", None)
    if research_semantics is not None:
        payload["research_semantics_hash"] = research_semantics.semantics_hash
    if minute_queries:
        payload["minute_query_identity_hash"] = typed_canonical_hash(
            [query.to_dict() for query in queries]
        )
    return typed_canonical_hash(payload)


def build_mainline_operator_registry() -> TrustedOperatorRegistry:
    """从主线定义清单派生封闭准入注册表。"""
    manifest = build_mainline_operator_manifest()
    return build_operator_registry_from_manifest(manifest)


def compile_operator_graph_dag(
    plan: object, registry: TrustedOperatorRegistry
) -> DagSpec:
    """从同一 OperatorDefinition 派生 NodeSpec，并接入正式 Runtime 注册表。"""
    recipe = getattr(plan, "recipe", None)
    admission_hash = getattr(plan, "admission_hash", None)
    registry_hash = getattr(plan, "registry_hash", None)
    topological_order = getattr(plan, "topological_order", None)
    metric_proofs = getattr(plan, "metric_proofs", None)
    if recipe is None or not isinstance(topological_order, tuple):
        raise ValueError("operator graph plan 结构无效")
    if (
        not isinstance(metric_proofs, tuple)
        or not metric_proofs
        or any(not isinstance(item, MetricReachabilityProof) for item in metric_proofs)
    ):
        raise ValueError("operator graph plan 缺少已验证 metric reachability proof")
    admitted = registry.admit(recipe)
    if (
        admitted.admission_hash != admission_hash
        or admitted.registry_hash != registry_hash
    ):
        raise ValueError("operator graph plan 与当前可信注册表不一致")

    manifest = build_mainline_operator_manifest()
    node_by_id = {item.node_id: item for item in recipe.nodes}
    definitions = {}
    for node in recipe.nodes:
        binding = registry.binding(node.operator_id, node.operator_version)
        if isinstance(binding.implementation_token, ProjectOperatorImplementationToken):
            continue
        definition = manifest.require_operator(node.operator_id, node.operator_version)
        definitions[node.node_id] = definition
        if binding.implementation_id != definition.implementation_ref.implementation_id:
            raise ValueError(
                f"operator implementation 未绑定或漂移: {node.operator_id}"
            )

    retry = RetryPolicy(2, ("worker_crash", "heartbeat_timeout"))

    def build_node(node_id: str) -> NodeSpec:
        node = node_by_id[node_id]
        spec = registry.require_operator(node.operator_id, node.operator_version)
        binding = registry.binding(node.operator_id, node.operator_version)
        token = binding.implementation_token
        if isinstance(token, ProjectOperatorImplementationToken):
            implementation_id = token.implementation_id
            implementation_identity = {
                "bundle_hash": token.manifest.bundle_hash,
            }
            partition = PartitionSpec(False, ())
            cacheable = False
        else:
            definition = definitions[node_id]
            implementation_id = definition.implementation_ref.implementation_id
            implementation_identity = None
            partition = PartitionSpec(
                bool(definition.partition_keys), definition.partition_keys
            )
            cacheable = definition.cache_profile_ref == "cache.semantic-pure.v1"
        configuration_hash = _node_configuration_hash(plan, node.to_dict())
        if implementation_identity is not None:
            configuration_hash = typed_canonical_hash(
                {
                    "configuration_hash": configuration_hash,
                    "implementation_identity": implementation_identity,
                }
            )
        return NodeSpec(
            node_id,
            implementation_id,
            tuple((item.port, item.artifact_type) for item in spec.input_ports),
            tuple((item.port, item.artifact_type) for item in spec.output_ports),
            ResourceBudget(**dict(spec.resource_profile)),
            retry,
            CheckpointPolicy.REQUIRED,
            partition,
            cacheable=cacheable,
            pure=cacheable,
            configuration_hash=configuration_hash,
        )

    nodes = tuple(build_node(node_id) for node_id in topological_order)
    edges = tuple(
        Edge(
            binding.source_node_id,
            binding.source_output_port,
            node.node_id,
            binding.input_port,
            {
                item.port: item.artifact_type
                for item in registry.require_operator(
                    node.operator_id, node.operator_version
                ).input_ports
            }[binding.input_port],
        )
        for node in recipe.nodes
        for binding in node.inputs
    )
    dag = DagSpec(recipe.graph_id, nodes, edges)
    project_ids = {
        token.implementation_id
        for node in recipe.nodes
        for token in (
            registry.binding(
                node.operator_id, node.operator_version
            ).implementation_token,
        )
        if isinstance(token, ProjectOperatorImplementationToken)
    }
    if not project_ids:
        return compile_production_dag(dag, build_production_registry())
    # Admission 阶段不加载项目代码；仍逐个复验所有核心实现的正式注册事实。
    for node in nodes:
        if node.implementation_id not in project_ids:
            compile_production_dag(
                DagSpec(f"admission.{node.node_id}", (node,), ()),
                build_production_registry(),
            )
    return dag


def admitted_implementation_manifest_hash(
    registry: TrustedOperatorRegistry,
) -> str:
    """返回当前组合注册表实际准入实现的唯一摘要。"""
    core_manifest_hash = build_mainline_operator_manifest().manifest_hash
    implementation_identities = getattr(registry, "implementation_identities", None)
    project_id = getattr(registry, "project_id", None)
    if not implementation_identities:
        return core_manifest_hash
    if not isinstance(project_id, str) or not project_id:
        raise ValueError("项目组合注册表缺少 project_id")
    return typed_canonical_hash(
        {
            "contract_version": "research-admitted-implementation-manifest-v1",
            "core_manifest_hash": core_manifest_hash,
            "project_id": project_id,
            "implementation_identities": {
                str(key): dict(value)
                for key, value in sorted(implementation_identities.items())
            },
        }
    )


__all__ = [
    "admitted_implementation_manifest_hash",
    "build_mainline_operator_registry",
    "compile_operator_graph_dag",
]
