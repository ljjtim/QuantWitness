"""在 Package 编译期验证 ResultSpec 与受信算子端口、指标证明闭合。"""

from __future__ import annotations

from typing import Mapping, Protocol

from research_pipeline.platform.operator_contracts import OperatorGraphRecipe

from .contracts import RESULT_SPEC_VERSION, ResultSpec, ResultTableSpec
from .errors import ResultContractError


_FORMAL_CAUSAL_TABLE_PATHS = {
    "research.feature-set.v1": "features",
    "research.label.v1": "labels",
    "research.minute-features.v1": "features",
    "research.minute-labels.v1": "labels",
}


class ResultOperatorResolver(Protocol):
    def require_operator(self, operator_id: str, operator_version: str) -> object:
        """返回带 output_ports 的可信算子合同。"""


def compile_result_spec(
    payload: object,
    *,
    recipe: OperatorGraphRecipe,
    resolver: ResultOperatorResolver,
) -> ResultSpec:
    if not isinstance(payload, Mapping) or set(payload) != {"contract_version", "tables"}:
        raise ResultContractError("result spec 顶层 schema 无效")
    if payload["contract_version"] != RESULT_SPEC_VERSION:
        raise ResultContractError("result spec 版本不受支持")
    raw_tables = payload["tables"]
    if not isinstance(raw_tables, (list, tuple)) or not raw_tables or any(
        not isinstance(item, Mapping) for item in raw_tables
    ):
        raise ResultContractError("result spec tables 必须是非空映射列表")
    tables = list(ResultTableSpec.from_dict(item) for item in raw_tables)
    nodes = {item.node_id: item for item in recipe.nodes}
    for table in tables:
        node = nodes.get(table.source_node_id)
        if node is None:
            raise ResultContractError(f"result table 引用了未知 source node: {table.source_node_id}")
        operator = resolver.require_operator(node.operator_id, node.operator_version)
        outputs = {
            item.port: item.artifact_type
            for item in getattr(operator, "output_ports", ())
        }
        if outputs.get(table.source_port) != table.artifact_type:
            raise ResultContractError(
                f"result table source port/artifact type 不匹配: {table.table_id}"
            )
    selected = {
        (
            item.source_node_id,
            item.source_port,
            item.artifact_type,
            item.path_prefix,
        )
        for item in tables
    }
    formal_roots = {
        item.source_node_id for item in tables if item.role != "diagnostic"
    }
    reachable_nodes = set(formal_roots)
    pending = list(formal_roots)
    while pending:
        node_id = pending.pop()
        node = nodes[node_id]
        for binding in node.inputs:
            if binding.source_node_id in reachable_nodes:
                continue
            reachable_nodes.add(binding.source_node_id)
            pending.append(binding.source_node_id)
    for node in recipe.nodes:
        if node.node_id not in reachable_nodes:
            continue
        operator = resolver.require_operator(node.operator_id, node.operator_version)
        for port in getattr(operator, "output_ports", ()):
            artifact_type = str(port.artifact_type)
            path_prefix = _FORMAL_CAUSAL_TABLE_PATHS.get(artifact_type)
            identity = (
                node.node_id,
                str(port.port),
                artifact_type,
                path_prefix,
            )
            if path_prefix is None or identity in selected:
                continue
            stable = f"{node.node_id}.{port.port}".replace("_", "-")
            tables.append(ResultTableSpec(
                table_id=f"causal-time.{stable}",
                role="diagnostic",
                source_node_id=node.node_id,
                source_port=str(port.port),
                artifact_type=artifact_type,
                schema_id=f"research.causal-time.{stable}.v2",
                path_prefix=path_prefix,
            ))
            selected.add(identity)
    return ResultSpec.build(tables)


__all__ = ["ResultOperatorResolver", "compile_result_spec"]
