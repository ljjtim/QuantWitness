"""Typed DAG 的结构校验和稳定拓扑排序。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import NodeSpec, _require_fields, _require_id
from .errors import RuntimeGraphError


DAG_CONTRACT_VERSION = "research-runtime-dag-v1"


@dataclass(frozen=True)
class Edge:
    source_node: str
    source_port: str
    target_node: str
    target_port: str
    artifact_type: str

    def to_dict(self) -> dict[str, str]:
        return {"source_node": self.source_node, "source_port": self.source_port, "target_node": self.target_node, "target_port": self.target_port, "artifact_type": self.artifact_type}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Edge:
        fields = {"source_node", "source_port", "target_node", "target_port", "artifact_type"}
        _require_fields(payload, fields, "Edge")
        return cls(*(str(payload[key]) for key in ("source_node", "source_port", "target_node", "target_port", "artifact_type")))


@dataclass(frozen=True)
class DagSpec:
    name: str
    nodes: tuple[NodeSpec, ...]
    edges: tuple[Edge, ...]
    contract_version: str = DAG_CONTRACT_VERSION

    def __post_init__(self) -> None:
        try:
            _require_id(self.name, "dag name")
            if self.contract_version != DAG_CONTRACT_VERSION:
                raise RuntimeGraphError("DAG contract version 不受支持")
            by_id = {node.node_id: node for node in self.nodes}
            if not self.nodes or len(by_id) != len(self.nodes):
                raise RuntimeGraphError("DAG 节点不能为空且 node_id 不得重复")
            consumed: set[tuple[str, str]] = set()
            for edge in self.edges:
                if edge.source_node not in by_id or edge.target_node not in by_id:
                    raise RuntimeGraphError("edge 引用了不存在的节点")
                source = dict(by_id[edge.source_node].output_types)
                target = dict(by_id[edge.target_node].input_types)
                if source.get(edge.source_port) != edge.artifact_type or target.get(edge.target_port) != edge.artifact_type:
                    raise RuntimeGraphError("edge 端口类型不匹配")
                target_key = (edge.target_node, edge.target_port)
                if target_key in consumed:
                    raise RuntimeGraphError("一个输入端口只能有一个所有者")
                consumed.add(target_key)
            self.topological_order()
        except RuntimeGraphError:
            raise
        except Exception as exc:
            raise RuntimeGraphError(str(exc)) from exc

    @property
    def dag_id(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def topological_order(self) -> tuple[str, ...]:
        incoming = {node.node_id: 0 for node in self.nodes}
        outgoing = {node.node_id: [] for node in self.nodes}
        for edge in self.edges:
            incoming[edge.target_node] += 1
            outgoing[edge.source_node].append(edge.target_node)
        ready = sorted(key for key, count in incoming.items() if count == 0)
        order: list[str] = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            for target in sorted(outgoing[current]):
                incoming[target] -= 1
                if incoming[target] == 0:
                    ready.append(target)
                    ready.sort()
        if len(order) != len(self.nodes):
            raise RuntimeGraphError("DAG 存在环")
        return tuple(order)

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "nodes": [node.to_dict() for node in self.nodes], "edges": [edge.to_dict() for edge in self.edges], "contract_version": self.contract_version}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> DagSpec:
        _require_fields(payload, {"name", "nodes", "edges", "contract_version"}, "DagSpec")
        return cls(str(payload["name"]), tuple(NodeSpec.from_dict(item) for item in payload["nodes"]), tuple(Edge.from_dict(item) for item in payload["edges"]), str(payload["contract_version"]))


__all__ = ["DAG_CONTRACT_VERSION", "DagSpec", "Edge"]
