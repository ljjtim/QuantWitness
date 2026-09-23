"""有限、白名单、可哈希的研究变换图合同。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from enum import Enum
from types import MappingProxyType
from typing import Iterable, Mapping

from research_pipeline.catalog import CompiledCatalog, TransformContract, TransformDescriptor
from research_pipeline.catalog.source_loader import load_contract_payload
from research_pipeline.catalog.transforms import validate_transform_manifest
from research_pipeline.platform import typed_canonical_hash

from .errors import QueryIRInvalidError


TRANSFORM_GRAPH_VERSION = "research-transform-graph-v1"
COMPILED_TRANSFORM_GRAPH_VERSION = "compiled-research-transform-graph-v1"


class TransformGraphError(QueryIRInvalidError):
    """变换图合同、端口、时间方向或注册实现无效。"""


class TransformNodeType(str, Enum):
    SCAN_REF = "ScanRef"
    AS_OF_JOIN = "AsOfJoin"
    INTERVAL_JOIN = "IntervalJoin"
    CALENDAR_ALIGN = "CalendarAlign"
    RESAMPLE = "Resample"
    LAG = "Lag"
    ROLLING = "Rolling"
    CROSS_SECTION = "CrossSectionTransform"
    FEATURE = "Feature"
    LABEL = "Label"
    EVENT_WINDOW = "EventWindow"


class TransformPortType(str, Enum):
    FRAME = "frame"
    FEATURE = "feature"
    LABEL = "label"
    EVENT = "event"
    SIGNAL = "signal"


def _freeze(value: object, field_name: str) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TransformGraphError(f"{field_name} 的 key 必须是字符串")
        return MappingProxyType(
            {key: _freeze(item, f"{field_name}.{key}") for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item, field_name) for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TransformGraphError(f"{field_name} 含不支持类型: {type(value).__name__}")


def _plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TransformGraphError(f"{field_name} 必须是非空字符串")
    return value


def _sha256(value: object, field_name: str) -> str:
    text = _text(value, field_name)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise TransformGraphError(f"{field_name} 必须是 sha256 小写摘要")
    return text


def _temporal_value(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise TransformGraphError(f"{field_name} 必须是 ISO 日期或带时区时间")
    try:
        if len(value) == 10:
            return datetime.combine(date.fromisoformat(value), time.min, timezone.utc)
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise TransformGraphError(f"{field_name} 不是有效 ISO 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TransformGraphError(f"{field_name} 的 datetime 必须带时区")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class TransformInput:
    source_node_id: str
    source_port: str = "result"
    target_port: str = "input"

    def __post_init__(self) -> None:
        _text(self.source_node_id, "source_node_id")
        _text(self.source_port, "source_port")
        _text(self.target_port, "target_port")

    def to_dict(self) -> dict[str, str]:
        return {
            "source_node_id": self.source_node_id,
            "source_port": self.source_port,
            "target_port": self.target_port,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "TransformInput":
        if set(payload) != {"source_node_id", "source_port", "target_port"}:
            raise TransformGraphError("TransformInput schema 不匹配")
        return cls(
            str(payload["source_node_id"]),
            str(payload["source_port"]),
            str(payload["target_port"]),
        )


@dataclass(frozen=True)
class TransformGraphNode:
    node_id: str
    node_type: TransformNodeType
    inputs: tuple[TransformInput, ...]
    output_port_type: TransformPortType
    output_fields: tuple[str, ...]
    parameters: Mapping[str, object] = field(default_factory=dict)
    transform_id: str | None = None
    transform_version: int | None = None
    input_bindings: Mapping[str, object] = field(default_factory=dict)
    output_bindings: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _text(self.node_id, "node_id")
        if not isinstance(self.node_type, TransformNodeType):
            raise TransformGraphError("node_type 不受支持")
        if not isinstance(self.output_port_type, TransformPortType):
            raise TransformGraphError("output_port_type 不受支持")
        if not self.output_fields or any(
            not isinstance(item, str) or not item.strip() for item in self.output_fields
        ):
            raise TransformGraphError("output_fields 必须是非空字段 ID")
        if len(self.output_fields) != len(set(self.output_fields)):
            raise TransformGraphError("output_fields 不能重复")
        targets = [item.target_port for item in self.inputs]
        if len(targets) != len(set(targets)):
            raise TransformGraphError("同一节点 target_port 不能重复")
        object.__setattr__(self, "parameters", _freeze(self.parameters, "parameters"))
        object.__setattr__(self, "input_bindings", _freeze(self.input_bindings, "input_bindings"))
        object.__setattr__(self, "output_bindings", _freeze(self.output_bindings, "output_bindings"))
        for name in ("input_bindings", "output_bindings"):
            if any(
                not isinstance(value, str) or not value.strip()
                for value in getattr(self, name).values()
            ):
                raise TransformGraphError(f"{name} 的值必须是非空 Catalog field ID")
        if self.node_type == TransformNodeType.SCAN_REF:
            if (
                self.inputs
                or self.transform_id is not None
                or self.transform_version is not None
                or self.input_bindings
                or self.output_bindings
            ):
                raise TransformGraphError("ScanRef 不接受输入或 Transform 实现")
            if set(self.parameters) != {"request_id"}:
                raise TransformGraphError("ScanRef 只能声明 request_id")
            _text(self.parameters["request_id"], "ScanRef.request_id")
        else:
            if not self.inputs:
                raise TransformGraphError(f"{self.node_type.value} 至少需要一个输入")
            _text(self.transform_id, "transform_id")
            if type(self.transform_version) is not int or self.transform_version < 1:
                raise TransformGraphError("transform_version 必须是正整数")
        _validate_node_parameters(self)

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "inputs": [item.to_dict() for item in self.inputs],
            "output_port_type": self.output_port_type.value,
            "output_fields": list(self.output_fields),
            "parameters": _plain(self.parameters),
            "transform_id": self.transform_id,
            "transform_version": self.transform_version,
            "input_bindings": _plain(self.input_bindings),
            "output_bindings": _plain(self.output_bindings),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "TransformGraphNode":
        expected = {
            "node_id", "node_type", "inputs", "output_port_type", "output_fields",
            "parameters", "transform_id", "transform_version",
            "input_bindings", "output_bindings",
        }
        if set(payload) != expected:
            raise TransformGraphError("TransformGraphNode schema 不匹配")
        raw_inputs = payload["inputs"]
        if not isinstance(raw_inputs, list) or any(
            not isinstance(item, Mapping) for item in raw_inputs
        ):
            raise TransformGraphError("TransformGraphNode inputs 必须是对象列表")
        parameters = payload["parameters"]
        if (
            not isinstance(parameters, Mapping)
            or not isinstance(payload["input_bindings"], Mapping)
            or not isinstance(payload["output_bindings"], Mapping)
        ):
            raise TransformGraphError("TransformGraphNode parameters/bindings 必须是映射")
        output_fields = payload["output_fields"]
        if not isinstance(output_fields, list):
            raise TransformGraphError("TransformGraphNode output_fields 必须是列表")
        return cls(
            str(payload["node_id"]),
            TransformNodeType(str(payload["node_type"])),
            tuple(TransformInput.from_dict(item) for item in raw_inputs),
            TransformPortType(str(payload["output_port_type"])),
            tuple(str(item) for item in output_fields),
            parameters,
            None if payload["transform_id"] is None else str(payload["transform_id"]),
            None if payload["transform_version"] is None else int(payload["transform_version"]),
            payload["input_bindings"],
            payload["output_bindings"],
        )


def _validate_node_parameters(node: TransformGraphNode) -> None:
    parameters = node.parameters
    if node.node_type == TransformNodeType.LAG:
        if set(parameters) != {"periods"} or type(parameters["periods"]) is not int or parameters["periods"] < 1:
            raise TransformGraphError("Lag periods 必须是正整数")
    elif node.node_type == TransformNodeType.ROLLING:
        expected = {"window", "min_periods", "aggregation"}
        if set(parameters) != expected:
            raise TransformGraphError("Rolling 参数 schema 不匹配")
        if (
            type(parameters["window"]) is not int
            or type(parameters["min_periods"]) is not int
            or not 1 <= parameters["min_periods"] <= parameters["window"]
            or parameters["aggregation"] not in {"mean", "sum", "std", "min", "max"}
        ):
            raise TransformGraphError("Rolling 参数无效")
    elif node.node_type == TransformNodeType.CROSS_SECTION:
        if set(parameters) != {"method"} or parameters["method"] not in {
            "rank", "zscore", "winsorize", "demean"
        }:
            raise TransformGraphError("CrossSectionTransform method 不在白名单")
    elif node.node_type == TransformNodeType.CALENDAR_ALIGN:
        if set(parameters) != {"calendar_id", "direction"} or parameters["direction"] not in {"exact", "backward"}:
            raise TransformGraphError("CalendarAlign 只允许 exact/backward")
    elif node.node_type == TransformNodeType.RESAMPLE:
        if set(parameters) != {"interval", "closed", "label"} or parameters["closed"] != "right" or parameters["label"] != "right":
            raise TransformGraphError("Resample 必须使用右闭合、右标记的已完成 bar")
        _text(parameters["interval"], "Resample.interval")
    elif node.node_type == TransformNodeType.FEATURE:
        if set(parameters) != {"feature_id"}:
            raise TransformGraphError("Feature 参数 schema 不匹配")
        _text(parameters["feature_id"], "Feature.feature_id")
    elif node.node_type == TransformNodeType.LABEL:
        if set(parameters) != {"label_id", "horizon", "available_after"}:
            raise TransformGraphError("Label 参数 schema 不匹配")
        if type(parameters["horizon"]) is not int or parameters["horizon"] < 1:
            raise TransformGraphError("Label horizon 必须是正整数")
        if parameters["available_after"] not in {"exit", "settlement"}:
            raise TransformGraphError("Label 只能在退出或结算后可见")
    elif node.node_type == TransformNodeType.EVENT_WINDOW:
        if set(parameters) != {"before", "after", "anchor_field"}:
            raise TransformGraphError("EventWindow 参数 schema 不匹配")
        if any(type(parameters[key]) is not int or parameters[key] < 0 for key in ("before", "after")):
            raise TransformGraphError("EventWindow before/after 必须是非负整数")
        if parameters["before"] == parameters["after"] == 0:
            raise TransformGraphError("EventWindow 不能为空")
        _text(parameters["anchor_field"], "EventWindow.anchor_field")


@dataclass(frozen=True)
class TransformGraph:
    graph_id: str
    nodes: tuple[TransformGraphNode, ...]
    outputs: tuple[str, ...]
    contract_version: str = TRANSFORM_GRAPH_VERSION

    def __post_init__(self) -> None:
        _text(self.graph_id, "graph_id")
        if self.contract_version != TRANSFORM_GRAPH_VERSION:
            raise TransformGraphError("TransformGraph contract_version 不受支持")
        if not self.nodes:
            raise TransformGraphError("TransformGraph nodes 不能为空")
        node_ids = [item.node_id for item in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise TransformGraphError("TransformGraph node_id 不能重复")
        if not self.outputs or len(self.outputs) != len(set(self.outputs)):
            raise TransformGraphError("TransformGraph outputs 必须非空且唯一")
        if not set(self.outputs) <= set(node_ids):
            raise TransformGraphError("TransformGraph outputs 引用未知节点")

    def to_dict(self) -> dict[str, object]:
        return {
            "graph_id": self.graph_id,
            "nodes": [item.to_dict() for item in self.nodes],
            "outputs": list(self.outputs),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "TransformGraph":
        if set(payload) != {"graph_id", "nodes", "outputs", "contract_version"}:
            raise TransformGraphError("TransformGraph schema 不匹配")
        nodes = payload["nodes"]
        outputs = payload["outputs"]
        if not isinstance(nodes, list) or any(not isinstance(item, Mapping) for item in nodes):
            raise TransformGraphError("TransformGraph nodes 必须是对象列表")
        if not isinstance(outputs, list):
            raise TransformGraphError("TransformGraph outputs 必须是列表")
        return cls(
            str(payload["graph_id"]),
            tuple(TransformGraphNode.from_dict(item) for item in nodes),
            tuple(str(item) for item in outputs),
            str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class CompiledTransformGraph:
    graph: TransformGraph
    topological_order: tuple[str, ...]
    catalog_hash: str
    transform_registry_hash: str
    graph_hash: str
    lineage_hash: str
    contract_version: str = COMPILED_TRANSFORM_GRAPH_VERSION

    def __post_init__(self) -> None:
        _sha256(self.catalog_hash, "catalog_hash")
        _sha256(self.transform_registry_hash, "transform_registry_hash")
        if self.contract_version != COMPILED_TRANSFORM_GRAPH_VERSION:
            raise TransformGraphError("CompiledTransformGraph 版本不受支持")
        if self.graph_hash != typed_canonical_hash(self.payload()):
            raise TransformGraphError("TransformGraph hash 不一致")
        if self.lineage_hash != typed_canonical_hash(self.lineage_payload()):
            raise TransformGraphError("TransformGraph lineage hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "graph": self.graph.to_dict(),
            "topological_order": list(self.topological_order),
            "catalog_hash": self.catalog_hash,
            "transform_registry_hash": self.transform_registry_hash,
            "contract_version": self.contract_version,
        }

    def lineage_payload(self) -> dict[str, object]:
        transform_refs = sorted(
            {
                f"{node.transform_id}@{node.transform_version}"
                for node in self.graph.nodes
                if node.transform_id is not None
            }
        )
        return {
            "lineage_type": "transform_graph",
            "graph_hash": self.graph_hash,
            "catalog_hash": self.catalog_hash,
            "transform_registry_hash": self.transform_registry_hash,
            "transform_refs": transform_refs,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            **self.payload(),
            "graph_hash": self.graph_hash,
            "lineage_hash": self.lineage_hash,
        }


def compile_transform_graph(
    graph: TransformGraph,
    *,
    catalog: CompiledCatalog,
    descriptors: Iterable[TransformDescriptor],
) -> CompiledTransformGraph:
    """用 Catalog 合同和受信实现清单编译有限变换图。"""

    descriptor_items = tuple(descriptors)
    descriptor_by_id = {item.transform_id: item for item in descriptor_items}
    if len(descriptor_by_id) != len(descriptor_items):
        raise TransformGraphError("Transform descriptor ID 重复")
    contracts: dict[str, TransformContract] = {}
    for transform_id, raw in catalog.transforms.items():
        loaded = load_contract_payload({"kind": "transform", **dict(raw)})
        if not isinstance(loaded, TransformContract):
            raise TransformGraphError("Catalog Transform 类型无效")
        contract = loaded
        contracts[transform_id] = contract
    implementation_ids = {item.implementation_id for item in contracts.values()}
    if set(descriptor_by_id) != implementation_ids:
        raise TransformGraphError(
            "Transform descriptor 集合必须与当前 Catalog 已批准实现完全一致"
        )
    try:
        validate_transform_manifest(contracts.values(), descriptor_items)
    except Exception as exc:
        raise TransformGraphError(f"Catalog Transform manifest 无效: {exc}") from exc
    node_by_id = {item.node_id: item for item in graph.nodes}
    order = _topological_order(graph, node_by_id)
    for node_id in order:
        node = node_by_id[node_id]
        for binding in node.inputs:
            source = node_by_id.get(binding.source_node_id)
            if source is None:
                raise TransformGraphError(
                    f"节点 {node.node_id} 引用未知来源: {binding.source_node_id}"
                )
            if binding.source_port != "result":
                raise TransformGraphError("首版 TransformGraph 只允许 result 来源端口")
        if node.node_type == TransformNodeType.SCAN_REF:
            for field_id in node.output_fields:
                catalog.require_field(field_id)
            continue
        contract = contracts.get(str(node.transform_id))
        descriptor = (
            None
            if contract is None
            else descriptor_by_id.get(contract.implementation_id)
        )
        if contract is None or contract.transform_version != node.transform_version:
            raise TransformGraphError(
                f"节点 {node.node_id} 引用未知 Catalog Transform/version"
            )
        if descriptor is None or descriptor.descriptor_version != "transform-descriptor-v2":
            raise TransformGraphError(
                f"节点 {node.node_id} 必须绑定 transform-descriptor-v2"
            )
        available_fields = {
            field_id
            for binding in node.inputs
            for field_id in node_by_id[binding.source_node_id].output_fields
        }
        if contract.input_schema:
            _validate_bound_schema(
                node,
                contract,
                catalog,
                available_fields,
                node_by_id,
            )
        else:
            if node.input_bindings or node.output_bindings:
                raise TransformGraphError("字段绑定只允许用于显式端口 schema Transform")
            missing_inputs = set(contract.inputs) - available_fields
            if missing_inputs:
                raise TransformGraphError(
                    f"节点 {node.node_id} 缺少 Transform 输入字段: {sorted(missing_inputs)}"
                )
            if node.output_fields != contract.outputs:
                raise TransformGraphError(f"节点 {node.node_id} 输出 schema 与 Catalog 不一致")
        _validate_registered_time_semantics(node, contract)
        _validate_parameter_schema(node.parameters, contract.parameters_schema, node.node_id)
    _reject_label_ancestry(graph, node_by_id)
    registry_payload = {
        "descriptors": [item.to_dict() for item in sorted(descriptor_items, key=lambda item: item.transform_id)]
    }
    registry_hash = typed_canonical_hash(registry_payload)
    payload = {
        "graph": graph.to_dict(),
        "topological_order": list(order),
        "catalog_hash": catalog.catalog_hash,
        "transform_registry_hash": registry_hash,
        "contract_version": COMPILED_TRANSFORM_GRAPH_VERSION,
    }
    graph_hash = typed_canonical_hash(payload)
    lineage_payload = {
        "lineage_type": "transform_graph",
        "graph_hash": graph_hash,
        "catalog_hash": catalog.catalog_hash,
        "transform_registry_hash": registry_hash,
        "transform_refs": sorted(
            {
                f"{node.transform_id}@{node.transform_version}"
                for node in graph.nodes
                if node.transform_id is not None
            }
        ),
    }
    return CompiledTransformGraph(
        graph,
        order,
        catalog.catalog_hash,
        registry_hash,
        graph_hash,
        typed_canonical_hash(lineage_payload),
    )


def _topological_order(
    graph: TransformGraph,
    node_by_id: Mapping[str, TransformGraphNode],
) -> tuple[str, ...]:
    dependencies = {
        node.node_id: {item.source_node_id for item in node.inputs}
        for node in graph.nodes
    }
    for node_id, items in dependencies.items():
        if node_id in items:
            raise TransformGraphError("TransformGraph 不允许自环")
        unknown = items - set(node_by_id)
        if unknown:
            raise TransformGraphError(f"TransformGraph 引用未知节点: {sorted(unknown)}")
    ready = sorted(node_id for node_id, items in dependencies.items() if not items)
    result: list[str] = []
    while ready:
        current = ready.pop(0)
        result.append(current)
        for node_id in sorted(dependencies):
            if current in dependencies[node_id]:
                dependencies[node_id].remove(current)
                if not dependencies[node_id] and node_id not in result and node_id not in ready:
                    ready.append(node_id)
                    ready.sort()
    if len(result) != len(graph.nodes):
        raise TransformGraphError("TransformGraph 依赖存在环")
    return tuple(result)


def _validate_registered_time_semantics(
    node: TransformGraphNode,
    contract: TransformContract,
) -> None:
    policy = contract.time_policy
    expected = {
        "node_type", "output_port", "visibility", "future_dependency"
    }
    if not expected <= set(policy):
        raise TransformGraphError(f"Transform {contract.transform_id} 时间传播合同不完整")
    if policy["node_type"] != node.node_type.value:
        raise TransformGraphError(f"节点 {node.node_id} 类型与 Catalog Transform 不一致")
    if policy["output_port"] != node.output_port_type.value:
        raise TransformGraphError(f"节点 {node.node_id} 输出端口与时间合同不一致")
    if type(policy["future_dependency"]) is not bool:
        raise TransformGraphError("future_dependency 必须是 boolean")
    if node.output_port_type in {TransformPortType.FEATURE, TransformPortType.SIGNAL}:
        if policy["future_dependency"] or policy["visibility"] not in {
            "at_or_before_input", "at_or_before_as_of"
        }:
            raise TransformGraphError("feature/signal 不允许未来可见性")
    if node.output_port_type == TransformPortType.LABEL and policy["visibility"] not in {
        "after_exit", "after_settlement"
    }:
        raise TransformGraphError("label 必须在退出或结算后可见")
    if node.node_type == TransformNodeType.AS_OF_JOIN:
        required = {"available_time_field", "revision_field", "revision_selection"}
        if not required <= set(policy) or policy["revision_selection"] != "latest_available_at_or_before_as_of":
            raise TransformGraphError("AsOfJoin 必须绑定公告可见日和修订选择")
        if not {str(policy["available_time_field"]), str(policy["revision_field"])} <= set(contract.inputs):
            raise TransformGraphError("AsOfJoin PIT 字段必须进入输入 schema")
    if node.node_type == TransformNodeType.INTERVAL_JOIN:
        required = {
            "effective_from_field", "effective_to_field", "available_time_field"
        }
        if not required <= set(policy):
            raise TransformGraphError("IntervalJoin 必须绑定有效区间和可见时间")
        if not {str(policy[key]) for key in required} <= set(contract.inputs):
            raise TransformGraphError("IntervalJoin 时态字段必须进入输入 schema")


def _validate_parameter_schema(
    parameters: Mapping[str, object],
    schema: Mapping[str, object],
    node_id: str,
) -> None:
    required = schema.get("required", ())
    properties = schema.get("properties", {})
    additional = schema.get("additionalProperties", False)
    if not isinstance(required, (tuple, list)) or not isinstance(properties, Mapping):
        raise TransformGraphError(f"节点 {node_id} parameters_schema 无效")
    if not set(required) <= set(parameters):
        raise TransformGraphError(f"节点 {node_id} 缺少必需参数")
    if additional is not True and not set(parameters) <= set(properties):
        raise TransformGraphError(f"节点 {node_id} 含未登记参数")
    for name, value in parameters.items():
        rule = properties.get(name, {})
        if not isinstance(rule, Mapping):
            raise TransformGraphError(f"节点 {node_id} 参数 {name} schema 无效")
        expected_type = rule.get("type")
        if expected_type == "integer" and type(value) is not int:
            raise TransformGraphError(f"节点 {node_id} 参数 {name} 必须是 integer")
        if expected_type == "number" and (
            isinstance(value, bool) or not isinstance(value, (int, float))
        ):
            raise TransformGraphError(f"节点 {node_id} 参数 {name} 必须是 number")
        if expected_type == "string" and not isinstance(value, str):
            raise TransformGraphError(f"节点 {node_id} 参数 {name} 必须是 string")
        if expected_type == "boolean" and type(value) is not bool:
            raise TransformGraphError(f"节点 {node_id} 参数 {name} 必须是 boolean")
        if "enum" in rule and value not in rule["enum"]:
            raise TransformGraphError(f"节点 {node_id} 参数 {name} 不在 enum")
        if "minimum" in rule and isinstance(value, (int, float)) and value < rule["minimum"]:
            raise TransformGraphError(f"节点 {node_id} 参数 {name} 小于 minimum")


def _validate_bound_schema(
    node: TransformGraphNode,
    contract: TransformContract,
    catalog: CompiledCatalog,
    available_fields: set[str],
    node_by_id: Mapping[str, TransformGraphNode],
) -> None:
    edge_ports = {binding.target_port for binding in node.inputs}
    if edge_ports != set(contract.inputs):
        raise TransformGraphError(f"节点 {node.node_id} 输入边未完整闭合 Catalog 端口")
    if set(node.input_bindings) != set(contract.inputs):
        raise TransformGraphError(f"节点 {node.node_id} input_bindings 不完整")
    if set(node.output_bindings) != set(contract.outputs):
        raise TransformGraphError(f"节点 {node.node_id} output_bindings 不完整")
    input_fields = tuple(str(node.input_bindings[port]) for port in contract.inputs)
    output_fields = tuple(str(node.output_bindings[port]) for port in contract.outputs)
    if not set(input_fields) <= available_fields:
        raise TransformGraphError(f"节点 {node.node_id} 绑定了不可达输入字段")
    for binding in node.inputs:
        bound_field = str(node.input_bindings[binding.target_port])
        source_fields = set(node_by_id[binding.source_node_id].output_fields)
        if bound_field not in source_fields:
            raise TransformGraphError(
                f"节点 {node.node_id} 端口 {binding.target_port} 未绑定对应来源字段"
            )
    if output_fields != node.output_fields:
        raise TransformGraphError(f"节点 {node.node_id} 输出绑定与 output_fields 不一致")
    for port, field_id in (*zip(contract.inputs, input_fields), *zip(contract.outputs, output_fields)):
        field_contract = catalog.require_field(field_id)
        schema = contract.input_schema.get(port) or contract.output_schema.get(port)
        if not isinstance(schema, Mapping) or schema.get("data_type") != field_contract["data_type"]:
            raise TransformGraphError(f"节点 {node.node_id} 端口 {port} 数据类型不匹配")


def _reject_label_ancestry(
    graph: TransformGraph,
    node_by_id: Mapping[str, TransformGraphNode],
) -> None:
    for node in graph.nodes:
        if node.output_port_type not in {TransformPortType.FEATURE, TransformPortType.SIGNAL}:
            continue
        pending = [item.source_node_id for item in node.inputs]
        visited: set[str] = set()
        while pending:
            source_id = pending.pop()
            if source_id in visited:
                continue
            visited.add(source_id)
            source = node_by_id[source_id]
            if source.output_port_type == TransformPortType.LABEL:
                raise TransformGraphError("label 端口不能成为 feature/signal 的祖先")
            pending.extend(item.source_node_id for item in source.inputs)


def select_as_of_records(
    records: Iterable[Mapping[str, object]],
    *,
    entity_fields: tuple[str, ...],
    available_time_field: str,
    revision_field: str,
    as_of: str,
) -> tuple[Mapping[str, object], ...]:
    """按可见时间选择每个经济实体的最新已知修订。"""

    if not entity_fields or len(entity_fields) != len(set(entity_fields)):
        raise TransformGraphError("entity_fields 必须非空且唯一")
    cutoff = _temporal_value(as_of, "as_of")
    visible: dict[tuple[object, ...], list[Mapping[str, object]]] = {}
    economic_keys: set[tuple[object, ...]] = set()
    for record in records:
        required = {*entity_fields, available_time_field, revision_field}
        if not required <= set(record):
            raise TransformGraphError("AsOfJoin 记录缺少实体、可见或修订字段")
        entity = tuple(record[field] for field in entity_fields)
        available = _temporal_value(record[available_time_field], available_time_field)
        revision = _temporal_value(record[revision_field], revision_field)
        economic_key = (*entity, available, revision)
        if economic_key in economic_keys:
            raise TransformGraphError("AsOfJoin 记录经济键重复")
        economic_keys.add(economic_key)
        if available <= cutoff and revision <= cutoff:
            visible.setdefault(entity, []).append(record)
    selected: list[Mapping[str, object]] = []
    for entity in sorted(visible, key=repr):
        candidates = visible[entity]
        latest = max(
            (
                _temporal_value(item[revision_field], revision_field),
                _temporal_value(item[available_time_field], available_time_field),
            )
            for item in candidates
        )
        winners = [
            item for item in candidates
            if (
                _temporal_value(item[revision_field], revision_field),
                _temporal_value(item[available_time_field], available_time_field),
            ) == latest
        ]
        if len(winners) != 1:
            raise TransformGraphError("AsOfJoin 最新修订存在冲突")
        selected.append(MappingProxyType(dict(winners[0])))
    return tuple(selected)


def select_interval_records(
    records: Iterable[Mapping[str, object]],
    *,
    entity_fields: tuple[str, ...],
    effective_from_field: str,
    effective_to_field: str,
    available_time_field: str,
    as_of: str,
) -> tuple[Mapping[str, object], ...]:
    """选择在 as_of 已可见且区间有效的唯一记录。"""

    if not entity_fields or len(entity_fields) != len(set(entity_fields)):
        raise TransformGraphError("entity_fields 必须非空且唯一")
    cutoff = _temporal_value(as_of, "as_of")
    active: dict[tuple[object, ...], list[Mapping[str, object]]] = {}
    seen: set[tuple[object, ...]] = set()
    for record in records:
        required = {
            *entity_fields,
            effective_from_field,
            effective_to_field,
            available_time_field,
        }
        if not required <= set(record):
            raise TransformGraphError("IntervalJoin 记录缺少时态字段")
        entity = tuple(record[field] for field in entity_fields)
        start = _temporal_value(record[effective_from_field], effective_from_field)
        raw_end = record[effective_to_field]
        end = None if raw_end is None else _temporal_value(raw_end, effective_to_field)
        available = _temporal_value(record[available_time_field], available_time_field)
        if end is not None and start >= end:
            raise TransformGraphError("IntervalJoin 有效区间必须是非空半开区间")
        key = (*entity, start, end, available)
        if key in seen:
            raise TransformGraphError("IntervalJoin 记录经济键重复")
        seen.add(key)
        if available <= cutoff and start <= cutoff and (end is None or cutoff < end):
            active.setdefault(entity, []).append(record)
    selected: list[Mapping[str, object]] = []
    for entity in sorted(active, key=repr):
        candidates = active[entity]
        if len(candidates) != 1:
            raise TransformGraphError("IntervalJoin 在 as_of 存在重叠有效记录")
        selected.append(MappingProxyType(dict(candidates[0])))
    return tuple(selected)


__all__ = [
    "COMPILED_TRANSFORM_GRAPH_VERSION",
    "TRANSFORM_GRAPH_VERSION",
    "CompiledTransformGraph",
    "TransformGraph",
    "TransformGraphError",
    "TransformGraphNode",
    "TransformInput",
    "TransformNodeType",
    "TransformPortType",
    "compile_transform_graph",
    "select_as_of_records",
    "select_interval_records",
]
