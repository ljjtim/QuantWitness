"""可审计、按时点求值的有限 Universe IR。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash

from .errors import QueryIRInvalidError


UNIVERSE_IR_VERSION = "research-universe-ir-v1"
UNIVERSE_PLAN_VERSION = "compiled-research-universe-v1"


class UniverseIRError(QueryIRInvalidError):
    """Universe 声明、时态数据或求值结果无效。"""


class UniverseNodeType(str, Enum):
    BASE = "BaseUniverse"
    FILTER = "Filter"
    UNION = "Union"
    INTERSECTION = "Intersection"
    DIFFERENCE = "Difference"
    AS_OF = "AsOf"


class UniverseFilterOperator(str, Enum):
    EQ = "eq"
    IN = "in"
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"


class UniverseReasonCode(str, Enum):
    BASE_INCLUDED = "base_included"
    NOT_AVAILABLE_AT_AS_OF = "not_available_at_as_of"
    OUTSIDE_EFFECTIVE_INTERVAL = "outside_effective_interval"
    FILTER_INCLUDED = "filter_included"
    FILTER_EXCLUDED = "filter_excluded"
    UNION_INCLUDED = "union_included"
    UNION_MISSING = "union_missing"
    INTERSECTION_INCLUDED = "intersection_included"
    INTERSECTION_MISSING = "intersection_missing"
    DIFFERENCE_INCLUDED = "difference_included"
    DIFFERENCE_EXCLUDED = "difference_excluded"


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise UniverseIRError(f"{field_name} 必须是非空字符串")
    return value


def _instant(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise UniverseIRError(f"{field_name} 必须是带时区 ISO 时间")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise UniverseIRError(f"{field_name} 必须是带时区 ISO 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UniverseIRError(f"{field_name} 必须带时区")
    return parsed


def _plain_mapping(value: Mapping[str, object], field_name: str) -> MappingProxyType:
    if any(not isinstance(key, str) for key in value):
        raise UniverseIRError(f"{field_name} 的 key 必须是字符串")
    result: dict[str, object] = {}
    for key, item in value.items():
        if item is None or isinstance(item, (bool, int, float, str)):
            result[key] = item
        elif isinstance(item, (list, tuple)) and all(
            value is None or isinstance(value, (bool, int, float, str)) for value in item
        ):
            result[key] = tuple(item)
        else:
            raise UniverseIRError(f"{field_name}.{key} 含不支持类型")
    return MappingProxyType(result)


def _to_plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _to_plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_to_plain(item) for item in value]
    return value


@dataclass(frozen=True)
class UniverseFieldContract:
    field_id: str
    observation_role: str

    def __post_init__(self) -> None:
        _text(self.field_id, "Universe field_id")
        if self.observation_role not in {"known_at_or_before_as_of", "future_outcome"}:
            raise UniverseIRError("Universe observation_role 不受支持")

    def to_dict(self) -> dict[str, str]:
        return {"field_id": self.field_id, "observation_role": self.observation_role}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "UniverseFieldContract":
        if set(payload) != {"field_id", "observation_role"}:
            raise UniverseIRError("UniverseFieldContract schema 不匹配")
        return cls(str(payload["field_id"]), str(payload["observation_role"]))


@dataclass(frozen=True)
class UniverseNode:
    node_id: str
    node_type: UniverseNodeType
    inputs: tuple[str, ...] = ()
    parameters: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _text(self.node_id, "Universe node_id")
        if not isinstance(self.node_type, UniverseNodeType):
            raise UniverseIRError("Universe node_type 不受支持")
        if any(not isinstance(item, str) or not item.strip() for item in self.inputs):
            raise UniverseIRError("Universe inputs 必须是非空节点 ID")
        if len(self.inputs) != len(set(self.inputs)):
            raise UniverseIRError("Universe inputs 不能重复")
        if not isinstance(self.parameters, Mapping):
            raise UniverseIRError("Universe parameters 必须是映射")
        object.__setattr__(
            self, "parameters", _plain_mapping(self.parameters, "Universe parameters")
        )
        _validate_universe_node(self)

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "inputs": list(self.inputs),
            "parameters": _to_plain(self.parameters),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "UniverseNode":
        if set(payload) != {"node_id", "node_type", "inputs", "parameters"}:
            raise UniverseIRError("UniverseNode schema 不匹配")
        inputs = payload["inputs"]
        parameters = payload["parameters"]
        if not isinstance(inputs, list) or not isinstance(parameters, Mapping):
            raise UniverseIRError("UniverseNode inputs/parameters 类型无效")
        return cls(
            str(payload["node_id"]),
            UniverseNodeType(str(payload["node_type"])),
            tuple(str(item) for item in inputs),
            parameters,
        )


def _validate_universe_node(node: UniverseNode) -> None:
    parameters = node.parameters
    if node.node_type == UniverseNodeType.BASE:
        if node.inputs or set(parameters) != {"source_id"}:
            raise UniverseIRError("BaseUniverse 只能声明 source_id 且不能有输入")
        _text(parameters["source_id"], "BaseUniverse.source_id")
    elif node.node_type == UniverseNodeType.FILTER:
        if len(node.inputs) != 1 or set(parameters) != {"field_id", "operator", "values"}:
            raise UniverseIRError("Filter 必须有一个输入和固定参数")
        _text(parameters["field_id"], "Filter.field_id")
        try:
            operator = UniverseFilterOperator(str(parameters["operator"]))
        except ValueError as exc:
            raise UniverseIRError("Filter operator 不受支持") from exc
        values = parameters["values"]
        if not isinstance(values, tuple):
            raise UniverseIRError("Filter values 必须是有限列表")
        if operator == UniverseFilterOperator.EQ and len(values) != 1:
            raise UniverseIRError("Filter eq 必须有一个值")
        if operator == UniverseFilterOperator.IN and not values:
            raise UniverseIRError("Filter in 不能为空")
        if operator in {UniverseFilterOperator.IS_TRUE, UniverseFilterOperator.IS_FALSE} and values:
            raise UniverseIRError("Filter boolean operator 不接受 values")
    elif node.node_type in {
        UniverseNodeType.UNION,
        UniverseNodeType.INTERSECTION,
        UniverseNodeType.DIFFERENCE,
    }:
        if len(node.inputs) != 2 or parameters:
            raise UniverseIRError(f"{node.node_type.value} 必须恰有两个输入且无参数")
    elif node.node_type == UniverseNodeType.AS_OF:
        if len(node.inputs) != 1 or set(parameters) != {"at"}:
            raise UniverseIRError("AsOf 必须有一个输入和 at")
        _instant(parameters["at"], "AsOf.at")


@dataclass(frozen=True)
class UniverseIR:
    universe_id: str
    as_of: str
    nodes: tuple[UniverseNode, ...]
    output_node_id: str
    field_contracts: tuple[UniverseFieldContract, ...] = ()
    contract_version: str = UNIVERSE_IR_VERSION

    def __post_init__(self) -> None:
        _text(self.universe_id, "universe_id")
        _instant(self.as_of, "Universe as_of")
        if self.contract_version != UNIVERSE_IR_VERSION:
            raise UniverseIRError("UniverseIR contract_version 不受支持")
        if not self.nodes:
            raise UniverseIRError("UniverseIR nodes 不能为空")
        node_ids = [item.node_id for item in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise UniverseIRError("UniverseIR node_id 不能重复")
        if self.output_node_id not in set(node_ids):
            raise UniverseIRError("UniverseIR output_node_id 引用未知节点")
        field_ids = [item.field_id for item in self.field_contracts]
        if len(field_ids) != len(set(field_ids)):
            raise UniverseIRError("Universe field contract 不能重复")

    def to_dict(self) -> dict[str, object]:
        return {
            "universe_id": self.universe_id,
            "as_of": self.as_of,
            "nodes": [item.to_dict() for item in self.nodes],
            "output_node_id": self.output_node_id,
            "field_contracts": [item.to_dict() for item in self.field_contracts],
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "UniverseIR":
        expected = {
            "universe_id", "as_of", "nodes", "output_node_id", "field_contracts",
            "contract_version",
        }
        if set(payload) != expected:
            raise UniverseIRError("UniverseIR schema 不匹配")
        nodes = payload["nodes"]
        fields = payload["field_contracts"]
        if (
            not isinstance(nodes, list)
            or not isinstance(fields, list)
            or any(not isinstance(item, Mapping) for item in (*nodes, *fields))
        ):
            raise UniverseIRError("UniverseIR nodes/field_contracts 必须是对象列表")
        return cls(
            str(payload["universe_id"]),
            str(payload["as_of"]),
            tuple(UniverseNode.from_dict(item) for item in nodes),
            str(payload["output_node_id"]),
            tuple(UniverseFieldContract.from_dict(item) for item in fields),
            str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class CompiledUniversePlan:
    universe: UniverseIR
    topological_order: tuple[str, ...]
    plan_hash: str
    contract_version: str = UNIVERSE_PLAN_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != UNIVERSE_PLAN_VERSION:
            raise UniverseIRError("CompiledUniversePlan 版本不受支持")
        if self.plan_hash != typed_canonical_hash(self.payload()):
            raise UniverseIRError("Universe plan hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "universe": self.universe.to_dict(),
            "topological_order": list(self.topological_order),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "plan_hash": self.plan_hash}


def compile_universe_ir(universe: UniverseIR) -> CompiledUniversePlan:
    node_by_id = {item.node_id: item for item in universe.nodes}
    dependencies = {item.node_id: set(item.inputs) for item in universe.nodes}
    for node_id, inputs in dependencies.items():
        if node_id in inputs:
            raise UniverseIRError("UniverseIR 不允许自环")
        unknown = inputs - set(node_by_id)
        if unknown:
            raise UniverseIRError(f"UniverseIR 引用未知节点: {sorted(unknown)}")
    ready = sorted(key for key, value in dependencies.items() if not value)
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for node_id in sorted(dependencies):
            if current in dependencies[node_id]:
                dependencies[node_id].remove(current)
                if not dependencies[node_id] and node_id not in order and node_id not in ready:
                    ready.append(node_id)
                    ready.sort()
    if len(order) != len(universe.nodes):
        raise UniverseIRError("UniverseIR 依赖存在环")
    fields = {item.field_id: item for item in universe.field_contracts}
    for node in universe.nodes:
        if node.node_type == UniverseNodeType.AS_OF and _instant(
            node.parameters["at"], "AsOf.at"
        ) != _instant(universe.as_of, "Universe as_of"):
            raise UniverseIRError("Universe AsOf 必须与计划 as_of 完全一致")
        if node.node_type != UniverseNodeType.FILTER:
            continue
        field_id = str(node.parameters["field_id"])
        contract = fields.get(field_id)
        if contract is None:
            raise UniverseIRError(f"Universe Filter 引用未登记字段: {field_id}")
        if contract.observation_role == "future_outcome":
            raise UniverseIRError("Universe 不允许未来字段")
    plan = {
        "universe": universe.to_dict(),
        "topological_order": order,
        "contract_version": UNIVERSE_PLAN_VERSION,
    }
    return CompiledUniversePlan(universe, tuple(order), typed_canonical_hash(plan))


@dataclass(frozen=True)
class UniverseObservation:
    instrument: str
    available_at: str
    effective_from: str
    effective_to: str | None
    attributes: Mapping[str, object]
    attribute_available_at: Mapping[str, str]

    def __post_init__(self) -> None:
        _text(self.instrument, "Universe instrument")
        available = _instant(self.available_at, "available_at")
        effective_from = _instant(self.effective_from, "effective_from")
        effective_to = None if self.effective_to is None else _instant(self.effective_to, "effective_to")
        if effective_to is not None and effective_from >= effective_to:
            raise UniverseIRError("Universe 有效区间必须是非空半开区间")
        attributes = _plain_mapping(self.attributes, "Universe attributes")
        if set(attributes) != set(self.attribute_available_at):
            raise UniverseIRError("Universe attribute_available_at 必须覆盖全部属性")
        for field_id, value in self.attribute_available_at.items():
            _text(field_id, "attribute field_id")
            _instant(value, f"attribute_available_at.{field_id}")
        object.__setattr__(self, "attributes", attributes)
        object.__setattr__(self, "attribute_available_at", MappingProxyType(dict(self.attribute_available_at)))
        if available > effective_from and self.effective_to is not None and available >= effective_to:
            raise UniverseIRError("Universe 记录在失效后才可见")


@dataclass(frozen=True)
class UniverseSourceSnapshot:
    source_id: str
    snapshot_hash: str
    observations: tuple[UniverseObservation, ...]

    def __post_init__(self) -> None:
        _text(self.source_id, "Universe source_id")
        if len(self.snapshot_hash) != 64 or any(char not in "0123456789abcdef" for char in self.snapshot_hash):
            raise UniverseIRError("Universe snapshot_hash 必须是 sha256")
        if not self.observations:
            raise UniverseIRError("Universe source snapshot 不能为空")
        keys = [
            (item.instrument, item.available_at, item.effective_from, item.effective_to)
            for item in self.observations
        ]
        if len(keys) != len(set(keys)):
            raise UniverseIRError("Universe source snapshot 存在重复经济键")


@dataclass(frozen=True)
class UniverseDecision:
    instrument: str
    included: bool
    reason_codes: tuple[str, ...]
    source_lineage: tuple[str, ...]


@dataclass(frozen=True)
class UniverseResult:
    as_of: str
    decisions: tuple[UniverseDecision, ...]
    plan_hash: str
    result_hash: str

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(item.instrument for item in self.decisions if item.included)


@dataclass
class _MemberState:
    included: bool
    reasons: list[str]
    lineage: set[str]
    attributes: Mapping[str, object]
    attribute_available_at: Mapping[str, str]


def evaluate_universe(
    plan: CompiledUniversePlan,
    *,
    sources: Mapping[str, UniverseSourceSnapshot],
) -> UniverseResult:
    """只对调用方提供的不可变时态快照求值，不读取数据库。"""

    as_of = _instant(plan.universe.as_of, "Universe as_of")
    node_by_id = {item.node_id: item for item in plan.universe.nodes}
    values: dict[str, dict[str, _MemberState]] = {}
    for node_id in plan.topological_order:
        node = node_by_id[node_id]
        if node.node_type == UniverseNodeType.BASE:
            source_id = str(node.parameters["source_id"])
            source = sources.get(source_id)
            if source is None or source.source_id != source_id:
                raise UniverseIRError(f"Universe source 缺失: {source_id}")
            values[node_id] = _evaluate_base(source, as_of)
        elif node.node_type == UniverseNodeType.FILTER:
            values[node_id] = _evaluate_filter(node, values[node.inputs[0]], as_of)
        elif node.node_type == UniverseNodeType.AS_OF:
            node_as_of = _instant(node.parameters["at"], "AsOf.at")
            if node_as_of != as_of:
                raise UniverseIRError("Universe AsOf 必须与计划 as_of 完全一致")
            values[node_id] = _copy_states(values[node.inputs[0]])
        else:
            values[node_id] = _evaluate_set(node, values)
    final = values[plan.universe.output_node_id]
    decisions = tuple(
        UniverseDecision(
            instrument,
            state.included,
            tuple(state.reasons),
            tuple(sorted(state.lineage)),
        )
        for instrument, state in sorted(final.items())
    )
    if not any(item.included for item in decisions):
        raise UniverseIRError("Universe 最终结果为空")
    payload = {
        "as_of": plan.universe.as_of,
        "decisions": [
            {
                "instrument": item.instrument,
                "included": item.included,
                "reason_codes": list(item.reason_codes),
                "source_lineage": list(item.source_lineage),
            }
            for item in decisions
        ],
        "plan_hash": plan.plan_hash,
    }
    return UniverseResult(
        plan.universe.as_of,
        decisions,
        plan.plan_hash,
        typed_canonical_hash(payload),
    )


def _evaluate_base(
    source: UniverseSourceSnapshot,
    as_of: datetime,
) -> dict[str, _MemberState]:
    grouped: dict[str, list[UniverseObservation]] = {}
    for observation in source.observations:
        grouped.setdefault(observation.instrument, []).append(observation)
    result: dict[str, _MemberState] = {}
    for instrument, observations in grouped.items():
        available = [item for item in observations if _instant(item.available_at, "available_at") <= as_of]
        active = [
            item for item in available
            if _instant(item.effective_from, "effective_from") <= as_of
            and (item.effective_to is None or as_of < _instant(item.effective_to, "effective_to"))
        ]
        if len(active) > 1:
            raise UniverseIRError(f"Universe 时间冲突: {instrument}")
        if active:
            selected = active[0]
            result[instrument] = _MemberState(
                True,
                [UniverseReasonCode.BASE_INCLUDED.value],
                {source.snapshot_hash},
                selected.attributes,
                selected.attribute_available_at,
            )
        else:
            reason = (
                UniverseReasonCode.NOT_AVAILABLE_AT_AS_OF.value
                if not available
                else UniverseReasonCode.OUTSIDE_EFFECTIVE_INTERVAL.value
            )
            result[instrument] = _MemberState(False, [reason], {source.snapshot_hash}, {}, {})
    return result


def _evaluate_filter(
    node: UniverseNode,
    source: Mapping[str, _MemberState],
    as_of: datetime,
) -> dict[str, _MemberState]:
    result = _copy_states(source)
    field_id = str(node.parameters["field_id"])
    operator = UniverseFilterOperator(str(node.parameters["operator"]))
    values = tuple(node.parameters["values"])
    for state in result.values():
        if not state.included:
            continue
        available_at = state.attribute_available_at.get(field_id)
        if available_at is None or _instant(available_at, field_id) > as_of:
            raise UniverseIRError(f"Universe 字段在 as_of 不可见: {field_id}")
        value = state.attributes.get(field_id)
        passed = {
            UniverseFilterOperator.EQ: value == values[0] if values else False,
            UniverseFilterOperator.IN: value in values,
            UniverseFilterOperator.IS_TRUE: value is True,
            UniverseFilterOperator.IS_FALSE: value is False,
        }[operator]
        state.included = passed
        state.reasons.append(
            UniverseReasonCode.FILTER_INCLUDED.value
            if passed else UniverseReasonCode.FILTER_EXCLUDED.value
        )
    return result


def _evaluate_set(
    node: UniverseNode,
    values: Mapping[str, dict[str, _MemberState]],
) -> dict[str, _MemberState]:
    left = values[node.inputs[0]]
    right = values[node.inputs[1]]
    instruments = sorted(set(left) | set(right))
    result: dict[str, _MemberState] = {}
    for instrument in instruments:
        left_state = left.get(instrument)
        right_state = right.get(instrument)
        left_in = bool(left_state and left_state.included)
        right_in = bool(right_state and right_state.included)
        if node.node_type == UniverseNodeType.UNION:
            included = left_in or right_in
            reason = (
                UniverseReasonCode.UNION_INCLUDED.value
                if included else UniverseReasonCode.UNION_MISSING.value
            )
        elif node.node_type == UniverseNodeType.INTERSECTION:
            included = left_in and right_in
            reason = (
                UniverseReasonCode.INTERSECTION_INCLUDED.value
                if included else UniverseReasonCode.INTERSECTION_MISSING.value
            )
        else:
            included = left_in and not right_in
            reason = (
                UniverseReasonCode.DIFFERENCE_INCLUDED.value
                if included else UniverseReasonCode.DIFFERENCE_EXCLUDED.value
            )
        existing_states = tuple(
            state for state in (left_state, right_state) if state is not None
        )
        if not existing_states:
            continue
        if included:
            contributing_states = tuple(
                state
                for state in existing_states
                if state.included
            )
        elif node.node_type == UniverseNodeType.DIFFERENCE and left_state is not None:
            contributing_states = (left_state,)
        else:
            contributing_states = existing_states
        attributes, attribute_available_at = _merge_member_payload(
            contributing_states,
            instrument=instrument,
        )
        reasons = list(left_state.reasons if left_state else ()) + list(right_state.reasons if right_state else ())
        lineage = set(left_state.lineage if left_state else ()) | set(right_state.lineage if right_state else ())
        result[instrument] = _MemberState(
            included,
            [*reasons, reason],
            lineage,
            attributes,
            attribute_available_at,
        )
    return result


def _merge_member_payload(
    states: tuple[_MemberState, ...],
    *,
    instrument: str,
) -> tuple[Mapping[str, object], Mapping[str, str]]:
    attributes: dict[str, object] = {}
    available_at: dict[str, str] = {}
    for state in states:
        for field_id, value in state.attributes.items():
            if field_id in attributes and (
                attributes[field_id] != value
                or available_at[field_id] != state.attribute_available_at[field_id]
            ):
                raise UniverseIRError(
                    f"Universe 集合属性冲突: {instrument}/{field_id}"
                )
            attributes[field_id] = value
            available_at[field_id] = state.attribute_available_at[field_id]
    return MappingProxyType(attributes), MappingProxyType(available_at)


def _copy_states(source: Mapping[str, _MemberState]) -> dict[str, _MemberState]:
    return {
        instrument: _MemberState(
            state.included,
            list(state.reasons),
            set(state.lineage),
            state.attributes,
            state.attribute_available_at,
        )
        for instrument, state in source.items()
    }


__all__ = [
    "UNIVERSE_IR_VERSION",
    "UNIVERSE_PLAN_VERSION",
    "CompiledUniversePlan",
    "UniverseDecision",
    "UniverseFieldContract",
    "UniverseFilterOperator",
    "UniverseIR",
    "UniverseIRError",
    "UniverseNode",
    "UniverseNodeType",
    "UniverseObservation",
    "UniverseReasonCode",
    "UniverseResult",
    "UniverseSourceSnapshot",
    "compile_universe_ir",
    "evaluate_universe",
]
