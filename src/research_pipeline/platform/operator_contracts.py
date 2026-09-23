"""通用研究算子、策略和声明式算子图的跨层合同。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from types import MappingProxyType
from typing import Mapping

from .canonical import typed_canonical_hash
from .errors import MainlineError


OPERATOR_CONTRACT_VERSION = "research-operator-contract-v1"
STRATEGY_CONTRACT_VERSION = "research-strategy-contract-v1"
OPERATOR_GRAPH_RECIPE_VERSION = "research-operator-graph-recipe-v1"
OPERATOR_GRAPH_ADMISSION_VERSION = "research-operator-graph-admission-v1"

_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_EXECUTABLE_KEYS = {
    "callable",
    "code",
    "expression",
    "function",
    "module",
    "path",
    "python",
    "script",
    "sql",
}


def operator_dag_runtime_hash(record: Mapping[str, object]) -> str:
    """从 v2 终态记录生成 Result 与 Evidence 共用的 Runtime 身份。"""

    outputs = record.get("outputs")
    if not isinstance(outputs, Mapping):
        raise OperatorContractError("operator DAG run record 缺少 outputs")
    dag = record.get("dag")
    if isinstance(dag, Mapping):
        nodes = dag.get("nodes")
        edges = dag.get("edges")
        if isinstance(nodes, list) and isinstance(edges, list):
            node_ids = {
                str(item.get("node_id"))
                for item in nodes
                if isinstance(item, Mapping)
                and isinstance(item.get("node_id"), str)
            }
            source_nodes = {
                str(item.get("source_node"))
                for item in edges
                if isinstance(item, Mapping)
                and isinstance(item.get("source_node"), str)
            }
            terminal_nodes = node_ids - source_nodes
            if len(terminal_nodes) == 1:
                terminal_outputs = outputs.get(next(iter(terminal_nodes)))
                if isinstance(terminal_outputs, Mapping) and terminal_outputs:
                    return typed_canonical_hash({
                        "run_id": record.get("run_id"),
                        "dag_id": record.get("dag_id"),
                        "event_chain_head": record.get("event_chain_head"),
                        "final_outputs": dict(sorted(terminal_outputs.items())),
                    })
    return typed_canonical_hash({
        "run_id": record.get("run_id"),
        "dag_id": record.get("dag_id"),
        "event_chain_head": record.get("event_chain_head"),
        "outputs": dict(sorted(outputs.items())),
    })


class OperatorContractError(MainlineError):
    """算子、策略或算子图声明不符合封闭合同。"""

    error_code = "operator_contract_invalid"


class ParameterType(str, Enum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"
    STRING = "string"
    INTEGER_LIST = "integer_list"
    NUMBER_LIST = "number_list"
    STRING_LIST = "string_list"
    STRING_LIST_ALLOW_EMPTY = "string_list_allow_empty"
    JSON = "json"


class StrategyRole(str, Enum):
    SCHEDULE = "schedule"
    UNIVERSE = "universe"
    RANKING = "ranking"
    SELECTION = "selection"
    WEIGHTING = "weighting"
    TRADABILITY = "tradability"


def _require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERN.fullmatch(value):
        raise OperatorContractError(f"{field} 必须是安全稳定 ID")
    return value


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or not _HASH_PATTERN.fullmatch(value):
        raise OperatorContractError(f"{field} 必须是 sha256 小写摘要")
    return value


def _require_string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise OperatorContractError(f"{field} 必须是字符串")
    return value


def _require_exact(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    if any(not isinstance(key, str) for key in payload):
        raise OperatorContractError(f"{field} 的字段名必须是字符串")
    if set(payload) != expected:
        raise OperatorContractError(
            f"{field} schema 不匹配；缺失={sorted(expected - set(payload))}，未知={sorted(set(payload) - expected)}"
        )


def _freeze_json(value: object, field: str) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise OperatorContractError(f"{field} 的 mapping key 必须是字符串")
        for key in value:
            if key.lower() in _FORBIDDEN_EXECUTABLE_KEYS:
                raise OperatorContractError(f"{field}.{key} 不允许可执行内容或路径")
        return MappingProxyType(
            {key: _freeze_json(item, f"{field}.{key}") for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, f"{field}[]") for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise OperatorContractError(f"{field} 不支持类型: {type(value).__name__}")


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain_json(item) for item in value]
    return value


def _require_sorted_unique(values: tuple[str, ...], field: str) -> None:
    if values != tuple(sorted(values)) or len(values) != len(set(values)):
        raise OperatorContractError(f"{field} 必须唯一并规范排序")
    for value in values:
        _require_id(value, field)


@dataclass(frozen=True)
class ParameterSpec:
    name: str
    value_type: ParameterType
    required: bool = True
    allowed_values: tuple[object, ...] = ()

    def __post_init__(self) -> None:
        _require_id(self.name, "parameter.name")
        if type(self.required) is not bool:
            raise OperatorContractError("parameter.required 必须是布尔值")
        if self.allowed_values:
            normalized = tuple(self._validate_scalar(item, "allowed_values") for item in self.allowed_values)
            if len(normalized) != len(set(normalized)):
                raise OperatorContractError("allowed_values 不得重复")

    def _validate_scalar(self, value: object, field: str) -> object:
        expected = {
            ParameterType.BOOLEAN: bool,
            ParameterType.INTEGER: int,
            ParameterType.NUMBER: (int, float),
            ParameterType.STRING: str,
            ParameterType.INTEGER_LIST: int,
            ParameterType.NUMBER_LIST: (int, float),
            ParameterType.STRING_LIST: str,
            ParameterType.STRING_LIST_ALLOW_EMPTY: str,
            ParameterType.JSON: object,
        }[self.value_type]
        if self.value_type is ParameterType.BOOLEAN:
            valid = type(value) is bool
        elif self.value_type in {ParameterType.INTEGER, ParameterType.INTEGER_LIST}:
            valid = type(value) is int
        elif self.value_type in {ParameterType.NUMBER, ParameterType.NUMBER_LIST}:
            valid = type(value) in {int, float}
        elif self.value_type is ParameterType.JSON:
            valid = isinstance(value, (Mapping, list, tuple, str, int, float, bool)) or value is None
        else:
            valid = isinstance(value, expected)
        if not valid:
            raise OperatorContractError(f"parameter {self.name} 的 {field} 类型无效")
        return value

    def validate(self, value: object) -> object:
        if self.value_type in {
            ParameterType.INTEGER_LIST,
            ParameterType.NUMBER_LIST,
            ParameterType.STRING_LIST,
            ParameterType.STRING_LIST_ALLOW_EMPTY,
        }:
            if not isinstance(value, (list, tuple)) or (
                not value and self.value_type is not ParameterType.STRING_LIST_ALLOW_EMPTY
            ):
                requirement = (
                    "列表"
                    if self.value_type is ParameterType.STRING_LIST_ALLOW_EMPTY
                    else "非空列表"
                )
                raise OperatorContractError(f"parameter {self.name} 必须是{requirement}")
            normalized = tuple(self._validate_scalar(item, self.name) for item in value)
            if len(normalized) != len(set(normalized)):
                raise OperatorContractError(f"parameter {self.name} 列表值不得重复")
            if self.allowed_values and not set(normalized).issubset(set(self.allowed_values)):
                raise OperatorContractError(f"parameter {self.name} 超出 allowed_values")
            return normalized
        normalized = self._validate_scalar(value, self.name)
        if self.allowed_values and normalized not in self.allowed_values:
            raise OperatorContractError(f"parameter {self.name} 超出 allowed_values")
        return normalized

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value_type": self.value_type.value,
            "required": self.required,
            "allowed_values": list(self.allowed_values),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ParameterSpec":
        _require_exact(payload, {"name", "value_type", "required", "allowed_values"}, "parameter")
        if not isinstance(payload["allowed_values"], (list, tuple)):
            raise OperatorContractError("parameter.allowed_values 必须是列表")
        try:
            value_type = ParameterType(_require_string(payload["value_type"], "parameter.value_type"))
        except ValueError as exc:
            raise OperatorContractError("parameter.value_type 不受支持") from exc
        return cls(
            name=_require_string(payload["name"], "parameter.name"),
            value_type=value_type,
            required=payload["required"],
            allowed_values=tuple(payload["allowed_values"]),
        )


@dataclass(frozen=True)
class PortSpec:
    port: str
    artifact_type: str

    def __post_init__(self) -> None:
        _require_id(self.port, "port")
        _require_id(self.artifact_type, "artifact_type")

    def to_dict(self) -> dict[str, str]:
        return {"port": self.port, "artifact_type": self.artifact_type}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PortSpec":
        _require_exact(payload, {"port", "artifact_type"}, "port")
        return cls(
            _require_string(payload["port"], "port.port"),
            _require_string(payload["artifact_type"], "port.artifact_type"),
        )


def _validate_parameter_specs(values: tuple[ParameterSpec, ...], field: str) -> None:
    names = tuple(item.name for item in values)
    if names != tuple(sorted(names)) or len(names) != len(set(names)):
        raise OperatorContractError(f"{field} 必须按名称唯一排序")


def _validate_ports(values: tuple[PortSpec, ...], field: str) -> None:
    names = tuple(item.port for item in values)
    if names != tuple(sorted(names)) or len(names) != len(set(names)):
        raise OperatorContractError(f"{field} 必须按 port 唯一排序")


@dataclass(frozen=True)
class StrategySpec:
    role: StrategyRole
    strategy_id: str
    strategy_version: str
    parameters: tuple[ParameterSpec, ...]
    code_hash: str
    pit_capabilities: tuple[str, ...]
    spec_hash: str
    contract_version: str = STRATEGY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_id(self.strategy_id, "strategy_id")
        _require_id(self.strategy_version, "strategy_version")
        _require_hash(self.code_hash, "strategy.code_hash")
        _validate_parameter_specs(self.parameters, "strategy.parameters")
        _require_sorted_unique(self.pit_capabilities, "strategy.pit_capabilities")
        if self.contract_version != STRATEGY_CONTRACT_VERSION:
            raise OperatorContractError("strategy contract version 不受支持")
        if self.spec_hash != typed_canonical_hash(self.payload()):
            raise OperatorContractError("strategy spec hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "role": self.role.value,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "parameters": [item.to_dict() for item in self.parameters],
            "code_hash": self.code_hash,
            "pit_capabilities": list(self.pit_capabilities),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "spec_hash": self.spec_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StrategySpec":
        expected = {
            "role", "strategy_id", "strategy_version", "parameters", "code_hash",
            "pit_capabilities", "contract_version", "spec_hash",
        }
        _require_exact(payload, expected, "strategy")
        if not isinstance(payload["parameters"], (list, tuple)) or any(
            not isinstance(item, Mapping) for item in payload["parameters"]
        ):
            raise OperatorContractError("strategy.parameters 必须是映射列表")
        if not isinstance(payload["pit_capabilities"], (list, tuple)):
            raise OperatorContractError("strategy.pit_capabilities 必须是列表")
        try:
            role = StrategyRole(_require_string(payload["role"], "strategy.role"))
        except ValueError as exc:
            raise OperatorContractError("strategy.role 不受支持") from exc
        return cls(
            role=role,
            strategy_id=_require_string(payload["strategy_id"], "strategy.strategy_id"),
            strategy_version=_require_string(payload["strategy_version"], "strategy.strategy_version"),
            parameters=tuple(ParameterSpec.from_dict(item) for item in payload["parameters"]),
            code_hash=_require_string(payload["code_hash"], "strategy.code_hash"),
            pit_capabilities=tuple(
                _require_string(item, "strategy.pit_capabilities[]")
                for item in payload["pit_capabilities"]
            ),
            spec_hash=_require_string(payload["spec_hash"], "strategy.spec_hash"),
            contract_version=_require_string(payload["contract_version"], "strategy.contract_version"),
        )

    @classmethod
    def build(
        cls,
        *,
        role: StrategyRole,
        strategy_id: str,
        strategy_version: str,
        parameters: tuple[ParameterSpec, ...],
        code_hash: str,
        pit_capabilities: tuple[str, ...] = (),
    ) -> "StrategySpec":
        ordered_parameters = tuple(sorted(parameters, key=lambda item: item.name))
        ordered_capabilities = tuple(sorted(pit_capabilities))
        payload = {
            "role": role.value,
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "parameters": [item.to_dict() for item in ordered_parameters],
            "code_hash": code_hash,
            "pit_capabilities": list(ordered_capabilities),
            "contract_version": STRATEGY_CONTRACT_VERSION,
        }
        return cls(
            role,
            strategy_id,
            strategy_version,
            ordered_parameters,
            code_hash,
            ordered_capabilities,
            typed_canonical_hash(payload),
        )


@dataclass(frozen=True)
class OperatorSpec:
    operator_id: str
    operator_version: str
    input_ports: tuple[PortSpec, ...]
    output_ports: tuple[PortSpec, ...]
    parameters: tuple[ParameterSpec, ...]
    strategy_roles: tuple[StrategyRole, ...]
    resource_profile: Mapping[str, int]
    determinism_mode: str
    seed_policy: str
    code_hash: str
    pit_capabilities: tuple[str, ...]
    spec_hash: str
    contract_version: str = OPERATOR_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_id(self.operator_id, "operator_id")
        _require_id(self.operator_version, "operator_version")
        _validate_ports(self.input_ports, "operator.input_ports")
        _validate_ports(self.output_ports, "operator.output_ports")
        if not self.output_ports:
            raise OperatorContractError("operator 必须声明输出端口")
        _validate_parameter_specs(self.parameters, "operator.parameters")
        role_values = tuple(item.value for item in self.strategy_roles)
        if role_values != tuple(sorted(role_values)) or len(role_values) != len(set(role_values)):
            raise OperatorContractError("operator.strategy_roles 必须唯一并规范排序")
        expected_resources = {"memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds"}
        if set(self.resource_profile) != expected_resources or any(
            type(value) is not int or value <= 0 for value in self.resource_profile.values()
        ):
            raise OperatorContractError("operator.resource_profile 必须包含四个正整数")
        object.__setattr__(self, "resource_profile", MappingProxyType(dict(self.resource_profile)))
        if self.determinism_mode not in {"deterministic", "seeded"}:
            raise OperatorContractError("operator.determinism_mode 不受支持")
        if self.seed_policy not in {"none", "fixed_root", "derived_partition"}:
            raise OperatorContractError("operator.seed_policy 不受支持")
        if (self.determinism_mode == "deterministic") != (self.seed_policy == "none"):
            raise OperatorContractError("operator determinism 与 seed policy 不匹配")
        _require_hash(self.code_hash, "operator.code_hash")
        _require_sorted_unique(self.pit_capabilities, "operator.pit_capabilities")
        if self.contract_version != OPERATOR_CONTRACT_VERSION:
            raise OperatorContractError("operator contract version 不受支持")
        if self.spec_hash != typed_canonical_hash(self.payload()):
            raise OperatorContractError("operator spec hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "operator_id": self.operator_id,
            "operator_version": self.operator_version,
            "input_ports": [item.to_dict() for item in self.input_ports],
            "output_ports": [item.to_dict() for item in self.output_ports],
            "parameters": [item.to_dict() for item in self.parameters],
            "strategy_roles": [item.value for item in self.strategy_roles],
            "resource_profile": dict(self.resource_profile),
            "determinism_mode": self.determinism_mode,
            "seed_policy": self.seed_policy,
            "code_hash": self.code_hash,
            "pit_capabilities": list(self.pit_capabilities),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "spec_hash": self.spec_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OperatorSpec":
        expected = {
            "operator_id", "operator_version", "input_ports", "output_ports", "parameters",
            "strategy_roles", "resource_profile", "determinism_mode", "seed_policy", "code_hash",
            "pit_capabilities", "contract_version", "spec_hash",
        }
        _require_exact(payload, expected, "operator")
        sequence_fields = ("input_ports", "output_ports", "parameters", "strategy_roles", "pit_capabilities")
        if any(not isinstance(payload[field], (list, tuple)) for field in sequence_fields):
            raise OperatorContractError("operator 列表字段类型无效")
        if any(not isinstance(item, Mapping) for field in ("input_ports", "output_ports", "parameters") for item in payload[field]):
            raise OperatorContractError("operator port/parameter 必须是映射列表")
        if not isinstance(payload["resource_profile"], Mapping):
            raise OperatorContractError("operator.resource_profile 必须是映射")
        try:
            roles = tuple(
                StrategyRole(_require_string(item, "operator.strategy_roles[]"))
                for item in payload["strategy_roles"]
            )
        except ValueError as exc:
            raise OperatorContractError("operator.strategy_roles 包含不受支持角色") from exc
        return cls(
            operator_id=_require_string(payload["operator_id"], "operator.operator_id"),
            operator_version=_require_string(payload["operator_version"], "operator.operator_version"),
            input_ports=tuple(PortSpec.from_dict(item) for item in payload["input_ports"]),
            output_ports=tuple(PortSpec.from_dict(item) for item in payload["output_ports"]),
            parameters=tuple(ParameterSpec.from_dict(item) for item in payload["parameters"]),
            strategy_roles=roles,
            resource_profile=dict(payload["resource_profile"]),
            determinism_mode=_require_string(payload["determinism_mode"], "operator.determinism_mode"),
            seed_policy=_require_string(payload["seed_policy"], "operator.seed_policy"),
            code_hash=_require_string(payload["code_hash"], "operator.code_hash"),
            pit_capabilities=tuple(
                _require_string(item, "operator.pit_capabilities[]")
                for item in payload["pit_capabilities"]
            ),
            spec_hash=_require_string(payload["spec_hash"], "operator.spec_hash"),
            contract_version=_require_string(payload["contract_version"], "operator.contract_version"),
        )

    @classmethod
    def build(
        cls,
        *,
        operator_id: str,
        operator_version: str,
        input_ports: tuple[PortSpec, ...],
        output_ports: tuple[PortSpec, ...],
        parameters: tuple[ParameterSpec, ...],
        strategy_roles: tuple[StrategyRole, ...],
        resource_profile: Mapping[str, int],
        determinism_mode: str,
        seed_policy: str,
        code_hash: str,
        pit_capabilities: tuple[str, ...] = (),
    ) -> "OperatorSpec":
        values = {
            "operator_id": operator_id,
            "operator_version": operator_version,
            "input_ports": tuple(sorted(input_ports, key=lambda item: item.port)),
            "output_ports": tuple(sorted(output_ports, key=lambda item: item.port)),
            "parameters": tuple(sorted(parameters, key=lambda item: item.name)),
            "strategy_roles": tuple(sorted(strategy_roles, key=lambda item: item.value)),
            "resource_profile": dict(resource_profile),
            "determinism_mode": determinism_mode,
            "seed_policy": seed_policy,
            "code_hash": code_hash,
            "pit_capabilities": tuple(sorted(pit_capabilities)),
        }
        payload = {
            "operator_id": operator_id,
            "operator_version": operator_version,
            "input_ports": [item.to_dict() for item in values["input_ports"]],
            "output_ports": [item.to_dict() for item in values["output_ports"]],
            "parameters": [item.to_dict() for item in values["parameters"]],
            "strategy_roles": [item.value for item in values["strategy_roles"]],
            "resource_profile": dict(resource_profile),
            "determinism_mode": determinism_mode,
            "seed_policy": seed_policy,
            "code_hash": code_hash,
            "pit_capabilities": list(values["pit_capabilities"]),
            "contract_version": OPERATOR_CONTRACT_VERSION,
        }
        return cls(**values, spec_hash=typed_canonical_hash(payload))


@dataclass(frozen=True)
class StrategySelection:
    role: StrategyRole
    strategy_id: str
    strategy_version: str
    parameters: Mapping[str, object]

    def __post_init__(self) -> None:
        _require_id(self.strategy_id, "strategy_selection.strategy_id")
        _require_id(self.strategy_version, "strategy_selection.strategy_version")
        object.__setattr__(self, "parameters", _freeze_json(self.parameters, "strategy.parameters"))

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role.value,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "parameters": _plain_json(self.parameters),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StrategySelection":
        _require_exact(payload, {"role", "strategy_id", "strategy_version", "parameters"}, "strategy_selection")
        if not isinstance(payload["parameters"], Mapping):
            raise OperatorContractError("strategy_selection.parameters 必须是映射")
        try:
            role = StrategyRole(_require_string(payload["role"], "strategy_selection.role"))
        except ValueError as exc:
            raise OperatorContractError("strategy_selection.role 不受支持") from exc
        return cls(
            role,
            _require_string(payload["strategy_id"], "strategy_selection.strategy_id"),
            _require_string(payload["strategy_version"], "strategy_selection.strategy_version"),
            payload["parameters"],
        )


@dataclass(frozen=True)
class InputBinding:
    input_port: str
    source_node_id: str
    source_output_port: str

    def __post_init__(self) -> None:
        _require_id(self.input_port, "input_binding.input_port")
        _require_id(self.source_node_id, "input_binding.source_node_id")
        _require_id(self.source_output_port, "input_binding.source_output_port")

    def to_dict(self) -> dict[str, str]:
        return {
            "input_port": self.input_port,
            "source_node_id": self.source_node_id,
            "source_output_port": self.source_output_port,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "InputBinding":
        _require_exact(payload, {"input_port", "source_node_id", "source_output_port"}, "input_binding")
        return cls(
            _require_string(payload["input_port"], "input_binding.input_port"),
            _require_string(payload["source_node_id"], "input_binding.source_node_id"),
            _require_string(payload["source_output_port"], "input_binding.source_output_port"),
        )


@dataclass(frozen=True)
class OperatorNodeRecipe:
    node_id: str
    operator_id: str
    operator_version: str
    inputs: tuple[InputBinding, ...]
    parameters: Mapping[str, object]
    strategies: tuple[StrategySelection, ...]

    def __post_init__(self) -> None:
        _require_id(self.node_id, "operator_node.node_id")
        _require_id(self.operator_id, "operator_node.operator_id")
        _require_id(self.operator_version, "operator_node.operator_version")
        input_ports = tuple(item.input_port for item in self.inputs)
        if input_ports != tuple(sorted(input_ports)) or len(input_ports) != len(set(input_ports)):
            raise OperatorContractError("operator_node.inputs 必须按 input_port 唯一排序")
        object.__setattr__(self, "parameters", _freeze_json(self.parameters, "operator.parameters"))
        roles = tuple(item.role.value for item in self.strategies)
        if roles != tuple(sorted(roles)) or len(roles) != len(set(roles)):
            raise OperatorContractError("operator_node.strategies 必须按 role 唯一排序")

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "operator_id": self.operator_id,
            "operator_version": self.operator_version,
            "inputs": [item.to_dict() for item in self.inputs],
            "parameters": _plain_json(self.parameters),
            "strategies": [item.to_dict() for item in self.strategies],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OperatorNodeRecipe":
        _require_exact(payload, {"node_id", "operator_id", "operator_version", "inputs", "parameters", "strategies"}, "operator_node")
        if not isinstance(payload["inputs"], (list, tuple)) or not isinstance(payload["strategies"], (list, tuple)) or not isinstance(payload["parameters"], Mapping):
            raise OperatorContractError("operator_node 子字段类型无效")
        if any(not isinstance(item, Mapping) for item in (*payload["inputs"], *payload["strategies"])):
            raise OperatorContractError("operator_node inputs/strategies 必须是映射列表")
        inputs = tuple(sorted((InputBinding.from_dict(item) for item in payload["inputs"]), key=lambda item: item.input_port))
        strategies = tuple(sorted((StrategySelection.from_dict(item) for item in payload["strategies"]), key=lambda item: item.role.value))
        return cls(
            _require_string(payload["node_id"], "operator_node.node_id"),
            _require_string(payload["operator_id"], "operator_node.operator_id"),
            _require_string(payload["operator_version"], "operator_node.operator_version"),
            inputs,
            payload["parameters"],
            strategies,
        )


@dataclass(frozen=True)
class OperatorGraphRecipe:
    graph_id: str
    nodes: tuple[OperatorNodeRecipe, ...]
    recipe_hash: str
    contract_version: str = OPERATOR_GRAPH_RECIPE_VERSION

    def __post_init__(self) -> None:
        _require_id(self.graph_id, "operator_graph.graph_id")
        node_ids = tuple(item.node_id for item in self.nodes)
        if not node_ids or node_ids != tuple(sorted(node_ids)) or len(node_ids) != len(set(node_ids)):
            raise OperatorContractError("operator_graph.nodes 必须非空并按 node_id 唯一排序")
        if self.contract_version != OPERATOR_GRAPH_RECIPE_VERSION:
            raise OperatorContractError("operator graph recipe version 不受支持")
        if self.recipe_hash != typed_canonical_hash(self.payload()):
            raise OperatorContractError("operator graph recipe hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "graph_id": self.graph_id,
            "nodes": [item.to_dict() for item in self.nodes],
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "recipe_hash": self.recipe_hash}

    @classmethod
    def build(cls, *, graph_id: str, nodes: tuple[OperatorNodeRecipe, ...]) -> "OperatorGraphRecipe":
        ordered = tuple(sorted(nodes, key=lambda item: item.node_id))
        payload = {
            "graph_id": graph_id,
            "nodes": [item.to_dict() for item in ordered],
            "contract_version": OPERATOR_GRAPH_RECIPE_VERSION,
        }
        return cls(graph_id, ordered, typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OperatorGraphRecipe":
        _require_exact(payload, {"graph_id", "nodes", "contract_version"}, "operator_graph")
        if payload["contract_version"] != OPERATOR_GRAPH_RECIPE_VERSION:
            raise OperatorContractError("operator graph recipe version 不受支持")
        if not isinstance(payload["nodes"], (list, tuple)) or any(not isinstance(item, Mapping) for item in payload["nodes"]):
            raise OperatorContractError("operator_graph.nodes 必须是映射列表")
        return cls.build(
            graph_id=_require_string(payload["graph_id"], "operator_graph.graph_id"),
            nodes=tuple(OperatorNodeRecipe.from_dict(item) for item in payload["nodes"]),
        )


@dataclass(frozen=True)
class AdmittedOperatorGraph:
    recipe: OperatorGraphRecipe
    topological_order: tuple[str, ...]
    operator_spec_hashes: Mapping[str, str]
    strategy_spec_hashes: Mapping[str, str]
    registry_hash: str
    admission_hash: str
    contract_version: str = OPERATOR_GRAPH_ADMISSION_VERSION

    def __post_init__(self) -> None:
        if set(self.topological_order) != {item.node_id for item in self.recipe.nodes}:
            raise OperatorContractError("operator admission 拓扑节点不闭合")
        for field in ("operator_spec_hashes", "strategy_spec_hashes"):
            value = getattr(self, field)
            if any(not isinstance(key, str) or not _HASH_PATTERN.fullmatch(item) for key, item in value.items()):
                raise OperatorContractError(f"{field} 无效")
            object.__setattr__(self, field, MappingProxyType(dict(value)))
        _require_hash(self.registry_hash, "registry_hash")
        if self.contract_version != OPERATOR_GRAPH_ADMISSION_VERSION:
            raise OperatorContractError("operator admission version 不受支持")
        if self.admission_hash != typed_canonical_hash(self.payload()):
            raise OperatorContractError("operator admission hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "recipe_hash": self.recipe.recipe_hash,
            "topological_order": list(self.topological_order),
            "operator_spec_hashes": dict(self.operator_spec_hashes),
            "strategy_spec_hashes": dict(self.strategy_spec_hashes),
            "registry_hash": self.registry_hash,
            "contract_version": self.contract_version,
        }


def validate_parameters(
    values: Mapping[str, object],
    specs: tuple[ParameterSpec, ...],
    field: str,
) -> dict[str, object]:
    """按唯一参数 schema 校验声明，拒绝未知字段和自由执行载荷。"""
    frozen = _freeze_json(values, field)
    if not isinstance(frozen, Mapping):
        raise OperatorContractError(f"{field} 必须是映射")
    by_name = {item.name: item for item in specs}
    unknown = set(frozen) - set(by_name)
    missing = {item.name for item in specs if item.required} - set(frozen)
    if unknown or missing:
        raise OperatorContractError(f"{field} 参数不匹配；缺失={sorted(missing)}，未知={sorted(unknown)}")
    return {key: by_name[key].validate(value) for key, value in frozen.items()}


__all__ = [
    "OPERATOR_CONTRACT_VERSION",
    "OPERATOR_GRAPH_ADMISSION_VERSION",
    "OPERATOR_GRAPH_RECIPE_VERSION",
    "STRATEGY_CONTRACT_VERSION",
    "AdmittedOperatorGraph",
    "InputBinding",
    "OperatorContractError",
    "OperatorGraphRecipe",
    "OperatorNodeRecipe",
    "OperatorSpec",
    "ParameterSpec",
    "ParameterType",
    "PortSpec",
    "StrategyRole",
    "StrategySelection",
    "StrategySpec",
    "operator_dag_runtime_hash",
    "validate_parameters",
]
