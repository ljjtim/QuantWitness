"""仓库内可信算子和策略的唯一准入注册表。"""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Mapping

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.operator_contracts import (
    OPERATOR_GRAPH_ADMISSION_VERSION,
    AdmittedOperatorGraph,
    OperatorContractError,
    OperatorGraphRecipe,
    OperatorSpec,
    PortSpec,
    StrategyRole,
    StrategySpec,
    validate_parameters,
)

from .errors import ExtensionError
from .governance import validate_framework_implementation_boundary


_IMPLEMENTATION_ID = re.compile(r"^[a-z][a-z0-9_.-]*$")
_OPERATOR_DEFINITION_VERSION = "research-operator-definition-v3"
_OPERATOR_MANIFEST_VERSION = "research-operator-definition-manifest-v2"
_IMPLEMENTATION_SCOPES = {"core", "project"}


@dataclass(frozen=True)
class OperatorImplementationRef:
    """仓库内静态实现引用；只由平台代码构造，ResearchPackage 不可提供。"""

    implementation_id: str
    code_fingerprint: str
    capability: str
    module_name: str
    symbol_name: str
    code_hash: str
    dependency_modules: tuple[str, ...] = ()
    implementation_scope: str = "core"

    def __post_init__(self) -> None:
        validate_framework_implementation_boundary(
            implementation_scope=self.implementation_scope,
            implementation_id=self.implementation_id,
            module_name=self.module_name,
            dependency_modules=self.dependency_modules,
        )
        if not _IMPLEMENTATION_ID.fullmatch(self.implementation_id):
            raise ExtensionError("OperatorDefinition implementation_id 无效")
        if not all(
            isinstance(value, str) and value
            for value in (
                self.code_fingerprint,
                self.capability,
                self.module_name,
                self.symbol_name,
            )
        ):
            raise ExtensionError("OperatorDefinition implementation ref 不完整")
        if len(self.code_hash) != 64 or any(character not in "0123456789abcdef" for character in self.code_hash):
            raise ExtensionError("OperatorDefinition code_hash 必须是 sha256 小写摘要")
        if len(set(self.dependency_modules)) != len(self.dependency_modules) or any(
            not isinstance(item, str) or not item for item in self.dependency_modules
        ):
            raise ExtensionError("OperatorDefinition dependency_modules 无效")
        if self.implementation_scope not in _IMPLEMENTATION_SCOPES:
            raise ExtensionError("OperatorDefinition implementation_scope 无效")

    def to_dict(self) -> dict[str, object]:
        return {
            "implementation_id": self.implementation_id,
            "code_fingerprint": self.code_fingerprint,
            "capability": self.capability,
            "module_name": self.module_name,
            "symbol_name": self.symbol_name,
            "code_hash": self.code_hash,
            "dependency_modules": list(self.dependency_modules),
            "implementation_scope": self.implementation_scope,
        }


@dataclass(frozen=True)
class OperatorRuntimeAdapterRef:
    """仓库内静态 Runtime 适配器引用；不属于 ResearchPackage 输入。"""

    adapter_id: str
    module_name: str
    symbol_name: str
    code_hash: str
    dependency_modules: tuple[str, ...] = ()
    contract_version: str = "research-operator-runtime-adapter-ref-v2"

    def __post_init__(self) -> None:
        if not _IMPLEMENTATION_ID.fullmatch(self.adapter_id):
            raise ExtensionError("OperatorDefinition runtime adapter_id 无效")
        if not self.module_name or not self.symbol_name:
            raise ExtensionError("OperatorDefinition runtime adapter ref 不完整")
        if len(self.code_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.code_hash
        ):
            raise ExtensionError("OperatorDefinition runtime adapter code_hash 必须是 sha256 小写摘要")
        if len(set(self.dependency_modules)) != len(self.dependency_modules) or any(
            not isinstance(item, str) or not item for item in self.dependency_modules
        ):
            raise ExtensionError("OperatorDefinition runtime adapter dependency_modules 无效")
        if self.contract_version != "research-operator-runtime-adapter-ref-v2":
            raise ExtensionError("OperatorDefinition runtime adapter 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "module_name": self.module_name,
            "symbol_name": self.symbol_name,
            "code_hash": self.code_hash,
            "dependency_modules": list(self.dependency_modules),
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class OperatorDefinition:
    """编译准入与 Runtime 执行共同消费的唯一算子定义。"""

    operator_spec: OperatorSpec
    implementation_ref: OperatorImplementationRef
    runtime_adapter_ref: OperatorRuntimeAdapterRef
    cache_profile_ref: str
    cache_compatibility_mode: str
    resource_hint_ref: str
    partition_keys: tuple[str, ...]
    definition_hash: str
    contract_version: str = _OPERATOR_DEFINITION_VERSION

    def __post_init__(self) -> None:
        if self.operator_spec.code_hash != self.implementation_ref.code_hash:
            raise ExtensionError(f"OperatorDefinition code hash 漂移: {self.name}")
        if not self.operator_spec.output_ports:
            raise ExtensionError(f"OperatorDefinition 缺少 output schema: {self.name}")
        if self.cache_profile_ref != "cache.semantic-pure.v1" or not self.resource_hint_ref:
            raise ExtensionError(f"OperatorDefinition profile ref 不完整: {self.name}")
        if self.cache_compatibility_mode not in {"byte_exact", "numerical"}:
            raise ExtensionError(f"OperatorDefinition cache compatibility mode 无效: {self.name}")
        if self.contract_version != _OPERATOR_DEFINITION_VERSION:
            raise ExtensionError(f"OperatorDefinition 版本不受支持: {self.name}")
        if len(set(self.partition_keys)) != len(self.partition_keys) or any(not item for item in self.partition_keys):
            raise ExtensionError(f"OperatorDefinition partition keys 无效: {self.name}")
        if self.definition_hash != typed_canonical_hash(self.payload()):
            raise ExtensionError(f"OperatorDefinition hash 不一致: {self.name}")

    @property
    def name(self) -> str:
        return self.operator_spec.operator_id

    @property
    def version(self) -> str:
        return self.operator_spec.operator_version

    @property
    def input_schema(self) -> tuple[PortSpec, ...]:
        return self.operator_spec.input_ports

    @property
    def output_schema(self) -> tuple[PortSpec, ...]:
        return self.operator_spec.output_ports

    def payload(self) -> dict[str, object]:
        return {
            "operator_spec": self.operator_spec.to_dict(),
            "implementation_ref": self.implementation_ref.to_dict(),
            "runtime_adapter_ref": self.runtime_adapter_ref.to_dict(),
            "cache_profile_ref": self.cache_profile_ref,
            "cache_compatibility_mode": self.cache_compatibility_mode,
            "resource_hint_ref": self.resource_hint_ref,
            "partition_keys": list(self.partition_keys),
            "contract_version": self.contract_version,
        }

    @classmethod
    def build(
        cls,
        *,
        operator_spec: OperatorSpec,
        implementation_ref: OperatorImplementationRef,
        runtime_adapter_ref: OperatorRuntimeAdapterRef,
        cache_profile_ref: str,
        cache_compatibility_mode: str,
        resource_hint_ref: str,
        partition_keys: tuple[str, ...] = (),
    ) -> "OperatorDefinition":
        payload = {
            "operator_spec": operator_spec.to_dict(),
            "implementation_ref": implementation_ref.to_dict(),
            "runtime_adapter_ref": runtime_adapter_ref.to_dict(),
            "cache_profile_ref": cache_profile_ref,
            "cache_compatibility_mode": cache_compatibility_mode,
            "resource_hint_ref": resource_hint_ref,
            "partition_keys": list(partition_keys),
            "contract_version": _OPERATOR_DEFINITION_VERSION,
        }
        return cls(
            operator_spec,
            implementation_ref,
            runtime_adapter_ref,
            cache_profile_ref,
            cache_compatibility_mode,
            resource_hint_ref,
            partition_keys,
            typed_canonical_hash(payload),
        )


@dataclass(frozen=True)
class CompiledOperatorManifest:
    definitions: tuple[OperatorDefinition, ...]
    manifest_hash: str
    contract_version: str = _OPERATOR_MANIFEST_VERSION

    def __post_init__(self) -> None:
        canonical = tuple(sorted(self.definitions, key=lambda item: (item.name, item.version)))
        if not canonical or canonical != self.definitions:
            raise ExtensionError("OperatorDefinition manifest 必须非空并规范排序")
        names = {(item.name, item.version) for item in canonical}
        implementations = {item.implementation_ref.implementation_id for item in canonical}
        adapters = {item.runtime_adapter_ref.adapter_id for item in canonical}
        if len(names) != len(canonical):
            raise ExtensionError("OperatorDefinition 名称和版本重复")
        if len(implementations) != len(canonical):
            raise ExtensionError("OperatorDefinition implementation_id 重复")
        if len(adapters) != len(canonical):
            raise ExtensionError("OperatorDefinition runtime adapter_id 重复")
        if self.contract_version != _OPERATOR_MANIFEST_VERSION:
            raise ExtensionError("OperatorDefinition manifest 版本不受支持")
        if self.manifest_hash != typed_canonical_hash(self.payload()):
            raise ExtensionError("OperatorDefinition manifest hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "definitions": [item.payload() | {"definition_hash": item.definition_hash} for item in self.definitions],
            "contract_version": self.contract_version,
        }

    def require_operator(self, name: str, version: str) -> OperatorDefinition:
        try:
            return next(item for item in self.definitions if (item.name, item.version) == (name, version))
        except StopIteration as exc:
            raise ExtensionError(f"OperatorDefinition 未注册: {name}@{version}") from exc

    def require_implementation(self, implementation_id: str) -> OperatorDefinition:
        try:
            return next(
                item for item in self.definitions if item.implementation_ref.implementation_id == implementation_id
            )
        except StopIteration as exc:
            raise ExtensionError(f"OperatorDefinition implementation 未注册: {implementation_id}") from exc


def compile_operator_manifest(definitions: tuple[OperatorDefinition, ...]) -> CompiledOperatorManifest:
    canonical = tuple(sorted(definitions, key=lambda item: (item.name, item.version)))
    payload: Mapping[str, object] = {
        "definitions": [item.payload() | {"definition_hash": item.definition_hash} for item in canonical],
        "contract_version": _OPERATOR_MANIFEST_VERSION,
    }
    return CompiledOperatorManifest(canonical, typed_canonical_hash(payload))


@dataclass(frozen=True)
class RegisteredOperatorBinding:
    operator_id: str
    operator_version: str
    code_hash: str
    implementation_token: object
    implementation_id: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.implementation_token, (str, bytes)) or callable(self.implementation_token):
            raise ExtensionError("operator implementation token 不能是路径、bytes 或任意 callable")
        if self.implementation_id is not None and not _IMPLEMENTATION_ID.fullmatch(self.implementation_id):
            raise ExtensionError("operator implementation_id 无效")


class TrustedOperatorRegistry:
    """只接受代码内静态绑定，不从 ResearchPackage 解析实现入口。"""

    def __init__(
        self,
        *,
        operators: tuple[OperatorSpec, ...],
        strategies: tuple[StrategySpec, ...],
        bindings: tuple[RegisteredOperatorBinding, ...],
    ) -> None:
        operator_map = {(item.operator_id, item.operator_version): item for item in operators}
        strategy_map = {
            (item.role, item.strategy_id, item.strategy_version): item for item in strategies
        }
        binding_map = {(item.operator_id, item.operator_version): item for item in bindings}
        if not operators or len(operator_map) != len(operators):
            raise ExtensionError("trusted operator registry 为空或算子版本重复")
        if len(strategy_map) != len(strategies):
            raise ExtensionError("trusted operator registry 的策略版本重复")
        if set(binding_map) != set(operator_map):
            raise ExtensionError("operator bindings 与 specs 不闭合")
        for key, spec in operator_map.items():
            if binding_map[key].code_hash != spec.code_hash:
                raise ExtensionError(f"operator code hash 漂移: {spec.operator_id}")
        self._operators = MappingProxyType(operator_map)
        self._strategies = MappingProxyType(strategy_map)
        self._bindings = MappingProxyType(binding_map)
        payload = {
            "operators": [
                {**item.payload(), "spec_hash": item.spec_hash}
                for item in sorted(operators, key=lambda value: (value.operator_id, value.operator_version))
            ],
            "strategies": [
                {**item.payload(), "spec_hash": item.spec_hash}
                for item in sorted(
                    strategies,
                    key=lambda value: (value.role.value, value.strategy_id, value.strategy_version),
                )
            ],
            "bindings": [
                {
                    "operator_id": item.operator_id,
                    "operator_version": item.operator_version,
                    "code_hash": item.code_hash,
                }
                for item in sorted(bindings, key=lambda value: (value.operator_id, value.operator_version))
            ],
        }
        self.registry_hash = typed_canonical_hash(payload)

    def require_operator(self, operator_id: str, operator_version: str) -> OperatorSpec:
        try:
            return self._operators[(operator_id, operator_version)]
        except KeyError as exc:
            raise ExtensionError(f"operator 未注册或版本不匹配: {operator_id}@{operator_version}") from exc

    def require_strategy(
        self,
        role: StrategyRole,
        strategy_id: str,
        strategy_version: str,
    ) -> StrategySpec:
        try:
            return self._strategies[(role, strategy_id, strategy_version)]
        except KeyError as exc:
            raise ExtensionError(
                f"strategy 未注册或版本不匹配: {role.value}/{strategy_id}@{strategy_version}"
            ) from exc

    def binding(self, operator_id: str, operator_version: str) -> RegisteredOperatorBinding:
        self.require_operator(operator_id, operator_version)
        return self._bindings[(operator_id, operator_version)]

    def operator_ids(self) -> tuple[str, ...]:
        return tuple(sorted({item.operator_id for item in self._operators.values()}))

    @property
    def operator_specs(self) -> tuple[OperatorSpec, ...]:
        """返回按稳定键排序的只读算子合同快照。"""
        return tuple(
            sorted(
                self._operators.values(),
                key=lambda item: (item.operator_id, item.operator_version),
            )
        )

    @property
    def strategy_specs(self) -> tuple[StrategySpec, ...]:
        """返回按稳定键排序的只读策略合同快照。"""
        return tuple(
            sorted(
                self._strategies.values(),
                key=lambda item: (item.role.value, item.strategy_id, item.strategy_version),
            )
        )

    def admit(self, recipe: OperatorGraphRecipe) -> AdmittedOperatorGraph:
        """验证参数、策略、端口和依赖闭包，返回内容寻址准入结果。"""
        node_specs: dict[str, OperatorSpec] = {}
        operator_hashes: dict[str, str] = {}
        strategy_hashes: dict[str, str] = {}
        node_by_id = {item.node_id: item for item in recipe.nodes}
        try:
            for node in recipe.nodes:
                spec = self.require_operator(node.operator_id, node.operator_version)
                node_specs[node.node_id] = spec
                operator_hashes[node.node_id] = spec.spec_hash
                validate_parameters(node.parameters, spec.parameters, f"node.{node.node_id}.parameters")
                expected_roles = tuple(item.value for item in spec.strategy_roles)
                actual_roles = tuple(item.role.value for item in node.strategies)
                if actual_roles != expected_roles:
                    raise OperatorContractError(
                        f"node {node.node_id} 策略角色不匹配；期望={expected_roles}，实际={actual_roles}"
                    )
                for selection in node.strategies:
                    strategy = self.require_strategy(
                        selection.role,
                        selection.strategy_id,
                        selection.strategy_version,
                    )
                    validate_parameters(
                        selection.parameters,
                        strategy.parameters,
                        f"node.{node.node_id}.strategy.{selection.role.value}",
                    )
                    strategy_hashes[f"{node.node_id}.{selection.role.value}"] = strategy.spec_hash

            dependencies: dict[str, set[str]] = {item.node_id: set() for item in recipe.nodes}
            children: dict[str, set[str]] = {item.node_id: set() for item in recipe.nodes}
            for node in recipe.nodes:
                spec = node_specs[node.node_id]
                expected_ports = {item.port: item.artifact_type for item in spec.input_ports}
                actual_ports = {item.input_port for item in node.inputs}
                if actual_ports != set(expected_ports):
                    raise OperatorContractError(
                        f"node {node.node_id} 输入端口不匹配；期望={sorted(expected_ports)}，实际={sorted(actual_ports)}"
                    )
                for binding in node.inputs:
                    if binding.source_node_id not in node_by_id:
                        raise OperatorContractError(
                            f"node {node.node_id} 引用了未知来源节点: {binding.source_node_id}"
                        )
                    source_spec = node_specs[binding.source_node_id]
                    source_ports = {item.port: item.artifact_type for item in source_spec.output_ports}
                    if binding.source_output_port not in source_ports:
                        raise OperatorContractError(
                            f"node {node.node_id} 引用了未知来源端口: {binding.source_output_port}"
                        )
                    if source_ports[binding.source_output_port] != expected_ports[binding.input_port]:
                        raise OperatorContractError(f"node {node.node_id} 输入输出 artifact type 不闭合")
                    dependencies[node.node_id].add(binding.source_node_id)
                    children[binding.source_node_id].add(node.node_id)

            ready = sorted(node_id for node_id, values in dependencies.items() if not values)
            topological_order: list[str] = []
            while ready:
                current = ready.pop(0)
                topological_order.append(current)
                for child in sorted(children[current]):
                    dependencies[child].remove(current)
                    if not dependencies[child]:
                        ready.append(child)
                        ready.sort()
            if len(topological_order) != len(recipe.nodes):
                raise OperatorContractError("operator graph 存在依赖环")
            payload = {
                "recipe_hash": recipe.recipe_hash,
                "topological_order": topological_order,
                "operator_spec_hashes": operator_hashes,
                "strategy_spec_hashes": strategy_hashes,
                "registry_hash": self.registry_hash,
                "contract_version": OPERATOR_GRAPH_ADMISSION_VERSION,
            }
            return AdmittedOperatorGraph(
                recipe,
                tuple(topological_order),
                operator_hashes,
                strategy_hashes,
                self.registry_hash,
                typed_canonical_hash(payload),
            )
        except ExtensionError:
            raise
        except OperatorContractError as exc:
            raise ExtensionError(str(exc)) from exc


__all__ = [
    "CompiledOperatorManifest",
    "OperatorDefinition",
    "OperatorImplementationRef",
    "RegisteredOperatorBinding",
    "TrustedOperatorRegistry",
    "compile_operator_manifest",
]
