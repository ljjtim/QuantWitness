"""正式 Typed DAG 的唯一 Runtime 编译器。"""

from __future__ import annotations

import importlib

from .errors import RuntimeRegistryError
from .graph import DagSpec
from .operator_definitions import (
    build_mainline_operator_manifest,
    production_implementation_code_hash,
)
from .registry import ImplementationRegistry


def build_production_registry() -> ImplementationRegistry:
    """只登记已进入正式 OperatorDefinition 清单的核心实现。"""
    registry = ImplementationRegistry()
    for definition in build_mainline_operator_manifest().definitions:
        implementation = definition.implementation_ref
        registry.register(
            implementation.implementation_id,
            implementation.code_fingerprint,
            (implementation.capability,),
        )
    return registry


def compile_production_dag(
    dag: DagSpec,
    registry: ImplementationRegistry | None = None,
) -> DagSpec:
    """验证正式计划中的每个节点都解析到固定真实实现。"""
    trusted = registry or build_production_registry()
    definitions = {
        item.implementation_ref.implementation_id: item
        for item in build_mainline_operator_manifest().definitions
    }
    for node in dag.nodes:
        descriptor = trusted.require(node.implementation_id)
        definition = definitions.get(node.implementation_id)
        if (
            definition is None
            or descriptor.code_fingerprint
            != definition.implementation_ref.code_fingerprint
        ):
            raise RuntimeRegistryError(
                f"正式 implementation 指纹不可信: {node.implementation_id}"
            )
        implementation = definition.implementation_ref
        if implementation.implementation_scope != "core":
            raise RuntimeRegistryError(
                f"正式 DAG 禁止项目作用域实现: {node.implementation_id}"
            )
        module = importlib.import_module(implementation.module_name)
        if getattr(module, implementation.symbol_name, None) is None:
            raise RuntimeRegistryError(
                f"正式 implementation 没有真实代码落点: {node.implementation_id}"
            )
        if not node.pure or not node.cacheable:
            raise RuntimeRegistryError(
                f"正式研究节点必须声明纯函数和语义缓存: {node.node_id}"
            )
    return dag


__all__ = [
    "build_production_registry",
    "compile_production_dag",
    "production_implementation_code_hash",
]
