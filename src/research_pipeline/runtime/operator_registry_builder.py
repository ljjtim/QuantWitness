"""从已验证 OperatorDefinition manifest 构造唯一正式 registry。"""

from __future__ import annotations

from research_pipeline.extensions import (
    CompiledOperatorManifest,
    RegisteredOperatorBinding,
    TrustedOperatorRegistry,
)


def build_operator_registry_from_manifest(
    manifest: CompiledOperatorManifest,
) -> TrustedOperatorRegistry:
    """不重读公共入口，直接从已通过晋级门的 manifest 构造 registry。"""

    operators = tuple(item.operator_spec for item in manifest.definitions)
    bindings = tuple(
        RegisteredOperatorBinding(
            item.name,
            item.version,
            item.operator_spec.code_hash,
            object(),
            item.implementation_ref.implementation_id,
        )
        for item in manifest.definitions
    )
    return TrustedOperatorRegistry(
        operators=operators,
        strategies=(),
        bindings=bindings,
    )


__all__ = ["build_operator_registry_from_manifest"]
