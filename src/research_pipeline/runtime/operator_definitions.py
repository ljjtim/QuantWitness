"""主线算子合同、实现和 Runtime 提示的唯一公共汇总入口。"""

from __future__ import annotations

from functools import lru_cache

from research_pipeline.extensions import (
    CompiledOperatorManifest,
    ExtensionError,
    compile_operator_manifest,
)

from .errors import RuntimeRegistryError
from .operator_definition_factory import (
    _implementation_code_hash,
    _resolve_source_module as _resolve_source_module,
)
from .operator_families import (
    build_daily_model_operator_definitions,
    build_data_event_daily_operator_definitions,
    build_minute_operator_definitions,
)


@lru_cache(maxsize=1)
def build_mainline_operator_manifest() -> CompiledOperatorManifest:
    definitions = (
        *build_minute_operator_definitions(),
        *build_data_event_daily_operator_definitions(),
        *build_daily_model_operator_definitions(),
    )
    manifest = compile_operator_manifest(definitions)
    from .operator_promotion import validate_mainline_operator_promotions

    validate_mainline_operator_promotions(manifest)
    return manifest


def production_implementation_code_hash(implementation_id: str) -> str:
    manifest = build_mainline_operator_manifest()
    try:
        return manifest.require_implementation(
            implementation_id
        ).implementation_ref.code_hash
    except ExtensionError as exc:
        raise RuntimeRegistryError(
            f"未知正式 implementation: {implementation_id}"
        ) from exc


def production_runtime_adapter_code_hash(adapter_id: str) -> str:
    """按当前仓库字节重算受信 Runtime adapter 摘要。"""
    references = [
        item.runtime_adapter_ref
        for item in build_mainline_operator_manifest().definitions
        if item.runtime_adapter_ref.adapter_id == adapter_id
    ]
    if len(references) != 1:
        raise RuntimeRegistryError(f"未知正式 Runtime adapter: {adapter_id}")
    reference = references[0]
    return _implementation_code_hash(
        implementation_id=reference.adapter_id,
        code_fingerprint="operator-runtime-adapter-v1",
        module_name=reference.module_name,
        symbol_name=reference.symbol_name,
        dependency_modules=reference.dependency_modules,
    )


__all__ = [
    "build_mainline_operator_manifest",
    "production_implementation_code_hash",
    "production_runtime_adapter_code_hash",
]
