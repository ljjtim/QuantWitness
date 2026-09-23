"""各领域 resolver 从唯一 manifest 获取的只读投影。"""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import ExtensionKind
from .manifest import CompiledExtensionManifest


@dataclass(frozen=True)
class ExtensionProjection:
    extension_id: str
    kind: ExtensionKind
    code_hash: str
    input_types: tuple[str, ...]
    output_types: tuple[str, ...]
    capabilities: tuple[str, ...]


def project_extensions(manifest: CompiledExtensionManifest, kind: ExtensionKind) -> tuple[ExtensionProjection, ...]:
    return tuple(ExtensionProjection(item.extension_id, item.kind, item.code_hash, item.input_types, item.output_types, item.capabilities) for item in manifest.descriptors_for_kind(kind))


__all__ = ["ExtensionProjection", "project_extensions"]
