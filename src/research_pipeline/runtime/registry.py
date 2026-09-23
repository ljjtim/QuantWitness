"""封闭 implementation/provider capability 注册表。"""

from __future__ import annotations

from dataclasses import dataclass

from research_pipeline.extensions import CompiledExtensionManifest

from .contracts import _require_id
from .errors import RuntimeRegistryError


@dataclass(frozen=True)
class ImplementationDescriptor:
    implementation_id: str
    code_fingerprint: str
    capabilities: tuple[str, ...]


class ImplementationRegistry:
    def __init__(self) -> None:
        self._items: dict[str, ImplementationDescriptor] = {}

    def register(self, implementation_id: str, code_fingerprint: str, capabilities: tuple[str, ...]) -> ImplementationDescriptor:
        try:
            _require_id(implementation_id, "implementation_id")
            if implementation_id in self._items:
                raise RuntimeRegistryError(f"implementation 重复注册: {implementation_id}")
            if not code_fingerprint:
                raise RuntimeRegistryError("code_fingerprint 不能为空")
            if len(set(capabilities)) != len(capabilities):
                raise RuntimeRegistryError("capability 不得重复")
            for capability in capabilities:
                _require_id(capability, "capability")
            descriptor = ImplementationDescriptor(implementation_id, str(code_fingerprint), tuple(capabilities))
            self._items[implementation_id] = descriptor
            return descriptor
        except RuntimeRegistryError:
            raise
        except Exception as exc:
            raise RuntimeRegistryError(str(exc)) from exc

    def require(self, implementation_id: str) -> ImplementationDescriptor:
        try:
            return self._items[implementation_id]
        except KeyError as exc:
            raise RuntimeRegistryError(f"implementation 未注册: {implementation_id}") from exc

    def ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._items))

    def providers_for(self, capability: str) -> tuple[ImplementationDescriptor, ...]:
        return tuple(self._items[key] for key in sorted(self._items) if capability in self._items[key].capabilities)

    @classmethod
    def from_extension_manifest(cls, manifest: CompiledExtensionManifest) -> "ImplementationRegistry":
        registry = cls()
        for descriptor in manifest.descriptors:
            registry.register(descriptor.extension_id, descriptor.code_hash, descriptor.capabilities)
        return registry


__all__ = ["ImplementationDescriptor", "ImplementationRegistry"]
