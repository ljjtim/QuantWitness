"""扩展依赖图编译与 canonical manifest。"""

from __future__ import annotations

from dataclasses import dataclass

from research_pipeline.platform.canonical import typed_canonical_hash

from typing import Mapping

from .contracts import ExtensionDescriptor, ExtensionKind
from .errors import ExtensionError


EXTENSION_MANIFEST_VERSION = "research-extension-manifest-v1"


@dataclass(frozen=True)
class CompiledExtensionManifest:
    descriptors: tuple[ExtensionDescriptor, ...]
    topological_order: tuple[str, ...]
    manifest_hash: str
    contract_version: str = EXTENSION_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if not self.descriptors or tuple(sorted(self.descriptors, key=lambda item: item.extension_id)) != self.descriptors:
            raise ExtensionError("extension manifest 必须非空并规范排序")
        if self.contract_version != EXTENSION_MANIFEST_VERSION or self.manifest_hash != typed_canonical_hash(self.payload()):
            raise ExtensionError("extension manifest hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"descriptors": [item.to_dict() for item in self.descriptors], "topological_order": list(self.topological_order), "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "manifest_hash": self.manifest_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "CompiledExtensionManifest":
        if set(payload) != {"descriptors", "topological_order", "manifest_hash", "contract_version"}:
            raise ExtensionError("extension manifest schema 无效")
        descriptors, order = payload["descriptors"], payload["topological_order"]
        if not isinstance(descriptors, list) or any(not isinstance(item, Mapping) for item in descriptors) or not isinstance(order, list) or any(not isinstance(item, str) for item in order):
            raise ExtensionError("extension manifest 列表字段无效")
        manifest = cls(tuple(ExtensionDescriptor.from_dict(item) for item in descriptors if isinstance(item, Mapping)), tuple(order), str(payload["manifest_hash"]), str(payload["contract_version"]))
        compiled = compile_extension_manifest(manifest.descriptors)
        if compiled.topological_order != manifest.topological_order or compiled.manifest_hash != manifest.manifest_hash:
            raise ExtensionError("extension manifest 依赖编译结果不一致")
        return manifest

    def descriptor(self, extension_id: str) -> ExtensionDescriptor:
        try:
            return next(item for item in self.descriptors if item.extension_id == extension_id)
        except StopIteration as exc:
            raise ExtensionError(f"extension 未注册: {extension_id}") from exc

    def descriptors_for_kind(self, kind: ExtensionKind) -> tuple[ExtensionDescriptor, ...]:
        return tuple(item for item in self.descriptors if item.kind is kind)


def compile_extension_manifest(descriptors: tuple[ExtensionDescriptor, ...]) -> CompiledExtensionManifest:
    by_id = {item.extension_id: item for item in descriptors}
    if not descriptors or len(by_id) != len(descriptors):
        raise ExtensionError("extension manifest 为空或 ID 重复")
    for item in descriptors:
        unknown = set(item.dependencies) - set(by_id)
        if unknown:
            raise ExtensionError(f"extension 依赖未注册: {sorted(unknown)}")
        available_outputs = {output for dependency in item.dependencies for output in by_id[dependency].output_types}
        if not set(item.input_types).issubset(available_outputs):
            raise ExtensionError(f"extension 输入输出不闭合: {item.extension_id}")
    indegree = {extension_id: 0 for extension_id in by_id}
    children = {extension_id: [] for extension_id in by_id}
    for item in descriptors:
        for dependency in item.dependencies:
            indegree[item.extension_id] += 1
            children[dependency].append(item.extension_id)
    ready = sorted(extension_id for extension_id, degree in indegree.items() if degree == 0)
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for child in sorted(children[current]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
                ready.sort()
    if len(order) != len(by_id):
        raise ExtensionError("extension 依赖存在环")
    normalized = tuple(sorted(descriptors, key=lambda item: item.extension_id))
    payload = {"descriptors": [item.to_dict() for item in normalized], "topological_order": order, "contract_version": EXTENSION_MANIFEST_VERSION}
    return CompiledExtensionManifest(normalized, tuple(order), typed_canonical_hash(payload))


__all__ = ["EXTENSION_MANIFEST_VERSION", "CompiledExtensionManifest", "compile_extension_manifest"]
