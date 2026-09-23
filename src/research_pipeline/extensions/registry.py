"""代码内 allowlist 与 ResearchPackage 引用门禁。"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from .errors import ExtensionError
from .manifest import CompiledExtensionManifest


@dataclass(frozen=True)
class RegisteredExtension:
    extension_id: str
    code_hash: str
    implementation_token: object

    def __post_init__(self) -> None:
        if isinstance(self.implementation_token, (str, bytes)) or callable(self.implementation_token):
            raise ExtensionError("implementation token 不能是模块路径、bytes 或任意 callable")


class ControlledExtensionRegistry:
    def __init__(self, manifest: CompiledExtensionManifest, allowlist: Mapping[str, RegisteredExtension]) -> None:
        if set(allowlist) != {item.extension_id for item in manifest.descriptors}:
            raise ExtensionError("builder allowlist 与 compiled manifest 不一致")
        for descriptor in manifest.descriptors:
            registered = allowlist[descriptor.extension_id]
            if registered.extension_id != descriptor.extension_id or registered.code_hash != descriptor.code_hash:
                raise ExtensionError(f"extension code hash 漂移: {descriptor.extension_id}")
        self.manifest = manifest
        self._allowlist = MappingProxyType(dict(allowlist))

    def resolve(self, extension_id: str) -> RegisteredExtension:
        try:
            return self._allowlist[extension_id]
        except KeyError as exc:
            raise ExtensionError(f"extension 未注册: {extension_id}") from exc

    def require_package_extensions(self, payload: Mapping[str, object]) -> tuple[RegisteredExtension, ...]:
        if set(payload) != {"extension_ids"} or not isinstance(payload["extension_ids"], list):
            raise ExtensionError("ResearchPackage 扩展引用只能包含 extension_ids")
        ids = payload["extension_ids"]
        if any(not isinstance(item, str) for item in ids) or len(ids) != len(set(ids)):
            raise ExtensionError("extension_ids 必须是唯一字符串列表")
        return tuple(self.resolve(item) for item in sorted(ids))


__all__ = ["ControlledExtensionRegistry", "RegisteredExtension"]
