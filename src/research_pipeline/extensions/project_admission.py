"""显式项目算子 bundle 与内置算子的单次组合准入。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from research_pipeline.platform import typed_canonical_hash

from .errors import ExtensionError
from .operators import RegisteredOperatorBinding, TrustedOperatorRegistry
from .governance import validate_project_operator_artifacts
from .project_bundle import (
    ProjectOperatorBundleManifest,
    verify_project_operator_bundle,
)


@dataclass(frozen=True)
class ProjectOperatorImplementationToken:
    """不可调用的项目实现身份；实际代码加载由受控 Worker 负责。"""

    manifest: ProjectOperatorBundleManifest
    implementation_id: str


ProjectImplementationToken = ProjectOperatorImplementationToken
ProjectBundleManifest = ProjectOperatorBundleManifest


class AdmittedProjectOperatorRegistry(TrustedOperatorRegistry):
    """只对当前编译有效的内置 + 项目算子组合注册表。"""

    def __init__(
        self,
        *,
        builtin_registry: TrustedOperatorRegistry,
        bundles: tuple[tuple[Path, ProjectBundleManifest], ...],
    ) -> None:
        if not bundles:
            raise ExtensionError("项目组合注册表至少需要一个显式 extension bundle")
        project_ids = {manifest.project_id for _, manifest in bundles}
        if len(project_ids) != 1:
            raise ExtensionError("一次准入只能使用同一 project_id 的 extension bundles")
        bundle_hashes = tuple(manifest.bundle_hash for _, manifest in bundles)
        if len(bundle_hashes) != len(set(bundle_hashes)):
            raise ExtensionError("extension bundle 不得重复")

        builtin_specs = builtin_registry.operator_specs
        project_specs = tuple(manifest.operator_spec for _, manifest in bundles)
        project_output_types = {
            port.artifact_type for specification in project_specs for port in specification.output_ports
        }
        for specification in project_specs:
            validate_project_operator_artifacts(
                specification,
                builtin_specs,
                project_artifact_types=project_output_types,
            )
        builtin_ids = {item.operator_id for item in builtin_specs}
        project_operator_ids = tuple(item.operator_id for item in project_specs)
        if len(project_operator_ids) != len(set(project_operator_ids)):
            raise ExtensionError("项目算子 ID 重复或版本冲突")
        collisions = sorted(builtin_ids.intersection(project_operator_ids))
        if collisions:
            raise ExtensionError(f"项目算子不得覆盖内置算子 ID: {collisions}")
        allowed_pit_capabilities = {
            capability for item in builtin_specs for capability in item.pit_capabilities
        }
        unknown_capabilities = sorted({
            capability
            for item in project_specs
            for capability in item.pit_capabilities
            if capability not in allowed_pit_capabilities
        })
        if unknown_capabilities:
            raise ExtensionError(f"项目算子引用未登记 PIT capability: {unknown_capabilities}")

        bindings = [
            builtin_registry.binding(item.operator_id, item.operator_version)
            for item in builtin_specs
        ]
        tokens: dict[tuple[str, str], ProjectImplementationToken] = {}
        identities: dict[str, Mapping[str, object]] = {}
        bundle_paths: dict[str, Path] = {}
        for path, manifest in bundles:
            spec = manifest.operator_spec
            implementation_id = f"project.operator.{manifest.bundle_hash[:32]}"
            token = ProjectOperatorImplementationToken(
                manifest,
                implementation_id,
            )
            identity = {
                "project_id": manifest.project_id,
                "bundle_hash": manifest.bundle_hash,
                "bundle_id": manifest.bundle_id,
                "operator_spec_hash": spec.spec_hash,
                "implementation_kind": "python_worker",
                "source_tree_hash": manifest.source_tree_hash,
                "dependency_lock_hash": manifest.dependency_lock_hash,
                "permissions": manifest.permissions.to_dict(),
                "requires_python": manifest.requires_python,
                "abi_version": manifest.abi_version,
            }
            key = (spec.operator_id, spec.operator_version)
            tokens[key] = token
            bindings.append(
                RegisteredOperatorBinding(
                    spec.operator_id,
                    spec.operator_version,
                    spec.code_hash,
                    token,
                )
            )
            identities[implementation_id] = MappingProxyType(identity)
            bundle_paths[implementation_id] = path

        super().__init__(
            operators=(*builtin_specs, *project_specs),
            strategies=builtin_registry.strategy_specs,
            bindings=tuple(bindings),
        )
        base_registry_hash = self.registry_hash
        ordered_identities = {
            key: dict(identities[key]) for key in sorted(identities)
        }
        self.project_id = next(iter(project_ids))
        self.bundle_hashes = tuple(sorted(bundle_hashes))
        self.implementation_identities = MappingProxyType(ordered_identities)
        self.bundle_paths = MappingProxyType(dict(sorted(bundle_paths.items())))
        self._project_tokens = MappingProxyType(tokens)
        self._project_tokens_by_implementation = MappingProxyType({
            token.implementation_id: token for token in tokens.values()
        })
        self.registry_hash = typed_canonical_hash({
            "builtin_registry_hash": builtin_registry.registry_hash,
            "combined_operator_registry_hash": base_registry_hash,
            "project_id": self.project_id,
            "project_implementations": ordered_identities,
        })

    def project_token(
        self,
        operator_id: str,
        operator_version: str,
    ) -> ProjectImplementationToken | None:
        return self._project_tokens.get((operator_id, operator_version))

    def project_token_by_implementation(
        self,
        implementation_id: str,
    ) -> ProjectImplementationToken | None:
        """按已编译 DAG 中的实现 ID 取回不可调用的准入令牌。"""
        return self._project_tokens_by_implementation.get(implementation_id)


def build_admitted_operator_registry(
    bundle_paths: Iterable[str | Path],
    *,
    builtin_registry: TrustedOperatorRegistry,
    expected_project_id: str | None = None,
) -> TrustedOperatorRegistry:
    """复验显式 bundle 并构造单次组合注册表；空输入保持核心身份兼容。"""
    paths = tuple(Path(path).resolve(strict=True) for path in bundle_paths)
    if not paths:
        return builtin_registry
    verified = tuple((path, verify_project_operator_bundle(path)) for path in paths)
    if expected_project_id is not None and any(
        manifest.project_id != expected_project_id for _, manifest in verified
    ):
        raise ExtensionError("extension bundle 与预期 project_id 不一致")
    return AdmittedProjectOperatorRegistry(
        builtin_registry=builtin_registry,
        bundles=verified,
    )


__all__ = [
    "AdmittedProjectOperatorRegistry",
    "ProjectOperatorImplementationToken",
    "build_admitted_operator_registry",
]
