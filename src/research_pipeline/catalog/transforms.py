"""受控 Transform descriptor、manifest 与实现复核。"""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping

from .models import TransformContract

from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import CatalogReferenceError


def _freeze_metadata(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_metadata(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_metadata(item) for item in value)
    return value


def _plain_metadata(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_metadata(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain_metadata(item) for item in value]
    return value


@dataclass(frozen=True)
class TransformDescriptor:
    transform_id: str
    implementation_version: int
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    deterministic: bool
    code_hash: str
    parameters_schema: Mapping[str, Any] = field(default_factory=dict)
    time_policy: Mapping[str, Any] = field(default_factory=dict)
    resource_hint: Mapping[str, Any] = field(default_factory=dict)
    input_schema: Mapping[str, Any] = field(default_factory=dict)
    output_schema: Mapping[str, Any] = field(default_factory=dict)
    descriptor_version: str = "transform-descriptor-v1"

    def __post_init__(self) -> None:
        for name in (
            "parameters_schema", "time_policy", "resource_hint", "input_schema", "output_schema"
        ):
            value = getattr(self, name)
            if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
                raise CatalogReferenceError(f"Transform descriptor {name} 必须是字符串键映射")
            object.__setattr__(self, name, _freeze_metadata(value))
        if self.descriptor_version not in {"transform-descriptor-v1", "transform-descriptor-v2"}:
            raise CatalogReferenceError("Transform descriptor 版本不受支持")
        if self.descriptor_version == "transform-descriptor-v2" and (
            not self.time_policy or not self.resource_hint or not self.input_schema or not self.output_schema
        ):
            raise CatalogReferenceError("Transform descriptor v2 必须绑定端口 schema、时间传播和资源提示")
        if bool(self.input_schema) != bool(self.output_schema) or (
            self.input_schema and (
                set(self.input_schema) != set(self.inputs)
                or set(self.output_schema) != set(self.outputs)
            )
        ):
            raise CatalogReferenceError("Transform descriptor 端口 schema 不完整")

    def to_dict(self) -> dict[str, object]:
        payload = {
            "transform_id": self.transform_id,
            "implementation_version": self.implementation_version,
            "inputs": list(self.inputs),
            "outputs": list(self.outputs),
            "deterministic": self.deterministic,
            "code_hash": self.code_hash,
        }
        if self.descriptor_version == "transform-descriptor-v2":
            payload.update({
                "parameters_schema": _plain_metadata(self.parameters_schema),
                "time_policy": _plain_metadata(self.time_policy),
                "resource_hint": _plain_metadata(self.resource_hint),
                "input_schema": _plain_metadata(self.input_schema),
                "output_schema": _plain_metadata(self.output_schema),
                "descriptor_version": self.descriptor_version,
            })
        return payload


def code_hash_for_transform(implementation: Callable[..., object]) -> str:
    try:
        source = inspect.getsource(implementation)
    except (OSError, TypeError) as exc:
        raise CatalogReferenceError("Transform 实现必须有可审计源码") from exc
    return typed_canonical_hash({"source": source.replace("\r\n", "\n")})


def describe_transform(
    implementation: Callable[..., object], *, transform_id: str, implementation_version: int,
    inputs: tuple[str, ...], outputs: tuple[str, ...], deterministic: bool = True,
    parameters_schema: Mapping[str, Any] | None = None,
    time_policy: Mapping[str, Any] | None = None,
    resource_hint: Mapping[str, Any] | None = None,
    input_schema: Mapping[str, Any] | None = None,
    output_schema: Mapping[str, Any] | None = None,
) -> TransformDescriptor:
    metadata_supplied = any(
        item is not None
        for item in (
            parameters_schema, time_policy, resource_hint, input_schema, output_schema
        )
    )
    if metadata_supplied and any(
        item is None
        for item in (
            parameters_schema, time_policy, resource_hint, input_schema, output_schema
        )
    ):
        raise CatalogReferenceError("Transform descriptor v2 元数据必须完整提供")
    return TransformDescriptor(
        transform_id,
        implementation_version,
        inputs,
        outputs,
        deterministic,
        code_hash_for_transform(implementation),
        parameters_schema or {},
        time_policy or {},
        resource_hint or {},
        input_schema or {},
        output_schema or {},
        "transform-descriptor-v2" if metadata_supplied else "transform-descriptor-v1",
    )


def build_transform_manifest(descriptors: tuple[TransformDescriptor, ...]) -> dict[str, object]:
    ids = [item.transform_id for item in descriptors]
    if len(ids) != len(set(ids)):
        raise CatalogReferenceError("Transform manifest ID 重复")
    return {"manifest_version": "transform-manifest-v1", "transforms": [item.to_dict() for item in sorted(descriptors, key=lambda item: item.transform_id)]}


def verify_transform_implementation(implementation: Callable[..., object], descriptor: TransformDescriptor) -> None:
    actual = code_hash_for_transform(implementation)
    if actual != descriptor.code_hash:
        raise CatalogReferenceError(f"Transform 实现 hash 不匹配: {descriptor.transform_id}")


def validate_transform_manifest(
    contracts: Iterable[TransformContract],
    descriptors: Iterable[TransformDescriptor],
) -> None:
    """确认声明合同只引用 allowlist manifest 中完全一致的实现。"""

    descriptor_items = tuple(descriptors)
    descriptor_by_id = {item.transform_id: item for item in descriptor_items}
    if len(descriptor_by_id) != len(descriptor_items):
        raise CatalogReferenceError("Transform manifest ID 重复")
    for contract in contracts:
        descriptor = descriptor_by_id.get(contract.implementation_id)
        if descriptor is None:
            raise CatalogReferenceError(
                f"Transform 引用未注册实现: {contract.implementation_id}"
            )
        if (
            descriptor.implementation_version != contract.transform_version
            or descriptor.code_hash != contract.code_hash
            or descriptor.inputs != contract.inputs
            or descriptor.outputs != contract.outputs
        ):
            raise CatalogReferenceError(
                f"Transform manifest 与合同不一致: {contract.transform_id}"
            )
        expected_descriptor_version = (
            "transform-descriptor-v2"
            if contract.input_schema
            else "transform-descriptor-v1"
        )
        if descriptor.descriptor_version != expected_descriptor_version:
            raise CatalogReferenceError(
                f"Transform descriptor 版本与合同不一致: {contract.transform_id}"
            )
        if contract.input_schema and (
            dict(descriptor.parameters_schema) != dict(contract.parameters_schema)
            or dict(descriptor.time_policy) != dict(contract.time_policy)
            or dict(descriptor.resource_hint) != dict(contract.resource_hint)
            or dict(descriptor.input_schema) != dict(contract.input_schema)
            or dict(descriptor.output_schema) != dict(contract.output_schema)
        ):
            raise CatalogReferenceError(
                f"Transform descriptor v2 元数据与合同不一致: {contract.transform_id}"
            )


__all__ = ["TransformDescriptor", "build_transform_manifest", "code_hash_for_transform", "describe_transform", "validate_transform_manifest", "verify_transform_implementation"]
