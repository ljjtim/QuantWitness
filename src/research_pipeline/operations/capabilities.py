"""只读加载唯一能力清单，并从同一对象生成机器与文本输出。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from research_pipeline.platform import MainlineError


CAPABILITY_MANIFEST_VERSION = "research-capability-manifest-v1"
CAPABILITY_DISCOVERY_VERSION = "research-capability-discovery-v1"
CAPABILITY_FIELDS = (
    "id",
    "state",
    "commands",
    "required_inputs",
    "implementation_anchors",
    "verification_anchors",
    "evidence_level",
    "trust_level",
    "notes",
)


class CapabilityDiscoveryError(MainlineError):
    error_code = "capability_discovery_invalid"


def capability_manifest_path() -> Path:
    """返回随源码和 wheel 一起分发的唯一清单。"""

    path = Path(__file__).resolve().parents[1] / "capabilities.json"
    if not path.is_file():
        raise CapabilityDiscoveryError("缺少唯一能力清单 research_pipeline/capabilities.json")
    return path


def load_capability_manifest() -> dict[str, object]:
    try:
        payload = json.loads(capability_manifest_path().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CapabilityDiscoveryError("能力清单无法读取") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "contract_version", "states", "capabilities",
    }:
        raise CapabilityDiscoveryError("能力清单顶层 schema 不匹配")
    if payload["contract_version"] != CAPABILITY_MANIFEST_VERSION:
        raise CapabilityDiscoveryError("能力清单版本不受支持")
    capabilities = payload["capabilities"]
    if not isinstance(capabilities, list):
        raise CapabilityDiscoveryError("capabilities 必须是序列")
    for descriptor in capabilities:
        if not isinstance(descriptor, Mapping) or tuple(descriptor) != CAPABILITY_FIELDS:
            raise CapabilityDiscoveryError("CapabilityDescriptor 字段或顺序不匹配")
    return payload


def discovery_payload() -> dict[str, object]:
    manifest = load_capability_manifest()
    return {
        "contract_version": CAPABILITY_DISCOVERY_VERSION,
        "manifest_contract_version": manifest["contract_version"],
        "states": manifest["states"],
        "capabilities": manifest["capabilities"],
    }


def require_available_capability(capability_id: str) -> dict[str, object]:
    """只允许选择已实现状态；planned 只可发现，不能当成可运行。"""

    matches = [
        item for item in load_capability_manifest()["capabilities"]
        if item["id"] == capability_id
    ]
    if len(matches) != 1:
        raise CapabilityDiscoveryError(f"未知 capability: {capability_id}")
    descriptor = matches[0]
    if descriptor["state"] == "planned":
        raise CapabilityDiscoveryError(f"capability 尚不可运行: {capability_id}")
    return dict(descriptor)


def render_capabilities_text(payload: Mapping[str, object]) -> str:
    lines = ["capability\tstate\ttrust_level\tcommands"]
    for descriptor in payload["capabilities"]:
        lines.append(
            "\t".join((
                str(descriptor["id"]),
                str(descriptor["state"]),
                str(descriptor["trust_level"]),
                ",".join(str(item) for item in descriptor["commands"]),
            ))
        )
    return "\n".join(lines)


__all__ = [
    "CAPABILITY_DISCOVERY_VERSION",
    "CAPABILITY_FIELDS",
    "CAPABILITY_MANIFEST_VERSION",
    "CapabilityDiscoveryError",
    "capability_manifest_path",
    "discovery_payload",
    "load_capability_manifest",
    "require_available_capability",
    "render_capabilities_text",
]
