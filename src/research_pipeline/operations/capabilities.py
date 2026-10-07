"""只读加载唯一能力清单，并从同一对象生成机器与文本输出。"""

from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Mapping

from research_pipeline.platform import (
    MainlineError, REQUIRED_GATE_IDS, ReleaseAcceptanceInput, ReleaseEnvelope,
    ReleaseGateReceipt, load_build_manifest, typed_canonical_hash,
    verify_release_envelope,
)


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



def validate_capability_claims(manifest: Mapping[str, object]) -> list[str]:
    """校验公开声明的一致性，返回需要独立发布证据的能力。"""

    if not manifest.get("capabilities"):
        raise CapabilityDiscoveryError("能力清单不能为空")
    sealed = []
    seen = set()
    for item in manifest["capabilities"]:
        identity = item["id"]
        if identity in seen or item["state"] not in {"planned", "local_only", "sealed"}:
            raise CapabilityDiscoveryError("能力 ID 重复或状态无效")
        seen.add(identity)
        claims = (
            item["state"] == "sealed", item["trust_level"] == "sealed",
            item["evidence_level"] == "sealed_release_acceptance",
        )
        if any(claims) and not all(claims):
            raise CapabilityDiscoveryError(f"能力 sealed 声明不一致: {identity}")
        if all(claims):
            sealed.append(identity)
    return sealed


def _read_evidence_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"证据必须是 JSON 对象: {path}")
    return payload


def _evidence_path(root: Path, anchor: str) -> Path:
    path = (root / anchor).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("证据路径超出显式根目录")
    return path


def _artifact_bytes(root: Path, reference: Mapping[str, str]) -> bytes:
    content = _evidence_path(root, reference["anchor"]).read_bytes()
    if hashlib.sha256(content).hexdigest() != reference["artifact_sha256"]:
        raise ValueError(f"证据内容已改变: {reference['anchor']}")
    return content


def validate_capability_release_evidence(
    *,
    project_root: Path,
    evidence_root: Path,
    manifest: Mapping[str, object],
    rules: list[Mapping[str, object]],
    release_candidate_id: str,
    as_of: str,
) -> None:
    """消费独立目录中的发布闭包与逐能力测试证据，不执行测试或访问数据库。"""

    build = load_build_manifest(evidence_root / "build-manifest.json")
    envelope = ReleaseEnvelope.from_dict(_read_evidence_json(evidence_root / "release-envelope.json"))
    acceptance = ReleaseAcceptanceInput.from_dict(_read_evidence_json(evidence_root / "acceptance-input.json"))
    receipts = tuple(
        ReleaseGateReceipt.from_dict(_read_evidence_json(evidence_root / "receipts" / f"{gate}.json"))
        for gate in REQUIRED_GATE_IDS
    )
    verify_release_envelope(envelope, manifest=build, receipts=receipts, acceptance=acceptance, as_of=as_of)
    if not release_candidate_id or envelope.release_candidate_id != release_candidate_id:
        raise ValueError("晋级证据不属于要求验收的候选")
    for anchor, digest, input_key in (
        ("capabilities.json", envelope.capabilities_digest, "src/research_pipeline/capabilities.json"),
        ("dependency-lock.json", envelope.dependency_lock_digest, "release/dependency-distributions.json"),
    ):
        _artifact_bytes(evidence_root, {"anchor": anchor, "artifact_sha256": digest})
        if build.input_digests.get(input_key) != digest:
            raise ValueError("封存能力清单或依赖锁未绑定 BuildManifest")
    for receipt in receipts:
        content = _artifact_bytes(evidence_root, {
            "anchor": f"evidence/{receipt.gate_id}.json",
            "artifact_sha256": receipt.evidence_hashes.get("acceptance"),
        })
        gate = json.loads(content)
        if (
            gate.get("gate_id") != receipt.gate_id or gate.get("status") != "pass"
            or gate.get("evidence_scope") != "release_candidate"
            or gate.get("release_candidate_id") != release_candidate_id
            or gate.get("build_manifest_hash") != build.manifest_hash
        ):
            raise ValueError("封存 Gate 未绑定当前候选")
    capabilities_bytes = (project_root / "src/research_pipeline/capabilities.json").read_bytes()
    if (
        json.loads(capabilities_bytes) != manifest
        or hashlib.sha256(capabilities_bytes).hexdigest() != envelope.capabilities_digest
        or build.source_tree_digest != typed_canonical_hash(dict(build.input_digests))
    ):
        raise ValueError("晋级证据未绑定当前能力清单或源码输入")
    required_source = "src/research_pipeline/capabilities.json"
    if required_source not in build.input_digests:
        raise ValueError("BuildManifest 缺少能力清单")
    for anchor, digest in build.input_digests.items():
        if anchor.startswith(("src/", "tests/")):
            _artifact_bytes(project_root, {"anchor": anchor, "artifact_sha256": digest})
    gates = {item.gate_id: item for item in receipts}
    descriptors = {item["id"]: item for item in manifest["capabilities"]}
    categories = ("formal_execution_test", "negative_test", "independent_verification")
    for rule in rules:
        identity = rule["capability_id"]
        if descriptors[identity]["state"] != "sealed":
            continue
        domains: dict[str, set[str]] = {category: set() for category in categories}
        producers: dict[str, set[str]] = {category: set() for category in categories}
        used_cases: set[tuple[str, str, str]] = set()
        for category in categories:
            references = rule["promotion_evidence"][category]
            if not references:
                raise ValueError(f"{identity} 缺少 {category} 证据")
            for reference in references:
                proof = json.loads(_artifact_bytes(evidence_root, reference))
                if not isinstance(proof, dict) or (
                    proof.get("contract_version") != "research-capability-promotion-evidence-v2"
                    or proof.get("capability_id") != identity
                    or proof.get("category") != category
                    or proof.get("status") != "pass"
                    or proof.get("subject_manifest_hash") != typed_canonical_hash(manifest)
                    or proof.get("release_candidate_id") != release_candidate_id
                    or proof.get("build_manifest_hash") != build.manifest_hash
                ):
                    raise ValueError(f"{identity} 晋级收据未绑定当前候选、能力或类别")
                for field in ("producer_id", "trust_domain"):
                    if not isinstance(proof.get(field), str) or not proof[field]:
                        raise ValueError(f"晋级收据缺少 {field}")
                domains[category].add(proof["trust_domain"])
                producers[category].add(proof["producer_id"])
                gate_id = proof.get("gate_id")
                if gate_id not in gates:
                    raise ValueError("晋级收据引用未知 Gate")
                gate_content = _artifact_bytes(evidence_root, proof["gate_evidence"])
                gate = json.loads(gate_content)
                if (
                    gates[gate_id].evidence_hashes.get("acceptance") != hashlib.sha256(gate_content).hexdigest()
                    or gate.get("gate_id") != gate_id
                    or gate.get("status") != "pass"
                    or gate.get("evidence_scope") != "release_candidate"
                    or gate.get("release_candidate_id") != release_candidate_id
                    or gate.get("build_manifest_hash") != build.manifest_hash
                ):
                    raise ValueError("晋级证据与封套 Gate 不一致")
                _validate_promotion_test_cases(
                    proof, gate, evidence_root, project_root, build.input_digests,
                    descriptors[identity]["verification_anchors"], used_cases,
                )
        for identities in (domains, producers):
            if identities["independent_verification"] & (
                identities["formal_execution_test"] | identities["negative_test"]
            ):
                raise ValueError(f"{identity} 的独立验证与执行证据来自同一信任域或生产者")


def _validate_promotion_test_cases(
    proof: Mapping[str, object], gate: Mapping[str, object], evidence_root: Path,
    project_root: Path, input_digests: Mapping[str, str],
    verification_anchors: list[str], used_cases: set[tuple[str, str, str]],
) -> None:
    try:
        report = ET.fromstring(_artifact_bytes(evidence_root, proof["junit"]))
    except ET.ParseError as exc:
        raise ValueError("晋级 JUnit 无法解析") from exc
    cases = list(report.iter("testcase"))
    if not cases or any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
        raise ValueError("晋级 JUnit 无实际测试、存在失败或跳过")
    counts = gate.get("pytest")
    if not isinstance(counts, Mapping) or counts != {
        "tests": len(cases), "failures": 0, "errors": 0, "skipped": 0,
    }:
        raise ValueError("Gate 测试计数与实际 JUnit 不一致")
    selected = proof.get("test_cases")
    if not isinstance(selected, list) or not selected:
        raise ValueError("晋级收据必须列出实际测试项")
    for selected_case in selected:
        source = selected_case["source"]
        classname = selected_case["classname"]
        name = selected_case["name"]
        symbol = name.split("[", 1)[0]
        source_key = source.removeprefix("research_pipeline/")
        expected_classname = source_key.removesuffix(".py").replace("/", ".")
        normalized_classname = classname.removeprefix("research_pipeline.")
        if normalized_classname != expected_classname and not normalized_classname.startswith(expected_classname + "."):
            raise ValueError("JUnit 测试项与声明源码不一致")
        registered = {anchor.removeprefix("research_pipeline/") for anchor in verification_anchors}
        if source_key + "::" + symbol not in registered:
            raise ValueError("晋级测试项未登记在能力验证锚点")
        digest = input_digests.get(source_key)
        if digest is None or gate.get("test_file_hashes", {}).get(source_key) != digest:
            raise ValueError("晋级测试源码未绑定 BuildManifest 和 Gate")
        _artifact_bytes(project_root, {"anchor": source_key, "artifact_sha256": digest})
        matches = [case for case in cases if case.get("classname") == classname and case.get("name") == name]
        identity = (source_key, normalized_classname, name)
        if len(matches) != 1 or identity in used_cases:
            raise ValueError("晋级测试项缺失、重复或跨类别复用")
        used_cases.add(identity)


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
    "validate_capability_claims",
    "validate_capability_release_evidence",
]
