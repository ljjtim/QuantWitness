"""冻结整改基线，并阻止已知失败能力在证据补齐前升级。"""

from __future__ import annotations

import ast
import hashlib
from importlib.util import resolve_name
import json
from pathlib import Path
from typing import Mapping

from research_pipeline.operations.capabilities import load_capability_manifest
from research_pipeline.platform import MainlineError, typed_canonical_hash
from research_pipeline.runtime.operator_definitions import (
    build_mainline_operator_manifest,
)
from research_pipeline.operations.framework_boundary import audit_framework_boundary


REMEDIATION_BASELINE_VERSION = "research-remediation-baseline-v1"
CONTAINMENT_GATE_VERSION = "research-capability-containment-gate-v1"
EVIDENCE_CHECKLIST_VERSION = "research-capability-evidence-checklist-v1"
PROMOTION_EVIDENCE_RECEIPT_VERSION = "research-capability-promotion-evidence-v1"
PROMOTION_EVIDENCE_CATEGORIES = (
    "formal_execution_test",
    "negative_test",
    "independent_verification",
)
STATE_RANK = {"planned": 0, "local_only": 1, "sealed": 2}


class CapabilityContainmentError(MainlineError):
    """整改基线或能力晋级门禁无效。"""

    error_code = "capability_containment_invalid"


def load_remediation_baseline(path: str | Path) -> dict[str, object]:
    """读取并验证发布的整改基线，不信任自报 hash。"""

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CapabilityContainmentError("整改基线无法读取") from exc
    _validate_baseline(payload)
    return payload


def refresh_remediation_baseline(
    repository_root: str | Path,
    baseline_policy: Mapping[str, object],
    *,
    public_commands: tuple[str, ...],
    public_help_text: str,
    recipe_projection: list[dict[str, object]],
    current_snapshot: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """保留人工审查事实，只确定性刷新可由源码重建的快照。"""

    policy = _baseline_policy(baseline_policy)
    body = {
        "contract_version": REMEDIATION_BASELINE_VERSION,
        "audit_sources": policy["audit_sources"],
        "audit_findings": policy["audit_findings"],
        "containment": policy["containment"],
        "snapshot": dict(current_snapshot) if current_snapshot is not None else build_current_snapshot(
            repository_root,
            public_commands=public_commands,
            public_help_text=public_help_text,
            recipe_projection=recipe_projection,
        ),
    }
    return {**body, "baseline_hash": typed_canonical_hash(body)}


def build_current_snapshot(
    repository_root: str | Path,
    *,
    public_commands: tuple[str, ...],
    public_help_text: str,
    recipe_projection: list[dict[str, object]],
) -> dict[str, object]:
    """确定性冻结公开命令、能力、算子、recipe 和源码依赖图。"""

    root = Path(repository_root).resolve()
    current_root = Path(__file__).resolve().parents[4]
    if root != current_root:
        raise CapabilityContainmentError(
            "repository_root 与当前导入的 research_pipeline 源码不一致"
        )
    package_root = root / "research_pipeline"
    source_root = package_root / "src" / "research_pipeline"
    manifest_path = source_root / "capabilities.json"
    if not manifest_path.is_file() or not source_root.is_dir():
        raise CapabilityContainmentError("repository_root 不含 research_pipeline 源码")
    manifest = load_capability_manifest()
    capabilities = [
        {"id": item["id"], "state": item["state"]}
        for item in manifest["capabilities"]
    ]
    operator_manifest = build_mainline_operator_manifest()
    dependency_snapshot = _dependency_snapshot(source_root)
    return {
        "capability_manifest": {
            "payload_hash": typed_canonical_hash(manifest),
            "capability_count": len(capabilities),
            "states": capabilities,
        },
        "public_cli": {
            "commands": list(public_commands),
            "help_sha256": hashlib.sha256(
                public_help_text.encode("utf-8")
            ).hexdigest(),
        },
        "operator_registry": {
            "definition_count": len(operator_manifest.definitions),
            "manifest_hash": operator_manifest.manifest_hash,
        },
        "recipe_registry": {
            "recipe_count": len(recipe_projection),
            "execution_ready_count": sum(
                bool(item["execution_ready"]) for item in recipe_projection
            ),
            "projection_hash": typed_canonical_hash(recipe_projection),
        },
        "architecture_import_graph": dependency_snapshot,
        "framework_boundary": audit_framework_boundary(root),
    }


def evaluate_capability_containment(
    *,
    repository_root: str | Path,
    baseline: Mapping[str, object],
    current_snapshot: Mapping[str, object],
    manifest: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """返回稳定门禁结果；失败不修改清单或源码。"""

    _validate_baseline(baseline)
    current_manifest = dict(manifest or load_capability_manifest())
    issues: list[dict[str, object]] = []
    boundary = current_snapshot.get("framework_boundary")
    if not isinstance(boundary, Mapping) or boundary.get("status") != "pass":
        codes = sorted({
            str(issue.get("code"))
            for issue in boundary.get("issues", ())
            if isinstance(issue, Mapping)
        }) if isinstance(boundary, Mapping) else []
        issues.append({
            "code": "framework_boundary_failed",
            "capability_id": None,
            "category": None,
            "boundary_issue_codes": codes,
        })
    import_graph = current_snapshot.get("architecture_import_graph")
    cyclic_components = (
        import_graph.get("cyclic_components")
        if isinstance(import_graph, Mapping)
        else None
    )
    if not isinstance(cyclic_components, list) or cyclic_components:
        issues.append({
            "code": "architecture_import_cycle",
            "capability_id": None,
            "category": None,
            "cyclic_components": (
                cyclic_components if isinstance(cyclic_components, list) else []
            ),
        })
    if dict(current_snapshot) != baseline["snapshot"]:
        issues.append({
            "code": "baseline_snapshot_drift",
            "capability_id": None,
            "category": None,
            "sections": sorted(
                key
                for key in set(current_snapshot) | set(baseline["snapshot"])
                if current_snapshot.get(key) != baseline["snapshot"].get(key)
            ),
        })
    expected_manifest_hash = baseline["snapshot"]["capability_manifest"]["payload_hash"]
    if typed_canonical_hash(current_manifest) != expected_manifest_hash:
        issues.append({
            "code": "capability_manifest_drift",
            "capability_id": None,
            "category": None,
        })
    descriptors = {
        item["id"]: item
        for item in current_manifest.get("capabilities", ())
        if isinstance(item, Mapping) and isinstance(item.get("id"), str)
    }
    for rule in baseline["containment"]:
        capability_id = rule["capability_id"]
        descriptor = descriptors.get(capability_id)
        if descriptor is None:
            issues.append({
                "code": "contained_capability_missing",
                "capability_id": capability_id,
                "category": None,
            })
            continue
        state = descriptor.get("state")
        maximum_state = rule["maximum_state"]
        if state not in STATE_RANK or STATE_RANK[state] > STATE_RANK[maximum_state]:
            issues.append({
                "code": "capability_state_exceeds_containment",
                "capability_id": capability_id,
                "category": None,
                "actual_state": state,
                "maximum_state": maximum_state,
            })
        if state != "sealed":
            continue
        registered = set(descriptor.get("verification_anchors", ()))
        receipts: list[dict[str, object]] = []
        seen_anchors: set[str] = set()
        for category in PROMOTION_EVIDENCE_CATEGORIES:
            references = rule["promotion_evidence"][category]
            if not references:
                issues.append({
                    "code": "promotion_evidence_missing",
                    "capability_id": capability_id,
                    "category": category,
                })
                continue
            for reference in references:
                anchor = reference["anchor"]
                if anchor in seen_anchors:
                    issues.append({
                        "code": "promotion_evidence_not_distinct",
                        "capability_id": capability_id,
                        "category": category,
                    })
                seen_anchors.add(anchor)
                if anchor not in registered:
                    issues.append({
                        "code": "promotion_evidence_not_registered",
                        "capability_id": capability_id,
                        "category": category,
                    })
                receipt, receipt_issue = _load_promotion_evidence_receipt(
                    repository_root=repository_root,
                    reference=reference,
                    capability_id=capability_id,
                    category=category,
                    manifest_hash=typed_canonical_hash(current_manifest),
                )
                if receipt_issue is not None:
                    issues.append({
                        "code": receipt_issue,
                        "capability_id": capability_id,
                        "category": category,
                    })
                elif receipt is not None:
                    receipts.append(receipt)
        independent_domains = {
            item["trust_domain"]
            for item in receipts
            if item["category"] == "independent_verification"
        }
        execution_domains = {
            item["trust_domain"]
            for item in receipts
            if item["category"] != "independent_verification"
        }
        if independent_domains & execution_domains:
            issues.append({
                "code": "independent_trust_domain_not_distinct",
                "capability_id": capability_id,
                "category": "independent_verification",
            })
    issues.sort(key=lambda item: (
        str(item.get("capability_id") or ""),
        str(item["code"]),
        str(item.get("category") or ""),
    ))
    body = {
        "contract_version": CONTAINMENT_GATE_VERSION,
        "status": "pass" if not issues else "fail",
        "baseline_hash": baseline["baseline_hash"],
        "manifest_payload_hash": typed_canonical_hash(current_manifest),
        "issues": issues,
    }
    return {**body, "gate_hash": typed_canonical_hash(body)}


def build_evidence_checklist(
    baseline: Mapping[str, object],
) -> dict[str, object]:
    """从基线投影恢复 sealed 状态所需的三类证据。"""

    _validate_baseline(baseline)
    items = [
        {
            "capability_id": rule["capability_id"],
            "maximum_state": rule["maximum_state"],
            "remediation_tasks": list(rule["remediation_tasks"]),
            "required_evidence": {
                category: list(rule["promotion_evidence"][category])
                for category in PROMOTION_EVIDENCE_CATEGORIES
            },
        }
        for rule in baseline["containment"]
    ]
    body = {
        "contract_version": EVIDENCE_CHECKLIST_VERSION,
        "baseline_hash": baseline["baseline_hash"],
        "items": items,
    }
    return {**body, "checklist_hash": typed_canonical_hash(body)}


def _baseline_policy(payload: Mapping[str, object]) -> dict[str, object]:
    expected = {
        "contract_version",
        "audit_sources",
        "audit_findings",
        "containment",
    }
    available = {key: payload[key] for key in expected if key in payload}
    if set(available) != expected:
        raise CapabilityContainmentError("整改基线 policy 字段不闭合")
    if available["contract_version"] != REMEDIATION_BASELINE_VERSION:
        raise CapabilityContainmentError("整改基线版本不受支持")
    _validate_policy_lists(available)
    return available


def _validate_baseline(payload: Mapping[str, object]) -> None:
    expected = {
        "contract_version",
        "audit_sources",
        "audit_findings",
        "containment",
        "snapshot",
        "baseline_hash",
    }
    if not isinstance(payload, Mapping) or set(payload) != expected:
        raise CapabilityContainmentError("整改基线顶层 schema 不匹配")
    _validate_policy_lists(payload)
    body = {key: payload[key] for key in expected if key != "baseline_hash"}
    if payload["baseline_hash"] != typed_canonical_hash(body):
        raise CapabilityContainmentError("整改基线 hash 不匹配")


def _validate_policy_lists(payload: Mapping[str, object]) -> None:
    for field in ("audit_sources", "audit_findings", "containment"):
        if not isinstance(payload.get(field), list) or not payload[field]:
            raise CapabilityContainmentError(f"整改基线 {field} 必须为非空序列")
    capability_ids: list[str] = []
    for rule in payload["containment"]:
        expected = {
            "capability_id",
            "maximum_state",
            "finding_ids",
            "remediation_tasks",
            "promotion_evidence",
        }
        if not isinstance(rule, Mapping) or set(rule) != expected:
            raise CapabilityContainmentError("containment rule schema 不匹配")
        capability_id = rule["capability_id"]
        if not isinstance(capability_id, str) or not capability_id:
            raise CapabilityContainmentError("containment capability_id 无效")
        capability_ids.append(capability_id)
        if rule["maximum_state"] not in STATE_RANK:
            raise CapabilityContainmentError("containment maximum_state 无效")
        for field in ("finding_ids", "remediation_tasks"):
            values = rule[field]
            if not isinstance(values, list) or not values or any(
                not isinstance(item, str) or not item for item in values
            ):
                raise CapabilityContainmentError(f"containment {field} 无效")
        evidence = rule["promotion_evidence"]
        if not isinstance(evidence, Mapping) or set(evidence) != set(
            PROMOTION_EVIDENCE_CATEGORIES
        ):
            raise CapabilityContainmentError("promotion_evidence schema 不匹配")
        for category in PROMOTION_EVIDENCE_CATEGORIES:
            references = evidence[category]
            if not isinstance(references, list):
                raise CapabilityContainmentError("promotion_evidence 引用无效")
            for reference in references:
                if not isinstance(reference, Mapping) or set(reference) != {
                    "anchor",
                    "artifact_sha256",
                }:
                    raise CapabilityContainmentError("promotion_evidence 引用无效")
                if (
                    not isinstance(reference["anchor"], str)
                    or not reference["anchor"]
                    or not _is_sha256(reference["artifact_sha256"])
                ):
                    raise CapabilityContainmentError("promotion_evidence 引用无效")
    if len(capability_ids) != len(set(capability_ids)):
        raise CapabilityContainmentError("containment capability_id 重复")


def _dependency_snapshot(source_root: Path) -> dict[str, object]:
    modules: set[str] = set()
    parsed: list[tuple[str, Path, ast.AST]] = []
    for path in sorted(source_root.rglob("*.py")):
        module = _module_name(source_root, path)
        modules.add(module)
        parsed.append((module, path, ast.parse(path.read_text(encoding="utf-8"))))
    edges: set[tuple[str, str]] = set()
    for module, path, tree in parsed:
        package = module if path.name == "__init__.py" else module.rpartition(".")[0]
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    try:
                        targets = [resolve_name("." * node.level + (node.module or ""), package)]
                    except (ImportError, ValueError):
                        targets = []
                else:
                    targets = [node.module] if node.module else []
            else:
                continue
            for target in targets:
                if target and target.startswith("research_pipeline"):
                    edges.add((module, target))
    graph_nodes = sorted(modules | {target for _, target in edges})
    normalized_edges = [list(item) for item in sorted(edges)]
    cyclic_components = _cyclic_components(graph_nodes, edges)
    return {
        "module_count": len(modules),
        "edge_count": len(normalized_edges),
        "graph_hash": typed_canonical_hash({
            "modules": sorted(modules),
            "edges": normalized_edges,
        }),
        "cyclic_components": cyclic_components,
    }


def _load_promotion_evidence_receipt(
    *,
    repository_root: str | Path,
    reference: Mapping[str, str],
    capability_id: str,
    category: str,
    manifest_hash: str,
) -> tuple[dict[str, object] | None, str | None]:
    root = Path(repository_root).resolve()
    path = (root / reference["anchor"]).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        return None, "promotion_evidence_outside_repository"
    if not path.is_file():
        return None, "promotion_evidence_artifact_missing"
    payload_bytes = path.read_bytes()
    if hashlib.sha256(payload_bytes).hexdigest() != reference["artifact_sha256"]:
        return None, "promotion_evidence_hash_mismatch"
    try:
        payload = json.loads(payload_bytes)
    except json.JSONDecodeError:
        return None, "promotion_evidence_receipt_invalid"
    expected_fields = {
        "contract_version",
        "capability_id",
        "category",
        "status",
        "subject_manifest_hash",
        "producer_id",
        "trust_domain",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        return None, "promotion_evidence_receipt_invalid"
    if (
        payload["contract_version"] != PROMOTION_EVIDENCE_RECEIPT_VERSION
        or payload["capability_id"] != capability_id
        or payload["category"] != category
        or payload["status"] != "pass"
        or payload["subject_manifest_hash"] != manifest_hash
        or not isinstance(payload["producer_id"], str)
        or not payload["producer_id"]
        or not isinstance(payload["trust_domain"], str)
        or not payload["trust_domain"]
    ):
        return None, "promotion_evidence_receipt_invalid"
    return payload, None


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _module_name(source_root: Path, path: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(("research_pipeline", *parts)) if parts else "research_pipeline"


def _cyclic_components(
    nodes: list[str],
    edges: set[tuple[str, str]],
) -> list[list[str]]:
    adjacency = {node: [] for node in nodes}
    for source, target in edges:
        adjacency.setdefault(source, []).append(target)
    for targets in adjacency.values():
        targets.sort()
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    active: set[str] = set()
    components: list[list[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        active.add(node)
        for target in adjacency.get(node, ()):
            if target not in indices:
                visit(target)
                lowlinks[node] = min(lowlinks[node], lowlinks[target])
            elif target in active:
                lowlinks[node] = min(lowlinks[node], indices[target])
        if lowlinks[node] != indices[node]:
            return
        component: list[str] = []
        while True:
            member = stack.pop()
            active.remove(member)
            component.append(member)
            if member == node:
                break
        component.sort()
        if len(component) > 1 or (component[0], component[0]) in edges:
            components.append(component)

    for node in nodes:
        if node not in indices:
            visit(node)
    return sorted(components)


__all__ = [
    "CONTAINMENT_GATE_VERSION",
    "EVIDENCE_CHECKLIST_VERSION",
    "PROMOTION_EVIDENCE_CATEGORIES",
    "PROMOTION_EVIDENCE_RECEIPT_VERSION",
    "REMEDIATION_BASELINE_VERSION",
    "CapabilityContainmentError",
    "build_current_snapshot",
    "build_evidence_checklist",
    "evaluate_capability_containment",
    "load_remediation_baseline",
    "refresh_remediation_baseline",
]
