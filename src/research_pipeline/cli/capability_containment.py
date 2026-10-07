"""在最外层组装整改基线所需的跨层只读快照。"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
from typing import Mapping

from research_pipeline.cli.parser import FINAL_COMMANDS, build_parser
from research_pipeline.operations.capabilities import (
    CapabilityDiscoveryError, load_capability_manifest,
    validate_capability_claims, validate_capability_release_evidence,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.operations.capability_containment import (
    CONTAINMENT_GATE_VERSION,
    EVIDENCE_CHECKLIST_VERSION,
    PROMOTION_EVIDENCE_CATEGORIES,
    PROMOTION_EVIDENCE_RECEIPT_VERSION,
    REMEDIATION_BASELINE_VERSION,
    CapabilityContainmentError,
    build_current_snapshot as _build_current_snapshot,
    build_evidence_checklist,
    evaluate_capability_containment as _evaluate_capability_containment,
    load_remediation_baseline,
    refresh_remediation_baseline as _refresh_remediation_baseline,
)
from research_pipeline.packages.reference_transform_recipes import (
    build_reference_transform_recipes,
)


_NODE_LOCAL_MUTATION_TESTS = (
    "research_pipeline/tests/test_framework_intrusion_gate.py::test_renamed_specialization_still_fails",
    "research_pipeline/tests/test_heterogeneous_project_conformance.py::test_core_discovery_and_generic_package_compile_without_project_tree",
    "research_pipeline/tests/test_operator_governance.py::test_renamed_public_semantic_identity_requires_approved_promotion",
    "research_pipeline/tests/test_operator_governance.py::test_builtin_inventory_expansion_cannot_self_approve",
)


def _validate_public_semantic_registries() -> None:
    from research_pipeline.evidence.verification_result import (
        validate_builtin_verification_semantics,
    )
    from research_pipeline.platform.metric_contracts import (
        build_mainline_metric_registry,
    )

    build_reference_transform_recipes()
    build_mainline_metric_registry()
    validate_builtin_verification_semantics()


def _validate_node_local_mutations(repository_root: str | Path) -> None:
    root = Path(repository_root).resolve()
    source = root / "research_pipeline/src"
    environment = dict(os.environ)
    environment.pop("PYTEST_ADDOPTS", None)
    environment["PYTHONPATH"] = os.pathsep.join((
        str(source), environment.get("PYTHONPATH", ""),
    ))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            *_NODE_LOCAL_MUTATION_TESTS,
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        raise CapabilityContainmentError(
            "node_local_mutation_gate_failed: " + result.stdout[-2000:] + result.stderr[-1000:]
        )


def _recipe_projection() -> list[dict[str, object]]:
    return [
        {
            "recipe_id": recipe.reference_id,
            "research_kind": recipe.research_kind,
            "execution_ready": recipe.execution_ready,
        }
        for recipe in build_reference_transform_recipes()
    ]


def build_current_snapshot(repository_root: str | Path) -> dict[str, object]:
    """从公开 CLI 与正式注册表组装确定性快照。"""

    _validate_public_semantic_registries()

    return _build_current_snapshot(
        repository_root,
        public_commands=FINAL_COMMANDS,
        public_help_text=build_parser().format_help(),
        recipe_projection=_recipe_projection(),
    )


def refresh_remediation_baseline(
    repository_root: str | Path,
    baseline_policy: Mapping[str, object],
    *,
    promotion_project_roots: Mapping[str, str | Path] | None = None,
    promotion_evidence_root: str | Path | None = None,
) -> dict[str, object]:
    """保留人工审查策略并刷新当前跨层快照。"""

    from research_pipeline.cli.operator_promotion import (
        validate_mainline_operator_promotion_evidence,
    )

    validate_mainline_operator_promotion_evidence(
        project_roots=promotion_project_roots,
        evidence_root=promotion_evidence_root,
    )
    _validate_public_semantic_registries()
    current_snapshot = _build_current_snapshot(
        repository_root,
        public_commands=FINAL_COMMANDS,
        public_help_text=build_parser().format_help(),
        recipe_projection=_recipe_projection(),
    )
    boundary = current_snapshot["framework_boundary"]
    if boundary["status"] != "pass":
        codes = sorted({str(issue["code"]) for issue in boundary["issues"]})
        raise CapabilityContainmentError(
            f"framework structural gate 未通过: {', '.join(codes)}"
        )
    cyclic_components = current_snapshot["architecture_import_graph"][
        "cyclic_components"
    ]
    if cyclic_components:
        raise CapabilityContainmentError(
            f"architecture import cycle gate 未通过: {cyclic_components}"
        )
    _validate_node_local_mutations(repository_root)
    return _refresh_remediation_baseline(
        repository_root,
        baseline_policy,
        public_commands=FINAL_COMMANDS,
        public_help_text=build_parser().format_help(),
        recipe_projection=_recipe_projection(),
        current_snapshot=current_snapshot,
    )


def evaluate_public_capability_release(
    *,
    project_root: str | Path,
    baseline: Mapping[str, object] | None = None,
    manifest: Mapping[str, object] | None = None,
    release_evidence_root: str | Path | None = None,
    release_candidate_id: str | None = None,
    as_of: str | None = None,
) -> dict[str, object]:
    """公开源码直接消费显式晋级声明，不依赖私有基线的位置。"""

    current = load_capability_manifest() if manifest is None else manifest
    issues: list[dict[str, object]] = []
    try:
        sealed = validate_capability_claims(current)
    except CapabilityDiscoveryError as exc:
        return {"status": "fail", "issues": [{"code": "capability_claim_invalid", "message": str(exc)}]}
    if baseline is not None:
        build_evidence_checklist(baseline)
        ranks = {"planned": 0, "local_only": 1, "sealed": 2}
        descriptors = {item["id"]: item for item in current["capabilities"]}
        for rule in baseline["containment"]:
            descriptor = descriptors.get(rule["capability_id"])
            if descriptor is None or ranks[descriptor["state"]] > ranks[rule["maximum_state"]]:
                issues.append({"code": "capability_state_exceeds_containment", "capability_id": rule["capability_id"]})
    if sealed:
        if baseline is None:
            issues.append({"code": "promotion_authorization_missing", "capability_ids": sealed})
        else:
            rules = {item["capability_id"]: item for item in baseline["containment"]}
            for identity in sealed:
                if identity not in rules:
                    issues.append({"code": "promotion_authorization_missing", "capability_id": identity})
            if baseline["snapshot"]["capability_manifest"]["payload_hash"] != typed_canonical_hash(current):
                issues.append({"code": "capability_manifest_drift"})
        if release_evidence_root is None or not release_candidate_id:
            issues.append({"code": "promotion_release_evidence_missing"})
        if not issues:
            try:
                validate_capability_release_evidence(
                    project_root=Path(project_root).resolve(),
                    evidence_root=Path(release_evidence_root).resolve(),
                    manifest=current, rules=baseline["containment"],
                    release_candidate_id=release_candidate_id,
                    as_of=as_of or datetime.now(timezone.utc).isoformat(),
                )
            except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                issues.append({"code": "promotion_release_evidence_invalid", "message": str(exc)})
    return {"status": "fail" if issues else "pass", "sealed_capabilities": sealed, "issues": issues}


def evaluate_capability_containment(
    *,
    repository_root: str | Path,
    baseline: Mapping[str, object],
    manifest: Mapping[str, object] | None = None,
    release_evidence_root: str | Path | None = None,
    release_candidate_id: str | None = None,
    as_of: str | None = None,
) -> dict[str, object]:
    """保留源码快照门禁，并要求 sealed 能力消费实际发布证据。"""

    result = _evaluate_capability_containment(
        repository_root=repository_root,
        baseline=baseline,
        current_snapshot=build_current_snapshot(repository_root),
        manifest=manifest,
    )
    if result["status"] == "fail" and release_evidence_root is None:
        return result
    public = evaluate_public_capability_release(
        project_root=Path(repository_root) / "research_pipeline", baseline=baseline,
        manifest=manifest, release_evidence_root=release_evidence_root,
        release_candidate_id=release_candidate_id, as_of=as_of,
    )
    issues = result["issues"]
    if release_evidence_root is not None:
        # 独立目录的 v2 收据由实际证据消费者复验，源码和状态上限门禁照常保留。
        issues = [item for item in issues if not (
            item["code"].startswith("promotion_evidence_")
            or item["code"] == "independent_trust_domain_not_distinct"
        )]
    if public["issues"]:
        issues = [*issues, *public["issues"]]
    body = {key: value for key, value in result.items() if key != "gate_hash"}
    body.update(status="fail" if issues else "pass", issues=issues)
    return {**body, "gate_hash": typed_canonical_hash(body)}


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
    "evaluate_public_capability_release",
    "load_remediation_baseline",
    "refresh_remediation_baseline",
]
