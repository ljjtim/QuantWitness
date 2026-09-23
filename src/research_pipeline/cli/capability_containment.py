"""在最外层组装整改基线所需的跨层只读快照。"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from typing import Mapping

from research_pipeline.cli.parser import FINAL_COMMANDS, build_parser
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


def evaluate_capability_containment(
    *,
    repository_root: str | Path,
    baseline: Mapping[str, object],
    manifest: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """以当前源码快照执行能力止血门禁。"""

    return _evaluate_capability_containment(
        repository_root=repository_root,
        baseline=baseline,
        current_snapshot=build_current_snapshot(repository_root),
        manifest=manifest,
    )


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
