"""在外层编排中核对公共算子晋级的真实项目与测试证据。"""

from __future__ import annotations

import ast
import os
from pathlib import Path
import subprocess
import sys
from typing import Mapping

from research_pipeline.extensions import (
    CompiledOperatorManifest,
    ExtensionError,
    OperatorDefinition,
)
from research_pipeline.extensions.governance import OperatorPromotionReview
from research_pipeline.packages import compile_research_package, load_research_package
from research_pipeline.runtime.operator_definitions import (
    build_mainline_operator_manifest,
)
from research_pipeline.runtime.operator_promotion import (
    MAINLINE_OPERATOR_PROMOTION_REVIEWS,
    operator_identity,
    validate_operator_promotion_identity,
)
from research_pipeline.runtime.operator_registry_builder import (
    build_operator_registry_from_manifest,
)


def validate_mainline_operator_promotion_evidence(
    manifest: CompiledOperatorManifest | None = None,
    *,
    project_roots: Mapping[str, str | Path] | None = None,
    evidence_root: str | Path | None = None,
) -> None:
    """从包编译层验证所有已批准晋级记录，不反向污染 Runtime。"""

    current = manifest or build_mainline_operator_manifest()
    approved = tuple(
        review for review in MAINLINE_OPERATOR_PROMOTION_REVIEWS if review.is_public
    )
    if not approved:
        return
    if not project_roots or evidence_root is None:
        raise ExtensionError("晋级审查必须显式提供复用项目目录和测试证据根")
    definitions = {operator_identity(item): item for item in current.definitions}
    admission = build_operator_registry_from_manifest(current)
    for review in approved:
        identity = (
            review.operator_id,
            review.operator_version,
            review.implementation_id,
            review.module_name,
        )
        definition = definitions.get(identity)
        if definition is None:
            raise ExtensionError(
                f"approved promotion record 没有对应公共定义: "
                f"{review.operator_id}@{review.operator_version}"
            )
        validate_approved_operator_promotion(
            review,
            definition,
            project_roots=project_roots,
            evidence_root=evidence_root,
            admission=admission,
        )


def validate_approved_operator_promotion(
    review: OperatorPromotionReview,
    definition: OperatorDefinition,
    *,
    project_roots: Mapping[str, str | Path],
    evidence_root: str | Path,
    admission: object,
) -> None:
    validate_operator_promotion_identity(review, definition)
    if not review.is_public:
        raise ExtensionError("只有 approved promotion record 可以进入公共 manifest")
    root = Path(evidence_root).resolve()
    if not root.is_dir():
        raise ExtensionError("晋级测试证据根不存在")
    oracle_refs = set(review.oracle_ids)
    attack_refs = set(review.attack_ids)
    if oracle_refs.intersection(attack_refs):
        raise ExtensionError("oracle 与攻击测试不得引用同一证据节点")
    for reference in (*review.oracle_ids, *review.attack_ids):
        _validate_evidence_reference(root, reference, review.operator_id)
    invalid_reuse = next(
        (
            project_id
            for project_id in review.reuse_projects
            if any(
                marker in project_id.lower()
                for marker in ("fixture", "reference", "example")
            )
        ),
        None,
    )
    if invalid_reuse is not None:
        raise ExtensionError(
            f"晋级复用项目不能使用 fixture/reference/example: {invalid_reuse}"
        )
    graph_signatures = [
        _validate_reuse_project(
            project_id,
            project_roots=project_roots,
            operator_id=review.operator_id,
            operator_version=review.operator_version,
            admission=admission,
        )
        for project_id in review.reuse_projects
    ]
    if len(set(graph_signatures)) != len(graph_signatures):
        raise ExtensionError("晋级复用项目的算子图同形，不能证明异构复用")
    references = (*review.oracle_ids, *review.attack_ids)
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    environment.pop("PYTEST_ADDOPTS", None)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *references],
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ExtensionError("晋级 oracle/攻击测试无法执行") from exc
    if result.returncode != 0:
        raise ExtensionError("晋级 oracle/攻击测试未通过")


def _validate_reuse_project(
    project_id: str,
    *,
    project_roots: Mapping[str, str | Path],
    operator_id: str,
    operator_version: str,
    admission: object,
) -> tuple[tuple[str, str], ...]:
    lowered = project_id.lower()
    if any(marker in lowered for marker in ("fixture", "reference", "example")):
        raise ExtensionError(
            f"晋级复用项目不能使用 fixture/reference/example: {project_id}"
        )
    declared_root = project_roots.get(project_id)
    if declared_root is None:
        raise ExtensionError(f"晋级复用项目缺少显式目录: {project_id}")
    project_root = Path(declared_root).resolve()
    if not project_root.is_dir():
        raise ExtensionError(f"晋级复用项目不存在: {project_id}")
    try:
        package = load_research_package(project_root)
    except Exception as exc:
        raise ExtensionError(f"晋级复用项目无法加载: {project_id}") from exc
    if package.package_slug != project_id:
        raise ExtensionError(f"晋级复用项目目录与 package_slug 不一致: {project_id}")
    graph = package.spec_payload.get("graph")
    nodes = graph.get("nodes") if isinstance(graph, Mapping) else None
    if not isinstance(nodes, (list, tuple)):
        raise ExtensionError(f"晋级复用项目缺少声明式算子图: {project_id}")
    signature = tuple(
        sorted(
            (str(item.get("operator_id")), str(item.get("operator_version")))
            for item in nodes
            if isinstance(item, Mapping)
        )
    )
    if (operator_id, operator_version) not in signature:
        raise ExtensionError(f"晋级复用项目未实际消费目标算子: {project_id}")
    try:
        compile_research_package(package, admission=admission)
    except Exception as exc:
        raise ExtensionError(
            f"晋级复用项目无法由当前 registry 编译: {project_id}"
        ) from exc
    return signature


def _validate_evidence_reference(
    evidence_root: Path,
    reference: str,
    operator_id: str,
) -> None:
    if reference.count("::") != 1:
        raise ExtensionError(f"晋级证据必须引用证据根内 pytest 节点: {reference}")
    relative_path, test_name = reference.split("::", 1)
    path = (evidence_root / relative_path).resolve()
    try:
        path.relative_to(evidence_root)
    except ValueError as exc:
        raise ExtensionError(f"晋级证据越出显式证据根: {reference}") from exc
    if not path.is_file():
        raise ExtensionError(f"晋级证据文件不存在: {reference}")
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError) as exc:
        raise ExtensionError(f"晋级证据无法解析: {reference}") from exc
    function = next(
        (
            item
            for item in tree.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
            and item.name == test_name
        ),
        None,
    )
    if function is None:
        raise ExtensionError(f"晋级证据测试节点不存在: {reference}")
    segment = ast.get_source_segment(source, function) or ""
    if operator_id not in segment:
        raise ExtensionError(f"晋级证据没有绑定目标 operator: {reference}")


__all__ = [
    "validate_approved_operator_promotion",
    "validate_mainline_operator_promotion_evidence",
]
