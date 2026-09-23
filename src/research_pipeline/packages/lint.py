"""不授予运行资格的 ResearchPackage lint 结果合同。"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash

from .models import ResearchPackage, ResearchPackageError
from .plan_contracts import OperatorGraphPlan


LINT_REPORT_VERSION = "research-package-lint-v1"


def build_lint_report(
    package: ResearchPackage,
    plan: OperatorGraphPlan,
    *,
    catalog_lock: str | Path | None = None,
    source_verification: Mapping[str, object],
    resource_summary: Mapping[str, object],
) -> dict[str, object]:
    """一次返回本地编译结果、准入缺口和已声明资源预算。"""
    missing = {"data_source", "output"}
    if catalog_lock is None:
        field_check: dict[str, object] = {
            "status": "pending",
            "missing": ["catalog_lock"],
        }
        missing.add("catalog_lock")
    else:
        field_check = _check_catalog_fields(plan, catalog_lock)
    checks = {
        "schema": {"status": "pass", "package_hash": package.package_hash},
        "fields": field_check,
        "operators": {
            "status": "pass",
            "registry_hash": plan.registry_hash,
            "recipe_hash": plan.recipe.recipe_hash,
            "operator_count": len(plan.recipe.nodes),
        },
        "metrics": {
            "status": "pass",
            "proof_hashes": [item.proof_digest for item in plan.metric_proofs],
        },
        "result": {
            "status": "pass",
            "metric_contract_hash": package.metric_contract.contract_hash,
            "claim_contract_hash": package.claim_contract.contract_hash,
        },
        "source": {
            "status": "pass",
            **dict(source_verification),
        },
        "resources": dict(resource_summary),
    }
    values = {
        "status": "linted",
        "execution_ready": False,
        "package_id": package.package_id,
        "package_hash": package.package_hash,
        "package_plan_hash": plan.plan_hash,
        "metric_contract_hash": package.metric_contract.contract_hash,
        "claim_contract_hash": package.claim_contract.contract_hash,
        "source_provenance_hash": package.source_provenance_hash,
        "source_verification": dict(source_verification),
        "query_count": len(plan.queries),
        "checks": checks,
        "missing_requirements": sorted(missing),
        "next_command": _next_admit_command(package),
        "contract_version": LINT_REPORT_VERSION,
    }
    return {**values, "lint_hash": typed_canonical_hash(values)}


def _check_catalog_fields(
    plan: OperatorGraphPlan,
    catalog_lock: str | Path,
) -> dict[str, object]:
    from research_pipeline.data_plane.service import load_compiled_catalog

    catalog = load_compiled_catalog(catalog_lock)
    checked: list[dict[str, object]] = []
    for request_id, query in zip(plan.request_ids, plan.queries, strict=True):
        dataset = catalog.datasets.get(query.dataset_id)
        if dataset is None or dataset.get("dataset_version") != query.dataset_version:
            raise ResearchPackageError(
                f"lint 字段检查找不到 dataset/version: {query.dataset_id}@{query.dataset_version}"
            )
        referenced = {
            *query.field_ids,
            *(item.field_id for item in query.filters),
            *(item.field_id for item in query.sort),
        }
        missing = sorted(referenced - set(dataset["fields"]))
        if missing:
            raise ResearchPackageError(
                f"lint 字段检查发现 Catalog 缺失字段: {request_id}={missing}"
            )
        checked.append(
            {
                "request_id": request_id,
                "dataset_id": query.dataset_id,
                "dataset_version": query.dataset_version,
                "field_ids": sorted(referenced),
            }
        )
    return {
        "status": "pass",
        "catalog_hash": catalog.catalog_hash,
        "requests": checked,
    }


def _next_admit_command(
    package: ResearchPackage,
) -> str:
    return (
        "python -m research_pipeline package admit "
        f"--package <{package.package_id}-目录> "
        "--catalog-lock <Catalog-Lock> --data-db <只读DuckDB> "
        "--output <新的已准入计划目录> --json"
    )


__all__ = ["LINT_REPORT_VERSION", "build_lint_report"]
