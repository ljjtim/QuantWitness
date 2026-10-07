"""不授予运行资格的 ResearchPackage lint 结果合同。"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Mapping

from research_pipeline.platform import MainlineError, typed_canonical_hash

from .models import ResearchPackage, ResearchPackageError
from .plan_contracts import OperatorGraphPlan


LINT_REPORT_VERSION = "research-package-lint-v1"


def diagnose_research_package(root: str | Path) -> list[dict[str, str]]:
    """复用严格读取的局部校验，收集不依赖完整研究包的声明问题。"""
    from . import store
    from .models import _freeze, _require_text

    directory = Path(root).resolve()
    issues: list[dict[str, str]] = []

    def check(file: str, field: str, validate: Callable[[], object], *, code: str = "package_field_invalid") -> bool:
        try:
            validate()
        except (MainlineError, TypeError, KeyError) as exc:
            issues.append({
                "code": code,
                "file": file,
                "field": field,
                "message": str(exc),
                "action": f"修正 {file} 的 {field} 声明后重新运行 package lint。",
            })
            return False
        return True

    def mapping(file: str, field: str, value: object, expected: set[str]) -> bool:
        try:
            store._typed_mapping(value, expected, field)
        except ResearchPackageError:
            if not isinstance(value, dict):
                return check(file, field, lambda: store._typed_mapping(value, expected, field))
            for key in sorted(expected - set(value)):
                issues.append({
                    "code": "package_field_missing", "file": file,
                    "field": f"{field}.{key}" if field else key,
                    "message": f"缺少必填字段 {key}",
                    "action": f"在 {file} 中补齐 {field + '.' if field else ''}{key} 后重新运行 package lint。",
                })
            for key in sorted(set(value) - expected, key=str):
                issues.append({
                    "code": "package_field_unknown", "file": file,
                    "field": f"{field}.{key}" if field else str(key),
                    "message": f"未知字段 {key}",
                    "action": f"删除或修正 {file} 中的未知字段 {key} 后重新运行 package lint。",
                })
            return False
        return True

    payloads: dict[str, dict[str, object]] = {}
    for file in store.PACKAGE_FILES:
        try:
            payloads[file] = store._load_yaml(directory, file)
        except ResearchPackageError as exc:
            issues.append({
                "code": "package_yaml_invalid", "file": file, "field": "$",
                "message": str(exc),
                "action": f"补齐或修正 {file}，确保 YAML 顶层是映射后重新运行 package lint。",
            })

    package = payloads.get("package.yaml")
    if package is not None:
        file = "package.yaml"
        mapping(file, "", package, store._PACKAGE_KEYS)
        if package.get("builder_id") == store.OPERATOR_GRAPH_BUILDER_ID:
            check(file, "$layout", lambda: store._validate_declarative_layout(directory), code="package_layout_invalid")
        for field in sorted(store._PACKAGE_KEYS - {"metric_contract", "claim_contract"}):
            if field in package:
                check(file, field, lambda: _require_text(package[field], field))
        for field, keys, loader in (
            ("metric_contract", store._METRIC_KEYS, store._load_metric_contract),
            ("claim_contract", store._CLAIM_KEYS, store._load_claim_contract),
        ):
            if field in package and mapping(file, field, package[field], keys):
                check(file, field, lambda: loader(package[field]))

    for file, field, loader in (
        ("sources/sources.yaml", "sources", store._load_source),
        ("localization.yaml", "decisions", store._load_localization),
    ):
        payload = payloads.get(file)
        if payload is None:
            continue
        mapping(file, "", payload, {field})
        if field not in payload:
            continue
        if check(file, field, lambda: store._typed_list(payload[field], f"{file}.{field}")):
            for index, item in enumerate(payload[field]):
                check(file, f"{field}[{index}]", lambda: loader(item))

    spec = payloads.get("spec/research.yaml")
    if spec is not None:
        file = "spec/research.yaml"
        # 仅解释已有纯声明合同，不创建半完整 ResearchPackage 或注册表。
        check(file, "$", lambda: _freeze(spec))
        from .compiler import _sequence, _text, compile_query_requests

        as_of_valid = check(file, "as_of", lambda: _text(spec.get("as_of"), "spec.as_of"))
        requests_valid = check(file, "requests", lambda: _sequence(spec.get("requests"), "spec.requests"))
        if as_of_valid and requests_valid:
            check(file, "requests", lambda: compile_query_requests(spec["requests"], as_of=spec["as_of"]))
        if package is not None and package.get("builder_id") == store.OPERATOR_GRAPH_BUILDER_ID:
            from research_pipeline.platform.operator_contracts import OperatorGraphRecipe
            from research_pipeline.research.semantics import ResearchSemantics

            if "graph" in spec:
                check(file, "graph", lambda: OperatorGraphRecipe.from_dict(spec["graph"]))
            if "research_semantics" in spec and spec["research_semantics"] is not None:
                check(file, "research_semantics", lambda: ResearchSemantics.from_dict(spec["research_semantics"]))
    return sorted(issues, key=lambda issue: (issue["file"], issue["field"], issue["code"], issue["message"]))


def validate_lint_declarations(root: str | Path) -> None:
    """聚合失败只用于修改声明，不授予加载、准入或运行资格。"""
    issues = diagnose_research_package(root)
    if issues:
        error = ResearchPackageError("研究包声明存在缺口：" + "；".join(issue["message"] for issue in issues))
        error.failure_payload = {
            "issues": issues,
            "execution_ready": False,
            "checks": {
                "schema": {"status": "fail"},
                "compilation": {"status": "pending", "reason": "声明缺口修正后才能严格加载和编译。"},
            },
        }
        raise error


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
        "required_inputs": sorted(missing),
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


__all__ = ["LINT_REPORT_VERSION", "build_lint_report", "diagnose_research_package", "validate_lint_declarations"]
