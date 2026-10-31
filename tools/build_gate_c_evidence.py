"""从四项正式 Result 生成计划、窗口、只读源库和指标数值验收证据。

输入采用 research-gate-c-input-v2，路径相对输入文件解析。data_scope 显式
选择 synthetic 或 real；每项研究用 expected_metrics 声明预期数值、单位和
样本量。合成研究只证明工程与确定性数值验收，候选绑定由命令行参数提供。
"""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if os.environ.get("RESEARCH_PIPELINE_ACCEPTANCE_RUNTIME") != "installed" and str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import research_pipeline  # noqa: E402
from research_pipeline.platform import typed_canonical_hash  # noqa: E402
from research_pipeline.platform.operator_contracts import OperatorGraphRecipe  # noqa: E402
from research_pipeline.results import ResultStore, metrics_from_snapshot  # noqa: E402

from release_evidence_binding import release_evidence_binding  # noqa: E402


INPUT_VERSION = "research-gate-c-input-v2"
EVIDENCE_VERSION = "research-gate-c-evidence-v3"
METRIC_ABSOLUTE_TOLERANCE = 1e-12


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"JSON 无法读取: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON 顶层必须是对象: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text(item: dict[str, object], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Gate C 缺少非空字段: {field}")
    return value


def _path(item: dict[str, object], field: str, base: Path) -> Path:
    return (base / _text(item, field)).resolve()


def _numeric_acceptance(item: dict[str, object], metrics: tuple) -> list[dict[str, object]]:
    expected = item.get("expected_metrics")
    if not isinstance(expected, list) or not expected or any(not isinstance(row, dict) for row in expected):
        raise ValueError("Gate C expected_metrics 必须是非空指标预期列表")
    by_ref = {}
    for row in expected:
        metric_ref = _text(row, "metric_ref")
        value = row.get("value")
        sample_size = row.get("sample_size")
        if metric_ref in by_ref:
            raise ValueError("Gate C expected_metrics 指标引用重复")
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError("Gate C 指标预期值必须是有限数值")
        if type(sample_size) is not int or sample_size <= 0:
            raise ValueError("Gate C 指标预期样本量必须是正整数")
        _text(row, "unit")
        by_ref[metric_ref] = row
    if set(by_ref) != {metric.metric_ref for metric in metrics}:
        raise ValueError("Gate C 预期指标集合与正式 Result 不一致")
    accepted = []
    for metric in metrics:
        row = by_ref[metric.metric_ref]
        if metric.unit != row["unit"] or metric.sample_size != row["sample_size"]:
            raise ValueError(f"Gate C 指标单位或样本量不一致: {metric.metric_ref}")
        if not math.isclose(metric.value, row["value"], rel_tol=0, abs_tol=METRIC_ABSOLUTE_TOLERANCE):
            raise ValueError(f"Gate C 指标数值与预期不一致: {metric.metric_ref}")
        accepted.append({
            **metric.to_dict(), "expected_value": row["value"],
            "absolute_error": abs(metric.value - row["value"]),
            "absolute_tolerance": METRIC_ABSOLUTE_TOLERANCE,
        })
    return accepted


def _request_windows(
    plan: dict[str, object], maximum_days: int, study_request_ids: set[str],
) -> list[dict[str, object]]:
    requests = plan.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("Gate C 正式计划必须包含数据请求")
    windows = []
    observed = set()
    for item in requests:
        if not isinstance(item, dict) or not isinstance(item.get("query"), dict):
            raise ValueError("Gate C 计划请求 schema 无效")
        query = item["query"]
        request_id = _text(item, "request_id")
        if request_id in observed:
            raise ValueError("Gate C 计划 request_id 重复")
        observed.add(request_id)
        time_range = query.get("time_range")
        if not isinstance(time_range, dict):
            raise ValueError("Gate C 请求缺少显式时间窗口")
        try:
            start = date.fromisoformat(str(time_range["start"]))
            end = date.fromisoformat(str(time_range["end"]))
        except (KeyError, ValueError) as exc:
            raise ValueError("Gate C 请求时间窗口无效") from exc
        is_study_window = request_id in study_request_ids
        days = (end - start).days + 1
        if days <= 0 or (is_study_window and days > maximum_days):
            raise ValueError("Gate C 研究窗口超出批准范围或日期倒置")
        windows.append({
            "request_id": request_id,
            "dataset_id": _text(query, "dataset_id"),
            "start": start.isoformat(), "end": end.isoformat(),
            "calendar_days": days, "study_window": is_study_window,
        })
    if not study_request_ids or not study_request_ids <= observed:
        raise ValueError("Gate C 小窗口请求集合与批准输入不一致")
    return windows


def _database_evidence(item: dict[str, object], *, scope: str, result_id: str) -> dict[str, object]:
    evidence = item.get("database_evidence")
    if not isinstance(evidence, dict):
        raise ValueError("Gate C 缺少只读数据库证据")
    if (
        evidence.get("data_scope") != scope
        or evidence.get("result_id") != result_id
        or evidence.get("read_only") is not True
        or evidence.get("database_unchanged") is not True
    ):
        raise ValueError("Gate C 数据库只读证据与结果或数据范围不一致")
    _text(evidence, "database_path")
    before, after = evidence.get("before"), evidence.get("after")
    if (
        not isinstance(before, dict) or not isinstance(after, dict)
        or set(before) != {"size_bytes", "mtime_ns"}
        or before != after
        or any(type(value) is not int or value <= 0 for value in before.values())
    ):
        raise ValueError("Gate C 数据库前后大小和修改时间证据无效或发生变化")
    return evidence


def _plan_identity_payload(plan: dict[str, object]) -> dict[str, object]:
    """复验完整Recipe，再使用正式计划的已验证Recipe引用投影。"""
    recipe_payload = dict(plan["recipe"])
    declared_recipe_hash = recipe_payload.pop("recipe_hash", None)
    recipe = OperatorGraphRecipe.from_dict(recipe_payload)
    if recipe.recipe_hash != declared_recipe_hash:
        raise ValueError("Gate C plan recipe 内容与 recipe_hash 不一致")
    return {
        **{key: value for key, value in plan.items() if key != "plan_hash"},
        "recipe": recipe.recipe_hash,
        "identity_projection": "validated-recipe-ref-v1",
    }


def _validate_reference(
    item: dict[str, object], maximum_days: int, *, scope: str, base: Path,
) -> dict[str, object]:
    plan_path = _path(item, "plan", base)
    result_directory = _path(item, "result_directory", base)
    result_root = _path(item, "result_store", base)
    plan = _load_json(plan_path)
    if plan.get("contract_version") != "research-operator-graph-package-v2":
        raise ValueError("Gate C 必须使用正式算子图计划")
    plan_hash = plan.get("plan_hash")
    if plan_hash != typed_canonical_hash(_plan_identity_payload(plan)):
        raise ValueError("Gate C plan 内容与 plan_hash 不一致")
    recipe = plan.get("recipe")
    if not isinstance(recipe, dict) or recipe.get("graph_id") != _text(item, "expected_graph_id"):
        raise ValueError("Gate C DAG 身份与批准输入不一致")

    # 数值验收使用同一完整快照，诊断表与支持文件也检查实际内容。
    snapshot = ResultStore(result_root, create=False).open_snapshot(result_directory)
    bundle = snapshot.bundle
    if bundle.contract_version != "research-result-v4" or bundle.status != "finalized":
        raise ValueError("Gate C 只接受已 finalize 的 research-result-v4")
    if bundle.run.status != "succeeded":
        raise ValueError("Gate C 只接受执行成功的正式 Result")
    if bundle.result_id != _text(item, "expected_result_id"):
        raise ValueError("Gate C Result 身份与批准输入不一致")
    if (
        bundle.plan_hash != plan_hash or bundle.package_hash != plan.get("package_hash")
        or bundle.result_spec.to_dict() != plan.get("result_spec")
        or bundle.run.fixed_clock != plan.get("fixed_clock")
    ):
        raise ValueError("Gate C plan 与 Result 冻结合同不一致")
    namespace = result_directory.parent
    if {path.name for path in namespace.iterdir() if path.is_dir()} != {bundle.result_id}:
        raise ValueError("每个成功 Run 必须恰好只有一个正式 ResultBundle")

    metrics = _numeric_acceptance(item, metrics_from_snapshot(snapshot))

    request_ids = item.get("study_request_ids")
    if (
        not isinstance(request_ids, list) or not request_ids
        or any(not isinstance(value, str) or not value for value in request_ids)
        or len(request_ids) != len(set(request_ids))
    ):
        raise ValueError("Gate C study_request_ids 必须是非空且唯一的请求列表")
    if not set(request_ids) <= set(bundle.formal_input_request_ids):
        raise ValueError("Gate C 研究窗口必须绑定 Result 的正式输入")
    windows = _request_windows(plan, maximum_days, set(request_ids))
    if {row["request_id"] for row in windows} != {row.request_id for row in bundle.input_revisions}:
        raise ValueError("Gate C plan 请求与 Result 输入修订不一致")
    if scope == "real" and any(str(row["dataset_id"]).startswith("quantwitness.synthetic_") for row in windows):
        raise ValueError("Gate C 公开合成数据不得声明为真实市场证据")
    database = _database_evidence(item, scope=scope, result_id=bundle.result_id)
    return {
        "reference": _text(item, "reference"), "dag_family": _text(item, "dag_family"),
        "graph_id": recipe["graph_id"], "plan_hash": bundle.plan_hash,
        "package_hash": bundle.package_hash, "project_id": bundle.project_id,
        "run_id": bundle.run_id, "result_id": bundle.result_id,
        "result_contract": bundle.contract_version, "result_bundle_count_for_run": 1,
        "table_count": len(bundle.tables), "input_revision_count": len(bundle.input_revisions),
        "request_windows": windows,
        "execution_status": bundle.run.status,
        "limitations": list(bundle.run.limitations),
        "metric_count": len(metrics), "metrics": metrics,
        "data_scope": scope, "database_evidence": database,
    }


def build_gate_c_evidence(
    *, input_path: Path, output: Path,
    release_candidate_id: str | None = None, build_manifest_path: Path | None = None,
) -> dict[str, object]:
    if output.exists():
        raise ValueError("Gate C 输出目录必须不存在")
    inputs = _load_json(input_path)
    if inputs.get("contract_version") != INPUT_VERSION:
        raise ValueError("Gate C input 必须使用 v2；历史 v1 收据不能重新贴签")
    scope = inputs.get("data_scope")
    if scope not in {"synthetic", "real"}:
        raise ValueError("Gate C 必须显式声明 synthetic 或 real 数据范围")
    references = inputs.get("references")
    if not isinstance(references, list) or len(references) != 4 or any(not isinstance(item, dict) for item in references):
        raise ValueError("Gate C 必须完整覆盖四项研究")
    if len({_text(item, "reference") for item in references}) != 4:
        raise ValueError("Gate C 四项研究 reference 必须唯一")
    maximum_days = inputs.get("maximum_window_days")
    minimum_families = inputs.get("minimum_dag_families")
    if type(maximum_days) is not int or maximum_days <= 0:
        raise ValueError("Gate C maximum_window_days 必须是正整数")
    if type(minimum_families) is not int or not 3 <= minimum_families <= 4:
        raise ValueError("Gate C 至少需要三种不同 DAG family")
    binding = release_evidence_binding(
        release_candidate_id=release_candidate_id,
        build_manifest_path=build_manifest_path, project=PROJECT_ROOT,
    )
    results = [_validate_reference(item, maximum_days, scope=scope, base=input_path.resolve().parent) for item in references]
    families = sorted({str(item["dag_family"]) for item in results})
    if len(families) < minimum_families or len({item["graph_id"] for item in results}) < minimum_families:
        raise ValueError("Gate C 至少需要三种不同 DAG family 和对应实际图")
    if len({(item["project_id"], item["run_id"]) for item in results}) != 4:
        raise ValueError("Gate C 四项研究必须来自四个独立 Run")
    evidence = {
        "contract_version": EVIDENCE_VERSION, "gate_id": "gate-c", "status": "pass", **binding,
        "data_scope": scope,
        "acceptance_scope": "engineering" if scope == "synthetic" else "real_market_window",
        "runtime_origin": str(Path(research_pipeline.__file__).resolve()),
        "input_hash": _sha256(input_path), "reference_count": len(results),
        "dag_family_count": len(families), "dag_families": families,
        "all_results_contract": "research-result-v4",
        "acceptance_basis": "result_contract_and_metric_values",
        "each_successful_run_has_one_result_bundle": True,
        "accepted_metric_count": sum(item["metric_count"] for item in results), "references": results,
    }
    output.mkdir(parents=True)
    (output / "gate-c-evidence.json").write_text(
        json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")), encoding="utf-8",
    )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="生成四项研究的 Gate C 证据，显式区分合成与真实数据")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--release-candidate-id")
    parser.add_argument("--build-manifest")
    args = parser.parse_args()
    evidence = build_gate_c_evidence(
        input_path=Path(args.input).resolve(), output=Path(args.output).resolve(),
        release_candidate_id=args.release_candidate_id,
        build_manifest_path=Path(args.build_manifest).resolve() if args.build_manifest else None,
    )
    print(json.dumps({key: evidence[key] for key in (
        "status", "reference_count", "dag_family_count", "data_scope", "acceptance_scope", "evidence_scope",
    )}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
