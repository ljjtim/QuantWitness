"""算子身份边界：grid_data。"""

from __future__ import annotations

from .grid_data_contract import _columnar_materialization_plans, _verify_data_bundle
from .grid_measurement import _measurement
from datetime import datetime
from pathlib import Path
from research_pipeline.data_plane import ArtifactResolver, DataPlaneError, DataPlaneRequestExecutionError, DatasetArtifactRef, SnapshotIntegrityError
from research_pipeline.data_plane import DataPlaneExecutionBudget
from research_pipeline.data_plane.execution_estimate import ExecutionEstimate, load_execution_estimates
from research_pipeline.data_plane.service import _write_json_atomic, materialize_dataset_plan
from research_pipeline.platform.metric_contracts import build_mainline_metric_registry
from research_pipeline.runtime.adapters.common import _capture, _environment, _external_result
from research_pipeline.runtime.operator_runtime import OperatorRuntimeContext, RuntimeNodeValue
from time import perf_counter
from typing import Mapping
import json


REQUEST_PARTIAL_INDEX_VERSION = "data-request-partial-index-v1"


def execute_operator_graph_data(
    *,
    manifest: Mapping[str, object],
    admitted_plans: Mapping[str, object],
    data_db: str | Path,
    source_databases: Mapping[str, str | Path] | None = None,
    artifact_root: str | Path,
    root_seed: int,
    fixed_clock: str,
    execution_budget: DataPlaneExecutionBudget,
    scratch_root: str | Path,
    request_recovery_root: str | Path | None = None,
) -> dict[str, object]:
    """只执行数据物化节点，避免统一 Runtime 中一个节点代跑后续节点。"""
    database = Path(data_db).resolve()
    databases = _source_database_map(database, source_databases)
    artifacts = Path(artifact_root).resolve()
    if not database.is_file():
        raise ValueError("显式只读 data-db 不存在")
    if root_seed != manifest["root_seed"] or fixed_clock != manifest["fixed_clock"]:
        raise ValueError("run 的 clock/root_seed 必须与正式计划完全一致")
    datetime.fromisoformat(fixed_clock)
    artifacts.mkdir(parents=True, exist_ok=True)
    database_before = _fingerprint(database)
    source_fingerprints_before = {
        profile: _fingerprint(path) for profile, path in sorted(databases.items())
    }
    started = perf_counter()
    verified_dataset_manifests: dict[str, Mapping[str, object]] = {}
    execution_estimates = load_execution_estimates(
        manifest.get("execution_estimates"),
        request_ids=tuple(sorted(admitted_plans)),
    )
    data_bundle = _materialize_or_load_data(
        admitted_plans=admitted_plans,
        execution_estimates=execution_estimates,
        database=database,
        source_databases=databases,
        artifact_root=artifacts / "data",
        verified_manifests_out=verified_dataset_manifests,
        execution_budget=execution_budget,
        scratch_root=scratch_root,
        partial_index_path=(
            None
            if request_recovery_root is None
            else Path(request_recovery_root).resolve() / "partial-index.json"
        ),
    )
    measurement = _measurement("data_plane", started, data_bundle["bundle_hash"])
    database_after = _fingerprint(database)
    source_fingerprints_after = {
        profile: _fingerprint(path) for profile, path in sorted(databases.items())
    }
    if (
        database_before != database_after
        or source_fingerprints_before != source_fingerprints_after
    ):
        raise ValueError("真实只读运行前后数据库 size/mtime 发生变化")
    result = {
        "status": "data_succeeded",
        "data_bundle_hash": data_bundle["bundle_hash"],
        "data_bundle": data_bundle,
        "database_fingerprint_before": database_before,
        "database_fingerprint_after": database_after,
        "database_fingerprints_by_profile_before": source_fingerprints_before,
        "database_fingerprints_by_profile_after": source_fingerprints_after,
        "database_unchanged": True,
        "measurement": measurement,
    }
    # 只在同一调用栈内交给 Runtime adapter 生成观察指标，不持久化第二份 manifest。
    result["_verified_dataset_manifests"] = verified_dataset_manifests
    return result


def _materialize_or_load_data(
    *,
    admitted_plans: Mapping[str, object],
    execution_estimates: Mapping[str, ExecutionEstimate],
    database: Path,
    source_databases: Mapping[str, Path],
    artifact_root: Path,
    verified_manifests_out: dict[str, Mapping[str, object]] | None = None,
    execution_budget: DataPlaneExecutionBudget,
    scratch_root: str | Path,
    partial_index_path: str | Path | None = None,
) -> dict[str, object]:
    columnar_plans = _columnar_materialization_plans(admitted_plans)
    artifact_root = Path(artifact_root).resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    columnar_estimates = {
        request_id: execution_estimates[request_id]
        for request_id in columnar_plans
    }
    references = {}
    fingerprints = []
    completed_request_ids: list[str] = []
    partials = _load_request_partial_index(partial_index_path)
    resolver = ArtifactResolver(artifact_root)
    for request_id, plan in sorted(columnar_plans.items()):
        partial = partials.get(request_id)
        if partial is not None and partial["admitted_plan_hash"] == plan.plan_hash:
            try:
                reference = DatasetArtifactRef.from_dict(partial["reference"])
                dataset = resolver.resolve(reference)
                if dataset.manifest.get("admitted_plan_hash") != plan.plan_hash:
                    raise SnapshotIntegrityError(
                        "partial DatasetArtifactRef 与当前 plan 不一致"
                    )
            except (DataPlaneError, KeyError, OSError, TypeError):
                partials.pop(request_id, None)
                _save_request_partial_index(partial_index_path, partials)
            else:
                references[request_id] = reference.to_dict()
                fingerprints.append(dict(partial["database_fingerprint"]))
                completed_request_ids.append(request_id)
                continue
        elif partial is not None:
            partials.pop(request_id, None)
            _save_request_partial_index(partial_index_path, partials)
        source_database = source_databases.get(plan.source_profile)
        if source_database is None:
            raise _request_execution_error(
                request_id=request_id,
                plan=plan,
                execution_budget=execution_budget,
                completed_request_ids=completed_request_ids,
                request_status="not_opened",
                cause=ValueError("Catalog source_profile 未绑定只读数据库"),
            )
        try:
            result = materialize_dataset_plan(
                plan=plan,
                data_db=source_database,
                artifact_root=artifact_root,
                execution_budget=execution_budget,
                execution_estimate=columnar_estimates[request_id],
                scratch_root=Path(scratch_root) / request_id,
            )
        except Exception as exc:
            raise _request_execution_error(
                request_id=request_id,
                plan=plan,
                execution_budget=execution_budget,
                completed_request_ids=completed_request_ids,
                request_status="opened",
                cause=exc,
            ) from exc
        references[request_id] = result["reference"]
        fingerprints.append(result["database_fingerprint_after"])
        completed_request_ids.append(request_id)
        partials[request_id] = {
            "request_id": request_id,
            "admitted_plan_hash": plan.plan_hash,
            "reference": dict(result["reference"]),
            "database_fingerprint": dict(result["database_fingerprint_after"]),
        }
        _save_request_partial_index(partial_index_path, partials)
    from research_pipeline.data_plane.research_data_bundle import (
        build_research_data_bundle,
    )

    payload = build_research_data_bundle(
        admitted_plan_hashes={
            request_id: plan.plan_hash
            for request_id, plan in sorted(columnar_plans.items())
        },
        references=references,
        database_fingerprints=fingerprints,
    )
    verified_manifests = _verify_data_bundle(
        payload,
        columnar_plans,
        artifact_root,
    )
    if verified_manifests_out is not None:
        verified_manifests_out.clear()
        verified_manifests_out.update(verified_manifests)
    return payload


def _load_request_partial_index(
    path: str | Path | None,
) -> dict[str, dict[str, object]]:
    """逐项读取节点内部恢复索引；单项损坏不会牵连其他 request。"""

    if path is None:
        return {}
    target = Path(path).resolve()
    if not target.is_file():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    if (
        not isinstance(payload, Mapping)
        or set(payload) != {"contract_version", "requests"}
        or payload.get("contract_version") != REQUEST_PARTIAL_INDEX_VERSION
        or not isinstance(payload.get("requests"), Mapping)
    ):
        return {}
    result: dict[str, dict[str, object]] = {}
    for request_id, raw in payload["requests"].items():
        if (
            not isinstance(request_id, str)
            or not isinstance(raw, Mapping)
            or set(raw) != {
                "request_id",
                "admitted_plan_hash",
                "reference",
                "database_fingerprint",
            }
            or raw.get("request_id") != request_id
            or not isinstance(raw.get("admitted_plan_hash"), str)
            or not isinstance(raw.get("reference"), Mapping)
            or not isinstance(raw.get("database_fingerprint"), Mapping)
        ):
            continue
        result[request_id] = {
            "request_id": request_id,
            "admitted_plan_hash": raw["admitted_plan_hash"],
            "reference": dict(raw["reference"]),
            "database_fingerprint": dict(raw["database_fingerprint"]),
        }
    return result


def _save_request_partial_index(
    path: str | Path | None,
    partials: Mapping[str, Mapping[str, object]],
) -> None:
    if path is None:
        return
    _write_json_atomic(
        path,
        {
            "contract_version": REQUEST_PARTIAL_INDEX_VERSION,
            "requests": {
                request_id: dict(partial)
                for request_id, partial in sorted(partials.items())
            },
        },
    )


def _request_execution_error(
    *,
    request_id: str,
    plan: object,
    execution_budget: DataPlaneExecutionBudget,
    completed_request_ids: list[str],
    request_status: str,
    cause: Exception,
) -> DataPlaneRequestExecutionError:
    """错误正文不携带 SQL/参数；定位事实只来自正式计划。"""

    return DataPlaneRequestExecutionError(
        f"data-plane request 执行失败: request={request_id}, "
        f"object={plan.object_name}",
        failure_payload={
            "contract_version": "data-plane-request-failure-v1",
            "request_status": request_status,
            "request_id": request_id,
            "dataset_id": plan.query.dataset_id,
            "binding_id": plan.binding_id,
            "object_name": plan.object_name,
            "provider": "duckdb",
            "output_budget": plan.query.budget.to_dict(),
            "execution_budget": execution_budget.to_dict(),
            "completed_request_ids": list(completed_request_ids),
            "underlying_exception_type": type(cause).__name__,
        },
    )


def _source_database_map(
    primary: Path,
    source_databases: Mapping[str, str | Path] | None,
) -> dict[str, Path]:
    databases = {"source": primary}
    for profile, raw_path in sorted((source_databases or {}).items()):
        if not isinstance(profile, str) or not profile.strip() or profile == "source":
            raise ValueError("额外 source_profile 必须非空且不能覆盖 source")
        path = Path(raw_path).resolve()
        if not path.is_file():
            raise ValueError(f"source_profile 只读数据库不存在: {profile}")
        databases[profile] = path
    return databases


def _fingerprint(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def execute_data_columnar_materialize_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    """物化普通列式请求；分钟请求只由独立的引用扫描节点处理。"""

    env = _environment(context)
    request_recovery_root = (
        context.external_store.root.parent
        / "request-recovery"
        / context.node.node_id
    )
    result = execute_operator_graph_data(
        manifest=env.manifest,
        admitted_plans=env.admitted_plans,
        data_db=env.database,
        source_databases=env.source_databases,
        artifact_root=request_recovery_root,
        root_seed=env.root_seed,
        fixed_clock=env.fixed_clock,
        execution_budget=DataPlaneExecutionBudget.from_resource_budget(
            context.effective_resource_budget
        ),
        scratch_root=context.work_dir,
        request_recovery_root=request_recovery_root,
    )
    result = dict(result)
    verified_dataset_manifests = result.pop(
        "_verified_dataset_manifests",
        None,
    )
    observations, metric_rows = _dataset_manifest_observations(
        data_bundle=result["data_bundle"],
        admitted_plans=env.admitted_plans,
        verified_dataset_manifests=verified_dataset_manifests,
    )
    result["observations"] = observations
    _capture(context, "data", result)
    return _external_result(
        context,
        result,
        directories={"data": request_recovery_root / "data"},
        parquet_rows=({"observation": metric_rows} if metric_rows else None),
    )


def _dataset_manifest_observations(
    *,
    data_bundle: object,
    admitted_plans: Mapping[str, object],
    verified_dataset_manifests: object,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """复用上游已验证 manifest 读取行数，不重新读取或哈希数据工件。"""

    if not isinstance(data_bundle, Mapping):
        raise ValueError("columnar data 结果缺少 data bundle")
    references = data_bundle.get("references")
    if not isinstance(references, Mapping):
        raise ValueError("columnar data bundle 缺少引用集合")
    if (
        not isinstance(verified_dataset_manifests, Mapping)
        or set(verified_dataset_manifests) != set(references)
    ):
        raise ValueError("columnar data bundle 缺少已验证 manifest 集合")
    metric_ref = "data.row_count@1.0.0"
    metric = build_mainline_metric_registry().require(metric_ref)
    observations = []
    metric_rows = []
    for request_id, raw_reference in sorted(references.items()):
        if not isinstance(request_id, str) or not isinstance(raw_reference, Mapping):
            raise ValueError("columnar data bundle 引用无效")
        plan = admitted_plans.get(request_id)
        if plan is None:
            raise ValueError(f"columnar data bundle 引用未知请求: {request_id}")
        reference = DatasetArtifactRef.from_dict(raw_reference)
        manifest = verified_dataset_manifests[request_id]
        if not isinstance(manifest, Mapping):
            raise ValueError(f"columnar data manifest 无效: {request_id}")
        row_count = manifest.get("row_count")
        if type(row_count) is not int or row_count < 0:
            raise ValueError(f"columnar data manifest 行数无效: {request_id}")
        query = getattr(plan, "query", None)
        time_range = getattr(query, "time_range", None)
        if time_range is None:
            raise ValueError(f"columnar data 请求缺少时间范围: {request_id}")
        sample_start = time_range.start.isoformat()
        sample_end = time_range.end.isoformat()
        observation = {
            "contract_version": "dataset-manifest-observation-v1",
            "request_id": request_id,
            "dataset_reference_id": reference.physical_snapshot_id,
            "manifest_hash": reference.manifest_hash,
            "row_count": row_count,
            "metric_ref": metric_ref,
            "sample_start": sample_start,
            "sample_end": sample_end,
        }
        observations.append(observation)
        metric_rows.append({
            "request_id": request_id,
            "metric_ref": metric_ref,
            "value": float(row_count),
            "unit": metric.unit,
            "sample_start": sample_start,
            "sample_end": sample_end,
            "sample_size": row_count,
            "status": "computed",
        })
    return observations, metric_rows
