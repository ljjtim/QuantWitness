"""minute_data 算子族及其直接共享实现。"""

from __future__ import annotations

from research_pipeline.platform.metric_contracts import builtin_metric_output_path

from typing import Mapping
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.data_plane import PartitionedDatasetRef, audit_adjustment_gate, build_minute_partitioned_dataset, build_minute_scan_plan
from research_pipeline.data_plane.research_data_bundle import merge_research_data_reference
from research_pipeline.platform.metric_contracts import build_mainline_metric_registry
from ..operator_runtime import OperatorRuntimeContext, RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _environment, _external_result, _input_external_payload, _parameters
from .minute_io import _minute_root, _minute_scan_plan, _session_bundle_from_payload


def execute_data_minute_scan_v1(context: OperatorRuntimeContext) -> RuntimeNodeValue:
    request_id, plan = _minute_scan_plan(context)
    dataset = build_minute_partitioned_dataset(plan)
    adjustment = audit_adjustment_gate(
        source_adjustment_mode=str(
            _environment(context).admitted_plans[request_id].query.adjustment
        ),
        snapshot=None,
    )
    data_bundle = merge_research_data_reference(
        None,
        request_id=request_id,
        admitted_plan_hash=plan.admitted_plan_hash,
        reference=dataset.to_dict(),
    )
    payload = {
        "contract_version": "runtime-minute-scan-artifact",
        "request_id": request_id,
        "scan_plan_hash": plan.plan_hash,
        "source_revision_hash": plan.source_revision_hash,
        "partitioned_dataset": dataset.to_dict(),
        "adjustment_audit": adjustment.to_dict(),
        "quality_status": "pending_stream_consumption",
        "source_snapshot_hash": dataset.reference_id,
        "data_bundle": data_bundle,
    }
    return _external_result(context, payload)


def execute_research_bars_minute_resample_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    source_payload = _input_external_payload(context, "minute_1m")
    if source_payload.get("quality_status") != "pending_stream_consumption":
        raise ValueError("分钟 resample 输入质量状态无效")
    request_id = str(source_payload["request_id"])
    admitted = _environment(context).admitted_plans.get(request_id)
    if admitted is None:
        raise ValueError("分钟 resample 缺少 scan 的已准入 QueryIR")
    # 重新编译只读扫描计划并校验物理源，避免信任上游自报路径。
    asset_class = str(admitted.minute_asset_class)
    source_directory = {
        "cn_stock": "stock",
        "cn_etf": "fund",
        "cn_index": "index",
        "cn_future": "futures",
    }[asset_class]
    scan_plan = build_minute_scan_plan(
        admitted,
        source=_minute_root(context) / source_directory,
        allowed_root=_minute_root(context),
    )
    raw_dataset = source_payload.get("partitioned_dataset")
    if not isinstance(raw_dataset, Mapping):
        raise ValueError("分钟 resample 输入缺少分区引用")
    dataset = PartitionedDatasetRef.from_dict(raw_dataset)
    expected_dataset = build_minute_partitioned_dataset(scan_plan)
    if dataset != expected_dataset:
        raise ValueError("分钟 resample 输入扫描计划或物理源发生漂移")
    parameters = _parameters(context)
    interval_minutes = int(parameters["interval_minutes"])
    bundle = _session_bundle_from_payload(parameters, context)
    if "minute_scope" in parameters:
        from research_pipeline.domain.minute_request_scope import minute_request_sessions
        for _ in minute_request_sessions(bundle, instruments=scan_plan.instruments,
              start_at=scan_plan.start_at, end_at=scan_plan.end_at, policy_id=scan_plan.minute_session_policy_ref):
            pass
    if interval_minutes != 1:
        raise ValueError("分钟行情仅支持一分钟")
    plan_hash = typed_canonical_hash({
        "scan_plan_hash": scan_plan.plan_hash,
        "interval_minutes": 1,
        "session_policy_bundle_hash": bundle.bundle_hash,
        "minute_capability_manifest_hash": scan_plan.minute_capability_manifest_hash,
    })
    artifact_semantics_hash = typed_canonical_hash({
        "interval_minutes": 1,
        "source_dataset": dataset.reference_id,
        "session_policy_bundle_hash": bundle.bundle_hash,
    })
    payload = {
        "contract_version": "runtime-minute-bars-artifact",
        "request_id": request_id,
        "interval_minutes": interval_minutes,
        "resample_plan_hash": plan_hash,
        "artifact_semantics_hash": artifact_semantics_hash,
        "session_policy_bundle_hash": bundle.bundle_hash,
        **({"minute_scope": parameters["minute_scope"]} if "minute_scope" in parameters else {}),
        "quality_policy_refs": list(scan_plan.minute_quality_policy_refs),
        "quality_status": "pending_stream_consumption",
        "price_mode": "raw",
        "partitioned_dataset": dataset.to_dict(),
        "source_snapshot_hash": source_payload["source_snapshot_hash"],
    }
    return _external_result(context, payload)


def execute_research_observation_minute_bars_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    """从已验证的分区 manifest 生成纯观察行数指标，不读取价格行或运行交易仿真。"""

    bars = _input_external_payload(context, "bars")
    raw_dataset = bars.get("partitioned_dataset")
    if not isinstance(raw_dataset, Mapping):
        raise ValueError("分钟观察输入缺少分区 manifest")
    dataset = PartitionedDatasetRef.from_dict(raw_dataset)
    parameters = _parameters(context)
    request_id = str(parameters["request_id"])
    if bars.get("request_id") != request_id:
        raise ValueError("分钟观察请求与 bars 来源不一致")
    admitted = _environment(context).admitted_plans.get(request_id)
    if admitted is None:
        raise ValueError("分钟观察缺少已准入 QueryIR")
    query = admitted.query
    row_count = sum(item.row_count for item in dataset.partitions)
    if row_count <= 0:
        raise ValueError("分钟观察分区行数必须为正")
    metric_ref = "minute.row_count@1.0.0"
    metric_definition = build_mainline_metric_registry().require(metric_ref)
    metric_rows = [{
        "metric_ref": metric_ref,
        "value": float(row_count),
        "unit": metric_definition.unit,
        "sample_start": query.time_range.start_at.isoformat(),
        "sample_end": query.time_range.end_at.isoformat(),
        "sample_size": row_count,
        "status": "computed",
    }]
    payload = {
        "contract_version": "runtime-minute-observation-v1",
        "request_id": request_id,
        "dataset_reference_id": dataset.reference_id,
        "partition_count": len(dataset.partitions),
        "row_count": row_count,
    }
    return _external_result(
        context,
        payload,
        parquet_rows={builtin_metric_output_path("research.minute-observation.v1"): metric_rows},
        completion_metadata=RuntimeCompletionMetadata(
            artifact_hashes={"observation": typed_canonical_hash(payload)},
            counts={"observed_row_count": row_count},
        ),
    )
