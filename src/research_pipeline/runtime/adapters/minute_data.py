"""minute_data 算子族及其直接共享实现。"""

from __future__ import annotations

import shutil
from typing import Mapping
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.data_plane import PartitionedDatasetResolver, PartitionedDatasetRef, audit_adjustment_gate, build_minute_partitioned_dataset, build_minute_resample_plan, build_minute_scan_plan, execute_minute_resample, inspect_parquet_partition, minute_resample_schema
from research_pipeline.data_plane.research_data_bundle import merge_research_data_reference
from research_pipeline.domain import load_session_policy_bundle
from research_pipeline.platform.metric_contracts import build_mainline_metric_registry
from ..operator_graph_evidence import build_minute_observation_validity_facts
from ..operator_runtime import OperatorRuntimeContext, RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _capture, _environment, _external_result, _input_admitted_plans, _input_external_payload, _input_merged_data_bundle, _json_ready, _parameters
from .minute_io import _minute_root, _minute_scan_plan, _session_instruments


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
    _capture(context, "minute_data", payload)
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
    bundle = load_session_policy_bundle()
    if interval_minutes == 1:
        dataset_result = dataset
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
    else:
        instruments = _session_instruments(scan_plan, bundle)
        resample_plan = build_minute_resample_plan(
            scan_plan,
            interval_minutes=interval_minutes,
            session_bundle=bundle,
            instruments=instruments,
        )
        return _commit_resampled_minute_dataset(
            context,
            request_id=request_id,
            scan_plan=scan_plan,
            source_dataset=dataset,
            resample_plan=resample_plan,
        )
    payload = {
        "contract_version": "runtime-minute-bars-artifact",
        "request_id": request_id,
        "interval_minutes": interval_minutes,
        "resample_plan_hash": plan_hash,
        "artifact_semantics_hash": artifact_semantics_hash,
        "session_policy_bundle_hash": bundle.bundle_hash,
        "quality_policy_refs": list(scan_plan.minute_quality_policy_refs),
        "quality_status": "pending_stream_consumption",
        "price_mode": "raw",
        "partitioned_dataset": dataset_result.to_dict(),
        "source_snapshot_hash": source_payload["source_snapshot_hash"],
    }
    return _external_result(context, payload)


def _commit_resampled_minute_dataset(
    context: OperatorRuntimeContext,
    *,
    request_id: str,
    scan_plan,
    source_dataset: PartitionedDatasetRef,
    resample_plan,
) -> RuntimeNodeOutputs:
    """逐月写派生周期；任一时刻只打开一个输入月和一个输出 writer。"""

    import pyarrow.parquet as pq

    staging = context.external_store.prepare()
    resolver = PartitionedDatasetResolver(
        {"minute_data": _minute_root(context)},
        max_batch_rows=65_536,
        max_batch_bytes=min(
            scan_plan.budget.max_batch_bytes,
            context.effective_resource_budget.memory_bytes // 4,
        ),
    )
    output_partitions = []
    partition_receipts = []
    try:
        for source_ref in source_dataset.partitions:
            source_partition = resolver.resolve_partition(
                source_dataset,
                source_ref.partition_key,
            )

            class _PartitionBatches:
                source_identity = typed_canonical_hash(source_ref.to_dict())

                def __iter__(self):
                    return source_partition.iter_batches(
                        columns=scan_plan.source_columns,
                        batch_size=65_536,
                    )

            stream = execute_minute_resample(_PartitionBatches(), plan=resample_plan)
            target = staging / "bars" / source_ref.partition_key / "data.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(target, minute_resample_schema())
            try:
                for batch in stream:
                    writer.write_batch(batch)
            finally:
                writer.close()
            manifest = stream.manifest
            lineage = {
                "resample_plan_hash": manifest.resample_plan_hash,
                "artifact_semantics_hash": manifest.artifact_semantics_hash,
                "input_partition_id": manifest.input_reference_id,
                "status_counts": dict(manifest.status_counts),
            }
            output_partitions.append(inspect_parquet_partition(
                target,
                allowed_root=staging,
                partition_key=source_ref.partition_key,
                logical_start=source_ref.logical_start,
                logical_end=source_ref.logical_end,
                root_role="runtime_artifact",
                source_kind="runtime_derived",
                sort_keys=("code", "dt"),
                lineage=lineage,
            ))
            partition_receipts.append({
                "partition_key": source_ref.partition_key,
                **manifest.to_dict(),
            })
        dataset_lineage = {
            "minute_asset_class": scan_plan.minute_asset_class,
            "minute_session_policy_ref": scan_plan.minute_session_policy_ref,
            "minute_quality_policy_refs": list(scan_plan.minute_quality_policy_refs),
            "interval_minutes": resample_plan.interval_minutes,
            "source_dataset_id": source_dataset.dataset_id,
            "source_dataset_reference_id": source_dataset.reference_id,
            "resample_plan_hash": resample_plan.plan_hash,
            "artifact_semantics_hash": resample_plan.artifact_semantics_hash,
        }
        dataset = PartitionedDatasetRef(
            dataset_id=f"minute/resampled/{resample_plan.artifact_semantics_hash}",
            timestamp_field="dt",
            instrument_field="code",
            instruments=source_dataset.instruments,
            allowed_columns=tuple(minute_resample_schema().names),
            partitions=tuple(output_partitions),
            lineage=dataset_lineage,
            universe_snapshot_id=source_dataset.universe_snapshot_id,
        )
        payload = {
            "contract_version": "runtime-minute-bars-artifact",
            "request_id": request_id,
            "interval_minutes": resample_plan.interval_minutes,
            "resample_plan_hash": resample_plan.plan_hash,
            "artifact_semantics_hash": resample_plan.artifact_semantics_hash,
            "session_policy_bundle_hash": resample_plan.session_policy_bundle_hash,
            "quality_policy_refs": list(scan_plan.minute_quality_policy_refs),
            "quality_status": "pending_stream_consumption",
            "price_mode": "raw",
            "partitioned_dataset": dataset.to_dict(),
            "partition_receipts": partition_receipts,
            "source_snapshot_hash": source_dataset.reference_id,
        }
        (staging / "result.json").write_text(
            canonical_json(_json_ready(payload)),
            encoding="utf-8",
        )
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs.single(RuntimeNodeValue.external(commit))


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
        parquet_rows={"observation": metric_rows},
        completion_metadata=RuntimeCompletionMetadata(
            artifact_hashes={"observation": typed_canonical_hash(payload)},
            counts={"observed_row_count": row_count},
        ),
    )


def execute_research_validity_minute_observation_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    """只证明分钟清单观察链；标签、搜索、统计推断和交易明确不适用。"""

    return _execute_research_validity_minute_observation(
        context,
        data_ports=("minute_1m", "decision_minute_1m"),
    )


def execute_research_validity_adjusted_minute_observation_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    """合并复权上下文和两条分钟输入后生成观察链 validity。"""

    return _execute_research_validity_minute_observation(
        context,
        data_ports=("data", "minute_1m", "decision_minute_1m"),
    )


def _execute_research_validity_minute_observation(
    context: OperatorRuntimeContext,
    *,
    data_ports: tuple[str, ...],
) -> RuntimeNodeValue:
    """只从算子声明的显式入边闭合分钟观察所需数据引用。"""

    environment = _environment(context)
    observation = _input_external_payload(context, "observation")
    request_id = str(_parameters(context)["request_id"])
    if observation.get("request_id") != request_id:
        raise ValueError("分钟观察 validity 请求与观察工件不一致")
    data_bundle = _input_merged_data_bundle(context, *data_ports)
    facts = build_minute_observation_validity_facts(
        admitted_plans=_input_admitted_plans(environment, data_bundle),
        data_bundle=data_bundle,
        observation=observation,
        fixed_clock=environment.fixed_clock,
    )
    return _external_result(context, facts)
