"""minute_io 算子族及其直接共享实现。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shutil
from typing import Callable, Iterable, Iterator, Mapping
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.data_plane import MinuteScanBudget, PartitionedDatasetResolver, PartitionedDatasetRef, build_minute_resample_plan, build_minute_scan_plan, execute_minute_resample, inspect_parquet_partition
from research_pipeline.domain import load_session_policy_bundle
from research_pipeline.research.minute_operators import MINUTE_OPERATOR_ARTIFACT_VERSION
from ..operator_runtime import OperatorRuntimeContext, RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _environment, _input_external_root, _json_ready, _local_date, _parameters


def _minute_root(context: OperatorRuntimeContext) -> Path:
    root = _environment(context).minute_data_root
    if root is None:
        raise ValueError("分钟 DAG 必须显式提供 --minute-data-root")
    return root


def _minute_request(context: OperatorRuntimeContext) -> tuple[str, object]:
    request_ids = _parameters(context).get("request_ids")
    if not isinstance(request_ids, (list, tuple)) or len(request_ids) != 1:
        raise ValueError("分钟 scan 每个节点必须恰好绑定一个 request_id")
    request_id = str(request_ids[0])
    admitted = _environment(context).admitted_plans.get(request_id)
    if admitted is None:
        raise ValueError(f"分钟 scan 缺少已准入 QueryIR: {request_id}")
    return request_id, admitted


def _minute_scan_plan(context: OperatorRuntimeContext):
    request_id, admitted = _minute_request(context)
    return request_id, _build_minute_scan_plan_for_request(
        context,
        request_id=request_id,
        admitted=admitted,
        parameters=_parameters(context),
    )


def _build_minute_scan_plan_for_request(
    context: OperatorRuntimeContext,
    *,
    request_id: str,
    admitted,
    parameters: Mapping[str, object],
):
    asset_class = str(admitted.minute_asset_class)
    source_directory = {
        "cn_stock": "stock",
        "cn_etf": "fund",
        "cn_index": "index",
        "cn_future": "futures",
    }.get(asset_class)
    if source_directory is None:
        raise ValueError(f"分钟 scan 资产类别不受支持: {asset_class}")
    budget = MinuteScanBudget(
        max_selected_file_bytes=int(parameters["max_source_bytes"]),
        max_batch_bytes=int(parameters["max_batch_bytes"]),
    )
    plan = build_minute_scan_plan(
        admitted,
        source=_minute_root(context) / source_directory,
        allowed_root=_minute_root(context),
        budget=budget,
    )
    if plan.query_max_rows > int(parameters["max_returned_rows"]):
        raise ValueError("分钟 QueryIR 返回行预算超过算子声明")
    if plan.minute_capability_manifest_hash != parameters["scope_binding_hash"]:
        raise ValueError("分钟 scan 能力 manifest 绑定与 QueryIR 不一致")
    return plan


def _minute_scan_plan_from_environment(
    context: OperatorRuntimeContext,
    request_id: str,
):
    admitted = _environment(context).admitted_plans.get(request_id)
    if admitted is None:
        raise ValueError(f"分钟输入缺少已准入 QueryIR: {request_id}")
    matches = [
        parameters
        for parameters in _environment(context).node_parameters.values()
        if (
            (
                parameters.get("request_ids") == [request_id]
                or parameters.get("request_ids") == (request_id,)
            )
            and "max_source_bytes" in parameters
            and "max_batch_bytes" in parameters
        )
    ]
    if len(matches) != 1:
        raise ValueError("分钟输入无法唯一定位 scan 节点参数")
    return _build_minute_scan_plan_for_request(
        context,
        request_id=request_id,
        admitted=admitted,
        parameters=matches[0],
    )


def _session_instruments(scan_plan, bundle):
    by_id = {}
    for policy in bundle.policies:
        instrument = policy.instrument
        if instrument.instrument_id in scan_plan.instruments:
            by_id[instrument.instrument_id] = instrument
    if set(by_id) != set(scan_plan.instruments):
        raise ValueError("分钟 session bundle 未覆盖扫描标的")
    return tuple(by_id[key] for key in sorted(by_id))


def _aware(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        from zoneinfo import ZoneInfo

        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed


def _record_batch_rows(batch: object) -> Iterator[dict[str, object]]:
    """逐行投影一个有界 RecordBatch，不创建整批 Python list。"""

    names = tuple(batch.schema.names)
    columns = tuple(batch.column(index) for index in range(len(names)))
    for row_index in range(int(batch.num_rows)):
        yield {
            name: column[row_index].as_py()
            for name, column in zip(names, columns, strict=True)
        }


def _external_parquet_rows(
    root: Path,
    prefix: str,
    *,
    columns: tuple[str, ...],
) -> Iterator[dict[str, object]]:
    """逐批读取已经由 ExternalArtifactStore 复验的列式事实。"""

    import pyarrow.parquet as pq

    files = tuple(sorted((root / prefix).rglob("*.parquet")))
    if not files:
        raise ValueError(f"外部工件缺少列式事实: {prefix}")
    for path in files:
        parquet = pq.ParquetFile(path)
        if not set(columns) <= set(parquet.schema_arrow.names):
            raise ValueError(f"外部工件列式 schema 不完整: {prefix}")
        for batch in parquet.iter_batches(batch_size=8_192, columns=list(columns)):
            yield from _record_batch_rows(batch)


def _operator_bar_partitions(
    context: OperatorRuntimeContext,
    payload: Mapping[str, object],
    *,
    port: str = "bars",
) -> tuple[
    PartitionedDatasetRef,
    Iterator[tuple[object, Iterator[dict[str, object]]]],
]:
    """逐分区提供规范分钟行；调用者必须在进入下一分区前消费当前迭代器。"""

    snapshot_hash = str(payload["source_snapshot_hash"])
    raw_dataset = payload.get("partitioned_dataset")
    if not isinstance(raw_dataset, Mapping):
        raise ValueError("分钟 bars 工件缺少分区数据引用")
    dataset = PartitionedDatasetRef.from_dict(raw_dataset)
    input_root = _input_external_root(context, port)
    roots = {}
    roles = {item.root_role for item in dataset.partitions}
    if "minute_data" in roles:
        roots["minute_data"] = _minute_root(context)
    if "runtime_artifact" in roles:
        roots["runtime_artifact"] = input_root
    resolver = PartitionedDatasetResolver(roots)
    normalized = "bar_start" in dataset.allowed_columns
    raw_plan = None
    if not normalized:
        request_id = str(payload.get("request_id", ""))
        scan_plan = _minute_scan_plan_from_environment(context, request_id)
        bundle = load_session_policy_bundle()
        raw_plan = build_minute_resample_plan(
            scan_plan,
            interval_minutes=int(payload.get("interval_minutes", 0)),
            session_bundle=bundle,
            instruments=_session_instruments(scan_plan, bundle),
        )

    def partition_streams() -> Iterator[
        tuple[object, Iterator[dict[str, object]]]
    ]:
        for partition in dataset.partitions:
            source = resolver.resolve_partition(dataset, partition.partition_key)

            def normalized_rows(
                *, source=source, partition=partition
            ) -> Iterator[dict[str, object]]:
                source_batches = source.iter_batches(
                    columns=dataset.allowed_columns,
                    batch_size=65_536,
                )
                batches = source_batches
                if raw_plan is not None:

                    class _RawPartitionBatches:
                        source_identity = typed_canonical_hash(partition.to_dict())

                        def __iter__(self):
                            return source_batches

                    batches = execute_minute_resample(
                        _RawPartitionBatches(),
                        plan=raw_plan,
                    )
                for batch in batches:
                    for row in _record_batch_rows(batch):
                        completed = row.get("completed") is True
                        normalized_row = {
                            "instrument": str(row["code"]),
                            "bar_start": _aware(row["bar_start"]),
                            "bar_end": _aware(row["dt"]),
                            "available_time": _aware(row["dt"]),
                            "trading_date": _local_date(
                                row["trading_date"], "trading_date"
                            ),
                            "session_id": str(row["session_id"]),
                            "interval_minutes": int(row["interval_minutes"]),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "money": row.get("money"),
                            "avg": row["avg"],
                            "open_interest": row.get("open_interest"),
                            "completed": completed,
                            "quality_status": (
                                "pass" if completed else str(row["bar_status"])
                            ),
                            "source_snapshot_hash": snapshot_hash,
                            "source_partition_id": partition.partition_key,
                        }
                        for field in (
                            "factor",
                            "adjustment_ratio",
                            "adjustment_mode",
                            "factor_snapshot_hash",
                        ):
                            if field in row:
                                normalized_row[field] = row[field]
                        yield normalized_row

            yield partition, normalized_rows()

    return dataset, partition_streams()


def _minute_artifact_partitions(
    context: OperatorRuntimeContext,
    payload: Mapping[str, object],
    *,
    port: str,
) -> tuple[
    PartitionedDatasetRef,
    Iterator[tuple[object, Iterator[dict[str, object]]]],
]:
    """逐分区读取上游分钟算子列式工件。"""

    raw_dataset = payload.get("partitioned_dataset")
    if not isinstance(raw_dataset, Mapping):
        raise ValueError(f"分钟 {port} 工件缺少分区数据引用")
    dataset = PartitionedDatasetRef.from_dict(raw_dataset)
    root = _input_external_root(context, port)
    resolver = PartitionedDatasetResolver(
        {"runtime_artifact": root},
        max_batch_rows=8_192,
        max_batch_bytes=min(
            64 * 1024 * 1024,
            context.effective_resource_budget.memory_bytes // 4,
        ),
    )

    def partition_streams() -> Iterator[
        tuple[object, Iterator[dict[str, object]]]
    ]:
        for partition in dataset.partitions:
            source = resolver.resolve_partition(dataset, partition.partition_key)

            def rows(*, source=source) -> Iterator[dict[str, object]]:
                for batch in source.iter_batches(
                    columns=dataset.allowed_columns,
                    batch_size=8_192,
                ):
                    for row in _record_batch_rows(batch):
                        for field in (
                            "bar_end",
                            "available_time",
                            "decision_time",
                            "max_source_observation_time",
                            "max_source_available_time",
                            "label_start",
                            "label_end",
                            "first_actual_observation_time",
                            "last_actual_observation_time",
                        ):
                            if field in row:
                                row[field] = _aware(row[field])
                        yield row

            yield partition, rows()

    return dataset, partition_streams()


def _minute_timestamp_type(pa):
    return pa.timestamp("us", tz="Asia/Shanghai")


def _minute_storage_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _minute_storage_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_minute_storage_value(item) for item in value]
    return value


def _minute_feature_schema(feature_ids: tuple[str, ...], *, pre: bool):
    import pyarrow as pa

    fields = [
        pa.field("instrument", pa.string()),
        pa.field("bar_end", _minute_timestamp_type(pa)),
        pa.field("available_time", _minute_timestamp_type(pa)),
        pa.field("decision_time", _minute_timestamp_type(pa)),
        pa.field("max_source_observation_time", _minute_timestamp_type(pa)),
        pa.field("max_source_available_time", _minute_timestamp_type(pa)),
        pa.field("source_partition_ids", pa.list_(pa.string())),
        pa.field("session_id", pa.string()),
        pa.field("source_snapshot_hash", pa.string()),
        pa.field(
            "features",
            pa.struct([pa.field(feature_id, pa.float64()) for feature_id in feature_ids]),
        ),
    ]
    if pre:
        fields.extend(
            [
                pa.field("adjustment_anchor_factor", pa.float64()),
                pa.field("adjustment_snapshot_identity_hash", pa.string()),
            ]
        )
    return pa.schema(fields)


def _minute_label_schema(*, pre: bool):
    import pyarrow as pa

    fields = [
        pa.field("instrument", pa.string()),
        pa.field("bar_end", _minute_timestamp_type(pa)),
        pa.field("available_time", _minute_timestamp_type(pa)),
        pa.field("decision_time", _minute_timestamp_type(pa)),
        pa.field("label_start", _minute_timestamp_type(pa)),
        pa.field("label_end", _minute_timestamp_type(pa)),
        pa.field("first_actual_observation_time", _minute_timestamp_type(pa)),
        pa.field("last_actual_observation_time", _minute_timestamp_type(pa)),
        pa.field("source_snapshot_hash", pa.string()),
        pa.field("future_return", pa.float64()),
    ]
    if pre:
        fields.extend(
            [
                pa.field("adjustment_anchor_factor", pa.float64()),
                pa.field("adjustment_snapshot_identity_hash", pa.string()),
            ]
        )
    return pa.schema(fields)


def _minute_signal_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("instrument", pa.string()),
            pa.field("bar_end", _minute_timestamp_type(pa)),
            pa.field("available_time", _minute_timestamp_type(pa)),
            pa.field("decision_time", _minute_timestamp_type(pa)),
            pa.field("source_snapshot_hash", pa.string()),
            pa.field("signal", pa.int8()),
        ]
    )


def _minute_target_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("instrument", pa.string()),
            pa.field("bar_end", _minute_timestamp_type(pa)),
            pa.field("available_time", _minute_timestamp_type(pa)),
            pa.field("decision_time", _minute_timestamp_type(pa)),
            pa.field("source_snapshot_hash", pa.string()),
            pa.field("signal", pa.int8()),
            pa.field("target_quantity", pa.int64()),
            pa.field("target_hash", pa.string()),
            pa.field("portfolio_target_json", pa.string()),
        ]
    )


def _write_minute_row_partition(
    target: Path,
    *,
    rows: Iterable[Mapping[str, object]],
    schema: object,
    transform: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None,
) -> int:
    """按固定小批写出一个分区，并为零行结果保留带类型空 Parquet。"""

    import pyarrow as pa
    import pyarrow.parquet as pq

    target.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    buffered: list[Mapping[str, object]] = []
    row_count = 0

    def flush() -> None:
        nonlocal writer, row_count
        if not buffered:
            return
        table = pa.Table.from_pylist([dict(item) for item in buffered], schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(target, schema)
        writer.write_table(table)
        row_count += int(table.num_rows)
        buffered.clear()

    try:
        for raw in rows:
            projected = transform(raw) if transform is not None else raw
            buffered.append(
                {
                    str(key): _minute_storage_value(value)
                    for key, value in projected.items()
                }
            )
            if len(buffered) >= 2_048:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        pq.write_table(pa.Table.from_pylist([], schema=schema), target)
    return row_count


def _commit_partitioned_minute_rows(
    context: OperatorRuntimeContext,
    *,
    source_dataset: PartitionedDatasetRef,
    partition_rows: Iterable[tuple[object, Iterable[Mapping[str, object]]]],
    artifact_type: str,
    operator_id: str,
    prefix: str,
    timestamp_field: str,
    schema: object,
    parameters: Mapping[str, object],
    adjustment_context: Mapping[str, object],
    extra_manifest: Mapping[str, object] | None = None,
    transform: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None,
) -> RuntimeNodeOutputs:
    """逐分区提交分钟行级事实；JSON 只保存小型 manifest。"""

    parameters_hash = typed_canonical_hash(dict(parameters))
    staging = context.external_store.prepare()
    output_partitions = []
    row_count = 0
    try:
        for source_ref, rows in partition_rows:
            target = staging / prefix / str(source_ref.partition_key) / "data.parquet"
            row_count += _write_minute_row_partition(
                target,
                rows=rows,
                schema=schema,
                transform=transform,
            )
            output_partitions.append(
                inspect_parquet_partition(
                    target,
                    allowed_root=staging,
                    partition_key=str(source_ref.partition_key),
                    logical_start=source_ref.logical_start,
                    logical_end=source_ref.logical_end,
                    root_role="runtime_artifact",
                    source_kind="runtime_derived",
                    sort_keys=("instrument", timestamp_field),
                    lineage={
                        "input_partition_id": typed_canonical_hash(
                            source_ref.to_dict()
                        ),
                        "operator_id": operator_id,
                        "parameters_hash": parameters_hash,
                    },
                )
            )
        dataset = PartitionedDatasetRef(
            dataset_id=f"{artifact_type}/{parameters_hash}",
            timestamp_field=timestamp_field,
            instrument_field="instrument",
            instruments=source_dataset.instruments,
            universe_snapshot_id=source_dataset.universe_snapshot_id,
            allowed_columns=tuple(schema.names),
            partitions=tuple(output_partitions),
            lineage={
                "source_dataset_reference_id": source_dataset.reference_id,
                "operator_id": operator_id,
                "parameters_hash": parameters_hash,
            },
        )
        content_hash = typed_canonical_hash(
            {
                "source_dataset_reference_id": source_dataset.reference_id,
                "operator_id": operator_id,
                "parameters_hash": parameters_hash,
                "row_count": row_count,
            }
        )
        manifest = {
            "contract_version": MINUTE_OPERATOR_ARTIFACT_VERSION,
            "operator_id": operator_id,
            "parameters_hash": parameters_hash,
            "row_count": row_count,
            "partition_count": len(output_partitions),
            "content_hash": content_hash,
            "time_lineage_fields": [
                "instrument",
                timestamp_field,
                "available_time",
                "decision_time",
                "source_snapshot_hash",
            ],
            **dict(extra_manifest or {}),
        }
        payload = {
            "manifest": manifest,
            "partitioned_dataset": dataset.to_dict(),
            **dict(adjustment_context),
        }
        (staging / "result.json").write_text(
            canonical_json(_json_ready(payload)), encoding="utf-8"
        )
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs(
        {commit.artifact_name: RuntimeNodeValue.external(commit)},
        completion_metadata=RuntimeCompletionMetadata(
            artifact_hashes={prefix: content_hash},
            counts={f"{prefix}_row_count": row_count},
        ),
    )


_MINUTE_ADJUSTMENT_CONTEXT_KEYS = (
    "adjustment_snapshot_identity_hash",
    "adjustment_candidates",
    "adjustment_effective_times",
    "adjustment_previous_closes",
    "adjustment_snapshot",
    "source_bars_snapshot_hash",
)


def _minute_adjustment_context(payload: Mapping[str, object]) -> dict[str, object]:
    """沿 Feature→Signal→Target 保留 Result 独立复核真正消费的 PIT 载荷。"""

    return {
        key: payload[key]
        for key in _MINUTE_ADJUSTMENT_CONTEXT_KEYS
        if key in payload
    }
