"""归档来源的合同复验与有界读取。"""
from pathlib import Path
from .admitted_plan_codec import admitted_plan_from_dict
from .dataset_files import DatasetArtifactRef, ParquetArtifactResolver
from .errors import SnapshotIntegrityError
from .partitioned_artifacts import PartitionedDatasetRef, PartitionedDatasetResolver

def _snapshot_fields(plan):
    return (plan.temporal_selection.required_scan_fields
            if plan.temporal_selection.requires_consumer_binding else plan.query.field_ids)


def _snapshot_batches(dataset, files, plan, *, columns, batch_size):
    if not plan.temporal_selection.requires_consumer_binding:
        yield from dataset.iter_batches(columns=columns, batch_size=batch_size)
        return
    import pyarrow.parquet as pq

    # 归档复制保留完整版本事实；业务消费者仍须绑定自己的观察时点。
    for path in files:
        with pq.ParquetFile(path, pre_buffer=False) as parquet:
            for batch in parquet.iter_batches(columns=list(columns), batch_size=batch_size, use_threads=False):
                if batch.nbytes > dataset.max_batch_bytes:
                    raise SnapshotIntegrityError("归档时态批次超过批准内存上限")
                yield batch


def _logical_query(query):
    value = query.to_dict()
    value.pop("budget", None)
    return value


def require_archived_plan_equivalent(original, current):
    """物理绑定可改变，逻辑查询及实际可见性规则必须保持。"""
    if _logical_query(original.query) != _logical_query(current.query):
        raise SnapshotIntegrityError("封存输入只支持逻辑查询语义等同的重用")
    fields = (
        "availability_policy_hash", "revision_policy_hash", "field_types", "field_nullables",
        "primary_key", "event_time_field", "instrument_field", "input_claim_ceiling",
        "daily_availability_policy_ref", "daily_availability_rule", "session_close_binding",
        "minute_dataset_semantics_hash", "minute_asset_class", "minute_instrument_role",
        "minute_session_policy_ref", "minute_quality_policy_refs", "minute_timezone",
        "minute_timestamp_storage", "minute_timestamp_role", "minute_bar_interval",
        "minute_availability_rule", "minute_time_normalization_version", "result_cardinality",
    )
    for field in fields:
        before, after = getattr(original, field), getattr(current, field)
        if field in {"field_types", "field_nullables"}:
            before, after = dict(before), dict(after)
        if before != after:
            raise SnapshotIntegrityError(f"封存输入时间或逻辑合同改变: {field}")
    if original.temporal_selection.to_dict() != current.temporal_selection.to_dict():
        raise SnapshotIntegrityError("封存输入时态选择合同改变")
    if original.factor_publication is not None:
        raise SnapshotIntegrityError("封存输入不接收因子 publication")


def _resolve_entry(entry):
    """复验已有引用，只返回已封存的文件与 Arrow schema。"""
    import pyarrow.parquet as pq

    original = admitted_plan_from_dict(entry["original_plan"])
    root = Path(entry["root"])
    if entry["kind"] == "dataset":
        reference = DatasetArtifactRef.from_dict(entry["reference"])
        dataset = ParquetArtifactResolver(root).resolve(reference)
        if dataset.manifest.get("admitted_plan_hash") != original.plan_hash:
            raise SnapshotIntegrityError("归档快照未绑定所声明原准入")
        if dataset.manifest.get("temporal_source", False) is not original.temporal_selection.requires_consumer_binding:
            raise SnapshotIntegrityError("归档时态来源模式与原准入不一致")
        files = tuple(root / reference.relative_path / part for part in reference.partitions)
        if tuple(dataset.schema.names) != _snapshot_fields(original):
            raise SnapshotIntegrityError("归档快照列与原准入投影不一致")
        return original, reference, files, dataset.schema, dataset
    reference = PartitionedDatasetRef.from_dict(entry["reference"])
    if original.minute_dataset_semantics_hash is None or original.query.adjustment != "raw":
        raise SnapshotIntegrityError("封存分钟只支持已准入 raw 分钟")
    if reference.lineage.get("admitted_plan_hash") != original.plan_hash:
        raise SnapshotIntegrityError("分钟引用未绑定所声明原准入")
    columns = dict(original.columns)
    expected_columns = {columns[x] for x in original.query.field_ids}
    if set(reference.allowed_columns) != expected_columns:
        raise SnapshotIntegrityError("分钟引用列与原准入投影不一致")
    if reference.timestamp_field != columns[original.event_time_field] or reference.instrument_field != columns[original.instrument_field]:
        raise SnapshotIntegrityError("分钟引用时间或证券列不一致")
    if reference.instruments != original.query.universe.instruments or reference.universe_snapshot_id != original.query.universe.snapshot_id:
        raise SnapshotIntegrityError("分钟引用股票池与原准入不一致")
    span = original.query.time_range
    for partition in reference.partitions:
        if partition.source_kind != "catalog_raw" or partition.lineage.get("admitted_plan_hash") != original.plan_hash:
            raise SnapshotIntegrityError("分钟分区来源或原准入不一致")
        if not (partition.logical_start < span.end_at and partition.logical_end > span.start_at):
            raise SnapshotIntegrityError("分钟引用含原准入时间范围外的分区")
    if reference.partitions[0].logical_start > span.start_at or reference.partitions[-1].logical_end < span.end_at:
        raise SnapshotIntegrityError("分钟引用未覆盖原准入时间范围")
    for left, right in zip(reference.partitions, reference.partitions[1:]):
        if left.logical_end != right.logical_start:
            raise SnapshotIntegrityError("分钟引用分区范围存在缺口")
    resolver = PartitionedDatasetResolver({p.root_role: root for p in reference.partitions})
    resolved = resolver.resolve(reference)
    files = tuple(root / part.relative_path for part in reference.partitions)
    schema = pq.ParquetFile(files[0]).schema_arrow
    return original, reference, files, schema, resolved


def _archive_order_fields(plan):
    return tuple(dict.fromkeys((*plan.primary_key,
        *(plan.temporal_selection.source_order_fields if plan.temporal_selection.requires_consumer_binding else ()))))


def _archive_publish_batches(dataset, files, plan, schema, estimate, *, columns=None, filters=()):
    import pyarrow as pa

    if columns is not None or filters:
        selected_columns = columns or _snapshot_fields(plan)
        schema = pa.schema([schema.field(name) for name in selected_columns])
        source = dataset._iter_ordered_file_batches(
            columns=selected_columns, filters=filters, allowed_columns=tuple(dataset.schema.names),
            batch_size=min(estimate.provider_batch_rows, 65536))
    else:
        source = _snapshot_batches(dataset, files, plan, columns=_snapshot_fields(plan),
                                   batch_size=min(estimate.provider_batch_rows, 65536))
    if not estimate.has_order_by:
        # 准入已逐键证明全局顺序；发布门禁仍逐批检查顺序和 PIT。
        for batch in source:
            if 4 * batch.nbytes + 16 * batch.num_rows > estimate.provider_duckdb_memory_bytes:
                raise SnapshotIntegrityError("归档流式批次超过节点内存预算")
            yield batch
        return
    batches, total_bytes, rows = [], 0, 0
    for batch in source:
        total_bytes += batch.nbytes
        rows += batch.num_rows
        if 4 * total_bytes + 16 * rows > estimate.provider_duckdb_memory_bytes:
            raise SnapshotIntegrityError("归档 Arrow 排序超过节点内存预算")
        batches.append(batch)
    table = pa.Table.from_batches(batches, schema=schema)
    table = table.sort_by([(field, "ascending") for field in _archive_order_fields(plan)])
    yield from table.to_batches(max_chunksize=estimate.provider_batch_rows)

