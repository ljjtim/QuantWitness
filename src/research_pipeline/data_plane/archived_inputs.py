"""封存输入的当前准入、Arrow 读取与快照发布。

公开入口：
load_archived_input_manifest(path_or_payload) -> 规范化 dict（原计划内联，根为绝对路径）。
admit_archived_queries(queries, *, catalog, execution_budgets, manifest)
    -> (admitted, estimates, platform_admission)
materialize_archived_inputs(*, manifest, admitted_plans, execution_estimates,
    execution_budget, artifact_root) -> (data_bundle, verified_manifests)

清单：{"contract_version":"archived-input-manifest-v1", "requests": {
 request_id: {"kind":"dataset"|"minute", "original_plan": <文件路径或plan字典>,
 "root": <快照允许根或minute根>, "reference": <现有引用字典>,
 "binding_id": <当前Catalog明确批准的归档binding>}}}。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
import json
from pathlib import Path

from research_pipeline.catalog import CatalogPreflight
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory, ObjectExecutionEvidence
from research_pipeline.platform.canonical import typed_canonical_hash

from .admission import admit_query
from .admitted_plan_codec import admitted_plan_from_dict
from .dataset_artifacts import ArtifactResolver, DatasetArtifactRef, build_dataset_artifact_ref
from .errors import SnapshotIntegrityError
from .execution_estimate import build_execution_estimate
from .partitioned_artifacts import PartitionedDatasetRef, PartitionedDatasetResolver
from .research_data_bundle import build_research_data_bundle
from .revision import SourceFileEvidence, SourceRevision
from .snapshot_identity import build_logical_snapshot
from .snapshots import publish_parquet_snapshot, verify_parquet_snapshot


ARCHIVED_INPUT_MANIFEST_VERSION = "archived-input-manifest-v1"
_ENTRY_FIELDS = {"kind", "original_plan", "root", "reference", "binding_id"}


def load_archived_input_manifest(path_or_payload):
    """把来源路径和原计划冻结为可随当前 plan 保存的清单。"""
    if isinstance(path_or_payload, Mapping):
        payload = dict(path_or_payload)
        base = Path.cwd()
    else:
        path = Path(path_or_payload).resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        base = path.parent
    if not isinstance(payload, Mapping) or set(payload) != {"contract_version", "requests"}:
        raise SnapshotIntegrityError("封存输入清单 schema 无效")
    if payload["contract_version"] != ARCHIVED_INPUT_MANIFEST_VERSION:
        raise SnapshotIntegrityError("封存输入清单版本不受支持")
    entries = payload["requests"]
    if not isinstance(entries, Mapping) or not entries:
        raise SnapshotIntegrityError("封存输入 requests 必须非空")
    normalized = {}
    for request_id, raw in sorted(entries.items()):
        if not isinstance(request_id, str) or not request_id or not isinstance(raw, Mapping) or set(raw) != _ENTRY_FIELDS:
            raise SnapshotIntegrityError("封存输入 request schema 无效")
        if raw["kind"] not in {"dataset", "minute"} or not isinstance(raw["reference"], Mapping):
            raise SnapshotIntegrityError("封存输入仅接受 DatasetArtifactRef 或 PartitionedDatasetRef")
        original = raw["original_plan"]
        if not isinstance(original, Mapping):
            original_path = Path(original)
            if not original_path.is_absolute():
                original_path = base / original_path
            original = json.loads(original_path.read_text(encoding="utf-8"))
        plan = admitted_plan_from_dict(dict(original))
        if original.get("plan_hash") != plan.plan_hash:
            raise SnapshotIntegrityError("封存输入原准入身份不一致")
        if not isinstance(raw["binding_id"], str) or not raw["binding_id"]:
            raise SnapshotIntegrityError("封存输入必须声明当前归档 binding_id")
        root = Path(raw["root"])
        if not root.is_absolute():
            root = base / root
        normalized[request_id] = {
            "kind": raw["kind"], "original_plan": plan.to_dict(),
            "root": str(root.resolve()), "reference": dict(raw["reference"]),
            "binding_id": raw["binding_id"],
        }
    return {"contract_version": ARCHIVED_INPUT_MANIFEST_VERSION, "requests": normalized}


def verify_archived_input_manifest(manifest):
    """节点复用前复验冻结引用，文件变化不能被 checkpoint 掩盖。"""
    manifest = load_archived_input_manifest(manifest)
    for entry in manifest["requests"].values():
        _resolve_entry(entry)


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
    if original.temporal_selection.requires_consumer_binding or original.factor_publication is not None:
        raise SnapshotIntegrityError("首批封存输入不接收逐决策版本来源或因子 publication")


def _resolve_entry(entry):
    """复验已有引用，只返回已封存的文件与 Arrow schema。"""
    import pyarrow.parquet as pq

    original = admitted_plan_from_dict(entry["original_plan"])
    root = Path(entry["root"])
    if entry["kind"] == "dataset":
        reference = DatasetArtifactRef.from_dict(entry["reference"])
        dataset = ArtifactResolver(root).resolve(reference)
        if dataset.manifest.get("admitted_plan_hash") != original.plan_hash:
            raise SnapshotIntegrityError("归档快照未绑定所声明原准入")
        if dataset.manifest.get("temporal_source") is True:
            raise SnapshotIntegrityError("首批封存输入不支持逐决策时态快照")
        files = tuple(root / reference.relative_path / part for part in reference.partitions)
        if tuple(dataset.schema.names) != original.query.field_ids:
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


class ArchivedInputInspector:
    """只用已验证 Arrow schema 提供归档物理 inventory。"""

    def __init__(self, entry, *, source_profile, environment, object_name):
        self.entry = entry
        self.source_profile = source_profile
        self.environment = environment
        self.object_name = object_name

    def observe_current_schema(self, object_name):
        if object_name != self.object_name:
            raise SnapshotIntegrityError("归档 Inspector 不能观察未绑定对象")
        schema = _resolve_entry(self.entry)[3]
        return PhysicalInventory(
            "parquet", self.source_profile, self.environment, object_name,
            tuple(PhysicalColumn(field.name, str(field.type), field.nullable, index)
                  for index, field in enumerate(schema)),
        )


def _evidence(entry, plan, files, reference):
    import pyarrow.parquet as pq
    from .execution_budget import _CANONICAL_INSTRUMENT_UTF8_BYTES

    rows = 0
    largest_rows = 0
    largest_bytes = 0
    width_bounds = {}
    physical_columns = dict(plan.columns)
    for file in files:
        metadata = pq.ParquetFile(file).metadata
        rows += metadata.num_rows
        largest_rows = max(largest_rows, metadata.num_rows)
        file_bytes = 0
        for group_index in range(metadata.num_row_groups):
            group = metadata.row_group(group_index)
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                matching = [field_id for field_id, name in physical_columns.items() if name == column.path_in_schema]
                if not matching:
                    continue
                file_bytes += column.total_uncompressed_size
                for field_id in matching:
                    if dict(plan.field_types).get(field_id) == "string":
                        # 页脚编码大小不是单元格宽度；通用表下方用有界投影读取实测。
                        if entry["kind"] == "minute" and field_id == plan.instrument_field:
                            width_bounds[field_id] = _CANONICAL_INSTRUMENT_UTF8_BYTES
        largest_bytes = max(largest_bytes, file_bytes)
    if entry["kind"] == "dataset":
        dataset = _resolve_entry(entry)[4]
        for field_id, dtype in plan.field_types:
            if dtype != "string":
                continue
            maximum = 0
            for batch in dataset.iter_batches(columns=(field_id,), batch_size=min(plan.query.budget.batch_size, 65536)):
                for value in batch.column(0).to_pylist():
                    if value is not None:
                        maximum = max(maximum, len(value.encode("utf-8")))
            width_bounds[field_id] = maximum
    revision = reference.source_revision_hash if entry["kind"] == "dataset" else reference.reference_id
    partitioned = entry["kind"] == "minute"
    return ObjectExecutionEvidence(
        object_name=plan.object_name, object_kind="archived_parquet",
        dependency_chain=(plan.object_name,), source_rows_upper=rows,
        expanded_rows_upper=None, variable_width_upper=tuple(sorted(width_bounds.items())),
        has_json_expansion=False, has_window=False, has_order_by=True,
        query_scope_hash=typed_canonical_hash(plan.query.to_dict()), database_revision=revision,
        expansion_bound_method=None, method="arrow_archived_input_v1",
        partition_count=len(files) if partitioned else 0,
        partition_rows_upper=largest_rows if partitioned else None,
        partition_uncompressed_bytes_upper=largest_bytes if partitioned else None,
        partition_key="archived_month" if partitioned else None,
        partition_bound_method="parquet_footer_month_scope_v1" if partitioned else None,
    )


def admit_archived_queries(queries, *, catalog, execution_budgets, manifest):
    """使用调用方已编译的 QueryIR 完成当前归档准入。"""
    manifest = load_archived_input_manifest(manifest)
    if set(queries) != set(manifest["requests"]) or set(queries) != set(execution_budgets):
        raise SnapshotIntegrityError("封存输入、研究请求和节点预算集合不闭合")
    admitted, estimates, attestations, source_facts = {}, {}, [], {}
    for request_id, query in sorted(queries.items()):
        entry = manifest["requests"][request_id]
        original, reference, files, schema, _ = _resolve_entry(entry)
        if _logical_query(query) != _logical_query(original.query):
            raise SnapshotIntegrityError("封存输入只支持逻辑查询语义等同的重用")
        binding = catalog.bindings.get(entry["binding_id"])
        if binding is None:
            raise SnapshotIntegrityError("当前 Catalog 缺少已批准的归档 binding")
        if binding["source_profile"] == original.source_profile or binding["status"] != "approved":
            raise SnapshotIntegrityError("归档必须使用独立 parquet source_profile/binding")
        inspector = ArchivedInputInspector(entry, source_profile=binding["source_profile"], environment=binding["environment"], object_name=binding["object_name"])
        resolved, attestation = CatalogPreflight(catalog).resolve_current_binding(
            inspector=inspector, dataset_id=query.dataset_id, dataset_version=query.dataset_version,
            source_profile=binding["source_profile"], environment=binding["environment"],
            binding_version=binding["binding_version"],
        )
        plan = admit_query(query, catalog=catalog, binding=resolved, attestation=attestation)
        require_archived_plan_equivalent(original, plan)
        expected_mapping = {field_id: field_id for field_id in original.query.field_ids} if entry["kind"] == "dataset" else dict(original.columns)
        if dict(plan.columns) != expected_mapping:
            raise SnapshotIntegrityError("归档物理字段映射与已封存列不一致")
        evidence = _evidence(entry, plan, files, reference)
        estimate = build_execution_estimate(plan, evidence=evidence, execution_budget=execution_budgets[request_id])
        if entry["kind"] == "dataset":
            required = 4 * evidence.source_rows_upper * estimate.projected_row_width_upper + 16 * evidence.source_rows_upper
            if required > estimate.provider_duckdb_memory_bytes:
                raise SnapshotIntegrityError("归档 Arrow 排序超过节点内存预算")
        admitted[request_id], estimates[request_id] = plan, estimate
        attestations.append(attestation.to_dict())
        source_facts[request_id] = {"kind": entry["kind"], "reference": entry["reference"], "original_plan_hash": original.plan_hash}
    pit = {request_id: {"availability_policy_hash": plan.availability_policy_hash, "revision_policy_hash": plan.revision_policy_hash, "query": plan.query.to_dict()} for request_id, plan in admitted.items()}
    return admitted, estimates, {
        "catalog_hash": catalog.catalog_hash,
        "pit_contract_hash": typed_canonical_hash(pit),
        "drift_proof_hash": typed_canonical_hash({"catalog_hash": catalog.catalog_hash, "attestations": attestations, "archived_sources": source_facts}),
        "execution_estimates_hash": typed_canonical_hash({key: value.to_dict() for key, value in estimates.items()}),
    }


def rebind_archived_minute(entry, plan):
    """复验分钟文件，保留原引用并绑定当前新准入，不复制分钟行。"""
    original, reference, _, _, _ = _resolve_entry(entry)
    if entry["kind"] != "minute":
        raise SnapshotIntegrityError("来源不是分钟分区引用")
    require_archived_plan_equivalent(original, plan)
    lineage = {**dict(reference.lineage), "admitted_plan_hash": plan.plan_hash,
               "archived_source_reference_id": reference.reference_id,
               "archived_original_plan_hash": original.plan_hash}
    partitions = tuple(replace(partition, lineage={**dict(partition.lineage), **lineage}) for partition in reference.partitions)
    return replace(reference, dataset_id=f"minute/{plan.plan_hash}", lineage=lineage, partitions=partitions)


def materialize_archived_inputs(*, manifest, admitted_plans, execution_estimates, execution_budget, artifact_root):
    """在节点预算内按主键重排，再发布当前计划绑定的普通快照。"""
    import pyarrow as pa

    manifest = load_archived_input_manifest(manifest)
    if set(admitted_plans) - set(manifest["requests"]) or set(admitted_plans) - set(execution_estimates):
        raise SnapshotIntegrityError("归档物化计划、估算和清单集合不闭合")
    root = Path(artifact_root)
    references, verified = {}, {}
    for request_id, plan in sorted(admitted_plans.items()):
        entry = manifest["requests"][request_id]
        original, reference, files, schema, dataset = _resolve_entry(entry)
        require_archived_plan_equivalent(original, plan)
        estimate = execution_estimates[request_id]
        if estimate.query_scope_hash != typed_canonical_hash(plan.query.to_dict()) or estimate.object_name != plan.object_name or not estimate.accepts_runtime_budget(execution_budget):
            raise SnapshotIntegrityError("归档物化预算证据与当前计划不一致")
        if entry["kind"] == "minute":
            references[request_id] = rebind_archived_minute(entry, plan).to_dict()
            continue
        file_hashes = {
            str(Path(entry["root"]) / reference.relative_path / item["relative_path"]): item["sha256"]
            for item in dataset.manifest["files"]
        }
        source = SourceRevision(
            source_id=plan.binding_id, source_kind="archived_parquet",
            files=tuple(SourceFileEvidence(file.relative_to(entry["root"]).as_posix(),
                file.stat().st_size, file.stat().st_mtime_ns, file_hashes[str(file)]) for file in files),
            schema_hash=reference.schema_hash, watermark=reference.manifest_hash,
            evidence_strength="manifest_hash",
        )
        logical = build_logical_snapshot(plan, source_revisions=(source,), output_schema=schema,
            provider_version="arrow-archived-input-v1", compiler_version="archived-query-equivalence-v1")
        path = _existing_snapshot(root, logical.logical_snapshot_id, plan)
        if path is None:
            batches, total_bytes, rows = [], 0, 0
            for batch in dataset.iter_batches(columns=plan.query.field_ids, batch_size=min(estimate.provider_batch_rows, 65536)):
                total_bytes += batch.nbytes
                rows += batch.num_rows
                if 4 * total_bytes + 16 * rows > estimate.provider_duckdb_memory_bytes:
                    raise SnapshotIntegrityError("归档 Arrow 排序超过节点内存预算")
                batches.append(batch)
            table = pa.Table.from_batches(batches, schema=schema)
            table = table.sort_by([(field, "ascending") for field in plan.primary_key])
            path = publish_parquet_snapshot(batches=table.to_batches(max_chunksize=estimate.provider_batch_rows),
                schema=table.schema, plan=plan, logical_snapshot=logical, root=root,
                before_commit=lambda entry=entry: _resolve_entry(entry))
        references[request_id] = build_dataset_artifact_ref(path, allowed_root=root).to_dict()
        verified[request_id] = verify_parquet_snapshot(path)
    return build_research_data_bundle(admitted_plan_hashes={key: value.plan_hash for key, value in admitted_plans.items()}, references=references), verified



def _existing_snapshot(root, logical_id, plan):
    """直接复用当前逻辑身份的已提交快照，恢复不重读排序源表。"""
    from .gates import gate_policy_hash

    parent = root / logical_id[:2] / logical_id
    if not parent.exists():
        return None
    for path in sorted(parent.iterdir()):
        if not path.is_dir() or not (path / "COMMITTED").is_file():
            continue
        manifest = verify_parquet_snapshot(path)
        if (manifest.get("logical_snapshot_id"), manifest.get("admitted_plan_hash"), manifest.get("gate_policy_hash")) != (logical_id, plan.plan_hash, gate_policy_hash(plan)):
            raise SnapshotIntegrityError("已提交归档快照的当前准入或门禁身份不一致")
        return path
    return None


def validate_archived_minute_scan(manifest, request_id, scan_plan, minute_root):
    """新扫描计划只能引用冻结的原分钟文件集合和查询范围。"""
    manifest = load_archived_input_manifest(manifest)
    entry = manifest["requests"].get(request_id)
    if entry is None or entry["kind"] != "minute":
        raise SnapshotIntegrityError("分钟扫描缺少冻结来源")
    if Path(minute_root).resolve() != Path(entry["root"]).resolve():
        raise SnapshotIntegrityError("分钟扫描根与冻结来源不一致")
    original, reference, _, _, _ = _resolve_entry(entry)
    if (scan_plan.start_at, scan_plan.end_at, scan_plan.as_of) != (original.query.time_range.start_at, original.query.time_range.end_at, original.query.as_of_instant):
        raise SnapshotIntegrityError("分钟扫描时间范围与冻结原准入不一致")
    if set(scan_plan.source_columns) != set(reference.allowed_columns) or scan_plan.instruments != reference.instruments or scan_plan.universe_snapshot_id != reference.universe_snapshot_id:
        raise SnapshotIntegrityError("分钟扫描投影或股票池与冻结来源不一致")
    expected = {part.relative_path: part for part in reference.partitions}
    if {part.relative_path for part in scan_plan.partitions} != set(expected):
        raise SnapshotIntegrityError("分钟扫描文件集合与冻结来源不一致")
    for part in scan_plan.partitions:
        previous = expected[part.relative_path]
        if any(getattr(part, name) != getattr(previous, name) for name in ("size", "mtime_ns", "row_count", "row_groups", "schema_hash")):
            raise SnapshotIntegrityError("分钟扫描物理文件与冻结来源不一致")
