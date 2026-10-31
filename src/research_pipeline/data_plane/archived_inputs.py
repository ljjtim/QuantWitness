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

from .archive_read import (
    _archive_order_fields, _archive_publish_batches, _logical_query, _resolve_entry, _snapshot_batches, _snapshot_fields, require_archived_plan_equivalent
)
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












class ArchivedInputInspector:
    """只用已验证 Arrow schema 提供归档物理 inventory。"""

    def __init__(self, entry, *, source_profile, environment, object_name, verified_schema=None):
        self.entry = entry
        self.source_profile = source_profile
        self.environment = environment
        self.object_name = object_name
        self._verified_schema = verified_schema

    def observe_current_schema(self, object_name):
        if object_name != self.object_name:
            raise SnapshotIntegrityError("归档 Inspector 不能观察未绑定对象")
        schema = self._verified_schema
        if schema is None:
            schema = _resolve_entry(self.entry)[3]
        return PhysicalInventory(
            "parquet", self.source_profile, self.environment, object_name,
            tuple(PhysicalColumn(field.name, str(field.type), field.nullable, index)
                  for index, field in enumerate(schema)),
        )


def _evidence(entry, plan, files, reference, *, dataset, execution_budget):
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
    ordered = False
    if entry["kind"] == "dataset":
        order_fields = _archive_order_fields(plan)
        string_fields = tuple(field for field, dtype in plan.field_types
                              if dtype == "string" and field in dataset.allowed_columns)
        columns = tuple(dict.fromkeys((*order_fields, *string_fields)))
        previous, ordered = None, True
        for batch in _snapshot_batches(dataset, files, plan, columns=columns,
                                       batch_size=min(plan.query.budget.batch_size, 65536)):
            offset, proof_rows = 0, batch.num_rows
            while offset < batch.num_rows:
                part = batch.slice(offset, min(proof_rows, batch.num_rows - offset))
                # 切片仍持有原 Arrow 批次；Python 投影仅在本次小批内存活。
                while batch.nbytes + 7 * part.nbytes + part.num_rows * len(columns) * 128 > execution_budget.memory_bytes:
                    if part.num_rows == 1:
                        raise SnapshotIntegrityError("归档顺序与字段宽度检查超过节点内存预算")
                    proof_rows = max(1, part.num_rows // 2)
                    part = batch.slice(offset, proof_rows)
                values = {field: part.column(field).to_pylist() for field in columns}
                for field in string_fields:
                    width_bounds[field] = max(width_bounds.get(field, 0),
                        max((len(value.encode("utf-8")) for value in values[field] if value is not None), default=0))
                for row in zip(*(values[field] for field in order_fields)):
                    key = tuple((value is None, value) for value in row)
                    if previous is not None and key <= previous:
                        ordered = False
                    previous = key
                offset += part.num_rows
                del values
    revision = reference.source_revision_hash if entry["kind"] == "dataset" else reference.reference_id
    partitioned = entry["kind"] == "minute"
    return ObjectExecutionEvidence(
        object_name=plan.object_name, object_kind="archived_parquet",
        dependency_chain=(plan.object_name,), source_rows_upper=rows,
        expanded_rows_upper=None, variable_width_upper=tuple(sorted(width_bounds.items())),
        has_json_expansion=False, has_window=False, has_order_by=not ordered,
        query_scope_hash=typed_canonical_hash(plan.query.to_dict()), database_revision=revision,
        expansion_bound_method=None, method="arrow_archived_ordered_v2" if ordered else "arrow_archived_input_v1",
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
        original, reference, files, schema, dataset = _resolve_entry(entry)
        if _logical_query(query) != _logical_query(original.query):
            raise SnapshotIntegrityError("封存输入只支持逻辑查询语义等同的重用")
        binding = catalog.bindings.get(entry["binding_id"])
        if binding is None:
            raise SnapshotIntegrityError("当前 Catalog 缺少已批准的归档 binding")
        if binding["source_profile"] == original.source_profile or binding["status"] != "approved":
            raise SnapshotIntegrityError("归档必须使用独立 parquet source_profile/binding")
        inspector = ArchivedInputInspector(entry, source_profile=binding["source_profile"], environment=binding["environment"], object_name=binding["object_name"], verified_schema=schema)
        resolved, attestation = CatalogPreflight(catalog).resolve_current_binding(
            inspector=inspector, dataset_id=query.dataset_id, dataset_version=query.dataset_version,
            source_profile=binding["source_profile"], environment=binding["environment"],
            binding_version=binding["binding_version"],
        )
        plan = admit_query(query, catalog=catalog, binding=resolved, attestation=attestation)
        require_archived_plan_equivalent(original, plan)
        expected_mapping = {field_id: field_id for field_id in _snapshot_fields(original)} if entry["kind"] == "dataset" else dict(original.columns)
        if dict(plan.columns) != expected_mapping:
            raise SnapshotIntegrityError("归档物理字段映射与已封存列不一致")
        evidence = _evidence(entry, plan, files, reference, dataset=dataset, execution_budget=execution_budgets[request_id])
        estimate = build_execution_estimate(plan, evidence=evidence, execution_budget=execution_budgets[request_id])
        if entry["kind"] == "dataset" and evidence.has_order_by:
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


def rebind_archived_minute(entry, plan, *, resolved=None):
    """复验分钟文件，保留原引用并绑定当前新准入，不复制分钟行。"""
    original, reference, _, _, _ = resolved if resolved is not None else _resolve_entry(entry)
    if entry["kind"] != "minute":
        raise SnapshotIntegrityError("来源不是分钟分区引用")
    require_archived_plan_equivalent(original, plan)
    lineage = {**dict(reference.lineage), "admitted_plan_hash": plan.plan_hash,
               "archived_source_reference_id": reference.reference_id,
               "archived_original_plan_hash": original.plan_hash}
    partitions = tuple(replace(partition, lineage={**dict(partition.lineage), **lineage}) for partition in reference.partitions)
    return replace(reference, dataset_id=f"minute/{plan.plan_hash}", lineage=lineage, partitions=partitions)






def materialize_archived_inputs(*, manifest, admitted_plans, execution_estimates, execution_budget, artifact_root, persist_inputs=False):
    """默认引用原归档；显式保存时按预算发布快照，保留全部版本。"""

    if type(persist_inputs) is not bool:
        raise SnapshotIntegrityError("persist_inputs 必须是布尔值")
    manifest = load_archived_input_manifest(manifest)
    if set(admitted_plans) - set(manifest["requests"]) or set(admitted_plans) - set(execution_estimates):
        raise SnapshotIntegrityError("归档物化计划、估算和清单集合不闭合")
    root = Path(artifact_root)
    references, verified = {}, {}
    for request_id, plan in sorted(admitted_plans.items()):
        entry = manifest["requests"][request_id]
        request_status = "not_opened"
        try:
            resolved = _resolve_entry(entry)
            original, reference, files, schema, dataset = resolved
            request_status = "opened"
            require_archived_plan_equivalent(original, plan)
            estimate = execution_estimates[request_id]
            if estimate.query_scope_hash != typed_canonical_hash(plan.query.to_dict()) or estimate.object_name != plan.object_name or not estimate.accepts_runtime_budget(execution_budget):
                raise SnapshotIntegrityError("归档物化预算证据与当前计划不一致")
            if entry["kind"] == "minute":
                references[request_id] = rebind_archived_minute(entry, plan, resolved=resolved).to_dict()
                continue
            if not persist_inputs:
                from dataclasses import asdict
                from .direct_sources import DirectSourceSchema, publish_direct_descriptor, no_spill_budget
                direct_budget, direct_estimate = no_spill_budget(execution_budget, estimate)
                payload = {
                    "contract_version": "direct-source-descriptor-v1",
                    "source_kind": "archived_parquet", "archive_entry": dict(entry),
                    "plan": plan.to_dict(), "admitted_plan_hash": plan.plan_hash,
                    "temporal_source": plan.temporal_selection.requires_consumer_binding,
                    "schema": asdict(DirectSourceSchema.from_arrow_schema(schema)),
                    "execution_budget": direct_budget.to_dict(),
                    "execution_estimate": direct_estimate.to_dict(),
                    "row_count": dataset.manifest["row_count"],
                }
                direct_reference = publish_direct_descriptor(payload, artifact_root=root)
                references[request_id] = direct_reference.to_dict()
                verified[request_id] = dict(payload, source_identity=direct_reference.source_identity,
                    schema_hash=direct_reference.schema_hash, source_revision_hash=direct_reference.source_revision_hash)
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
                batches = _archive_publish_batches(dataset, files, original, schema, estimate)
                path = publish_parquet_snapshot(batches=batches,
                    schema=schema, plan=plan, logical_snapshot=logical, root=root,
                    before_commit=lambda entry=entry: _resolve_entry(entry),
                    temporal_source=plan.temporal_selection.requires_consumer_binding)
            references[request_id] = build_dataset_artifact_ref(path, allowed_root=root).to_dict()
            verified[request_id] = verify_parquet_snapshot(path)
        except Exception as exc:
            exc.failure_payload = {
                "contract_version": "data-plane-request-failure-v1",
                "request_status": request_status, "request_id": request_id,
                "dataset_id": plan.query.dataset_id, "binding_id": plan.binding_id,
                "object_name": plan.object_name, "provider": "archived_parquet",
                "output_budget": plan.query.budget.to_dict(),
                "execution_budget": execution_budget.to_dict(),
                "completed_request_ids": sorted(references),
                "underlying_exception_type": type(exc).__name__,
            }
            raise
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
