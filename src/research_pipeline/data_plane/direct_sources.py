"""直接来源描述、只读分批扫描及恢复时来源复验。"""
from __future__ import annotations

import base64
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
import json
from pathlib import Path
from types import MappingProxyType

from factor_contracts import FactorReadLease
from research_pipeline.platform.canonical import typed_canonical_hash
from .admitted_plan_codec import admitted_plan_from_dict
from .dataset_files import DirectDatasetRef, VerifiedDataset, _filter_expression
from .errors import SnapshotIntegrityError
from .execution_budget import DataPlaneExecutionBudget
from .execution_estimate import ExecutionEstimate
from .factor_publication import require_factor_publication_unchanged
from .revision import probe_duckdb_source


@dataclass(frozen=True)
class DirectSourceSchema:
    """保留字段类型、时区、精度及元数据的 Arrow schema。"""
    ipc_base64: str

    @classmethod
    def from_arrow_schema(cls, schema):
        return cls(base64.b64encode(schema.serialize().to_pybytes()).decode("ascii"))

    def to_arrow_schema(self):
        import pyarrow as pa
        return pa.ipc.read_schema(pa.BufferReader(base64.b64decode(self.ipc_base64, validate=True)))


def no_spill_budget(execution_budget, execution_estimate):
    """只收紧临时空间上限；已有估算要求落盘时拒绝直接执行。"""
    if not execution_estimate.accepts_runtime_budget(execution_budget):
        raise SnapshotIntegrityError("直接读取预算低于准入预算")
    if execution_estimate.required_temp_bytes or execution_estimate.may_spill:
        raise SnapshotIntegrityError("直接读取禁止 spill，请提高内存后重新准入或显式保存输入")
    return (replace(execution_budget, temp_bytes=0),
            replace(execution_estimate, execution_temp_bytes=0))


def publish_direct_descriptor(payload, *, artifact_root):
    payload = dict(payload)
    schema = DirectSourceSchema(**payload["schema"]).to_arrow_schema()
    schema_hash = typed_canonical_hash(str(schema))
    if payload["source_kind"] == "duckdb":
        revision_hash = typed_canonical_hash(payload["source_revision"])
        content_identity = revision_hash
    else:
        original_ref = payload["archive_entry"]["reference"]
        revision_hash = original_ref["source_revision_hash"]
        content_identity = original_ref["manifest_hash"]
    source_identity = typed_canonical_hash({
        "admitted_plan_hash": payload["admitted_plan_hash"],
        "source_revision_hash": revision_hash, "schema_hash": schema_hash,
        "source_content_identity": content_identity, "implementation": "direct-source-reader-v1",
        "provider_version": payload.get("provider_version"),
        "compiler_version": payload.get("compiler_version"),
    })
    payload.update(source_identity=source_identity, schema_hash=schema_hash,
                   source_revision_hash=revision_hash)
    identity = typed_canonical_hash(payload)
    relative = f"direct/{identity}.json"
    from research_pipeline.platform.canonical import canonical_json
    path = Path(artifact_root) / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(canonical_json(payload), encoding='utf-8')
    temporary.replace(path)
    return DirectDatasetRef(relative, identity, source_identity, schema_hash, revision_hash)


class DirectVerifiedDataset:
    """与文件数据集共用 PIT 选择，扫描来源由只读 provider 提供。"""
    iter_batches_at = VerifiedDataset.iter_batches_at
    iter_temporal_fact_batches = VerifiedDataset.iter_temporal_fact_batches
    _record_batch = VerifiedDataset._record_batch
    __reduce__ = VerifiedDataset.__reduce__

    def __init__(self, *, descriptor, reference, resolver):
        self.manifest = MappingProxyType(dict(descriptor))
        self._reference = reference
        self._resolver = resolver
        self.plan = admitted_plan_from_dict(descriptor["plan"])
        self.schema = DirectSourceSchema(**descriptor["schema"]).to_arrow_schema()
        self.allowed_columns = tuple(self.schema.names)
        self.max_batch_rows = resolver.max_batch_rows
        self.max_batch_bytes = resolver.max_batch_bytes
        self.budget = DataPlaneExecutionBudget(**descriptor["execution_budget"])
        self.estimate = ExecutionEstimate.from_dict(descriptor["execution_estimate"])

    def _verify(self):
        payload = _load_descriptor(self._resolver, self._reference)
        if payload != dict(self.manifest):
            raise SnapshotIntegrityError("直接来源绑定发生变化")
        verify_direct_source(payload, self.plan)

    @contextmanager
    def _source(self, *, columns=None, filters=()):
        database = self.manifest.get("database")
        lease = FactorReadLease(database) if self.plan.factor_publication else nullcontext()
        with lease:
            self._verify()
            try:
                if self.manifest["source_kind"] == "archived_parquet":
                    from .archive_read import _resolve_entry
                    yield _resolve_entry(self.manifest["archive_entry"])
                else:
                    with open_projected_stream(
                        database, self.plan, self.budget, self.estimate,
                        columns=columns or self.allowed_columns, filters=filters,
                    ) as stream:
                        import pyarrow as pa
                        expected = pa.schema([self.schema.field(name) for name in (columns or self.allowed_columns)], metadata=self.schema.metadata)
                        if stream.schema != expected:
                            raise SnapshotIntegrityError("直接来源输出 schema 发生变化")
                        yield stream
            finally:
                self._verify()

    def iter_batches(self, *, columns, filters=(), batch_size=65_536):
        if self.manifest["temporal_source"]:
            raise SnapshotIntegrityError("逐决策时态来源必须通过 iter_batches_at 读取")
        yield from self._scan(columns=columns, filters=filters,
                              allowed_columns=self.allowed_columns, batch_size=batch_size)

    def _iter_ordered_file_batches(self, *, columns, filters, allowed_columns, batch_size):
        yield from self._scan(columns=columns, filters=filters,
                              allowed_columns=allowed_columns, batch_size=batch_size)

    def _scan(self, *, columns, filters, allowed_columns, batch_size):
        import pyarrow as pa
        import pyarrow.dataset as ds
        if not columns or len(columns) != len(set(columns)) or not set(columns) <= set(allowed_columns):
            raise SnapshotIntegrityError("扫描列必须非空、唯一且已经批准")
        if type(batch_size) is not int or not 0 < batch_size <= self.max_batch_rows:
            raise SnapshotIntegrityError("扫描 batch_size 超过批准上限")
        expression = _filter_expression(filters, allowed_columns=allowed_columns, schema=self.schema)
        # 质量检查所需的键、证券和时点随投影读取，不扩大为全部业务列。
        scan_columns = (self.plan.temporal_selection.required_scan_fields
                        if self.manifest["temporal_source"] else tuple(dict.fromkeys(
                            (*columns, *self.plan.primary_key, self.plan.event_time_field, self.plan.instrument_field))))
        from .gates import QualityGateSession
        gate_plan = (self.plan if self.manifest["temporal_source"] else replace(
            self.plan, query=replace(self.plan.query, field_ids=scan_columns),
            temporal_selection=replace(self.plan.temporal_selection, public_projection=scan_columns)))
        gate = QualityGateSession(gate_plan, temporal_source=self.manifest["temporal_source"])
        with self._source(columns=scan_columns, filters=filters) as source:
            if self.manifest["source_kind"] == "archived_parquet":
                # 归档先下推日期、证券和投影；需要排序时只整理本次裁剪范围。
                from .archive_read import _archive_publish_batches
                original, _, files, schema, dataset = source
                batches = _archive_publish_batches(dataset, files, original, schema, self.estimate,
                                                   columns=scan_columns, filters=filters)
            else:
                batches = iter(source)
            for batch in batches:
                if batch.nbytes > self.max_batch_bytes:
                    raise SnapshotIntegrityError("直接读取 RecordBatch 超过批准内存上限")
                table = pa.Table.from_batches([batch])
                gate_batch = table.select(list(scan_columns)).to_batches()[0] if batch.num_rows else None
                if gate_batch is not None:
                    gate.validate_batch(gate_batch)
                if expression is not None and self.manifest["source_kind"] == "archived_parquet":
                    table = ds.dataset(table).scanner(filter=expression, use_threads=False).to_table()
                yield from table.select(list(columns)).to_batches(max_chunksize=batch_size)

    def parquet_uncompressed_bytes(self, *, columns):
        raise SnapshotIntegrityError("直接来源没有 Parquet footer；请使用执行估算做内存准入")


def _load_descriptor(resolver, reference):
    path = resolver.path_policy.resolve_contained_path(
        allowed_root=resolver.allowed_root, candidate=reference.relative_path,
        root_role="dataset_artifact_allowed_root", path_role="direct_source_descriptor",
        expected_kind="file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeError) as exc:
        raise SnapshotIntegrityError("直接来源描述无法解析") from exc
    if typed_canonical_hash(payload) != reference.descriptor_hash:
        raise SnapshotIntegrityError("直接来源描述身份发生变化")
    for name in ("source_identity", "schema_hash", "source_revision_hash"):
        if payload.get(name) != getattr(reference, name):
            raise SnapshotIntegrityError(f"直接来源 {name} 发生变化")
    return payload


def verify_direct_source(payload, plan):
    if payload["admitted_plan_hash"] != plan.plan_hash:
        raise SnapshotIntegrityError("直接来源与准入计划身份不一致")
    if payload["source_kind"] == "archived_parquet":
        from .archive_read import _resolve_entry, require_archived_plan_equivalent
        original, _, _, schema, _ = _resolve_entry(payload["archive_entry"])
        require_archived_plan_equivalent(original, plan)
        if schema != DirectSourceSchema(**payload["schema"]).to_arrow_schema():
            raise SnapshotIntegrityError("归档直接来源 schema 变化")
        return
    if payload["source_kind"] != "duckdb":
        raise SnapshotIntegrityError("直接来源类型不受支持")
    database = Path(payload["database"])
    if plan.factor_publication:
        require_factor_publication_unchanged(plan.factor_publication, database, storage_table=plan.object_name)
    revision = probe_duckdb_source(database, allowed_root=database.parent,
                                  object_name=plan.object_name, source_id=plan.binding_id)
    if revision.to_dict() != payload["source_revision"]:
        raise SnapshotIntegrityError("直接来源修订发生变化，不能恢复旧输入")


def resolve_direct_dataset(resolver, reference):
    descriptor = _load_descriptor(resolver, reference)
    dataset = DirectVerifiedDataset(descriptor=descriptor, reference=reference, resolver=resolver)
    database = descriptor.get("database")
    lease = FactorReadLease(database) if dataset.plan.factor_publication else nullcontext()
    with lease:
        dataset._verify()
    return dataset


def projected_query(plan, estimate, *, columns, filters):
    """在冻结计划外包装参数化投影与过滤，让 DuckDB 下推日期及证券条件。"""
    from .providers.sql import compile_duckdb_query, compile_json_array_projection_source
    source = (None if estimate.json_array_projection is None
              else compile_json_array_projection_source(estimate.json_array_projection))
    sql, params = compile_duckdb_query(
        plan, source_expression=source,
        preserve_temporal_facts=plan.temporal_selection.requires_consumer_binding)
    # 无用户 limit 时，预算上限由 ColumnarStream 执行；去掉编译器的预算
    # LIMIT，避免它阻挡逐日谓词下推。显式 limit 仍保留原查询语义。
    if plan.query.limit is None and not plan.temporal_selection.requires_consumer_binding:
        if sql.endswith(" LIMIT ?"):
            sql = sql[:-len(" LIMIT ?")]
            params = params[:-1]
    def quote(name):
        return '"' + name.replace('"', '""') + '"'
    predicates = []
    params = list(params)
    for item in filters:
        field = quote(item.field)
        if item.operator in {"is_null", "is_valid"}:
            predicates.append(field + (" IS NULL" if item.operator == "is_null" else " IS NOT NULL"))
        elif item.operator == "in":
            predicates.append(field + " IN (" + ",".join("?" for _ in item.value) + ")")
            params.extend(item.value)
        else:
            operator = {"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[item.operator]
            predicates.append(f"{field} {operator} ?")
            params.append(item.value)
    wrapped = "SELECT " + ",".join(quote(name) for name in columns) + " FROM (" + sql.rstrip().rstrip(";") + ") AS direct_input"
    if predicates:
        wrapped += " WHERE " + " AND ".join(predicates)
    if plan.temporal_selection.requires_consumer_binding:
        order = [(name, False) for name in dict.fromkeys((*plan.primary_key, *plan.temporal_selection.source_order_fields))]
    else:
        order = [(item.field_id, item.descending) for item in plan.query.sort]
    if order:
        wrapped += " ORDER BY " + ",".join(quote(name) + (" DESC" if descending else " ASC") for name, descending in order)
    return wrapped, params


def open_projected_stream(database, plan, budget, estimate, *, columns, filters, schema_only=False):
    """沿用 provider 分配与列式流合同，使用只读连接执行包装 SQL，禁用落盘。"""
    import duckdb
    from .execution_budget import build_provider_execution_plan
    from .stream import ColumnarStream
    if budget.temp_bytes != 0 or estimate.execution_temp_bytes != 0 or estimate.required_temp_bytes:
        raise SnapshotIntegrityError("直接读取不允许临时落盘")
    if not estimate.accepts_runtime_budget(budget) or estimate.query_scope_hash != typed_canonical_hash(plan.query.to_dict()) or estimate.object_name != plan.object_name:
        raise SnapshotIntegrityError("直接读取执行估算与计划或预算不一致")
    allocation = build_provider_execution_plan(plan, budget, variable_width_upper=dict(estimate.variable_width_upper))
    sql, params = projected_query(plan, estimate, columns=columns, filters=filters)
    if schema_only:
        sql = "SELECT * FROM (" + sql + ") AS direct_schema WHERE FALSE"
    connection = duckdb.connect(str(database), read_only=True)
    try:
        connection.execute(f"SET threads = {allocation.cpu_slots:d}")
        connection.execute(f"SET memory_limit = '{allocation.duckdb_memory_bytes:d}B'")
        connection.execute("SET temp_directory = ''")
        connection.execute("SET max_temp_directory_size = '0B'")
        reader = connection.execute(sql, params).to_arrow_reader(allocation.batch_rows)
        return ColumnarStream(reader, connection, max_rows=plan.query.budget.max_rows,
                              max_bytes=plan.query.budget.max_bytes)
    except BaseException:
        connection.close()
        raise
