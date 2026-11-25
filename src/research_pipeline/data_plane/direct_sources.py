"""直接来源描述、只读分批扫描及恢复时来源复验。"""
from __future__ import annotations

import base64
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
import json
import copy
from pathlib import Path
from types import MappingProxyType

from factor_contracts import FactorReadLease
from research_pipeline.platform.canonical import typed_canonical_hash
from .admitted_plan_codec import admitted_plan_from_dict
from .dataset_files import DirectDatasetRef, VerifiedDataset, _filter_expression, _query_dataset_filters
from .errors import QualityGateError, QueryIRInvalidError, SnapshotIntegrityError
from .execution_budget import DataPlaneExecutionBudget
from .execution_estimate import ExecutionEstimate
from .factor_publication import require_factor_publication_unchanged
from .query_ir import DateRangeV1
from .revision import probe_duckdb_source
from .verification_lifecycle import current_artifact_verification


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
    iter_temporal_fact_batches = VerifiedDataset.iter_temporal_fact_batches
    _record_batch = VerifiedDataset._record_batch
    __reduce__ = VerifiedDataset.__reduce__

    def __init__(self, *, descriptor, reference, resolver):
        self.manifest = MappingProxyType(copy.deepcopy(descriptor))
        self._reference = reference
        self._resolver = resolver
        self.plan = admitted_plan_from_dict(descriptor["plan"])
        self.schema = DirectSourceSchema(**descriptor["schema"]).to_arrow_schema()
        self.allowed_columns = tuple(self.schema.names)
        self.max_batch_rows = resolver.max_batch_rows
        self.max_batch_bytes = resolver.max_batch_bytes
        self.budget = DataPlaneExecutionBudget(**descriptor["execution_budget"])
        self.estimate = ExecutionEstimate.from_dict(descriptor["execution_estimate"])
        self._verified_archive = None
        self._verified_plan_payload = None
        self._verified_descriptor_obj = None

    def _verify(self, connection=None):
        from .stream import time_source_verification

        time_source_verification(self._verify_binding, connection)

    def _verify_binding(self, connection):
        payload = _load_descriptor(self._resolver, self._reference, borrow=True)
        if payload is not self._verified_descriptor_obj:
            if payload != self.manifest:
                raise SnapshotIntegrityError("直接来源绑定发生变化")
            self._verified_descriptor_obj = payload
        if payload["source_kind"] == "archived_parquet":
            from .archive_read import _resolve_entry, require_archived_plan_equivalent
            current = self.plan.identity_payload()
            if self._verified_archive is None:
                if payload["admitted_plan_hash"] != self.plan.plan_hash:
                    raise SnapshotIntegrityError("直接来源与准入计划身份不一致")
                archive = _resolve_entry(payload["archive_entry"])
                require_archived_plan_equivalent(archive[0], self.plan)
                if archive[3] != self.schema:
                    raise SnapshotIntegrityError("归档直接来源 schema 变化")
                self._verified_archive = archive
                self._verified_plan_payload = copy.deepcopy(current)
            elif current != self._verified_plan_payload:
                raise SnapshotIntegrityError("直接来源准入计划发生变化")
            archive = self._verified_archive
            archive[4]._require_current_binding()
        else:
            verify_direct_source(payload, self.plan, connection=connection)

    @contextmanager
    def _source(self, *, columns=None, filters=(), consumer_time=None):
        database = self.manifest.get("database")
        lease = FactorReadLease(database) if self.plan.factor_publication else nullcontext()
        context = open_direct_connection(database) if database is not None else nullcontext()
        with lease, context as connection:
            self._verify(connection)
            try:
                if self.manifest["source_kind"] == "archived_parquet":
                    yield self._verified_archive
                else:
                    with open_projected_stream(
                        database, self.plan, self.budget, self.estimate,
                        columns=columns or self.allowed_columns, filters=filters,
                        connection=connection, consumer_time=consumer_time,
                    ) as stream:
                        import pyarrow as pa
                        expected = pa.schema([self.schema.field(name) for name in (columns or self.allowed_columns)], metadata=self.schema.metadata)
                        if stream.schema != expected:
                            raise SnapshotIntegrityError("直接来源输出 schema 发生变化")
                        yield stream
            finally:
                self._verify(connection)

    def iter_batches(self, *, columns, filters=(), batch_size=65_536):
        if self.manifest["temporal_source"]:
            raise SnapshotIntegrityError("逐决策时态来源必须通过 iter_batches_at 读取")
        yield from self._scan(columns=columns, filters=filters,
                              allowed_columns=self.allowed_columns, batch_size=batch_size)

    def iter_batches_at(self, *, plan, consumer_time, columns, filters=(), batch_size=65_536):
        """DuckDB 在同一列式查询内按消费时点选择版本。"""
        VerifiedDataset._validate_temporal_request(self, plan, columns, batch_size)
        temporal = plan.temporal_selection
        if (self.manifest["source_kind"] != "duckdb"
                or self.estimate.intermediate_bytes_upper > plan.query.budget.max_bytes
                or 4 * self.estimate.intermediate_bytes_upper > self.estimate.provider_duckdb_memory_bytes):
            yield from VerifiedDataset.iter_batches_at(
                self, plan=plan, consumer_time=consumer_time, columns=columns,
                filters=filters, batch_size=batch_size,
            )
            return
        if tuple((item.field_id, item.descending) for item in plan.query.sort[:len(plan.primary_key)]) != tuple(
                (field, False) for field in plan.primary_key):
            raise QueryIRInvalidError("逐决策查询排序必须以完整主键升序开头")
        for selector in (temporal.revision_selector, temporal.effective_interval_selector):
            if selector is not None and plan.primary_key[:len(selector.entity_fields)] != selector.entity_fields:
                raise QueryIRInvalidError("逐决策时态实体字段必须是已验证主键排序的前缀")
        _filter_expression((*_query_dataset_filters(plan), *filters),
                           allowed_columns=temporal.required_scan_fields, schema=self.schema)
        import duckdb

        try:
            with self._source(columns=columns, filters=filters, consumer_time=consumer_time) as batches:
                for batch in batches:
                    if batch.nbytes > self.max_batch_bytes:
                        raise SnapshotIntegrityError("直接读取 RecordBatch 超过批准内存上限")
                    for offset in range(0, batch.num_rows, batch_size):
                        yield batch.slice(offset, batch_size)
        except duckdb.InvalidInputException as exc:
            if any(message in str(exc) for message in (
                "时态最新修订存在冲突", "时态有效区间重叠", "时态生效日期缺失", "日历或合约映射覆盖",
            )):
                raise QueryIRInvalidError(str(exc)) from exc
            if any(message in str(exc) for message in ("非空约束失败", "行数预算", "primary_key 重复")):
                raise QualityGateError(str(exc)) from exc
            raise

    def _iter_ordered_file_batches(self, *, columns, filters, allowed_columns, batch_size):
        yield from self._scan(columns=columns, filters=filters,
                              allowed_columns=allowed_columns, batch_size=batch_size)

    def _scan(self, *, columns, filters, allowed_columns, batch_size):
        if not columns or len(columns) != len(set(columns)) or not set(columns) <= set(allowed_columns):
            raise SnapshotIntegrityError("扫描列必须非空、唯一且已经批准")
        if type(batch_size) is not int or not 0 < batch_size <= self.max_batch_rows:
            raise SnapshotIntegrityError("扫描 batch_size 超过批准上限")
        _filter_expression(filters, allowed_columns=allowed_columns, schema=self.schema)
        # 质量检查所需的键、证券和时点随投影读取，不扩大为全部业务列。
        instrument_columns = (() if self.plan.instrument_field is None
                              else (self.plan.instrument_field,))
        scan_columns = (self.plan.temporal_selection.required_scan_fields
                        if self.manifest["temporal_source"] else tuple(dict.fromkeys(
                            (*columns, *self.plan.primary_key, self.plan.event_time_field, *instrument_columns))))
        from .gates import QualityGateSession
        from .stream import time_source_verification
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
                if batch.num_rows:
                    time_source_verification(gate.validate_batch, batch)
                selected = batch.select(columns)
                for offset in range(0, batch.num_rows, batch_size):
                    yield selected.slice(offset, batch_size)

    def parquet_uncompressed_bytes(self, *, columns):
        raise SnapshotIntegrityError("直接来源没有 Parquet footer；请使用执行估算做内存准入")


def _load_descriptor(resolver, reference, *, borrow=False):
    path = resolver.path_policy.resolve_contained_path(
        allowed_root=resolver.allowed_root, candidate=reference.relative_path,
        root_role="dataset_artifact_allowed_root", path_role="direct_source_descriptor",
        expected_kind="file")
    stat = path.stat()
    stamp = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    session = current_artifact_verification()
    identity = str(path) + "|" + reference.descriptor_hash

    def load():
        return _read_verified_descriptor(path, reference), stamp

    payload, expected = load() if session is None else session.resolve(
        "direct_descriptor", identity, load,
    )
    if stamp != expected:
        raise SnapshotIntegrityError("直接来源描述发生变化")
    for name in ("source_identity", "schema_hash", "source_revision_hash"):
        if payload.get(name) != getattr(reference, name):
            raise SnapshotIntegrityError(f"直接来源 {name} 发生变化")
    return payload if borrow else copy.deepcopy(payload)


def _read_verified_descriptor(path, reference):
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


def verify_direct_source(payload, plan, *, connection=None):
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
                                  object_name=plan.object_name, source_id=plan.binding_id, connection=connection)
    if revision.to_dict() != payload["source_revision"]:
        raise SnapshotIntegrityError("直接来源修订发生变化，不能恢复旧输入")


def resolve_direct_dataset(resolver, reference):
    descriptor = _load_descriptor(resolver, reference, borrow=True)
    def build():
        return DirectVerifiedDataset(descriptor=descriptor, reference=reference, resolver=resolver)

    session = current_artifact_verification()
    identity = "|".join((str(resolver.allowed_root), reference.relative_path,
                         reference.descriptor_hash, str(resolver.max_batch_rows),
                         str(resolver.max_batch_bytes)))
    dataset = build() if session is None else session.resolve("direct_dataset", identity, build)
    database = descriptor.get("database")
    lease = FactorReadLease(database) if dataset.plan.factor_publication else nullcontext()
    with lease:
        dataset._verify()
    return dataset


def _dataset_filter_sql(item, quote, *, logical_type=None):
    field = quote(item.field)
    if item.operator in {"is_null", "is_valid"}:
        return field + (" IS NULL" if item.operator == "is_null" else " IS NOT NULL"), ()

    def parameter(value):
        # 金额按 Arrow 的十进制比较口径传入 DuckDB，避免浮点参数提升整列精度。
        if logical_type == "decimal128":
            return Decimal(str(value))
        return value

    if item.operator == "in":
        return field + " IN (" + ",".join("?" for _ in item.value) + ")", tuple(map(parameter, item.value))
    operator = {"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[item.operator]
    clause = f"{field} {operator} ?"
    if logical_type in {"float32", "float64"} and item.operator in {"lt", "le", "gt", "ge"}:
        clause = f"(NOT isnan({field}) AND {clause})"
    return clause, (parameter(item.value),)


def projected_query(plan, estimate, *, columns, filters, epoch_date_source=None, consumer_time=None):
    """将投影、业务过滤和单时点选择交给只读 SQL。"""
    from .providers.sql import compile_duckdb_query, compile_json_array_projection_source, quote_identifier
    source, source_parameters = None, ()
    if estimate.json_array_projection is not None:
        source = compile_json_array_projection_source(estimate.json_array_projection)
    elif epoch_date_source is not None:
        scope = _physical_date_scope(plan, filters)
        source, source_parameters = epoch_date_source.expression(plan, scope)
    if consumer_time is not None:
        stable = plan.temporal_selection.stable_filter_fields
        types = dict(plan.field_types)
        predicates = tuple(
            (item.field, _dataset_filter_sql(item, quote_identifier, logical_type=types[item.field]))
            for item in (*_query_dataset_filters(plan), *filters)
        )
        return compile_duckdb_query(
            plan.bind_consumer_time(consumer_time), source_expression=source, source_parameters=source_parameters,
            apply_limit=plan.query.limit is not None, direct_consumer_fields=columns,
            direct_scan_filters=tuple(predicate for field, predicate in predicates if field in stable),
            direct_output_filters=tuple(predicate for _, predicate in predicates),
        )
    preserve_facts = plan.temporal_selection.requires_consumer_binding
    inner_limit = plan.query.limit is not None and not preserve_facts
    sql, params = compile_duckdb_query(
        plan, source_expression=source, source_parameters=source_parameters,
        preserve_temporal_facts=preserve_facts, order_output=inner_limit, apply_limit=inner_limit,
    )
    types = dict(plan.field_types)
    predicates = []
    for item in filters:
        clause, values = _dataset_filter_sql(item, quote_identifier, logical_type=types.get(item.field))
        predicates.append(clause)
        params.extend(values)
    projection = ",".join(quote_identifier(name) for name in columns)
    wrapped = f"SELECT {projection} FROM ({sql}) AS direct_input"
    if predicates:
        wrapped += " WHERE " + " AND ".join(predicates)
    if preserve_facts:
        order = [(name, False) for name in dict.fromkeys((*plan.primary_key, *plan.temporal_selection.source_order_fields))]
    else:
        order = [(item.field_id, item.descending) for item in plan.query.sort]
    if order:
        wrapped += " ORDER BY " + ",".join(quote_identifier(name) + (" DESC" if descending else " ASC")
                                          for name, descending in order)
    return wrapped, params


def _physical_date_scope(plan, filters):
    scope = plan.query.time_range
    temporal = plan.temporal_selection
    if (
        not isinstance(scope, DateRangeV1)
        or plan.query.limit is not None
        or temporal.revision_selector is not None
        or temporal.effective_interval_selector is not None
    ):
        return scope
    start, end = scope.start, scope.end
    for item in filters:
        if item.field != plan.event_time_field or type(item.value) is not date:
            continue
        # 严格边界仍由外层条件执行。物理范围包含边界日即可。
        if item.operator in ("eq", "ge", "gt"):
            start = max(start, item.value)
        if item.operator in ("eq", "le", "lt"):
            end = min(end, item.value)
    return DateRangeV1(start, end) if start <= end else scope


def open_direct_connection(database):
    import duckdb

    return duckdb.connect(str(database), read_only=True)


def open_projected_stream(database, plan, budget, estimate, *, columns, filters, schema_only=False,
                          connection=None, consumer_time=None):
    """沿用 provider 分配与列式流合同，使用只读连接执行包装 SQL，禁用落盘。"""
    from .execution_budget import build_provider_execution_plan
    from .providers.epoch_date import DuckDBEpochDateSource
    from .stream import ColumnarStream, open_duckdb_arrow_reader
    if budget.temp_bytes != 0 or estimate.execution_temp_bytes != 0 or estimate.required_temp_bytes:
        raise SnapshotIntegrityError("直接读取不允许临时落盘")
    if not estimate.accepts_runtime_budget(budget) or estimate.query_scope_hash != typed_canonical_hash(plan.query.to_dict()) or estimate.object_name != plan.object_name:
        raise SnapshotIntegrityError("直接读取执行估算与计划或预算不一致")
    allocation = build_provider_execution_plan(plan, budget, variable_width_upper=dict(estimate.variable_width_upper))
    owns_connection = connection is None
    if owns_connection:
        connection = open_direct_connection(database)
    try:
        connection.execute(f"SET threads = {allocation.cpu_slots:d}")
        connection.execute(f"SET memory_limit = '{allocation.duckdb_memory_bytes:d}B'")
        connection.execute("SET temp_directory = ''")
        connection.execute("SET max_temp_directory_size = '0B'")
        sql, params = projected_query(
            plan, estimate, columns=columns, filters=filters,
            epoch_date_source=None if schema_only else DuckDBEpochDateSource(connection),
            consumer_time=consumer_time,
        )
        if schema_only:
            sql = "SELECT * FROM (" + sql + ") AS direct_schema WHERE FALSE"
        reader = open_duckdb_arrow_reader(connection, sql, params, allocation.batch_rows)
        return ColumnarStream(reader, connection if owns_connection else None,
                              max_rows=plan.query.budget.max_rows, max_bytes=plan.query.budget.max_bytes,
                              measure_arrow_reads=True)
    except BaseException:
        if owns_connection:
            connection.close()
        raise
