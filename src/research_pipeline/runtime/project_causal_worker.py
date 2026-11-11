"""Worker 核心为单个冻结键批提供输入并记录实际交付。"""

from __future__ import annotations

from dataclasses import replace
from collections import OrderedDict
from contextlib import nullcontext
from types import SimpleNamespace
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from research_pipeline.platform.causal_time import CausalTimeContractError
from research_pipeline.platform.project_causal_contract import parse_causal_plan
from .project_causal import CausalReadTrace
from .project_table_input import ProjectTableInput
from .project_causal_minute import ProjectCausalMinuteInput
from research_pipeline.extensions.project_bundle import _freeze_payload
from research_pipeline.data_plane.verification_lifecycle import (
    RunScopedArtifactVerification,
    activate_artifact_verification,
)


class _CausalSourceVerification(RunScopedArtifactVerification):
    """会话内复用完整验证，并逐次核对实际归档文件是否变化。"""

    def __init__(self):
        super().__init__("causal-worker-session")
        self._file_stats = {}

    def resolve(self, namespace, identity, verifier, *, descriptor=None):
        key = (namespace, identity)
        for path, expected in self._file_stats.get(key, ()):
            if self._stat(path) != expected:
                raise CausalTimeContractError("因果会话归档来源发生变化")
        value = super().resolve(namespace, identity, verifier, descriptor=descriptor)
        if key not in self._file_stats and descriptor is not None:
            paths = ()
            if namespace == "dataset_artifact":
                root = Path(descriptor["root"]) / descriptor["reference"].relative_path
                paths = (*value[1], root / "manifest.json", root / "COMMITTED")
            elif namespace == "parquet_snapshot":
                root = Path(descriptor["path"])
                paths = tuple(root / item["relative_path"] for item in value["files"])
                paths = (*paths, root / "manifest.json", root / "COMMITTED")
            self._file_stats[key] = tuple((path, self._stat(path)) for path in paths)
        return value

    @staticmethod
    def _stat(path):
        try:
            stat = path.stat()
        except OSError as exc:
            raise CausalTimeContractError("因果会话归档来源不可用") from exc
        return stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


class CausalArchiveReaders:
    """仅当前显式会话保留有界只读句柄，不缓存行情行或因果轨迹。"""

    def __init__(self, max_open_files=8):
        self._files = OrderedDict()
        self._max_open_files = max_open_files
        self.verification = _CausalSourceVerification()
        self._candidate_indexes = {}

    def open(self, path):
        path = str(path)
        value = self._files.pop(path, None)
        if value is None:
            value = (pq.ParquetFile(path), {})
        self._files[path] = value
        while len(self._files) > self._max_open_files:
            reader, _ = self._files.popitem(last=False)[1]
            reader.close()
        return value

    def candidates(self, paths, source, partition_ids, source_timezone, item):
        """冻结文件清单只读一次footer，逐批按时间范围选择实际文件。"""
        paths = tuple(paths)
        key = (paths, source.observation_column, source_timezone,
               None if source.daily_time is None else source.daily_time["timezone"])
        index = self._candidate_indexes.get(key)
        if index is None:
            entries = []
            for path in paths:
                signature = _CausalSourceVerification._stat(path)
                parquet, cache = self.open(path)
                field = parquet.schema_arrow.field(source.observation_column)
                spans = _source_row_group_spans(parquet, source, source_timezone, cache)
                if _CausalSourceVerification._stat(path) != signature:
                    raise CausalTimeContractError("因果会话归档来源发生变化")
                entries.append((path, signature, field, spans))
            index = tuple(entries)
            self._candidate_indexes[key] = index
        # 被排除文件也核对元数据，防止变化后的时间范围绕过本批筛选。
        for path, signature, field, spans in index:
            if _CausalSourceVerification._stat(path) != signature:
                raise CausalTimeContractError("因果会话归档来源发生变化")
            groups = _window_row_groups(field, spans, source, partition_ids, source_timezone, item)
            if groups:
                yield path, groups

    def close(self):
        for reader, _ in self._files.values():
            reader.close()
        self._files.clear()
        self._candidate_indexes.clear()


def run_causal_entry(entry, context, inputs, output_root, causal_context, *, archive_readers=None):
    scope = (activate_artifact_verification(archive_readers.verification)
             if archive_readers is not None else nullcontext())
    with scope:
        return _run_causal_entry(entry, context, inputs, output_root, causal_context,
                                 archive_readers=archive_readers)


def _run_causal_entry(entry, context, inputs, output_root, causal_context, *, archive_readers=None):
    plan = parse_causal_plan(causal_context["plan"])
    if len(plan.work_items) != 1:
        raise CausalTimeContractError("Worker 每次必须只执行一个核心键批")
    item = plan.work_items[0]
    trace = CausalReadTrace(
        plan, item,
        inherited_lineage=causal_context["inherited_lineage"],
        source_partition_identities=causal_context["partition_identities"],
    )
    by_port = {value.port: value for value in inputs}
    restricted = []
    for source in plan.sources:
        original = by_port[source.port]
        if not isinstance(original, (ProjectTableInput, ProjectCausalMinuteInput)):
            raise CausalTimeContractError("正式因果输入必须是核心解析的已验证列式工件")
        source_timezone = causal_context["source_timezones"][source.port]
        direct = None
        raw_reference = causal_context.get("direct_sources", {}).get(source.port)
        if raw_reference is not None:
            from research_pipeline.data_plane.direct_sources import DirectDatasetRef

            if not isinstance(original, ProjectTableInput):
                raise CausalTimeContractError("直接因果输入必须交接来源描述")
            direct = original.resolve_causal_direct_source(DirectDatasetRef.from_dict(raw_reference))

        def factory(*, columns, partition_ids, batch_size, original=original,
                    source=source, source_timezone=source_timezone, direct=direct):
            if isinstance(original, ProjectCausalMinuteInput):
                for partition, batch in original.iter_partition_batches(
                    columns=columns, partition_ids=partition_ids, batch_size=batch_size
                ):
                    for name in {source.observation_column, source.available_column}:
                        index = batch.schema.get_field_index(name)
                        array = batch.column(index)
                        if pa.types.is_timestamp(array.type) and array.type.tz is None:
                            array = pa.Array.from_pandas(pd.Series(array.to_pandas()).dt.tz_localize(source_timezone))
                            batch = batch.set_column(index, name, array)
                    yield partition, batch
                return
            if direct is not None:
                filters = _direct_window_filters(direct, source, item, source_timezone)
                if direct.manifest.get("temporal_source"):
                    batches = direct.iter_batches_at(
                        plan=direct.plan, consumer_time=item.decision_time,
                        columns=columns, filters=filters, batch_size=batch_size,
                    )
                else:
                    batches = direct.iter_batches(columns=columns, filters=filters, batch_size=batch_size)
                for batch in batches:
                    yield from _partition_source_batch(batch, source, partition_ids, source_timezone)
                return
            # 原始路径只留在核心闭包，项目只接收 restricted wrapper。
            candidates = (archive_readers.candidates(original._paths.values(), source, partition_ids,
                                                     source_timezone, item)
                          if archive_readers is not None else ((path, None) for path in original._paths.values()))
            for path, selected_groups in candidates:
                cached = archive_readers.open(path) if archive_readers is not None else None
                with nullcontext(cached[0]) if cached is not None else pq.ParquetFile(path) as parquet:
                    row_groups = (selected_groups if selected_groups is not None else
                                  _source_window_row_groups(parquet, source, partition_ids, source_timezone, item))
                    for batch in parquet.iter_batches(columns=list(columns), row_groups=row_groups,
                                                      batch_size=batch_size, use_threads=False):
                        if batch.nbytes > original._max_batch_bytes:
                            raise CausalTimeContractError("因果输入批次超过内存预算")
                        yield from _partition_source_batch(batch, source, partition_ids, source_timezone)

        restricted.append(trace.wrap_input(source.port, factory))
    state = by_port.get("runtime_state")
    if state is not None:
        restricted.append(state)
    worker_parameters = dict(context.parameters)
    worker_parameters["causal_plan"] = _freeze_payload(plan.to_dict())
    result = entry(replace(context, parameters=worker_parameters), tuple(restricted), output_root)
    for value in inputs:
        if isinstance(value, ProjectCausalMinuteInput):
            value.assert_complete()
    facts = trace.core_time_facts()
    # 时间字符串沿用原精度；来源列表保留对象引用，不经逐行JSON展开。
    record_fields = facts.drop(columns=["source_partition_ids"], errors="ignore")
    records = json.loads(record_fields.to_json(orient="records", date_format="iso", date_unit="ns"))
    if "source_partition_ids" in facts:
        for record, source_ids in zip(records, facts["source_partition_ids"]):
            record["source_partition_ids"] = source_ids
    return result, {"key_columns": list(plan.key_columns), "records": records,
                    "lineage": list(trace.lineage())}


def _direct_window_filters(dataset, source, item, source_timezone):
    """把冻结观测窗口下推给只读 provider，交付仍由原时间规则约束。"""
    from research_pipeline.data_plane import DatasetFilter

    field = dataset.schema.field(source.observation_column)
    bounds = [item.window_start, item.window_end]
    upper_operator = "le"
    if pa.types.is_date(field.type):
        timezone = source.daily_time["timezone"] if source.daily_time is not None else source_timezone
        bounds = [value.tz_convert(timezone).date() for value in bounds]
    elif pa.types.is_timestamp(field.type):
        if source.daily_time is not None:
            bounds = [value.tz_convert(source.daily_time["timezone"]).normalize() for value in bounds]
            bounds[1] += pd.Timedelta(days=1)
            upper_operator = "lt"
        if field.type.tz is None:
            timezone = source.daily_time["timezone"] if source.daily_time is not None else source_timezone
            bounds = [value.tz_convert(timezone).tz_localize(None).to_pydatetime() for value in bounds]
        else:
            bounds = [value.to_pydatetime() for value in bounds]
    else:
        return ()
    return (DatasetFilter(source.observation_column, "ge", bounds[0]),
            DatasetFilter(source.observation_column, upper_operator, bounds[1]))


def _partition_source_batch(batch, source, partition_ids, source_timezone):
    """文件与直接来源共用时区和逻辑月份划分，窗口由因果包装器裁剪。"""
    for name in (() if source.daily_time is not None else {source.observation_column, source.available_column}):
        index = batch.schema.get_field_index(name)
        array = batch.column(index)
        if pa.types.is_timestamp(array.type) and array.type.tz is None:
            if source_timezone is None:
                raise CausalTimeContractError("因果输入缺少核心时区")
            array = pa.Array.from_pandas(pd.Series(array.to_pandas()).dt.tz_localize(source_timezone))
            batch = batch.set_column(index, name, array)
    observations = batch.column(batch.schema.get_field_index(source.observation_column))
    # 逻辑月份使用源的时区；UTC 归一只用于窗口不等式。
    if source.daily_time is not None and pa.types.is_date(observations.type):
        years, months = pc.year(observations), pc.month(observations)
    else:
        local_observations = pd.to_datetime(pd.Series(observations.to_pandas()))
        if source.daily_time is None and source_timezone is not None:
            local_observations = local_observations.dt.tz_convert(source_timezone)
        years = pa.Array.from_pandas(local_observations.dt.year)
        months = pa.Array.from_pandas(local_observations.dt.month)
    for partition in partition_ids:
        year, month = map(int, partition.split("-"))
        selected = batch.filter(pc.and_(pc.equal(years, year), pc.equal(months, month)))
        if selected.num_rows:
            yield partition, selected


def _source_window_row_groups(parquet, source, partition_ids, source_timezone, item, *, footer_cache=None):
    """按原始日期/时点裁剪完整行组；最终可见窗口仍由因果读取器限制。"""
    field = parquet.schema_arrow.field(source.observation_column)
    spans = _source_row_group_spans(parquet, source, source_timezone, footer_cache)
    return _window_row_groups(field, spans, source, partition_ids, source_timezone, item)


def _source_row_group_spans(parquet, source, source_timezone, footer_cache):
    """只保留观测列的行组边界，不保留宽表schema或行情载荷。"""
    field = parquet.schema_arrow.field(source.observation_column)
    if not (pa.types.is_date(field.type) or pa.types.is_timestamp(field.type)):
        return (None,) * parquet.num_row_groups
    key = (source.observation_column, source_timezone,
           None if source.daily_time is None else source.daily_time["timezone"])
    spans = None if footer_cache is None else footer_cache.get(key)
    if spans is None:
        column = parquet.schema.names.index(source.observation_column)
        spans = []
        for index in range(parquet.num_row_groups):
            stats = parquet.metadata.row_group(index).column(column).statistics
            if stats is None or not stats.has_min_max or stats.null_count != 0:
                spans.append(None)
                continue
            lower, upper = pd.Timestamp(stats.min), pd.Timestamp(stats.max)
            month_bounds = (lower, upper)
            if source.daily_time is None and source_timezone is not None:
                month_bounds = tuple(value.tz_localize(source_timezone) if value.tzinfo is None
                    else value.tz_convert(source_timezone) for value in month_bounds)
            spans.append((lower, upper, *(value.strftime("%Y-%m") for value in month_bounds)))
        spans = tuple(spans)
        if footer_cache is not None:
            footer_cache[key] = spans
    return spans


def _window_row_groups(field, spans, source, partition_ids, source_timezone, item):
    filters = _direct_window_filters(SimpleNamespace(schema=pa.schema([field])), source, item, source_timezone)
    if not filters:
        return list(range(len(spans)))
    lower, upper = (pd.Timestamp(value.value) for value in filters)
    upper_exclusive = filters[1].operator == "lt"
    selected = []
    for index, span in enumerate(spans):
        if span is None:
            selected.append(index)
            continue
        first, last, first_month, last_month = span
        if (any(first_month <= partition <= last_month for partition in partition_ids)
                and last >= lower and (first < upper if upper_exclusive else first <= upper)):
            selected.append(index)
    return selected
