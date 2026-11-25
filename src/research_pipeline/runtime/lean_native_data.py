"""原生 LEAN 直接读取时使用的行投影和消费时间边界。"""
from __future__ import annotations

from collections import OrderedDict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa

from research_pipeline.data_plane import DatasetArtifactRef
from .project_causal import daily_source_times
from functools import lru_cache
from threading import RLock
from time import perf_counter_ns
from typing import Callable, Iterable, Mapping

from research_pipeline.data_plane.stream import current_read_timing


@lru_cache(maxsize=128)
def _parse_at(text):
    return datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)


def _at(value):
    result = value if isinstance(value, datetime) else _parse_at(value)
    if result.tzinfo is None:
        raise ValueError("原生数据时点必须包含时区")
    return result


class NativeDataBinding:
    """把已准入来源的投影器绑定到冻结证券和时间范围。"""

    def __init__(self, declaration: Mapping, reader: Callable[[Mapping], Iterable[Mapping]], *,
                 independent_rows=False):
        self.declaration = dict(declaration)
        self.reader = reader
        self.independent_rows = independent_rows
        self.page_size = declaration.get("page_size", 2048)
        self.history_page_size = declaration.get("history_page_size", self.page_size)
        if not 0 < self.page_size <= 65536:
            raise ValueError("原生绑定分页行数无效")
        self.symbols = frozenset(declaration["symbols"])
        self.start = _at(declaration["history_start_at"])
        self.end = _at(declaration["end_at"])
        if self.start >= self.end:
            raise ValueError("原生数据绑定范围无效")

    def rows(self, request):
        symbol = request["symbol"]
        if symbol not in self.symbols and not (self.declaration["kind"] == "universe" and symbol == "*"):
            raise ValueError("证券不在已准入研究数据域")
        if request["purpose"] not in {"feed", "history", "seed", "universe"}:
            raise ValueError("原生数据请求用途无效")
        start, end, consumer = (_at(request[name]) for name in ("start_at", "end_at", "consumer_time"))
        if start < self.start or end > self.end or end < start:
            raise ValueError("原生数据请求超出已准入历史范围")
        cutoff = min(consumer, end)
        history = request["purpose"] in {"history", "seed"}
        previous = None
        for row in self.reader(request):
            timing = current_read_timing()
            if timing is not None:
                checking = perf_counter_ns()
            if not self.independent_rows:
                row = dict(row)
                if timing is not None:
                    copied = perf_counter_ns()
                    timing.delivery_copy_ns += copied - checking
                    checking = copied
            if history and row.get("execution_open"):
                if timing is not None:
                    timing.visibility_ns += perf_counter_ns() - checking
                continue
            available = _at(row["available_at"])
            if previous is not None and available < previous:
                raise ValueError("原生数据来源必须按可见时点稳定排序")
            previous = available
            if available > cutoff or available <= start:
                if timing is not None:
                    timing.visibility_ns += perf_counter_ns() - checking
                continue
            row_symbol = row.get("symbol", symbol)
            if row_symbol not in self.symbols:
                raise ValueError("原生数据行证券超出已准入研究数据域")
            if symbol != "*" and row_symbol != symbol:
                raise ValueError("原生数据行与请求证券不一致")
            # 原始观测区间独立保存，交付和History始终使用可见时点。
            if _at(row["end_time"]) > available:
                raise ValueError("原生数据不能早于观测完成时点交付")
            if timing is not None:
                timing.visibility_ns += perf_counter_ns() - checking
            yield row


def admitted_dataset_reader(dataset, plan, project_batch):
    """通过既有逐消费时点读取器选版，再投影成原生数据行。"""
    def read(request):
        from research_pipeline.data_plane.dataset_artifacts import DatasetFilter
        filters = () if request["symbol"] == "*" else (
            DatasetFilter(plan.instrument_field, "eq", request["symbol"]),)
        options = dict(columns=plan.query.field_ids, filters=filters, batch_size=2048)
        temporal = getattr(dataset, "manifest", {}).get("temporal_source") is True
        if request["purpose"] in {"feed", "universe"} and temporal:
            # 原始事实接口保留每次发布/修订，不能提前按期末consumer_time选最终版本。
            batches = dataset.iter_temporal_fact_batches(plan=plan, **options)
        else:
            batches = (dataset.iter_batches_at(plan=plan, consumer_time=_at(request["consumer_time"]), **options)
                       if temporal else dataset.iter_batches(**options))
        for batch in batches:
            yield from project_batch(batch, request)
    return read


@lru_cache(maxsize=8)
def _timestamp_converter(kind):
    import pyarrow as pa

    # 不同证券重复同一组行情时刻；按 Arrow 单位、时区和整数刻度复用精确值。
    @lru_cache(maxsize=2048)
    def convert(tick):
        return pa.scalar(tick, type=kind).as_py()

    return convert


def _native_column_values(column):
    import pyarrow as pa

    kind = column.type
    if pa.types.is_timestamp(kind) and not column.null_count:
        ticks = column.cast(pa.int64()).to_numpy(zero_copy_only=False).tolist()
        return list(map(_timestamp_converter(kind), ticks))
    # 十进制列得到 Decimal 对象数组，含空值时仍保留 None 和原始小数位。
    if pa.types.is_string(kind) or pa.types.is_decimal(kind) or (not column.null_count and (
            pa.types.is_integer(kind) or pa.types.is_floating(kind) or pa.types.is_boolean(kind))):
        return column.to_numpy(zero_copy_only=False).tolist()
    if pa.types.is_date32(kind) and isinstance(column, pa.Array) and not column.null_count:
        # 同一截面的日期重复数千次，只需为不同日期创建 Python 的 date 对象。
        encoded = column.dictionary_encode()
        dates = encoded.dictionary.to_pylist()
        return [dates[index] for index in encoded.indices.to_numpy().tolist()]
    return column.to_pylist()


def native_batch_rows(batch):
    """按列转换有界 Arrow 批，保留日期、精确十进制、整数和空值。"""
    timing = current_read_timing()
    started = perf_counter_ns() if timing is not None else 0
    rows = [{} for _ in range(batch.num_rows)]
    if timing is not None:
        timing.row_build_ns += perf_counter_ns() - started
    for name, column in zip(batch.schema.names, batch.columns):
        started = perf_counter_ns() if timing is not None else 0
        values = _native_column_values(column)
        if timing is not None:
            converted = perf_counter_ns()
            timing.column_convert_ns += converted - started
        for row, value in zip(rows, values):
            row[name] = value
        if timing is not None:
            timing.row_build_ns += perf_counter_ns() - converted
    return rows


class NativeDailyBarReader:
    """全证券共享有界日缓存，单证券游标只转换自己的Arrow切片。"""

    def __init__(self, days, load_day, projector, *, symbol_column="instrument", memory_bytes,
                 cache_days=2, day_windows=None):
        self.days = tuple(days)
        self.dates = tuple(day if isinstance(day, date) else date.fromisoformat(day) for day in self.days)
        self.load_day = load_day
        self.projector = projector
        self.symbol_column = symbol_column
        self.memory_bytes = memory_bytes
        self.cache_days = cache_days
        self.day_windows = day_windows
        self.cache = OrderedDict()
        self.lock = RLock()
        self.retained_bytes = 0

    def _day(self, day):
        import numpy as np
        import pyarrow as pa
        import pyarrow.compute as pc
        with self.lock:
            if day in self.cache:
                self.cache.move_to_end(day)
                return self.cache[day]
            table = self.load_day(day)
            if not isinstance(table, pa.Table):
                batches = []
                total = 0
                for batch in table:
                    total += batch.nbytes
                    if total > self.memory_bytes:
                        raise ValueError("原生共享日行情超过冻结缓存内存额度")
                    batches.append(batch)
                if not batches:
                    return None, {}
                table = pa.Table.from_batches(batches)
            if table.nbytes > self.memory_bytes:
                raise ValueError("原生共享日行情超过冻结缓存内存额度")
            encoded = pc.dictionary_encode(table[self.symbol_column].combine_chunks())
            numbers = encoded.indices.to_numpy(zero_copy_only=False)
            boundaries = np.flatnonzero(np.r_[True, numbers[1:] != numbers[:-1], True]) if len(numbers) else []
            ranges = {}
            codes = encoded.dictionary.to_pylist()
            for start, end in zip(boundaries[:-1], boundaries[1:]):
                ranges.setdefault(codes[int(numbers[start])], []).append((int(start), int(end - start)))
            while self.cache and (len(self.cache) >= self.cache_days
                                  or self.retained_bytes + table.nbytes > self.memory_bytes):
                _, (removed, _) = self.cache.popitem(last=False)
                self.retained_bytes -= removed.nbytes
            self.cache[day] = table, ranges
            self.retained_bytes += table.nbytes
            return table, ranges

    def __call__(self, request):
        from bisect import bisect_left
        # 按冻结交易日取前一个实际会话，长假不能用固定自然日回退。
        start, end = _at(request["start_at"]), _at(request["end_at"])
        position = max(0, bisect_left(self.dates, start.date()) - 1)
        last = end.date()
        if self.day_windows is not None:
            position = 0
        for offset in range(position, len(self.days)):
            day, value = self.days[offset], self.dates[offset]
            if self.day_windows is not None:
                begin, finish = self.day_windows[day]
                if finish < start or begin > end:
                    continue
            elif value > last:
                break
            table, ranges = self._day(day)
            if table is None:
                continue
            if request["symbol"] == "*":
                for batch in table.to_batches(max_chunksize=2048):
                    for raw in native_batch_rows(batch):
                        yield self.projector(raw, request)
                continue
            for start, length in ranges.get(request["symbol"], ()):
                for raw in native_batch_rows(table.slice(start, length)):
                    yield self.projector(raw, request)


_ZONE = ZoneInfo("Asia/Shanghai")


def _aware(value, timezone=_ZONE):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone) if value.tzinfo is None else value
    return datetime.combine(value, time.min, timezone)


def _bounds(plan, daily_time=None):
    query = plan.query
    scope = query.time_range
    if hasattr(scope, "start_at"):
        return scope.start_at, query.visible_end_at
    start = datetime.combine(scope.start, time.min, _ZONE)
    end = datetime.combine(scope.end + timedelta(days=1), time.min, _ZONE)
    if daily_time:
        sessions = [date.fromisoformat(value) if isinstance(value, str) else value for value in daily_time["sessions"]]
        following = next((value for value in sessions if value > scope.end), None)
        if following is not None:
            end = datetime.combine(following, time(9, 30), _ZONE)
    as_of = query.as_of
    if as_of is None:
        return start, end
    if isinstance(as_of, str):
        as_of = datetime.fromisoformat(as_of) if "T" in as_of or " " in as_of else date.fromisoformat(as_of)
    cutoff = (_aware(as_of) if isinstance(as_of, datetime) else
              datetime.combine(as_of + timedelta(days=1), time.min, _ZONE))
    return start, min(end, cutoff)


def _projector(plan, binding, *, futures_calendar=None, date_lookup=None):
    """时间来自正式计划；行中的任意同名字段不授予可见资格。"""
    temporal = plan.temporal_selection
    visibility = temporal.visibility_filter
    revision = temporal.revision_selector
    interval = temporal.effective_interval_selector
    clocks = tuple(field for field, _ in visibility.time_fields) if visibility else ()
    if revision is not None:
        clocks = tuple(dict.fromkeys((*clocks, revision.order_fields[0])))
    required = {plan.event_time_field, plan.instrument_field, *clocks} - {None}
    if interval is not None:
        interval_fields = {interval.effective_from_field, interval.effective_to_field, *interval.entity_fields}
        required.update(interval_fields)
        if binding.kind not in {"event", "universe"} or not interval_fields <= set(binding.columns):
            raise ValueError("有效区间事件须交付完整实体键与生效/失效字段，不能作为无区间快照")
    if not required <= set(plan.query.field_ids):
        raise ValueError("原生来源须公开已准入观测、证券及可见/修订时间列")
    timezone = ZoneInfo(temporal.source_timezone or "Asia/Shanghai")
    inclusive_by_field = dict(visibility.time_fields) if visibility else {}
    columns = binding.columns
    # 读取器按正式计划的字段顺序逐行建字典。全列原序交付可直接使用该行。
    full_projection = columns == plan.query.field_ids
    if binding.kind == "bars":
        from research_pipeline.platform.daily_projection import daily_projection
        price_columns = daily_projection(plan)
    else:
        price_columns = {}

    @lru_cache(maxsize=4096)
    def single_date(value):
        observations, available = daily_source_times(pd.Series([value]), binding.daily_time)
        return observations.iloc[0].to_pydatetime(), available.iloc[0].to_pydatetime()

    mapped_date = date_lookup or single_date

    def clock_value(row, field):
        raw = row[field]
        if isinstance(raw, date) and not isinstance(raw, datetime) and binding.daily_time:
            return mapped_date(raw)[1]
        point = _aware(raw, timezone)
        if not inclusive_by_field.get(field, True):
            # Timestamp的排他边界使用核心相同的微秒精度；日期边界进入下个来源日。
            point += timedelta(microseconds=1) if isinstance(raw, datetime) or isinstance(raw, str) and "T" in raw else timedelta(days=1)
        return point

    def project(row, request):
        timing = current_read_timing()
        started = perf_counter_ns() if timing is not None else 0
        value = row[plan.event_time_field]
        if binding.daily_time:
            observed, available = mapped_date(value)
        elif plan.session_close_binding is not None:
            observed = plan.session_close_binding.completion_time(row[plan.instrument_field], value)
            available = (futures_calendar.next_open(row[plan.instrument_field], value)
                         if binding.kind == "bars" and futures_calendar is not None else observed)
        else:
            observed = _aware(value, timezone)
            if clocks:
                available = None
            elif plan.minute_availability_rule:
                available = observed
            else:
                raise ValueError("原生来源缺少可执行的Catalog可见时钟")
        for field in clocks:
            point = clock_value(row, field)
            if available is None or point > available:
                available = point
        if timing is not None:
            projected = perf_counter_ns()
            timing.visibility_ns += projected - started
        result = {"symbol": str(row[plan.instrument_field]), "time": observed,
                  "end_time": observed, "available_at": available}
        if binding.kind == "bars":
            if futures_calendar is not None:
                session = futures_calendar.session(row[plan.instrument_field], value)
                eligible = [item for item in session.segments if item.bar_eligible]
                result["time"] = min(item.starts_at for item in eligible)
                result["end_time"] = max(item.ends_at for item in eligible)
            else:
                result["time"] = datetime.combine(observed.astimezone(_ZONE).date(), time(9, 30), _ZONE)
            result.update({name: row[price_columns[name]] for name in ("open", "high", "low", "close", "volume")})
        else:
            result["values"] = row if full_projection else {name: row[name] for name in columns}
        if timing is not None:
            timing.field_project_ns += perf_counter_ns() - projected
        return result
    return project


def _event_batch_projector(plan, binding):
    if not binding.daily_time:
        project = _projector(plan, binding)
        return lambda batch, request: (project(row, request) for row in native_batch_rows(batch))

    visibility = plan.temporal_selection.visibility_filter
    revision = plan.temporal_selection.revision_selector
    fields = {plan.event_time_field}
    if visibility:
        fields.update(field for field, _ in visibility.time_fields)
    if revision:
        fields.add(revision.order_fields[0])
    dates = {}

    def lookup(value):
        if value not in dates:
            remember([value])
        return dates[value]

    def remember(values):
        observed, available = daily_source_times(pd.Series(values), binding.daily_time)
        if len(dates) + len(values) > 8192:
            dates.clear()
        dates.update(zip(values, zip(observed.array.to_pydatetime(), available.array.to_pydatetime())))

    project = _projector(plan, binding, date_lookup=lookup)

    def project_batch(batch, request):
        # 一批中相同的公告和统计日期只映射一次；每行仍独立合并修订时钟。
        pending = {}
        for field in fields:
            column = batch.column(field)
            if pa.types.is_date(column.type):
                pending.update(dict.fromkeys(day for day in column.unique().to_pylist()
                                             if day is not None and day not in dates))
        if pending:
            remember(list(pending))
        return (project(row, request) for row in native_batch_rows(batch))

    return project_batch


def _universe_scan_days(dataset, plan, memory):
    """在现有读数和内存额度内合并按日期排序的日截面。"""
    estimate = getattr(dataset, "estimate", None)
    if estimate is None:
        return 8
    symbols = len(plan.query.universe.instruments)
    fixed = {item.field_id for item in plan.query.filters if item.operator == "eq"}
    axes = {plan.event_time_field, plan.instrument_field}
    if not symbols or not axes.issubset(plan.primary_key) or set(plan.primary_key) - axes - fixed:
        return 8
    # 主键的其他维度已固定，一天至多每证券一行。Arrow 仍按原批大小流式消费。
    byte_limit = min(memory, estimate.provider_duckdb_memory_bytes, estimate.output_max_bytes)
    rows = min(estimate.output_max_rows, byte_limit // estimate.projected_row_width_upper)
    return max(1, min(128, rows // symbols))


def _daily_reader(dataset, plan, binding, projector, memory):
    from bisect import bisect_left, bisect_right
    from contextlib import closing
    from research_pipeline.data_plane import DatasetFilter

    dates = (binding.daily_time["sessions"] if binding.daily_time
             and binding.daily_time["rule"] != "business_effective_date" else
             (plan.query.time_range.start + timedelta(days=offset)
              for offset in range((plan.query.time_range.end - plan.query.time_range.start).days + 1)))
    days = tuple(day for day in (date.fromisoformat(str(value)) for value in dates)
                 if plan.query.time_range.start <= day <= plan.query.time_range.end)
    manifest = getattr(dataset, "manifest", {})
    temporal = plan.temporal_selection

    def scan(operator, value, batch_size):
        options = dict(columns=plan.query.field_ids,
                       filters=(DatasetFilter(plan.event_time_field, operator, value),),
                       batch_size=batch_size)
        if manifest.get("temporal_source"):
            if temporal.revision_selector is not None or temporal.effective_interval_selector is not None:
                raise ValueError("含修订的日行情须使用逐可见事件来源，不能按期末日快照缓存")
            # 无修订或有效区间的逐日状态保留原始事实，逐行可见时点仍在binding消费边界裁剪。
            return dataset.iter_temporal_fact_batches(plan=plan, **options)
        return dataset.iter_batches(**options)

    daily = NativeDailyBarReader(days, lambda day: scan("eq", day, 65536), projector,
                                symbol_column=plan.instrument_field, memory_bytes=memory)
    if (binding.kind != "universe" or manifest.get("source_kind") != "duckdb"
            or temporal.revision_selector is not None or temporal.effective_interval_selector is not None
            or plan.primary_key[:1] != (plan.event_time_field,)
            or not plan.query.sort or plan.query.sort[0].field_id != plan.event_time_field
            or plan.query.sort[0].descending):
        return daily

    source_timezone = ZoneInfo(binding.daily_time["timezone"])
    scan_days = _universe_scan_days(dataset, plan, memory)

    def reader(request):
        if request["symbol"] != "*" or request["purpose"] not in {"universe", "feed"}:
            yield from daily(request)
            return
        # 保留前一实际交易日用于次日可见数据，且只扫描本次请求覆盖的准入日期。
        start, end, consumer = (_at(request[name]).astimezone(source_timezone).date()
                                for name in ("start_at", "end_at", "consumer_time"))
        first = max(0, bisect_left(days, start) - 1)
        last = bisect_right(days, min(end, consumer))
        for offset in range(first, last, scan_days):
            selected = days[offset:min(offset + scan_days, last)]
            with closing(scan("in", selected, min(2048, dataset.max_batch_rows))) as batches:
                for batch in batches:
                    if batch.nbytes > memory:
                        raise ValueError("原生Universe扫描超过冻结缓存内存额度")
                    for raw in native_batch_rows(batch):
                        yield projector(raw, request)

    return reader


def _with_daily_open(reader, market, binding, memory):
    def project(row, request):
        day = row[market.open_plan.event_time_field]
        if market.futures_calendar is not None:
            session = market.futures_calendar.session(row[market.open_plan.instrument_field], day)
            point = min(item.starts_at for item in session.segments if item.bar_eligible)
        else:
            point = datetime.combine(day, time(9, 30), _ZONE)
        return {"symbol": row[market.open_plan.instrument_field], "time": point, "end_time": point,
                "available_at": point, "execution_open": True, "open": row[market.open_column]}
    def load(day):
        return market._read_batches(market.open_dataset, market.open_plan, date.fromisoformat(str(day)),
            (market.open_plan.instrument_field, market.open_plan.event_time_field, market.open_column))
    opening = NativeDailyBarReader(market.dates, load, project,
        symbol_column=market.open_plan.instrument_field, memory_bytes=memory)
    def combined(request):
        import heapq
        yield from heapq.merge(reader(request), opening(request), key=lambda row: (row["available_at"], row.get("execution_open", False)))
    return combined


def _resolve_dataset(resolver, reference, plan):
    dataset = resolver.resolve(DatasetArtifactRef.from_dict(reference))
    if dataset.manifest.get("admitted_plan_hash") != plan.plan_hash:
        raise ValueError("原生输入工件与冻结准入计划不一致")
    return dataset
