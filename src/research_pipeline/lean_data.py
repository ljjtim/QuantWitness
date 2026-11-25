"""原生 LEAN Python 策略的研究数据读取接口。"""

from bisect import bisect_right
from collections import OrderedDict
from datetime import date, datetime, time, timedelta, timezone
from dataclasses import replace
from time import perf_counter
from zoneinfo import ZoneInfo

from research_pipeline.platform.lean_python_lifetime import _open_readers, register_pit_shutdown


def _field_value(value):
    # C# 单行 Fields 允许策略修改。只在访问了此接口时才解包 JsonElement。
    kind = getattr(value, "ValueKind", None)
    if kind is None:
        if hasattr(value, "Keys"):
            return {key: _field_value(value[key]) for key in value.Keys}
        if hasattr(value, "GetType") and value.GetType().IsArray:
            return [_field_value(item) for item in value]
        return value
    kind = str(kind)
    if kind == "String":
        return value.GetString()
    if kind == "Null":
        return None
    if kind in {"True", "False"}:
        return value.GetBoolean()
    if kind == "Array":
        return [_field_value(item) for item in value.EnumerateArray()]
    if kind == "Object":
        return {item.Name: _field_value(item.Value) for item in value.EnumerateObject()}
    raw = value.GetRawText()
    return float(raw) if any(ch in raw for ch in ".eE") else int(raw)


def read_field_groups(rows, binding_id):
    """按原顺序交付声明字段和对应值，相同布局共用字段名。"""
    groups, available_at = [], None
    for row in rows:
        if row.BindingId != binding_id:
            raise ValueError("研究数据绑定与请求不一致")
        stamp = row.AvailableAt
        if not isinstance(stamp, datetime):
            stamp = datetime.fromisoformat(stamp.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff"))
        stamp = stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        if available_at is not None and stamp != available_at:
            raise ValueError("同批研究数据的可见时点必须一致")
        available_at = stamp
        names = tuple(row.FieldNames)
        if not groups or groups[-1][0] != names:
            groups.append((names, []))
        groups[-1][1].append([_field_value(value) for value in row.FieldValues])
    return groups, available_at


def read_fields(rows, binding_id):
    """读取同一可见时点的业务字段，返回字典列表和可见时间。"""
    groups, stamp = read_field_groups(rows, binding_id)
    return [dict(zip(names, row, strict=True)) for names, values in groups for row in values], stamp


def _source_cutoff(now, rule, sessions):
    if rule == "business_effective_date":
        return now.date()
    last = now.date() if now.time() >= time(9, 30) else now.date() - timedelta(days=1)
    position = bisect_right(sessions, last) - 1
    if position < 0:
        return date.min
    if rule == "session_open":
        return sessions[position]
    if rule == "next_session_open":
        return sessions[position - 1] if position else date.min
    if rule == "next_session_open_following_publication_date":
        return sessions[position] - timedelta(days=1)
    raise ValueError("未支持的可见日期规则：" + rule)


_PIT_BLOCK_DAYS = 28


class ReadOnlyPIT:
    """复用一个只读连接，用算法当前时钟裁剪已准入查询，返回 Arrow 表。"""

    def __init__(self, algorithm, sources):
        import duckdb
        import pyarrow as pa
        from research_pipeline.catalog.discovery import JsonArrayProjectionEvidence
        from research_pipeline.data_plane.providers.epoch_date import DuckDBEpochDateSource
        from research_pipeline.data_plane.providers.sql import compile_json_array_projection_source
        from research_pipeline.data_plane.admitted_plan_codec import admitted_plan_from_dict

        self._connection = None
        register_pit_shutdown()
        databases = {item["database"] for item in sources.values()}
        if len(databases) != 1:
            raise ValueError("同一个 PIT 查询器须绑定一个本地数据库")
        self._algorithm = algorithm
        self._sources = {}
        self._blocks = OrderedDict()
        self._block_bytes = 0
        self._statistics = {
            name: dict(queries=0, block_queries=0, block_hits=0, read_seconds=0.0, slice_seconds=0.0)
            for name in sources
        }
        for name, source in sources.items():
            daily = source["daily_time"]
            plan = admitted_plan_from_dict(source["plan"])
            sessions = tuple(date.fromisoformat(day) for day in daily["sessions"])
            projection = source.get("source_projection")
            expression = (compile_json_array_projection_source(JsonArrayProjectionEvidence.from_dict(projection))
                          if projection else None)
            self._sources[name] = plan, daily["rule"], sessions, tuple(source["columns"]), expression
        self._connection = duckdb.connect(databases.pop(), read_only=True)
        try:
            self._connection.execute("SET threads = ?", [min(item["threads"] for item in sources.values())])
            memory = min(item["memory_bytes"] for item in sources.values())
            self._block_limit = memory // 2
            self._connection.execute("SET memory_limit = ?", [f"{memory}B"])
            self._connection.execute("SET temp_directory = ''")
            self._epoch_date_source = DuckDBEpochDateSource(self._connection)
            relations = {}
            self._universe_relations = {}
            for name, (plan, *_rest) in self._sources.items():
                instruments = plan.query.universe.instruments
                if not instruments:
                    continue
                if instruments not in relations:
                    relation = f"__pit_instruments_{len(relations)}"
                    self._connection.register(relation, pa.table({"instrument_id": pa.array(instruments, type=pa.string())}))
                    relations[instruments] = relation
                self._universe_relations[name] = relations[instruments]
            _open_readers.add(self)
        except BaseException:
            self.close()
            raise

    def close(self):
        _open_readers.discard(self)
        algorithm = getattr(self, "_algorithm", None)
        self._algorithm = None
        connection, self._connection = self._connection, None
        if connection is not None:
            connection.close()
        if hasattr(self, "_blocks"):
            self._blocks.clear()
            self._block_bytes = 0
        if algorithm is not None and hasattr(algorithm, "log"):
            import json
            algorithm.log("PIT read statistics " + json.dumps(self.statistics, sort_keys=True))

    @property
    def statistics(self):
        """按绑定返回实际查询次数以及数据库读取、可见切片用时。"""
        return {name: counters.copy() for name, counters in self._statistics.items()}

    def _query(self, binding_id, plan, projected, first, last, *, block_limit=None):
        import pyarrow as pa
        from research_pipeline.data_plane.providers.sql import compile_duckdb_query

        expression = self._sources[binding_id][-1]
        scope = plan.query.time_range
        empty = first > last
        query = replace(plan.query, field_ids=projected,
                        time_range=replace(scope, start=scope.start if empty else first,
                                           end=scope.start if empty else last))
        temporal = replace(plan.temporal_selection, public_projection=projected)
        source_parameters = ()
        if expression is None and not empty:
            expression, source_parameters = self._epoch_date_source.expression(plan, query.time_range)
        sql, params = compile_duckdb_query(
            replace(plan, query=query, temporal_selection=temporal), source_expression=expression,
            source_parameters=source_parameters,
            universe_relation=self._universe_relations.get(binding_id),
        )
        if empty:
            sql = f"SELECT * FROM ({sql}) AS frozen_query LIMIT 0"
        counters = self._statistics[binding_id]
        counters["queries"] += 1
        if block_limit is not None:
            counters["block_queries"] += 1
        started = perf_counter()
        try:
            with self._connection.execute(sql, params).to_arrow_reader(min(query.budget.batch_size, 65536)) as reader:
                batches, rows, size = [], 0, 0
                for batch in reader:
                    rows += batch.num_rows
                    size += batch.nbytes
                    if rows > query.budget.max_rows or size > query.budget.max_bytes:
                        if block_limit is None:
                            raise ValueError("PIT 查询超出已准入的行数或字节预算")
                        return None
                    if block_limit is not None and size > block_limit:
                        return None
                    batches.append(batch)
                return pa.Table.from_batches(batches, schema=reader.schema)
        finally:
            counters["read_seconds"] += perf_counter() - started

    def read(self, binding_id, *, start=None, end=None):
        import duckdb
        import pyarrow.compute as pc

        plan, rule, sessions, columns, _expression = self._sources[binding_id]
        zone = ZoneInfo("Asia/Shanghai")
        utc = self._algorithm.utc_time
        now = (utc.replace(tzinfo=timezone.utc) if utc.tzinfo is None else utc).astimezone(zone)
        if plan.query.as_of:
            frozen = plan.query.as_of
            at = datetime.fromisoformat(frozen) if "T" in frozen else datetime.combine(date.fromisoformat(frozen), time.max, zone)
            now = min(now, at.astimezone(zone))
        cutoff = _source_cutoff(now, rule, sessions)
        scope = plan.query.time_range
        first = max(scope.start, start or scope.start)
        last = min(scope.end, cutoff, end or scope.end)
        temporal = plan.temporal_selection
        event = plan.event_time_field
        clocks = temporal.visibility_filter.time_fields if temporal.visibility_filter else ()
        date_block = (
            dict(plan.field_types).get(event) == "date32"
            and all(field == event for field, _inclusive in clocks)
            and not (temporal.revision_selector or temporal.effective_interval_selector
                     or temporal.session_close_selection or plan.factor_publication or plan.query.limit is not None)
        )
        projected = tuple(dict.fromkeys((*columns, event))) if date_block or rule in {"next_session_open", "session_open"} else columns
        if date_block and temporal.requires_consumer_binding:
            for field, inclusive in clocks:
                bound, exclusive = temporal.source_cutoff_parameter(field, now)
                if exclusive or not inclusive:
                    bound -= timedelta(days=1)
                last = min(last, bound)
        counters = self._statistics[binding_id]
        result = None
        if date_block and first <= last and (last - first).days < _PIT_BLOCK_DAYS and self._block_limit:
            block = self._blocks.get(binding_id)
            if block is not None and block[0] <= first <= last <= block[1]:
                self._blocks.move_to_end(binding_id)
                counters["block_hits"] += 1
            else:
                block_last = first + timedelta(days=min(_PIT_BLOCK_DAYS - 1, (scope.end - first).days))
                block_plan = plan
                if temporal.requires_consumer_binding:
                    # 这里只读取单一日期可见时钟。SQL 提前加载，真实时钟仍在 Arrow 交付边界裁剪。
                    if any(not inclusive for _field, inclusive in clocks):
                        block_last = min(block_last, date.max - timedelta(days=1))
                        bound = block_last + timedelta(days=1)
                    else:
                        bound = block_last
                    block_plan = plan.bind_consumer_time(datetime.combine(bound, time.min, ZoneInfo(temporal.source_timezone or zone.key)))
                try:
                    table = self._query(binding_id, block_plan, projected, first, block_last,
                                        block_limit=self._block_limit)
                except duckdb.OutOfMemoryException:
                    table = None
                block = None
                if table is not None and block_last >= last:
                    if binding_id in self._blocks:
                        self._block_bytes -= self._blocks.pop(binding_id)[2].nbytes
                    while self._block_bytes + table.nbytes > self._block_limit:
                        self._block_bytes -= self._blocks.popitem(last=False)[1][2].nbytes
                    self._block_bytes += table.nbytes
                    block = self._blocks[binding_id] = (first, block_last, table)
            if block is not None:
                started = perf_counter()
                dates = block[2].column(event)
                result = block[2].filter(pc.and_(pc.greater_equal(dates, first), pc.less_equal(dates, last)))
                counters["slice_seconds"] += perf_counter() - started
        if result is None:
            if temporal.requires_consumer_binding:
                plan = plan.bind_consumer_time(now)
            result = self._query(binding_id, plan, projected, first, last)
        if rule in {"next_session_open", "session_open"} and not set(
                result.column(event).unique().to_pylist()).issubset(sessions):
            raise ValueError("行情日期超出冻结交易日历")
        return result if projected == columns else result.select(columns)
