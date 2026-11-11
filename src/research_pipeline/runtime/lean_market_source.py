"""从已准入来源按日读取日线或一分钟，不生成行情副本。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from time import perf_counter

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef, DatasetFilter, PartitionedDatasetRef
from research_pipeline.data_plane import build_minute_resample_plan, execute_minute_resample, MinuteResampleBudget
from .adapters.common import _environment, _input_external_root
from .adapters.minute_io import _minute_root, _minute_scan_plan_from_environment, _normalized_bar_batch, _session_instruments


_ZONE = ZoneInfo("Asia/Shanghai")


def _validate_futures_session_batch(batch, calendar, day):
    """按合约广播冻结会话边界，保留夜盘和小节内完整分钟校验。"""
    if not batch.num_rows:
        return
    codes = pc.dictionary_encode(batch["instrument"])
    if isinstance(codes, pa.ChunkedArray):
        codes = codes.combine_chunks()
    if codes.null_count or batch["bar_start"].null_count or batch["bar_end"].null_count:
        raise ValueError("期货分钟会话字段为空")
    sessions = [tuple(segment for segment in calendar.session(code, day).segments if segment.bar_eligible)
                for code in codes.dictionary.to_pylist()]
    stamp = pa.timestamp("us", tz="UTC")
    starts = pc.cast(pc.cast(batch["bar_start"], stamp), pa.int64()).to_numpy(zero_copy_only=False)
    ends = pc.cast(pc.cast(batch["bar_end"], stamp), pa.int64()).to_numpy(zero_copy_only=False)
    indices = codes.indices.to_numpy(zero_copy_only=False)
    accepted = np.zeros(batch.num_rows, dtype=bool)
    for offset in range(max(map(len, sessions), default=0)):
        lower = [segments[offset].starts_at if offset < len(segments) else None for segments in sessions]
        upper = [segments[offset].ends_at if offset < len(segments) else None for segments in sessions]
        # 缺少该小节的合约使用空区间，不允许借用其他合约的会话。
        lower = pc.fill_null(pc.cast(pa.array(lower, type=stamp), pa.int64()), 1).to_numpy()
        upper = pc.fill_null(pc.cast(pa.array(upper, type=stamp), pa.int64()), 0).to_numpy()
        accepted |= (starts >= lower[indices]) & (ends <= upper[indices])
    invalid = np.flatnonzero(~(accepted & (starts < ends)))
    if invalid.size:
        code = batch["instrument"][int(invalid[0])].as_py()
        raise ValueError(f"期货分钟与冻结会话不一致：{code} {day}")


def lifecycle_inputs(payload, symbols):
    """独立交接已确认的上市末日，不从行情或会话边界推断退市。"""
    from collections.abc import Mapping

    if not isinstance(payload, Mapping) or set(payload) - set(symbols):
        raise ValueError("lifecycle必须按冻结证券代码声明")
    result = {}
    for code, row in payload.items():
        if not isinstance(row, Mapping):
            raise ValueError(f"lifecycle证券记录无效：{code}")
        if row.get("delisted_date") is None:
            continue
        ending = date.fromisoformat(row["delisted_date"])
        available = datetime.fromisoformat(row["available_at"])
        source = row.get("source_ref")
        if available.tzinfo is None or available.utcoffset() is None or not isinstance(source, str) or not source.strip():
            raise ValueError(f"退市日期必须有带时区的可见时间及来源：{code}")
        result[code] = {"delisted_date": ending.isoformat(), "available_at": available.isoformat(),
                        "source_ref": source}
    return result


def terminal_observation_at(market, lifecycle, symbols):
    """全部证券已退市时，保留请求终点的独立账户观察。"""
    finish = getattr(market, "terminal_clock_end", getattr(market, "clock_end", None))
    if finish is None or not market.execution_windows:
        return None
    last = max(datetime.fromisoformat(row["end_at"]) for row in market.execution_windows.values())
    if finish <= last:
        return None
    for code in symbols:
        ending = lifecycle.get(code)
        if ending is None:
            return None
        effective = datetime.combine(date.fromisoformat(ending["delisted_date"]) + timedelta(days=1), time.min, _ZONE)
        if finish < max(effective, datetime.fromisoformat(ending["available_at"])):
            return None
    return finish.isoformat()


def _cash_sessions(scope, symbols, start, end, *, clock_end=None, lifecycle=None):
    from research_pipeline.domain.minute_request_scope import minute_request_sessions
    from research_pipeline.domain.session_calendar import SessionCalendarResolver

    resolver = SessionCalendarResolver(scope.calendar)
    members, windows = {}, {}
    window_values = {}
    endings = {code: max(datetime.combine(date.fromisoformat(row["delisted_date"]) + timedelta(days=1), time.min, _ZONE),
                         datetime.fromisoformat(row["available_at"]))
               for code, row in (lifecycle or {}).items()}
    until = min(end, max(endings.values())) if set(symbols) <= endings.keys() else end
    if start >= until:
        return members, windows
    for policy, day, begin, finish in minute_request_sessions(scope.calendar, instruments=symbols,
            start_at=start, end_at=until):
        ending = endings.get(policy.instrument.instrument_id)
        if ending is not None:
            if begin >= ending:
                continue
            finish = min(finish, ending)
        segments = tuple(item for item in resolver._segments_for_date(policy, day) if item.bar_eligible)
        opening, closing = min(item.starts_at for item in segments), max(item.ends_at for item in segments)
        finish = min(finish, clock_end) if clock_end is not None else finish
        if begin > finish:
            continue
        key = day.isoformat()
        window_key = (begin, finish, opening, closing)
        window = window_values.get(window_key)
        if window is None:
            window = {"start_at": begin.astimezone(_ZONE).isoformat(), "end_at": finish.astimezone(_ZONE).isoformat(),
                "session_open_at": opening.isoformat(), "session_close_at": closing.isoformat()}
            window_values[window_key] = window
        if key in windows and windows[key] != window:
            raise ValueError("同一现货账户的执行窗口与冻结会话不一致")
        windows[key] = window
        members.setdefault(key, []).append(policy.instrument.instrument_id)
    return {day: pa.array(sorted(set(codes))) for day, codes in members.items()}, windows


class MinuteMarketSource:
    def __init__(self, context, payload, scope, symbols, lifecycle=None):
        from research_pipeline.data_plane.daily_stream import DayBatchReader

        if payload.get("interval_minutes") != 1:
            raise ValueError("LEAN分钟输入仅支持一分钟")
        self.frequency = "1m"
        self.dataset = PartitionedDatasetRef.from_dict(payload["partitioned_dataset"])
        if set(self.dataset.instruments) != set(symbols):
            raise ValueError("分钟行情证券与策略范围不一致")
        self.plan = _minute_scan_plan_from_environment(context, str(payload["request_id"]))
        self.futures_calendar = None
        if getattr(self.plan, "asset_class", None) == "cn_future" or any(
                item.instrument.asset_class == "cn_future" and item.instrument.instrument_id in symbols
                for item in scope.calendar.policies):
            from .lean_futures_inputs import FuturesCalendar
            self.futures_calendar = FuturesCalendar(scope.calendar, symbols)
            self.dates = self.futures_calendar.dates_between(self.plan.start_at, self.plan.visible_end)
        else:
            self.clock_end = min(self.plan.end_at, self.plan.as_of)
            self.terminal_clock_end = min(self.plan.end_at - timedelta(microseconds=1), self.plan.as_of)
            self._symbols_by_day, self.execution_windows = _cash_sessions(scope, symbols,
                self.plan.start_at, self.plan.visible_end, clock_end=self.clock_end, lifecycle=lifecycle)
            self.dates = tuple(sorted(self._symbols_by_day))
        self._physical_days = {}
        self._retained_bytes = context.effective_resource_budget.memory_bytes // 6
        self.resample_plan = build_minute_resample_plan(self.plan, interval_minutes=1,
            session_bundle=scope.calendar, instruments=_session_instruments(self.plan, scope.calendar),
            budget=MinuteResampleBudget(output_batch_rows=min(65_536,
                max(1, context.effective_resource_budget.memory_bytes // (32 * 256)))))
        roots = {"minute_data": _minute_root(context), "runtime_artifact": _input_external_root(context, "bars")}
        self.reader = DayBatchReader(self.dataset, roots, columns=self.dataset.allowed_columns,
            memory_bytes=context.effective_resource_budget.memory_bytes // 3)
        self.source_identity = payload["source_snapshot_hash"]
        self.timings = {}

    def get_day(self, day):
        started = perf_counter()
        if self.futures_calendar is not None:
            yield from self._get_futures_day(day)
            return
        table = self.reader.get_day(date.fromisoformat(day))
        read_at = perf_counter()
        # 月文件覆盖整个窗口的证券并集；仅把当日冻结会话内的证券交给分钟校验。
        mask = pc.is_in(table[self.dataset.instrument_field], value_set=self._symbols_by_day[day])
        if not pc.all(mask).as_py():
            table = table.filter(mask)
        filtered_at = perf_counter()
        normalize_seconds = 0.0
        iterator = iter(self._normalized(table, day[:7]))
        while True:
            before = perf_counter()
            try:
                batch = next(iterator)
            except StopIteration:
                normalize_seconds += perf_counter() - before
                break
            normalize_seconds += perf_counter() - before
            yield batch
        self.timings[day] = {"read_seconds": read_at - started,
            "scope_filter_seconds": filtered_at - read_at, "normalize_seconds": normalize_seconds,
            "cache_bytes": self.reader.cached_bytes}

    def _normalized(self, table, partition_key):
        if "bar_start" in table.column_names:
            batches = table.to_batches(max_chunksize=65_536)
        else:
            class Source:
                source_identity = self.source_identity

                def __iter__(inner):
                    return iter(table.to_batches(max_chunksize=65_536))

            batches = execute_minute_resample(Source(), plan=self.resample_plan,
                columns=("code", "dt", "bar_start", "trading_date", "session_id", "interval_minutes",
                         "open", "high", "low", "close", "volume", "completed", "bar_status"))
        for batch in batches:
            if "instrument" in batch.schema.names and "bar_end" in batch.schema.names:
                yield batch
            else:
                yield _normalized_bar_batch(batch, self.source_identity, partition_key, columns=(
                    "instrument", "bar_start", "bar_end", "available_time", "trading_date",
                    "interval_minutes", "open", "high", "low", "close", "volume", "completed", "quality_status"))

    def _get_futures_day(self, day):
        sessions = [self.futures_calendar.session(code, day) for code in self.futures_calendar.symbols
                    if (code, date.fromisoformat(day)) in self.futures_calendar.by_key]
        physical_dates = set()
        for session in sessions:
            for segment in session.segments:
                if not segment.bar_eligible:
                    continue
                current = segment.starts_at.date()
                while current <= segment.ends_at.date():
                    physical_dates.add(current)
                    current += timedelta(days=1)
        for physical_day in sorted(physical_dates):
            if physical_day not in self._physical_days:
                self._physical_days[physical_day] = self.reader.get_day(physical_day)
            table = self._physical_days[physical_day]
            for batch in self._normalized(table, physical_day.isoformat()[:7]):
                dates = batch.column("trading_date")
                if dates.null_count:
                    raise ValueError("期货分钟缺少正式会话解析的trading_date")
                selected = batch.filter(pc.equal(dates, pa.scalar(date.fromisoformat(day))))
                if not selected.num_rows:
                    continue
                _validate_futures_session_batch(selected, self.futures_calendar, day)
                yield selected
        index = self.dates.index(day)
        following = self.dates[index + 1:]
        if following:
            next_day = date.fromisoformat(following[0])
            earliest = min(segment.starts_at.date() for (code, value), session in self.futures_calendar.by_key.items()
                           if value == next_day for segment in session.segments if segment.bar_eligible)
            self._physical_days = {value: table for value, table in self._physical_days.items() if value >= earliest}
        else:
            self._physical_days.clear()
        if sum(table.get_total_buffer_size() for table in self._physical_days.values()) > self._retained_bytes:
            raise MemoryError("跨交易日分钟缓存超过声明内存额度")

    @property
    def statistics(self):
        return self.reader.statistics

    def close(self):
        self._physical_days.clear()
        self.reader.close()


class DailyMarketSource:
    def __init__(self, context, payload, scope, symbols, request_id, open_request_id, lifecycle=None):
        self.frequency = "1d"
        bundle = payload.get("data_bundle", payload)
        raw = bundle.get("references", {}).get(request_id)
        if raw is None:
            raise ValueError("日线输入缺少bars_request_id对应来源")
        self.plan = _environment(context).admitted_plans.get(request_id)
        if self.plan is None or self.plan.query.adjustment not in {"raw", "none", "unadjusted"}:
            raise ValueError("日线成交行情必须绑定未复权的准入查询")
        if set(self.plan.query.universe.instruments) != set(symbols):
            raise ValueError("日线行情证券与策略范围不一致")
        reference = DatasetArtifactRef.from_dict(raw)
        resolver = ArtifactResolver(_input_external_root(context, "bars") / "data",
            max_batch_bytes=context.effective_resource_budget.memory_bytes // 8)
        self.dataset = resolver.resolve(reference)
        self.open_plan = _environment(context).admitted_plans[open_request_id]
        self.open_dataset = resolver.resolve(DatasetArtifactRef.from_dict(bundle["references"][open_request_id]))
        from research_pipeline.platform.daily_projection import daily_projection
        self.open_column = daily_projection(self.open_plan)["open"]
        physical = daily_projection(self.plan)
        required = ("open", "high", "low", "close", "volume")
        if not set(required) <= set(physical):
            raise ValueError("日线准入查询缺少OHLCV字段映射")
        self.columns = {name: physical[name] for name in required}
        self.code_field, self.date_field = self.plan.instrument_field, self.plan.event_time_field
        self.source_identity = bundle["bundle_hash"]
        self.start, self.end = self.plan.query.time_range.start, self.plan.query.time_range.end
        self.futures_calendar = None
        if getattr(self.plan.query, "dataset_id", None) == "cn_futures.daily_bar":
            from .lean_futures_inputs import FuturesCalendar
            self.futures_calendar = FuturesCalendar(scope.calendar, symbols)
            self.dates = tuple(sorted({day.isoformat() for code, day in self.futures_calendar.by_key
                                      if self.start <= day <= self.end}))
        else:
            self.clock_end = datetime.combine(self.end, time(15), _ZONE)
            _, self.execution_windows = _cash_sessions(scope, symbols,
                datetime.combine(self.start, time.min, _ZONE),
                datetime.combine(self.end + timedelta(days=1), time.min, _ZONE),
                **({"lifecycle": lifecycle} if lifecycle else {}))
            self.dates = tuple(sorted(self.execution_windows))
        self._available_at = {
            day: datetime.combine(date.fromisoformat(next_day), time(9, 30), _ZONE)
            for day, next_day in zip(self.dates, self.dates[1:])
        }
        self._statistics = {"daily_queries": 0, "rows": 0, "open_rows": 0}

    def _read_batches(self, dataset, plan, day, columns):
        options = dict(columns=columns, filters=(DatasetFilter(plan.event_time_field, "eq", day),),
                       batch_size=65_536)
        if self.futures_calendar is not None and plan.temporal_selection.requires_consumer_binding:
            selection = plan.session_close_binding
            # 这里只预读当天；交给引擎的每行仍保留该合约的实际可见时刻。
            codes = tuple(code for code in plan.query.universe.instruments
                          if (code, day) in self.futures_calendar.by_key)
            if not codes:
                return iter(())
            at = max(selection.completion_time(code, day) for code in codes)
            options["filters"] += (DatasetFilter(plan.instrument_field, "in", codes),)
            return dataset.iter_batches_at(plan=plan, consumer_time=at, **options)
        return dataset.iter_batches(**options)

    def get_day(self, day):
        value = date.fromisoformat(day)
        open_rows = {}
        for batch in self._read_batches(self.open_dataset, self.open_plan, value,
                (self.open_plan.instrument_field, self.open_column)):
            for code, price in zip(batch[self.open_plan.instrument_field].to_pylist(), batch[self.open_column].to_pylist()):
                if code in open_rows:
                    raise ValueError("日线开盘来源含重复证券")
                open_rows[code] = price
            self._statistics["open_rows"] += batch.num_rows
        names = (self.code_field, self.date_field, *self.columns.values())
        batches = self._read_batches(self.dataset, self.plan, value, names)
        self._statistics["daily_queries"] += 2
        stamp = pa.timestamp("us", tz="Asia/Shanghai")
        for batch in batches:
            size = batch.num_rows
            self._statistics["rows"] += size
            def col(field):
                return batch.column(batch.schema.get_field_index(field))
            codes = col(self.code_field).to_pylist()
            if any(code not in open_rows for code in codes):
                raise ValueError("日线缺少同证券、同日期的已准入开盘价格")
            opens = pa.array([open_rows[code] for code in codes], type=col(self.columns["open"]).type)
            if opens.to_pylist() != col(self.columns["open"]).to_pylist():
                raise ValueError("开盘来源与完整日线的原始开盘价格不一致")
            # 完整日线在下一交易日开盘可见；末日无后续会话时只供内部估值。
            arrays = {"instrument": col(self.code_field),
                "bar_start": pa.repeat(pa.scalar(datetime.combine(value, time(9, 30), _ZONE), type=stamp), size),
                "bar_end": pa.repeat(pa.scalar(datetime.combine(value, time(15), _ZONE), type=stamp), size),
                "available_time": pa.repeat(pa.scalar(self._available_at.get(day), type=stamp), size),
                "completed": pa.repeat(True, size), "quality_status": pa.repeat("pass", size),
                "interval_minutes": pa.repeat(1440, size),
                **{name: opens if name == "open" else col(field) for name, field in self.columns.items()}}
            if self.futures_calendar is not None:
                sessions = [self.futures_calendar.session(code, value) for code in codes]
                arrays["bar_start"] = pa.array([min(item.starts_at for item in session.segments if item.bar_eligible)
                                                 for session in sessions], type=stamp)
                arrays["bar_end"] = pa.array([max(item.ends_at for item in session.segments if item.bar_eligible)
                                               for session in sessions], type=stamp)
                arrays["available_time"] = pa.array([self.futures_calendar.next_open(code, value) for code in codes], type=stamp)
                arrays["trading_date"] = pa.repeat(pa.scalar(value, pa.date32()), size)
            yield pa.RecordBatch.from_arrays(list(arrays.values()), names=list(arrays))

    @property
    def statistics(self):
        return dict(self._statistics)

    def close(self):
        pass


def open_market_source(context, payload, scope, symbols, parameters, lifecycle=None):
    frequency = parameters.get("frequency", "1m")
    if frequency == "1m":
        return MinuteMarketSource(context, payload, scope, symbols, lifecycle)
    if frequency == "1d":
        return DailyMarketSource(context, payload, scope, symbols, parameters["bars_request_id"], parameters["open_request_id"], lifecycle)
    raise ValueError("行情频率仅支持1m或1d")
