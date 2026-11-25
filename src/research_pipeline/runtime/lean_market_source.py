"""从已准入来源按日读取日线或一分钟，不生成行情副本。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from itertools import groupby
from pathlib import Path
from zoneinfo import ZoneInfo
from time import perf_counter

import pyarrow as pa

from research_pipeline.data_plane import (
    ArtifactResolver, DatasetFilter, PartitionedDatasetRef,
)
from research_pipeline.domain.minute_request_scope import MinuteRequestCalendar
from .adapters.common import _environment, _input_external_root
from .adapters.minute_io import _minute_root, _minute_scan_plan_from_environment


_ZONE = ZoneInfo("Asia/Shanghai")


from .lean_minute_parquet import _cash_lifecycle_endings, _cash_session_groups


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


def _cash_sessions(scope, symbols, start, end, *, clock_end=None, lifecycle=None, calendar=None):
    members, windows = {}, {}
    endings = _cash_lifecycle_endings(lifecycle)
    until = min(end, max(endings.values())) if set(symbols) <= endings.keys() else end
    if start >= until:
        return members, windows
    if calendar is None:
        calendar = MinuteRequestCalendar(scope.calendar)
    for codes, sessions, ending in _cash_session_groups(calendar, symbols, start, until, endings):
        for session in sessions:
            begin, finish = session.start_at, session.end_at
            if ending is not None:
                if begin >= ending:
                    continue
                finish = min(finish, ending)
            finish = min(finish, clock_end) if clock_end is not None else finish
            if begin > finish:
                continue
            day = session.trading_date
            window = (begin, finish, session.session_open, session.session_close)
            if windows.setdefault(day, window) != window:
                raise ValueError("同一现货账户的执行窗口与冻结会话不一致")
            members.setdefault(day, []).extend(codes)
    members_by_day = {}
    windows_by_day = {}
    for day, (begin, finish, opening, closing) in windows.items():
        key = day.isoformat()
        members_by_day[key] = pa.array(sorted(set(members[day])))
        windows_by_day[key] = {
            "start_at": begin.astimezone(_ZONE).isoformat(), "end_at": finish.astimezone(_ZONE).isoformat(),
            "session_open_at": opening.isoformat(), "session_close_at": closing.isoformat(),
        }
    return members_by_day, windows_by_day


class MinuteMarketSource:
    def __init__(self, context, payload, scope, symbols, lifecycle=None):
        self.scope = scope

        if payload.get("interval_minutes") != 1:
            raise ValueError("LEAN分钟输入仅支持一分钟")
        self.frequency = "1m"
        self.dataset = PartitionedDatasetRef.from_dict(payload["partitioned_dataset"])
        self._lifecycle = lifecycle
        self._symbols = frozenset(symbols)
        self._research_symbols = frozenset(self.dataset.instruments)
        if not self._symbols <= self._research_symbols:
            raise ValueError("分钟行情证券与策略范围不一致")
        self.plan = _minute_scan_plan_from_environment(context, str(payload["request_id"]))
        self._set_sessions(symbols)
        self._reader_memory = min(context.effective_resource_budget.memory_bytes // 3, 256 * 1024**2)
        self._root = _minute_root(context)
        self.source_identity = payload["source_snapshot_hash"]
        self._minute_manifest = None

    def _set_sessions(self, symbols):
        scope = self.scope
        self.futures_calendar = None
        if getattr(self.plan, "asset_class", None) == "cn_future" or any(
                item.instrument.asset_class == "cn_future" and item.instrument.instrument_id in symbols
                for item in scope.calendar.policies):
            from .lean_futures_inputs import FuturesCalendar
            self.futures_calendar = FuturesCalendar(scope.calendar, symbols)
            self.dates = self.futures_calendar.dates_between(self.plan.start_at, self.plan.visible_end)
        else:
            self._request_calendar = MinuteRequestCalendar(scope.calendar)
            self.clock_end = min(self.plan.end_at, self.plan.as_of)
            self.terminal_clock_end = min(self.plan.end_at - timedelta(microseconds=1), self.plan.as_of)
            self._symbols_by_day, self.execution_windows = _cash_sessions(scope, symbols,
                self.plan.start_at, self.plan.visible_end, clock_end=self.clock_end, lifecycle=self._lifecycle,
                calendar=self._request_calendar)
            self.dates = tuple(sorted(self._symbols_by_day))

    def native_minute_manifest(self):
        from .lean_minute_parquet import minute_parquet_manifest

        if self._minute_manifest is None:
            self._minute_manifest = minute_parquet_manifest(self)
        return self._minute_manifest

    def native_minute_calendar_windows(self):
        if self.futures_calendar is None:
            windows = self.execution_windows
            if self._symbols != self._research_symbols:
                _, windows = _cash_sessions(self.scope, self.dataset.instruments,
                    self.plan.start_at, self.plan.visible_end, clock_end=self.clock_end, lifecycle=self._lifecycle,
                    calendar=self._request_calendar)
            return [{"start_at": item["start_at"], "end_at": item["end_at"]} for item in windows.values()]
        from .lean_minute_parquet import _schedule_segment_windows

        points = {(start, end) for _, start, end in _schedule_segment_windows(self.native_minute_manifest()["schedules"])
                  if end > self.plan.start_at and start < self.plan.visible_end}
        return [{"start_at": start.isoformat(), "end_at": end.isoformat()}
                for start, end in sorted(points)]

    @property
    def statistics(self):
        return {}

    def close(self):
        pass


class DailyMarketSource:
    def __init__(self, context, payload, scope, symbols, request_id, open_request_id, lifecycle=None):
        bundle = payload.get("data_bundle", payload)
        raw = bundle.get("references", {}).get(request_id)
        if raw is None:
            raise ValueError("日线输入缺少bars_request_id对应来源")
        plans = _environment(context).admitted_plans
        self.descriptor = dict(root=str((_input_external_root(context, "bars") / "data").resolve()),
            reference=raw, open_reference=bundle["references"][open_request_id],
            plan=plans[request_id].to_dict(), open_plan=plans[open_request_id].to_dict(),
            source_identity=bundle["bundle_hash"])
        self._setup(self.descriptor, scope, symbols, context.effective_resource_budget.memory_bytes,
                    plans[request_id], plans[open_request_id], lifecycle)

    @classmethod
    def from_frozen(cls, descriptor, scope, symbols, memory, lifecycle):
        from research_pipeline.data_plane.admitted_plan_codec import admitted_plan_from_dict

        market = cls.__new__(cls)
        market._setup(descriptor, scope, symbols, memory, admitted_plan_from_dict(descriptor["plan"]),
                      admitted_plan_from_dict(descriptor["open_plan"]), lifecycle)
        return market

    def _setup(self, descriptor, scope, symbols, memory, plan, open_plan, lifecycle):
        from .lean_native_data import _resolve_dataset

        self.scope = scope
        self.frequency = "1d"
        self.plan = plan
        if self.plan is None or self.plan.query.adjustment not in {"raw", "none", "unadjusted"}:
            raise ValueError("日线成交行情必须绑定未复权的准入查询")
        if not set(symbols) <= set(self.plan.query.universe.instruments):
            raise ValueError("日线行情来源未覆盖交易域")
        resolver = ArtifactResolver(Path(descriptor["root"]), max_batch_bytes=memory // 8)
        self.dataset = _resolve_dataset(resolver, descriptor["reference"], plan)
        self.open_plan = open_plan
        self.open_dataset = _resolve_dataset(resolver, descriptor["open_reference"], open_plan)
        from research_pipeline.platform.daily_projection import daily_projection
        self.open_column = daily_projection(self.open_plan)["open"]
        physical = daily_projection(self.plan)
        required = ("open", "high", "low", "close", "volume")
        if not set(required) <= set(physical):
            raise ValueError("日线准入查询缺少OHLCV字段映射")
        self.columns = {name: physical[name] for name in required}
        self.code_field, self.date_field = self.plan.instrument_field, self.plan.event_time_field
        self.source_identity = descriptor["source_identity"]
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
            # 合约下一会话可以不同，分别按各自的消费时点选版。
            codes = sorted(code for code in plan.query.universe.instruments
                           if (code, day) in self.futures_calendar.by_key)
            # 连续同可见时点的证券一次查询，保留正式证券顺序。
            groups = groupby(codes, key=lambda code: selection.completion_time(code, day))
            return (batch for at, members in groups for batch in dataset.iter_batches_at(
                plan=plan, consumer_time=at, columns=columns, batch_size=options["batch_size"],
                filters=(*options["filters"], DatasetFilter(plan.instrument_field, "in", tuple(members)))))
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
