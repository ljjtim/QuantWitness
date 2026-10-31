"""从已准入来源按日读取日线或一分钟，不生成行情副本。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa

from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef, DatasetFilter, PartitionedDatasetRef
from research_pipeline.data_plane import build_minute_resample_plan, execute_minute_resample
from .adapters.common import _environment, _input_external_root
from .adapters.minute_io import _minute_root, _minute_scan_plan_from_environment, _normalized_bar_batch, _session_instruments


_ZONE = ZoneInfo("Asia/Shanghai")


def _calendar_dates(scope, symbols, start, end):
    from research_pipeline.domain.minute_request_scope import minute_request_sessions

    sessions = minute_request_sessions(scope.calendar, instruments=symbols,
        start_at=start, end_at=end)
    return tuple(sorted({day.isoformat() for _, day, _, _ in sessions}))


class MinuteMarketSource:
    def __init__(self, context, payload, scope, symbols):
        from research_pipeline.data_plane.daily_stream import DayBatchReader

        if payload.get("interval_minutes") != 1:
            raise ValueError("LEAN分钟输入仅支持一分钟")
        self.frequency = "1m"
        self.dataset = PartitionedDatasetRef.from_dict(payload["partitioned_dataset"])
        if set(self.dataset.instruments) != set(symbols):
            raise ValueError("分钟行情证券与策略范围不一致")
        self.plan = _minute_scan_plan_from_environment(context, str(payload["request_id"]))
        self.dates = _calendar_dates(scope, symbols, self.plan.start_at, self.plan.end_at)
        self.resample_plan = build_minute_resample_plan(self.plan, interval_minutes=1,
            session_bundle=scope.calendar, instruments=_session_instruments(self.plan, scope.calendar))
        roots = {"minute_data": _minute_root(context), "runtime_artifact": _input_external_root(context, "bars")}
        self.reader = DayBatchReader(self.dataset, roots, columns=self.dataset.allowed_columns,
            memory_bytes=context.effective_resource_budget.memory_bytes // 3)
        self.source_identity = payload["source_snapshot_hash"]

    def get_day(self, day):
        table = self.reader.get_day(date.fromisoformat(day))
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
            yield _normalized_bar_batch(batch, self.source_identity, day[:7])

    @property
    def statistics(self):
        return self.reader.statistics

    def close(self):
        self.reader.close()


class DailyMarketSource:
    def __init__(self, context, payload, scope, symbols, request_id, open_request_id):
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
        self.dates = _calendar_dates(scope, symbols,
            datetime.combine(self.start, time.min, _ZONE),
            datetime.combine(self.end + timedelta(days=1), time.min, _ZONE))
        self._available_at = {
            day: datetime.combine(date.fromisoformat(next_day), time(9, 30), _ZONE)
            for day, next_day in zip(self.dates, self.dates[1:])
        }
        self._statistics = {"daily_queries": 0, "rows": 0, "open_rows": 0}

    def get_day(self, day):
        value = date.fromisoformat(day)
        open_rows = {}
        for batch in self.open_dataset.iter_batches(
                columns=(self.open_plan.instrument_field, self.open_column),
                filters=(DatasetFilter(self.open_plan.event_time_field, "eq", value),), batch_size=65_536):
            for code, price in zip(batch[self.open_plan.instrument_field].to_pylist(), batch[self.open_column].to_pylist()):
                if code in open_rows:
                    raise ValueError("日线开盘来源含重复证券")
                open_rows[code] = price
            self._statistics["open_rows"] += batch.num_rows
        names = (self.code_field, self.date_field, *self.columns.values())
        batches = self.dataset.iter_batches(columns=names,
            filters=(DatasetFilter(self.date_field, "eq", value),), batch_size=65_536)
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
            yield pa.RecordBatch.from_arrays(list(arrays.values()), names=list(arrays))

    @property
    def statistics(self):
        return dict(self._statistics)

    def close(self):
        pass


def open_market_source(context, payload, scope, symbols, parameters):
    frequency = parameters.get("frequency", "1m")
    if frequency == "1m":
        return MinuteMarketSource(context, payload, scope, symbols)
    if frequency == "1d":
        return DailyMarketSource(context, payload, scope, symbols, parameters["bars_request_id"], parameters["open_request_id"])
    raise ValueError("行情频率仅支持1m或1d")
