"""将已准入分钟行情的文件与会话投影给 LEAN。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from research_pipeline.data_plane import (
    PartitionedDatasetResolver,
    build_minute_partitioned_dataset,
)
from .lean_futures_inputs import FuturesCalendar, ZONE


def _cash_lifecycle_endings(lifecycle):
    return {code: max(datetime.combine(date.fromisoformat(row["delisted_date"]) + timedelta(days=1), time.min, ZONE),
                      datetime.fromisoformat(row["available_at"]))
            for code, row in (lifecycle or {}).items()}


def _cash_session_groups(calendar, symbols, start, end, endings):
    calendar.check_window(symbols, start, end)
    groups = {}
    for code in symbols:
        _, sessions = calendar.sessions(code, start, end)
        ending = endings.get(code)
        codes, _, _ = groups.setdefault((id(sessions), ending), ([], sessions, ending))
        codes.append(code)
    return groups.values()


def _research_sessions(market):
    plan = market.plan
    scope = market.scope
    calendar = (market.futures_calendar if market._symbols == market._research_symbols else
                FuturesCalendar(scope.calendar, market.dataset.instruments))
    for (code, day), session in calendar.by_key.items():
        for segment in session.segments:
            if (segment.bar_eligible and segment.ends_at >= plan.start_at
                    and segment.starts_at < plan.visible_end):
                yield code, day, segment.starts_at, segment.ends_at


def _schedules(market):
    by_schedule = {}
    if market.futures_calendar is None:
        plan = market.plan
        endings = _cash_lifecycle_endings(market._lifecycle)
        session_segments = {}
        for codes, sessions, ending in _cash_session_groups(market._request_calendar,
                market.dataset.instruments, plan.start_at, plan.visible_end, endings):
            segments = []
            for session in sessions:
                if ending is not None and session.start_at >= ending:
                    continue
                key = (session.trading_date, session.segments)
                shared = session_segments.get(key)
                if shared is None:
                    shared = tuple((session.trading_date, segment.starts_at, segment.ends_at)
                                   for segment in session.segments)
                    session_segments[key] = shared
                segments.extend(shared)
            by_schedule.setdefault(tuple(sorted(segments, key=lambda item: item[1])), []).extend(codes)
    else:
        by_code = {}
        for code, day, start, end in _research_sessions(market):
            by_code.setdefault(code, []).append((day, start, end))
        for code in market.dataset.instruments:
            segments = tuple(sorted(by_code.pop(code, ()), key=lambda item: item[1]))
            by_schedule.setdefault(segments, []).append(code)
    return _encode_schedules(by_schedule)


def _encode_schedules(by_schedule):
    # 共享时段表保留完整时钟；证券可以选多个不连续区间，含独有时段。
    shared = sorted({segment for schedule in by_schedule for segment in schedule},
                    key=lambda item: (item[1], item[2], item[0]))
    indices = {segment: position for position, segment in enumerate(shared)}
    groups = []
    for schedule, codes in by_schedule.items():
        ranges = []
        for segment in schedule:
            position = indices[segment]
            if ranges and position == ranges[-1][1]:
                ranges[-1][1] += 1
            else:
                ranges.append([position, position + 1])
        groups.append({"symbols": sorted(codes), "ranges": ranges})
    templates = {}
    segments = []
    for day, start, end in shared:
        template = (
            (start.date() - day).days, start.isoformat().split("T", 1)[1],
            (end.date() - day).days, end.isoformat().split("T", 1)[1],
        )
        segments.append((day, templates.setdefault(template, len(templates))))
    return {"segments": segments, "templates": list(templates), "groups": groups}


def _schedule_segment_windows(schedules):
    for day, template in schedules["segments"]:
        start_offset, start, end_offset, end = schedules["templates"][template]
        yield (day,
               datetime.combine(day + timedelta(days=start_offset), time.fromisoformat(start)),
               datetime.combine(day + timedelta(days=end_offset), time.fromisoformat(end)))


def _partition(reference, path, symbols):
    schema = pq.ParquetFile(path).schema_arrow
    timestamp_type = schema.field("dt").type

    def bound(value):
        if not timestamp_type.tz:
            value = value.astimezone(ZONE).replace(tzinfo=None)
        return pa.scalar(value, type=timestamp_type)

    field = ds.field("dt")
    expression = ((field >= bound(reference.logical_start)) & (field < bound(reference.logical_end))
                  & ds.field("code").isin(symbols))
    fragment = ds.ParquetFileFormat().make_fragment(str(path), filesystem=fs.LocalFileSystem())
    groups = [group.id for selected in fragment.split_by_row_group(filter=expression, schema=schema)
              for group in selected.row_groups]
    return {"path": str(path), "month": reference.partition_key,
        "start_at": reference.logical_start.isoformat(), "end_at": reference.logical_end.isoformat(),
        "size": reference.size, "mtime_ticks": 621355968000000000 + reference.mtime_ns // 100,
        "row_count": reference.row_count, "row_groups": reference.row_groups,
        "timestamp_utc": timestamp_type.tz is not None, "groups": groups}


def minute_parquet_manifest(market):
    """只传递元数据。共用同一日历的证券在清单中引用同一份会话序列。"""
    dataset = market.dataset
    if dataset != build_minute_partitioned_dataset(market.plan):
        raise ValueError("LEAN分钟行情必须引用当前已准入的原始月文件")
    mandatory = {"code", "dt", "open", "high", "low", "close", "volume", "money"}
    if not mandatory <= set(dataset.allowed_columns):
        raise ValueError("LEAN分钟行情缺少已准入的行情或质量校验列")
    resolver = PartitionedDatasetResolver({"minute_data": market._root})
    partitions = []
    for reference in dataset.partitions:
        path = resolver.resolve_partition(dataset, reference.partition_key).path
        partitions.append(_partition(reference, path, dataset.instruments))
    return {"partitions": partitions, "memory_bytes": market._reader_memory,
        "quality_columns": [name for name in ("avg", "open_interest") if name in dataset.allowed_columns],
        "schedules": _schedules(market)}
