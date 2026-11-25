"""把正式准入来源装配为原生LEAN订阅和历史服务。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from contextlib import ExitStack
from zoneinfo import ZoneInfo
from dataclasses import replace

from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef
from research_pipeline.data_plane.admitted_plan_codec import validate_pit_binding
from .adapters.common import _environment, _input_external_payload, _input_external_root
from .lean_native_data import _aware, _bounds


_ZONE = ZoneInfo("Asia/Shanghai")


def _calendar_events(strategy):
    events = set()
    # 冻结终点可能是期货尾部结算观察；推进账户时钟，不增加交易会话。
    if strategy.get("end_at"):
        events.add(strategy["end_at"])
    for window in strategy.get("execution_windows", {}).values():
        events.update(window[name] for name in ("start_at", "end_at", "session_open_at", "session_close_at")
                      if _aware(window["start_at"]) <= _aware(window[name]) <= _aware(window["end_at"]))
    for session in strategy.get("sessions", ()):
        for key in ("start_at", "end_at", "open_at", "close_at"):
            if session.get(key):
                events.add(session[key])
        for segment in session.get("segments", ()):
            for key in ("start_at", "end_at"):
                if segment.get(key):
                    events.add(segment[key])
    if strategy.get("terminal_observation_at"):
        events.add(strategy["terminal_observation_at"])
    return sorted(events, key=_aware)


def _calendar_windows(strategy):
    windows = []
    for window in strategy.get("execution_windows", {}).values():
        # 现货真实会话由数据准入保留，午休不构造行情，独立时钟只推进调度。
        windows.append({"start_at": window["start_at"], "end_at": window["end_at"]})
    for session in strategy.get("sessions", ()):
        segments = session.get("segments", ())
        windows.extend({"start_at": item["start_at"], "end_at": item["end_at"]} for item in segments)
        if not segments and session.get("start_at") and session.get("end_at"):
            windows.append({"start_at": session["start_at"], "end_at": session["end_at"]})
    unique = {(item["start_at"], item["end_at"]): item for item in windows}
    return sorted(unique.values(), key=lambda item: _aware(item["start_at"]))


def prepare_native_data(context, parameters, declaration, market, strategy):
    with ExitStack() as setup:
        return _prepare_native_data(context, parameters, declaration, market, strategy, setup)


def _prepare_native_data(context, parameters, declaration, market, strategy, setup):
    from research_pipeline.data_plane.direct_sources import DirectVerifiedDataset
    from .lean_native_direct import direct_source, direct_targets
    import duckdb

    environment = _environment(context)
    events = _calendar_events(strategy)
    calendar_windows = _calendar_windows(strategy)
    if not events:
        raise ValueError("原生策略缺少冻结会话时钟")
    execution_start, execution_end = min(map(_aware, events)), max(map(_aware, events))
    memory = context.effective_resource_budget.memory_bytes
    bindings = []
    pit_sources = {}
    pit_databases = set()
    for binding in declaration.input_bindings:
        if binding.kind == "targets":
            direct, symbols = direct_targets(context, parameters)
            start, end = execution_start - timedelta(microseconds=1), execution_end
            info = dict(binding_id=binding.binding_id, kind="event", resolution="event", symbols=symbols,
                        trade_symbols=strategy["symbols"], history_start_at=start.isoformat(),
                        start_at=execution_start.isoformat(), end_at=end.isoformat(), page_size=1)
            bindings.append(dict(info, direct=direct))
            continue
        primary_minute = binding.input_port == "bars" and binding.resolution == "1m"
        if primary_minute:
            minute_plan = market.plan
            start, end = minute_plan.start_at, min(minute_plan.end_at, minute_plan.as_of)
            symbols = list(market.dataset.instruments)
            calendar_windows.extend(market.native_minute_calendar_windows())
        else:
            plan = environment.admitted_plans[binding.request_id]
            if binding.daily_time is None and plan.daily_availability_rule == "next_session_open":
                sessions = sorted({value for policy in market.scope.calendar.policies for value in policy.trading_dates})
                binding = replace(binding, daily_time={"rule": "next_session_open", "timezone": "Asia/Shanghai",
                                                       "sessions": [value.isoformat() for value in sessions]})
            start, end = _bounds(plan, binding.daily_time)
            symbols = list(plan.query.universe.instruments)
            if binding.kind == "bars" and binding.input_port == "bars":
                if market.futures_calendar is not None:
                    for session in market.futures_calendar.by_key.values():
                        calendar_windows.extend({"start_at": item.starts_at.isoformat(), "end_at": item.ends_at.isoformat()}
                                                for item in session.segments if item.bar_eligible
                                                and item.ends_at > start and item.starts_at <= end)
                elif binding.daily_time:
                    for raw_day in binding.daily_time["sessions"]:
                        day = date.fromisoformat(raw_day) if isinstance(raw_day, str) else raw_day
                        begin, finish = datetime.combine(day, time(9, 30), _ZONE), datetime.combine(day, time(15), _ZONE)
                        if finish > start and begin <= end:
                            calendar_windows.append({"start_at": begin.isoformat(), "end_at": finish.isoformat()})
        symbol_set = set(symbols)
        info = dict(binding_id=binding.binding_id, kind=binding.kind, resolution=binding.resolution,
                    symbols=symbols, trade_symbols=[code for code in strategy["symbols"] if code in symbol_set],
                    history_start_at=start.isoformat(), start_at=execution_start.isoformat(),
                    end_at=end.isoformat(), page_size=(240 if binding.kind == "bars" and binding.resolution == "1m"
                                                     else 1 if binding.kind == "bars" else 2048))
        if not primary_minute and plan.temporal_selection.effective_interval_selector is not None:
            info["effective_interval"] = plan.temporal_selection.effective_interval_selector.to_dict()
        if primary_minute:
            info["direct_minute"] = True
        else:
            payload = _input_external_payload(context, binding.input_port)
            bundle = payload.get("data_bundle", payload)
            source_root = _input_external_root(context, binding.input_port) / "data"
            reference = bundle["references"][binding.request_id]
            resolver = ArtifactResolver(source_root, max_batch_bytes=memory // 16)
            dataset = resolver.resolve(DatasetArtifactRef.from_dict(reference))
            if binding.kind == "pit":
                if not isinstance(dataset, DirectVerifiedDataset) or dataset.manifest["source_kind"] != "duckdb":
                    raise ValueError("算法内 PIT 直读需要当前准入的 DuckDB 直接来源")
                validate_pit_binding(dataset.plan, binding)
                # resolve 已核实输入身份。准备期间复用 DuckDB 实例，避免每个表重载数据库目录。
                database = dataset.manifest["database"]
                if database not in pit_databases:
                    setup.enter_context(duckdb.connect(database, read_only=True))
                    pit_databases.add(database)
                projection = dataset.estimate.json_array_projection
                pit_sources[binding.binding_id] = dict(database=database,
                    source_projection=projection.to_dict() if projection else None,
                    plan=dataset.plan.to_dict(), columns=binding.columns, daily_time=binding.daily_time,
                    memory_bytes=min(memory // 8, dataset.estimate.provider_duckdb_memory_bytes),
                    threads=min(context.effective_resource_budget.cpu_slots, dataset.estimate.execution_cpu_slots))
                continue
            info["direct"] = direct_source(source_root, reference, binding, plan, memory,
                                           market=market if binding.input_port == "bars" else None,
                                           temp_bytes=context.effective_resource_budget.temp_bytes)
        bindings.append(info)
    if pit_sources:
        strategy["pit_sources"] = pit_sources
    return _native_manifests(strategy, bindings, events, calendar_windows)


def _native_manifests(strategy, bindings, events, calendar_windows):
    direct_bindings = [item for item in bindings if "direct" in item]
    lean_bindings = []
    for item in bindings:
        binding = {name: value for name, value in item.items() if name not in ("direct", "trade_symbols")}
        if binding["symbols"] == strategy["symbols"]:
            binding.pop("symbols")
        lean_bindings.append(binding)
    spot_windows = strategy.get("execution_windows")
    if spot_windows:
        spot = {"execution_windows": spot_windows}
        spot_points = set(_calendar_events(spot))
        events = [point for point in events if point not in spot_points]
        covered = {(item["start_at"], item["end_at"]) for item in spot_windows.values()}
    else:
        covered = set()
    windows = {(item["start_at"], item["end_at"]): item for item in calendar_windows}
    # 不重复保存已由现货开始、结束及会话边界确定的时钟。
    native_manifest = {
        "bindings": lean_bindings,
        "spot_execution_clock": bool(spot_windows),
        "spot_minute_clock": bool(spot_windows and strategy.get("frequency") == "1m"),
        "calendar_events": events,
        "calendar_windows": sorted((item for key, item in windows.items() if key not in covered),
                                   key=lambda item: _aware(item["start_at"])),
    }
    return native_manifest, direct_bindings
