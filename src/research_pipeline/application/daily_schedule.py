"""准入后读取排程所需的日期和证券键，冻结到同一份研究计划。"""
from collections import defaultdict
from dataclasses import replace
from datetime import date
from sys import getsizeof

from research_pipeline.data_plane.query_ir import DateRangeV1, QueryPurpose
from research_pipeline.platform.daily_schedule import freeze_schedule, schedule_requests


def bind_daily_schedules(recipe, admitted, budgets, estimates, *, archive, args):
    from research_pipeline.packages.project_causal_admission import _validate_daily_source
    from .research_plan import _database_fingerprint, _source_database_map
    nodes = {}
    keys_by_request = {}
    databases = {}
    for node in recipe.nodes:
        plan = node.parameters.get("causal_plan", {})
        if "daily_schedule" not in plan:
            continue
        if set(plan) & {"work_items", "entity_pools", "source_files"}:
            raise ValueError("研究包日频排程不能自报准入结果")
        rule = plan["daily_schedule"]
        requests = schedule_requests(rule)
        for key in requests:
            source = admitted[key]
            scope = source.query.time_range
            if (not isinstance(scope, DateRangeV1) or scope.start.isoformat() > rule["first_observation"]
                    or scope.end.isoformat() < rule["last_observation"]):
                raise ValueError("排程来源请求须覆盖完整观察范围")
            _validate_daily_source({"daily_time": plan["sources"][0]["daily_time"],
                                    "observation_column": source.event_time_field,
                                    "available_column": source.event_time_field}, source)
        for key in sorted(requests - keys_by_request.keys()):
            source = admitted[key]
            if (source.query.purpose != QueryPurpose.FEATURE
                    or source.temporal_selection.requires_consumer_binding or source.query.limit is not None
                    or not source.instrument_field or source.factor_publication is not None):
                raise ValueError("排程键须来自完整的下一交易日开盘可见日频证券请求")
            columns = [source.event_time_field, source.instrument_field]
            if not set(columns) <= set(source.query.field_ids):
                raise ValueError("日频排程的日期与证券列须在公开投影中")
            if archive is not None:
                dataset = archive[key]
                if dataset is None:
                    raise ValueError("日频排程须使用日频数据集归档")
                batches = dataset.iter_batches(columns=columns,
                                               batch_size=min(source.query.budget.batch_size, dataset.max_batch_rows))
                keys_by_request[key] = _collect_keys(batches, budgets[key].memory_bytes)
            else:
                from research_pipeline.data_plane.direct_sources import no_spill_budget, open_projected_stream
                paths = _source_database_map(data_db=args.data_db, source_db=getattr(args, "source_db", ()))
                path = paths[source.source_profile]
                before = _database_fingerprint(path)
                budget, estimate = no_spill_budget(budgets[key], estimates[key])
                with open_projected_stream(path, source, budget, estimate, columns=columns, filters=()) as batches:
                    keys_by_request[key] = _collect_keys(batches, budgets[key].memory_bytes)
                if _database_fingerprint(path) != before:
                    raise ValueError("日频排程准备期间来源发生变化")
                facts = {"database": str(path), **before}
                if databases.setdefault(source.source_profile, facts) != facts:
                    raise ValueError("日频排程准备期间来源发生变化")
        frozen = freeze_schedule(plan, keys_by_request)
        if archive is None and requests:
            frozen["source_files"] = {profile: databases[profile] for profile in sorted(
                {admitted[key].source_profile for key in requests})}
        nodes[node.node_id] = replace(node, parameters={**node.parameters, "causal_plan": frozen})
    return nodes


def _collect_keys(batches, memory_bytes):
    keys = defaultdict(set)
    instruments = {}
    for batch in batches:
        for day, code in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=True):
            if type(day) is not date or not isinstance(code, str) or not code:
                raise ValueError("排程键须为非空日期和证券代码")
            keys[day].add(instruments.setdefault(code, code))
        used = (getsizeof(keys) + getsizeof(instruments) + sum(map(getsizeof, instruments))
                + sum(getsizeof(day) + getsizeof(group) for day, group in keys.items()))
        if used + batch.nbytes > memory_bytes:
            raise ValueError("日频排程证券日期键超过来源节点内存预算")
    return {day.isoformat(): group for day, group in keys.items()}


def verify_schedule_sources(recipe, args):
    from .research_plan import _database_fingerprint, _source_database_map
    sources = {}
    for node in recipe.nodes:
        for profile, facts in node.parameters.get("causal_plan", {}).get("source_files", {}).items():
            if sources.setdefault(profile, facts) != facts:
                raise ValueError("日频排程的同一来源版本不一致")
    if sources:
        paths = _source_database_map(data_db=args.data_db, source_db=getattr(args, "source_db", ()))
        for profile, facts in sources.items():
            path = paths.get(profile)
            if path is None or {"database": str(path), **_database_fingerprint(path)} != facts:
                raise ValueError("日频排程冻结来源已改变，须重新准入")
