"""按证券读取列式逐日规则，并复用正式请求覆盖语义。"""
from __future__ import annotations

import json
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime
from functools import lru_cache
from itertools import groupby
from types import SimpleNamespace
from typing import NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from research_pipeline.platform.arrow_rows import record_batch_rows
from research_pipeline.platform.minute_reference import require_published_minute_capability_binding

from .minute_request_scope import _require_rule_coverage
from .minute_rule_snapshots import (
    MINUTE_RULE_CAPABILITY_CONSUMER, MINUTE_RULE_VERSION, PROJECT_RULE_BUNDLE_VERSION,
    MinuteRuleResolver, MinuteRuleSnapshot, MinuteRuleSnapshotError, MinuteRuleSource,
    _date, _datetime,
)
from .minute_scope_tables import (
    DAILY_REFERENCE_FILE, DAILY_REFERENCE_SCHEMA, _DAILY_FIELDS, _DAILY_TIMELINE,
    _DAILY_RULE_IDS, _binding, _directory, _require_binding, _rows,
)
from .session_calendar import PROJECT_SESSION_BUNDLE_VERSION

_ASSET_PREFIX = {"cn_stock": "cn_stock", "cn_etf": "cn_fund"}
_RULE_KINDS = {section: rule_id.split(".")[-2] for rule_id, section in _DAILY_RULE_IDS.items()}


def normalize_daily_batch(batch):
    """补齐固定物理列，已有显式时间、来源和修订保持原值。"""
    if isinstance(batch, pa.RecordBatch):
        batch = pa.Table.from_batches([batch])
    if not isinstance(batch, pa.Table) or set(batch.column_names) - set(DAILY_REFERENCE_SCHEMA.names):
        raise MinuteRuleSnapshotError("逐日参考批次须使用声明的 Arrow 列")
    fields = {name: batch[name] for name in batch.column_names}
    if not {"instrument_id", "asset_class", "trading_date"} <= fields.keys():
        raise MinuteRuleSnapshotError("逐日参考批次缺少证券、资产类型或交易日")
    for section, kind in _RULE_KINDS.items():
        revision = fields.get(f"{section}_revision")
        if revision is None:
            continue
        present = pc.is_valid(revision)
        stock = pc.equal(fields["asset_class"], "cn_stock")
        rule_id = pc.if_else(stock, f"rule.cn_stock.{kind}.v1", f"rule.cn_fund.{kind}.v1")
        for field, value in (("rule_id", rule_id), ("status", "supported"), ("contract_version", MINUTE_RULE_VERSION)):
            name = f"{section}_{field}"
            if name not in fields:
                fields[name] = pc.if_else(present, value, pa.scalar(None, type=pa.string()))
    try:
        return pa.Table.from_arrays([
            fields[field.name] if field.name in fields else pa.nulls(batch.num_rows, field.type)
            for field in DAILY_REFERENCE_SCHEMA
        ], schema=DAILY_REFERENCE_SCHEMA)
    except pa.ArrowException as exc:
        raise MinuteRuleSnapshotError("逐日参考批次列类型不符") from exc


class _DailyRule(NamedTuple):
    instrument_id: str
    rule_id: str
    effective_from: date
    effective_to: date
    available_at: datetime | None
    revision: int
    status: str
    source_ids: tuple
    unsupported_reason: str | None
    parameters: tuple


def _daily_batches(path):
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(DAILY_REFERENCE_SCHEMA, check_metadata=False):
            raise MinuteRuleSnapshotError("逐日参考表列或类型不符")

        yield from parquet.iter_batches(batch_size=32768)


def group_daily_batches(batches):
    def fragments():
        for batch in batches:
            if not batch.num_rows:
                continue
            codes = pc.dictionary_encode(batch["instrument_id"])
            if codes.null_count:
                raise MinuteRuleSnapshotError("逐日参考缺少证券")
            indices = codes.indices.to_numpy()
            boundaries = np.r_[0, np.flatnonzero(indices[1:] != indices[:-1]) + 1, batch.num_rows]
            dictionary = codes.dictionary.to_pylist()
            for first, last in zip(boundaries, boundaries[1:]):
                yield dictionary[indices[first]], batch.slice(int(first), int(last - first))

    for instrument, fragments_by_code in groupby(fragments(), key=lambda item: item[0]):
        yield instrument, pa.Table.from_batches([batch for _, batch in fragments_by_code])


def _daily_rows(table):
    columns = ["instrument_id", "asset_class", "trading_date"]
    for section, fields in _DAILY_FIELDS.items():
        columns.extend(f"{section}_{field.name}" for field in _DAILY_TIMELINE)
        columns.extend(f"{section}_{name}" for name in fields)
    for batch in table.select(columns).to_batches():
        yield from record_batch_rows(batch, as_dict=False)


def _daily_rules(rows, instrument, templates, valid_sources, parse_date, parse_time):
    previous_day = None
    prefix = _ASSET_PREFIX[instrument.asset_class]
    for code, asset_class, raw_day, *values in rows:
        day = parse_date(raw_day, "trading_date")
        if asset_class != instrument.asset_class or (previous_day is not None and day < previous_day):
            raise MinuteRuleSnapshotError(f"逐日规则资产身份或业务日期顺序无效：{code} {raw_day}")
        previous_day = day
        offset = 0
        active = False
        for section, fields in _DAILY_FIELDS.items():
            stop = offset + len(_DAILY_TIMELINE)
            rule_id, revision, status, raw_at, sources, parameter_index, reason, version = values[offset:stop]
            dynamics = values[stop:stop + len(fields)]
            offset = stop + len(fields)
            if revision is None:
                if any(item is not None for item in (*values[stop - len(_DAILY_TIMELINE):stop], *dynamics)):
                    raise MinuteRuleSnapshotError(f"逐日规则无修订但存在业务值：{code} {raw_day} {section}")
                continue
            active = True
            expected = f"rule.{prefix}.{_RULE_KINDS[section]}.v1"
            if (rule_id != expected or revision < 1 or version != MINUTE_RULE_VERSION
                    or sources is None or not valid_sources(tuple(sources))
                    or type(parameter_index) is not int or not 0 <= parameter_index < len(templates)):
                raise MinuteRuleSnapshotError(f"逐日规则身份、来源或参数索引无效：{code} {raw_day} {section}")
            template = templates[parameter_index]
            dynamic = {key: value for key, value in zip(fields, dynamics) if value is not None}
            if template.keys() & dynamic.keys():
                raise MinuteRuleSnapshotError(f"逐日行情参数与制度模板字段重复：{code} {raw_day} {section}")
            if status == "supported" and raw_at is not None and sources and reason is None:
                at = parse_time(raw_at, "available_at")
            elif status == "unsupported" and raw_at is None and not template and not dynamic and reason:
                at = None
            else:
                raise MinuteRuleSnapshotError(f"逐日规则状态或可见时点无效：{code} {raw_day} {section}")
            yield _DailyRule(code, rule_id, day, day, at, revision, status, tuple(sources), reason,
                             tuple(template.items()) + tuple(dynamic.items()))
        if not active:
            raise MinuteRuleSnapshotError(f"逐日参考行缺少规则：{code} {raw_day}")


def _read_low_frequency_rules(directory, parameter_sets):
    for row in _rows(directory, "rules.parquet"):
        index = row.pop("parameter_index")
        if type(index) is not int or not 0 <= index < len(parameter_sets):
            raise MinuteRuleSnapshotError("低频规则参数索引无效")
        yield MinuteRuleSnapshot.from_dict(row | {"parameters": parameter_sets[index]})


def group_low_frequency_rules(rules, metadata, source_map):
    low_frequency = defaultdict(list)
    last = None
    for rule in rules:
        key = (rule.instrument_id, rule.rule_id, rule.revision)
        instrument = metadata.get(rule.instrument_id)
        if (last is not None and key <= last or instrument is None
                or rule.asset_class != instrument.asset_class or not set(rule.source_ids) <= source_map.keys()):
            raise MinuteRuleSnapshotError(f"低频规则身份、版本顺序或来源无效：{key}")
        last = key
        low_frequency[rule.instrument_id].append(rule)
    return low_frequency


def validate_columnar_rules(scope, coverage_requests, *, sources=None, rules=None,
                            daily_batches=None, templates=None, live_rules=None, live_sources=None):
    """验证全部版本流，并按请求时点检查规则参数与可见性。"""
    directory = _directory(scope.root, scope.reference)
    calendar, instruments = scope._calendar_parts
    binding = dict(_binding(scope.rule_metadata))
    if (scope.rule_metadata["contract_version"] != PROJECT_RULE_BUNDLE_VERSION
            or calendar.contract_version != PROJECT_SESSION_BUNDLE_VERSION
            or binding.get("consumer_id") != MINUTE_RULE_CAPABILITY_CONSUMER):
        raise MinuteRuleSnapshotError("列式规则须绑定完整项目日历和规则能力")
    try:
        manifest = require_published_minute_capability_binding(
            consumer_id=binding["consumer_id"], manifest_hash=binding["minute_capability_manifest_hash"],
            contract_version=binding["contract_version"], binding_hash=binding["binding_hash"],
        )
    except ValueError as exc:
        raise MinuteRuleSnapshotError("列式规则未绑定已发布分钟能力 manifest") from exc
    sources = (tuple(MinuteRuleSource.from_dict(row) for row in _rows(directory, "sources.parquet"))
               if sources is None else sources)
    def checked_sources(items):
        _require_binding(calendar, SimpleNamespace(capability_binding=binding, sources=items))
        source_map = {source.source_id: source for source in items}
        if (len(source_map) != len(items) or tuple(source_map) != tuple(sorted(source_map))
                or any(source.source_kind == "platform_capability"
                       and not source.locator.endswith("#" + binding["minute_capability_manifest_hash"])
                       for source in items)):
            raise MinuteRuleSnapshotError("列式规则来源重复、无序或能力身份不一致")
        return source_map

    source_map = checked_sources(sources)
    metadata = {item.instrument_id: item for item in instruments}
    if len(metadata) != len(instruments) or tuple(metadata) != tuple(sorted(metadata)):
        raise MinuteRuleSnapshotError("列式规则证券须排序且唯一")
    requests = defaultdict(list)
    for request in coverage_requests:
        for code in request["instruments"]:
            if code not in metadata:
                raise MinuteRuleSnapshotError(f"规则证券缺失: {code}")
            requests[code].append(request | {"instruments": (code,)})
    policies = defaultdict(list)
    for policy in calendar.policies:
        policies[policy.instrument.instrument_id].append(policy)
    required = {}
    for item in manifest.coverages:
        required.setdefault(item.instrument.asset_class, set(item.required_rule_ids))
    required["cn_stock"].discard("rule.cn_stock.adjustment_factor_snapshot.v1")
    if templates is None:
        templates = [json.loads(row["parameters"]) for row in _rows(directory, "parameters.parquet")]
    if any(not isinstance(item, dict) for item in templates):
        raise MinuteRuleSnapshotError("列式规则参数模板必须是对象")
    if live_rules is None:
        if rules is None:
            rules = _read_low_frequency_rules(directory, templates)
        low_frequency = group_low_frequency_rules(rules, metadata, source_map)
    @lru_cache(maxsize=4096)
    def valid_sources(values):
        return values == tuple(sorted(set(values))) and all(key in source_map for key in values)

    parse_date = lru_cache(maxsize=8192)(_date)
    parse_time = lru_cache(maxsize=8192)(_datetime)
    from .minute_scope_columnar_coverage import check_unique_daily_coverage, daily_batch_evidence

    source_ranks = pa.array(list(source_map), type=pa.string())
    day_tables = {}

    def verified_batches():
        nonlocal source_map, source_ranks, sources
        batches = _daily_batches(directory / DAILY_REFERENCE_FILE) if daily_batches is None else daily_batches
        for chunk in batches:
            raw = pa.Table.from_batches([chunk]) if isinstance(chunk, pa.RecordBatch) else chunk
            if live_sources is not None and len(live_sources) != len(source_map):
                sources = tuple(sorted(live_sources, key=lambda item: item.source_id))
                source_map = checked_sources(sources)
                source_ranks = pa.array(list(source_map), type=pa.string())
                valid_sources.cache_clear()
            for batch in raw.to_batches(max_chunksize=32768):
                if batch.num_rows:
                    yield daily_batch_evidence(batch, templates, source_ranks)

    groups = group_daily_batches(verified_batches())
    with closing(groups) as daily_groups:
        group = next(daily_groups, None)
        for code, instrument in metadata.items():
            if instrument.asset_class not in _ASSET_PREFIX:
                raise MinuteRuleSnapshotError("列式逐日参考规则仅用于股票和ETF")
            if group is not None and group[0] < code:
                raise MinuteRuleSnapshotError(f"逐日参考证券未知或顺序无效：{group[0]}")
            if live_sources is not None and len(live_sources) != len(source_map):
                sources = tuple(sorted(live_sources, key=lambda item: item.source_id))
                source_map = checked_sources(sources)
                source_ranks = pa.array(list(source_map), type=pa.string())
                valid_sources.cache_clear()
            combined = live_rules(code) if live_rules is not None else low_frequency.pop(code, [])
            if group is not None and group[0] == code:
                daily = group[1]
                group = next(daily_groups, None)
                if check_unique_daily_coverage(daily, instrument, combined, templates,
                                               required[instrument.asset_class], policies[code], requests[code], day_tables):
                    continue
                combined.extend(_daily_rules(_daily_rows(daily), instrument, templates,
                                             valid_sources, parse_date, parse_time))
            versions = defaultdict(list)
            for rule in combined:
                versions[rule.rule_id].append(rule.revision)
            missing = required[instrument.asset_class] - versions.keys()
            if missing:
                raise MinuteRuleSnapshotError(f"规则覆盖缺失: {code} {sorted(missing)}")
            for rule_id, revisions in versions.items():
                if sorted(revisions) != list(range(1, len(revisions) + 1)):
                    raise MinuteRuleSnapshotError(f"规则版本流不完整: {code} {rule_id}")
            if requests[code]:
                resolver = MinuteRuleResolver(SimpleNamespace(sources=sources, rules=combined))
                code_calendar = SimpleNamespace(policies=policies[code], contract_version=calendar.contract_version)
                for request in requests[code]:
                    _require_rule_coverage(resolver, {code: instrument}, code_calendar, **request)
        if group is not None:
            raise MinuteRuleSnapshotError(f"逐日参考证券未知或顺序无效：{group[0]}")
