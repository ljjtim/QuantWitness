"""对普通单版本交易日批量验证现货规则，复杂修订交由时点解析器。"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .cash_price_limits import EXPLICIT_PRICE_LIMIT_FIELDS
from .minute_request_scope import _STOCK_REQUIRED_FIELDS
from .minute_rule_snapshots import MINUTE_RULE_VERSION, MinuteRuleSnapshotError, _datetime, _hash
from .minute_scope_tables import _DAILY_FIELDS
from .time import CN_MARKET_TIMEZONE

_ZONE = ZoneInfo(CN_MARKET_TIMEZONE)
_EPOCH_ORDINAL = 719163
_MICROS = 1_000_000
_PRICE_DENOMINATOR = 1_000_000
_FIXED_OFFSET_START = date(1992, 1, 1).toordinal()
_DAY_MICROS = 86400 * _MICROS


def _all(column, value):
    return not column.null_count and bool(pc.all(pc.equal(column, value)).as_py())


def _matches(column, value):
    return pc.fill_null(pc.equal(column, value), False).to_numpy(zero_copy_only=False)


def _bits(column):
    return pc.is_valid(column).to_numpy(zero_copy_only=False)


def _ints(column, missing=-1):
    source = column.chunk(0) if column.num_chunks == 1 else column.combine_chunks()
    if not source.null_count:
        return source.to_numpy()
    data = np.frombuffer(source.buffers()[1], dtype=np.int64, count=len(source), offset=source.offset * 8)
    return np.where(_bits(source), data, missing)


@lru_cache(maxsize=16384)
def _canonical_seconds(value):
    return int(_datetime(value, "available_at").timestamp())


def _parsed_seconds(column, *, allow_null=False):
    if column.null_count and not allow_null:
        return None
    encoded = pc.dictionary_encode(column.combine_chunks())
    try:
        seconds = pa.array([_canonical_seconds(value) for value in encoded.dictionary.to_pylist()],
                           type=pa.int64())
    except MinuteRuleSnapshotError:
        return None
    return pc.take(seconds, encoded.indices).fill_null(np.iinfo(np.int64).max // _MICROS).to_numpy(zero_copy_only=False)

@lru_cache(maxsize=8192)
def _day_bounds(day, segments):
    resolved = [item.build(day) for item in segments]
    return (
        min(item.starts_at for item in resolved),
        min(item.starts_at for item in resolved if item.bar_eligible),
        max(item.ends_at for item in resolved if item.bar_eligible),
    )


def _policy_slices(policy, day_tables):
    dates_key = id(policy.trading_dates)
    numbers = day_tables.get(dates_key)
    if numbers is None:
        numbers = np.fromiter((day.toordinal() for day in policy.trading_dates), dtype=np.int32)
        day_tables[dates_key] = numbers
    return numbers


def _request_points(days, instrument, policies, requests, day_tables):
    if not requests:
        return []
    policies = sorted(policies, key=lambda row: row.effective_from)
    if (not policies or any(item.instrument != instrument or item.available_at is None
                            or any(segment.start_day_offset or segment.end_day_offset for segment in item.segments)
                            for item in policies)
            or any(left.effective_to >= right.effective_from for left, right in zip(policies, policies[1:]))):
        return None
    result = []
    for request in requests:
        if request.get("stock_raw", False) and instrument.asset_class != "cn_stock":
            return None
        start, end = request["start_at"], request["end_at"]
        first_day = start.astimezone(_ZONE).date().toordinal()
        last_day = (end - timedelta(microseconds=1)).astimezone(_ZONE).date().toordinal()
        start_us, end_us = (round(point.timestamp() * _MICROS) for point in (start, end))
        pointer = first_day
        for policy in policies:
            first = max(first_day, policy.effective_from.toordinal())
            last = min(last_day, policy.effective_to.toordinal())
            if first > last:
                continue
            if pointer != first:
                return None
            pointer = last + 1
            # 原合同在自然日期间检查政策可见性，包括无会话日期。
            first_open, first_bar_start, first_bar_end = _day_bounds(date.fromordinal(first), policy.segments)
            if policy.available_at > first_open or first < _FIXED_OFFSET_START:
                # 历史夏令时的跨度由逐日时点解析器处理。
                return None
            numbers = _policy_slices(policy, day_tables)
            lo, hi = np.searchsorted(numbers, (first, last + 1))
            days_since_first = (numbers[lo:hi].astype(np.int64) - first) * _DAY_MICROS
            starts = days_since_first + round(first_bar_start.timestamp() * _MICROS)
            ends = days_since_first + round(first_bar_end.timestamp() * _MICROS)
            active = (starts < end_us) & (ends >= start_us)
            needed = numbers[lo:hi][active]
            rows = np.searchsorted(days, needed)
            if np.any(rows == len(days)) or not np.array_equal(days[rows], needed):
                return None
            points = np.maximum(starts[active], start_us)
            result.append((rows, points, policy.policy_id, request.get("stock_raw", False)))
        if pointer <= last_day:
            return None
    return result


def _low_frequency_coverage(rules, daily_ids, required, days, requests):
    grouped = defaultdict(list)
    for rule in rules:
        grouped[rule.rule_id].append(rule)
    if daily_ids & grouped.keys() or required - daily_ids - grouped.keys():
        return False
    for rule_id, revisions in grouped.items():
        if any(rule.revision != index for index, rule in enumerate(revisions, 1)):
            return False
        by_date = sorted(revisions, key=lambda rule: rule.effective_from)
        if any(left.effective_to >= right.effective_from for left, right in zip(by_date, by_date[1:])):
            return False
        kind = rule_id.split(".")[-2]
        expected = _STOCK_REQUIRED_FIELDS.get(kind, set())
        for rows, points, policy_id, _ in requests:
            if not len(rows):
                continue
            requested_days = days[rows]
            starts = [rule.effective_from.toordinal() for rule in by_date]
            ends = [rule.effective_to.toordinal() for rule in by_date]
            selected = np.searchsorted(starts, requested_days, side="right") - 1
            if np.any(selected < 0) or np.any(np.asarray(ends)[selected] < requested_days):
                return False
            for index in np.unique(selected):
                rule = by_date[index]
                if (rule.status != "supported" or rule.available_at is None
                        or round(rule.available_at.timestamp() * _MICROS) > points[selected == index].min()):
                    return False
                parameters = dict(rule.parameters)
                if expected - parameters.keys() or (kind == "session" and parameters.get("session_policy_id") != policy_id):
                    return False
                if kind == "adjustment_factor_snapshot":
                    for key in expected:
                        _hash(parameters.get(key), f"{rule.instrument_id} {rule_id} {key}")
    return True


def _templates_valid(indices, templates, table, section, required, rows):
    active_names = set()
    fields = _DAILY_FIELDS[section]
    distinct = np.unique(indices)
    for index in distinct:
        template = templates[index]
        requested = rows if len(distinct) == 1 else rows[indices[rows] == index]
        if template.keys() & fields.keys():
            return None
        active_names.update(template)
        if len(requested):
            for key in required - template.keys():
                if key not in fields or (table[f"{section}_{key}"].null_count and not np.all(_bits(table[f"{section}_{key}"])[requested])):
                    return None
            if section == "prices":
                bounded = table["__price_bounded"].to_numpy(zero_copy_only=False)[requested]
                if np.any(bounded) and template.get("price_limit_rounding") != "half_up_to_quote_unit":
                    return None
                if template.keys() & (EXPLICIT_PRICE_LIMIT_FIELDS | {
                    "reference_previous_close_units", "price_limit_ratio_ppm", "price_limit_mode",
                }):
                    return None
    return active_names | {key for key in fields if table[f"{section}_{key}"].null_count < table.num_rows}


def _price_batch_evidence(table):
    names = [f"prices_{key}" for key in _DAILY_FIELDS["prices"]]
    names.extend(("suspension_paused", "lifecycle_trading_status", "lifecycle_stock_board", "lifecycle_listing_phase"))
    price = table.select(names)
    accepted = np.ones(price.num_rows, dtype=bool)
    mode = price["prices_price_limit_mode"]
    bounded = _matches(mode, "bounded")
    suspended = _matches(mode, "suspended")
    unbounded = _matches(mode, "unbounded")
    scale = _ints(price["prices_price_scale"])
    accepted &= (bounded | suspended | unbounded) & (scale >= 0) & (scale <= 8)
    high = _ints(price["prices_high_limit_units"])
    low = _ints(price["prices_low_limit_units"])
    ratio = _ints(price["prices_price_limit_ratio_ppm"])
    has_high = _bits(price["prices_high_limit_units"])
    has_low = _bits(price["prices_low_limit_units"])
    has_ratio = _bits(price["prices_price_limit_ratio_ppm"])
    accepted &= ~(unbounded & (has_high | has_low | has_ratio))
    limited = ~unbounded
    accepted &= ~(limited & ((low > high) | (low < bounded.astype(np.int64))))
    previous = _ints(price["prices_reference_previous_close_units"])
    accepted &= ~(suspended & ((previous < 1) | ~_matches(price["suspension_paused"], True)
                              | ~_matches(price["lifecycle_trading_status"], "suspended")))
    explicit_fields = [_bits(price[f"prices_{key}"]) for key in EXPLICIT_PRICE_LIMIT_FIELDS]
    explicit = np.logical_or.reduce(explicit_fields)
    complete = np.logical_and.reduce(explicit_fields)
    accepted &= ~(explicit & (~bounded | ~complete | _bits(price["prices_reference_previous_close_units"]) | has_ratio))
    kind = price["prices_reference_price_kind"]
    issue = _matches(kind, "issue_price")
    previous_kind = _matches(kind, "previous_close")
    accepted &= ~(explicit & ~(issue | previous_kind))
    if np.any(issue):
        boards = pa.array(["sh_main", "sz_main", "chinext", "star", "bse"])
        eligible = pc.is_in(price["lifecycle_stock_board"], value_set=boards).to_numpy(zero_copy_only=False)
        accepted &= ~(issue & (~_matches(table["asset_class"], "cn_stock") | ~eligible
                               | ~_matches(price["lifecycle_listing_phase"], "ipo")))
    ref = np.where(explicit, _ints(price["prices_reference_price_units"]), previous)[bounded]
    up = np.where(explicit, _ints(price["prices_price_limit_up_ratio_ppm"]), ratio)[bounded]
    down = np.where(explicit, _ints(price["prices_price_limit_down_ratio_ppm"]), ratio)[bounded]
    denominator = _PRICE_DENOMINATOR
    valid_limits = (ref >= 1) & (up >= 1) & (down >= 1) & (down < denominator)
    valid_limits &= up <= (np.iinfo(np.int64).max - denominator // 2) // np.maximum(ref, 1) - denominator
    safe_ref = np.where(valid_limits, ref, 1)
    safe_up = np.where(valid_limits, up, 1)
    safe_down = np.where(valid_limits, down, 1)
    expected_high = (safe_ref * (denominator + safe_up) + denominator // 2) // denominator
    expected_low = (safe_ref * (denominator - safe_down) + denominator // 2) // denominator
    accepted[bounded] &= valid_limits & (high[bounded] == expected_high) & (low[bounded] == expected_low)
    seconds = _parsed_seconds(price["prices_reference_price_available_at"], allow_null=True)
    if seconds is None:
        accepted &= unbounded
        seconds = np.zeros(price.num_rows, dtype=np.int64)
    return pa.array(accepted), pa.array(seconds), pa.array(limited), pa.array(bounded)


def daily_batch_evidence(batch, templates, source_ranks):
    """在同一批中校验共享字段、规则来源、修订时间和模板索引。"""
    day = pc.dictionary_encode(batch["trading_date"])
    try:
        dates = pc.cast(day.dictionary, pa.date32())
        canonical = pc.equal(pc.cast(dates, pa.string()), day.dictionary)
        ordinal = pc.if_else(canonical, pc.add(pc.cast(dates, pa.int32()), _EPOCH_ORDINAL),
                             pa.scalar(None, type=pa.int32()))
        day_numbers = pc.take(ordinal, day.indices)
    except pa.ArrowException:
        # 非规范日期交由逐条解析器定位。
        day_numbers = pa.nulls(batch.num_rows, pa.int32())
    table = pa.Table.from_batches([batch])
    stock = pc.equal(batch["asset_class"], "cn_stock")
    valid_asset = pc.or_(stock, pc.equal(batch["asset_class"], "cn_etf"))
    output = [day_numbers]
    names = ["__ordinal"]
    times = []
    for section, kind in _KINDS.items():
        rule_id = pc.if_else(stock, f"rule.cn_stock.{kind}.v1", f"rule.cn_fund.{kind}.v1")
        sources = batch[f"{section}_source_ids"]
        counts = pc.list_value_length(sources)
        ranks = pc.index_in(pc.list_flatten(sources), value_set=source_ranks)
        parents = pc.list_parent_indices(sources).to_numpy(zero_copy_only=False)
        ids = ranks.fill_null(-1).to_numpy(zero_copy_only=False)
        bad_sources = np.zeros(batch.num_rows, dtype=bool)
        bad_sources[parents[ids < 0]] = True
        duplicate = (parents[1:] == parents[:-1]) & (ids[1:] <= ids[:-1])
        bad_sources[parents[1:][duplicate]] = True
        indices = batch[f"{section}_parameter_index"]
        checks = (
            valid_asset, pc.equal(batch[f"{section}_rule_id"], rule_id),
            pc.equal(batch[f"{section}_status"], "supported"),
            pc.equal(batch[f"{section}_contract_version"], MINUTE_RULE_VERSION),
            pc.is_null(batch[f"{section}_unsupported_reason"]), pc.greater(counts, 0),
            pc.greater_equal(indices, 0), pc.less(indices, len(templates)),
        )
        valid = ~bad_sources
        for check in checks:
            valid &= check.fill_null(False).to_numpy(zero_copy_only=False)
        stamps = batch[f"{section}_available_at"]
        for previous, seconds in times:
            if stamps.equals(previous):
                break
        else:
            seconds = _parsed_seconds(table[f"{section}_available_at"])
            times.append((stamps, seconds))
        output.extend((pa.array(valid), pa.array(seconds) if seconds is not None
                       else pa.nulls(batch.num_rows, pa.int64())))
        names.extend((f"__{section}_valid", f"__{section}_seconds"))
    output.extend(_price_batch_evidence(table))
    names.extend(("__price_valid", "__reference_seconds", "__price_limited", "__price_bounded"))
    return pa.RecordBatch.from_arrays([*batch.columns, *output], names=[*batch.schema.names, *names])


_KINDS = {"lifecycle": "instrument_lifecycle", "prices": "price_limit", "suspension": "suspension"}


def check_unique_daily_coverage(table, instrument, rules, templates, required, policies, requests, day_tables):
    """对整批已验证的列逐证券核对连续修订及请求范围。"""
    if not _all(table["asset_class"], instrument.asset_class) or table["__ordinal"].null_count:
        return False
    days = table["__ordinal"].to_numpy(zero_copy_only=False)
    if np.any(days[1:] <= days[:-1]):
        return False
    request_points = _request_points(days, instrument, policies, requests, day_tables)
    if request_points is None:
        return False
    prefix = "cn_stock" if instrument.asset_class == "cn_stock" else "cn_fund"
    if instrument.asset_class == "cn_stock" and any(not request.get("stock_raw", False) for request in requests):
        required = required | {"rule.cn_stock.adjustment_factor_snapshot.v1"}
    daily_ids = {f"rule.{prefix}.{kind}.v1" for kind in _KINDS.values()}
    if not _low_frequency_coverage(rules, daily_ids, required, days, request_points):
        return False
    revisions = np.arange(1, table.num_rows + 1)
    rows = np.unique(np.concatenate([item[0] for item in request_points])) if request_points else np.array([], dtype=int)
    used_names = set()
    for section, kind in _KINDS.items():
        seconds_column = table[f"__{section}_seconds"]
        if (not np.array_equal(_ints(table[f"{section}_revision"]), revisions)
                or not pc.all(table[f"__{section}_valid"]).as_py() or seconds_column.null_count):
            return False
        seconds = seconds_column.to_numpy(zero_copy_only=False)
        if any(np.any(seconds[positions] * _MICROS > points) for positions, points, _, _ in request_points):
            return False
        names = _templates_valid(_ints(table[f"{section}_parameter_index"]), templates, table,
                                 section, _STOCK_REQUIRED_FIELDS[kind], rows)
        if names is None or names & used_names:
            return False
        used_names.update(names)
    if "etf_category" in used_names:
        return False
    accepted = table["__price_valid"].to_numpy(zero_copy_only=False)
    limited = table["__price_limited"].to_numpy(zero_copy_only=False)
    seconds = table["__reference_seconds"].to_numpy(zero_copy_only=False)
    return all(np.all(accepted[positions] & (~limited[positions] | (seconds[positions] * _MICROS <= points)))
               for positions, points, _, _ in request_points)
