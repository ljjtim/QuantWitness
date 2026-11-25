"""从正式来源交接LEAN行情及规则，保存策略配置与运行所需记录。"""
from __future__ import annotations

from research_pipeline.platform.asset_taxonomy import MINUTE_TARGET_ASSET_CLASSES
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Mapping
from contextlib import ExitStack
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
import csv
import json
from pathlib import Path
import shutil
from time import perf_counter
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.minute_rule_snapshots import MinuteRuleResolver, MinuteRuleSnapshot, MinuteRuleSource
from research_pipeline.domain.strategy_declaration import (
    parse_stock_corporate_actions,
    parse_strategy_declaration,
    project_strategy_worker_parameters,
)
from research_pipeline.extensions.project_bundle import verify_project_operator_bundle
from .adapters.common import _input_external_payload
from .adapters.minute_io import _minute_scope_from_payload
from .adapters.minute_strategy_pit import strategy_snapshot_context


_TZ = ZoneInfo("Asia/Shanghai")


def _json_time(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"LEAN输入不能序列化{type(value).__name__}")


def _write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, default=_json_time, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False), encoding="utf-8")


def _validate_bindings(declaration):
    bars = [item for item in declaration.input_bindings
            if item.kind == "bars" and item.input_port == "bars" and item.resolution == declaration.frequency]
    if len(bars) != 1:
        raise ValueError("原生LEAN须有唯一执行行情绑定")


def _bar_schema():
    stamp = pa.timestamp("us", tz="UTC")
    return pa.schema([
        ("instrument_id", pa.string()),
        ("bar_start", stamp), ("bar_end", stamp), ("available_time", stamp),
        *((name + "_units", pa.int64()) for name in ("open", "high", "low", "close")),
        ("volume", pa.int64()),
    ])


def _price_encoder():
    """在一次执行内复用报价整数化；有界缓存不保留行情行或证券对象。"""
    @lru_cache(maxsize=65536)
    def encode(value, scale):
        return int((Decimal(str(value)) * (10 ** scale)).to_integral_value(rounding=ROUND_HALF_UP))
    return encode


def _convert_bar_batch(batch, symbols, *, frequency="1m", price_scale=2, price_scales=None,
                       price_encoder=None, symbol_array=None):
    mask = pc.is_in(batch.column("instrument"), value_set=pa.array(symbols) if symbol_array is None else symbol_array)
    if not pc.all(mask).as_py():
        batch = batch.filter(mask)
    if not batch.num_rows:
        return None
    def col(name):
        return batch.column(name)
    required = ("bar_start", "bar_end", "completed", "quality_status", "interval_minutes")
    if frequency == "1m":
        required += ("available_time",)
    if any(col(name).null_count for name in required):
        raise ValueError("LEAN行情时间、完成标志或质量字段为空")
    valid = pc.and_(pc.equal(col("completed"), True), pc.equal(col("quality_status"), "pass"))
    interval = 1 if frequency == "1m" else 1440
    valid = pc.and_(valid, pc.equal(col("interval_minutes"), interval))
    visible = (pc.equal(col("available_time"), col("bar_end")) if frequency == "1m"
               else pc.fill_null(pc.greater(col("available_time"), col("bar_end")), True))
    valid = pc.and_(valid, visible)
    duration = pc.cast(pc.subtract(col("bar_end"), col("bar_start")), pa.int64())
    valid = pc.and_(valid, pc.greater(duration, 0) if frequency == "1d" and price_scales is not None
                   else pc.equal(duration, 60_000_000 if frequency == "1m" else 19_800_000_000))
    if not pc.all(valid).as_py():
        raise ValueError("LEAN行情周期、完成状态或可见时刻无效")
    if "adjustment_mode" in batch.schema.names:
        modes = set(pc.unique(col("adjustment_mode")).to_pylist())
        if modes - {None, "raw"}:
            raise ValueError("LEAN仅接受原始未复权分钟行情")
    stamp = pa.timestamp("us", tz="UTC")
    ends = pc.cast(col("bar_end"), stamp)
    arrays = {"instrument_id": pc.cast(col("instrument"), pa.string()),
        "bar_start": pc.cast(col("bar_start"), stamp), "bar_end": ends,
        "available_time": pc.cast(col("available_time"), stamp)}
    encode = price_encoder or _price_encoder()
    scales = {price_scale: symbols} if price_scales is None else {
        scale: [code for code in symbols if price_scales[code] == scale] for scale in set(price_scales.values())}
    for name in ("open", "high", "low", "close"):
        values = col(name)
        if values.null_count or not pc.all(pc.is_finite(values)).as_py():
            raise ValueError("LEAN行情缺少OHLC价格")
        # 按真实合约尺度half-up转换，避免二进制浮点舍位漂移。
        encoded_prices = pc.dictionary_encode(values)
        encoded_units = None
        prices = encoded_prices.dictionary.to_pylist()
        for scale, codes in scales.items():
            units = [encode(value, scale) for value in prices]
            if min(units) <= 0:
                raise ValueError("LEAN行情价格必须为正")
            mapped = pc.take(pa.array(units, type=pa.int64()), encoded_prices.indices)
            if len(scales) == 1:
                encoded_units = mapped
            else:
                selected = pc.is_in(col("instrument"), value_set=pa.array(codes))
                encoded_units = pc.if_else(selected, mapped, encoded_units)
        arrays[name + "_units"] = encoded_units
    volume = col("volume")
    if volume.null_count or not pc.all(pc.greater_equal(volume, 0)).as_py():
        raise ValueError("LEAN分钟成交量须为非负整数")
    integer_volume = pc.cast(volume, pa.int64(), safe=True)
    arrays["volume"] = integer_volume
    schema = _bar_schema()
    return pa.RecordBatch.from_arrays([arrays[name] for name in schema.names], schema=schema)


def frozen_rule_files(scope):
    from research_pipeline.domain.minute_scope_input import DAILY_REFERENCE_FILE

    directory = Path(scope.root).resolve() / "minute-scopes" / scope.reference["name"]
    daily = directory / DAILY_REFERENCE_FILE
    return {"rules_path": str(directory / "rules.parquet"),
            "parameters_path": str(directory / "parameters.parquet"),
            **({"daily_reference_path": str(daily)} if daily.is_file() else {}),
            "scope": dict(scope.reference)}


def _frozen_rule_rows(files, *, rule_ids, action_dates=None):
    """列式过滤后仅构造目标规则；参数按冻结行号读取。"""
    from research_pipeline.domain.minute_scope_input import (
        DAILY_PARAMETER_COLUMNS, daily_reference_batches, daily_reference_sections,
    )

    if not rule_ids or action_dates == {}:
        return []
    rule_keys = pa.array(rule_ids, type=pa.string())
    codes = None if action_dates is None else pa.array(list(action_dates), type=pa.string())
    if codes is not None:
        # 编码证券+日期后，在列数组上定位每行生效区间内的首个所需除权日。
        stride = 1 << 32
        targets = np.array([rank * stride + date.fromisoformat(day).toordinal()
            for rank, days in enumerate(action_dates.values()) for day in days], dtype=np.int64)
        epoch = date(1970, 1, 1).toordinal()
    selected = []
    def batches():
        with pq.ParquetFile(files["rules_path"]) as parquet:
            yield from parquet.iter_batches(batch_size=32768)
        if "daily_reference_path" in files:
            for section in daily_reference_sections(rule_ids):
                yield from daily_reference_batches(files["daily_reference_path"], section)

    for batch in batches():
        filtered = batch.filter(pc.is_in(batch["rule_id"], value_set=rule_keys))
        if codes is not None:
            ranks = pc.index_in(filtered["instrument_id"], value_set=codes)
            matching = pc.is_valid(ranks)
            filtered = filtered.filter(matching)
            base = ranks.filter(matching).to_numpy().astype(np.int64) * stride + epoch
            start = base + pc.cast(pc.cast(filtered["effective_from"], pa.date32()), pa.int32()).to_numpy()
            end = base + pc.cast(pc.cast(filtered["effective_to"], pa.date32()), pa.int32()).to_numpy()
            positions = np.minimum(np.searchsorted(targets, start), len(targets) - 1)
            candidate = targets[positions]
            filtered = filtered.filter(pa.array((candidate >= start) & (candidate <= end)))
        selected.extend(filtered.to_pylist())
    needed = {row["parameter_index"] for row in selected}
    if not needed:
        return []
    parameters = {}
    indices = sorted(needed)
    with pq.ParquetFile(files["parameters_path"]) as parquet:
        offset = 0
        for batch in parquet.iter_batches(batch_size=32768, columns=["parameters"]):
            end = offset + batch.num_rows
            for index in indices[bisect_left(indices, offset):bisect_left(indices, end)]:
                parameters[index] = json.loads(batch["parameters"][index - offset].as_py())
            offset = end
            if offset > indices[-1]:
                break
    for row in selected:
        daily = {key: row.pop(key) for key in DAILY_PARAMETER_COLUMNS if key in row}
        row["parameters"] = parameters[row.pop("parameter_index")] | {
            key: value for key, value in daily.items() if value is not None}
    return selected


def _rule_table_price_limit_instruments(rules_path, rule_id, codes):
    """返回 rules.parquet 中存在独立 price_limit 规则的证券，避免列式快路径遮蔽跨日规则或冲突。"""
    codes_array = pa.array(codes, type=pa.string())
    matched = set()
    with pq.ParquetFile(rules_path) as parquet:
        for batch in parquet.iter_batches(batch_size=32768, columns=["rule_id", "instrument_id"]):
            mask = pc.and_(
                pc.equal(batch["rule_id"], rule_id),
                pc.is_in(batch["instrument_id"], value_set=codes_array),
            )
            if pc.any(mask).as_py():
                matched.update(batch["instrument_id"].filter(mask).to_pylist())
                if len(matched) == len(codes):
                    break
    return matched


def action_reference_units(files, asset_class, keys):
    """按每个除权日 09:30 读取当时可见的前收参考价。"""
    if not keys:
        return {}
    keys = tuple(dict.fromkeys(keys))
    result = {}
    if "daily_reference_path" in files:
        prefix = "cn_fund" if asset_class == "cn_etf" else "cn_stock"
        rule_id = f"rule.{prefix}.price_limit.v1"
        codes = tuple(sorted({code for code, _ in keys}))
        shadowed = _rule_table_price_limit_instruments(files["rules_path"], rule_id, codes)
        columnar_keys = tuple(key for key in keys if key[0] not in shadowed)
        if columnar_keys:
            result = _columnar_action_reference_units(files["daily_reference_path"], columnar_keys)
        keys = tuple(key for key in keys if key not in result)
    if keys:
        result.update(_rule_action_reference_units(files, asset_class, keys))
    return result


def _columnar_action_reference_units(path, keys):
    codes = tuple(sorted({code for code, _ in keys}))
    stride = 1 << 32
    ranks = {code: rank for rank, code in enumerate(codes)}
    dates = {day: date.fromisoformat(day) for _, day in keys}
    epoch = date(1970, 1, 1).toordinal()
    targets = {
        ranks[code] * stride + dates[day].toordinal() - epoch: (code, day)
        for code, day in keys
    }
    target_array = pa.array(sorted(targets), type=pa.int64())
    codes_array = pa.array(codes, type=pa.string())
    cutoffs = {day: datetime.combine(value, time(9, 30), _TZ) for day, value in dates.items()}
    columns = (
        "instrument_id", "trading_date", "prices_revision", "prices_available_at",
        "prices_status", "prices_reference_price_kind", "prices_reference_price_units",
        "prices_reference_previous_close_units",
    )
    winners = {}
    conflicts = set()
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=32768, columns=columns):
            rank_values = pc.index_in(batch["instrument_id"], value_set=codes_array)
            ranks64 = pc.cast(rank_values, pa.int64())
            day_offsets = pc.cast(pc.cast(batch["trading_date"], pa.date32()), pa.int32())
            packed = pc.add(pc.multiply(ranks64, stride), day_offsets)
            matches = pc.and_(pc.is_valid(packed), pc.is_in(packed, value_set=target_array))
            if not pc.any(matches).as_py():
                continue
            selected = batch.filter(matches)
            for identity, row in zip(packed.filter(matches).to_pylist(), selected.to_pylist()):
                key = targets[identity]
                revision = row["prices_revision"]
                if revision is None:
                    continue
                status = row["prices_status"]
                available = row["prices_available_at"]
                if status != "unsupported" and (
                    available is None or datetime.fromisoformat(available) > cutoffs[key[1]]
                ):
                    continue
                if key in winners and revision == winners[key][0]:
                    conflicts.add(key)
                    continue
                if key not in winners or revision > winners[key][0]:
                    conflicts.discard(key)
                    value = (row["prices_reference_price_units"]
                             if row["prices_reference_price_kind"] == "previous_close"
                             else row["prices_reference_previous_close_units"])
                    winners[key] = revision, status, value
    result = {}
    for key, (_, status, value) in winners.items():
        if key in conflicts:
            continue
        if status == "supported":
            if value is None:
                raise ValueError(f"公司行动缺少封存除权参考价：{key[0]} {key[1]}")
            result[key] = value
    return result


def _rule_action_reference_units(files, asset_class, keys):
    prefix = "cn_fund" if asset_class == "cn_etf" else "cn_stock"
    rule_id = f"rule.{prefix}.price_limit.v1"
    days_by_code = defaultdict(set)
    for code, day in keys:
        days_by_code[code].add(day)
    rows = _frozen_rule_rows(files, rule_ids=[rule_id], action_dates={
        code: sorted(days) for code, days in days_by_code.items()})
    source_path = Path(files["rules_path"]).with_name("sources.parquet")
    sources = tuple(MinuteRuleSource.from_dict(row) for row in pq.read_table(source_path).to_pylist())
    rules = tuple(MinuteRuleSnapshot.from_dict(row) for row in rows)
    resolver = MinuteRuleResolver(SimpleNamespace(sources=sources, rules=rules))
    return {(code, day): previous_close_units(resolver, rule_id, code, day)
            for code, day in dict.fromkeys(keys)}


def previous_close_units(resolver, rule_id, code, day):
    effective = date.fromisoformat(day)
    binding = resolver.resolve(rule_id=rule_id, instrument_id=code, effective_on=effective,
        as_of=datetime.combine(effective, time(9, 30), _TZ))
    parameters = dict(binding.rule.parameters)
    value = (parameters.get("reference_price_units")
             if parameters.get("reference_price_kind") == "previous_close"
             else parameters.get("reference_previous_close_units"))
    if value is None:
        raise ValueError(f"公司行动缺少封存除权参考价：{code} {day}")
    return value


def _actions(snapshot_context, symbols, dates, rules_by_reference, *, price_scale=2, start_at=None, end_at=None,
             lifecycle=None, load_references=None):
    if "adjustment_snapshots" in snapshot_context:
        members = snapshot_context["adjustment_snapshots"]
    else:
        members = {snapshot_context["adjustment_snapshot"]["instrument_id"]: snapshot_context}
    actions = {code: member["financial_corporate_actions"] for code, member in members.items()}
    return _action_events(actions, symbols, dates, rules_by_reference, price_scale=price_scale,
                          start_at=start_at, end_at=end_at, lifecycle=lifecycle, load_references=load_references)


def _action_events(actions_by_code, symbols, dates, rules_by_reference, *, price_scale=2, start_at=None,
                   end_at=None, lifecycle=None, load_references=None):
    if set(actions_by_code) != set(symbols):
        raise ValueError("正式公司行动必须完整覆盖冻结证券集合")
    events, seeds = [], []
    reference_events = []
    start_at = start_at or datetime.combine(date.fromisoformat(dates[0]), time(9, 30), _TZ)
    end_at = end_at or datetime.combine(date.fromisoformat(dates[-1]), time(15), _TZ)
    supported = {"cash_dividend", "stock_dividend", "split"}
    def point(day, clock):
        return None if day is None else f"{day}T{clock}:00+08:00"
    for code, actions in actions_by_code.items():
        for action in actions:
            relevant_end = max(value for value in (action["ex_date"], action["pay_date"],
                action.get("shares_arrival_date"), action.get("shares_sellable_date")) if value)
            if action["record_date"] > dates[-1] or relevant_end < dates[0]:
                continue
            # 初始空仓没有窗口前已除权的应收权益，也没有需要调整的旧价格。
            ex_at = datetime.combine(date.fromisoformat(action["ex_date"]), time(9, 30), _TZ)
            if ex_at < start_at:
                continue
            if action["kind"] not in supported:
                raise ValueError(f"LEAN公司行动类型未支持：{action['kind']}")
            ending = (lifecycle or {}).get(code)
            delisted = (ending is not None and action["ex_date"] > ending["delisted_date"]
                        and datetime.fromisoformat(ending["available_at"]) <= ex_at)
            needs_reference = ex_at <= end_at and not delisted
            reference = (rules_by_reference(code, action["ex_date"])
                         if needs_reference and load_references is None else None)
            cash = action["kind"] == "cash_dividend"
            source = action.get("source_ref")
            if not source:
                raise ValueError("LEAN公司行动缺少封存source_ref")
            event = {"action_id": action["action_id"], "revision": action["revision"], "kind": action["kind"],
                "symbol": code.split(".")[0], "available_at": action["announcement_available_time"],
                "record_at": point(action["record_date"], "15:00"), "ex_at": point(action["ex_date"], "09:30"),
                "cash_pay_at": point(action["pay_date"], "09:30") if cash else None,
                "shares_arrival_date": action.get("shares_arrival_date"),
                "shares_arrival_at": point(action.get("shares_arrival_date"),
                    "15:00" if action.get("shares_arrival_date") == action["record_date"] else "09:30"),
                "share_settlement_policy": action.get("share_settlement_policy", "actual_arrival"),
                "shares_sellable_at": point(action.get("shares_sellable_date"), "09:30"),
                "cash_per_share": float(Decimal(action.get("cash_per_share_microunits", 0)) / 1000000
                    + Decimal(action.get("cash_per_share_units", 0)) / 100),
                "ratio_numerator": 1 if cash else action["ratio_numerator"],
                "ratio_denominator": 1 if cash else action["ratio_denominator"],
                "ex_reference_price": (None if reference is None
                    else float(Decimal(reference) / (10 ** price_scale))), "source_ref": source}
            events.append(event)
            if needs_reference and load_references is not None:
                reference_events.append((event, (code, action["ex_date"])))
    if reference_events:
        references = load_references([key for _, key in reference_events])
        for event, key in reference_events:
            event["ex_reference_price"] = float(Decimal(references[key]) / (10 ** price_scale))
    versions_by_action = {}
    for event in events:
        versions_by_action.setdefault(event["action_id"], []).append(event)
    for action_id, versions in sorted(versions_by_action.items()):
        record_times = sorted({datetime.fromisoformat(event["record_at"]) for event in versions
                               if datetime.fromisoformat(event["record_at"]) < start_at})
        if not record_times:
            continue
        registered = None
        for record_at in record_times:
            visible = [event for event in versions
                       if datetime.fromisoformat(event["available_at"]) <= record_at]
            if visible:
                latest = max(visible, key=lambda event: event["revision"])
                if datetime.fromisoformat(latest["record_at"]) == record_at:
                    registered = latest
                    break
        if registered is None:
            raise ValueError(f"登记时没有可见行动版本：{action_id}")
        seeds.append({"action_id": action_id, "revision": registered["revision"], "quantity": 0,
            "record_at": registered["record_at"], "source_ref": "initial_empty_portfolio:" + registered["source_ref"]})
    return events, seeds


def _metadata(runtime, root, *, price_scale=2, futures=False):
    if isinstance(runtime, Mapping):
        template = Path(runtime["installation_root"]) / runtime.get("metadata_relative", "rp-integration/metadata")
    else:
        template = Path(runtime) / "rp-integration/metadata"
    for name in ("market-hours", "symbol-properties"):
        shutil.copytree(template / name, root / "metadata" / name)
    if futures:
        return
    path = root / "metadata/symbol-properties/symbol-properties-database.csv"
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream))
    for row in rows:
        if row and row[0] == "jqchina":
            row[7] = row[9] = "1"
            row[6] = str(Decimal(1) / (10 ** price_scale))
    with path.open("w", encoding="utf-8", newline="") as stream:
        csv.writer(stream).writerows(rows)
    for market in ("jqchina", "usa"):
        for name in ("map_files", "factor_files"):
            (root / "metadata/equity" / market / name).mkdir(parents=True, exist_ok=True)


def _compact_execution_windows(windows):
    from collections import Counter

    fields = ("start_at", "end_at", "session_open_at", "session_close_at")
    times = {}
    for day, row in windows.items():
        points = tuple(row[field] for field in fields)
        if any(not point.startswith(day + "T") for point in points):
            raise ValueError("现货执行窗口必须与交易日一致")
        times[day] = tuple(point[len(day):] for point in points)
    default = Counter(times.values()).most_common(1)[0][0]
    return {"default": default, "overrides": {day: row for day, row in times.items() if row != default}}


def prepare_lean_inputs(context, parameters, declaration, bundle_path, output_root) -> dict:
    """准备执行配置并交接已打开的行情来源。"""
    with ExitStack() as resources:
        result = _prepare_lean_inputs(context, parameters, declaration, bundle_path, output_root, resources)
        resources.pop_all()
        return result


def _prepare_lean_inputs(context, parameters, declaration, bundle_path, output_root, resources):
    from .lean_market_source import lifecycle_inputs, open_market_source, terminal_observation_at

    if isinstance(declaration, Mapping):
        declaration = parse_strategy_declaration(declaration)
    _validate_bindings(declaration)
    future = declaration.asset_class == "cn_future"
    instruments = tuple(InstrumentKey.from_dict(item) for item in parameters["instruments"])
    symbols = sorted(item.instrument_id for item in instruments)
    if (len(set(symbols)) != len(symbols) or not symbols
            or declaration.asset_class not in MINUTE_TARGET_ASSET_CLASSES
            or any(item.asset_class != declaration.asset_class or item.currency != "CNY" for item in instruments)):
        raise ValueError("LEAN证券必须为同一声明资产类别的不重复人民币股票、ETF或真实期货合约")
    if future and any(item.contract_kind not in {"future", "future_contract"}
                      or any(token in item.instrument_id.upper() for token in ("8888", "9999", "CONTINUOUS"))
                      for item in instruments):
        raise ValueError("LEAN期货必须使用真实合约，不能交易连续或品种代码")
    price_scale = parameters.get("price_scale", 3 if declaration.asset_class == "cn_etf" else 2)
    if not future and (type(price_scale) is not int or price_scale != (3 if declaration.asset_class == "cn_etf" else 2)):
        raise ValueError("股票价格精度须为2，ETF价格精度须为3")
    manifest = verify_project_operator_bundle(bundle_path)
    if manifest.bundle_hash != declaration.bundle_id:
        raise ValueError("LEAN策略bundle与冻结声明不一致")
    if manifest.lean_algorithm != declaration.algorithm:
        raise ValueError("LEAN原生算法入口与冻结bundle不一致")
    worker_parameters = project_strategy_worker_parameters(parameters, manifest.operator_spec)
    bars = _input_external_payload(context, "bars")
    if parameters.get("frequency", "1m") == "1m" and bars.get("price_mode") != "raw":
        raise ValueError("LEAN正式bars必须明确声明price_mode=raw")
    scope = _minute_scope_from_payload(parameters, context)
    if scope is None:
        raise ValueError("LEAN必须声明正式冻结minute_scope")
    if bars.get("minute_scope", scope.reference) != scope.reference:
        raise ValueError("LEAN bars与执行minute_scope引用不一致")
    rules_hash = scope.reference["rules_hash"]
    if parameters.get("rule_bundle_hash", rules_hash) != rules_hash:
        raise ValueError("LEAN规则身份与冻结scope不一致")
    direct_actions = "stock_corporate_actions" in parameters
    if direct_actions and (declaration.asset_class != "cn_stock" or declaration.frequency != "1m"
                           or "snapshot" in context.inputs):
        raise ValueError("公司行动直读仅用于原始股票分钟执行")
    snapshot = None if future or direct_actions else _input_external_payload(context, "snapshot")
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError("LEAN输入输出目录必须为空，不能复用旧run")
    lifecycle = {} if future else lifecycle_inputs(parameters.get("lifecycle", {}), symbols)
    market = open_market_source(context, bars, scope, symbols, parameters, lifecycle)
    resources.callback(market.close)
    dates = list(market.dates)
    if not dates:
        raise ValueError("LEAN声明范围没有交易日")
    direct_minute = market.frequency == "1m"
    frozen_spot = direct_minute and not future
    rules_files = frozen_rule_files(scope) if not future else None
    if frozen_spot and "daily_reference_path" in rules_files and scope.has_admitted_coverage(
        instruments=symbols, start_at=market.plan.start_at, end_at=market.plan.visible_end, stock_raw=direct_actions,
    ):
        rules_files["validated_daily_prices"] = True
    execution_windows = {} if future else market.execution_windows
    terminal_at = None if future else terminal_observation_at(market, lifecycle, symbols)
    futures_inputs = None
    rule_preparation_started = perf_counter()
    action_limitations = ()
    if future:
        from .lean_futures_inputs import FuturesInputs, FuturesSettlementSource
        futures_inputs = FuturesInputs(scope, symbols, dates, calendar=market.futures_calendar,
                                       roll_plans=parameters.get("roll_plans", ()))
        settlements = FuturesSettlementSource(context, parameters, bars, futures_inputs)
        price_scales = {code: item["price_scale"] for code, item in futures_inputs.contracts.items()}
        price_scale = price_scales[symbols[0]]
        events, seeds = [], []
    else:
        price_scales = None
        action_options = dict(price_scale=price_scale,
            start_at=datetime.fromisoformat(execution_windows[dates[0]]["start_at"]),
            end_at=datetime.fromisoformat(terminal_at or execution_windows[dates[-1]]["end_at"]),
            lifecycle=lifecycle,
            load_references=lambda keys: action_reference_units(rules_files, declaration.asset_class, keys))
        if direct_actions:
            from .adapters.minute_adjustment import compile_stock_action_facts, _financial_date_completion_metadata

            settings = parse_stock_corporate_actions(parameters["stock_corporate_actions"])
            facts = compile_stock_action_facts(
                context, input_port="corporate_actions", action_request_id=settings["action_request_id"],
                calendar_request_id=settings["calendar_request_id"], instrument_ids=tuple(symbols),
                applicable_start=action_options["start_at"], applicable_end=action_options["end_at"],
                as_of=market.plan.as_of, missing_date_policy=settings.get("financial_missing_date_policy", "error"),
                financial_entitlement_exclusions=settings.get("financial_entitlement_exclusions", ()),
            )
            events, seeds = _action_events({code: [item.to_dict() for item in actions]
                                           for code, actions in facts.items()}, symbols, dates, None, **action_options)
            metadata = _financial_date_completion_metadata(settings)
            action_limitations = metadata.limitations if metadata else ()
        else:
            financial = strategy_snapshot_context(snapshot, source_bars_hash=market.source_identity,
                **({"frequency": "1d"} if parameters.get("frequency") == "1d" else {}))
            events, seeds = _actions(financial, symbols, dates, None, **action_options)
    rule_preparation_seconds = perf_counter() - rule_preparation_started
    source = root / "strategy-source"
    shutil.copytree(Path(bundle_path) / "sources", source)
    # 完整生命周期来源供账户按时点消费，不把未来退市记录交给策略参数。
    worker_parameters.pop("lifecycle", None)
    worker_parameters.pop("stock_corporate_actions", None)
    host_parameters = {"slippage_bps": parameters["slippage_bps"], "slippage_ticks": parameters["slippage_ticks"],
        "participation_cap_ppm": parameters["participation_ppm"]}
    worker_parameters.update(host_parameters)
    strategy_input = {"dates": dates, "symbols": symbols,
        "instruments": {item.instrument_id: item.to_dict() for item in instruments},
        "parameters": worker_parameters,
        "declaration": declaration.to_dict(),
        "frequency": market.frequency, "algorithm": declaration.algorithm.to_dict(),
        "initial_cash_units": parameters["initial_cash_units"], "initial_entitlements": seeds,
        **({"lifecycle": lifecycle} if not future else {}),
        **({"terminal_observation_at": terminal_at} if terminal_at else {}),
        "minute_scope": dict(scope.reference),
        **({"execution_windows": execution_windows} if not future else {}),
        **({"roll_plans": futures_inputs.roll_plans, "contracts": futures_inputs.contracts,
            "sessions": futures_inputs.sessions, "start_at": futures_inputs.start_at.isoformat(),
            "end_at": futures_inputs.end_at.isoformat(), "settlement_only": futures_inputs.settlement_only}
           if future else {})}
    _write_json(root / "algorithm-manifest.json", manifest.to_dict())
    _write_json(root / "actions.json", events)
    _metadata(parameters["lean_runtime"], root, price_scale=price_scale, futures=future)

    minute_parquet = market.native_minute_manifest() if direct_minute else None

    if frozen_spot:
        delivery = {"source_kind": "frozen_minute_parquet", "rules_parquet": rules_files}
    else:
        delivery = {"source_kind": "frozen_financial_day", "financial_day": {
            "scope_root": str(Path(scope.root).resolve()), "scope": dict(scope.reference),
            "memory_bytes": context.effective_resource_budget.memory_bytes,
            **({"market": market.descriptor} if not direct_minute else {}),
            **({"settlements": settlements.descriptor} if future else {"lifecycle": lifecycle}),
        }, **({"rules_parquet": rules_files} if not future else {})}
    input_manifest = {**delivery, "price_scale": price_scale,
        "asset_class": declaration.asset_class,
        "timestamp_semantics": "completed_bar_end_asia_shanghai", "symbols": symbols,
        "frequency": market.frequency, "dates": dates,
        **({"minute_parquet": minute_parquet} if direct_minute else {}),
        **({"terminal_observation_at": terminal_at} if terminal_at else {}),
        **({"execution_windows": _compact_execution_windows(execution_windows)} if not future else {}),
        **({"contracts": futures_inputs.contracts,
            "sessions": futures_inputs.sessions, "start_at": futures_inputs.start_at.isoformat(),
            "end_at": futures_inputs.end_at.isoformat(), "settlement_only": futures_inputs.settlement_only}
           if future else {})}
    return {"run": str(root), "input_path": str(root / "input.json"),
        "strategy_input_path": str(root / "strategy-input.json"), "days": len(dates),
        "dates": dates, "symbols": len(symbols), "actions": len(events), "initial_entitlements": len(seeds),
        "algorithm": declaration.algorithm.to_dict(),
        "rule_preparation_seconds": rule_preparation_seconds,
        **({"action_limitations": action_limitations} if action_limitations else {}),
        "_market_source": market, "_input_manifest": input_manifest, "_strategy_input": strategy_input}
