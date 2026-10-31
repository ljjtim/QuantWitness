"""从正式来源交接LEAN日行情，保存策略配置与运行所需记录。"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
import csv
import json
from pathlib import Path
import shutil
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.minute_rule_snapshots import MinuteRuleResolver
from research_pipeline.domain.strategy_declaration import parse_strategy_declaration, project_strategy_worker_parameters
from research_pipeline.extensions.project_bundle import verify_project_operator_bundle
from .adapters.common import _input_external_payload
from .adapters.minute_io import _minute_scope_from_payload
from .adapters.minute_strategy_pit import strategy_snapshot_context
from .strategy_signal_inputs import open_strategy_signal_inputs, signal_payload


_TZ = ZoneInfo("Asia/Shanghai")
_KINDS = ("instrument_lifecycle", "lot_size", "price_limit", "settlement", "suspension", "trading_fee")


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
    bars = [item for item in declaration.input_bindings if item.source == "bars"]
    signals = [item for item in declaration.input_bindings if item.source == "signals"]
    if (len(bars) != 1 or not set(bars[0].columns) <= {"open", "high", "low", "close", "volume"}
            or not bars[0].columns or bars[0].lookback_bars < 1
            or len(signals) > 1 or any(item.lookback_bars != 1 for item in signals)):
        raise ValueError("LEAN须声明一个OHLCV行情binding及可选一个lookback=1信号binding")
    if declaration.schedules or set(declaration.subscriptions) - {"initialize", "before_trading", "bar", "execution_feedback", "after_trading"}:
        raise ValueError("LEAN当前有限事件接口不支持scheduled或其他调度")


def _bar_schema():
    stamp = pa.timestamp("us", tz="UTC")
    return pa.schema([
        ("instrument_id", pa.string()), ("symbol", pa.string()), ("time", stamp),
        ("bar_start", stamp), ("bar_end", stamp), ("available_time", stamp), ("receipt_time", stamp),
        *((name + "_units", pa.int64()) for name in ("open", "high", "low", "close")),
        *((name, pa.float64()) for name in ("open", "high", "low", "close")),
        ("volume", pa.int64()), ("paused", pa.bool_()), ("completed", pa.bool_()),
        ("quality_status", pa.string()), ("interval_minutes", pa.int64()),
    ])


def _convert_bar_batch(batch, symbols, *, frequency="1m", price_scale=2):
    mask = pc.is_in(batch.column("instrument"), value_set=pa.array(symbols))
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
    valid = pc.and_(valid, pc.equal(duration, 60_000_000 if frequency == "1m" else 19_800_000_000))
    if not pc.all(valid).as_py():
        raise ValueError("LEAN行情周期、完成状态或可见时刻无效")
    if "adjustment_mode" in batch.schema.names:
        modes = set(pc.unique(col("adjustment_mode")).to_pylist())
        if modes - {None, "raw"}:
            raise ValueError("LEAN仅接受原始未复权分钟行情")
    stamp = pa.timestamp("us", tz="UTC")
    ends = pc.cast(col("bar_end"), stamp)
    arrays = {"instrument_id": pc.cast(col("instrument"), pa.string()),
        "symbol": pc.cast(col("instrument"), pa.string()), "time": ends,
        "bar_start": pc.cast(col("bar_start"), stamp), "bar_end": ends,
        "available_time": pc.cast(col("available_time"), stamp),
        "receipt_time": pc.cast(col("available_time"), stamp)}
    for name in ("open", "high", "low", "close"):
        values = col(name)
        if values.null_count:
            raise ValueError("LEAN行情缺少OHLC价格")
        # 与现有分钟执行一致，价格按分half-up转换，避免二进制浮点舍位漂移。
        encoded_prices = pc.dictionary_encode(values)
        units = [int((Decimal(str(value)) * (10 ** price_scale)).to_integral_value(rounding=ROUND_HALF_UP))
                 for value in encoded_prices.dictionary.to_pylist()]
        if min(units) <= 0:
            raise ValueError("LEAN行情价格必须为正")
        arrays[name + "_units"] = pc.take(pa.array(units, type=pa.int64()), encoded_prices.indices)
        arrays[name] = pc.cast(values, pa.float64())
    volume = col("volume")
    if volume.null_count or not pc.all(pc.greater_equal(volume, 0)).as_py():
        raise ValueError("LEAN分钟成交量须为非负整数")
    integer_volume = pc.cast(volume, pa.int64(), safe=True)
    arrays.update(volume=integer_volume, paused=pa.repeat(False, batch.num_rows),
        completed=pa.repeat(True, batch.num_rows), quality_status=pa.repeat("pass", batch.num_rows),
        interval_minutes=pa.repeat(pa.scalar(interval, pa.int64()), batch.num_rows))
    schema = _bar_schema()
    return pa.RecordBatch.from_arrays([arrays[name] for name in schema.names], schema=schema)


def _rules_for_day(scope, resolver, day, symbols, observed, *, asset_class="cn_stock"):
    at = datetime.combine(day, time(9, 30), _TZ)
    result = []
    prefix = "cn_fund" if asset_class == "cn_etf" else "cn_stock"
    for code in symbols:
        # 无业务日期覆盖的历史证券不虚构规则；当日真实bars必须有规则。
        any_coverage = any(rule.effective_from <= day <= rule.effective_to
            for kind in _KINDS for rule in resolver.revisions(
                rule_id=f"rule.{prefix}.{kind}.v1", instrument_id=code))
        if not any_coverage:
            if code in observed:
                raise ValueError(f"正式行情缺少当日历史规则：{code} {day}")
            continue
        selected = [resolver.resolve(rule_id=f"rule.{prefix}.{kind}.v1",
            instrument_id=code, effective_on=day, as_of=at).rule for kind in _KINDS]
        merged, sources = {}, set()
        for rule in selected:
            for name, value in rule.parameters:
                if name in merged and merged[name] != value:
                    raise ValueError(f"冻结规则参数冲突：{code} {day} {name}")
                merged[name] = value
            sources.update(rule.source_ids)
        result.append({"instrument_id": code, "effective_from": day.isoformat(), "effective_to": day.isoformat(),
            "available_at": max(rule.available_at for rule in selected).isoformat(),
            "parameters": merged, "source_ids": sorted(sources)})
    return result


def _actions(snapshot_context, symbols, dates, rules_by_reference, *, price_scale=2):
    if "adjustment_snapshots" in snapshot_context:
        members = snapshot_context["adjustment_snapshots"]
    else:
        members = {snapshot_context["adjustment_snapshot"]["instrument_id"]: snapshot_context}
    if set(members) != set(symbols):
        raise ValueError("正式snapshot必须完整覆盖冻结证券集合")
    events, seeds = [], []
    supported = {"cash_dividend", "stock_dividend", "split"}
    def point(day, clock):
        return None if day is None else f"{day}T{clock}:00+08:00"
    for code, member in members.items():
        for action in member["financial_corporate_actions"]:
            relevant_end = max(value for value in (action["ex_date"], action["pay_date"],
                action.get("shares_arrival_date"), action.get("shares_sellable_date")) if value)
            if action["record_date"] > dates[-1] or relevant_end < dates[0]:
                continue
            if action["kind"] not in supported:
                raise ValueError(f"LEAN公司行动类型未支持：{action['kind']}")
            reference = rules_by_reference(code, action["ex_date"])
            cash = action["kind"] == "cash_dividend"
            source = action.get("source_ref")
            if not source:
                raise ValueError("LEAN公司行动缺少封存source_ref")
            event = {"action_id": action["action_id"], "revision": action["revision"], "kind": action["kind"],
                "symbol": code.split(".")[0], "available_at": action["announcement_available_time"],
                "record_at": point(action["record_date"], "15:00"), "ex_at": point(action["ex_date"], "09:30"),
                "cash_pay_at": point(action["pay_date"], "09:30") if cash else None,
                "shares_arrival_at": point(action.get("shares_arrival_date"), "09:30"),
                "shares_sellable_at": point(action.get("shares_sellable_date"), "09:30"),
                "cash_per_share": float(Decimal(action.get("cash_per_share_microunits", 0)) / 1000000
                    + Decimal(action.get("cash_per_share_units", 0)) / 100),
                "ratio_numerator": 1 if cash else action["ratio_numerator"],
                "ratio_denominator": 1 if cash else action["ratio_denominator"],
                "ex_reference_price": float(Decimal(reference) / (10 ** price_scale)), "source_ref": source}
            events.append(event)
            if action["record_date"] < dates[0]:
                seeds.append({"action_id": action["action_id"], "revision": action["revision"], "quantity": 0,
                    "record_at": event["record_at"], "source_ref": "initial_empty_portfolio:" + source})
    return events, seeds


def _signals(context, declaration, instruments, day):
    daily = {"available_at": day + "T09:30:00+08:00", "selection": []}
    with open_strategy_signal_inputs(context, declaration=declaration, instruments=instruments,
                                     day=date.fromisoformat(day)) as provider:
        if provider is None:
            return daily
        while provider.next_row is not None:
            at = provider.next_row["decision_time"]
            references = provider.visible(at)
            if at.astimezone(_TZ).date().isoformat() != day:
                raise ValueError("日信号读取超出请求日期")
            rows = [signal_payload(asdict(item)) for item in references if item.available_at == at]
            if not daily["selection"]:
                daily["available_at"] = at.isoformat()
            daily["selection"].extend(rows)
    return daily


def _metadata(runtime, root, *, price_scale=2):
    if isinstance(runtime, Mapping):
        template = Path(runtime["installation_root"]) / runtime.get("metadata_relative", "rp-integration/metadata")
    else:
        template = Path(runtime) / "rp-integration/metadata"
    for name in ("market-hours", "symbol-properties"):
        shutil.copytree(template / name, root / "metadata" / name)
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


def prepare_lean_inputs(context, parameters, declaration, bundle_path, output_root) -> dict:
    """准备小型执行配置，行情及规则通过日数据流按需交付。"""
    from .lean_market_source import open_market_source
    from .lean_day_stream import LeanDayStream, pack_day_bars

    if isinstance(declaration, Mapping):
        declaration = parse_strategy_declaration(declaration)
    _validate_bindings(declaration)
    instruments = tuple(InstrumentKey.from_dict(item) for item in parameters["instruments"])
    symbols = sorted(item.instrument_id for item in instruments)
    if (len(set(symbols)) != len(symbols) or not symbols
            or declaration.asset_class not in {"cn_stock", "cn_etf"}
            or any(item.asset_class != declaration.asset_class or item.currency != "CNY" for item in instruments)):
        raise ValueError("LEAN证券必须为同一声明资产类别的不重复人民币股票或ETF")
    price_scale = parameters.get("price_scale", 3 if declaration.asset_class == "cn_etf" else 2)
    if type(price_scale) is not int or price_scale != (3 if declaration.asset_class == "cn_etf" else 2):
        raise ValueError("股票价格精度须为2，ETF价格精度须为3")
    manifest = verify_project_operator_bundle(bundle_path)
    if manifest.bundle_hash != declaration.bundle_id:
        raise ValueError("LEAN策略bundle与冻结声明不一致")
    bars = _input_external_payload(context, "bars")
    if parameters.get("frequency", "1m") == "1m" and bars.get("price_mode") != "raw":
        raise ValueError("LEAN正式bars必须明确声明price_mode=raw")
    scope = _minute_scope_from_payload(parameters, context)
    if scope is None:
        raise ValueError("LEAN必须声明正式冻结minute_scope")
    if bars.get("minute_scope", scope.reference) != scope.reference:
        raise ValueError("LEAN bars与执行minute_scope引用不一致")
    if parameters.get("rule_bundle_hash", scope.rules.bundle_hash) != scope.rules.bundle_hash:
        raise ValueError("LEAN规则身份与冻结scope不一致")
    snapshot = _input_external_payload(context, "snapshot")
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError("LEAN输入输出目录必须为空，不能复用旧run")
    market = open_market_source(context, bars, scope, symbols, parameters)
    dates = list(market.dates)
    if not dates:
        market.close()
        raise ValueError("LEAN声明范围没有交易日")
    financial = strategy_snapshot_context(snapshot, source_bars_hash=market.source_identity,
        **({"frequency": "1d"} if parameters.get("frequency") == "1d" else {}))
    resolver = MinuteRuleResolver(scope.rules)
    def reference(code, day):
        prefix = "cn_fund" if declaration.asset_class == "cn_etf" else "cn_stock"
        binding = resolver.resolve(rule_id=f"rule.{prefix}.price_limit.v1", instrument_id=code,
            effective_on=date.fromisoformat(day), as_of=datetime.combine(date.fromisoformat(day), time(9, 30), _TZ))
        value = dict(binding.rule.parameters).get("reference_previous_close_units")
        if value is None:
            raise ValueError(f"公司行动缺少封存除权参考价：{code} {day}")
        return value
    events, seeds = _actions(financial, symbols, dates, reference, price_scale=price_scale)
    source = root / "strategy-source"
    shutil.copytree(Path(bundle_path) / "sources", source)
    module = manifest.entry_module
    worker_parameters = dict(project_strategy_worker_parameters(parameters, manifest.operator_spec, rule_bundle=scope.rules))
    host_parameters = {"slippage_bps": parameters["slippage_bps"], "slippage_ticks": parameters["slippage_ticks"],
        "participation_cap_ppm": parameters["participation_ppm"]}
    worker_parameters.update(host_parameters)
    _write_json(root / "strategy-input.json", {"dates": dates, "symbols": symbols,
        "instruments": {item.instrument_id: item.to_dict() for item in instruments},
        "parameters": worker_parameters,
        "declaration": declaration.to_dict(),
        "frequency": market.frequency, "entry": {"module": module, "function": manifest.entry_function},
        "initial_cash_units": parameters["initial_cash_units"], "initial_entitlements": seeds,
        "minute_scope": dict(scope.reference)})
    _write_json(root / "actions.json", events)
    _metadata(parameters["lean_runtime"], root, price_scale=price_scale)

    class DayLoader:
        def __call__(self, day):
            converted = [_convert_bar_batch(batch, symbols, frequency=market.frequency, price_scale=price_scale)
                         for batch in market.get_day(day)]
            converted = [batch for batch in converted if batch is not None]
            table = pa.Table.from_batches(converted, schema=_bar_schema())
            observed = set(pc.unique(table["instrument_id"]).to_pylist())
            rules = _rules_for_day(scope, resolver, date.fromisoformat(day), symbols, observed, asset_class=declaration.asset_class)
            signals = _signals(context, declaration, instruments, day)
            return rules, pack_day_bars(table, symbols), signals

        def close(self):
            market.close()

    stream = LeanDayStream(dates, DayLoader(), frequency=market.frequency,
        memory_bytes=context.effective_resource_budget.memory_bytes // 4)
    try:
        _write_json(root / "input.json", {"source_kind": "direct_daily_stream", "price_scale": price_scale,
            "timestamp_semantics": "completed_bar_end_asia_shanghai", "symbols": symbols,
            "frequency": market.frequency, "dates": dates, "endpoint": stream.endpoint})
    except BaseException:
        stream.close()
        raise
    return {"run": str(root), "input_path": str(root / "input.json"),
        "strategy_input_path": str(root / "strategy-input.json"), "days": len(dates),
        "dates": dates, "symbols": len(symbols), "actions": len(events), "initial_entitlements": len(seeds),
        "entry_module": module, "entry_function": manifest.entry_function,
        "_day_stream": stream, "_market_source": market}
