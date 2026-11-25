"""在 Launcher 内按冻结来源读取单个交易日。"""
from functools import lru_cache
import json
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.compute as pc
import numpy as np

from research_pipeline.data_plane.verification_lifecycle import activate_artifact_verification

_BAR_DTYPE = np.dtype([
    ("instrument", "<i4"), ("bar_start", "<i8"), ("bar_end", "<i8"),
    ("open_units", "<i8"), ("high_units", "<i8"), ("low_units", "<i8"),
    ("close_units", "<i8"), ("volume", "<i8"),
])


class FinancialDayReader:
    def __init__(self, manifest):
        from types import SimpleNamespace
        from research_pipeline.domain.minute_scope_input import load_minute_scope_calendar, load_minute_scope_input
        from .lean_futures_inputs import FuturesInputs, FuturesSettlementSource
        from .lean_market_source import DailyMarketSource
        from .lean_native_direct import _verification
        from .lean_strategy_inputs import _price_encoder

        self.spec = spec = json.loads(Path(manifest).read_text(encoding="utf-8"))
        source = spec["financial_day"]
        self.asset_class = spec["asset_class"]
        self.symbols = spec["symbols"]
        self.symbol_array = pa.array(self.symbols)
        self.price_encoder = _price_encoder()
        self.frequency = spec["frequency"]
        self.memory = source["memory_bytes"]
        self.future = self.asset_class == "cn_future"
        self.verification = _verification(str(Path(manifest).parent))
        scope = (load_minute_scope_input(source["scope_root"], source["scope"]) if self.future else
                 SimpleNamespace(calendar=load_minute_scope_calendar(source["scope_root"], source["scope"])))
        with activate_artifact_verification(self.verification):
            self.market = DailyMarketSource.from_frozen(source["market"], scope, self.symbols, self.memory,
                source.get("lifecycle")) if self.frequency == "1d" else None
            if self.future:
                calendar = self.market.futures_calendar if self.market is not None else None
                self.futures = FuturesInputs(scope, self.symbols, spec["dates"], calendar=calendar)
                self.settlements = FuturesSettlementSource.from_frozen(source["settlements"], self.futures, self.memory)
                self.price_scales = {code: spec["contracts"][code]["price_scale"] for code in self.symbols}
            else:
                self.price_scales = None

    def read(self, day):
        from .lean_strategy_inputs import _convert_bar_batch

        started = perf_counter()
        batches = []
        bar_bytes = 0
        limit = self.memory // 8
        with activate_artifact_verification(self.verification):
            for batch in (() if self.market is None else self.market.get_day(day)):
                converted = _convert_bar_batch(batch, self.symbols, frequency=self.frequency,
                    price_scale=self.spec["price_scale"], price_scales=self.price_scales,
                    price_encoder=self.price_encoder, symbol_array=self.symbol_array)
                if converted is None:
                    continue
                bar_bytes += converted.num_rows * _BAR_DTYPE.itemsize
                if bar_bytes > limit:
                    raise ValueError("单日行情超过已声明内存额度")
                rows = np.empty(converted.num_rows, dtype=_BAR_DTYPE)
                rows["instrument"] = pc.index_in(converted["instrument_id"], value_set=self.symbol_array).to_numpy()
                for name in _BAR_DTYPE.names[1:]:
                    rows[name] = pc.cast(converted[name], pa.int64()).to_numpy(zero_copy_only=False)
                batches.append(rows)
            bars_at = perf_counter()
            rules = []
            settlements = []
            if self.future:
                rules = self.futures._rules.get(day, [])
                settlements = self.settlements.for_header(day)
        # 规则和结算按托管对象计费。字符串保留供应商的实际来源引用。
        bar_bytes += sum(224 + 2 * (len(row["source_ref"]) + len(row["instrument_id"])) for row in rules)
        bar_bytes += sum(128 + 2 * (len(row["source_ref"]) + len(row["instrument_id"])) for row in settlements)
        if bar_bytes > limit:
            raise ValueError("单日行情及金融事件超过已声明内存额度")
        if len(batches) == 1:
            bars = memoryview(batches[0]).cast("B")
        else:
            bars = b"".join(memoryview(batch).cast("B") for batch in batches)
        return bars, rules, settlements, bars_at - started, perf_counter() - bars_at


@lru_cache(maxsize=1)
def _reader(manifest):
    return FinancialDayReader(manifest)


def read_day(manifest, day):
    from System import DateOnly, DateTimeOffset, Decimal
    from System.Collections.Generic import List
    from System.Globalization import CultureInfo
    from QuantWitness.LeanInput import FinancialFutureRule, FinancialSettlement

    bars, raw_rules, raw_settlements, bars_seconds, rules_seconds = _reader(manifest).read(day)
    invariant = CultureInfo.InvariantCulture
    rules = List[FinancialFutureRule]()
    for row in raw_rules:
        p = row["parameters"]
        rules.Add(FinancialFutureRule(row["instrument_id"], DateOnly.Parse(row["effective_from"], invariant),
            DateOnly.Parse(row["effective_to"], invariant), DateTimeOffset.Parse(row["available_at"], invariant).UtcDateTime,
            p["long_margin_ppm"], p["short_margin_ppm"], p["open_fee_per_contract_units"], p["open_fee_rate_ppm"],
            p["close_fee_per_contract_units"], p["close_fee_rate_ppm"], p["close_today_fee_per_contract_units"],
            p["close_today_fee_rate_ppm"], Decimal.Parse(p["lower_limit"], invariant),
            Decimal.Parse(p["upper_limit"], invariant), p["close_priority"], row["source_ref"]))
    settlements = List[FinancialSettlement]()
    for row in raw_settlements:
        settlements.Add(FinancialSettlement(row["instrument_id"], DateOnly.Parse(row["trading_date"], invariant),
            Decimal.Parse(row["price"], invariant), DateTimeOffset.Parse(row["available_at"], invariant).UtcDateTime,
            row["source_ref"]))
    return bars, rules, settlements, bars_seconds, rules_seconds
