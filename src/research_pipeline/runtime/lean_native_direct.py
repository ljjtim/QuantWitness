"""从 LEAN 进程内按冻结绑定读取 Arrow 来源，保留逐消费时点选择。"""
from contextlib import closing
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from itertools import islice
from pathlib import Path
from types import SimpleNamespace
import json
import math
from tempfile import TemporaryDirectory
from threading import Lock
from zoneinfo import ZoneInfo

from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef
from research_pipeline.data_plane.admitted_plan_codec import admitted_plan_from_dict
from research_pipeline.data_plane.verification_lifecycle import RunScopedArtifactVerification, activate_artifact_verification
from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.lean_algorithm import LeanDataBinding
from .lean_native_data import NativeDataBinding, _at, admitted_dataset_reader


_ZONE = ZoneInfo("Asia/Shanghai")
_NET_EPOCH = datetime(1, 1, 1)
_binding_lock = Lock()


def direct_source(root, reference, binding, plan, memory, *, market=None, open_reference=None, temp_bytes=None):
    """只冻结来源描述，实际行情和事实由引擎按请求读取。"""
    source = dict(kind="source", root=str(Path(root).resolve()), reference=reference, plan=plan.to_dict(),
                  memory_bytes=memory, temp_bytes=temp_bytes, binding=asdict(binding))
    if (getattr(market, "open_dataset", None) is not None and binding.kind == "bars"
            and binding.input_port == "bars" and market.plan.plan_hash == plan.plan_hash):
        opening = market.open_dataset
        source["opening"] = dict(reference=open_reference or opening._reference.to_dict(),
                                 plan=market.open_plan.to_dict(), column=market.open_column, days=market.dates)
        calendar = market.futures_calendar
        if calendar is not None:
            source["futures"] = dict(calendar=calendar.calendar.to_dict(), symbols=calendar.symbols)
    return source


def direct_targets(context, parameters):
    from .portfolio_target_inputs import PortfolioTargetReader

    reader = PortfolioTargetReader(context)
    instruments = tuple(InstrumentKey.from_dict(value) for value in parameters["instruments"])
    return (dict(kind="targets", root=str(reader.root.resolve()), files=reader.files, memory_bytes=reader.limit,
                 instruments=parameters["instruments"]), [item.instrument_id for item in instruments])


def _target_index(descriptor):
    from bisect import bisect_right
    from .portfolio_target_inputs import PortfolioTargetReader, _target_weights

    instruments = tuple(InstrumentKey.from_dict(item) for item in descriptor["instruments"])
    reader = PortfolioTargetReader.from_files(Path(descriptor["root"]), descriptor["files"], descriptor["memory_bytes"])
    identities = {instrument.instrument_id: instrument for instrument in instruments}
    by_symbol = {code: {} for code in identities}
    decisions = []
    retained = len(identities) * 128
    # 决策时钟只保存一份，证券索引仅保存非零权重，消费时补齐清仓权重。
    with closing(reader.read_index_rows()) as rows:
        for ordinal, (raw, execution) in enumerate(rows):
            decision = raw["decision_time"]
            decisions.append((decision, execution, decision.isoformat()))
            retained += 256
            for code, weight in _target_weights(raw, identities).items():
                if weight:
                    by_symbol[code][ordinal] = weight
                    retained += 128
            if retained > reader.limit:
                raise ValueError("组合目标证券索引超过冻结内存额度")

    def read(request):
        start = _at(request["start_at"])
        stop = min(_at(request["end_at"]), _at(request["consumer_time"]))
        weights = by_symbol[request["symbol"]]
        first = bisect_right(decisions, start, key=lambda item: item[1])
        last = bisect_right(decisions, stop, key=lambda item: item[1])
        for offset in range(first, last):
            decision, execution, batch_id = decisions[offset]
            yield dict(symbol=request["symbol"], time=decision, end_time=decision, available_at=execution,
                       values=dict(weight_ppb=weights.get(offset, 0), batch_id=batch_id))

    return read


def _sort_rows(rows, request, schema, memory, temp_bytes, scratch):
    """按已准入时钟稳定排序，只扫描本次请求选出的可见事实。"""
    import pyarrow as pa
    import pyarrow.compute as pc
    from itertools import chain

    lower = _at(request["start_at"])
    upper = min(_at(request["end_at"]), _at(request["consumer_time"]))
    # 使用无业务列同名风险的嵌套结构，最终按原始 Arrow 类型恢复业务字段。
    timestamps = pa.timestamp("us", tz="UTC")
    structure = pa.struct(list(schema))
    projected = pa.schema([("available", timestamps), ("ordinal", pa.int64()),
                           ("begin", timestamps), ("finish", timestamps),
                           ("symbol", pa.string()), ("fields", structure)])

    def batches():
        batch = []
        for ordinal, row in enumerate(rows):
            available = _at(row["available_at"])
            if not lower < available <= upper:
                continue
            batch.append(dict(available=available, ordinal=ordinal, begin=_at(row["time"]),
                              finish=_at(row["end_time"]), symbol=row["symbol"], fields=row["values"]))
            if len(batch) == 256:
                yield build(batch)
                batch.clear()
        if batch:
            yield build(batch)

    def build(batch):
        result = pa.RecordBatch.from_pylist(batch, schema=projected)
        if result.nbytes > memory // 8:
            raise ValueError("原生事件批超过冻结排序内存额度")
        return result

    def emit(ordered):
        for batch in ordered:
            if batch.schema != projected:
                batch = batch.cast(projected)
            for row in batch.to_pylist():
                yield dict(symbol=row["symbol"], time=row["begin"], end_time=row["finish"],
                           available_at=row["available"], values=row["fields"])

    pending = iter(batches())
    buffered = []
    retained = 0
    for batch in pending:
        buffered.append(batch)
        retained += batch.get_total_buffer_size() + 8 * batch.num_rows
        if retained > memory // 8:
            break
    else:
        if buffered:
            table = pa.Table.from_batches(buffered, schema=projected)
            keys = ("available", "symbol", "ordinal")
            order = pc.sort_indices(table.select(keys), sort_keys=[(key, "ascending") for key in keys])
            yield from emit(table.take(order).to_batches(max_chunksize=2048))
        return

    # 超过内存排序额度的请求沿用有界溢写；排序器直接消费 Arrow 流。
    import duckdb
    with TemporaryDirectory(prefix="event-sort-", dir=scratch) as temporary:
        config = dict(threads=1, memory_limit=f"{memory // 8}B",
                      temp_directory=temporary if temp_bytes != 0 else "")
        if temp_bytes is not None:
            config["max_temp_directory_size"] = f"{temp_bytes}B"
        with duckdb.connect(":memory:", config=config) as connection:
            with pa.RecordBatchReader.from_batches(projected, chain(buffered, pending)) as stream:
                connection.register("facts", stream)
                with connection.execute(
                    "SELECT available, ordinal, begin, finish, symbol, fields FROM facts ORDER BY available, symbol, ordinal"
                ).to_arrow_reader(2048) as ordered:
                    yield from emit(ordered)


def _load_source(descriptor, scratch):
    from .lean_native_data import _daily_reader, _event_batch_projector, _projector, _with_daily_open, _resolve_dataset

    memory = descriptor["memory_bytes"]
    raw = descriptor["binding"]
    binding = LeanDataBinding(**{**raw, "columns": tuple(raw["columns"])})
    plan = admitted_plan_from_dict(descriptor["plan"])
    resolver = ArtifactResolver(Path(descriptor["root"]), max_batch_bytes=memory // 16)
    dataset = _resolve_dataset(resolver, descriptor["reference"], plan)
    calendar = None
    if "futures" in descriptor:
        from research_pipeline.domain.session_calendar import SessionPolicyBundle
        from .lean_futures_inputs import FuturesCalendar

        futures = descriptor["futures"]
        calendar = FuturesCalendar(SessionPolicyBundle.from_dict(futures["calendar"]), futures["symbols"])
    selection = plan.temporal_selection
    daily_universe = (binding.kind == "universe" and binding.daily_time is not None
                      and binding.daily_time["rule"] != "next_session_open_following_publication_date"
                      and selection.revision_selector is None and selection.effective_interval_selector is None)
    if binding.kind == "bars" or daily_universe:
        reader = _daily_reader(dataset, plan, binding, _projector(plan, binding, futures_calendar=calendar), memory // 8)
        if "opening" in descriptor:
            from .lean_market_source import DailyMarketSource

            opening = descriptor["opening"]
            open_plan = admitted_plan_from_dict(opening["plan"])
            open_resolver = ArtifactResolver(Path(descriptor["root"]), max_batch_bytes=memory // 8)
            market = SimpleNamespace(futures_calendar=calendar, dates=opening["days"], open_column=opening["column"],
                open_plan=open_plan, open_dataset=_resolve_dataset(open_resolver, opening["reference"], open_plan))
            market._read_batches = DailyMarketSource._read_batches.__get__(market)
            reader = _with_daily_open(reader, market, binding, memory // 8)
        return reader

    events = admitted_dataset_reader(dataset, plan, _event_batch_projector(plan, binding))
    event_fields = [dataset.schema.field(name) for name in binding.columns]

    def read(request):
        with closing(events(request)) as source:
            yield from _sort_rows(source, request, event_fields, memory, descriptor["temp_bytes"], scratch)

    return read


@lru_cache(maxsize=8)
def _manifest(path):
    return {info["binding_id"]: info
            for info in json.loads(Path(path).read_text(encoding="utf-8"))["bindings"]}


@lru_cache(maxsize=8)
def _verification(root):
    return RunScopedArtifactVerification(root)


@lru_cache(maxsize=128)
def _binding(path, binding_id):
    info = _manifest(path)[binding_id]
    source = info["direct"]
    if source["kind"] == "targets":
        reader = _target_index(source)
    elif source["kind"] == "source":
        with activate_artifact_verification(_verification(str(Path(path).parent))):
            reader = _load_source(source, Path(path).parent)
    else:
        raise ValueError("未知的原生数据来源")
    return NativeDataBinding(info, reader, independent_rows=True)


def _managed_time(value):
    from System import DateTime, DateTimeKind

    diff = _at(value).astimezone(_ZONE).replace(tzinfo=None) - _NET_EPOCH
    ticks = ((diff.days * 86400 + diff.seconds) * 1_000_000 + diff.microseconds) * 10
    return DateTime(ticks, DateTimeKind.Unspecified)


def _managed_value(value):
    from System import Array, Boolean, Double, Int64, Object, String, UInt64
    from System.Collections.Generic import Dictionary

    if value is None or isinstance(value, str):
        return value
    if type(value) is bool:
        return Boolean(value)
    if isinstance(value, int):
        return UInt64(value) if value >= 1 << 63 else Int64(value)
    if isinstance(value, float) and math.isfinite(value):
        return Double(value)
    if isinstance(value, (Decimal, date)):
        return value.isoformat() if isinstance(value, date) else str(value)
    if isinstance(value, (list, tuple)):
        return Array[Object]([_managed_value(item) for item in value])
    if isinstance(value, dict):
        result = Dictionary[String, Object]()
        for name, item in value.items():
            result.Add(name, _managed_value(item))
        return result
    raise ValueError(f"原生数据字段值类型或范围无效：{type(value).__name__}")


class _Cursor:
    def __init__(self, binding, request, symbol, universe, verification):
        self._verification = verification
        self._binding_id = binding.declaration["binding_id"]
        self._rows = binding.rows(request)
        self._page_size = (binding.history_page_size if request["purpose"] in {"history", "seed"}
                           else binding.page_size)
        self._symbol = symbol
        self._universe = universe
        self._symbols = {}
        self._field_names = {}

    def next_page(self):
        with activate_artifact_verification(self._verification):
            return self._next_page()

    def _next_page(self):
        from QuantConnect import SecurityType, Symbol
        from QuantConnect.Data import BaseData
        from QuantWitness.Lean.China import DailyOpenBar
        from QuantWitness.LeanInput import ResearchData, ResearchTradeBar
        from System import Array, Decimal as NetDecimal, Object, String
        from System.Collections.Generic import List
        from System.Globalization import CultureInfo, NumberStyles

        result = List[BaseData]()
        for row in islice(self._rows, self._page_size):
            symbol = self._symbol
            available = _managed_time(row["available_at"])
            if "values" in row:
                if self._universe:
                    code = row["symbol"]
                    symbol = self._symbols.get(code)
                    if symbol is None:
                        symbol = Symbol.Create(code.split(".")[0], SecurityType.Equity, "jqchina")
                        self._symbols[code] = symbol
                data = ResearchData()
                data.Symbol = symbol
                data.Time = available
                data.BindingId = self._binding_id
                data.ObservationTime = _managed_time(row["time"])
                data.ObservationEndTime = _managed_time(row["end_time"])
                fields = row["values"]
                layout = tuple(fields)
                names = self._field_names.get(layout)
                if names is None:
                    names = Array[String](layout)
                    self._field_names[layout] = names
                data.SetTypedFields(names, Array[Object]([_managed_value(value) for value in fields.values()]))
            elif row.get("execution_open"):
                data = DailyOpenBar(available, symbol,
                    NetDecimal.Parse(str(row["open"]), NumberStyles.Float, CultureInfo.InvariantCulture))
            else:
                values = [NetDecimal.Parse(str(row[name]), NumberStyles.Float, CultureInfo.InvariantCulture)
                          for name in ("open", "high", "low", "close", "volume")]
                data = ResearchTradeBar(_managed_time(row["time"]), _managed_time(row["end_time"]),
                                        available, symbol, *values)
            result.Add(data)
        return result

    def close(self):
        with activate_artifact_verification(self._verification):
            self._rows.close()


def open_cursor(path, binding_id, code, start_at, end_at, consumer_time, purpose, symbol, universe):
    from research_pipeline.platform.lean_python_lifetime import register_pit_shutdown

    register_pit_shutdown()
    # History 与订阅可同时发起首次请求；每个绑定的目标索引只构建一次。
    with _binding_lock:
        binding = _binding(path, binding_id)
    return _Cursor(binding, dict(symbol=code, start_at=start_at, end_at=end_at,
                                                   consumer_time=consumer_time, purpose=purpose), symbol, universe, _verification(str(Path(path).parent)))
