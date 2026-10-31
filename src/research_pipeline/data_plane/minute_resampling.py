"""基于版本化交易时段的分钟线逐批重采样。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import math
from itertools import chain
from typing import Any, Iterable, Iterator, Mapping
from zoneinfo import ZoneInfo

from research_pipeline.domain import (
    CN_MARKET_TIMEZONE,
    SessionCalendarError,
    SessionCalendarResolver,
    SessionInstrumentMetadata,
    SessionPolicyBundle,
)
from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import ProviderExecutionError, QueryIRInvalidError
from .minute_scan import MinuteScanPlan


MINUTE_RESAMPLE_PLAN_VERSION = "minute-resample-plan"
MINUTE_RESAMPLE_MANIFEST_VERSION = "minute-resample-manifest"
MINUTE_BAR_ARTIFACT_VERSION = "minute-bar-artifact"
SUPPORTED_MINUTE_INTERVALS = (1,)
AVG_POLICY = "positive-volume-weighted-source-avg"
_ZONE = ZoneInfo(CN_MARKET_TIMEZONE)


@dataclass(frozen=True)
class MinuteResampleBudget:
    output_batch_rows: int = 4096

    def __post_init__(self) -> None:
        if (
            type(self.output_batch_rows) is not int
            or not 1 <= self.output_batch_rows <= 65_536
        ):
            raise QueryIRInvalidError("分钟重采样批大小无效")

    def to_dict(self) -> dict[str, int]:
        return {"output_batch_rows": self.output_batch_rows}


@dataclass(frozen=True)
class MinuteBucketSpec:
    code: str
    trading_date: date
    session_id: str
    session_hash: str
    segment_id: str
    bar_start: datetime
    bar_end: datetime
    expected_minutes: int
    visible_minutes: int
    segment_tail_partial: bool
    query_boundary_partial: bool

    @property
    def key(self) -> tuple[str, datetime]:
        return self.code, self.bar_end


@dataclass(frozen=True)
class MinuteResamplePlan:
    scan_plan_hash: str
    interval_minutes: int
    session_policy_bundle_hash: str
    session_scope_binding_hash: str
    minute_capability_manifest_hash: str
    policy_revision: int | None
    instruments: tuple[SessionInstrumentMetadata, ...]
    start_at: datetime
    end_at: datetime
    as_of: datetime
    avg_policy: str
    budget: MinuteResampleBudget
    contract_version: str = MINUTE_RESAMPLE_PLAN_VERSION
    _session_bundle: SessionPolicyBundle = field(
        repr=False,
        compare=False,
        hash=False,
        default=None,
    )

    def __post_init__(self) -> None:
        if self.contract_version != MINUTE_RESAMPLE_PLAN_VERSION:
            raise QueryIRInvalidError("MinuteResamplePlan 合同不受支持")
        if self.interval_minutes not in SUPPORTED_MINUTE_INTERVALS:
            raise QueryIRInvalidError("分钟重采样周期不受支持")
        if self.avg_policy != AVG_POLICY or (self.policy_revision is not None and self.policy_revision < 1):
            raise QueryIRInvalidError("分钟重采样 avg 或 session revision 无效")
        if self._session_bundle is None:
            raise QueryIRInvalidError("分钟重采样缺少 session bundle")
        if any(
            item.tzinfo is None or item.utcoffset() is None
            for item in (self.start_at, self.end_at, self.as_of)
        ) or self.start_at >= self.end_at:
            raise QueryIRInvalidError("分钟重采样时间范围无效")
        identifiers = tuple(item.instrument_id for item in self.instruments)
        if not identifiers or identifiers != tuple(sorted(set(identifiers))):
            raise QueryIRInvalidError("分钟重采样标的必须非空、唯一且排序")

    @property
    def plan_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @property
    def artifact_semantics_hash(self) -> str:
        return typed_canonical_hash({
            "artifact_version": MINUTE_BAR_ARTIFACT_VERSION,
            "interval_minutes": self.interval_minutes,
            "session_policy_bundle_hash": self.session_policy_bundle_hash,
            "minute_capability_manifest_hash": self.minute_capability_manifest_hash,
            "policy_revision": self.policy_revision,
            "instruments": [item.to_dict() for item in self.instruments],
            "start_at": self.start_at.isoformat(timespec="microseconds"),
            "end_at": self.end_at.isoformat(timespec="microseconds"),
            "as_of": self.as_of.isoformat(timespec="microseconds"),
            "avg_policy": self.avg_policy,
        })

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "scan_plan_hash": self.scan_plan_hash,
            "interval_minutes": self.interval_minutes,
            "session_policy_bundle_hash": self.session_policy_bundle_hash,
            "session_scope_binding_hash": self.session_scope_binding_hash,
            "minute_capability_manifest_hash": self.minute_capability_manifest_hash,
            "policy_revision": self.policy_revision,
            "instruments": [item.to_dict() for item in self.instruments],
            "start_at": self.start_at.isoformat(timespec="microseconds"),
            "end_at": self.end_at.isoformat(timespec="microseconds"),
            "as_of": self.as_of.isoformat(timespec="microseconds"),
            "avg_policy": self.avg_policy,
            "budget": self.budget.to_dict(),
        }


@dataclass(frozen=True)
class MinuteResampleManifest:
    resample_plan_hash: str
    artifact_semantics_hash: str
    input_reference_id: str
    output_schema_hash: str
    output_rows: int
    output_batches: int
    status_counts: tuple[tuple[str, int], ...]
    contract_version: str = MINUTE_RESAMPLE_MANIFEST_VERSION

    @property
    def artifact_id(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "resample_plan_hash": self.resample_plan_hash,
            "artifact_semantics_hash": self.artifact_semantics_hash,
            "input_reference_id": self.input_reference_id,
            "output_schema_hash": self.output_schema_hash,
            "output_rows": self.output_rows,
            "output_batches": self.output_batches,
            "status_counts": dict(self.status_counts),
        }


def build_minute_resample_plan(
    scan_plan: MinuteScanPlan,
    *,
    interval_minutes: int,
    session_bundle: SessionPolicyBundle,
    instruments: Iterable[SessionInstrumentMetadata],
    policy_revision: int = 1,
    budget: MinuteResampleBudget | None = None,
) -> MinuteResamplePlan:
    """只冻结查询和 session 语义，不预生成全样本分钟桶。"""

    if interval_minutes not in SUPPORTED_MINUTE_INTERVALS:
        raise QueryIRInvalidError("仅支持一分钟行情")
    from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION
    if session_bundle.contract_version == PROJECT_SESSION_BUNDLE_VERSION:
        policy_revision = None
    metadata = tuple(sorted(instruments, key=lambda item: item.instrument_id))
    if tuple(item.instrument_id for item in metadata) != tuple(
        sorted(scan_plan.instruments)
    ):
        raise QueryIRInvalidError("session instrument 与扫描 universe 不一致")
    if any(item.asset_class != scan_plan.minute_asset_class for item in metadata):
        raise QueryIRInvalidError("session instrument asset_class 与扫描计划不一致")
    binding = dict(session_bundle.capability_binding)
    if (
        binding["minute_capability_manifest_hash"]
        != scan_plan.minute_capability_manifest_hash
    ):
        raise QueryIRInvalidError("session policy 与扫描计划未绑定同一分钟能力 manifest")
    policies_by_instrument = {}
    for policy in session_bundle.policies:
        if policy_revision is None or policy.revision == policy_revision:
            policies_by_instrument.setdefault(policy.instrument, []).append(policy)
    for instrument in metadata:
        policies = policies_by_instrument.get(instrument, ())
        if (
            not policies
            or any(item.policy_id != scan_plan.minute_session_policy_ref for item in policies)
        ):
            raise QueryIRInvalidError("session policy id/revision 与扫描计划不一致")
    return MinuteResamplePlan(
        scan_plan.plan_hash,
        interval_minutes,
        session_bundle.bundle_hash,
        session_bundle.capability_binding_hash,
        scan_plan.minute_capability_manifest_hash,
        policy_revision,
        metadata,
        scan_plan.start_at,
        scan_plan.end_at,
        scan_plan.as_of,
        AVG_POLICY,
        budget or MinuteResampleBudget(),
        _session_bundle=session_bundle,
    )


class _Accumulator:
    def __init__(self, *, has_open_interest: bool) -> None:
        self.has_open_interest = has_open_interest
        self.count = 0
        self.open: float | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.close: float | None = None
        self.volume = 0.0
        self.money = 0.0
        self.avg_numerator = 0.0
        self.avg_unavailable = False
        self.open_interest: float | None = None

    def add(self, row: Mapping[str, object]) -> None:
        values = {
            name: _finite(row.get(name), name)
            for name in ("open", "high", "low", "close", "volume", "money")
        }
        if values["volume"] < 0 or values["money"] < 0:
            raise ProviderExecutionError("分钟源 volume/money 不能为负")
        if not values["low"] <= min(values["open"], values["close"], values["high"]):
            raise ProviderExecutionError("分钟源 OHLC 下界不一致")
        if not values["high"] >= max(values["open"], values["close"], values["low"]):
            raise ProviderExecutionError("分钟源 OHLC 上界不一致")
        if self.count == 0:
            self.open = values["open"]
            self.high = values["high"]
            self.low = values["low"]
        else:
            self.high = max(float(self.high), values["high"])
            self.low = min(float(self.low), values["low"])
        self.close = values["close"]
        self.volume += values["volume"]
        self.money += values["money"]
        if values["volume"] > 0:
            avg = row.get("avg")
            if avg is None:
                self.avg_unavailable = True
            else:
                self.avg_numerator += _finite(avg, "avg") * values["volume"]
        if self.has_open_interest:
            value = row.get("open_interest")
            self.open_interest = None if value is None else _finite(value, "open_interest")
        self.count += 1

    def finish(self, spec: MinuteBucketSpec, interval_minutes: int) -> dict[str, object]:
        missing = spec.visible_minutes - self.count
        if missing:
            status = "missing_minutes"
        elif spec.query_boundary_partial:
            status = "query_boundary_partial"
        elif spec.segment_tail_partial:
            status = "segment_tail_partial"
        else:
            status = "complete"
        if self.volume == 0:
            avg, avg_status = None, "undefined_zero_volume"
        elif self.avg_unavailable:
            avg, avg_status = None, "unavailable_source_avg"
        else:
            avg = self.avg_numerator / self.volume
            avg_status = "volume_weighted_source_avg"
        return {
            "code": spec.code,
            "dt": spec.bar_end,
            "bar_start": spec.bar_start,
            "trading_date": spec.trading_date,
            "session_id": spec.session_id,
            "session_hash": spec.session_hash,
            "segment_id": spec.segment_id,
            "interval_minutes": interval_minutes,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "money": self.money,
            "avg": avg,
            "avg_status": avg_status,
            "open_interest": self.open_interest,
            "expected_minutes": spec.expected_minutes,
            "visible_minutes": spec.visible_minutes,
            "observed_minutes": self.count,
            "missing_minutes": missing,
            "completed": status == "complete",
            "bar_status": status,
        }


@dataclass(frozen=True)
class _ResampleSession:
    instrument_id: str
    trading_date: date
    session_id: str
    segments: tuple[Any, ...]


class MinuteResampleStream(Iterator[Any]):
    """只保留当前聚合桶，并按固定行数交付 RecordBatch。"""

    def __init__(self, source: Iterable[Any], *, plan: MinuteResamplePlan, columns: tuple[str, ...] | None = None) -> None:
        import pyarrow as pa

        full_schema = minute_resample_schema()
        self._columns = tuple(full_schema.names) if columns is None else tuple(columns)
        if not self._columns or len(set(self._columns)) != len(self._columns) or not set(self._columns) <= set(full_schema.names):
            raise QueryIRInvalidError("分钟重采样投影列必须非空、唯一且已声明")
        # 状态计数先于输出投影；其余未消费列不构造 Arrow 数组。
        internal_columns = set(self._columns) | {"bar_status"}
        self._schema = pa.schema([
            field for field in full_schema if field.name in internal_columns
        ])
        self._source = source
        self._plan = plan
        self._iterator = self._batch_iterator()
        self._manifest: MinuteResampleManifest | None = None
        self._input_identity: str | None = None
        self._rows = 0
        self._batches = 0
        self._statuses: dict[str, int] = {}
        self.closed = False
        self._instruments = {item.instrument_id: item for item in plan.instruments}
        self._has_open_interest = any(
            item.instrument_class == "future_contract" for item in plan.instruments
        )
        self._resolver = SessionCalendarResolver(plan._session_bundle)
        self._current_session = None
        self._current_session_hash = None
        self._query_start = _aware_local(plan.start_at)
        self._visible_end = min(_aware_local(plan.end_at), _aware_local(plan.as_of) + timedelta(microseconds=1))

    @property
    def schema(self) -> Any:
        import pyarrow as pa
        return pa.schema([self._schema.field(name) for name in self._columns])

    @property
    def manifest(self) -> MinuteResampleManifest:
        if self._input_identity is None:
            raise ProviderExecutionError("分钟重采样尚未完整消费")
        if self._manifest is None:
            self._manifest = MinuteResampleManifest(
                self._plan.plan_hash,
                self._plan.artifact_semantics_hash,
                self._input_identity,
                typed_canonical_hash(str(self.schema)),
                self._rows,
                self._batches,
                tuple(sorted(self._statuses.items())),
            )
        return self._manifest

    def __iter__(self) -> "MinuteResampleStream":
        return self

    def __next__(self) -> Any:
        if self.closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except StopIteration:
            self.closed = True
            raise
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        close = getattr(self._source, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "MinuteResampleStream":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _batch_iterator(self) -> Iterator[Any]:
        for batch in self._one_minute_batches():
            for item in batch.column("bar_status").value_counts().to_pylist():
                status = item["values"]
                self._statuses[status] = self._statuses.get(status, 0) + item["counts"]
            self._rows += batch.num_rows
            self._batches += 1
            yield batch.select(self._columns)
        # 来源身份仍在完整消费时复验；仅延迟消费者未必需要的摘要与清单构造。
        self._input_identity = _input_reference_id(self._source)

    def _aggregated_batches(
        self, source: Iterable[Any] | None = None,
        previous_key: tuple[str, datetime] | None = None,
    ) -> Iterator[Any]:
        import pyarrow as pa

        buffer: list[dict[str, object]] = []
        for row in self._resampled_rows(source, previous_key):
            buffer.append(row)
            if len(buffer) == self._plan.budget.output_batch_rows:
                yield pa.RecordBatch.from_pylist(buffer, schema=self._schema)
                buffer = []
        if buffer:
            yield pa.RecordBatch.from_pylist(buffer, schema=self._schema)

    def _one_minute_batches(self) -> Iterator[Any]:
        import pyarrow as pa

        parts: list[Any] = []
        count = 0
        limit = self._plan.budget.output_batch_rows

        def output_batch() -> Any:
            # 切片不能携带整块源缓冲区，避免下游按批计内存时反复计入大块载荷。
            table = pa.concat_tables(parts)
            return pa.RecordBatch.from_arrays(
                [pa.concat_arrays(column.chunks) for column in table.columns],
                schema=table.schema,
            )

        for table in self._one_minute_tables():
            offset = 0
            while offset < table.num_rows:
                size = min(limit - count, table.num_rows - offset)
                parts.append(table.slice(offset, size))
                count += size
                offset += size
                if count == limit:
                    yield output_batch()
                    parts = []
                    count = 0
        if parts:
            yield output_batch()

    def _one_minute_tables(self) -> Iterator[Any]:
        import pyarrow as pa

        def source_tables() -> Iterator[Any]:
            # 小源批次合并后统一校验，暂存上限与列式输出的最大批大小一致。
            parts = []
            count = 0
            for batch in self._source:
                offset = 0
                while offset < batch.num_rows:
                    size = min(65_536 - count, batch.num_rows - offset)
                    parts.append(batch.slice(offset, size))
                    count += size
                    offset += size
                    if count == 65_536:
                        yield pa.Table.from_batches(parts)
                        parts = []
                        count = 0
            if parts:
                yield pa.Table.from_batches(parts)

        chunks = source_tables()
        pending = None
        previous_key = None
        for chunk in chunks:
            table = chunk if pending is None else pa.concat_tables([pending, chunk])
            normalized = self._one_minute_table(table, previous_key)
            if normalized is None:
                # 未对齐时间仍按原桶聚合；保留的末行覆盖跨批次同桶记录。
                remainder = chain(table.to_batches(), chunks)
                for batch in self._aggregated_batches(remainder, previous_key):
                    yield pa.Table.from_batches([batch])
                return
            if table.num_rows > 1:
                yield normalized.slice(0, table.num_rows - 1)
                previous_key = (
                    table.column("code")[-2].as_py(),
                    table.column("dt")[-2].as_py(),
                )
            pending = table.slice(table.num_rows - 1)
        if pending is not None:
            # 只保留已通过对齐检查的批次末行，此处不会转入逐行聚合。
            yield self._one_minute_table(pending, previous_key)

    def _one_minute_table(
        self, table: Any, previous_key: tuple[str, datetime] | None,
    ) -> Any:
        import numpy as np
        import pyarrow as pa
        import pyarrow.compute as pc

        names = table.schema.names
        schema = self._schema
        if "dt" not in names or not pa.types.is_timestamp(table.schema.field("dt").type):
            raise ProviderExecutionError("分钟源 dt 必须是 timestamp")
        timestamps = table.column("dt").combine_chunks()
        if timestamps.null_count:
            raise ProviderExecutionError("分钟源 dt 必须是 timestamp")
        if "code" not in names or not pa.types.is_string(table.schema.field("code").type):
            raise ProviderExecutionError("分钟源出现计划外 instrument")
        codes = table.column("code").combine_chunks()
        if codes.null_count:
            raise ProviderExecutionError("分钟源出现计划外 instrument")
        first_key = codes[0].as_py(), timestamps[0].as_py()
        descending = pc.or_(
            pc.less(codes.slice(1), codes.slice(0, len(codes) - 1)),
            pc.and_(
                pc.equal(codes.slice(1), codes.slice(0, len(codes) - 1)),
                pc.less_equal(timestamps.slice(1), timestamps.slice(0, len(codes) - 1)),
            ),
        )
        if ((previous_key is not None and first_key <= previous_key)
                or pc.any(descending).as_py()):
            raise ProviderExecutionError("分钟源必须按 code,dt 严格递增且不得重复")
        if timestamps.type.tz is not None:
            # 已有带时区输入沿用原转换，不依赖 Arrow 额外的时区数据库。
            return None
        timestamps = pc.cast(timestamps, pa.timestamp("us"))

        def constant(value: Any, type_: Any, size: int) -> Any:
            return pa.repeat(pa.scalar(value, type=type_), size)

        metadata: dict[str, list[Any]] = {
            name: [] for name in ("trading_date", "session_id", "session_hash", "segment_id")
            if name in schema.names
        }
        code_ends = pc.indices_nonzero(pc.not_equal(
            codes.slice(1), codes.slice(0, len(codes) - 1)
        )).to_pylist()
        code_ends = [index + 1 for index in code_ends] + [table.num_rows]
        timestamp_units = pc.cast(timestamps, pa.int64()).to_numpy(zero_copy_only=False)
        query_start = np.datetime64(_naive_local(self._query_start), "us").astype(np.int64)
        visible_end = np.datetime64(_naive_local(self._visible_end), "us").astype(np.int64)
        segment_sizes = []
        segment_starts = []
        segment_bounds = {}
        offset = 0
        for code_end in code_ends:
            code = codes[offset].as_py()
            while offset < code_end:
                session, segment = self._session_segment(code, timestamps[offset].as_py())
                remaining = timestamp_units[offset:code_end]
                # 不同证券常共用时段边界；只复用数值换算，政策仍逐证券解析。
                key = (segment.starts_at, segment.ends_at)
                bounds = segment_bounds.get(key)
                if bounds is None:
                    bounds = tuple(np.datetime64(_naive_local(value), "us").astype(np.int64)
                                   for value in key)
                    segment_bounds[key] = bounds
                segment_start, segment_end = bounds
                size = int(np.searchsorted(remaining, segment_end, side="right"))
                segment_sizes.append(size)
                segment_starts.append(segment_start)
                values = {"trading_date": session.trading_date, "session_id": session.session_id,
                          "session_hash": self._current_session_hash, "segment_id": segment.segment_id}
                for name in metadata:
                    metadata[name].append(values[name])
                offset += size

        # 每行仍按所属历史时段检查对齐和可见范围，避免逐证券创建小数组。
        elapsed = timestamp_units - np.repeat(segment_starts, segment_sizes)
        if np.any((elapsed < 60_000_000) | (elapsed % 60_000_000 != 0)):
            return None
        if np.any((timestamp_units < query_start) | (timestamp_units >= visible_end)):
            raise ProviderExecutionError("分钟源行不在已准入查询边界内")

        def numeric(name: str, *, nullable: bool = False, used: Any = None) -> Any:
            column = table.column(name).combine_chunks() if name in names else pa.nulls(table.num_rows)
            selected = pc.filter(column, used) if used is not None else column
            if not nullable and selected.null_count:
                raise ProviderExecutionError(f"分钟源 {name} 必须是数值")
            if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
                if len(selected) != selected.null_count:
                    raise ProviderExecutionError(f"分钟源 {name} 必须是数值")
                return pa.nulls(table.num_rows, pa.float64())
            result = pc.cast(column, pa.float64(), safe=False)
            selected = pc.filter(result, used) if used is not None else result
            if pc.any(pc.invert(pc.is_finite(selected))).as_py():
                raise ProviderExecutionError(f"分钟源 {name} 必须是有限数")
            return result

        values = {name: numeric(name) for name in ("open", "high", "low", "close", "volume", "money")}
        if pc.any(pc.or_(pc.less(values["volume"], 0), pc.less(values["money"], 0))).as_py():
            raise ProviderExecutionError("分钟源 volume/money 不能为负")
        for name in ("open", "close", "high"):
            if pc.any(pc.greater(values["low"], values[name])).as_py():
                raise ProviderExecutionError("分钟源 OHLC 下界不一致")
        for name in ("open", "close"):
            if pc.any(pc.less(values["high"], values[name])).as_py():
                raise ProviderExecutionError("分钟源 OHLC 上界不一致")
        positive = pc.greater(values["volume"], 0)
        avg = numeric("avg", nullable=True, used=positive)
        # 与单行累加器保持相同浮点运算次序，也保留零值的符号行为。
        if "avg" in schema.names:
            numerator = pc.add(0.0, pc.multiply(avg, values["volume"]))
            values["avg"] = pc.if_else(
                positive, pc.divide(numerator, pc.if_else(positive, pc.add(0.0, values["volume"]), 1.0)),
                pa.scalar(None, pa.float64()),
            )
        for name in ("volume", "money"):
            if name in schema.names:
                values[name] = pc.add(0.0, values[name])
        if "avg_status" in schema.names:
            values["avg_status"] = pc.if_else(
                positive,
                pc.if_else(pc.is_null(avg), "unavailable_source_avg", "volume_weighted_source_avg"),
                "undefined_zero_volume",
            )
        values["open_interest"] = (
            numeric("open_interest", nullable=True) if self._has_open_interest
            else pa.nulls(table.num_rows, pa.float64())
        )
        segment_indices = pa.array(np.repeat(np.arange(len(segment_sizes)), segment_sizes))
        values.update({name: pc.take(pa.array(items, type=schema.field(name).type), segment_indices)
                       for name, items in metadata.items()})
        values.update({"code": codes, "dt": timestamps})
        if "bar_start" in schema.names:
            values["bar_start"] = pc.subtract(timestamps, pa.scalar(60_000_000, pa.duration("us")))
        for name, value in (
            ("interval_minutes", 1), ("expected_minutes", 1), ("visible_minutes", 1),
            ("observed_minutes", 1), ("missing_minutes", 0),
            ("completed", True), ("bar_status", "complete"),
        ):
            if name in schema.names:
                values[name] = constant(value, schema.field(name).type, table.num_rows)
        return pa.Table.from_arrays([values[name] for name in schema.names], schema=schema)

    def _resampled_rows(
        self, source: Iterable[Any] | None = None,
        previous_key: tuple[str, datetime] | None = None,
    ) -> Iterator[dict[str, object]]:
        current_spec: MinuteBucketSpec | None = None
        accumulator: _Accumulator | None = None
        for batch in self._source if source is None else source:
            rows = (row for offset in range(0, batch.num_rows, 4096)
                    for row in batch.slice(offset, 4096).to_pylist())
            for row in rows:
                code = str(row.get("code", ""))
                dt = row.get("dt")
                if not isinstance(dt, datetime):
                    raise ProviderExecutionError("分钟源 dt 必须是 timestamp")
                key = code, dt
                if previous_key is not None and key <= previous_key:
                    raise ProviderExecutionError("分钟源必须按 code,dt 严格递增且不得重复")
                previous_key = key
                spec = self._bucket_spec(code, dt)
                if current_spec is not None and spec.key != current_spec.key:
                    if accumulator is None:
                        raise ProviderExecutionError("分钟重采样内部状态缺失")
                    yield accumulator.finish(current_spec, self._plan.interval_minutes)
                    accumulator = None
                if accumulator is None:
                    current_spec = spec
                    accumulator = _Accumulator(has_open_interest=self._has_open_interest)
                accumulator.add(row)
        if current_spec is not None and accumulator is not None:
            yield accumulator.finish(current_spec, self._plan.interval_minutes)

    def _session_segment(self, code: str, dt: datetime) -> tuple[Any, Any]:
        instrument = self._instruments.get(code)
        if instrument is None:
            raise ProviderExecutionError("分钟源出现计划外 instrument")
        aware = dt.replace(tzinfo=_ZONE) if dt.tzinfo is None else dt.astimezone(_ZONE)
        session = self._current_session
        if (session is None or session.instrument_id != code
                or not any(segment.contains_completed_bar_end(aware) for segment in session.segments)):
            try:
                if "session_hash" in self._columns:
                    session = self._resolver.resolve_completed_bar_end(
                        instrument, aware, policy_revision=self._plan.policy_revision,
                    )
                else:
                    policy, trading_date, segments = self._resolver.resolve_completed_bar_segments(
                        instrument, aware, policy_revision=self._plan.policy_revision,
                    )
                    session = _ResampleSession(
                        code, trading_date,
                        f"{policy.policy_id}@{policy.revision}:{code}:{trading_date.isoformat()}",
                        segments,
                    )
            except SessionCalendarError as exc:
                raise ProviderExecutionError("分钟源 bar_end 不属于批准 session") from exc
            # 会话政策在开盘前选定；跨证券或离开会话时重新按历史可见性解析。
            self._current_session = session
            self._current_session_hash = session.session_hash if "session_hash" in self._columns else None
        segment = next(
            (
                item
                for item in session.segments
                if item.contains_completed_bar_end(aware)
            ),
            None,
        )
        if segment is None:
            raise ProviderExecutionError("分钟源 bar_end 不属于可聚合 segment")
        return session, segment

    def _bucket_spec(self, code: str, dt: datetime) -> MinuteBucketSpec:
        session, segment = self._session_segment(code, dt)
        aware = dt.replace(tzinfo=_ZONE) if dt.tzinfo is None else dt.astimezone(_ZONE)
        completed_offset = int((aware - segment.starts_at) / timedelta(minutes=1))
        if completed_offset < 1:
            raise ProviderExecutionError("分钟源 completed bar offset 无效")
        bucket_index = (completed_offset - 1) // self._plan.interval_minutes
        bucket_start = segment.starts_at + timedelta(
            minutes=bucket_index * self._plan.interval_minutes
        )
        natural_end = bucket_start + timedelta(minutes=self._plan.interval_minutes)
        bucket_end = min(natural_end, segment.ends_at)
        expected = int((bucket_end - bucket_start) / timedelta(minutes=1))
        visible = sum(
            self._query_start <= bucket_start + timedelta(minutes=offset) < self._visible_end
            for offset in range(1, expected + 1)
        )
        if visible <= 0:
            raise ProviderExecutionError("分钟源行不在已准入查询边界内")
        return MinuteBucketSpec(
            code,
            session.trading_date,
            session.session_id,
            self._current_session_hash,
            segment.segment_id,
            _naive_local(bucket_start),
            _naive_local(bucket_end),
            expected,
            visible,
            bucket_end != natural_end,
            visible != expected,
        )


def minute_resample_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        ("code", pa.string()),
        ("dt", pa.timestamp("us")),
        ("bar_start", pa.timestamp("us")),
        ("trading_date", pa.date32()),
        ("session_id", pa.string()),
        ("session_hash", pa.string()),
        ("segment_id", pa.string()),
        ("interval_minutes", pa.int16()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("money", pa.float64()),
        ("avg", pa.float64()),
        ("avg_status", pa.string()),
        ("open_interest", pa.float64()),
        ("expected_minutes", pa.int16()),
        ("visible_minutes", pa.int16()),
        ("observed_minutes", pa.int16()),
        ("missing_minutes", pa.int16()),
        ("completed", pa.bool_()),
        ("bar_status", pa.string()),
    ])


def _input_reference_id(source: Iterable[Any]) -> str:
    for name in ("source_identity", "reference_id", "content_hash"):
        value = getattr(source, name, None)
        if isinstance(value, str) and len(value) == 64:
            return value
    raise ProviderExecutionError("分钟重采样输入必须提供已有来源身份")


def _aware_local(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise QueryIRInvalidError("分钟计划时点必须带时区")
    return value.astimezone(_ZONE)


def _naive_local(value: datetime) -> datetime:
    return value.astimezone(_ZONE).replace(tzinfo=None)


def _finite(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProviderExecutionError(f"分钟源 {field_name} 必须是数值")
    result = float(value)
    if not math.isfinite(result):
        raise ProviderExecutionError(f"分钟源 {field_name} 必须是有限数")
    return result


def execute_minute_resample(
    source: Iterable[Any], *, plan: MinuteResamplePlan, columns: tuple[str, ...] | None = None
) -> MinuteResampleStream:
    if not isinstance(plan, MinuteResamplePlan):
        raise QueryIRInvalidError("分钟重采样必须使用已编译 MinuteResamplePlan")
    return MinuteResampleStream(source, plan=plan, columns=columns)


__all__ = [
    "AVG_POLICY",
    "MINUTE_BAR_ARTIFACT_VERSION",
    "MINUTE_RESAMPLE_MANIFEST_VERSION",
    "MINUTE_RESAMPLE_PLAN_VERSION",
    "SUPPORTED_MINUTE_INTERVALS",
    "MinuteBucketSpec",
    "MinuteResampleBudget",
    "MinuteResampleManifest",
    "MinuteResamplePlan",
    "MinuteResampleStream",
    "build_minute_resample_plan",
    "execute_minute_resample",
    "minute_resample_schema",
]
