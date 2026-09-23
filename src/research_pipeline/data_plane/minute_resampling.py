"""基于版本化交易时段的分钟线逐批重采样。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import math
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
SUPPORTED_MINUTE_INTERVALS = (1, 5, 15, 30, 60, 120)
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
    policy_revision: int
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
        if self.avg_policy != AVG_POLICY or self.policy_revision < 1:
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
        raise QueryIRInvalidError("仅支持 1/5/15/30/60/120 分钟")
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
    for instrument in metadata:
        policies = tuple(
            item
            for item in session_bundle.policies
            if item.instrument == instrument and item.revision == policy_revision
        )
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


class MinuteResampleStream(Iterator[Any]):
    """只保留当前聚合桶，并按固定行数交付 RecordBatch。"""

    def __init__(self, source: Iterable[Any], *, plan: MinuteResamplePlan) -> None:
        self._source = source
        self._plan = plan
        self._iterator = self._batch_iterator()
        self._manifest: MinuteResampleManifest | None = None
        self._rows = 0
        self._batches = 0
        self._statuses: dict[str, int] = {}
        self.closed = False
        self._instruments = {item.instrument_id: item for item in plan.instruments}
        self._resolver = SessionCalendarResolver(plan._session_bundle)

    @property
    def schema(self) -> Any:
        return minute_resample_schema()

    @property
    def manifest(self) -> MinuteResampleManifest:
        if self._manifest is None:
            raise ProviderExecutionError("分钟重采样尚未完整消费")
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
        import pyarrow as pa

        buffer: list[dict[str, object]] = []
        for row in self._resampled_rows():
            status = str(row["bar_status"])
            self._statuses[status] = self._statuses.get(status, 0) + 1
            self._rows += 1
            buffer.append(row)
            if len(buffer) == self._plan.budget.output_batch_rows:
                self._batches += 1
                yield pa.RecordBatch.from_pylist(buffer, schema=self.schema)
                buffer = []
        if buffer:
            self._batches += 1
            yield pa.RecordBatch.from_pylist(buffer, schema=self.schema)
        self._manifest = MinuteResampleManifest(
            self._plan.plan_hash,
            self._plan.artifact_semantics_hash,
            _input_reference_id(self._source),
            typed_canonical_hash(str(self.schema)),
            self._rows,
            self._batches,
            tuple(sorted(self._statuses.items())),
        )

    def _resampled_rows(self) -> Iterator[dict[str, object]]:
        previous_key: tuple[str, datetime] | None = None
        current_spec: MinuteBucketSpec | None = None
        accumulator: _Accumulator | None = None
        has_open_interest = any(
            item.instrument_class == "future_contract" for item in self._plan.instruments
        )
        for batch in self._source:
            columns = {
                name: batch.column(index)
                for index, name in enumerate(batch.schema.names)
            }
            for index in range(batch.num_rows):
                row = {name: column[index].as_py() for name, column in columns.items()}
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
                    accumulator = _Accumulator(has_open_interest=has_open_interest)
                accumulator.add(row)
        if current_spec is not None and accumulator is not None:
            yield accumulator.finish(current_spec, self._plan.interval_minutes)

    def _bucket_spec(self, code: str, dt: datetime) -> MinuteBucketSpec:
        instrument = self._instruments.get(code)
        if instrument is None:
            raise ProviderExecutionError("分钟源出现计划外 instrument")
        aware = dt.replace(tzinfo=_ZONE) if dt.tzinfo is None else dt.astimezone(_ZONE)
        try:
            session = self._resolver.resolve_completed_bar_end(
                instrument,
                aware,
                policy_revision=self._plan.policy_revision,
            )
        except SessionCalendarError as exc:
            raise ProviderExecutionError("分钟源 bar_end 不属于批准 session") from exc
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
        visible_end = min(
            _aware_local(self._plan.end_at),
            _aware_local(self._plan.as_of) + timedelta(microseconds=1),
        )
        query_start = _aware_local(self._plan.start_at)
        visible = sum(
            query_start <= bucket_start + timedelta(minutes=offset) < visible_end
            for offset in range(1, expected + 1)
        )
        if visible <= 0:
            raise ProviderExecutionError("分钟源行不在已准入查询边界内")
        return MinuteBucketSpec(
            code,
            session.trading_date,
            session.session_id,
            session.session_hash,
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
    source: Iterable[Any], *, plan: MinuteResamplePlan
) -> MinuteResampleStream:
    if not isinstance(plan, MinuteResamplePlan):
        raise QueryIRInvalidError("分钟重采样必须使用已编译 MinuteResamplePlan")
    return MinuteResampleStream(source, plan=plan)


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
