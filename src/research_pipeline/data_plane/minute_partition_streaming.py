"""分钟分区的单遍质量检查与有界 RecordBatch 交付。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import math
from typing import Iterable, Iterator

from research_pipeline.domain import (
    SessionCalendarError,
    SessionCalendarResolver,
    SessionInstrumentMetadata,
    SessionPolicyBundle,
)
from research_pipeline.platform.asset_taxonomy import CANONICAL_ASSET_CLASSES
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION
from research_pipeline.domain.time import CN_MARKET_TIMEZONE

from .errors import ProviderExecutionError, SnapshotIntegrityError
from .temporal import _as_utc_datetime


_DAY_MICROSECONDS = 86_400_000_000
_MINUTE_MICROSECONDS = 60_000_000
_STOCK_MORNING_FIRST = (9 * 60 + 31) * _MINUTE_MICROSECONDS
_STOCK_MORNING_LAST = (11 * 60 + 30) * _MINUTE_MICROSECONDS
_STOCK_AFTERNOON_FIRST = (13 * 60 + 1) * _MINUTE_MICROSECONDS
_STOCK_AFTERNOON_LAST = 15 * 60 * _MINUTE_MICROSECONDS
_STOCK_LIKE_ASSETS = frozenset(CANONICAL_ASSET_CLASSES) - {"cn_future"}


@dataclass(frozen=True)
class MinutePartitionQualityReceipt:
    """一个月完整消费后形成的小型质量回执。"""

    partition_key: str
    observed_rows: int
    observed_instrument_days: int
    checked_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "partition_key": self.partition_key,
            "observed_rows": self.observed_rows,
            "observed_instrument_days": self.observed_instrument_days,
            "checked_columns": list(self.checked_columns),
            "status": "pass",
        }


class VerifiedMinutePartitionStream:
    """边拉取边检查分钟行；未完整消费时不能提交项目输出。"""

    def __init__(
        self,
        partition,
        *,
        asset_class: str,
        interval_minutes: int,
        session_policy_ref: str,
        session_bundle: SessionPolicyBundle | None = None,
        session_instruments: Iterable[SessionInstrumentMetadata] = (),
        expected_instrument_days: Iterable[tuple[str, date]] | None = None,
        source_timezone: str = CN_MARKET_TIMEZONE,
    ) -> None:
        if asset_class not in {*_STOCK_LIKE_ASSETS, "cn_future"}:
            raise SnapshotIntegrityError("分钟分区资产类别不受支持")
        if type(interval_minutes) is not int or interval_minutes <= 0:
            raise SnapshotIntegrityError("分钟分区周期必须是正整数")
        if not isinstance(session_policy_ref, str) or not session_policy_ref:
            raise SnapshotIntegrityError("分钟分区缺少 session policy")
        if asset_class == "cn_future" and session_bundle is None:
            raise SnapshotIntegrityError("期货分钟分区缺少 session policy bundle")
        if asset_class == "cn_future" and not source_timezone:
            raise SnapshotIntegrityError("期货分钟分区缺少准入来源时区")
        self.partition = partition
        self.asset_class = asset_class
        self.interval_minutes = interval_minutes
        self.session_policy_ref = session_policy_ref
        self.session_bundle = session_bundle
        self.source_timezone = source_timezone
        self._session_resolver = SessionCalendarResolver(session_bundle) if session_bundle is not None else None
        self.session_instruments = {
            item.instrument_id: item for item in session_instruments
        }
        self.expected_instrument_days = (
            None
            if expected_instrument_days is None
            else frozenset(expected_instrument_days)
        )
        self._started = False
        self._completed = False
        self._receipt: MinutePartitionQualityReceipt | None = None

    @property
    def receipt(self) -> MinutePartitionQualityReceipt:
        if self._receipt is None:
            raise ProviderExecutionError("分钟分区尚未完整消费")
        return self._receipt

    def assert_complete(self) -> None:
        if not self._completed:
            raise ProviderExecutionError("项目算子未完整消费当前分钟分区")

    def iter_batches(
        self,
        *,
        columns: tuple[str, ...],
        batch_size: int = 65_536,
    ) -> Iterator[object]:
        if self._started:
            raise ProviderExecutionError("同一项目 attempt 只能消费分钟分区一次")
        self._started = True
        required = tuple(dict.fromkeys(("code", "dt", *columns)))
        if len(required) != len(("code", "dt", *columns)) and len(set(columns)) != len(columns):
            raise SnapshotIntegrityError("分钟分区请求列不能重复")
        raw = self.partition.iter_batches(columns=required, batch_size=batch_size)
        yield from self._validate_and_project(raw, columns=columns)

    def _validate_and_project(
        self,
        batches: Iterable[object],
        *,
        columns: tuple[str, ...],
    ) -> Iterator[object]:
        import numpy as np

        previous_code: str | None = None
        previous_dt_us: int | None = None
        current_day: tuple[str, date] | None = None
        current_day_count = 0
        observed_days: set[tuple[str, date]] = set()
        observed_rows = 0

        def finish_day() -> None:
            nonlocal current_day, current_day_count
            if current_day is None:
                return
            if self.asset_class in _STOCK_LIKE_ASSETS and self.interval_minutes == 1:
                if current_day_count != 240:
                    code, trading_day = current_day
                    raise ProviderExecutionError(
                        f"分钟分区 {code}/{trading_day.isoformat()} 必须恰好 240 根，"
                        f"实际 {current_day_count} 根"
                    )
            observed_days.add(current_day)
            current_day = None
            current_day_count = 0

        for batch in batches:
            names = tuple(batch.schema.names)
            code_values = batch.column(names.index("code")).to_numpy(zero_copy_only=False)
            times = batch.column(names.index("dt"))
            if times.type.tz is not None:
                # 已归一化的aware列先转成来源墙钟，不能把UTC epoch当作naive输入。
                dt_values = times.to_pandas().dt.tz_convert(
                    self.source_timezone or CN_MARKET_TIMEZONE).dt.tz_localize(None).to_numpy()
            else:
                dt_values = times.to_numpy(zero_copy_only=False)
            volume_values = (
                batch.column(names.index("volume")).to_numpy(zero_copy_only=False)
                if "volume" in columns
                else None
            )
            if self.asset_class in _STOCK_LIKE_ASSETS:
                # 逐批验证行约束，只在证券日边界维护跨批计数。
                dt_us_values = np.asarray(dt_values, dtype="datetime64[us]").view("int64")
                if batch.column(names.index("code")).null_count or np.any(
                    (code_values == "") | (dt_us_values == np.iinfo(np.int64).min)
                ):
                    raise ProviderExecutionError("分钟分区 code/dt 不能缺失")
                if batch.num_rows:
                    days, clock_us = np.divmod(dt_us_values, _DAY_MICROSECONDS)
                    failures = []

                    def reject_first(mask, message):
                        indices = np.flatnonzero(mask)
                        if indices.size:
                            failures.append((int(indices[0]), len(failures), message))

                    order_bad = np.zeros(batch.num_rows, dtype=bool)
                    order_bad[1:] = (code_values[1:] < code_values[:-1]) | (
                        (code_values[1:] == code_values[:-1])
                        & (dt_us_values[1:] <= dt_us_values[:-1])
                    )
                    if previous_code is not None:
                        order_bad[0] = code_values[0] < previous_code or (
                            code_values[0] == previous_code
                            and int(dt_us_values[0]) <= previous_dt_us
                        )
                    reject_first(order_bad, "分钟分区必须按 code,dt 严格递增且不得重复")
                    in_session = (
                        ((clock_us >= _STOCK_MORNING_FIRST) & (clock_us <= _STOCK_MORNING_LAST))
                        | ((clock_us >= _STOCK_AFTERNOON_FIRST) & (clock_us <= _STOCK_AFTERNOON_LAST))
                    ) & (clock_us % _MINUTE_MICROSECONDS == 0)
                    reject_first(~in_session, "分钟 bar_end 不属于批准的股票 completed-bar 时段")
                    if volume_values is not None:
                        reject_first(~np.isfinite(volume_values) | (volume_values < 0),
                                     "分钟分区 volume 必须是有限非负数")
                    if failures:
                        raise ProviderExecutionError(min(failures)[2])
                    boundaries = np.flatnonzero(
                        (code_values[1:] != code_values[:-1]) | (days[1:] != days[:-1])
                    ) + 1
                    starts = np.r_[0, boundaries]
                    stops = np.r_[boundaries, batch.num_rows]
                    for start, stop in zip(starts, stops):
                        day_key = (str(code_values[start]),
                                   (datetime(1970, 1, 1) + timedelta(days=int(days[start]))).date())
                        if current_day != day_key:
                            finish_day()
                            current_day = day_key
                        current_day_count += int(stop - start)
                    previous_code = str(code_values[-1])
                    previous_dt_us = int(dt_us_values[-1])
                    observed_rows += batch.num_rows
                yield batch if names == columns else batch.select(list(columns))
                continue
            for index in range(batch.num_rows):
                code = str(code_values[index])
                dt_us = int(np.datetime64(dt_values[index], "us").astype("int64"))
                if not code or dt_us == np.iinfo(np.int64).min:
                    raise ProviderExecutionError("分钟分区 code/dt 不能缺失")
                if previous_code is not None and (
                    code < previous_code
                    or (code == previous_code and dt_us <= int(previous_dt_us))
                ):
                    raise ProviderExecutionError(
                        "分钟分区必须按 code,dt 严格递增且不得重复"
                    )
                trading_day = self._validate_session(code, dt_us)
                day_key = (code, trading_day)
                if current_day != day_key:
                    finish_day()
                    current_day = day_key
                current_day_count += 1
                if volume_values is not None:
                    value = volume_values[index]
                    if value is None:
                        raise ProviderExecutionError("分钟分区 volume 不能缺失")
                    numeric = float(value)
                    if not math.isfinite(numeric) or numeric < 0.0:
                        raise ProviderExecutionError("分钟分区 volume 必须是有限非负数")
                previous_code = code
                previous_dt_us = dt_us
                observed_rows += 1
            if names == columns:
                yield batch
            else:
                yield batch.select(list(columns))
        finish_day()
        if observed_rows == 0:
            raise ProviderExecutionError("分钟分区在批准范围内没有可消费行")
        if (
            self.expected_instrument_days is not None
            and observed_days != self.expected_instrument_days
        ):
            missing = sorted(self.expected_instrument_days - observed_days)[:10]
            unexpected = sorted(observed_days - self.expected_instrument_days)[:10]
            raise ProviderExecutionError(
                f"分钟分区标的日与日线键不一致；缺失={missing}，额外={unexpected}"
            )
        self._receipt = MinutePartitionQualityReceipt(
            self.partition.reference.partition_key,
            observed_rows,
            len(observed_days),
            tuple(sorted(set(("code", "dt", *columns)))),
        )
        self._completed = True

    def _validate_session(self, code: str, dt_us: int) -> date:
        if self.asset_class in _STOCK_LIKE_ASSETS:
            return _validate_stock_like_completed_bar(dt_us)
        instrument = self.session_instruments.get(code)
        if instrument is None or self.session_bundle is None:
            raise ProviderExecutionError(f"期货 session policy 未覆盖标的: {code}")
        # raw dt 是来源墙钟整数，不是UTC Unix时点。
        instant = _as_utc_datetime(datetime(1970, 1, 1) + timedelta(microseconds=dt_us),
                                   self.source_timezone, "minute bar_end")
        try:
            session = self._session_resolver.resolve_completed_bar_end(
                instrument,
                instant,
                policy_revision=None if self.session_bundle.contract_version == PROJECT_SESSION_BUNDLE_VERSION else 1,
            )
        except SessionCalendarError as exc:
            raise ProviderExecutionError("分钟 bar_end 不属于批准的期货 session") from exc
        if (self.session_bundle.contract_version != PROJECT_SESSION_BUNDLE_VERSION
                and session.policy_id != self.session_policy_ref):
            raise ProviderExecutionError("分钟 bar_end 命中的 session policy 不一致")
        return session.trading_date


def _validate_stock_like_completed_bar(dt_us: int) -> date:
    day_us, time_us = divmod(dt_us, _DAY_MICROSECONDS)
    in_morning = (
        _STOCK_MORNING_FIRST <= time_us <= _STOCK_MORNING_LAST
        and (time_us - _STOCK_MORNING_FIRST) % _MINUTE_MICROSECONDS == 0
    )
    in_afternoon = (
        _STOCK_AFTERNOON_FIRST <= time_us <= _STOCK_AFTERNOON_LAST
        and (time_us - _STOCK_AFTERNOON_FIRST) % _MINUTE_MICROSECONDS == 0
    )
    if not (in_morning or in_afternoon):
        raise ProviderExecutionError("分钟 bar_end 不属于批准的股票 completed-bar 时段")
    return (datetime(1970, 1, 1) + timedelta(days=day_us)).date()


__all__ = [
    "MinutePartitionQualityReceipt",
    "VerifiedMinutePartitionStream",
]
