"""执行使用的不可变行情观察与分钟参与策略。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import cached_property
from zoneinfo import ZoneInfo

from research_pipeline.domain import Price
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError, require_canonical_asset_class, require_frequency,
    require_simulation_entry,
)

from .orders import SimulationContractError

INTRADAY_EXECUTION_POLICY_VERSION = "intraday-execution-policy-v1"
_ZONE = ZoneInfo("Asia/Shanghai")
_SUPPORTED_INTERVALS = (1, 5, 15, 30, 60, 120)


def _require_minute_asset(asset_class: str, *, tradable: bool) -> None:
    try:
        require_canonical_asset_class(asset_class)
        require_frequency(asset_class, "minute")
        if tradable:
            require_simulation_entry(asset_class, "minute_event")
    except AssetTaxonomyError as exc:
        raise SimulationContractError(str(exc)) from exc


@dataclass(frozen=True)
class IntradayExecutionPolicy:
    model_id: str = "next_bar_participation_v1"
    model_version: str = "1.0.0"
    participation_ppm: int = 100_000
    claim_ceiling: str = "bar_level_research_only"
    contract_version: str = INTRADAY_EXECUTION_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != INTRADAY_EXECUTION_POLICY_VERSION:
            raise SimulationContractError("分钟执行 policy version 不受支持")
        if self.model_id != "next_bar_participation_v1" or self.model_version != "1.0.0":
            raise SimulationContractError("首版只支持下一完成 bar 参与模型")
        if (
            type(self.participation_ppm) is not int
            or not 1 <= self.participation_ppm <= 1_000_000
        ):
            raise SimulationContractError("分钟参与率必须在 (0,100%] 内")
        if self.claim_ceiling != "bar_level_research_only":
            raise SimulationContractError("bar-level 仿真不能声明 Tick/LOB 能力")

    @property
    def policy_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "participation_ppm": self.participation_ppm,
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class MinuteExecutionBar:
    instrument_id: str
    asset_class: str
    trading_date: date
    session_id: str
    bar_start: datetime
    bar_end: datetime
    available_time: datetime
    receipt_time: datetime
    interval_minutes: int
    open_units: int
    high_units: int
    low_units: int
    close_units: int
    avg_units: int | None
    volume: int
    open_interest: int | None
    completed: bool
    quality_status: str
    source_snapshot_hash: str
    source_sequence: int

    def __post_init__(self) -> None:
        _require_minute_asset(self.asset_class, tradable=False)
        if not self.instrument_id.strip() or not self.session_id.strip():
            raise SimulationContractError("分钟执行 bar 身份无效")
        for field in ("bar_start", "bar_end", "available_time", "receipt_time"):
            value = getattr(self, field)
            require_aware_datetime(value, field)
            if value.utcoffset() != _ZONE.utcoffset(value):
                raise SimulationContractError("分钟执行 bar 必须使用 Asia/Shanghai 时区")
        if not self.bar_start < self.bar_end <= self.available_time <= self.receipt_time:
            raise SimulationContractError("分钟执行 bar 时间顺序无效")
        if self.interval_minutes not in _SUPPORTED_INTERVALS:
            raise SimulationContractError("分钟执行 bar 周期不受支持")
        prices = (self.open_units, self.high_units, self.low_units, self.close_units)
        if any(type(item) is not int or item <= 0 for item in prices):
            raise SimulationContractError("分钟执行价格必须是正整数定点值")
        if self.low_units > min(prices) or self.high_units < max(prices):
            raise SimulationContractError("分钟执行 OHLC 上下界不一致")
        if self.avg_units is not None and (
            type(self.avg_units) is not int or self.avg_units <= 0
        ):
            raise SimulationContractError("分钟执行 avg 必须是正整数定点值")
        if type(self.volume) is not int or self.volume < 0 or self.source_sequence < 0:
            raise SimulationContractError("分钟执行 volume/source sequence 无效")
        if self.asset_class == "cn_future":
            if type(self.open_interest) is not int or self.open_interest < 0:
                raise SimulationContractError("期货分钟执行必须提供非负 open_interest")
        elif self.open_interest is not None:
            raise SimulationContractError("非期货分钟执行不得提供 open_interest")
        _hash(self.source_snapshot_hash, "source_snapshot_hash")

    @cached_property
    def bar_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "trading_date": self.trading_date.isoformat(),
            "session_id": self.session_id,
            "bar_start": self.bar_start.isoformat(),
            "bar_end": self.bar_end.isoformat(),
            "available_time": self.available_time.isoformat(),
            "receipt_time": self.receipt_time.isoformat(),
            "interval_minutes": self.interval_minutes,
            "open_units": self.open_units,
            "high_units": self.high_units,
            "low_units": self.low_units,
            "close_units": self.close_units,
            "avg_units": self.avg_units,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "completed": self.completed,
            "quality_status": self.quality_status,
            "source_snapshot_hash": self.source_snapshot_hash,
            "source_sequence": self.source_sequence,
        }


def _hash(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise SimulationContractError(f"{field} 必须是 sha256")
    return value


@dataclass(frozen=True)
class OpeningSnapshot:
    instrument_hash: str
    open_price: Price
    high_limit: Price | None
    low_limit: Price | None
    paused: bool
    visible_capacity: int
    available_time: datetime
    adjustment: str = "none"

    def __post_init__(self) -> None:
        require_aware_datetime(self.available_time, "available_time")
        if self.visible_capacity < 0:
            raise SimulationContractError("visible_capacity 不能为负")
        if self.adjustment != "none":
            raise SimulationContractError("成交必须使用未复权价格")
        if (self.high_limit is None) != (self.low_limit is None):
            raise SimulationContractError("无限价快照的上下界必须同时为空")
        prices = (self.open_price,) if self.high_limit is None else (self.open_price, self.high_limit, self.low_limit)
        if any(not isinstance(item, Price) for item in prices):
            raise SimulationContractError("开盘和涨跌停快照价格类型无效")
        identities = {(item.scale, item.currency) for item in prices}
        if len(identities) != 1:
            raise SimulationContractError("开盘和涨跌停价格精度/币种不一致")
