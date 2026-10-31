"""证券身份、研究信号和目标持仓的不可变领域对象。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from numbers import Real

from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    require_canonical_asset_class,
    require_instrument_type,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError

from .time import require_aware_datetime


class DomainContractError(MainlineError):
    """基础金融身份或研究意图不满足合同。"""

    error_code = "domain_contract_invalid"


@dataclass(frozen=True)
class InstrumentId:
    code: str
    market: str
    instrument_type: str
    currency: str = "CNY"

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code.strip():
            raise DomainContractError("instrument code 必须是非空字符串")
        try:
            require_canonical_asset_class(self.market, field="instrument market")
            require_instrument_type(self.market, self.instrument_type)
        except AssetTaxonomyError as exc:
            raise DomainContractError(str(exc)) from exc
        if not isinstance(self.currency, str) or not self.currency.strip():
            raise DomainContractError("currency 必须是非空字符串")

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "market": self.market,
            "instrument_type": self.instrument_type,
            "currency": self.currency,
        }


@dataclass(frozen=True)
class Signal:
    instrument: InstrumentId
    decision_time: datetime
    value: float
    source_snapshot_hash: str

    def __post_init__(self) -> None:
        require_aware_datetime(self.decision_time, "decision_time")
        _require_finite(self.value, "signal value")
        _require_hash(self.source_snapshot_hash, "source_snapshot_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument": self.instrument.to_dict(),
            "decision_time": self.decision_time.isoformat(),
            "value": float(self.value),
            "source_snapshot_hash": self.source_snapshot_hash,
        }

    @property
    def signal_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


@dataclass(frozen=True)
class TargetWeight:
    instrument: InstrumentId
    weight: float

    def __post_init__(self) -> None:
        value = _require_finite(self.weight, "target weight")
        if value < 0 or value > 1:
            raise DomainContractError("target weight 必须在 [0, 1] 内")

    def to_dict(self) -> dict[str, object]:
        return {"instrument": self.instrument.to_dict(), "weight": float(self.weight)}


@dataclass(frozen=True)
class TargetPortfolio:
    decision_time: datetime
    targets: tuple[TargetWeight, ...]
    source_signal_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        require_aware_datetime(self.decision_time, "decision_time")
        if not isinstance(self.targets, tuple) or not self.targets:
            raise DomainContractError("targets 必须是非空 tuple")
        identities = tuple(
            (target.instrument.market, target.instrument.code) for target in self.targets
        )
        if identities != tuple(sorted(set(identities))):
            raise DomainContractError("targets 必须按 market/code 排序且不能重复")
        if sum(float(target.weight) for target in self.targets) > 1.0 + 1e-12:
            raise DomainContractError("target weight 总和不能超过 1")
        if (
            not isinstance(self.source_signal_hashes, tuple)
            or not self.source_signal_hashes
            or self.source_signal_hashes != tuple(sorted(set(self.source_signal_hashes)))
        ):
            raise DomainContractError("source_signal_hashes 必须排序、唯一且非空")
        for value in self.source_signal_hashes:
            _require_hash(value, "source_signal_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_time": self.decision_time.isoformat(),
            "targets": [target.to_dict() for target in self.targets],
            "source_signal_hashes": list(self.source_signal_hashes),
        }

    @property
    def portfolio_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


def _require_hash(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise DomainContractError(f"{field} 必须是 sha256 小写十六进制摘要")
    return value


def _require_finite(value: object, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, Real)
        or not math.isfinite(float(value))
    ):
        raise DomainContractError(f"{field} 必须是有限数")
    return float(value)


__all__ = [
    "DomainContractError",
    "InstrumentId",
    "Signal",
    "TargetPortfolio",
    "TargetWeight",
]
