"""通用标的、组合目标与订单意图合同。"""

from __future__ import annotations


from dataclasses import dataclass
from functools import cached_property, partial
from datetime import datetime
import math
from numbers import Integral, Real
from typing import Mapping

from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    require_canonical_asset_class,
    require_instrument_type,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import DomainContractError
from research_pipeline.platform.validation import (
    require_sha256, require_text,
)

from .time import require_aware_datetime


INSTRUMENT_KEY_VERSION = "research-instrument-key-v1"
PORTFOLIO_TARGET_VERSION = "research-portfolio-target-v1"


@dataclass(frozen=True)
class InstrumentKey:
    instrument_id: str
    asset_class: str
    venue: str
    currency: str
    contract_kind: str
    contract_version: str = INSTRUMENT_KEY_VERSION

    def __post_init__(self) -> None:
        for value, field in (
            (self.instrument_id, "instrument_id"),
            (self.asset_class, "asset_class"),
            (self.venue, "venue"),
            (self.contract_kind, "contract_kind"),
        ):
            _text(value, field)
        try:
            require_canonical_asset_class(self.asset_class)
            require_instrument_type(self.asset_class, self.contract_kind)
        except AssetTaxonomyError as exc:
            raise DomainContractError(str(exc)) from exc
        if not isinstance(self.currency, str) or len(self.currency) != 3 or not self.currency.isupper():
            raise DomainContractError("currency 必须是三位大写币种代码")
        if self.contract_version != INSTRUMENT_KEY_VERSION:
            raise DomainContractError("InstrumentKey 版本不受支持")

    @cached_property
    def instrument_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        return {
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "venue": self.venue,
            "currency": self.currency,
            "contract_kind": self.contract_kind,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "InstrumentKey":
        _exact_keys(
            payload,
            {
                "instrument_id", "asset_class", "venue", "currency",
                "contract_kind", "contract_version",
            },
            "InstrumentKey",
        )
        values = {}
        for key, value in payload.items():
            if not isinstance(value, str):
                raise DomainContractError(f"InstrumentKey {key} 必须是字符串")
            values[key] = value
        return cls(**values)


@dataclass(frozen=True)
class PortfolioTargetEntry:
    instrument: InstrumentKey
    target_type: str
    value: float | int

    def __post_init__(self) -> None:
        if self.target_type not in {"weight", "quantity", "notional"}:
            raise DomainContractError("target_type 不受支持")
        if self.target_type == "quantity":
            if isinstance(self.value, bool) or not isinstance(self.value, Integral):
                raise DomainContractError("quantity target 必须是整数")
        else:
            _finite(self.value, "target value")

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument": self.instrument.to_dict(),
            "target_type": self.target_type,
            "value": int(self.value) if self.target_type == "quantity" else float(self.value),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PortfolioTargetEntry":
        _exact_keys(payload, {"instrument", "target_type", "value"}, "PortfolioTargetEntry")
        instrument = payload["instrument"]
        if not isinstance(instrument, Mapping):
            raise DomainContractError("PortfolioTargetEntry instrument 必须是对象")
        target_type = str(payload["target_type"])
        raw_value = payload["value"]
        value: float | int
        if target_type == "quantity":
            if isinstance(raw_value, bool) or not isinstance(raw_value, Integral):
                raise DomainContractError("quantity target 必须是整数")
            value = int(raw_value)
        else:
            value = _finite(raw_value, "target value")
        return cls(
            InstrumentKey.from_dict(instrument),
            target_type,
            value,
        )


@dataclass(frozen=True)
class PortfolioTarget:
    decision_time: datetime
    target_type: str
    entries: tuple[PortfolioTargetEntry, ...]
    base_currency: str
    cash_weight: float | None
    short_allowed: bool
    leverage_limit: float
    source_hashes: tuple[str, ...]
    contract_version: str = PORTFOLIO_TARGET_VERSION

    def __post_init__(self) -> None:
        require_aware_datetime(self.decision_time, "decision_time")
        if self.target_type not in {"weight", "quantity", "notional"}:
            raise DomainContractError("PortfolioTarget target_type 不受支持")
        if self.base_currency != "CNY":
            raise DomainContractError("当前 PortfolioTarget base_currency 只支持 CNY")
        if not isinstance(self.entries, tuple):
            raise DomainContractError("PortfolioTarget entries 必须是 tuple")
        identities = tuple(
            (item.instrument.asset_class, item.instrument.venue, item.instrument.instrument_id)
            for item in self.entries
        )
        if identities != tuple(sorted(set(identities))):
            raise DomainContractError("PortfolioTarget entries 必须规范排序且唯一")
        if any(item.target_type != self.target_type for item in self.entries):
            raise DomainContractError("PortfolioTarget 不允许混合 target_type")
        if any(item.instrument.currency != self.base_currency for item in self.entries):
            raise DomainContractError("没有 FX model 时标的币种必须等于 base_currency")
        leverage = _finite(self.leverage_limit, "leverage_limit")
        if leverage < 1.0:
            raise DomainContractError("leverage_limit 不能小于 1")
        values = tuple(float(item.value) for item in self.entries)
        if not self.short_allowed and any(value < 0.0 for value in values):
            raise DomainContractError("short_allowed=false 时不能声明负目标")
        if self.target_type == "weight":
            if self.cash_weight is None:
                raise DomainContractError("weight target 必须显式声明 cash_weight")
            cash = _finite(self.cash_weight, "cash_weight")
            if not math.isclose(sum(values) + cash, 1.0, abs_tol=1e-12):
                raise DomainContractError("weight target 与 cash_weight 的净值必须合计为 1")
            if sum(abs(value) for value in values) > leverage + 1e-12:
                raise DomainContractError("weight target gross exposure 超过 leverage_limit")
        elif self.cash_weight is not None:
            raise DomainContractError("quantity/notional target 不能声明 cash_weight")
        if not isinstance(self.source_hashes, tuple) or not self.source_hashes:
            raise DomainContractError("PortfolioTarget 必须保留非空来源 lineage")
        if self.source_hashes != tuple(sorted(set(self.source_hashes))):
            raise DomainContractError("PortfolioTarget source_hashes 必须唯一并排序")
        for value in self.source_hashes:
            _hash(value, "source_hash")
        if self.contract_version != PORTFOLIO_TARGET_VERSION:
            raise DomainContractError("PortfolioTarget 版本不受支持")

    @cached_property
    def target_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @property
    def gross_exposure(self) -> float:
        return sum(abs(float(item.value)) for item in self.entries)

    @property
    def net_exposure(self) -> float:
        return sum(float(item.value) for item in self.entries)

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_time": self.decision_time.isoformat(),
            "target_type": self.target_type,
            "entries": [item.to_dict() for item in self.entries],
            "base_currency": self.base_currency,
            "cash_weight": self.cash_weight,
            "short_allowed": self.short_allowed,
            "leverage_limit": float(self.leverage_limit),
            "source_hashes": list(self.source_hashes),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PortfolioTarget":
        _exact_keys(
            payload,
            {
                "decision_time", "target_type", "entries", "base_currency",
                "cash_weight", "short_allowed", "leverage_limit", "source_hashes",
                "contract_version",
            },
            "PortfolioTarget",
        )
        entries = payload["entries"]
        source_hashes = payload["source_hashes"]
        if not isinstance(entries, list) or not all(isinstance(item, Mapping) for item in entries):
            raise DomainContractError("PortfolioTarget entries 必须是对象列表")
        if not isinstance(source_hashes, list):
            raise DomainContractError("PortfolioTarget source_hashes 必须是列表")
        short_allowed = payload["short_allowed"]
        if type(short_allowed) is not bool:
            raise DomainContractError("PortfolioTarget short_allowed 必须是布尔值")
        decision_time = payload["decision_time"]
        if not isinstance(decision_time, str):
            raise DomainContractError("PortfolioTarget decision_time 必须是字符串")
        return cls(
            decision_time=datetime.fromisoformat(decision_time),
            target_type=str(payload["target_type"]),
            entries=tuple(PortfolioTargetEntry.from_dict(item) for item in entries),
            base_currency=str(payload["base_currency"]),
            cash_weight=_optional_finite(payload["cash_weight"], "cash_weight"),
            short_allowed=short_allowed,
            leverage_limit=_finite(payload["leverage_limit"], "leverage_limit"),
            source_hashes=tuple(str(item) for item in source_hashes),
            contract_version=str(payload["contract_version"]),
        )


def _exact_keys(payload: Mapping[str, object], expected: set[str], name: str) -> None:
    if set(payload) != expected:
        raise DomainContractError(f"{name} 字段集合无效")


_text = partial(require_text, error=DomainContractError)


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(float(value)):
        raise DomainContractError(f"{field} 必须是有限数")
    return float(value)


_hash = partial(require_sha256, error=DomainContractError, requirement="必须是小写 sha256")


def _optional_finite(value: object, field: str) -> float | None:
    if value is None:
        return None
    return _finite(value, field)


__all__ = [
    "INSTRUMENT_KEY_VERSION",
    "PORTFOLIO_TARGET_VERSION",
    "InstrumentKey",
    "PortfolioTarget",
    "PortfolioTargetEntry",
]
