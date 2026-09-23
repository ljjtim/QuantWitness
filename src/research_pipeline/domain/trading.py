"""通用标的、组合目标与订单意图合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import math
from numbers import Integral, Real
from typing import Mapping

from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    require_canonical_asset_class,
    require_instrument_type,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.trading_capabilities import (
    EXECUTION_CAPABILITY_MATRIX,
    TradingCapabilityError,
    require_declared_trading_capability as _require_declared_trading_capability,
)

from .models import DomainContractError, InstrumentId, TargetPortfolio
from .time import require_aware_datetime


INSTRUMENT_KEY_VERSION = "research-instrument-key-v1"
PORTFOLIO_TARGET_VERSION = "research-portfolio-target-v1"
ORDER_INTENT_VERSION = "research-order-intent-v1"
TRADING_RULE_BINDING_VERSION = "research-trading-rule-binding-v1"

_POSITION_EFFECTS = {"auto", "open", "close", "close_today", "close_yesterday"}


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

    @property
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

    @property
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


@dataclass(frozen=True)
class TradingRuleBinding:
    instrument_hash: str
    rule_snapshot_hash: str
    available_at: datetime
    corporate_action_snapshot_hash: str | None = None
    multiplier_rule_hash: str | None = None
    fee_rule_hash: str | None = None
    margin_rule_hash: str | None = None
    settlement_rule_hash: str | None = None
    contract_version: str = TRADING_RULE_BINDING_VERSION

    def __post_init__(self) -> None:
        _hash(self.instrument_hash, "instrument_hash")
        _hash(self.rule_snapshot_hash, "rule_snapshot_hash")
        require_aware_datetime(self.available_at, "rule available_at")
        for field in (
            "corporate_action_snapshot_hash", "multiplier_rule_hash", "fee_rule_hash",
            "margin_rule_hash", "settlement_rule_hash",
        ):
            value = getattr(self, field)
            if value is not None:
                _hash(value, field)
        if self.contract_version != TRADING_RULE_BINDING_VERSION:
            raise DomainContractError("TradingRuleBinding 版本不受支持")

    def require_for(self, instrument: InstrumentKey, *, application_time: datetime) -> None:
        require_aware_datetime(application_time, "application_time")
        if self.instrument_hash != instrument.instrument_hash:
            raise DomainContractError("规则快照与标的身份不一致")
        if self.available_at > application_time:
            raise DomainContractError("规则快照在应用时点尚不可见")
        if instrument.asset_class in {"cn_stock", "cn_etf"}:
            if self.corporate_action_snapshot_hash is None:
                raise DomainContractError("现货目标缺少公司行动/除权规则快照")
        else:
            required = (
                self.multiplier_rule_hash,
                self.fee_rule_hash,
                self.margin_rule_hash,
                self.settlement_rule_hash,
            )
            if any(value is None for value in required):
                raise DomainContractError("期货目标缺少乘数、费用、保证金或结算规则快照")

    @property
    def binding_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_hash": self.instrument_hash,
            "rule_snapshot_hash": self.rule_snapshot_hash,
            "available_at": self.available_at.isoformat(),
            "corporate_action_snapshot_hash": self.corporate_action_snapshot_hash,
            "multiplier_rule_hash": self.multiplier_rule_hash,
            "fee_rule_hash": self.fee_rule_hash,
            "margin_rule_hash": self.margin_rule_hash,
            "settlement_rule_hash": self.settlement_rule_hash,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "TradingRuleBinding":
        _exact_keys(
            payload,
            {
                "instrument_hash", "rule_snapshot_hash", "available_at",
                "corporate_action_snapshot_hash", "multiplier_rule_hash",
                "fee_rule_hash", "margin_rule_hash", "settlement_rule_hash",
                "contract_version",
            },
            "TradingRuleBinding",
        )
        available_at = payload["available_at"]
        if not isinstance(available_at, str):
            raise DomainContractError("TradingRuleBinding available_at 必须是字符串")
        return cls(
            instrument_hash=str(payload["instrument_hash"]),
            rule_snapshot_hash=str(payload["rule_snapshot_hash"]),
            available_at=datetime.fromisoformat(available_at),
            corporate_action_snapshot_hash=_optional_text(
                payload["corporate_action_snapshot_hash"],
                "corporate_action_snapshot_hash",
            ),
            multiplier_rule_hash=_optional_text(payload["multiplier_rule_hash"], "multiplier_rule_hash"),
            fee_rule_hash=_optional_text(payload["fee_rule_hash"], "fee_rule_hash"),
            margin_rule_hash=_optional_text(payload["margin_rule_hash"], "margin_rule_hash"),
            settlement_rule_hash=_optional_text(payload["settlement_rule_hash"], "settlement_rule_hash"),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class OrderIntent:
    instrument: InstrumentKey
    side: str
    quantity: int
    position_effect: str
    decision_time: datetime
    order_time: datetime
    portfolio_target_hash: str
    market_data_artifact_hash: str
    rule_binding: TradingRuleBinding
    source_hashes: tuple[str, ...]
    contract_version: str = ORDER_INTENT_VERSION

    def __post_init__(self) -> None:
        if self.side not in {"buy", "sell"}:
            raise DomainContractError("OrderIntent side 只能是 buy/sell")
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, Integral) or self.quantity < 1:
            raise DomainContractError("OrderIntent quantity 必须是正整数")
        if self.position_effect not in _POSITION_EFFECTS:
            raise DomainContractError("position_effect 不受支持")
        if self.instrument.asset_class == "cn_future":
            if self.position_effect == "auto":
                raise DomainContractError("期货 OrderIntent 必须显式声明开平仓")
        elif self.position_effect != "auto":
            raise DomainContractError("股票和 ETF position_effect 只能是 auto")
        require_aware_datetime(self.decision_time, "decision_time")
        require_aware_datetime(self.order_time, "order_time")
        if self.order_time < self.decision_time:
            raise DomainContractError("OrderIntent order_time 早于 decision_time")
        _hash(self.portfolio_target_hash, "portfolio_target_hash")
        _hash(self.market_data_artifact_hash, "market_data_artifact_hash")
        self.rule_binding.require_for(self.instrument, application_time=self.order_time)
        if not self.source_hashes or self.source_hashes != tuple(sorted(set(self.source_hashes))):
            raise DomainContractError("OrderIntent source_hashes 必须非空、唯一并排序")
        for value in self.source_hashes:
            _hash(value, "source_hash")
        if self.contract_version != ORDER_INTENT_VERSION:
            raise DomainContractError("OrderIntent 版本不受支持")

    @property
    def intent_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument": self.instrument.to_dict(),
            "side": self.side,
            "quantity": int(self.quantity),
            "position_effect": self.position_effect,
            "decision_time": self.decision_time.isoformat(),
            "order_time": self.order_time.isoformat(),
            "portfolio_target_hash": self.portfolio_target_hash,
            "market_data_artifact_hash": self.market_data_artifact_hash,
            "rule_binding": self.rule_binding.to_dict(),
            "source_hashes": list(self.source_hashes),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OrderIntent":
        _exact_keys(
            payload,
            {
                "instrument", "side", "quantity", "position_effect", "decision_time",
                "order_time", "portfolio_target_hash", "market_data_artifact_hash",
                "rule_binding", "source_hashes", "contract_version",
            },
            "OrderIntent",
        )
        instrument = payload["instrument"]
        binding = payload["rule_binding"]
        source_hashes = payload["source_hashes"]
        if not isinstance(instrument, Mapping) or not isinstance(binding, Mapping):
            raise DomainContractError("OrderIntent instrument/rule_binding 必须是对象")
        if not isinstance(source_hashes, list):
            raise DomainContractError("OrderIntent source_hashes 必须是列表")
        decision_time = payload["decision_time"]
        order_time = payload["order_time"]
        if not isinstance(decision_time, str) or not isinstance(order_time, str):
            raise DomainContractError("OrderIntent 时间必须是字符串")
        return cls(
            instrument=InstrumentKey.from_dict(instrument),
            side=str(payload["side"]),
            quantity=_positive_integer(payload["quantity"], "quantity"),
            position_effect=str(payload["position_effect"]),
            decision_time=datetime.fromisoformat(decision_time),
            order_time=datetime.fromisoformat(order_time),
            portfolio_target_hash=str(payload["portfolio_target_hash"]),
            market_data_artifact_hash=str(payload["market_data_artifact_hash"]),
            rule_binding=TradingRuleBinding.from_dict(binding),
            source_hashes=tuple(str(item) for item in source_hashes),
            contract_version=str(payload["contract_version"]),
        )


# 代码中显式带 V1 的名字仅用于迁移期源码兼容；序列化版本仍由 contract_version 决定。
OrderIntentV1 = OrderIntent


def instrument_key_from_legacy(value: InstrumentId, *, venue: str | None = None) -> InstrumentKey:
    """只读迁移旧 InstrumentId；新写路径使用 InstrumentKey。"""
    resolved_venue = venue or _venue_from_code(value.code)
    contract_kind = {
        "stock": "stock",
        "etf": "etf",
        "future": "future_contract",
    }[value.instrument_type]
    return InstrumentKey(
        value.code,
        value.market,
        resolved_venue,
        value.currency,
        contract_kind,
    )


def portfolio_target_from_legacy(
    value: TargetPortfolio,
    *,
    venue_by_code: Mapping[str, str] | None = None,
) -> PortfolioTarget:
    """把旧只做多权重目标迁移为显式现金和杠杆合同。"""
    venues = {} if venue_by_code is None else venue_by_code
    entries = tuple(
        PortfolioTargetEntry(
            instrument_key_from_legacy(
                item.instrument,
                venue=venues.get(item.instrument.code),
            ),
            "weight",
            item.weight,
        )
        for item in value.targets
    )
    return PortfolioTarget(
        decision_time=value.decision_time,
        target_type="weight",
        entries=entries,
        base_currency="CNY",
        cash_weight=float(
            Decimal("1") - sum(
                (Decimal(str(item.weight)) for item in value.targets),
                Decimal("0"),
            )
        ),
        short_allowed=False,
        leverage_limit=1.0,
        source_hashes=value.source_signal_hashes,
    )


def require_declared_trading_capability(
    *,
    backend_id: str,
    target_type: str,
    asset_classes: tuple[str, ...],
    base_currency: str,
    short_allowed: object,
    leverage_limit: object,
) -> Mapping[str, object]:
    """校验 plan 中尚未物化的 target/asset/backend 能力声明。"""
    try:
        return _require_declared_trading_capability(
            backend_id=backend_id,
            target_type=target_type,
            asset_classes=asset_classes,
            base_currency=base_currency,
            short_allowed=short_allowed,
            leverage_limit=leverage_limit,
        )
    except TradingCapabilityError as exc:
        raise DomainContractError(str(exc)) from exc


def _venue_from_code(code: str) -> str:
    suffix = code.rpartition(".")[2]
    if not suffix:
        raise DomainContractError("旧 InstrumentId 缺少可推导 venue 的代码后缀")
    return suffix


def _exact_keys(payload: Mapping[str, object], expected: set[str], name: str) -> None:
    if set(payload) != expected:
        raise DomainContractError(f"{name} 字段集合无效")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainContractError(f"{field} 必须是非空字符串")
    return value


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(float(value)):
        raise DomainContractError(f"{field} 必须是有限数")
    return float(value)


def _hash(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise DomainContractError(f"{field} 必须是小写 sha256")
    return value


def _optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DomainContractError(f"{field} 必须是字符串或 null")
    return value


def _optional_finite(value: object, field: str) -> float | None:
    if value is None:
        return None
    return _finite(value, field)


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 1:
        raise DomainContractError(f"{field} 必须是正整数")
    return int(value)


__all__ = [
    "INSTRUMENT_KEY_VERSION",
    "ORDER_INTENT_VERSION",
    "PORTFOLIO_TARGET_VERSION",
    "TRADING_RULE_BINDING_VERSION",
    "EXECUTION_CAPABILITY_MATRIX",
    "InstrumentKey",
    "OrderIntent",
    "OrderIntentV1",
    "PortfolioTarget",
    "PortfolioTargetEntry",
    "TradingRuleBinding",
    "instrument_key_from_legacy",
    "portfolio_target_from_legacy",
    "require_declared_trading_capability",
]
