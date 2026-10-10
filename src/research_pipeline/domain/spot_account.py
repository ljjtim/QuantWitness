"""现货期初账户、税权和有来源证券承接的可封存合同。"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from fractions import Fraction
from types import UnionType
from typing import Mapping, Union, get_args, get_origin, get_type_hints

from .models import DomainContractError
from .time import require_aware_datetime


def _encode(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value):
        return {item.name: _encode(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, tuple):
        return [_encode(item) for item in value]
    return value


def _decode(annotation, value):
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        if value is None and type(None) in get_args(annotation):
            return None
        return _decode(next(item for item in get_args(annotation) if item is not type(None)), value)
    if origin is tuple:
        return tuple(_decode(get_args(annotation)[0], item) for item in value)
    if annotation in (date, datetime):
        return annotation.fromisoformat(value)
    if isinstance(annotation, type) and issubclass(annotation, AccountFact):
        return annotation.from_dict(value)
    if annotation in (str, int, bool) and type(value) is not annotation:
        raise DomainContractError("账户载荷字段类型不符")
    return value


class AccountFact:
    """序列化完整事实字段，日期与嵌套事实恢复为原合同类型。"""

    def to_dict(self) -> dict[str, object]:
        return _encode(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]):
        if set(payload) != {item.name for item in fields(cls)}:
            raise DomainContractError(f"{cls.__name__} 账户载荷字段不完整或含未知字段")
        hints = get_type_hints(cls)
        try:
            return cls(**{key: _decode(hints[key], value) for key, value in payload.items()})
        except (ValueError, TypeError) as exc:
            raise DomainContractError(f"{cls.__name__} 账户载荷无效") from exc


def _identity(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainContractError(f"{name} 不能为空")


def _amount(value: int, name: str, *, positive: bool = False) -> None:
    if type(value) is not int or value < int(positive):
        raise DomainContractError(f"{name} 必须是{'正' if positive else '非负'}整数")


@dataclass(frozen=True)
class AccountSource(AccountFact):
    source_ref: str
    available_at: datetime

    def __post_init__(self):
        _identity(self.source_ref, "source_ref")
        require_aware_datetime(self.available_at, "available_at")

    def require_visible(self, as_of: datetime) -> None:
        require_aware_datetime(as_of, "as_of")
        if self.available_at > as_of:
            raise DomainContractError("账户事实在该时点尚不可见")


@dataclass(frozen=True)
class OpeningCash(AccountFact):
    available_units: int
    restricted_units: int
    unsettled_units: int
    source: AccountSource
    restriction_reason: str | None = None

    def __post_init__(self):
        for name in ("available_units", "restricted_units", "unsettled_units"):
            _amount(getattr(self, name), name)
        if self.restricted_units and not self.restriction_reason:
            raise DomainContractError("期初受限现金必须有法律或业务限制说明")


@dataclass(frozen=True)
class SpotAcquisitionLot(AccountFact):
    lot_id: str
    instrument_hash: str
    quantity: int
    accounting_cost_units: int
    acquired_on: date | None
    sellable_at: datetime
    acquisition_method: str
    security_class: str
    source: AccountSource
    predecessor_lot_id: str | None = None
    predecessor_conversion_id: str | None = None

    def __post_init__(self):
        for name in ("lot_id", "instrument_hash", "acquisition_method", "security_class"):
            _identity(getattr(self, name), name)
        _amount(self.quantity, "quantity", positive=True)
        _amount(self.accounting_cost_units, "accounting_cost_units")
        require_aware_datetime(self.sellable_at, "sellable_at")
        if self.acquired_on is not None and self.acquired_on > self.sellable_at.date():
            raise DomainContractError("可卖日不能早于取得日")


@dataclass(frozen=True)
class OpeningCashClaim(AccountFact):
    claim_id: str
    cash_units: int
    due_at: datetime
    source: AccountSource

    def __post_init__(self):
        _identity(self.claim_id, "claim_id")
        _amount(self.cash_units, "cash_units", positive=True)
        require_aware_datetime(self.due_at, "due_at")


@dataclass(frozen=True)
class DividendEntitlement(AccountFact):
    entitlement_id: str
    dividend_id: str
    lot_id: str
    instrument_hash: str
    registered_quantity: int
    remaining_quantity: int
    taxable_per_share_numerator: int
    taxable_per_share_denominator: int
    acquired_on: date | None
    record_on: date
    tax_rule_id: str
    source: AccountSource
    prewithheld_units: int = 0
    predecessor_entitlement_id: str | None = None
    quantity_multiplier_numerator: int = 1
    quantity_multiplier_denominator: int = 1
    acquisition_method: str = "market_buy"
    security_class: str = "equity"
    registered_quantity_denominator: int = 1
    remaining_quantity_denominator: int = 1

    def __post_init__(self):
        for name in ("entitlement_id", "dividend_id", "lot_id", "instrument_hash", "tax_rule_id", "acquisition_method", "security_class"):
            _identity(getattr(self, name), name)
        _amount(self.registered_quantity, "registered_quantity", positive=True)
        _amount(self.remaining_quantity, "remaining_quantity")
        _amount(self.taxable_per_share_numerator, "taxable_per_share_numerator")
        _amount(self.taxable_per_share_denominator, "taxable_per_share_denominator", positive=True)
        _amount(self.prewithheld_units, "prewithheld_units")
        _amount(self.quantity_multiplier_numerator, "quantity_multiplier_numerator", positive=True)
        _amount(self.quantity_multiplier_denominator, "quantity_multiplier_denominator", positive=True)
        _amount(self.registered_quantity_denominator, "registered_quantity_denominator", positive=True)
        _amount(self.remaining_quantity_denominator, "remaining_quantity_denominator", positive=True)
        if self.remaining_quantity_fraction > self.registered_quantity_fraction:
            raise DomainContractError("剩余分红权利不能超过登记数量")

    @property
    def registered_quantity_fraction(self) -> Fraction:
        return Fraction(self.registered_quantity, self.registered_quantity_denominator)

    @property
    def remaining_quantity_fraction(self) -> Fraction:
        return Fraction(self.remaining_quantity, self.remaining_quantity_denominator)

    @property
    def quantity_multiplier(self) -> Fraction:
        """一股当前证券消耗的原分红权利数量。"""
        return Fraction(self.quantity_multiplier_numerator, self.quantity_multiplier_denominator)


@dataclass(frozen=True)
class HoldingPeriodRate(AccountFact):
    period_unit: str
    upper_periods: int | None
    upper_inclusive: bool
    rate_numerator: int
    rate_denominator: int

    def __post_init__(self):
        if self.period_unit not in {"month", "year"}:
            raise DomainContractError("持有期边界必须显式使用自然月或自然年")
        if self.upper_periods is not None:
            _amount(self.upper_periods, "upper_periods", positive=True)
        _amount(self.rate_numerator, "rate_numerator")
        _amount(self.rate_denominator, "rate_denominator", positive=True)
        if self.rate_numerator > self.rate_denominator:
            raise DomainContractError("税率不能大于一")


@dataclass(frozen=True)
class DividendTaxRule(AccountFact):
    rule_id: str
    investor_tax_identity: str
    security_classes: tuple[str, ...]
    acquisition_methods: tuple[str, ...]
    effective_from: date
    effective_until: date
    applicability_date: str
    rates: tuple[HoldingPeriodRate, ...]
    rounding: str
    source: AccountSource
    overwithholding_policy: str = "reject"

    def __post_init__(self):
        _identity(self.rule_id, "rule_id")
        _identity(self.investor_tax_identity, "investor_tax_identity")
        if not self.security_classes or not self.acquisition_methods:
            raise DomainContractError("税规则必须明确证券类别和取得方式")
        if self.effective_until < self.effective_from or self.applicability_date not in {"record_date", "transfer_date"}:
            raise DomainContractError("税规则适用日期无效")
        if self.rounding not in {"half_up", "floor"} or self.overwithholding_policy not in {"reject", "refund_receivable", "no_refund"}:
            raise DomainContractError("税规则须显式声明支持的舍入和多扣处理")
        if not self.rates or self.rates[-1].upper_periods is not None:
            raise DomainContractError("税率表须以无上限区间结束")
        bounds = [rate.upper_periods * (12 if rate.period_unit == "year" else 1) for rate in self.rates[:-1] if rate.upper_periods is not None]
        if len(bounds) != len(self.rates) - 1 or bounds != sorted(set(bounds)):
            raise DomainContractError("持有期边界必须严格递增")



@dataclass(frozen=True)
class OpeningPositionEntitlement(AccountFact):
    """期初未可卖权益引用已有批次；数量已计入该批次，不能再次增加资产。"""

    entitlement_id: str
    lot_id: str
    quantity: int
    due_at: datetime
    source: AccountSource

    def __post_init__(self):
        _identity(self.entitlement_id, "entitlement_id")
        _identity(self.lot_id, "lot_id")
        _amount(self.quantity, "quantity", positive=True)
        require_aware_datetime(self.due_at, "due_at")


@dataclass(frozen=True)
class SpotOpeningSnapshot(AccountFact):
    snapshot_id: str
    account_id: str
    currency: str
    started_at: datetime
    visible_cutoff: datetime
    investor_tax_identity: str
    exact_dividend_tax: bool
    cash: OpeningCash
    lots: tuple[SpotAcquisitionLot, ...]
    receivables: tuple[OpeningCashClaim, ...]
    payables: tuple[OpeningCashClaim, ...]
    dividend_entitlements: tuple[DividendEntitlement, ...]
    source: AccountSource
    tax_history_source: AccountSource | None = None
    tax_assessments: tuple[OpeningTaxAssessment, ...] = ()
    position_entitlements: tuple[OpeningPositionEntitlement, ...] = ()
    accounting_cost_method: str = "lot_cost"

    def __post_init__(self):
        for name in ("snapshot_id", "account_id", "investor_tax_identity"):
            _identity(getattr(self, name), name)
        if self.accounting_cost_method not in {"lot_cost", "average"}:
            raise DomainContractError("现货会计成本方法必须显式为批次成本或平均成本")
        if self.currency != "CNY":
            raise DomainContractError("普通现货账户当前仅支持 CNY")
        require_aware_datetime(self.started_at, "started_at")
        require_aware_datetime(self.visible_cutoff, "visible_cutoff")
        if self.visible_cutoff > self.started_at:
            raise DomainContractError("期初可见截止时间不能晚于研究起点")
        for items, key in ((self.lots, "lot_id"), (self.receivables, "claim_id"), (self.payables, "claim_id"), (self.dividend_entitlements, "entitlement_id"), (self.tax_assessments, "assessment_id"), (self.position_entitlements, "entitlement_id")):
            identities = [getattr(item, key) for item in items]
            if len(identities) != len(set(identities)):
                raise DomainContractError("期初事实业务身份重复")
        facts = (self.source, self.cash.source, *(item.source for item in self.lots + self.receivables + self.payables + self.dividend_entitlements + self.tax_assessments + self.position_entitlements))
        for fact in facts:
            fact.require_visible(self.visible_cutoff)
        lot_map = {lot.lot_id: lot for lot in self.lots}
        for lot in self.lots:
            if lot.acquired_on is not None and lot.acquired_on > self.started_at.date():
                raise DomainContractError("期初股份不能在未来取得")
        pending_by_lot: dict[str, int] = {}
        for right in self.position_entitlements:
            lot = lot_map.get(right.lot_id)
            if lot is None or lot.sellable_at != right.due_at or right.due_at <= self.started_at:
                raise DomainContractError("期初待上市权益必须对应未到可卖时点的真实批次")
            pending_by_lot[right.lot_id] = pending_by_lot.get(right.lot_id, 0) + right.quantity
            if pending_by_lot[right.lot_id] > lot.quantity:
                raise DomainContractError("期初待上市权益数量超过批次，不能重复计入资产")
        for right in self.dividend_entitlements:
            if right.record_on > self.started_at.date():
                raise DomainContractError("期初分红权利不能在未来登记")
            if right.remaining_quantity:
                lot = lot_map.get(right.lot_id)
                if lot is None or lot.instrument_hash != right.instrument_hash or (lot.acquired_on != right.acquired_on and right.predecessor_entitlement_id is None):
                    raise DomainContractError("期初未转让税权必须对应相同取得批次")
                if right.remaining_quantity_fraction > lot.quantity * right.quantity_multiplier:
                    raise DomainContractError("期初税权超过对应批次数量")
        if self.tax_history_source is not None:
            self.tax_history_source.require_visible(self.visible_cutoff)
        if self.exact_dividend_tax and self.lots:
            if self.tax_history_source is None or any(lot.acquired_on is None for lot in self.lots):
                raise DomainContractError("精确税务期初必须提供取得日和历史分红权利来源")
        if self.exact_dividend_tax and any(item.acquired_on is None for item in self.dividend_entitlements):
            raise DomainContractError("精确税务权利缺少取得日")


@dataclass(frozen=True)
class OpeningTaxAssessment(AccountFact):
    assessment_id: str
    assessed_units: int
    collected_units: int
    assessed_at: datetime
    due_at: datetime
    source: AccountSource

    def __post_init__(self):
        _identity(self.assessment_id, "assessment_id")
        _amount(self.assessed_units, "assessed_units")
        _amount(self.collected_units, "collected_units")
        require_aware_datetime(self.assessed_at, "assessed_at")
        require_aware_datetime(self.due_at, "due_at")
        if self.collected_units > self.assessed_units:
            raise DomainContractError("期初已扣税款不能超过核定税额")


@dataclass(frozen=True)
class SpotOpeningMark(AccountFact):
    instrument_hash: str
    price_numerator_units: int
    price_denominator: int
    observed_at: datetime
    source: AccountSource

    def __post_init__(self):
        _identity(self.instrument_hash, "instrument_hash")
        _amount(self.price_numerator_units, "price_numerator_units", positive=True)
        _amount(self.price_denominator, "price_denominator", positive=True)
        require_aware_datetime(self.observed_at, "observed_at")


@dataclass(frozen=True)
class DividendRegistrationPlan(AccountFact):
    dividend_id: str
    instrument_hash: str
    record_at: datetime
    ex_at: datetime
    pay_at: datetime
    cash_per_share_numerator: int
    cash_per_share_denominator: int
    taxable_per_share_numerator: int
    taxable_per_share_denominator: int
    tax_rule_id: str
    source: AccountSource

    def __post_init__(self):
        for name in ("dividend_id", "instrument_hash", "tax_rule_id"):
            _identity(getattr(self, name), name)
        for name in ("record_at", "ex_at", "pay_at"):
            require_aware_datetime(getattr(self, name), name)
        if not self.record_at <= self.ex_at <= self.pay_at:
            raise DomainContractError("分红登记、除权与支付时点顺序无效")
        for name in ("cash_per_share_numerator", "taxable_per_share_numerator"):
            _amount(getattr(self, name), name)
        for name in ("cash_per_share_denominator", "taxable_per_share_denominator"):
            _amount(getattr(self, name), name, positive=True)


@dataclass(frozen=True)
class DividendWithholdingRule(AccountFact):
    """派息预扣按本次登记应税基数计算，已扣金额抵减后续转让税款。"""

    rate_numerator: int
    rate_denominator: int
    rounding: str
    source: AccountSource

    def __post_init__(self):
        _amount(self.rate_numerator, "rate_numerator")
        _amount(self.rate_denominator, "rate_denominator", positive=True)
        if self.rate_numerator > self.rate_denominator or self.rounding not in {"half_up", "floor"}:
            raise DomainContractError("派息预扣须明确有效税率与金额舍入规则")


@dataclass(frozen=True)
class SecurityConversionPlan(AccountFact):
    conversion_id: str
    old_instrument_hash: str
    new_instrument_hash: str
    cancelled_at: datetime
    registered_at: datetime
    tradable_at: datetime
    cash_pay_at: datetime
    ratio_numerator: int
    ratio_denominator: int
    cash_per_old_share_numerator: int
    cash_per_old_share_denominator: int
    successor_cost_numerator: int
    successor_cost_denominator: int
    acquisition_date_policy: str
    tax_right_policy: str
    fractional_policy: str
    fractional_cash_price_numerator: int
    fractional_cash_price_denominator: int
    interim_price_numerator: int
    interim_price_denominator: int
    valuation_observed_at: datetime
    source: AccountSource
    valuation_source: AccountSource
    successor_acquisition_method: str = "conversion"
    successor_security_class: str = "equity"

    def __post_init__(self):
        for name in ("conversion_id", "old_instrument_hash", "new_instrument_hash", "successor_acquisition_method", "successor_security_class"):
            _identity(getattr(self, name), name)
        for name in ("cancelled_at", "registered_at", "tradable_at", "cash_pay_at", "valuation_observed_at"):
            require_aware_datetime(getattr(self, name), name)
        if not self.cancelled_at <= self.registered_at <= self.tradable_at or self.cash_pay_at < self.cancelled_at:
            raise DomainContractError("证券注销、登记、可交易或现金到账时点顺序无效")
        if self.old_instrument_hash == self.new_instrument_hash:
            raise DomainContractError("证券承接必须指定不同的后继证券")
        for name in ("ratio_numerator", "ratio_denominator", "cash_per_old_share_denominator", "successor_cost_denominator", "fractional_cash_price_denominator", "interim_price_denominator"):
            _amount(getattr(self, name), name, positive=True)
        for name in ("cash_per_old_share_numerator", "successor_cost_numerator", "fractional_cash_price_numerator", "interim_price_numerator"):
            _amount(getattr(self, name), name)
        if self.successor_cost_numerator > self.successor_cost_denominator:
            raise DomainContractError("后继成本分配比例不能超过一")
        if self.acquisition_date_policy not in {"inherit", "registration_date"} or self.tax_right_policy not in {"carry", "require_no_rights"}:
            raise DomainContractError("证券承接必须明确取得日和税权承接规则")
        if self.fractional_policy not in {"reject", "cash_in_lieu"}:
            raise DomainContractError("零碎权益必须明确拒绝或现金化规则")
        if self.interim_price_numerator <= 0:
            raise DomainContractError("转换期间必须有正的权益估值依据")


__all__ = [
    "AccountSource", "OpeningCash", "SpotAcquisitionLot", "OpeningCashClaim",
    "DividendEntitlement", "HoldingPeriodRate", "DividendTaxRule", "SpotOpeningSnapshot",
    "OpeningPositionEntitlement", "OpeningTaxAssessment", "SpotOpeningMark", "DividendRegistrationPlan", "SecurityConversionPlan",
]


@dataclass(frozen=True)
class CorporateActionLotRule(AccountFact):
    """来源明确的送转／拆股批次规则，比例按 CorporateAction 的总量语义。"""

    rule_id: str
    acquisition_date_policy: str
    sellable_at: datetime
    source: AccountSource
    acquisition_method: str = "stock_dividend"
    tax_right_policy: str = "require_no_rights"

    def __post_init__(self):
        _identity(self.rule_id, "rule_id")
        _identity(self.acquisition_method, "acquisition_method")
        require_aware_datetime(self.sellable_at, "sellable_at")
        if self.acquisition_date_policy not in {"inherit", "effective_date"}:
            raise DomainContractError("公司行动必须明确股份取得日承接规则")
        if self.tax_right_policy not in {"require_no_rights", "retain_original", "scale"}:
            raise DomainContractError("公司行动必须明确历史分红税权承接规则")


__all__.append("CorporateActionLotRule")
