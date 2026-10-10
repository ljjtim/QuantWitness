"""融资信用账户的协议、初始债务和指令事实。"""
from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime
import json
from typing import Mapping
from zoneinfo import ZoneInfo

from .models import DomainContractError
from .spot_account import AccountFact
from .time import require_aware_datetime

_SHANGHAI = ZoneInfo("Asia/Shanghai")

CREDIT_ACCOUNT_VERSION = "research-credit-account-v1"
CREDIT_CONTEXT_VERSION = "research-credit-account-context-v1"


def _identity(value, name):
    if not isinstance(value, str) or not value.strip():
        raise DomainContractError(f"融资 {name} 必须为非空字符串")


def _units(value, name, *, positive=False):
    if type(value) is not int or value < int(positive):
        raise DomainContractError(f"融资 {name} 必须为{'正' if positive else '非负'}整数")


def _at(value, name):
    if not isinstance(value, datetime):
        raise DomainContractError(f"融资 {name} 必须是带时区时点")
    require_aware_datetime(value, name)
    return value.astimezone(_SHANGHAI)


def _date(value, name):
    if type(value) is not date:
        raise DomainContractError(f"融资 {name} 必须是日期")


def _unique(values, name):
    if len(values) != len(set(values)):
        raise DomainContractError(f"融资 {name} 不能重复")


@dataclass(frozen=True)
class CreditCollateralRate(AccountFact):
    instrument_hash: str
    rate_ppm: int
    exchange_cap_ppm: int = 900_000
    security_category: str = "etf"
    static_pe_ppm: int | None = None
    category_source_ref: str | None = None

    def __post_init__(self):
        _identity(self.instrument_hash, "instrument_hash")
        _units(self.rate_ppm, "rate_ppm")
        _units(self.exchange_cap_ppm, "exchange_cap_ppm")
        caps = {"sse180_stock": 700_000, "other_a_stock": 650_000,
            "risk_warning_stock": 0, "delisting_stock": 0, "etf": 900_000}
        if self.security_category not in caps:
            raise DomainContractError("担保证券类别不在SSE支持范围")
        if self.category_source_ref is not None:
            _identity(self.category_source_ref, "category_source_ref")
        if self.security_category in {"sse180_stock", "other_a_stock"} and type(self.static_pe_ppm) is not int:
            raise DomainContractError("普通A股担保分类必须提供静态市盈率事实")
        if self.static_pe_ppm is not None and type(self.static_pe_ppm) is not int:
            raise DomainContractError("静态市盈率必须是整数ppm")
        cap = caps[self.security_category]
        if self.security_category != "etf" and self.static_pe_ppm is not None and (self.static_pe_ppm >= 300_000_000 or self.static_pe_ppm < 0):
            cap = 0
        if self.exchange_cap_ppm != cap or self.rate_ppm > cap:
            raise DomainContractError("担保折算与SSE证券类别、静态市盈率上限不符")



@dataclass(frozen=True)
class CreditStockRiskFact(AccountFact):
    instrument_hash: str
    session: date
    previous_session: date
    available_at: datetime
    market_collateral_ratio_ppm: int
    static_pe_ppm: int
    source_ref: str
    client_security_value_units: int | None = None
    client_asset_units: int | None = None
    client_debt_units: int | None = None

    def __post_init__(self):
        _identity(self.instrument_hash, "instrument_hash")
        _identity(self.source_ref, "source_ref")
        _date(self.session, "session")
        _date(self.previous_session, "previous_session")
        object.__setattr__(self, "available_at", _at(self.available_at, "available_at"))
        _units(self.market_collateral_ratio_ppm, "market_collateral_ratio_ppm")
        if self.market_collateral_ratio_ppm > 1_000_000 or type(self.static_pe_ppm) is not int:
            raise DomainContractError("股票集中度事实数值无效")
        for name in ("client_security_value_units", "client_asset_units", "client_debt_units"):
            value = getattr(self, name)
            if value is not None:
                _units(value, name)
        if self.client_security_value_units is not None and self.client_asset_units is not None and self.client_security_value_units > self.client_asset_units:
            raise DomainContractError("前一会话客户单证券市值不能超过现金证券总值")
        if self.previous_session >= self.session or self.available_at.date() > self.session:
            raise DomainContractError("股票集中度事实必须引用前一会话")


@dataclass(frozen=True)
class CreditRule(AccountFact):
    rule_id: str
    effective_start: date
    effective_end: date | None
    available_at: datetime
    source_ref: str
    assumption_source_ref: str
    eligible_instruments: tuple[str, ...]
    collateral_rates: tuple[CreditCollateralRate, ...]
    exchange_margin_ppm: int
    broker_margin_ppm: int
    maintenance_ratio_ppm: int
    withdrawal_ratio_ppm: int
    concentration_ppm: int
    credit_limit_units: int
    annual_rate_ppm: int
    interest_day_basis: int
    max_contract_days: int
    risk_grace_days: int
    repayment_order: tuple[str, ...]
    agreement_fact_kind: str = "explicit_research_assumption"
    stock_risk_facts: tuple[CreditStockRiskFact, ...] = ()
    remedy_ratio_ppm: int = 3_000_000

    def __post_init__(self):
        for name in ("rule_id", "source_ref", "assumption_source_ref"):
            _identity(getattr(self, name), name)
        _date(self.effective_start, "effective_start")
        if self.effective_end is not None:
            _date(self.effective_end, "effective_end")
            if self.effective_end < self.effective_start:
                raise DomainContractError("融资规则有效日期倒置")
        object.__setattr__(self, "available_at", _at(self.available_at, "available_at"))
        for name in ("exchange_margin_ppm", "broker_margin_ppm", "maintenance_ratio_ppm", "withdrawal_ratio_ppm", "concentration_ppm", "credit_limit_units", "annual_rate_ppm", "max_contract_days", "risk_grace_days"):
            _units(getattr(self, name), name)
        if self.max_contract_days > 184:
            raise DomainContractError("融资声明期限不能超过六个日历月上界")
        if min(self.exchange_margin_ppm, self.broker_margin_ppm, self.max_contract_days) <= 0:
            raise DomainContractError("开仓保证金比例与期限必须为正")
        if self.interest_day_basis not in (360, 365) or type(self.interest_day_basis) is not int:
            raise DomainContractError("计息年天数必须为360或365")
        if not 0 < self.concentration_ppm <= 1_000_000:
            raise DomainContractError("集中度上限须在(0,100%]内")
        if self.agreement_fact_kind not in {"agreement", "explicit_research_assumption"}:
            raise DomainContractError("协议事实必须区分实际协议与研究假设")
        _units(self.remedy_ratio_ppm, "remedy_ratio_ppm")
        if self.remedy_ratio_ppm < self.maintenance_ratio_ppm:
            raise DomainContractError("补足目标不能低于维持担保要求")
        if self.withdrawal_ratio_ppm < max(3_000_000, self.maintenance_ratio_ppm):
            raise DomainContractError("出金担保要求不能低于维持担保要求")
        if set(self.repayment_order) != {"interest", "principal"} or len(self.repayment_order) != 2:
            raise DomainContractError("还款顺序必须完整声明利息与本金")
        _unique(self.eligible_instruments, "eligible_instruments")
        _unique([item.instrument_hash for item in self.collateral_rates], "collateral_rates")
        for item in self.eligible_instruments:
            _identity(item, "eligible_instruments")

    @property
    def opening_margin_ppm(self):
        return max(self.exchange_margin_ppm, self.broker_margin_ppm)

    def visible_at(self, at: datetime):
        at = _at(at, "at")
        return self.available_at <= at and self.effective_start <= at.date() and (
            self.effective_end is None or at.date() <= self.effective_end
        )


@dataclass(frozen=True)
class CreditContract(AccountFact):
    contract_id: str
    opened_at: datetime
    maturity_date: date
    opening_rule_id: str
    principal_units: int
    interest_units: int
    interest_remainder_numerator: int
    interest_remainder_denominator: int
    last_accrual_date: date
    source_ref: str

    def __post_init__(self):
        for name in ("contract_id", "opening_rule_id", "source_ref"):
            _identity(getattr(self, name), name)
        object.__setattr__(self, "opened_at", _at(self.opened_at, "opened_at"))
        _date(self.maturity_date, "maturity_date")
        _date(self.last_accrual_date, "last_accrual_date")
        for name in ("principal_units", "interest_units", "interest_remainder_numerator"):
            _units(getattr(self, name), name)
        _units(self.interest_remainder_denominator, "interest_remainder_denominator", positive=True)
        if self.interest_remainder_numerator >= self.interest_remainder_denominator:
            raise DomainContractError("计息余数必须小于1分")
        if self.maturity_date < self.opened_at.date() or self.last_accrual_date < self.opened_at.date():
            raise DomainContractError("融资期限或计息日期早于起点")


@dataclass(frozen=True)
class CreditPositionLink(AccountFact):
    link_id: str
    contract_id: str
    instrument_hash: str
    lot_id: str
    quantity: int
    acquisition_units: int
    principal_units: int
    source_ref: str
    receivable_id: str | None = None
    successor_right_id: str | None = None

    def __post_init__(self):
        for name in ("link_id", "contract_id", "instrument_hash", "lot_id", "source_ref"):
            _identity(getattr(self, name), name)
        for name in ("quantity", "acquisition_units", "principal_units"):
            _units(getattr(self, name), name)
        if self.receivable_id is not None and self.successor_right_id is not None:
            raise DomainContractError("融资关联不能同时指向应收和后继权益")
        for name in ("receivable_id", "successor_right_id"):
            if getattr(self, name) is not None:
                _identity(getattr(self, name), name)


@dataclass(frozen=True)
class CreditOrderAllocation(AccountFact):
    order_id: str
    contract_id: str
    financing_limit_units: int
    own_cash_limit_units: int
    maturity_date: date
    source_ref: str

    def __post_init__(self):
        for name in ("order_id", "contract_id", "source_ref"):
            _identity(getattr(self, name), name)
        _units(self.financing_limit_units, "financing_limit_units", positive=True)
        _units(self.own_cash_limit_units, "own_cash_limit_units")
        _date(self.maturity_date, "maturity_date")


@dataclass(frozen=True)
class CreditInstruction(AccountFact):
    instruction_id: str
    kind: str
    contract_id: str | None
    amount_units: int
    requested_at: datetime
    available_at: datetime
    effective_at: datetime
    status: str
    new_maturity_date: date | None
    approval_at: datetime | None
    approval_source_ref: str | None
    source_ref: str

    def __post_init__(self):
        for name in ("instruction_id", "source_ref"):
            _identity(getattr(self, name), name)
        if self.contract_id is not None:
            _identity(self.contract_id, "contract_id")
        if self.kind not in {"repay", "extend"} or self.status not in {"approved", "cancelled", "rejected"}:
            raise DomainContractError("融资指令类型或状态无效")
        for name in ("requested_at", "available_at", "effective_at"):
            object.__setattr__(self, name, _at(getattr(self, name), name))
        if max(self.requested_at, self.available_at) > self.effective_at:
            raise DomainContractError("融资指令不能在受理前生效")
        _units(self.amount_units, "amount_units")
        if self.kind == "repay":
            if self.amount_units <= 0 or any(value is not None for value in (self.new_maturity_date, self.approval_at, self.approval_source_ref)):
                raise DomainContractError("直接还款须为正金额且不声明展期批准")
        else:
            if self.amount_units or self.contract_id is None or self.new_maturity_date is None:
                raise DomainContractError("展期须绑定合同和新期限且金额为0")
            _date(self.new_maturity_date, "new_maturity_date")
            if self.status == "approved":
                object.__setattr__(self, "approval_at", _at(self.approval_at, "approval_at"))
                _identity(self.approval_source_ref, "approval_source_ref")
                if self.approval_at > self.effective_at:
                    raise DomainContractError("展期批准在生效时点尚不可见")


@dataclass(frozen=True)
class CreditAccount(AccountFact):
    contract_version: str
    account_id: str
    currency: str
    cash_scale: int
    as_of: datetime
    rules: tuple[CreditRule, ...]
    contracts: tuple[CreditContract, ...]
    position_links: tuple[CreditPositionLink, ...]
    order_allocations: tuple[CreditOrderAllocation, ...]
    instructions: tuple[CreditInstruction, ...]
    profile_id: str = "sse-cny-financing-cash-securities-v1"
    repayment_link_policy: str = "proportional_principal_keep_quantity"
    interest_policy: str = "actual_days_open_inclusive_repay_exclusive_floor_with_remainder"
    risk_sell_policy: str = "maturity_then_contract_then_instrument"
    sale_settlement_policy: str = "next_session_preopen"

    def __post_init__(self):
        if self.contract_version != CREDIT_ACCOUNT_VERSION or self.currency != "CNY" or self.cash_scale != 2 or type(self.cash_scale) is not int:
            raise DomainContractError("融资账户版本、币种或金额精度无效")
        supported = {"profile_id": "sse-cny-financing-cash-securities-v1",
            "repayment_link_policy": "proportional_principal_keep_quantity",
            "interest_policy": "actual_days_open_inclusive_repay_exclusive_floor_with_remainder",
            "risk_sell_policy": "maturity_then_contract_then_instrument",
            "sale_settlement_policy": "next_session_preopen"}
        if any(getattr(self, key) != value for key, value in supported.items()):
            raise DomainContractError("融资声明包含未支持的协议计算方式")
        _identity(self.account_id, "account_id")
        object.__setattr__(self, "as_of", _at(self.as_of, "as_of"))
        if not self.rules:
            raise DomainContractError("融资账户必须声明规则历史")
        for values, name in ((self.rules, "rule_id"), (self.contracts, "contract_id"), (self.position_links, "link_id"), (self.order_allocations, "order_id"), (self.instructions, "instruction_id")):
            _unique([getattr(item, name) for item in values], name)
        rules = {item.rule_id: item for item in self.rules}
        contracts = {item.contract_id: item for item in self.contracts}
        for contract in self.contracts:
            if contract.opened_at > self.as_of or contract.last_accrual_date > self.as_of.date() or contract.opening_rule_id not in rules:
                raise DomainContractError("期初债务日期或规则引用无效")
            if contract.last_accrual_date != self.as_of.date():
                raise DomainContractError("期初利息必须计提至as_of日期，未付金额及余数须显式声明")
            require_credit_maturity(contract.opened_at.date(), contract.maturity_date, rules[contract.opening_rule_id].max_contract_days)
            if not rules[contract.opening_rule_id].visible_at(contract.opened_at):
                raise DomainContractError("开仓规则在历史开仓时点不可见")
        for link in self.position_links:
            if link.contract_id not in contracts:
                raise DomainContractError("融资批次引用未知期初合同")
        for contract in self.contracts:
            if sum(item.principal_units for item in self.position_links if item.contract_id == contract.contract_id) != contract.principal_units:
                raise DomainContractError("期初融资批次本金与合同不一致")
        allocation_contracts = [item.contract_id for item in self.order_allocations]
        _unique(allocation_contracts, "新开融资contract_id")
        if set(allocation_contracts) & set(contracts):
            raise DomainContractError("新成交不能装入期初存量融资合同")
        known_contracts = set(contracts) | set(allocation_contracts)
        for instruction in self.instructions:
            if instruction.contract_id is not None and instruction.contract_id not in known_contracts:
                raise DomainContractError("融资指令引用完全未知合同")
            if instruction.effective_at < self.as_of:
                raise DomainContractError("融资指令不能早于期初时点")
        opening_rules = [rule for rule in self.rules if rule.visible_at(self.as_of)]
        if len(opening_rules) != 1:
            raise DomainContractError("期初融资规则缺失或重叠")
        self.rule_at(self.as_of)

    def rule_at(self, at: datetime) -> CreditRule:
        at = _at(at, "at")
        rules = [item for item in self.rules if item.visible_at(at)]
        if len(rules) != 1:
            raise DomainContractError("当时融资规则缺失、重叠或尚不可见")
        official_minimum = 800_000 if datetime(2023, 9, 8, 15, tzinfo=_SHANGHAI) <= at < datetime(2026, 1, 19, tzinfo=_SHANGHAI) else 1_000_000
        if at.date() < date(2023, 2, 17) or rules[0].exchange_margin_ppm < official_minimum:
            raise DomainContractError("SSE融资比例低于当时有效官方最低或日期超出profile")
        return rules[0]


def six_month_limit(day: date) -> date:
    month = day.month + 6
    year = day.year + (month-1)//12
    month = (month-1)%12+1
    return date(year, month, min(day.day, monthrange(year, month)[1]))


def require_credit_maturity(opened_on: date, maturity: date, max_contract_days: int) -> None:
    if maturity <= opened_on or maturity > six_month_limit(opened_on) or (maturity-opened_on).days > max_contract_days:
        raise DomainContractError("融资期限须为正且不超过协议天数和六个日历月")


def parse_credit_account(value: str | Mapping[str, object] | CreditAccount) -> CreditAccount:
    """解析完整信用声明，返回可封存的不可变事实对象。"""
    if isinstance(value, CreditAccount):
        return value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise DomainContractError("credit_account 必须为完整JSON对象") from exc
    if not isinstance(value, Mapping):
        raise DomainContractError("credit_account 必须为JSON对象或CreditAccount")
    return CreditAccount.from_dict(value)


__all__ = ["CREDIT_ACCOUNT_VERSION", "CREDIT_CONTEXT_VERSION", "CreditAccount", "CreditRule", "CreditStockRiskFact", "CreditCollateralRate", "CreditContract", "CreditPositionLink", "CreditOrderAllocation", "CreditInstruction", "parse_credit_account", "six_month_limit", "require_credit_maturity"]
