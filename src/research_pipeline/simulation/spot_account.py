"""现货批次、税权与承接事件内核；余额仅由调用方的 Ledger 持有。"""

from __future__ import annotations

import json
from calendar import monthrange
from dataclasses import dataclass, replace
from datetime import date, datetime
from fractions import Fraction
from typing import Sequence

from research_pipeline.domain.spot_account import (
    AccountFact, AccountSource, DividendEntitlement, DividendRegistrationPlan,
    CorporateActionLotRule, DividendTaxRule, DividendWithholdingRule, SecurityConversionPlan, SpotAcquisitionLot, SpotOpeningMark,
    SpotOpeningSnapshot,
)
from research_pipeline.domain.corporate_actions import CorporateAction
from research_pipeline.domain.time import require_aware_datetime

from .events import FinancialEvent
from .ledger import CashPayable, CashReceivable, ExecutionGroup, PositionEntitlement, PositionLot, SpotLedgerState
from .orders import SimulationContractError


def _round(value: Fraction, policy: str = "half_up") -> int:
    if value < 0:
        raise SimulationContractError("账户金额不能为负")
    if policy == "half_up":
        value += Fraction(1, 2)
    elif policy != "floor":
        raise SimulationContractError("未支持的账户金额舍入规则")
    return value.numerator // value.denominator


def calendar_boundary(acquired_on: date, periods: int, unit: str) -> date:
    """自然月／年周年日，月末取对应月份实际末日。"""
    if type(periods) is not int or periods < 1 or unit not in {"month", "year"}:
        raise SimulationContractError("持有期边界参数无效")
    months = periods * (12 if unit == "year" else 1)
    year, month0 = divmod(acquired_on.year * 12 + acquired_on.month - 1 + months, 12)
    month = month0 + 1
    return date(year, month, min(acquired_on.day, monthrange(year, month)[1]))


def resolve_holding_rate(rule: DividendTaxRule, *, acquired_on: date, delivered_on: date) -> Fraction:
    if delivered_on < acquired_on:
        raise SimulationContractError("转让交割日不能早于取得日")
    for band in rule.rates:
        if band.upper_periods is None:
            return Fraction(band.rate_numerator, band.rate_denominator)
        boundary = calendar_boundary(acquired_on, band.upper_periods, band.period_unit)
        if delivered_on < boundary or (band.upper_inclusive and delivered_on == boundary):
            return Fraction(band.rate_numerator, band.rate_denominator)
    raise SimulationContractError("持有期税率表未覆盖交割日")


@dataclass(frozen=True)
class AccountEventRecord(AccountFact):
    sequence: int
    event_id: str
    kind: str
    effective_time: datetime
    source: AccountSource
    business_refs: tuple[str, ...]


@dataclass(frozen=True)
class LotTransferMatch(AccountFact):
    lot_id: str
    quantity: int
    accounting_cost_units: int
    acquired_on: date | None


@dataclass(frozen=True)
class SpotTransfer(AccountFact):
    transfer_id: str
    fill_event_id: str
    instrument_hash: str
    quantity: int
    delivered_on: date
    effective_time: datetime
    matches: tuple[LotTransferMatch, ...]
    source: AccountSource
    sequence: int


@dataclass(frozen=True)
class EntitlementConsumption(AccountFact):
    entitlement_id: str
    transfer_id: str
    lot_id: str
    quantity: int
    remaining_quantity: int
    prewithheld_credit_units: int
    assessment_id: str | None
    sequence: int
    quantity_denominator: int = 1
    remaining_quantity_denominator: int = 1


@dataclass(frozen=True)
class TaxAssessmentLine(AccountFact):
    entitlement_id: str
    transfer_id: str
    quantity: int
    acquired_on: date
    delivered_on: date
    rule_id: str
    rule_source: AccountSource
    gross_tax_units: int
    prewithheld_credit_units: int
    payable_units: int
    refund_units: int
    quantity_denominator: int = 1


@dataclass(frozen=True)
class TaxAssessment(AccountFact):
    assessment_id: str
    transfer_id: str | None
    assessed_at: datetime
    due_at: datetime
    tax_units: int
    lines: tuple[TaxAssessmentLine, ...]
    source: AccountSource
    opening_collected_units: int
    sequence: int


@dataclass(frozen=True)
class TaxCollectionAllocation(AccountFact):
    assessment_id: str
    cash_units: int

    def __post_init__(self):
        if not self.assessment_id or type(self.cash_units) is not int or self.cash_units <= 0:
            raise SimulationContractError("扣收分配必须有核定身份和正整数金额")


@dataclass(frozen=True)
class TaxCollection(AccountFact):
    collection_id: str
    effective_time: datetime
    allocations: tuple[TaxCollectionAllocation, ...]
    source: AccountSource
    sequence: int


@dataclass(frozen=True)
class SuccessorLotLink(AccountFact):
    old_lot_id: str
    old_quantity: int
    old_cost_units: int
    successor_lot: SpotAcquisitionLot | None
    fractional_quantity_numerator: int
    fractional_quantity_denominator: int
    cash_consideration_units: int
    cash_allocated_cost_units: int
    carried_entitlement_ids: tuple[str, ...]


@dataclass(frozen=True)
class SecurityConversion(AccountFact):
    plan: SecurityConversionPlan
    links: tuple[SuccessorLotLink, ...]
    registered: bool
    released: bool
    cash_paid: bool
    sequence: int
    released_lot_ids: tuple[str, ...] = ()

    @property
    def successor_quantity(self) -> int:
        return sum(link.successor_lot.quantity for link in self.links if link.successor_lot is not None)

    @property
    def cash_units(self) -> int:
        return sum(link.cash_consideration_units for link in self.links)


@dataclass(frozen=True)
class RegisteredDividend(AccountFact):
    plan: DividendRegistrationPlan
    registered_quantity: int
    cash_units: int
    recognized: bool
    paid: bool
    sequence: int



@dataclass(frozen=True)
class CorporateActionRecord(AccountFact):
    action_payload: str
    action_id: str
    revision: int
    record_lots: tuple[SpotAcquisitionLot, ...]
    applied: bool
    financial_event_id: str | None
    lot_rule: CorporateActionLotRule | None
    source: AccountSource
    links: tuple[SuccessorLotLink, ...] = ()
    arrival_event_id: str | None = None


@dataclass(frozen=True)
class PreparedSpotOpening:
    ledger_state: SpotLedgerState
    book: SpotAccountBook
    opening_nav_units: int
    position_market_values: tuple[tuple[str, int], ...]
    snapshot: SpotOpeningSnapshot
    marks: tuple[SpotOpeningMark, ...]


def prepare_opening(
    snapshot: SpotOpeningSnapshot, *, group: ExecutionGroup,
    marks: Sequence[SpotOpeningMark], rule_hash: str,
) -> PreparedSpotOpening:
    """只构造一次起点；恢复加载 Ledger 与 Book 的同一正式 checkpoint。"""
    if group.is_futures or group.currency != snapshot.currency:
        raise SimulationContractError("期初现货账户与执行组不一致")
    if len(rule_hash) != 64:
        raise SimulationContractError("期初必须绑定正式规则身份")
    mark_map = {mark.instrument_hash: mark for mark in marks}
    if len(mark_map) != len(marks):
        raise SimulationContractError("期初估值证券身份重复")
    quantities: dict[str, list[int]] = {}
    for lot in snapshot.lots:
        bucket = quantities.setdefault(lot.instrument_hash, [0, 0])
        bucket[0 if lot.sellable_at <= snapshot.started_at else 1] += lot.quantity
    if set(mark_map) != set(quantities):
        raise SimulationContractError("期初估值须恰好覆盖已持有证券")
    market_values = []
    for key, bucket in sorted(quantities.items()):
        mark = mark_map[key]
        mark.source.require_visible(snapshot.visible_cutoff)
        if mark.observed_at > snapshot.started_at or mark.observed_at > mark.source.available_at:
            raise SimulationContractError("期初估值观测时点不能晚于起点或其可见时间")
        market_values.append((key, _round(Fraction(mark.price_numerator_units, mark.price_denominator) * sum(bucket))))
    payables = [CashPayable(item.claim_id, item.cash_units) for item in snapshot.payables]
    for item in snapshot.tax_assessments:
        if item.assessed_at > snapshot.started_at:
            raise SimulationContractError("期初税款不能在未来核定")
        outstanding = item.assessed_units - item.collected_units
        if outstanding:
            payables.append(CashPayable(item.assessment_id, outstanding))
    if len({item.payable_id for item in payables}) != len(payables):
        raise SimulationContractError("期初应付与税务核定身份重复")
    state = SpotLedgerState(
        group=group, available_cash_units=snapshot.cash.available_units,
        frozen_cash_units=snapshot.cash.restricted_units,
        unsettled_cash_units=snapshot.cash.unsettled_units,
        positions=tuple(PositionLot(key, sellable=bucket[0], unsettled=bucket[1]) for key, bucket in sorted(quantities.items())),
        cash_receivables=tuple(CashReceivable(item.claim_id, item.due_at.date(), item.cash_units) for item in sorted(snapshot.receivables, key=lambda item: item.claim_id)),
        cash_payables=tuple(sorted(payables, key=lambda item: item.payable_id)),
        position_entitlements=tuple(PositionEntitlement(item.entitlement_id, next(lot.instrument_hash for lot in snapshot.lots if lot.lot_id == item.lot_id), item.due_at.date(), item.quantity) for item in sorted(snapshot.position_entitlements, key=lambda item: item.entitlement_id)),
    )
    book = SpotAccountBook(
        account_id=snapshot.account_id, group_id=group.group_id,
        snapshot_id=snapshot.snapshot_id, started_at=snapshot.started_at,
        investor_tax_identity=snapshot.investor_tax_identity,
        exact_dividend_tax=snapshot.exact_dividend_tax,
        accounting_cost_method=snapshot.accounting_cost_method,
        lots=tuple(sorted(snapshot.lots, key=lambda lot: lot.lot_id)),
        entitlements=tuple(sorted(snapshot.dividend_entitlements, key=lambda item: item.entitlement_id)),
        assessments=tuple(TaxAssessment(item.assessment_id, None, item.assessed_at, item.due_at, item.assessed_units, (), item.source, item.collected_units, 0) for item in snapshot.tax_assessments),
    )
    book._apply_average_costs()
    book._record("opening", snapshot.snapshot_id, snapshot.started_at, snapshot.source)
    net = state.total_cash_units + sum(value for _, value in market_values) - sum(item.cash_units for item in payables)
    return PreparedSpotOpening(state, book, net, tuple(market_values), snapshot, tuple(marks))


@dataclass
class SpotAccountBook(AccountFact):
    """只持有业务事实；协调器将返回事件与本对象在同一金融批次发布。"""

    account_id: str
    group_id: str
    snapshot_id: str
    started_at: datetime
    investor_tax_identity: str
    exact_dividend_tax: bool
    accounting_cost_method: str = "lot_cost"
    lots: tuple[SpotAcquisitionLot, ...] = ()
    entitlements: tuple[DividendEntitlement, ...] = ()
    transfers: tuple[SpotTransfer, ...] = ()
    consumptions: tuple[EntitlementConsumption, ...] = ()
    assessments: tuple[TaxAssessment, ...] = ()
    collections: tuple[TaxCollection, ...] = ()
    dividends: tuple[RegisteredDividend, ...] = ()
    conversions: tuple[SecurityConversion, ...] = ()
    event_records: tuple[AccountEventRecord, ...] = ()
    corporate_action_records: tuple[CorporateActionRecord, ...] = ()
    acquisition_lots: tuple[SpotAcquisitionLot, ...] = ()

    def _apply_average_costs(self) -> None:
        if self.accounting_cost_method != "average":
            return
        groups: dict[str, list[SpotAcquisitionLot]] = {}
        for lot in self.lots:
            groups.setdefault(lot.instrument_hash, []).append(lot)
        normalized = []
        for lots in groups.values():
            total_quantity = sum(lot.quantity for lot in lots)
            total_cost = sum(lot.accounting_cost_units for lot in lots)
            previous, cumulative = 0, 0
            for lot in sorted(lots, key=lambda item: item.lot_id):
                cumulative += lot.quantity
                allocated = _round(Fraction(total_cost * cumulative, total_quantity))
                normalized.append(replace(lot, accounting_cost_units=allocated - previous))
                previous = allocated
        self.lots = tuple(sorted(normalized, key=lambda item: item.lot_id))

    def _check_time(self, effective_time: datetime, source: AccountSource) -> None:
        require_aware_datetime(effective_time, "effective_time")
        source.require_visible(effective_time)
        if effective_time < self.started_at or (self.event_records and effective_time < self.event_records[-1].effective_time):
            raise SimulationContractError("账户事件不能早于起点或已处理事件")

    def _record(self, kind: str, business_id: str, effective_time: datetime, source: AccountSource, refs: tuple[str, ...] = ()) -> AccountEventRecord:
        self._check_time(effective_time, source)
        event_id = f"spot:{self.account_id}:{kind}:{business_id}"
        if not business_id or any(item.event_id == event_id for item in self.event_records):
            raise SimulationContractError("账户业务事件身份为空或重复")
        record = AccountEventRecord(len(self.event_records) + 1, event_id, kind, effective_time, source, refs)
        self.event_records += (record,)
        return record

    def _event(self, record: AccountEventRecord, kind: str, values: dict[str, object], rule_hash: str, *, suffix: str = "") -> FinancialEvent:
        return FinancialEvent(
            record.event_id + suffix, kind, record.effective_time,
            record.effective_time.date().isoformat(), self.group_id, rule_hash,
            tuple(sorted({**values, "account_id": self.account_id, "account_sequence": record.sequence, "source_ref": record.source.source_ref, "available_at": record.source.available_at.isoformat()}.items())),
            parent_id=record.business_refs[0] if record.business_refs else None,
        )

    def record_buy_fill(
        self, fill: FinancialEvent, *, lot_id: str, acquired_on: date,
        sellable_at: datetime, source: AccountSource,
        acquisition_method: str = "market_buy", security_class: str = "equity",
    ) -> SpotAcquisitionLot:
        values = fill.values()
        if fill.kind != "fill" or values.get("side") != "buy" or fill.group_id != self.group_id:
            raise SimulationContractError("买入批次必须投影同执行组真实 buy fill")
        self._check_time(fill.effective_time, source)
        if acquired_on != fill.effective_time.date():
            raise SimulationContractError("成交取得日必须对应实际成交日")
        if any(lot.lot_id == lot_id for lot in self.lots) or any(fill.event_id in item.business_refs for item in self.event_records if item.kind == "buy_fill"):
            raise SimulationContractError("买入批次或成交投影重复")
        if any(item.plan.old_instrument_hash == values["instrument_hash"] for item in self.conversions):
            raise SimulationContractError("已注销证券不能再增加取得批次")
        lot = SpotAcquisitionLot(lot_id, str(values["instrument_hash"]), values["quantity"], values["notional_units"] + values["fee_units"], acquired_on, sellable_at, acquisition_method, security_class, source)
        self._record("buy_fill", lot_id, fill.effective_time, source, (fill.event_id,))
        self.acquisition_lots += (lot,)
        self.lots = tuple(sorted((*self.lots, lot), key=lambda item: item.lot_id))
        self._apply_average_costs()
        return next(item for item in self.lots if item.lot_id == lot_id)

    def record_sell_fill(
        self, fill: FinancialEvent, *, transfer_id: str, delivered_on: date,
        source: AccountSource,
    ) -> SpotTransfer:
        values = fill.values()
        if fill.kind != "fill" or values.get("side") != "sell" or fill.group_id != self.group_id:
            raise SimulationContractError("转让必须投影同执行组真实 sell fill")
        return self._project_transfer(fill, transfer_id=transfer_id, delivered_on=delivered_on, source=source, quantity=values["quantity"], instrument_hash=str(values["instrument_hash"]), require_sellable=True)

    def _project_transfer(
        self, financial_event: FinancialEvent, *, transfer_id: str,
        delivered_on: date, source: AccountSource, quantity: int,
        instrument_hash: str, require_sellable: bool,
    ) -> SpotTransfer:
        self._check_time(financial_event.effective_time, source)
        if delivered_on < financial_event.effective_time.date():
            raise SimulationContractError("来源声明的转让交割日不能早于实际转让事件")
        if any(item.transfer_id == transfer_id or item.fill_event_id == financial_event.event_id for item in self.transfers):
            raise SimulationContractError("转让身份或成交投影重复")
        if type(quantity) is not int or quantity <= 0:
            raise SimulationContractError("转让数量必须为正整数")
        key = instrument_hash
        candidates = sorted((lot for lot in self.lots if lot.instrument_hash == key), key=lambda lot: (lot.acquired_on or date.min, lot.lot_id))
        remaining, matches, updates = quantity, [], {lot.lot_id: lot for lot in self.lots}
        instrument_cost = sum(lot.accounting_cost_units for lot in candidates)
        instrument_quantity = sum(lot.quantity for lot in candidates)
        allocated_cost = 0
        for lot in candidates:
            if not remaining:
                break
            if require_sellable and lot.sellable_at > financial_event.effective_time:
                continue
            used = min(remaining, lot.quantity)
            if lot.acquired_on is not None and delivered_on < lot.acquired_on:
                raise SimulationContractError("交割不能早于股份取得")
            if self.exact_dividend_tax and lot.acquired_on is None:
                raise SimulationContractError("精确税务转让批次缺少取得日")
            cost = lot.accounting_cost_units if used == lot.quantity else _round(Fraction(lot.accounting_cost_units * used, lot.quantity))
            if self.accounting_cost_method == "average":
                cumulative_cost = _round(Fraction(instrument_cost * (quantity - remaining + used), instrument_quantity))
                cost = cumulative_cost - allocated_cost
                allocated_cost = cumulative_cost
            matches.append(LotTransferMatch(lot.lot_id, used, cost, lot.acquired_on))
            if used == lot.quantity:
                del updates[lot.lot_id]
            else:
                updates[lot.lot_id] = replace(lot, quantity=lot.quantity - used, accounting_cost_units=lot.accounting_cost_units if self.accounting_cost_method == "average" else lot.accounting_cost_units - cost)
            remaining -= used
        if remaining:
            raise SimulationContractError("转让数量超过可卖取得批次余额，剩余批次尚不可卖或数量不足")
        if self.accounting_cost_method == "average" and instrument_quantity > quantity:
            retained = sorted((lot for lot in updates.values() if lot.instrument_hash == key), key=lambda item: item.lot_id)
            remaining_cost = instrument_cost - allocated_cost
            previous, cumulative = 0, 0
            for lot in retained:
                cumulative += lot.quantity
                allocated = _round(Fraction(remaining_cost * cumulative, instrument_quantity - quantity))
                updates[lot.lot_id] = replace(lot, accounting_cost_units=allocated - previous)
                previous = allocated
        entitlement_updates = {item.entitlement_id: item for item in self.entitlements}
        consumptions = []
        sequence = len(self.event_records) + 1
        for match in matches:
            for right in self.entitlements:
                if right.lot_id != match.lot_id or not right.remaining_quantity:
                    continue
                exact = match.quantity * right.quantity_multiplier
                if exact > right.remaining_quantity_fraction:
                    raise SimulationContractError("转让权利数量无法按来源精确承接")
                used = exact
                consumed_before = right.registered_quantity_fraction - right.remaining_quantity_fraction
                before = _round(right.prewithheld_units * consumed_before / right.registered_quantity_fraction)
                after = _round(right.prewithheld_units * (consumed_before + used) / right.registered_quantity_fraction)
                left = right.remaining_quantity_fraction - used
                consumptions.append(EntitlementConsumption(right.entitlement_id, transfer_id, match.lot_id, used.numerator, left.numerator, after - before, None, sequence, used.denominator, left.denominator))
                entitlement_updates[right.entitlement_id] = replace(right, remaining_quantity=left.numerator, remaining_quantity_denominator=left.denominator)
        record = self._record("transfer", transfer_id, financial_event.effective_time, source, (financial_event.event_id,))
        transfer = SpotTransfer(transfer_id, financial_event.event_id, key, quantity, delivered_on, financial_event.effective_time, tuple(matches), source, record.sequence)
        self.lots = tuple(sorted(updates.values(), key=lambda lot: lot.lot_id))
        self.entitlements = tuple(sorted(entitlement_updates.values(), key=lambda item: item.entitlement_id))
        self.consumptions += tuple(consumptions)
        self.transfers += (transfer,)
        return transfer

    def register_dividend(self, plan: DividendRegistrationPlan, *, effective_time: datetime, rule_hash: str, withholding: DividendWithholdingRule | None = None) -> tuple[FinancialEvent, ...]:
        self._check_time(effective_time, plan.source)
        if effective_time != plan.record_at:
            raise SimulationContractError("分红权利必须在声明的登记时点固定")
        if any(item.plan.dividend_id == plan.dividend_id for item in self.dividends):
            raise SimulationContractError("分红登记重复")
        held = tuple(lot for lot in self.lots if lot.instrument_hash == plan.instrument_hash)
        rights = tuple(DividendEntitlement(
            f"{plan.dividend_id}:{lot.lot_id}", plan.dividend_id, lot.lot_id, lot.instrument_hash,
            lot.quantity, lot.quantity, plan.taxable_per_share_numerator, plan.taxable_per_share_denominator,
            lot.acquired_on, plan.record_at.date(), plan.tax_rule_id, plan.source,
            acquisition_method=lot.acquisition_method, security_class=lot.security_class,
        ) for lot in held)
        ids = {item.entitlement_id for item in self.entitlements}
        if any(item.entitlement_id in ids for item in rights):
            raise SimulationContractError("分红权利身份重复")
        if self.exact_dividend_tax and any(lot.acquired_on is None for lot in held):
            raise SimulationContractError("精确分红税需要真实股份取得日")
        total = sum(lot.quantity for lot in held)
        cash = _round(Fraction(plan.cash_per_share_numerator * total, plan.cash_per_share_denominator))
        if withholding is not None:
            withholding.source.require_visible(effective_time)
            rate = Fraction(withholding.rate_numerator, withholding.rate_denominator)
            taxable = Fraction(plan.taxable_per_share_numerator, plan.taxable_per_share_denominator)
            withheld = _round(total * taxable * rate, withholding.rounding)
            if withheld > cash:
                raise SimulationContractError("派息预扣税超过本次毛现金股息")
            allocated, quantity = 0, 0
            updated_rights = []
            for right in rights:
                quantity += right.registered_quantity
                cumulative = _round(quantity * taxable * rate, withholding.rounding)
                updated_rights.append(replace(right, prewithheld_units=cumulative-allocated))
                allocated = cumulative
            rights = tuple(updated_rights)
            cash -= withheld
        record = self._record("dividend_registered", plan.dividend_id, effective_time, plan.source)
        self.dividends += (RegisteredDividend(plan, total, cash, False, False, record.sequence),)
        self.entitlements = tuple(sorted((*self.entitlements, *rights), key=lambda item: item.entitlement_id))
        return ()

    def recognize_dividend(self, dividend_id: str, *, effective_time: datetime, rule_hash: str) -> tuple[FinancialEvent, ...]:
        dividend = self._dividend(dividend_id)
        self._check_time(effective_time, dividend.plan.source)
        if effective_time != dividend.plan.ex_at or dividend.recognized:
            raise SimulationContractError("分红应收须在除权时确认且不能重复")
        record = self._record("dividend_recognized", dividend_id, effective_time, dividend.plan.source)
        events = () if not dividend.cash_units else (self._event(record, "corporate_action", {
            "instrument_hash": dividend.plan.instrument_hash, "cash_delta_units": 0,
            "sellable_delta": 0, "unsettled_delta": 0,
            "cash_receivable_units": dividend.cash_units,
            "receivable_id": f"dividend:{dividend_id}", "cash_due_date": dividend.plan.pay_at.date().isoformat(),
        }, rule_hash),)
        self.dividends = tuple(replace(item, recognized=True) if item.plan.dividend_id == dividend_id else item for item in self.dividends)
        return events

    def pay_dividend(self, dividend_id: str, *, effective_time: datetime, source: AccountSource, rule_hash: str) -> tuple[FinancialEvent, ...]:
        dividend = self._dividend(dividend_id)
        self._check_time(effective_time, source)
        if not dividend.recognized or dividend.paid or effective_time < dividend.plan.pay_at:
            raise SimulationContractError("分红应收未确认、重复支付或尚未到期")
        record = self._record("dividend_paid", dividend_id, effective_time, source)
        events = () if not dividend.cash_units else (self._event(record, "settlement", {"receivable_id": f"dividend:{dividend_id}", "cash_units": 0, "quantity": 0}, rule_hash),)
        self.dividends = tuple(replace(item, paid=True) if item.plan.dividend_id == dividend_id else item for item in self.dividends)
        return events

    def _dividend(self, dividend_id: str) -> RegisteredDividend:
        dividend = next((item for item in self.dividends if item.plan.dividend_id == dividend_id), None)
        if dividend is None:
            raise SimulationContractError("分红尚未登记")
        return dividend

    def assess_transfer(
        self, transfer_id: str, *, assessment_id: str, rules: Sequence[DividendTaxRule],
        effective_time: datetime, source: AccountSource, rule_hash: str,
        due_at: datetime | None = None,
    ) -> tuple[FinancialEvent, ...]:
        self._check_time(effective_time, source)
        require_aware_datetime(due_at or effective_time, "due_at")
        transfer = next((item for item in self.transfers if item.transfer_id == transfer_id), None)
        if transfer is None or any(item.assessment_id == assessment_id or item.transfer_id == transfer_id for item in self.assessments):
            raise SimulationContractError("转让不存在或税款核定重复")
        if transfer.delivered_on > effective_time.date():
            raise SimulationContractError("税务核定不能使用尚未交割的转让")
        rule_map = {rule.rule_id: rule for rule in rules}
        if len(rule_map) != len(rules):
            raise SimulationContractError("同一次核定税规则身份不能重复")
        rights = {item.entitlement_id: item for item in self.entitlements}
        lines = []
        for consumption in self.consumptions:
            if consumption.transfer_id != transfer_id:
                continue
            right = rights[consumption.entitlement_id]
            rule = rule_map.get(right.tax_rule_id)
            if rule is None or right.acquired_on is None:
                raise SimulationContractError("税权缺少明确来源税规则或取得日")
            rule.source.require_visible(effective_time)
            if rule.investor_tax_identity != self.investor_tax_identity or right.security_class not in rule.security_classes or right.acquisition_method not in rule.acquisition_methods:
                raise SimulationContractError("税规则与投资者、证券类别或取得方式不匹配")
            applicable_on = right.record_on if rule.applicability_date == "record_date" else transfer.delivered_on
            if not rule.effective_from <= applicable_on <= rule.effective_until:
                raise SimulationContractError("税规则有效区间未覆盖本次权利或转让")
            rate = resolve_holding_rate(rule, acquired_on=right.acquired_on, delivered_on=transfer.delivered_on)
            gross = _round(Fraction(consumption.quantity * right.taxable_per_share_numerator, consumption.quantity_denominator * right.taxable_per_share_denominator) * rate, rule.rounding)
            credit = consumption.prewithheld_credit_units
            refund = max(credit - gross, 0)
            if refund and rule.overwithholding_policy == "reject":
                raise SimulationContractError("来源规则未允许处理超过核定额的已代扣税款")
            if rule.overwithholding_policy == "no_refund":
                refund = 0
            lines.append(TaxAssessmentLine(right.entitlement_id, transfer_id, consumption.quantity, right.acquired_on, transfer.delivered_on, rule.rule_id, rule.source, gross, credit, max(gross - credit, 0), refund, consumption.quantity_denominator))
        total = sum(item.payable_units for item in lines)
        refund = sum(item.refund_units for item in lines)
        record = self._record("tax_assessed", assessment_id, effective_time, source, (transfer_id,))
        events = []
        if total:
            events.append(self._event(record, "tax_assessed", {"assessment_id": assessment_id, "transfer_id": transfer_id, "tax_units": total}, rule_hash))
        if refund:
            events.append(self._event(record, "corporate_action", {
                "instrument_hash": transfer.instrument_hash, "cash_delta_units": 0,
                "sellable_delta": 0, "unsettled_delta": 0, "cash_receivable_units": refund,
                "receivable_id": f"tax-refund:{assessment_id}", "cash_due_date": (due_at or effective_time).date().isoformat(),
            }, rule_hash, suffix=":refund"))
        self.assessments += (TaxAssessment(assessment_id, transfer_id, effective_time, due_at or effective_time, total, tuple(lines), source, 0, record.sequence),)
        self.consumptions = tuple(replace(item, assessment_id=assessment_id) if item.transfer_id == transfer_id else item for item in self.consumptions)
        return tuple(events)

    def collect_tax(
        self, collection_id: str, *, allocations: Sequence[TaxCollectionAllocation],
        available_cash_units: int, effective_time: datetime, source: AccountSource,
        rule_hash: str,
    ) -> tuple[FinancialEvent, ...]:
        self._check_time(effective_time, source)
        if any(item.collection_id == collection_id for item in self.collections):
            raise SimulationContractError("税款扣收身份重复")
        ids = [item.assessment_id for item in allocations]
        if not allocations or len(ids) != len(set(ids)):
            raise SimulationContractError("扣收分配不能为空或重复指向同一核定")
        if type(available_cash_units) is not int or available_cash_units < sum(item.cash_units for item in allocations):
            raise SimulationContractError("可用现金不足缴税，保留应付而不自动融资")
        for item in allocations:
            if item.cash_units > self.unpaid_tax_units(item.assessment_id):
                raise SimulationContractError("扣收分配超过已核定未付税款")
        record = self._record("tax_collected", collection_id, effective_time, source, tuple(ids))
        events = tuple(self._event(record, "tax_collected", {"collection_id": collection_id, "assessment_id": item.assessment_id, "cash_units": item.cash_units}, rule_hash, suffix=f":{item.assessment_id}") for item in sorted(allocations, key=lambda item: item.assessment_id))
        self.collections += (TaxCollection(collection_id, effective_time, tuple(sorted(allocations, key=lambda item: item.assessment_id)), source, record.sequence),)
        return events

    def unpaid_tax_units(self, assessment_id: str) -> int:
        """从核定／实际扣收事实求未付额，不保存另一份现金或应付余额。"""
        assessment = next((item for item in self.assessments if item.assessment_id == assessment_id), None)
        if assessment is None:
            raise SimulationContractError("扣收没有对应税务核定")
        collected = assessment.opening_collected_units + sum(item.cash_units for collection in self.collections for item in collection.allocations if item.assessment_id == assessment_id)
        return assessment.tax_units - collected

    def payment_obligations(self, *, as_of: datetime) -> tuple[tuple[str, int, str], ...]:
        require_aware_datetime(as_of, "as_of")
        if self.event_records and as_of < self.event_records[-1].effective_time:
            raise SimulationContractError("终态检查时点不能早于已处理账户事实")
        return tuple((item.assessment_id, self.unpaid_tax_units(item.assessment_id), "due" if item.due_at <= as_of else "pending") for item in self.assessments if self.unpaid_tax_units(item.assessment_id))

    def register_corporate_action(self, action: CorporateAction, *, effective_time: datetime, source: AccountSource) -> None:
        self._check_time(effective_time, source)
        if effective_time.date() != action.record_date or action.announcement_available_time > effective_time:
            raise SimulationContractError("公司行动登记必须在真实登记日且公告已可见")
        if any(item.action_id == action.action_id for item in self.corporate_action_records):
            raise SimulationContractError("公司行动登记重复；修订必须在登记前确定")
        held = tuple(lot for lot in self.lots if lot.instrument_hash == action.instrument_hash)
        self._record("corporate_action_registered", action.action_id, effective_time, source)
        self.corporate_action_records += (CorporateActionRecord(json.dumps(action.to_dict(), ensure_ascii=False, sort_keys=True), action.action_id, action.revision, held, False, None, None, source),)

    def record_corporate_action(
        self, action: CorporateAction, financial_event: FinancialEvent, *,
        source: AccountSource, lot_rule: CorporateActionLotRule | None = None,
        record_snapshot: tuple[SpotAcquisitionLot, ...] | None = None,
    ) -> None:
        """投影已有公司行动事件，不再生成现金／数量事件或伪造成交。"""
        self._check_time(financial_event.effective_time, source)
        values = financial_event.values()
        if financial_event.kind != "corporate_action" or financial_event.group_id != self.group_id or values.get("instrument_hash") != action.instrument_hash:
            raise SimulationContractError("公司行动投影必须绑定同证券同执行组正式事件")
        if financial_event.parent_id != action.action_id or action.announcement_available_time > financial_event.effective_time:
            raise SimulationContractError("公司行动正式事件身份或可见时点不符")
        existing = next((item for item in self.corporate_action_records if item.action_id == action.action_id), None)
        if values.get("action_phase") == "shares_arrival":
            if existing is None or not existing.applied or existing.arrival_event_id is not None or json.loads(existing.action_payload) != action.to_dict():
                raise SimulationContractError("股份到账缺少已生效逐批事实、重复到账或行动内容改变")
            if financial_event.effective_time.date() != action.shares_arrival_date or values.get("arrived_quantity") != sum(link.successor_lot.quantity for link in existing.links if link.successor_lot is not None):
                raise SimulationContractError("股份到账日期或数量与逐批经济权益不一致")
            self._record("corporate_action_arrived", action.action_id, financial_event.effective_time, source, (financial_event.event_id,))
            self.corporate_action_records = tuple(replace(item, arrival_event_id=financial_event.event_id) if item.action_id == action.action_id else item for item in self.corporate_action_records)
            return
        if financial_event.effective_time.date() != action.effective_date:
            raise SimulationContractError("公司行动正式事件日期与来源生效日不符")
        if existing is not None and (existing.applied or existing.revision != action.revision or json.loads(existing.action_payload) != action.to_dict()):
            raise SimulationContractError("公司行动重复投影或登记后修订内容变化")
        current = tuple(lot for lot in self.lots if lot.instrument_hash == action.instrument_hash)
        if action.contract_version == 2 and action.kind == "delisting_cash":
            # 退市现金注销按生效时真实批次退出，申报截止日不作为分红登记基数。
            recorded = current
        elif record_snapshot is None:
            if existing is not None:
                recorded = existing.record_lots
            elif action.record_date == action.effective_date:
                recorded = current
            else:
                raise SimulationContractError("跨日公司行动缺少真实登记批次，不能使用生效持仓补推")
        else:
            recorded = record_snapshot
            if existing is not None and recorded != existing.record_lots:
                raise SimulationContractError("提供的登记批次与正式登记事实不一致")
            if len({lot.lot_id for lot in recorded}) != len(recorded):
                raise SimulationContractError("公司行动登记批次身份重复")
            for lot in recorded:
                lot.source.require_visible(financial_event.effective_time)
                if lot.source.available_at.date() > action.record_date:
                    raise SimulationContractError("提供的登记批次来源在登记日尚不可见")
                if lot.instrument_hash != action.instrument_hash or (lot.acquired_on is not None and lot.acquired_on > action.record_date):
                    raise SimulationContractError("公司行动登记批次含其他证券或登记后取得股份")
        if action.kind == "cash_dividend":
            raise SimulationContractError("账户现金分红须显式关联 DividendRegistrationPlan，不能再次投影普通现金分红事件")
        updated = {lot.lot_id: lot for lot in self.lots}
        rights = {item.entitlement_id: item for item in self.entitlements}
        links = []
        quantity_delta = int(values.get("sellable_delta", 0)) + int(values.get("unsettled_delta", 0)) + int(values.get("position_entitlement_quantity", 0)) + int(values.get("frozen_delta", 0))
        if action.kind in {"split", "reverse_split", "stock_dividend"}:
            if lot_rule is None:
                raise SimulationContractError("送转或拆股须有来源明确的取得、可卖和税权规则")
            lot_rule.source.require_visible(financial_event.effective_time)
            if lot_rule.sellable_at < financial_event.effective_time:
                raise SimulationContractError("公司行动新增数量不能在生效前可卖")
            ratio = Fraction(action.ratio_numerator, action.ratio_denominator)
            related = tuple(item for item in self.entitlements if item.instrument_hash == action.instrument_hash and item.remaining_quantity)
            if related and lot_rule.tax_right_policy == "require_no_rights":
                raise SimulationContractError("公司行动来源规则不允许承接现存税权")
            if action.kind in {"split", "reverse_split"}:
                if lot_rule.acquisition_date_policy != "inherit" or (related and lot_rule.tax_right_policy != "scale"):
                    raise SimulationContractError("拆股须保留原取得批次，并显式按比例承接税权")
                incremental = action.contract_version == 2 and ratio >= 1
                for lot in current:
                    exact = lot.quantity * ratio
                    if exact.denominator != 1:
                        raise SimulationContractError("拆股批次零碎分配缺少来源依据")
                    quantity = int(exact) - lot.quantity if incremental else int(exact)
                    if not quantity:
                        continue
                    new_id = f"corporate:{action.action_id}:{lot.lot_id}"
                    successor = replace(lot, lot_id=new_id, quantity=quantity,
                        accounting_cost_units=0 if incremental else lot.accounting_cost_units,
                        sellable_at=max(lot.sellable_at, lot_rule.sellable_at), source=source,
                        predecessor_lot_id=lot.lot_id)
                    if not incremental:
                        del updated[lot.lot_id]
                    updated[new_id] = successor
                    carried_ids = []
                    for right in related:
                        if right.lot_id != lot.lot_id:
                            continue
                        multiplier = right.quantity_multiplier / ratio
                        if incremental:
                            # 拆股旧股份仍可卖；仅新增股份受限，两部分分别承接剩余税权。
                            remaining = right.remaining_quantity_fraction
                            consumed = right.registered_quantity_fraction - remaining
                            residual_withheld = right.prewithheld_units - _round(right.prewithheld_units * consumed / right.registered_quantity_fraction)
                            old_right_qty = remaining / ratio
                            bonus_right_qty = remaining - old_right_qty
                            old_withheld = _round(residual_withheld / ratio)
                            rights[right.entitlement_id] = replace(right, remaining_quantity=0, remaining_quantity_denominator=1)
                            for suffix, destination, part, withheld in (("existing", lot.lot_id, old_right_qty, old_withheld), ("bonus", new_id, bonus_right_qty, residual_withheld - old_withheld)):
                                identity = f"{right.entitlement_id}:{action.action_id}:{suffix}"
                                successor_right = replace(right, entitlement_id=identity, lot_id=destination,
                                    registered_quantity=part.numerator, registered_quantity_denominator=part.denominator,
                                    remaining_quantity=part.numerator, remaining_quantity_denominator=part.denominator,
                                    prewithheld_units=withheld, predecessor_entitlement_id=right.entitlement_id,
                                    quantity_multiplier_numerator=multiplier.numerator,
                                    quantity_multiplier_denominator=multiplier.denominator)
                                rights[identity] = successor_right
                                carried_ids.append(identity)
                        else:
                            rights[right.entitlement_id] = replace(right, lot_id=new_id, quantity_multiplier_numerator=multiplier.numerator, quantity_multiplier_denominator=multiplier.denominator)
                            carried_ids.append(right.entitlement_id)
                    links.append(SuccessorLotLink(lot.lot_id, lot.quantity, lot.accounting_cost_units, successor, 0, 1, 0, 0, tuple(carried_ids)))
                actual_delta = sum(link.successor_lot.quantity if incremental else link.successor_lot.quantity - link.old_quantity for link in links)
                if actual_delta != quantity_delta:
                    raise SimulationContractError("拆股正式事件数量与逐批来源比例不符")
            else:
                if lot_rule.tax_right_policy == "scale" or ratio <= 1:
                    raise SimulationContractError("送股必须保留原股份与既有税权，并明确正的新增数量")
                bonus = ratio - 1
                for lot in recorded:
                    exact = lot.quantity * bonus
                    if exact.denominator != 1:
                        raise SimulationContractError("送股批次零碎分配缺少来源依据")
                    if not exact:
                        continue
                    new_id = f"corporate:{action.action_id}:{lot.lot_id}"
                    acquired = lot.acquired_on if lot_rule.acquisition_date_policy == "inherit" else action.effective_date
                    successor = SpotAcquisitionLot(new_id, action.instrument_hash, int(exact), 0, acquired, lot_rule.sellable_at, lot_rule.acquisition_method, lot.security_class, source, lot.lot_id)
                    updated[new_id] = successor
                    links.append(SuccessorLotLink(lot.lot_id, lot.quantity, lot.accounting_cost_units, successor, 0, 1, 0, 0, ()))
                if sum(item.successor_lot.quantity for item in links) != quantity_delta:
                    raise SimulationContractError("送股正式事件数量与登记批次权利不符")
        elif action.kind == "delisting_cash":
            if quantity_delta != -sum(lot.quantity for lot in current):
                raise SimulationContractError("退市注销事件数量与真实取得批次不符")
            if not current:
                raise SimulationContractError("退市公司行动缺少持仓批次")
            self._project_transfer(financial_event, transfer_id=f"delisting:{action.action_id}", delivered_on=financial_event.effective_time.date(), source=source, quantity=sum(lot.quantity for lot in current), instrument_hash=action.instrument_hash, require_sellable=False)
            updated = {lot.lot_id: lot for lot in self.lots}
            rights = {item.entitlement_id: item for item in self.entitlements}
        else:
            raise SimulationContractError("当前股份批次未支持该公司行动，不能静默同步总量")
        self._record("corporate_action_projected", action.action_id, financial_event.effective_time, source, (financial_event.event_id,))
        record = CorporateActionRecord(json.dumps(action.to_dict(), ensure_ascii=False, sort_keys=True), action.action_id, action.revision, tuple(recorded), True, financial_event.event_id, lot_rule, source, tuple(links))
        self.corporate_action_records = tuple(item for item in self.corporate_action_records if item.action_id != action.action_id) + (record,)
        self.lots = tuple(sorted(updated.values(), key=lambda lot: lot.lot_id))
        self._apply_average_costs()
        self.entitlements = tuple(sorted(rights.values(), key=lambda item: item.entitlement_id))

    def apply_conversion(self, plan: SecurityConversionPlan, *, effective_time: datetime, rule_hash: str) -> tuple[FinancialEvent, ...]:
        self._check_time(effective_time, plan.source)
        plan.valuation_source.require_visible(effective_time)
        if effective_time != plan.cancelled_at or plan.valuation_observed_at > effective_time or plan.valuation_observed_at > plan.valuation_source.available_at:
            raise SimulationContractError("转换须在来源注销时点执行，权益估值不能来自未来")
        if any(item.plan.conversion_id == plan.conversion_id or item.plan.old_instrument_hash == plan.old_instrument_hash for item in self.conversions):
            raise SimulationContractError("证券转换重复")
        held = tuple(lot for lot in self.lots if lot.instrument_hash == plan.old_instrument_hash)
        if not held:
            raise SimulationContractError("证券转换没有对应取得批次")
        updated_rights = {item.entitlement_id: item for item in self.entitlements}
        links = []
        for lot in held:
            exact = Fraction(lot.quantity * plan.ratio_numerator, plan.ratio_denominator)
            whole = exact.numerator // exact.denominator
            fractional = exact - whole
            if fractional and plan.fractional_policy == "reject":
                raise SimulationContractError("来源未允许零碎后继权益现金化")
            rights = tuple(item for item in self.entitlements if item.lot_id == lot.lot_id and item.remaining_quantity)
            if rights and plan.tax_right_policy == "require_no_rights":
                raise SimulationContractError("来源不允许未转让红利税权承接")
            if rights and fractional:
                raise SimulationContractError("带税权零碎现金化必须另有明确转让与核定事实")
            new_cost = _round(Fraction(lot.accounting_cost_units * plan.successor_cost_numerator, plan.successor_cost_denominator))
            if not whole and new_cost:
                raise SimulationContractError("全现金化批次不能保留后继股份成本")
            cash = _round(Fraction(lot.quantity * plan.cash_per_old_share_numerator, plan.cash_per_old_share_denominator) + fractional * Fraction(plan.fractional_cash_price_numerator, plan.fractional_cash_price_denominator))
            new_id = f"{plan.conversion_id}:{lot.lot_id}"
            acquired = lot.acquired_on if plan.acquisition_date_policy == "inherit" else plan.registered_at.date()
            sellable_at = max(lot.sellable_at, plan.tradable_at)
            successor = None if not whole else SpotAcquisitionLot(new_id, plan.new_instrument_hash, whole, new_cost, acquired, sellable_at, plan.successor_acquisition_method, plan.successor_security_class, plan.source, lot.lot_id, plan.conversion_id)
            carried = []
            for right in rights:
                multiplier = right.quantity_multiplier * Fraction(plan.ratio_denominator, plan.ratio_numerator)
                updated_rights[right.entitlement_id] = replace(right, lot_id=new_id, instrument_hash=plan.new_instrument_hash, predecessor_entitlement_id=right.entitlement_id, quantity_multiplier_numerator=multiplier.numerator, quantity_multiplier_denominator=multiplier.denominator)
                carried.append(right.entitlement_id)
            links.append(SuccessorLotLink(lot.lot_id, lot.quantity, lot.accounting_cost_units, successor, fractional.numerator, fractional.denominator, cash, lot.accounting_cost_units - new_cost, tuple(carried)))
        record = self._record("security_conversion", plan.conversion_id, effective_time, plan.source)
        conversion = SecurityConversion(plan, tuple(links), False, False, False, record.sequence)
        events = (self._event(record, "security_conversion", {
            "conversion_id": plan.conversion_id, "old_instrument_hash": plan.old_instrument_hash,
            "old_quantity": sum(lot.quantity for lot in held), "new_instrument_hash": plan.new_instrument_hash,
            "successor_quantity": conversion.successor_quantity,
            "successor_entitlement_id": f"successor:{plan.conversion_id}",
            "register_at": plan.registered_at.isoformat(), "tradable_at": plan.tradable_at.isoformat(),
            "cash_receivable_units": conversion.cash_units, "receivable_id": f"conversion-cash:{plan.conversion_id}",
            "cash_due_date": plan.cash_pay_at.date().isoformat(),
        }, rule_hash),)
        self.lots = tuple(lot for lot in self.lots if lot.instrument_hash != plan.old_instrument_hash)
        self.entitlements = tuple(sorted(updated_rights.values(), key=lambda item: item.entitlement_id))
        self.conversions += (conversion,)
        return events

    def register_successor(self, conversion_id: str, *, effective_time: datetime, rule_hash: str) -> tuple[FinancialEvent, ...]:
        conversion = self._conversion(conversion_id)
        plan = conversion.plan
        self._check_time(effective_time, plan.source)
        if conversion.registered or effective_time < plan.registered_at:
            raise SimulationContractError("后继证券重复登记或尚未到登记时点")
        lots = tuple(link.successor_lot for link in conversion.links if link.successor_lot is not None)
        if {lot.lot_id for lot in lots} & {lot.lot_id for lot in self.lots}:
            raise SimulationContractError("后继证券批次身份重复")
        record = self._record("successor_registered", conversion_id, effective_time, plan.source)
        events = () if not conversion.successor_quantity else (self._event(record, "successor_registered", {"conversion_id": conversion_id, "instrument_hash": plan.new_instrument_hash, "entitlement_id": f"successor:{conversion_id}", "quantity": conversion.successor_quantity, "cash_units": 0}, rule_hash),)
        self.lots = tuple(sorted((*self.lots, *lots), key=lambda lot: lot.lot_id))
        self._apply_average_costs()
        self.conversions = tuple(replace(item, registered=True) if item.plan.conversion_id == conversion_id else item for item in self.conversions)
        return events

    def release_successor(self, conversion_id: str, *, effective_time: datetime, rule_hash: str) -> tuple[FinancialEvent, ...]:
        conversion = self._conversion(conversion_id)
        plan = conversion.plan
        self._check_time(effective_time, plan.source)
        if not conversion.registered or conversion.released or effective_time < plan.tradable_at:
            raise SimulationContractError("后继证券未登记、重复解禁或尚未可交易")
        lots = tuple(link.successor_lot for link in conversion.links if link.successor_lot is not None and link.successor_lot.lot_id not in conversion.released_lot_ids and link.successor_lot.sellable_at <= effective_time)
        if not lots and conversion.successor_quantity:
            raise SimulationContractError("后继批次仍有未到期的原股份交易限制")
        quantity = sum(lot.quantity for lot in lots)
        released_ids = tuple(sorted((*conversion.released_lot_ids, *(lot.lot_id for lot in lots))))
        completed = len(released_ids) == sum(link.successor_lot is not None for link in conversion.links)
        record = self._record("successor_released", f"{conversion_id}:{effective_time.isoformat()}", effective_time, plan.source, (conversion_id,))
        events = () if not quantity else (self._event(record, "settlement", {"instrument_hash": plan.new_instrument_hash, "quantity": quantity, "cash_units": 0}, rule_hash),)
        self.conversions = tuple(replace(item, released=completed, released_lot_ids=released_ids) if item.plan.conversion_id == conversion_id else item for item in self.conversions)
        return events

    def pay_conversion_cash(self, conversion_id: str, *, effective_time: datetime, source: AccountSource, rule_hash: str) -> tuple[FinancialEvent, ...]:
        conversion = self._conversion(conversion_id)
        self._check_time(effective_time, source)
        if conversion.cash_paid or effective_time < conversion.plan.cash_pay_at:
            raise SimulationContractError("换股现金重复支付或尚未到期")
        record = self._record("conversion_cash_paid", conversion_id, effective_time, source)
        events = () if not conversion.cash_units else (self._event(record, "settlement", {"receivable_id": f"conversion-cash:{conversion_id}", "cash_units": 0, "quantity": 0}, rule_hash),)
        self.conversions = tuple(replace(item, cash_paid=True) if item.plan.conversion_id == conversion_id else item for item in self.conversions)
        return events

    def _conversion(self, conversion_id: str) -> SecurityConversion:
        item = next((item for item in self.conversions if item.plan.conversion_id == conversion_id), None)
        if item is None:
            raise SimulationContractError("不存在已生效的证券转换")
        return item

    def pending_successor_values(self, *, as_of: datetime) -> tuple[tuple[str, int], ...]:
        """使用封存的转换期间估值，禁止从后继证券未来行情补价。"""
        require_aware_datetime(as_of, "as_of")
        if self.event_records and as_of < self.event_records[-1].effective_time:
            raise SimulationContractError("权益估值时点不能早于已处理账户事实")
        return tuple((f"successor:{item.plan.conversion_id}", _round(Fraction(item.successor_quantity * item.plan.interim_price_numerator, item.plan.interim_price_denominator))) for item in self.conversions if not item.registered)


__all__ = [
    "PreparedSpotOpening", "prepare_opening", "SpotAccountBook", "SpotTransfer",
    "EntitlementConsumption", "TaxAssessment", "TaxAssessmentLine", "TaxCollection",
    "TaxCollectionAllocation", "SecurityConversion", "SuccessorLotLink",
    "calendar_boundary", "resolve_holding_rate",
]
