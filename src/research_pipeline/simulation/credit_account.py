"""信用账本的纯债务、计息和关联本金计算。"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from fractions import Fraction

from research_pipeline.domain.credit_account import (
    CreditAccount, CreditContract, CreditPositionLink,
)
from research_pipeline.domain.spot_account import AccountFact
from .orders import SimulationContractError


@dataclass(frozen=True)
class CreditReservation(AccountFact):
    order_id: str
    contract_id: str
    principal_units: int
    margin_units: int
    own_cash_units: int

    def __post_init__(self):
        if not self.order_id or not self.contract_id or any(type(value) is not int or value < 0 for value in (self.principal_units, self.margin_units, self.own_cash_units)):
            raise SimulationContractError("融资预占身份或金额无效")


@dataclass(frozen=True)
class CreditSaleClaim(AccountFact):
    claim_id: str
    due_date: date
    cash_units: int
    contract_ids: tuple[str, ...]
    source_event_id: str

    def __post_init__(self):
        if not self.claim_id or type(self.cash_units) is not int or self.cash_units < 0 or not self.contract_ids:
            raise SimulationContractError("受限还债卖款事实无效")


@dataclass(frozen=True)
class CreditLedgerState(AccountFact):
    contracts: tuple[CreditContract, ...] = ()
    position_links: tuple[CreditPositionLink, ...] = ()
    reservations: tuple[CreditReservation, ...] = ()
    sale_claims: tuple[CreditSaleClaim, ...] = ()
    risk_status: str = "normal"
    notified_at: datetime | None = None
    remedy_date: date | None = None

    def __post_init__(self):
        if self.risk_status not in {"normal", "notified", "liquidating", "default"}:
            raise SimulationContractError("融资风险状态无效")
        for values, name in ((self.contracts, "contract_id"), (self.position_links, "link_id"), (self.reservations, "order_id"), (self.sale_claims, "claim_id")):
            ids = [getattr(item, name) for item in values]
            if len(ids) != len(set(ids)):
                raise SimulationContractError(f"信用账本{name}重复")
        ids = {item.contract_id for item in self.contracts}
        if any(item.contract_id not in ids for item in self.position_links):
            raise SimulationContractError("信用批次引用未知合同")
        for contract in self.contracts:
            if sum(item.principal_units for item in self.position_links if item.contract_id == contract.contract_id) != contract.principal_units:
                raise SimulationContractError("信用合同与关联本金不守恒")

    @property
    def principal_units(self):
        return sum(item.principal_units for item in self.contracts)

    @property
    def interest_units(self):
        return sum(item.interest_units for item in self.contracts)

    @property
    def reserved_principal_units(self):
        return sum(item.principal_units for item in self.reservations)

    @property
    def restricted_sale_units(self):
        return sum(item.cash_units for item in self.sale_claims)

    def reservation_for(self, order_id):
        return next((item for item in self.reservations if item.order_id == order_id), None)


def opening_credit_state(definition: CreditAccount) -> CreditLedgerState:
    return CreditLedgerState(contracts=definition.contracts, position_links=definition.position_links)


def reserve_credit(state: CreditLedgerState, reservation: CreditReservation, *, limit_units: int) -> CreditLedgerState:
    reservations = {item.order_id: item for item in state.reservations}
    reservations[reservation.order_id] = reservation
    if state.principal_units + sum(item.principal_units for item in reservations.values()) > limit_units:
        raise SimulationContractError("融资授信额度不足")
    return replace(state, reservations=tuple(reservations[key] for key in sorted(reservations)))


def release_credit(state: CreditLedgerState, order_id: str) -> CreditLedgerState:
    return replace(state, reservations=tuple(item for item in state.reservations if item.order_id != order_id))


def draw_credit(state: CreditLedgerState, *, order_id: str, principal_units: int, own_cash_units: int,
                notional_units: int, quantity: int, instrument_hash: str, at: datetime,
                maturity_date: date, opening_rule_id: str, link_id: str, source_ref: str) -> CreditLedgerState:
    reservation = state.reservation_for(order_id)
    if reservation is None or not 0 <= principal_units <= reservation.principal_units or not 0 <= own_cash_units <= reservation.own_cash_units:
        raise SimulationContractError("融资成交超过本单授信或自有资金预占")
    if principal_units + own_cash_units != notional_units or quantity <= 0:
        raise SimulationContractError("融资成交本金与自有支付不守恒")
    contracts = {item.contract_id: item for item in state.contracts}
    current = contracts.get(reservation.contract_id)
    if current is None and principal_units == 0:
        remaining = replace(reservation, own_cash_units=reservation.own_cash_units-own_cash_units)
        return replace(state, reservations=tuple(remaining if item.order_id == order_id else item for item in state.reservations))
    if current is None:
        current = CreditContract(reservation.contract_id, at, maturity_date, opening_rule_id, 0, 0, 0, 1, at.date(), source_ref)
    elif current.maturity_date != maturity_date:
        raise SimulationContractError("同一融资合同到期日不一致")
    elif current.opening_rule_id != opening_rule_id:
        raise SimulationContractError("新融资成交不得沿用不同开仓规则的存量合同")
    elif current.last_accrual_date != at.date():
        raise SimulationContractError("新增本金前必须将旧本金利息推进到当日")
    contracts[current.contract_id] = replace(current, principal_units=current.principal_units + principal_units)
    links = state.position_links
    if principal_units:
        links += (CreditPositionLink(link_id, current.contract_id, instrument_hash, link_id, quantity, notional_units, principal_units, source_ref),)
    remaining_principal = reservation.principal_units - principal_units
    remaining = replace(reservation, principal_units=remaining_principal,
        own_cash_units=reservation.own_cash_units-own_cash_units,
        margin_units=(reservation.margin_units * remaining_principal // reservation.principal_units) if reservation.principal_units else 0)
    reservations = tuple(remaining if item.order_id == order_id else item for item in state.reservations)
    return replace(state, contracts=tuple(contracts[key] for key in sorted(contracts)),
        position_links=links, reservations=reservations)


def accrue_credit_interest(state: CreditLedgerState, definition: CreditAccount, *, through: date):
    """计入[last_accrual_date, through)的自然日利息，余数不跨合同混用。"""
    contracts, facts = [], []
    for contract in state.contracts:
        if through < contract.last_accrual_date:
            raise SimulationContractError("融资计息不能倒退")
        current = contract
        day = contract.last_accrual_date
        remainder = Fraction(contract.interest_remainder_numerator, contract.interest_remainder_denominator)
        while day < through:
            at = datetime.combine(day, time(0), tzinfo=contract.opened_at.tzinfo)
            if day == contract.opened_at.date():
                at = max(at, contract.opened_at)
            rule = definition.rule_at(at)
            exact = remainder + Fraction(current.principal_units * rule.annual_rate_ppm, 1_000_000 * rule.interest_day_basis)
            units = exact.numerator // exact.denominator
            before = remainder
            remainder = exact - units
            facts.append({"contract_id": contract.contract_id, "from_date": day.isoformat(),
                "through_date": (day+timedelta(days=1)).isoformat(), "principal_units": current.principal_units,
                "annual_rate_ppm": rule.annual_rate_ppm, "interest_day_basis": rule.interest_day_basis,
                "rule_id": rule.rule_id, "interest_units": units,
                "remainder_before_numerator": before.numerator, "remainder_before_denominator": before.denominator,
                "remainder_numerator": remainder.numerator, "remainder_denominator": remainder.denominator})
            current = replace(current, interest_units=current.interest_units+units)
            day += timedelta(days=1)
        contracts.append(replace(current, last_accrual_date=through,
            interest_remainder_numerator=remainder.numerator, interest_remainder_denominator=remainder.denominator))
    return replace(state, contracts=tuple(contracts)), tuple(facts)


def repay_credit(state: CreditLedgerState, *, amount_units: int, repayment_order: tuple[str, ...], contract_ids: tuple[str, ...] = ()):
    if type(amount_units) is not int or amount_units < 0 or set(repayment_order) != {"interest", "principal"}:
        raise SimulationContractError("还款金额或顺序无效")
    ids = set(contract_ids) if contract_ids else {item.contract_id for item in state.contracts}
    if not ids <= {item.contract_id for item in state.contracts}:
        raise SimulationContractError("还款合同不存在")
    contracts = {item.contract_id: item for item in state.contracts}
    links = list(state.position_links)
    remaining, allocations = amount_units, []
    for item in sorted((item for item in state.contracts if item.contract_id in ids), key=lambda item: (item.maturity_date, item.contract_id)):
        current = item
        principal_paid = interest_paid = 0
        for component in repayment_order:
            field = f"{component}_units"
            paid = min(remaining, getattr(current, field))
            current = replace(current, **{field: getattr(current, field)-paid})
            remaining -= paid
            if component == "principal":
                principal_paid = paid
            else:
                interest_paid = paid
        contracts[item.contract_id] = current
        indexes = [i for i, link in enumerate(links) if link.contract_id == item.contract_id and link.principal_units]
        released = 0
        for index in indexes:
            link = links[index]
            units = principal_paid - released if index == indexes[-1] else principal_paid * link.principal_units // item.principal_units
            released += units
            links[index] = replace(link, principal_units=link.principal_units-units)
        if principal_paid or interest_paid:
            allocations.append({"contract_id": item.contract_id, "principal_paid_units": principal_paid, "interest_paid_units": interest_paid,
                "principal_before_units": item.principal_units, "principal_after_units": current.principal_units,
                "interest_before_units": item.interest_units, "interest_after_units": current.interest_units})
    return replace(state, contracts=tuple(contracts[key] for key in sorted(contracts)), position_links=tuple(link for link in links if link.principal_units or contracts[link.contract_id].interest_units)), tuple(allocations), amount_units-remaining


def detach_credit_sale(state: CreditLedgerState, *, instrument_hash: str, quantity: int, receivable_id: str | None = None, lot_consumptions=None):
    """按实际取得批次转出关联数量，本金转向受限应收且总债务不变。"""
    links, remaining, references = [], quantity, []
    by_lot = None if lot_consumptions is None else {item["lot_id"]: item["quantity"] for item in lot_consumptions}
    if by_lot is not None and sum(by_lot.values()) != quantity:
        raise SimulationContractError("融资卖券取得批次数量与真实成交不一致")
    for item in state.position_links:
        budget = remaining if by_lot is None else by_lot.get(item.lot_id, 0)
        sold = min(budget, item.quantity) if item.instrument_hash == instrument_hash and item.receivable_id is None and item.successor_right_id is None else 0
        if sold:
            remaining -= sold
            if by_lot is not None:
                by_lot[item.lot_id] -= sold
            cost = item.acquisition_units * sold // item.quantity
            principal = item.principal_units * sold // item.quantity
            references.append({"link_id": item.link_id, "contract_id": item.contract_id, "quantity": sold,
                "acquisition_units": cost, "principal_units": principal})
            if receivable_id is not None and principal:
                links.append(replace(item, link_id=f"{receivable_id}:{item.link_id}", lot_id=receivable_id,
                    quantity=0, acquisition_units=cost, principal_units=principal, receivable_id=receivable_id))
                item = replace(item, quantity=item.quantity-sold, acquisition_units=item.acquisition_units-cost,
                    principal_units=item.principal_units-principal)
            else:
                item = replace(item, quantity=item.quantity-sold, acquisition_units=item.acquisition_units-cost)
        links.append(item)
    return replace(state, position_links=tuple(links)), tuple(references)


def credit_valuation(state, definition: CreditAccount, *, at: datetime, positions: tuple[dict, ...], pending_successor_values=()):
    """交易所现金／证券profile；会计应收与担保分子分别列示。"""
    rule = definition.rule_at(at)
    credit = state.credit_state
    prices = {item["instrument_hash"]: item for item in positions}
    collateral = {item.instrument_hash: item.rate_ppm for item in rule.collateral_rates}
    contracts = {item.contract_id: item for item in credit.contracts}
    rules = {item.rule_id: item for item in definition.rules}
    receivables = {item.receivable_id: item.cash_units for item in state.cash_receivables}
    pending = dict(pending_successor_values)
    financed_values = {}
    related, profit, requirement = [], Fraction(0), Fraction(0)
    for link in credit.position_links:
        if not link.principal_units:
            continue
        if link.receivable_id is not None:
            peer_principal = sum(item.principal_units for item in credit.position_links if item.receivable_id == link.receivable_id)
            value = Fraction(receivables.get(link.receivable_id, 0) * link.principal_units, peer_principal)
        elif link.successor_right_id is not None:
            peers = next((item.quantity for item in state.successor_entitlements if item.entitlement_id == link.successor_right_id), 0)
            fraction = min(Fraction(1), Fraction(link.principal_units, link.acquisition_units)) if link.acquisition_units else Fraction(0)
            value = Fraction(pending.get(link.successor_right_id, 0) * link.quantity, peers)*fraction if peers else Fraction(0)
        else:
            mark = prices.get(link.instrument_hash)
            if link.quantity and mark is None:
                raise SimulationContractError("融资关联缺少当时可见估值")
            gross = Fraction(0) if not link.quantity else Fraction(mark["market_value_units"] * link.quantity, mark["quantity"])
            fraction = min(Fraction(1), Fraction(link.principal_units, link.acquisition_units)) if link.acquisition_units else Fraction(0)
            value = gross * fraction
            financed_values[link.instrument_hash] = financed_values.get(link.instrument_hash, Fraction(0)) + value
        rate = collateral.get(link.instrument_hash, 0)
        delta = value - link.principal_units
        gain = delta * Fraction(rate, 1_000_000) if delta >= 0 else delta
        margin_ppm = rules[contracts[link.contract_id].opening_rule_id].opening_margin_ppm
        margin = Fraction(link.principal_units * margin_ppm, 1_000_000)
        profit += gain
        requirement += margin
        related.append({"link_id": link.link_id, "contract_id": link.contract_id,
            "principal_units": link.principal_units, "value_numerator": value.numerator, "value_denominator": value.denominator,
            "profit_numerator": gain.numerator, "profit_denominator": gain.denominator,
            "margin_ppm": margin_ppm, "collateral_rate_ppm": rate,
            "receivable_id": link.receivable_id, "successor_right_id": link.successor_right_id})
    own_collateral = Fraction(0)
    for key, mark in prices.items():
        own_value = Fraction(mark["market_value_units"]) - financed_values.get(key, Fraction(0))
        if own_value < 0:
            raise SimulationContractError("融资关联数量重复计入持仓")
        own_collateral += own_value * Fraction(collateral.get(key, 0), 1_000_000)
    cash = state.available_cash_units + state.frozen_cash_units + state.unsettled_cash_units + state.withdrawal_reserved_units
    securities = sum(item["market_value_units"] for item in positions)
    receivable = sum(receivables.values()) + sum(pending.values())
    liability = state.payable_tax_units
    principal, interest = credit.principal_units, credit.interest_units
    margin = Fraction(cash) + own_collateral + profit - requirement - interest - liability
    reserved = sum(item.margin_units for item in credit.reservations)
    assets, debt = cash + securities, principal + interest
    return {"at": at.isoformat(), "session": at.date().isoformat(), "rule_id": rule.rule_id,
        "cash_units": cash, "security_value_units": securities, "receivable_units": receivable,
        "principal_units": principal, "interest_units": interest, "other_liability_units": liability,
        "net_asset_units": cash + securities + receivable - principal - interest - liability,
        "collateral_value_units": own_collateral.numerator // own_collateral.denominator,
        "margin_available_units": margin.numerator // margin.denominator,
        "margin_numerator": margin.numerator, "margin_denominator": margin.denominator,
        "margin_reserved_units": reserved, "credit_reserved_units": credit.reserved_principal_units,
        "maintenance_ratio_numerator": assets, "maintenance_ratio_denominator": debt,
        "risk_status": credit.risk_status, "positions": list(positions), "financing_links": related,
        "own_collateral_numerator": own_collateral.numerator, "own_collateral_denominator": own_collateral.denominator,
        "financing_profit_numerator": profit.numerator, "financing_profit_denominator": profit.denominator,
        "financing_margin_numerator": requirement.numerator, "financing_margin_denominator": requirement.denominator}


def reduce_credit_event(state: CreditLedgerState, event):
    values = event.values()
    kind = event.kind
    if kind == "corporate_action" and values.get("action_kind") in {"split", "reverse_split", "stock_dividend", "delisting_cash", "code_change"}:
        if any(item.instrument_hash == values["instrument_hash"] and item.quantity
               and (item.principal_units or any(contract.contract_id == item.contract_id and contract.interest_units for contract in state.contracts))
               for item in state.position_links):
            raise SimulationContractError("融资关联持仓的送转、拆并股或退市清算须有专项本金承接合同")
    if kind == "credit_reserved":
        if values.get("action") == "release":
            return release_credit(state, values["order_id"])
        return reserve_credit(state, CreditReservation.from_dict(values["reservation"]), limit_units=values["credit_limit_units"])
    if kind == "credit_interest":
        contracts = {item.contract_id: item for item in state.contracts}
        for fact in values["accruals"]:
            contract = contracts[fact["contract_id"]]
            if contract.last_accrual_date.isoformat() != fact["from_date"] or contract.principal_units != fact["principal_units"]:
                raise SimulationContractError("计息区间与当前本金不一致")
            exact = Fraction(contract.interest_remainder_numerator, contract.interest_remainder_denominator) + Fraction(contract.principal_units * fact["annual_rate_ppm"], 1_000_000 * fact["interest_day_basis"])
            units = exact.numerator // exact.denominator
            remainder = exact-units
            if units != fact["interest_units"] or remainder != Fraction(fact["remainder_numerator"], fact["remainder_denominator"]):
                raise SimulationContractError("融资计息事实不守恒")
            contracts[contract.contract_id] = replace(contract, interest_units=contract.interest_units+units,
                last_accrual_date=date.fromisoformat(fact["through_date"]), interest_remainder_numerator=remainder.numerator,
                interest_remainder_denominator=remainder.denominator)
        return replace(state, contracts=tuple(contracts[key] for key in sorted(contracts)))
    if kind in {"credit_repayment", "credit_sale_settled"}:
        if values.get("status") == "not_executed":
            if values["cash_units"] or values["allocations"] or values.get("reason") != "no_outstanding_debt":
                raise SimulationContractError("未发生还款事实不能支付资金")
            return state
        updated, allocations, paid = repay_credit(state, amount_units=values["cash_units"],
            repayment_order=tuple(values["repayment_order"]), contract_ids=tuple(values.get("contract_ids", ())))
        if paid != values["cash_units"] or list(allocations) != values["allocations"]:
            raise SimulationContractError("还款事实与合同债务不一致")
        if kind == "credit_sale_settled":
            updated = replace(updated, sale_claims=tuple(item for item in updated.sale_claims if item.claim_id != values["claim_id"]),
                position_links=tuple(replace(item, receivable_id=None, lot_id=f"residual:{item.link_id}", source_ref=event.event_id)
                    if item.receivable_id == values["claim_id"] else item for item in updated.position_links))
        return updated
    if kind == "credit_extension":
        contracts = []
        for contract in state.contracts:
            if contract.contract_id == values["contract_id"]:
                if values["status"] == "approved":
                    contract = replace(contract, maturity_date=date.fromisoformat(values["new_maturity_date"]))
            contracts.append(contract)
        return replace(state, contracts=tuple(contracts))
    if kind == "credit_risk":
        return replace(state, risk_status=values["risk_status"],
            notified_at=None if values["notified_at"] is None else datetime.fromisoformat(values["notified_at"]),
            remedy_date=None if values["remedy_date"] is None else date.fromisoformat(values["remedy_date"]))
    if kind == "fill" and values.get("credit_drawdown") is not None:
        fact = values["credit_drawdown"]
        return draw_credit(state, order_id=values["order_id"], principal_units=fact["principal_units"],
            own_cash_units=values["notional_units"]-fact["principal_units"], notional_units=values["notional_units"],
            quantity=values["quantity"], instrument_hash=values["instrument_hash"], at=event.effective_time,
            maturity_date=date.fromisoformat(fact["maturity_date"]), opening_rule_id=fact["opening_rule_id"],
            link_id=event.event_id, source_ref=event.event_id)
    if kind == "fill" and values.get("credit_sale") is not None:
        sale = values["credit_sale"]
        updated, references = detach_credit_sale(state, instrument_hash=values["instrument_hash"],
            quantity=values["quantity"], receivable_id=sale["claim_id"], lot_consumptions=sale["lot_consumptions"])
        if list(references) != sale["link_transfers"]:
            raise SimulationContractError("融资卖券批次事实不一致")
        return replace(updated, sale_claims=(*updated.sale_claims, CreditSaleClaim(sale["claim_id"],
            date.fromisoformat(sale["due_date"]), sale["cash_units"], tuple(sale["contract_ids"]), event.event_id)))
    if kind == "security_conversion":
        conversion = values.get("credit_conversion")
        if conversion is None:
            if any(item.instrument_hash == values["old_instrument_hash"] and item.principal_units for item in state.position_links):
                raise SimulationContractError("融资换股缺少债务承接事实")
            return state
        transfers = {item["before_link_id"]: item for item in conversion["links"]}
        links = []
        for link in state.position_links:
            transfer = transfers.get(link.link_id)
            if transfer is None:
                links.append(link)
            else:
                successor = transfer["successor_link"]
                cash_link = transfer["cash_link"]
                new_links = [CreditPositionLink.from_dict(item) for item in (successor, cash_link) if item is not None]
                if sum(item.principal_units for item in new_links) != link.principal_units:
                    raise SimulationContractError("换股承接融资本金不守恒")
                links.extend(new_links)
        claim = conversion["claim"]
        claims = state.sale_claims if claim is None else (*state.sale_claims, CreditSaleClaim.from_dict(claim))
        return replace(state, position_links=tuple(links), sale_claims=claims)
    if kind == "successor_registered":
        return replace(state, position_links=tuple(replace(item, successor_right_id=None)
            if item.successor_right_id == values["entitlement_id"] else item for item in state.position_links))
    return state


__all__ = ["CreditReservation", "CreditSaleClaim", "CreditLedgerState", "opening_credit_state", "reserve_credit", "release_credit", "draw_credit", "accrue_credit_interest", "repay_credit", "detach_credit_sale", "credit_valuation", "reduce_credit_event"]
