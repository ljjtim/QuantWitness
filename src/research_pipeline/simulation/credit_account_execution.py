"""融资账户接入公共显式订单执行的钩子与时钟事件。"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, time
from fractions import Fraction

from research_pipeline.domain.credit_account import (
    CREDIT_CONTEXT_VERSION, parse_credit_account, require_credit_maturity,
)
from research_pipeline.domain.order_stream import ExplicitOrderCommand
from research_pipeline.domain import Price
from research_pipeline.platform import typed_canonical_hash
from .credit_account import (
    CreditReservation, CreditSaleClaim, opening_credit_state, accrue_credit_interest, repay_credit,
    detach_credit_sale, credit_valuation,
)
from .events import FinancialEvent
from .ledger import reduce_spot, spot_order_execution_view
from .orders import SimulationContractError, ORDER_TERMINAL_STATES
from .costs import cash_price_amount_units, quote_cash_order_fee


@dataclass(frozen=True)
class FinancingQuoteView:
    """只读候选购买力；实际账本不接收借入现金。"""
    state: object
    principal_units: int

    @property
    def available_cash_units(self):
        return self.state.available_cash_units + self.principal_units

    @property
    def positions(self):
        return self.state.positions

    @property
    def group(self):
        return self.state.group


class CreditAccountExecution:
    def __init__(self, payload, *, account_execution, commands, sessions):
        self.definition = parse_credit_account(payload)
        self.account = account_execution
        self.sessions = tuple(sessions)
        snapshot = self.account.snapshot
        if self.definition.account_id != snapshot.account_id or self.definition.as_of != snapshot.started_at:
            raise SimulationContractError("信用账户身份及起点必须与现货期初一致")
        self.allocations = {item.order_id: item for item in self.definition.order_allocations}
        submissions = {item.order_id: item for item in commands if item.action == "submit"}
        if not set(self.allocations) <= set(submissions) or any(submissions[key].side != "buy" for key in self.allocations):
            raise SimulationContractError("融资分配必须绑定实际显式买单")
        lots = {item.lot_id: item for item in snapshot.lots}
        for link in self.definition.position_links:
            if link.successor_right_id is not None:
                raise SimulationContractError("信用期初后继权益必须先以现货期初批次登记")
            if link.receivable_id is not None:
                if link.receivable_id not in {item.receivable_id for item in self.account.initial_state.cash_receivables}:
                    raise SimulationContractError("期初债务关联应收不存在")
            elif link.lot_id not in lots or lots[link.lot_id].instrument_hash != link.instrument_hash:
                raise SimulationContractError("期初融资批次与现货股份不匹配")
        for lot_id in lots:
            if sum(link.quantity for link in self.definition.position_links if link.lot_id == lot_id) > lots[lot_id].quantity:
                raise SimulationContractError("期初融资批次数量超过现货持仓")
        official_cap = 700000 if self.account.initial_state.group.market == "cn_stock" else 900000
        if any(rate.exchange_cap_ppm > official_cap or rate.rate_ppm > official_cap or (rate.security_category == "etf") != (self.account.initial_state.group.market == "cn_etf") for rule in self.definition.rules for rate in rule.collateral_rates):
            raise SimulationContractError("担保折算超过SSE股票或ETF上限")
        self.rule_hash = typed_canonical_hash(self.definition.to_dict())
        self.last_marks = {item.instrument_hash: {"amount": Fraction(item.price_numerator_units, item.price_denominator),
            "observed_at": item.observed_at.isoformat(), "available_at": item.source.available_at.isoformat(), "source_ref": item.source.source_ref}
            for item in self.account.marks}
        self.events = []
        self._event_ids = set()
        self.valuations = []
        self.snapshots = []
        self.risk_commands = []
        self.instruction_index = 0
        self.instructions = tuple(sorted(enumerate(self.definition.instructions), key=lambda item: (item[1].effective_at, item[0])))
        self.sequence = 0
        self.opening_nav_units = None
        self.total_interest_units = 0
        self.interest_paid_units = 0

    def initialize(self, state):
        state = replace(state, credit_state=opening_credit_state(self.definition), cashflow_tracking=True)
        value = self.value(state, self.definition.as_of)
        if value["net_asset_units"] < 0:
            raise SimulationContractError("信用账户期初净资产不能为负")
        self.opening_nav_units = value["net_asset_units"]
        self.valuations.append({**value, "observation_kind": "opening"})
        return state

    @property
    def next_at(self):
        return None if self.instruction_index == len(self.instructions) else self.instructions[self.instruction_index][1].effective_at

    @property
    def managed_receivables(self):
        return frozenset(item.claim_id for item in getattr(self, "_state_credit", ()).sale_claims) if hasattr(self, "_state_credit") else frozenset()

    def mark(self, instrument_hash, price, *, at, source_ref):
        self.last_marks[instrument_hash] = {"amount": Fraction(price.units * 100, 10**price.scale),
            "price_units": price.units, "price_scale": price.scale,
            "observed_at": at.isoformat(), "available_at": at.isoformat(), "source_ref": source_ref}

    def value(self, state, at):
        rule = self.definition.rule_at(at)
        rates = {item.instrument_hash: item for item in rule.collateral_rates}
        if state.group.market == "cn_stock":
            for lot in state.positions:
                if not lot.sellable+lot.unsettled+lot.frozen:
                    continue
                rate = rates.get(lot.instrument_hash)
                if rate is None:
                    raise SimulationContractError("股票存续估值缺少有来源担保分类及静态PE事实")
                known = [item for item in rule.stock_risk_facts if item.instrument_hash == lot.instrument_hash and item.session == at.date() and item.available_at <= at]
                if any((item.static_pe_ppm >= 300000000 or item.static_pe_ppm < 0) and rate.rate_ppm for item in known):
                    raise SimulationContractError("股票担保折算与已知静态PE事实矛盾")
        positions = []
        for lot in state.positions:
            quantity = lot.sellable+lot.unsettled+lot.frozen
            if not quantity:
                continue
            mark = self.last_marks.get(lot.instrument_hash)
            if mark is None or datetime.fromisoformat(mark["available_at"]) > at:
                raise SimulationContractError("信用账户缺少当时可见证券价格")
            amount = mark["amount"] * quantity
            positions.append({"instrument_hash": lot.instrument_hash, "quantity": quantity,
                "market_value_units": (amount.numerator*2+amount.denominator)//(amount.denominator*2),
                **{key: value for key, value in mark.items() if key != "amount"},
                "price_numerator_units": mark["amount"].numerator, "price_denominator": mark["amount"].denominator})
        pending = self.account.book.pending_successor_values(as_of=at)
        return credit_valuation(state, self.definition, at=at, positions=tuple(positions), pending_successor_values=pending)

    def _apply(self, state, kind, at, values, *, parent_id=None):
        self.sequence += 1
        event = FinancialEvent(f"credit:{self.sequence}:{kind}", kind, at, at.date().isoformat(),
            state.group.group_id, self.rule_hash, tuple(sorted(values.items())), parent_id)
        state = reduce_spot(state, event)
        self.record(event, state)
        return state, (event, state)

    def record(self, event, state):
        if event.event_id not in self._event_ids:
            self._event_ids.add(event.event_id)
            self.events.append(event.to_dict())
            if event.kind == "credit_interest":
                self.total_interest_units += sum(item["interest_units"] for item in event.values()["accruals"])
            if event.kind in {"credit_repayment", "credit_sale_settled"}:
                self.interest_paid_units += sum(item["interest_paid_units"] for item in event.values()["allocations"])
        self._state_credit = state.credit_state

    def advance(self, state, *, until):
        output = []
        state, facts = self.accrue(state, until)
        output.extend(facts)
        for claim in tuple(state.credit_state.sale_claims):
            if claim.claim_id in self.account.managed_receivables:
                continue
            if claim.due_date < until.date() or claim.due_date == until.date() and until.time().replace(tzinfo=None) >= time(9, 15):
                event = self.sale_settlement_event(state, claim.claim_id, until)
                state = reduce_spot(state, event)
                self.record(event, state)
                output.append((event, state))
        while self.next_at is not None and self.next_at <= until:
            _, instruction = self.instructions[self.instruction_index]
            at = instruction.effective_at
            if instruction.kind == "repay":
                if instruction.status == "approved":
                    ids = () if instruction.contract_id is None else (instruction.contract_id,)
                    selected = [item for item in state.credit_state.contracts if not ids or item.contract_id in ids]
                    debt = sum(item.principal_units+item.interest_units for item in selected)
                    if not debt:
                        paid, allocations = 0, ()
                    else:
                        state, allocations, paid = self._repayment_quote(state, min(instruction.amount_units, debt), ids, at)
                    settled = max(0, state.available_cash_units-state.unwithdrawable_sale_units-state.payable_tax_units)
                    if paid > settled:
                        raise SimulationContractError("直接还款不得使用订单、出金预占及未结算卖款")
                    state, fact = self._apply(state, "credit_repayment", at,
                        {"instruction_id": instruction.instruction_id, "cash_units": paid,
                         "requested_units": instruction.amount_units, "status": "settled" if paid else "not_executed",
                         "reason": "" if paid else "no_outstanding_debt",
                         "contract_ids": list(ids), "repayment_order": list(self.definition.rule_at(at).repayment_order),
                         "allocations": list(allocations)}, parent_id=instruction.instruction_id)
                    output.append(fact)
            else:
                contract = next((item for item in state.credit_state.contracts if item.contract_id == instruction.contract_id), None)
                if contract is None:
                    raise SimulationContractError("展期引用未知融资合同")
                if instruction.status == "approved":
                    if at.date() > contract.maturity_date or instruction.new_maturity_date <= contract.maturity_date:
                        raise SimulationContractError("展期须在到期前批准且延长原期限")
                    require_credit_maturity(at.date(), instruction.new_maturity_date, self.definition.rule_at(at).max_contract_days)
                state, fact = self._apply(state, "credit_extension", at,
                    {"instruction_id": instruction.instruction_id, "contract_id": instruction.contract_id,
                     "status": instruction.status, "new_maturity_date": instruction.new_maturity_date.isoformat(),
                     "approval_at": None if instruction.approval_at is None else instruction.approval_at.isoformat(),
                     "approval_source_ref": instruction.approval_source_ref, "opening_rule_id": contract.opening_rule_id}, parent_id=instruction.instruction_id)
                output.append(fact)
            self.instruction_index += 1
        return state, tuple(output)

    def accrue(self, state, at):
        _, accruals = accrue_credit_interest(state.credit_state, self.definition, through=at.date())
        if not accruals:
            return state, ()
        state, fact = self._apply(state, "credit_interest", at, {"accruals": list(accruals)})
        return state, (fact,)

    def _repayment_quote(self, state, units, contracts, at):
        _, allocations, paid = repay_credit(state.credit_state, amount_units=units,
            repayment_order=self.definition.rule_at(at).repayment_order, contract_ids=contracts)
        return state, allocations, paid

    def sale_settlement_event(self, state, claim_id, at, *, cash_already_settled=False):
        claim = next(item for item in state.credit_state.sale_claims if item.claim_id == claim_id)
        _, allocations, paid = self._repayment_quote(state, claim.cash_units, claim.contract_ids, at)
        self.sequence += 1
        return FinancialEvent(f"credit:{self.sequence}:sale-settled", "credit_sale_settled", at, at.date().isoformat(),
            state.group.group_id, self.rule_hash, tuple(sorted({"claim_id": claim_id, "cash_units": paid,
            "settled_cash_units": claim.cash_units, "cash_already_settled": cash_already_settled, "contract_ids": list(claim.contract_ids),
            "repayment_order": list(self.definition.rule_at(at).repayment_order), "allocations": list(allocations)}.items())), claim.source_event_id)

    def reserve(self, execution, state, command, quantity, price, at, session, rules_hash, policy):
        allocation = self.allocations.get(command.order_id)
        if allocation is None:
            if command.side == "buy" and state.credit_state.risk_status != "normal":
                raise SimulationContractError("融资风险通知后暂停新增买入")
            return None
        if state.credit_state.risk_status != "normal":
            raise SimulationContractError("融资账户风险未补足，暂停新增融资")
        rule = self.definition.rule_at(at)
        if command.instrument.venue != "XSHG":
            raise SimulationContractError("融资profile仅支持SSE证券")
        official_cap = 700000 if command.instrument.asset_class == "cn_stock" else 900000
        if any(item.exchange_cap_ppm > official_cap or item.rate_ppm > official_cap for item in rule.collateral_rates):
            raise SimulationContractError("担保折算超过SSE股票或ETF上限")
        if command.instrument.instrument_hash not in rule.eligible_instruments:
            raise SimulationContractError("当时证券不具有融资资格")
        require_credit_maturity(at.date(), allocation.maturity_date, rule.max_contract_days)
        notional = cash_price_amount_units(price, quantity, cash_scale=2)
        existing = next((item for item in state.credit_state.contracts if item.contract_id == allocation.contract_id), None)
        if existing is not None and existing.opening_rule_id != rule.rule_id:
            raise SimulationContractError("新融资成交不得追加不同开仓规则的存量合同")
        used = sum(item.principal_units for item in state.credit_state.contracts if item.contract_id == allocation.contract_id)
        principal = min(notional, max(0, allocation.financing_limit_units-used))
        own_used = sum(event.values()["notional_units"]-event.values().get("credit_drawdown", {}).get("principal_units", 0)
            for event in execution.events if event.kind == "fill" and event.values().get("order_id") == command.order_id)
        value = self.value(state, at)
        old = state.credit_state.reservation_for(command.order_id)
        reserved = value["margin_reserved_units"]-(0 if old is None else old.margin_units)
        credit_reserved = value["credit_reserved_units"]-(0 if old is None else old.principal_units)
        if command.funds_policy == "resize":
            principal = min(principal, max(0, rule.credit_limit_units-state.credit_state.principal_units-credit_reserved),
                max(0, value["margin_available_units"]-reserved)*1_000_000//rule.opening_margin_ppm)
            own = min(notional-principal, max(0, allocation.own_cash_limit_units-own_used))
        else:
            own = notional-principal
            if own+own_used > allocation.own_cash_limit_units:
                raise SimulationContractError("本单自有成交分配不足")
        planned_notional = principal+own
        if planned_notional <= 0:
            raise SimulationContractError("剩余融资额度和自有成交分配不足")
        fee = quote_cash_order_fee(policy, execution.cost_state_for(command.order_id),
            fill_id=f"credit-estimate:{command.order_id}:{len(execution.events)}", notional_units=planned_notional).fee_units
        cash_reservation = state.reservation_for(command.order_id)
        free_cash = state.available_cash_units+(0 if cash_reservation is None else cash_reservation.cash_units)
        if command.funds_policy == "resize":
            own = min(own, max(0, free_cash-fee))
            planned_notional = principal+own
        margin = (principal*rule.opening_margin_ppm+999999)//1000000
        if margin > value["margin_available_units"]-reserved:
            raise SimulationContractError("融资保证金可用余额不足")
        exposure = next((item["market_value_units"] for item in value["positions"] if item["instrument_hash"] == command.instrument.instrument_hash), 0)+planned_notional
        asset_total = value["cash_units"]+value["security_value_units"]+principal-fee
        if exposure*1000000 > asset_total*rule.concentration_ppm:
            raise SimulationContractError("融资证券集中度超限")
        if command.instrument.asset_class == "cn_stock":
            facts = [item for item in rule.stock_risk_facts if item.instrument_hash == command.instrument.instrument_hash and item.session == session and item.available_at <= at]
            if len(facts) != 1:
                raise SimulationContractError("股票融资缺少前一交易会话集中度事实")
            fact = facts[0]
            if self.account.calendar_source is not None:
                self.account.calendar_source.require_visible(at)
            previous = [day for day in (self.account.calendar_sessions or self.sessions) if day < session]
            if previous and fact.previous_session != previous[-1]:
                raise SimulationContractError("股票融资集中度事实未对应前一会话")
            blocked = fact.market_collateral_ratio_ppm >= 250000 and (fact.static_pe_ppm >= 300000000 or fact.static_pe_ppm < 0)
            if any(getattr(fact, field) is None for field in ("client_security_value_units", "client_asset_units", "client_debt_units")):
                raise SimulationContractError("股票融资缺少客户前一会话担保集中度与负债事实")
            previous_snapshots = [item for item in self.snapshots if item["session"] == fact.previous_session.isoformat()]
            if previous_snapshots:
                prior = previous_snapshots[-1]
                prior_security = next((item["market_value_units"] for item in prior["positions"] if item["instrument_hash"] == command.instrument.instrument_hash), 0)
                if (fact.client_security_value_units, fact.client_asset_units, fact.client_debt_units) != (prior_security, prior["maintenance_ratio_numerator"], prior["maintenance_ratio_denominator"]):
                    raise SimulationContractError("股票融资前一会话客户事实与已封存账户不符")
            client_high = fact.client_security_value_units*1000000 >= fact.client_asset_units*700000
            low_maintenance = fact.client_debt_units and fact.client_asset_units*1000000 <= 3000000*fact.client_debt_units
            if blocked and client_high and low_maintenance:
                raise SimulationContractError("股票触发交易所担保物集中度暂停融资条件")
        state = execution.apply_financial_event(state, "cash_reserved", at, session, rules_hash, command.order_id, {"cash_units": own+fee})
        return execution.apply_financial_event(state, "credit_reserved", at, session, rules_hash, command.order_id,
            {"reservation": CreditReservation(command.order_id, allocation.contract_id, principal, margin, own).to_dict(), "credit_limit_units": rule.credit_limit_units})

    def quote_view(self, state, command):
        own = spot_order_execution_view(state, command.order_id)
        reservation = state.credit_state.reservation_for(command.order_id)
        if reservation is None:
            return own
        cash = state.reservation_for(command.order_id)
        return FinancingQuoteView(replace(own, available_cash_units=0 if cash is None else cash.cash_units), reservation.principal_units)

    def enrich_fill(self, state, command, values, at, event_id):
        reservation = state.credit_state.reservation_for(command.order_id)
        if values["side"] == "buy" and reservation is not None:
            principal = min(values["notional_units"], reservation.principal_units)
            own = values["notional_units"]-principal
            cash = state.reservation_for(command.order_id)
            if own > reservation.own_cash_units or cash is None or own+values["fee_units"] > cash.cash_units:
                raise SimulationContractError("融资成交费用及自有部分预占不足")
            allocation = self.allocations[command.order_id]
            values = {**values, "credit_drawdown": {"principal_units": principal,
                "contract_id": allocation.contract_id, "opening_rule_id": self.definition.rule_at(at).rule_id,
                "maturity_date": allocation.maturity_date.isoformat()}}
        if values["side"] == "sell":
            claim_id = f"credit-sale:{event_id}"
            preview = FinancialEvent(event_id, "fill", at, at.date().isoformat(), state.group.group_id,
                self.rule_hash, tuple(sorted({**values, "order_id": command.order_id}.items())), command.order_id)
            lot_consumptions = [{"lot_id": item.lot_id, "quantity": item.quantity}
                for item in self.account.preview_sell_matches(preview)]
            _, links = detach_credit_sale(state.credit_state, instrument_hash=values["instrument_hash"],
                quantity=values["quantity"], receivable_id=claim_id, lot_consumptions=lot_consumptions)
            active = [item for item in links if item["principal_units"] or any(c.contract_id == item["contract_id"] and c.interest_units for c in state.credit_state.contracts)]
            if active:
                net = max(0, values["notional_units"]-values["fee_units"])
                restricted = net*sum(item["quantity"] for item in active)//values["quantity"]
                due = self.account.next_settlement_session(at, self.sessions)
                values = {**values, "credit_sale": {"claim_id": claim_id, "due_date": due.isoformat(),
                    "cash_units": restricted, "contract_ids": sorted({item["contract_id"] for item in active}), "link_transfers": list(links), "lot_consumptions": lot_consumptions}}
        return values

    def record_account_fill(self, event, *, settlement_days):
        """在每笔成交发布时更新取得批次，供同刻下一笔成交使用。"""
        sellable_at = event.effective_time
        if event.values()["side"] == "buy" and settlement_days:
            session = self.account.next_settlement_session(event.effective_time, self.sessions)
            sellable_at = datetime.combine(session, time(9, 15), event.effective_time.tzinfo)
        self.account.record_fill(event, sellable_at=sellable_at,
            security_class="equity" if self.account.initial_state.group.market == "cn_stock" else "etf")

    def enrich_account_event(self, state, event):
        values = event.values()
        if event.kind != "security_conversion":
            return event
        conversion = next(item for item in self.account.book.conversions if item.plan.conversion_id == values["conversion_id"])
        originals = {item.old_lot_id: item for item in conversion.links}
        transfers, contracts, cash_units = [], set(), 0
        claim_id = values["receivable_id"]
        for link in state.credit_state.position_links:
            if link.instrument_hash != values["old_instrument_hash"] or not link.quantity or link.receivable_id is not None:
                continue
            source = originals.get(link.lot_id)
            if source is None:
                raise SimulationContractError("融资换股关联没有真实取得批次")
            plan = conversion.plan
            new_quantity = link.quantity*plan.ratio_numerator//plan.ratio_denominator
            successor_principal = link.principal_units*plan.successor_cost_numerator//plan.successor_cost_denominator if new_quantity else 0
            cash_principal = link.principal_units-successor_principal
            acquisition = link.acquisition_units*plan.successor_cost_numerator//plan.successor_cost_denominator if new_quantity else 0
            successor = None if not new_quantity else replace(link, instrument_hash=values["new_instrument_hash"],
                lot_id=source.successor_lot.lot_id, quantity=new_quantity, acquisition_units=acquisition,
                principal_units=successor_principal, successor_right_id=values["successor_entitlement_id"])
            cash_link = None if not cash_principal else replace(link, link_id=f"{claim_id}:{link.link_id}", lot_id=claim_id,
                quantity=0, acquisition_units=link.acquisition_units-acquisition, principal_units=cash_principal,
                successor_right_id=None, receivable_id=claim_id)
            if cash_link is not None:
                contracts.add(link.contract_id)
                cash_units += source.cash_consideration_units*link.quantity//source.old_quantity
            transfers.append({"before_link_id": link.link_id,
                "successor_link": None if successor is None else successor.to_dict(), "cash_link": None if cash_link is None else cash_link.to_dict()})
        claim = None
        if contracts:
            claim = CreditSaleClaim(claim_id, plan.cash_pay_at.date(), cash_units, tuple(sorted(contracts)), event.event_id).to_dict()
        payload = {**values, "credit_conversion": {"links": transfers, "claim": claim,
            "principal_allocation_policy": "source_successor_cost_fraction"}}
        return replace(event, payload=tuple(sorted(payload.items())))

    def after_account_event(self, state, event):
        claim_id = event.values().get("receivable_id")
        if event.kind != "settlement" or claim_id not in {item.claim_id for item in state.credit_state.sale_claims}:
            return state, ()
        repayment = self.sale_settlement_event(state, claim_id, event.effective_time, cash_already_settled=True)
        state = reduce_spot(state, repayment)
        self.record(repayment, state)
        return state, ((repayment, state),)

    def withdrawal_allowed(self, state, at, units, event_id=None):
        value = self.value(state, at)
        rule = self.definition.rule_at(at)
        own = dict(state.withdrawal_reservations).get(event_id, 0)
        assets, debt = value["maintenance_ratio_numerator"], value["maintenance_ratio_denominator"]
        margin = value["margin_available_units"]-value["margin_reserved_units"]-state.withdrawal_reserved_units+own
        return state.credit_state.risk_status == "normal" and units <= margin and (
            not debt or (assets*1000000 > rule.withdrawal_ratio_ppm*debt and (assets-units)*1000000 >= rule.withdrawal_ratio_ppm*debt))

    def check_risk(self, state, at, *, explicit=None):
        value = self.value(state, at)
        self.valuations.append(value)
        rule = self.definition.rule_at(at)
        debt = value["maintenance_ratio_denominator"]
        overdue = any((item.principal_units or item.interest_units) and (at.date() > item.maturity_date or at.date() == item.maturity_date and at.time().replace(tzinfo=None) >= time(15)) for item in state.credit_state.contracts)
        low = debt and value["maintenance_ratio_numerator"]*1000000 < debt*rule.maintenance_ratio_ppm
        remedied = not debt or value["maintenance_ratio_numerator"]*1000000 >= debt*rule.remedy_ratio_ppm
        current = state.credit_state
        status, notified, deadline = current.risk_status, current.notified_at, current.remedy_date
        if overdue:
            status, notified, deadline = "liquidating", notified or at, at.date()
        elif status == "normal" and low:
            status, notified = "notified", at
            calendar = self.account.calendar_sessions or self.sessions
            if self.account.calendar_source is not None:
                self.account.calendar_source.require_visible(at)
            following = [day for day in calendar if day > at.date()]
            if rule.risk_grace_days > len(following):
                raise SimulationContractError("追保期限需要覆盖补足会话的已知结算日历")
            deadline = following[rule.risk_grace_days-1] if rule.risk_grace_days else at.date()
        elif status != "normal" and remedied:
            status, notified, deadline = "normal", None, None
        if status == "notified" and deadline is not None and (at.date() > deadline or at.date() == deadline and at.time().replace(tzinfo=None) >= time(15)):
            status = "liquidating"
        output = []
        if (status, notified, deadline) != (current.risk_status, current.notified_at, current.remedy_date):
            state, fact = self._apply(state, "credit_risk", at, {"risk_status": status,
                "notified_at": None if notified is None else notified.isoformat(),
                "remedy_date": None if deadline is None else deadline.isoformat(),
                "reason": "maturity" if overdue else "maintenance", "valuation": value})
            output.append(fact)
        if explicit is not None and status != "normal":
            for command in explicit.submitted_commands:
                order_id = command.order_id
                if order_id.startswith("credit-risk:"):
                    continue
                order = explicit.broker.orders.get(order_id)
                if order is not None and order.status not in ORDER_TERMINAL_STATES:
                    state = explicit.cancel_active_order(state, order_id, at=at, session=at.date(), rules_hash=self.rule_hash, reason="credit_risk")
        return state, tuple(output)

    def submit_risk_orders(self, state, *, at, explicit, instruments, resolve):
        if explicit is None or state.credit_state.risk_status != "liquidating":
            return state
        trigger = state.credit_state.notified_at
        if trigger is None or at <= trigger:
            return state
        contracts = {item.contract_id: item for item in state.credit_state.contracts}
        ordered = sorted(state.credit_state.position_links, key=lambda item: (contracts[item.contract_id].maturity_date, item.contract_id, item.instrument_hash))
        hashes = list(dict.fromkeys(item.instrument_hash for item in ordered if item.principal_units))
        for instrument_hash in hashes:
            lot = next((item for item in state.positions if item.instrument_hash == instrument_hash), None)
            if lot is None or lot.sellable <= 0:
                continue
            instrument = instruments[instrument_hash]
            mark = self.last_marks[instrument_hash]
            scale = 3
            price = Price(int(mark["amount"]*10), scale, "CNY")
            identity = f"credit-risk:{at.date().isoformat()}:{instrument.instrument_id}"
            if identity in explicit.broker.orders:
                continue
            command = ExplicitOrderCommand(command_id=identity, action="submit", order_id=identity,
                instrument=instrument, decision_time=at, submitted_at=at, available_at=at,
                source_sequence=len(self.risk_commands), source_hashes=(self.rule_hash,), trading_date=at.date(),
                side="sell", quantity=lot.sellable, position_effect="auto", order_type="market", time_in_force="DAY",
                reference_price=price, reference_price_available_at=datetime.fromisoformat(mark["available_at"]), funds_policy="reject")
            self.risk_commands.append(command.to_dict())
            state = explicit.submit_system_command(command, session=at.date(), state=state, resolve=resolve)
        return state

    def snapshot(self, state, at):
        value = self.value(state, at)
        self.snapshots.append(value)
        return value

    def finish(self, state, at):
        if self.next_at is not None:
            raise SimulationContractError("融资指令未全部进入声明窗口")
        credit = state.credit_state
        if credit.reservations:
            raise SimulationContractError("融资订单终结后授信预占未释放")
        overdue = any((item.principal_units or item.interest_units) and item.maturity_date <= at.date() for item in credit.contracts)
        if overdue or credit.risk_status in {"liquidating", "default"}:
            raise SimulationContractError("未解决融资违约：到期欠款或风险处置尚未完成")
        value = self.value(state, at)
        return {"contract_version": CREDIT_CONTEXT_VERSION, "account_id": self.definition.account_id,
            "currency": "CNY", "cash_scale": 2, "definition": self.definition.to_dict(),
            "events": list(self.events), "valuations": list(self.valuations), "snapshots": list(self.snapshots),
            "opening_nav_units": self.opening_nav_units, "closing_nav_units": value["net_asset_units"],
            "closing_state": credit.to_dict(), "risk_status": credit.risk_status,
            "risk_commands": list(self.risk_commands), "interest_accrued_units": self.total_interest_units,
            "interest_paid_units": self.interest_paid_units}


__all__ = ["CreditAccountExecution", "FinancingQuoteView"]
