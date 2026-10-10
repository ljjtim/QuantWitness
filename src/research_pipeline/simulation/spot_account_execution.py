"""把显式期初、股东权益及税款指令接入日频事件顺序。"""
from __future__ import annotations

from datetime import date, datetime
from typing import Mapping, Sequence

from research_pipeline.domain.spot_account import (
    AccountSource, DividendRegistrationPlan, DividendTaxRule, SecurityConversionPlan,
    SpotOpeningMark, SpotOpeningSnapshot, CorporateActionLotRule, DividendWithholdingRule,
)
from research_pipeline.platform import typed_canonical_hash
from .events import FinancialEvent
from .ledger import ExecutionGroup, SpotLedgerState, reduce_spot
from .orders import SimulationContractError
from .spot_account import TaxCollectionAllocation, prepare_opening


class SpotAccountExecution:
    def __init__(self, payload: Mapping[str, object], *, group: ExecutionGroup):
        expected = {"opening_snapshot", "opening_marks", "tax_rules", "dividend_plans", "conversion_plans", "tax_instructions"}
        if set(payload) - {"corporate_action_lot_rules", "settlement_calendar", "dividend_withholding_rules"} != expected:
            raise SimulationContractError("现货账户输入字段不完整或包含未知字段")
        self.credit_hook = None
        self.input = dict(payload)
        self.corporate_action_lot_rules = {key: CorporateActionLotRule.from_dict(value)
            for key, value in payload.get("corporate_action_lot_rules", {}).items()}
        self.rule_hash = typed_canonical_hash(payload)
        calendar = payload.get("settlement_calendar")
        self.calendar_sessions = ()
        self.calendar_source = None
        if calendar is not None:
            if set(calendar) != {"sessions", "source"}:
                raise SimulationContractError("结算日历必须声明交易日及来源")
            self.calendar_sessions = tuple(date.fromisoformat(item) for item in calendar["sessions"])
            if tuple(sorted(set(self.calendar_sessions))) != self.calendar_sessions:
                raise SimulationContractError("结算日历必须唯一并按交易日排序")
            self.calendar_source = AccountSource.from_dict(calendar["source"])
        self.snapshot = SpotOpeningSnapshot.from_dict(payload["opening_snapshot"])
        self.marks = tuple(SpotOpeningMark.from_dict(item) for item in payload["opening_marks"])
        self.rules = tuple(DividendTaxRule.from_dict(item) for item in payload["tax_rules"])
        self.dividends = tuple(DividendRegistrationPlan.from_dict(item) for item in payload["dividend_plans"])
        self.conversions = tuple(SecurityConversionPlan.from_dict(item) for item in payload["conversion_plans"])
        self.withholding_rules = {key: DividendWithholdingRule.from_dict(value)
            for key, value in payload.get("dividend_withholding_rules", {}).items()}
        if not set(self.withholding_rules) <= {plan.dividend_id for plan in self.dividends}:
            raise SimulationContractError("派息预扣规则必须对应本次分红登记计划")
        prepared = prepare_opening(self.snapshot, group=group, marks=self.marks, rule_hash=self.rule_hash)
        if prepared.opening_nav_units < 0:
            raise SimulationContractError("普通现货期初净资产不能为负")
        self.initial_state = prepared.ledger_state
        self.book = prepared.book
        self.opening_nav_units = prepared.opening_nav_units
        self.events: list[FinancialEvent] = []
        self.snapshots: list[dict[str, object]] = []
        self.schedule = []
        for plan in self.dividends:
            for at, phase, kind in ((plan.record_at, 0, "dividend_register"), (plan.ex_at, 1, "dividend_recognize"), (plan.pay_at, 2, "dividend_pay")):
                self.schedule.append((at, phase, plan.dividend_id, kind, plan))
        for plan in self.conversions:
            for at, phase, kind in ((plan.cancelled_at, 0, "conversion"), (plan.registered_at, 1, "successor_register"), (plan.cash_pay_at, 3, "conversion_pay")):
                self.schedule.append((at, phase, plan.conversion_id, kind, plan))
        for claim in self.snapshot.receivables:
            if claim.due_at < self.snapshot.started_at:
                raise SimulationContractError("期初逾期应收须声明实际到账时间")
            self.schedule.append((claim.due_at, 3, claim.claim_id, "opening_receivable", claim))
        for lot in self.snapshot.lots:
            if lot.sellable_at > self.snapshot.started_at:
                self.schedule.append((lot.sellable_at, 3, lot.lot_id, "opening_release", lot))
        for item in payload["tax_instructions"]:
            if item.get("kind") not in {"assess", "collect"}:
                raise SimulationContractError("税款指令必须声明核定或扣收")
            at = datetime.fromisoformat(item["effective_time"])
            if at.tzinfo is None:
                raise SimulationContractError("税款指令必须带时区")
            self.schedule.append((at, 4 if item["kind"] == "assess" else 5, item["event_id"], item["kind"], dict(item)))
        self.schedule.sort(key=lambda item: item[:3])
        if any(item[0] < self.snapshot.started_at for item in self.schedule):
            raise SimulationContractError("期初之前的权益必须纳入快照，不能重复调度")
        self.cursor = 0

    @property
    def managed_receivables(self) -> frozenset[str]:
        return frozenset([*(f"dividend:{p.dividend_id}" for p in self.dividends),
                          *(f"conversion-cash:{p.conversion_id}" for p in self.conversions),
                          *(p.claim_id for p in self.snapshot.receivables)])

    @property
    def managed_position_entitlements(self) -> frozenset[str]:
        return frozenset(right.entitlement_id for right in self.snapshot.position_entitlements)

    def protected_quantities(self, at: datetime) -> dict[str, int]:
        result = {}
        pending_initial_releases = {entry[2] for entry in self.schedule[self.cursor:] if entry[3] == "opening_release"}
        conversions = {item.plan.conversion_id: item for item in self.book.conversions}
        for lot in self.book.lots:
            conversion = conversions.get(lot.predecessor_conversion_id)
            pending_conversion_release = conversion is not None and lot.lot_id not in conversion.released_lot_ids
            if lot.lot_id in pending_initial_releases or lot.sellable_at > at or pending_conversion_release:
                # 命名权益已由通用结算从未结持仓扣除，只保护该批次剩余数量。
                named_quantity = sum(right.quantity for right in self.snapshot.position_entitlements
                                     if right.lot_id == lot.lot_id)
                result[lot.instrument_hash] = result.get(lot.instrument_hash, 0) + max(0, lot.quantity - named_quantity)
        return result

    def advance(self, state: SpotLedgerState, *, until: datetime, inclusive: bool = True,
                before_execution: bool = False):
        output = []
        while self.cursor < len(self.schedule):
            at, _, identity, kind, item = self.schedule[self.cursor]
            if at > until or (at == until and (not inclusive or before_execution and kind in {"assess", "collect"})):
                break
            common = {"effective_time": at, "rule_hash": self.rule_hash}
            if kind == "dividend_register":
                events = self.book.register_dividend(item, withholding=self.withholding_rules.get(identity), **common)
            elif kind == "dividend_recognize":
                events = self.book.recognize_dividend(identity, **common)
            elif kind == "dividend_pay":
                events = self.book.pay_dividend(identity, source=item.source, **common)
            elif kind == "conversion":
                events = self.book.apply_conversion(item, **common)
                # 仅在可见转换已生效后读取真实后继批次，继承旧股份的较晚限制。
                conversion = next(value for value in self.book.conversions if value.plan.conversion_id == identity)
                predecessor_ids = {link.old_lot_id for link in conversion.links}
                pending = [entry for entry in self.schedule[self.cursor + 1:]
                           if not (entry[3] == "opening_release" and entry[2] in predecessor_ids)
                           and not (entry[3] == "successor_release" and entry[4].new_instrument_hash == item.old_instrument_hash)]
                release_times = sorted({link.successor_lot.sellable_at for link in conversion.links
                                        if link.successor_lot is not None})
                pending.extend((release_at, 2, identity, "successor_release", item) for release_at in release_times)
                pending.sort(key=lambda entry: entry[:3])
                self.schedule = self.schedule[:self.cursor + 1] + pending
            elif kind == "successor_register":
                events = self.book.register_successor(identity, **common)
            elif kind == "successor_release":
                events = self.book.release_successor(identity, **common)
            elif kind == "conversion_pay":
                events = self.book.pay_conversion_cash(identity, source=item.source, **common)
            elif kind in {"opening_receivable", "opening_release"}:
                values = {"cash_units": 0, "quantity": 0}
                if kind == "opening_receivable":
                    values["receivable_id"] = identity
                else:
                    named = [right for right in self.snapshot.position_entitlements if right.lot_id == item.lot_id
                             and any(active.entitlement_id == right.entitlement_id for active in state.position_entitlements)]
                    values.update(instrument_hash=item.instrument_hash, quantity=item.quantity - sum(right.quantity for right in named))
                events = []
                if kind == "opening_release":
                    for right in named:
                        events.append(FinancialEvent(f"opening:entitlement:{right.entitlement_id}", "settlement", at,
                            at.date().isoformat(), state.group.group_id, self.rule_hash,
                            (("cash_units", 0), ("entitlement_id", right.entitlement_id), ("quantity", 0))))
                if kind == "opening_receivable" or values["quantity"]:
                    events.append(FinancialEvent(f"opening:{kind}:{identity}", "settlement", at,
                        at.date().isoformat(), state.group.group_id, self.rule_hash,
                        tuple(sorted(values.items()))))
                events = tuple(events)
            elif kind == "assess":
                events = self.book.assess_transfer(item["transfer_id"], assessment_id=identity,
                    rules=self.rules, source=AccountSource.from_dict(item["source"]),
                    due_at=datetime.fromisoformat(item["due_at"]), **common)
            else:
                events = self.book.collect_tax(identity,
                    allocations=tuple(TaxCollectionAllocation.from_dict(row) for row in item["allocations"]),
                    available_cash_units=state.available_cash_units,
                    source=AccountSource.from_dict(item["source"]), **common)
            for event in events:
                if self.credit_hook is not None:
                    event = self.credit_hook.enrich_account_event(state, event)
                state = reduce_spot(state, event)
                self.events.append(event)
                output.append((event, state))
                if self.credit_hook is not None:
                    state, credit_output = self.credit_hook.after_account_event(state, event)
                    output.extend(credit_output)
            self.cursor += 1
        return state, tuple(output)

    def next_settlement_session(self, at: datetime, sessions: Sequence[date]) -> date:
        known = tuple(sessions)
        if self.calendar_source is not None:
            self.calendar_source.require_visible(at)
            if not set(known).issubset(self.calendar_sessions):
                raise SimulationContractError("账户结算日历未覆盖行情交易会话")
            known = self.calendar_sessions
        following = next((session for session in known if session > at.date()), None)
        if following is None:
            raise SimulationContractError("期末T+1买入缺少下一交易日的结算日历来源")
        return following

    def preview_sell_matches(self, event: FinancialEvent):
        """复用税务取得批次的真实转让规则，返回本次卖出的FIFO匹配。"""
        from copy import copy
        preview = copy(self.book)
        source = AccountSource(f"fill:{event.event_id}", event.effective_time)
        transfer = preview.record_sell_fill(event, transfer_id=event.event_id,
            delivered_on=event.effective_time.date(), source=source)
        return transfer.matches

    def record_fill(self, event: FinancialEvent, *, sellable_at: datetime, security_class: str) -> None:
        source = AccountSource(f"fill:{event.event_id}", event.effective_time)
        if event.values()["side"] == "buy":
            self.book.record_buy_fill(event, lot_id=event.event_id, acquired_on=event.effective_time.date(),
                                      sellable_at=sellable_at, source=source, security_class=security_class)
        else:
            self.book.record_sell_fill(event, transfer_id=event.event_id,
                                       delivered_on=event.effective_time.date(), source=source)

    def cashflow_valuation_components(self, state: SpotLedgerState, *, at: datetime) -> dict[str, object]:
        """按资金生效时点读取已声明权益估值，不提前登记收盘快照。"""
        pending = self.book.pending_successor_values(as_of=at)
        return {"pending_successor_units": sum(value for _, value in pending),
                "pending_successor_values": [[identity, value] for identity, value in pending],
                "payable_units": state.payable_tax_units,
                "account_source_ref": self.snapshot.source.source_ref}

    def snapshot_at(self, state: SpotLedgerState, *, at: datetime) -> int:
        pending = sum(value for _, value in self.book.pending_successor_values(as_of=at))
        self.snapshots.append({"session": at.date().isoformat(), "valuation_time": at.isoformat(),
                               "liabilities_units": state.payable_tax_units,
                               "pending_successor_units": pending,
                               "opening_nav_units": self.opening_nav_units})
        return pending

    def finish(self, at: datetime) -> None:
        if self.book.exact_dividend_tax and any(item.assessment_id is None for item in self.book.consumptions):
            raise SimulationContractError("精确红利税存在已转让但未核定的分红权利")
        if any(status == "due" for _, _, status in self.book.payment_obligations(as_of=at)):
            raise SimulationContractError("终态存在到期未履行的税款扣收义务")

    def context(self) -> dict[str, object]:
        return {"contract_version": "research-spot-account-context-v1", **self.input,
                "book": self.book.to_dict(), "snapshots": list(self.snapshots),
                "financial_events": [event.to_dict() for event in self.events]}
