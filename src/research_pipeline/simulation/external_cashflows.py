"""外部资金流的独立预占、逐笔估值和时间加权收益。"""
from __future__ import annotations

from datetime import datetime, time
from typing import Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.domain.external_cashflows import ExternalCashflow, parse_external_cashflows
from research_pipeline.platform import typed_canonical_hash
from .events import FinancialEvent
from .ledger import SpotLedgerState, reduce_spot
from .orders import SimulationContractError

_ZONE = ZoneInfo("Asia/Shanghai")


class CashflowReturns:
    """正净资产区间连乘；全额提款暂停，投资导致非正净资产则停止收益结论。"""

    def __init__(self, opening_nav_units: int):
        self.opening = opening_nav_units
        self.previous = opening_nav_units
        self.net_flow = 0
        self.wealth = 1.0
        self.peak = 1.0
        self.maximum_drawdown = 0.0
        self.funded = opening_nav_units > 0
        self.invalid = opening_nav_units < 0
        self.series: list[dict[str, object]] = []
        self.prior_daily_flow = 0

    @property
    def status(self) -> str:
        if self.invalid:
            return "not_applicable_nonpositive_nav"
        return "applicable" if self.funded else "waiting_for_funding"

    def _observe(self, nav_units: int) -> None:
        if self.previous > 0:
            if nav_units <= 0:
                self.invalid = True
            elif not self.invalid:
                self.wealth *= nav_units / self.previous
                self.peak = max(self.peak, self.wealth)
                self.maximum_drawdown = min(self.maximum_drawdown, self.wealth / self.peak - 1.0)
        elif nav_units < 0 or self.previous < 0:
            self.invalid = True
        elif nav_units > 0 and self.previous == 0:
            # 没有外部注资却从零出现净资产，无法定义这一段投资收益率。
            self.invalid = True
        self.previous = nav_units

    def flow(self, before: int, after: int, signed_flow_units: int) -> None:
        if after != before + signed_flow_units:
            raise SimulationContractError("资金流前后净资产不守恒")
        if signed_flow_units == 0:
            return
        self._observe(before)
        self.net_flow += signed_flow_units
        if after < 0:
            self.invalid = True
        if before == 0 and after > 0 and signed_flow_units > 0:
            self.funded = True
        self.previous = after

    def close(self, session: str, nav_units: int) -> None:
        self._observe(nav_units)
        row = {"session": session, "nav_units": nav_units,
               "net_flow_units": self.net_flow - self.prior_daily_flow,
               "cumulative_net_flow_units": self.net_flow,
               "investment_pnl_units": nav_units - self.opening - self.net_flow,
               "return_status": self.status}
        if self.status == "applicable":
            row.update(net_value=self.wealth, drawdown=self.wealth / self.peak - 1.0)
        self.series.append(row)
        self.prior_daily_flow = self.net_flow

    def summary(self, closing_nav_units: int) -> dict[str, object]:
        result = {"opening_nav_units": self.opening, "closing_nav_units": closing_nav_units,
                  "net_external_flow_units": self.net_flow,
                  "investment_pnl_units": closing_nav_units - self.opening - self.net_flow,
                  "return_status": self.status}
        if self.status == "applicable":
            result.update(total_return=self.wealth - 1.0, max_drawdown=self.maximum_drawdown)
        return result


class ExternalCashflowExecution:
    def __init__(self, plans: Sequence[ExternalCashflow], *, state: SpotLedgerState,
                 account_id: str, sessions: Sequence, started_at: datetime,
                 opening_nav_units: int, withdrawal_check=None):
        self.withdrawal_check = withdrawal_check
        self.plans = parse_external_cashflows(plans)
        self.account_id = account_id
        self.rule_hash = typed_canonical_hash({"contract_version": "research-external-cashflow-v1",
                                              "plans": [plan.to_dict() for plan in self.plans]})
        self.events: list[dict[str, object]] = []
        self.valuations: list[dict[str, object]] = []
        self.returns = CashflowReturns(opening_nav_units)
        self.rejections: dict[str, str] = {}
        self.schedule = []
        for index, plan in enumerate(self.plans):
            at = plan.effective_at.astimezone(_ZONE)
            if plan.account_id != account_id or plan.currency != state.group.currency:
                raise SimulationContractError("资金流账户或币种与普通现货账户不一致")
            if plan.accepted_at < started_at:
                raise SimulationContractError("资金流受理不能早于期初账户时点")
            if at.date() not in sessions or at.timetz().replace(tzinfo=None) not in {time(9, 30), time(15)}:
                raise SimulationContractError("资金流生效仅支持声明交易日开盘 09:30 或收盘 15:00")
            self.schedule.extend(((plan.accepted_at, index, 0, plan), (plan.effective_at, index, 1, plan)))
        self.schedule.sort(key=lambda row: row[:3])
        self.cursor = 0

    @property
    def next_at(self) -> datetime | None:
        return None if self.cursor == len(self.schedule) else self.schedule[self.cursor][0]

    def _event(self, plan: ExternalCashflow, at: datetime, kind: str, values: Mapping[str, object],
               *, state: SpotLedgerState, suffix: str) -> FinancialEvent:
        return FinancialEvent(f"external-cashflow:{plan.event_id}:{suffix}", kind, at,
            at.astimezone(_ZONE).date().isoformat(), state.group.group_id, self.rule_hash,
            tuple(sorted({"cashflow_id": plan.event_id, "account_id": plan.account_id,
                          "source_ref": plan.source_ref, **values}.items())))

    def _apply(self, state: SpotLedgerState, event: FinancialEvent):
        state = reduce_spot(state, event)
        self.events.append({**event.to_dict(), "ledger_sequence": state.applied_event_count,
                            "available_cash_units": state.available_cash_units,
                            "withdrawal_reserved_units": state.withdrawal_reserved_units,
                            "unwithdrawable_sale_units": state.unwithdrawable_sale_units,
                            "total_cash_units": state.total_cash_units,
                            "state_hash": state.state_hash})
        return state, (event, state)

    def settle_sales(self, state: SpotLedgerState, at: datetime):
        if not state.unwithdrawable_sale_units:
            return state, ()
        event = FinancialEvent(f"external-cashflow:settlement:{at.date().isoformat()}",
            "external_cashflow_settlement", at, at.date().isoformat(), state.group.group_id,
            self.rule_hash, (("cash_units", state.unwithdrawable_sale_units),))
        state, fact = self._apply(state, event)
        return state, (fact,)

    def advance(self, state: SpotLedgerState, *, until: datetime,
                valuation: Callable[[SpotLedgerState, datetime], Mapping[str, object]]):
        output = []
        while self.next_at is not None and self.next_at <= until:
            at, index, phase, plan = self.schedule[self.cursor]
            if phase == 0:
                if plan.direction == "withdrawal":
                    if plan.amount_units > state.withdrawable_cash_units() or (self.withdrawal_check is not None and not self.withdrawal_check(state, at, plan.amount_units)):
                        self.rejections[plan.event_id] = "insufficient_withdrawable_cash_at_request"
                    else:
                        event = self._event(plan, at, "external_cashflow_reserved",
                            {"cash_units": plan.amount_units}, state=state, suffix="request")
                        state, fact = self._apply(state, event)
                        output.append(fact)
            else:
                before = valuation(state, at)
                before_nav = int(before["nav_units"])
                status = "failed" if plan.event_id in self.rejections else plan.status
                reason = self.rejections.get(plan.event_id, plan.reason)
                if status == "settled" and plan.direction == "withdrawal" and (
                        dict(state.withdrawal_reservations).get(plan.event_id) != plan.amount_units
                        or plan.amount_units > state.withdrawable_cash_units(plan.event_id)
                        or self.withdrawal_check is not None and not self.withdrawal_check(state, at, plan.amount_units, plan.event_id)):
                    status, reason = "failed", "withdrawal_constraints_changed"
                before_sequence = state.applied_event_count
                reserved = dict(state.withdrawal_reservations).get(plan.event_id, 0)
                event = self._event(plan, at, "external_cashflow",
                    {"cash_units": plan.amount_units, "direction": plan.direction,
                     "status": status, "reason": reason, "reserved_before_units": reserved},
                    state=state, suffix="terminal")
                state, fact = self._apply(state, event)
                output.append(fact)
                after_nav = int(valuation(state, at)["nav_units"])
                signed = plan.signed_units if status == "settled" else 0
                self.returns.flow(before_nav, after_nav, signed)
                self.valuations.append({"sequence": len(self.valuations) + 1, "input_sequence": index,
                    "event_id": plan.event_id, "effective_at": at.isoformat(), "status": status,
                    "reason": reason, "signed_flow_units": signed, "nav_before_units": before_nav,
                    "nav_after_units": after_nav, "valuation_source": before["valuation_source"],
                    "valuation_at": before["valuation_at"], "valuation_components": before["components"],
                    "ledger_sequence_before": before_sequence,
                    "ledger_sequence_after": state.applied_event_count,
                    "withdrawal_reserved_before_units": reserved,
                    "withdrawal_reserved_after_units": dict(state.withdrawal_reservations).get(plan.event_id, 0)})
            self.cursor += 1
        return state, tuple(output)

    def context(self, closing_nav_units: int) -> dict[str, object]:
        if self.next_at is not None:
            raise SimulationContractError("资金流计划未全部执行到终态")
        return {"contract_version": "research-external-cashflow-context-v1", "account_id": self.account_id,
                "currency": "CNY", "cash_scale": 2, "plans": [plan.to_dict() for plan in self.plans],
                "events": list(self.events), "flow_valuations": list(self.valuations),
                "return_series": list(self.returns.series), **self.returns.summary(closing_nav_units)}


__all__ = ["CashflowReturns", "ExternalCashflowExecution"]
