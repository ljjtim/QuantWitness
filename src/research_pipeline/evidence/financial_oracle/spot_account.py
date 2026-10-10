"""从期初事实、真实成交和明确权益计划独立复核现货账户。"""

from __future__ import annotations

from calendar import monthrange
from copy import deepcopy
from datetime import date
from fractions import Fraction
from itertools import groupby
import json
from typing import Mapping

from research_pipeline.domain.corporate_actions import CorporateAction
from research_pipeline.domain.spot_account import (
    CorporateActionLotRule, DividendRegistrationPlan, DividendTaxRule, DividendWithholdingRule, SecurityConversionPlan,
    SpotAcquisitionLot, SpotOpeningMark, SpotOpeningSnapshot,
)

from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer, ordered_rows


CONTRACT_VERSION = "research-spot-account-context-v1"


def _require(condition, message):
    if not condition:
        raise EvidenceContractError(message)


def _time(value):
    return aware_datetime(value, "账户时间")


def _visible(source, at):
    _require(isinstance(source, Mapping) and bool(source.get("source_ref")), "账户事实缺少来源")
    _require(_time(source["available_at"]) <= at, "账户源事实在使用时尚不可见")


def _rounded(value, policy="half_up"):
    """按有理数手算人民币最小单位，避免浮点误差。"""
    _require(value >= 0 and policy in {"half_up", "floor"}, "账户金额或舍入方式无效")
    quotient, remainder = divmod(value.numerator, value.denominator)
    return quotient + int(policy == "half_up" and 2 * remainder >= value.denominator)


def _fraction(row, numerator, denominator):
    return Fraction(integer(row[numerator], numerator, minimum=0), integer(row[denominator], denominator, minimum=1))


def _index(rows, field):
    result = {}
    for row in rows:
        _require(isinstance(row, Mapping) and bool(row.get(field)), f"账户 {field} 缺失")
        _require(row[field] not in result, f"账户 {field} 重复")
        result[row[field]] = deepcopy(dict(row))
    return result


def _facts(rows, schema, field):
    return _index([schema.from_dict(row).to_dict() for row in rows], field)


def _same(actual, expected, message):
    def equal(left, right):
        if isinstance(right, dict):
            return isinstance(left, Mapping) and set(left) == set(right) and all(equal(left[key], value) for key, value in right.items())
        if isinstance(right, (list, tuple)):
            return isinstance(left, (list, tuple)) and len(left) == len(right) and all(equal(a, b) for a, b in zip(left, right))
        if type(right) in (int, bool):
            return type(left) is type(right) and left == right
        return left == right
    _require(equal(actual, expected), message)


def _rate(rule, acquired, delivered):
    """按自然月周年日比较，闰年与月末使用目标月份的实际末日。"""
    _require(delivered >= acquired, "税务交割日早于真实取得日")
    for band in rule["rates"]:
        periods = band["upper_periods"]
        if periods is None:
            return _fraction(band, "rate_numerator", "rate_denominator")
        target_month = acquired.month - 1 + periods * (12 if band["period_unit"] == "year" else 1)
        target_year = acquired.year + target_month // 12
        target_month = target_month % 12 + 1
        anniversary = date(target_year, target_month, min(acquired.day, monthrange(target_year, target_month)[1]))
        if delivered < anniversary or (delivered == anniversary and band["upper_inclusive"]):
            return _fraction(band, "rate_numerator", "rate_denominator")
    raise EvidenceContractError("持有期税率没有覆盖交割日")


class _AccountReplay:
    """只持有独立复算的明细，不导入生产内核或账本。"""

    def __init__(self, context, canonical, *, allow_zero_opening=False, cashflow_oracle=None, credit_oracle=None):
        self.cashflow_oracle = cashflow_oracle
        self.credit_oracle = credit_oracle
        self.context = context
        self.opening = SpotOpeningSnapshot.from_dict(context["opening_snapshot"]).to_dict()
        self.book = context["book"]
        self.start = _time(self.opening["started_at"])
        self.cutoff = _time(self.opening["visible_cutoff"])
        self.account = self.opening["account_id"]
        self.group = self.book["group_id"]
        for field in ("account_id", "snapshot_id", "started_at", "investor_tax_identity", "exact_dividend_tax"):
            _same(self.book[field], self.opening[field], "账户账本与期初身份不一致")
        self.lots = _index(self.opening["lots"], "lot_id")
        self.cost_method = self.opening["accounting_cost_method"]
        _same(self.book.get("accounting_cost_method", "lot_cost"), self.cost_method, "账户会计成本方法与期初声明不符")
        self.average_costs()
        self.rights = _index(self.opening["dividend_entitlements"], "entitlement_id")
        self.rules = _facts(context["tax_rules"], DividendTaxRule, "rule_id")
        self.dividend_plans = _facts(context["dividend_plans"], DividendRegistrationPlan, "dividend_id")
        self.conversion_plans = _facts(context["conversion_plans"], SecurityConversionPlan, "conversion_id")
        self.withholding_rules = {key: DividendWithholdingRule.from_dict(value).to_dict()
            for key, value in context.get("dividend_withholding_rules", {}).items()}
        _require(set(self.withholding_rules) <= set(self.dividend_plans), "派息预扣没有对应分红计划")
        self.instructions = _index(context["tax_instructions"], "event_id")
        self.corporate_declared = _index(self.book.get("corporate_action_records", []), "action_id")
        self.corporate_rules = {key: CorporateActionLotRule.from_dict(value).to_dict() for key, value in context.get("corporate_action_lot_rules", {}).items()}
        self.actions = {}
        self.action_hashes = {}
        for key, row in self.corporate_declared.items():
            action = CorporateAction.from_dict(json.loads(row["action_payload"]))
            _same(action.action_id, key, "公司行动源载荷身份与记录不符")
            _same(action.revision, row["revision"], "公司行动源载荷修订与记录不符")
            _require(action.kind in {"stock_dividend", "split", "reverse_split", "delisting_cash"}, "公司行动类型须使用已支持的账户专属计划")
            self.actions[key] = action.to_dict()
            self.action_hashes[key] = action.action_hash
        _require(set(self.corporate_rules).issubset(set(self.actions)), "公司行动批次规则没有对应源事实")
        self.corporate = {}
        self.delist_removed = {}
        self.position_rights = {}
        self.generic_released = {lot["lot_id"] for lot in self.opening["lots"] if _time(lot["sellable_at"]) <= _time(self.opening["started_at"])}
        acquisitions = context.get("acquisition_lots", self.book.get("acquisition_lots", []))
        if "acquisition_lots" in context and "acquisition_lots" in self.book:
            _same(context["acquisition_lots"], self.book["acquisition_lots"], "买入原始取得事实存在不同封存版本")
        self.acquisitions = _facts(acquisitions, SpotAcquisitionLot, "lot_id")
        self.used_acquisitions = set()
        self.acquisition_orders = {}
        self.used_instructions = set()
        self.fill_source = canonical["fills"]
        self.fills = None if credit_oracle is not None else _index(ordered_rows(canonical["fills"], order_by=("fill_time", "fill_id")), "fill_id")
        self.used_fills = set()
        self.events = _index(context["financial_events"], "event_id")
        self.generic_events = sorted((event for event in self.events.values() if event["kind"] == "settlement" and "account_sequence" not in dict(event["payload"]) and not event["event_id"].startswith("opening:")), key=lambda event: (_time(event["effective_time"]), "instrument_hash" not in dict(event["payload"]), event["event_id"]))
        self.generic_cursor = 0
        self.used_events = set()
        for event in self.events.values():
            payload = event["payload"]
            _require(isinstance(payload, list) and all(isinstance(item, list) and len(item) == 2 for item in payload), "账户金融事件载荷无效")
            _require(len(dict(payload)) == len(payload), "账户金融事件载荷字段重复")
            if "account_sequence" in dict(payload):
                _visible(dict(payload), _time(event["effective_time"]))
        self.declared = {name: _index(self.book[name], key) for name, key in (
            ("transfers", "transfer_id"), ("assessments", "assessment_id"),
            ("collections", "collection_id"),
        )}
        self.transfers = {}
        self.consumptions = []
        self.assessments = {}
        self.collections = {}
        self.dividends = {}
        self.conversions = {}
        self.cash = integer(self.opening["cash"]["available_units"], "期初可用现金", minimum=0)
        self.restricted = self.opening["cash"]["restricted_units"]
        self.unsettled = self.opening["cash"]["unsettled_units"]
        self.receivables = {row["claim_id"]: row["cash_units"] for row in self.opening["receivables"]}
        self.payables = {row["claim_id"]: row["cash_units"] for row in self.opening["payables"]}
        self.due_dates = {row["claim_id"]: _time(row["due_at"]) for row in self.opening["receivables"]}
        for row in self.opening["tax_assessments"]:
            _require(_time(row["assessed_at"]) <= self.start, "期初税款来自未来核定")
            key = row["assessment_id"]
            _require(key not in self.payables, "期初税款与普通应付身份重复")
            self.assessments[key] = {
                "assessment_id": key, "transfer_id": None, "assessed_at": row["assessed_at"],
                "due_at": row["due_at"], "tax_units": row["assessed_units"], "lines": [],
                "source": row["source"], "opening_collected_units": row["collected_units"], "sequence": 0,
            }
            if row["assessed_units"] > row["collected_units"]:
                self.payables[key] = row["assessed_units"] - row["collected_units"]
        marks = _facts(context["opening_marks"], SpotOpeningMark, "instrument_hash")
        quantities = {}
        for lot in self.lots.values():
            quantities[lot["instrument_hash"]] = quantities.get(lot["instrument_hash"], 0) + lot["quantity"]
        _same(set(marks), set(quantities), "期初估值未恰好覆盖持仓")
        market_value = 0
        for key, mark in marks.items():
            _visible(mark["source"], self.cutoff)
            _require(_time(mark["observed_at"]) <= min(self.start, _time(mark["source"]["available_at"])), "期初估值观测时间来自未来")
            market_value += _rounded(quantities[key] * _fraction(mark, "price_numerator_units", "price_denominator"))
        self.opening_cash = self.total_cash()
        self.opening_nav = self.opening_cash + market_value - sum(self.payables.values())
        if self.cashflow_oracle is not None:
            _same(self.cashflow_oracle.context["opening_nav_units"], self.opening_nav - (0 if credit_oracle is None else credit_oracle.principal_units + credit_oracle.interest_units), "资金流期初 NAV 与独立账户估值不符")
        _require(self.opening_nav >= 0 if allow_zero_opening else self.opening_nav > 0, "期初净资产不符合账户收益起点")
        self.allow_zero_opening = allow_zero_opening
        if credit_oracle is not None:
            credit_oracle.bind_account(self)
        records = self.book["event_records"]
        _require(isinstance(records, list) and bool(records), "账户缺少初始化事件")
        previous = self.start
        seen = set()
        for sequence, record in enumerate(records, 1):
            _same(record["sequence"], sequence, "账户事件业务序号不连续")
            at = _time(record["effective_time"])
            _require(at >= previous and record["event_id"] not in seen, "账户事件重复或时间倒退")
            _visible(record["source"], at)
            previous = at
            seen.add(record["event_id"])
        _same(records[0]["kind"], "opening", "账户必须以唯一初始化事件开始")
        self.records = iter(records)
        self.next_record = next(self.records, None)
        self.record = None
        calendar = context.get("settlement_calendar")
        if calendar is not None:
            _same(set(calendar), {"sessions", "source"}, "结算日历字段不完整")
            sessions = [date_value(item, "结算交易日") for item in calendar["sessions"]]
            _same(sessions, sorted(set(sessions)), "结算日历交易日须唯一并递增")
            _require(bool(sessions), "结算日历不能为空")
            _visible(calendar["source"], self.cutoff)
            snapshot_sessions = {date_value(item["session"], "账户会话") for item in context["snapshots"]}
            _require(snapshot_sessions.issubset(set(sessions)), "结算日历未覆盖账户会话")
        self.opening_schedule = []
        for claim in self.opening["receivables"]:
            _require(_time(claim["due_at"]) >= self.start, "期初逾期应收缺少实际到账时间")
            self.opening_schedule.append((_time(claim["due_at"]), "opening_receivable", claim["claim_id"], claim))
        for lot in self.opening["lots"]:
            if _time(lot["sellable_at"]) > self.start:
                self.opening_schedule.append((_time(lot["sellable_at"]), "opening_release", lot["lot_id"], lot))
        self.opening_schedule.sort(key=lambda item: item[:3])
        self.opening_cursor = 0
        for instruction in self.instructions.values():
            _require(instruction["kind"] in {"assess", "collect"} and _time(instruction["effective_time"]) >= self.start, "税务指令种类无效或早于期初")
            _visible(instruction["source"], _time(instruction["effective_time"]))

    def average_costs(self, instrument=None, remaining_cost=None):
        if self.cost_method != "average":
            return
        groups = {}
        for lot in self.lots.values():
            if instrument is None or lot["instrument_hash"] == instrument:
                groups.setdefault(lot["instrument_hash"], []).append(lot)
        for key, lots in groups.items():
            total = sum(lot["accounting_cost_units"] for lot in lots) if remaining_cost is None else remaining_cost
            quantity = sum(lot["quantity"] for lot in lots)
            cumulative = 0
            previous = 0
            # 金额先按累计份额舍入，再取差，批次合计始终等于总成本。
            for lot in sorted(lots, key=lambda lot: lot["lot_id"]):
                cumulative += lot["quantity"]
                allocated = _rounded(Fraction(total * cumulative, quantity))
                lot["accounting_cost_units"] = allocated - previous
                previous = allocated

    def total_cash(self):
        return self.cash + self.restricted + self.unsettled + sum(self.receivables.values()) + (0 if self.cashflow_oracle is None else sum(self.cashflow_oracle.reservations.values()))

    def bind(self, kind, business_id, at, source, refs=()):
        expected = {
            "sequence": self.record["sequence"], "event_id": f"spot:{self.account}:{kind}:{business_id}",
            "kind": kind, "effective_time": at, "source": source, "business_refs": list(refs),
        }
        _same(self.record, expected, "账户业务事件与原始事实不一致")
        _visible(source, _time(at))

    def emit(self, kind, values, suffix=""):
        record = self.record
        event_id = record["event_id"] + suffix
        event = self.events.get(event_id)
        _require(event is not None, "账户业务事件缺少对应金融事件")
        expected_payload = {
            **values, "account_id": self.account, "account_sequence": record["sequence"],
            "source_ref": record["source"]["source_ref"], "available_at": record["source"]["available_at"],
        }
        if self.credit_oracle is not None:
            credit_conversion = self.credit_oracle.account_event(self, event, values)
            if credit_conversion is not None:
                expected_payload["credit_conversion"] = credit_conversion
        _same(dict(event["payload"]), expected_payload, "账户金融事件金额或业务引用与独立计算不符")
        for field, expected in (
            ("kind", kind), ("group_id", self.group), ("effective_time", record["effective_time"]),
            ("session", _time(record["effective_time"]).date().isoformat()),
            ("parent_id", record["business_refs"][0] if record["business_refs"] else None),
        ):
            _same(event[field], expected, "账户金融事件信封与业务事件不符")
        _require(isinstance(event["rule_hash"], str) and len(event["rule_hash"]) == 64, "账户事件规则身份缺失")
        _require(event_id not in self.used_events, "账户金融事件重复应用")
        self.used_events.add(event_id)

    def claim(self, key, amount, due):
        if amount:
            _require(key not in self.receivables, "账户应收身份重复")
            self.receivables[key] = amount
            self.due_dates[key] = _time(due)

    def pay_claim(self, key, amount, earliest):
        at = _time(self.record["effective_time"])
        _require(at >= _time(earliest), "账户应收尚未到期到账")
        if amount:
            _same(self.receivables.get(key), amount, "账户应收到账金额不一致或重复到账")
            self.cash += self.receivables.pop(key)
            self.due_dates.pop(key)
            self.emit("settlement", {"receivable_id": key, "cash_units": 0, "quantity": 0})

    def opening_settlement_event(self, event_id, at, values):
        event = self.events.get(event_id)
        _require(event is not None and event_id not in self.used_events, "期初应收到账或限售释放缺少唯一封存金融事件")
        _same(dict(event["payload"]), values, "期初结算载荷与原始到期事实不符")
        _same((event["kind"], _time(event["effective_time"]), event["session"], event["group_id"], event["parent_id"]),
              ("settlement", at, at.date().isoformat(), self.group, None), "期初结算信封或到期时点不符")
        _require(isinstance(event["rule_hash"], str) and len(event["rule_hash"]) == 64, "期初结算缺少规则身份")
        self.used_events.add(event_id)

    def opening_settlement(self, entry):
        at, kind, key, fact = entry
        if kind == "opening_receivable":
            _same(self.receivables.get(key), fact["cash_units"], "期初应收重复或金额不符")
            self.cash += self.receivables.pop(key)
            self.due_dates.pop(key)
            self.opening_settlement_event(f"opening:{kind}:{key}", at, {"cash_units": 0, "quantity": 0, "receivable_id": key})
        else:
            _require(key in self.lots and self.lots[key]["quantity"] == fact["quantity"], "期初限售释放数量与在账批次不符")
            _require(key not in self.generic_released, "期初股份重复解禁")
            named = [right for right in self.opening["position_entitlements"] if right["lot_id"] == key]
            named_quantity = 0
            for right in named:
                _same(_time(right["due_at"]), at, "期初具名权益与批次到期时间不符")
                named_quantity += right["quantity"]
                self.opening_settlement_event(f"opening:entitlement:{right['entitlement_id']}", at, {"cash_units": 0, "entitlement_id": right["entitlement_id"], "quantity": 0})
            plain = fact["quantity"] - named_quantity
            _require(plain >= 0, "期初具名权益超过原批次，不能重复计入")
            if plain:
                self.opening_settlement_event(f"opening:{kind}:{key}", at, {"cash_units": 0, "instrument_hash": fact["instrument_hash"], "quantity": plain})
            self.generic_released.add(key)

    def generic_settlement(self, event):
        at = _time(event["effective_time"])
        values = dict(event["payload"])
        _same((event["group_id"], event["session"]), (self.group, at.date().isoformat()), "普通结算执行组或会话不符")
        _require(event["event_id"] not in self.used_events, "普通结算事件重复")
        _require(isinstance(event["rule_hash"], str) and len(event["rule_hash"]) == 64, "普通结算规则身份缺失")
        if "receivable_id" in values:
            key = values["receivable_id"]
            _require(key in self.receivables and at >= self.due_dates[key], "普通应收未知、重复到账或提前结算")
            _same(values, {"receivable_id": key, "cash_units": 0, "quantity": 0}, "普通应收结算载荷无效")
            self.cash += self.receivables.pop(key)
            self.due_dates.pop(key)
        elif "entitlement_id" in values:
            key = values["entitlement_id"]
            right = self.position_rights.get(key)
            _require(right is not None and at.date().isoformat() >= right["due"], "股份权利未知、重复解禁或未到期")
            _same(values, {"entitlement_id": key, "cash_units": 0, "quantity": 0}, "股份权利结算载荷无效")
            for lot_id in right["lots"]:
                _require(lot_id not in self.generic_released and lot_id in self.lots and _time(self.lots[lot_id]["sellable_at"]) <= at, "股份解禁批次不存在、重复或尚不可卖")
                self.generic_released.add(lot_id)
            del self.position_rights[key]
        elif "instrument_hash" in values:
            instrument = values["instrument_hash"]
            pending_lots = {lot_id for right in self.position_rights.values() for lot_id in right["lots"]}
            lots = [lot for lot in self.lots.values() if lot["instrument_hash"] == instrument and lot["lot_id"] not in pending_lots and lot["lot_id"] not in self.generic_released and _time(lot["sellable_at"]) <= at]
            expected = {"instrument_hash": instrument, "cash_units": 0}
            if "order_id" in values:
                order_id = values["order_id"]
                _same(event["parent_id"], order_id, "T+0释放父事件与订单不符")
                lots = [lot for lot in lots
                        if self.acquisition_orders.get(lot["lot_id"]) == (order_id, at)]
                expected["order_id"] = order_id
            quantity = sum(lot["quantity"] for lot in lots)
            _require(quantity > 0, "普通持仓结算没有未释放的到期股份或对应订单批次")
            _same(values, {**expected, "quantity": quantity}, "普通持仓结算数量与取得批次不符")
            self.generic_released.update(lot["lot_id"] for lot in lots)
        else:
            _require(self.unsettled > 0 and not values.get("quantity"), "普通结算没有待结算现金")
            _same(values, {"cash_units": self.unsettled, "quantity": 0}, "待结算现金释放金额不符")
            self.cash += self.unsettled
            self.unsettled = 0
        self.used_events.add(event["event_id"])

    def advance(self, at):
        while True:
            choices = []
            if self.next_record is not None:
                priority = 0 if self.next_record["kind"] == "opening" else 1 if self.next_record["kind"] in {"buy_fill", "corporate_action_projected", "transfer"} else 3
                if self.next_record["kind"] in {"corporate_action_projected", "transfer"} and self.generic_cursor < len(self.generic_events):
                    pending = self.generic_events[self.generic_cursor]
                    # 同刻到期的普通股份先释放；应收和新增股份仍按原权益依赖顺序处理。
                    if ("instrument_hash" in dict(pending["payload"])
                            and _time(pending["effective_time"]) == _time(self.next_record["effective_time"])):
                        priority = 3
                if self.cashflow_oracle is not None and self.next_record["kind"] in {"buy_fill", "transfer"}:
                    if (_time(self.next_record["effective_time"]).hour, _time(self.next_record["effective_time"]).minute) == (9, 30):
                        priority = 5
                choices.append((_time(self.next_record["effective_time"]), priority, "record"))
            if self.opening_cursor < len(self.opening_schedule):
                choices.append((self.opening_schedule[self.opening_cursor][0], 1, "opening"))
            if self.generic_cursor < len(self.generic_events):
                event = self.generic_events[self.generic_cursor]
                event_time = _time(event["effective_time"])
                order_id = dict(event["payload"]).get("order_id")
                # 具名T+0释放依赖同刻买入，资金流先到账、批次取得后再释放。
                priority = 6 if order_id is not None and (order_id, event_time) not in self.acquisition_orders.values() else 2
                choices.append((event_time, priority, "generic"))
            if self.cashflow_oracle is not None and self.cashflow_oracle.next_at is not None:
                choices.append((self.cashflow_oracle.next_at, -1 if self.cashflow_oracle.schedule[self.cashflow_oracle.cursor][2] == -1 else 4, "cashflow"))
            if self.credit_oracle is not None and self.credit_oracle.next_at is not None:
                choices.append((self.credit_oracle.next_at, self.credit_oracle.priority, "credit"))
            if not choices:
                break
            effective, _, kind = min(choices)
            if effective > at:
                break
            if kind == "credit":
                self.credit_oracle.apply_next(self)
            elif kind == "cashflow":
                self.cashflow_oracle.apply_next(self)
            elif kind == "opening":
                self.opening_settlement(self.opening_schedule[self.opening_cursor])
                self.opening_cursor += 1
            elif kind == "generic":
                self.generic_settlement(self.generic_events[self.generic_cursor])
                self.generic_cursor += 1
            else:
                self.record = self.next_record
                self.apply_record()
                self.next_record = next(self.records, None)

    def apply_record(self):
        kind = self.record["kind"]
        sequence = self.record["sequence"]
        if kind == "opening":
            _require(sequence == 1, "期初账户被重复初始化")
            self.bind(kind, self.opening["snapshot_id"], self.opening["started_at"], self.opening["source"])
        elif kind in {"buy_fill", "transfer"}:
            self.trade(kind)
        elif kind == "tax_assessed":
            self.assess()
        elif kind == "tax_collected":
            self.collect()
        elif kind in {"corporate_action_registered", "corporate_action_projected", "corporate_action_arrived"}:
            self.corporate_action(kind)
        elif kind.startswith("dividend_"):
            self.dividend(kind)
        elif kind in {"security_conversion", "successor_registered", "successor_released", "conversion_cash_paid"}:
            self.conversion(kind)
        else:
            raise EvidenceContractError(f"账户独立复核未覆盖业务事件: {kind}")
        _require(self.cash >= 0 and all(value >= 0 for value in self.payables.values()), "账户现金不足或应付变为负数")

    def trade(self, kind):
        record = self.record
        refs = record["business_refs"]
        _require(len(refs) == 1, "账户转让或买入缺少唯一来源事件")
        delisting = next((key for key, row in self.corporate_declared.items() if row["financial_event_id"] == refs[0] and self.actions[key]["kind"] == "delisting_cash"), None)
        if delisting is not None:
            _require(kind == "transfer" and delisting not in self.delist_removed, "退市转让重复或方向无效")
            action = self.actions[delisting]
            _same(record["source"], {"source_ref": action["source_ref"], "available_at": action["announcement_available_time"]}, "退市转让来源与公告不符")
            held = sorted((deepcopy(lot) for lot in self.lots.values() if lot["instrument_hash"] == action["instrument_hash"]), key=lambda row: row["lot_id"])
            _require(bool(held), "退市注销缺少当前股份")
            self.delist_removed[delisting] = held
            # 注销是法定转让事实，直接匹配原股份，不创建或借用虚构成交。
            fill = {"side": "sell", "fill_time": record["effective_time"], "session": record["effective_time"][:10], "instrument_hash": action["instrument_hash"], "quantity": sum(lot["quantity"] for lot in held), "notional_units": 0, "fee_units": 0}
        else:
            _require(refs[0] not in self.used_fills, "账户转让或买入重复消费真实成交")
            if self.credit_oracle is not None:
                from .credit_account import fill_by_id
                fill = fill_by_id(self.fill_source, refs[0])
            else:
                _require(refs[0] in self.fills, "账户转让或买入没有真实成交")
                fill = self.fills[refs[0]]
        side = "buy" if kind == "buy_fill" else "sell"
        _same(fill["side"], side, "账户成交方向不一致")
        _same(_time(fill["fill_time"]), _time(record["effective_time"]), "账户成交时点不一致")
        _same(date_value(fill["session"], "成交会话"), _time(record["effective_time"]).date(), "账户成交会话不一致")
        quantity = integer(fill["quantity"], "成交数量", minimum=1)
        notional = integer(fill["notional_units"], "成交金额", minimum=0)
        fee = integer(fill["fee_units"], "成交费用", minimum=0)
        if delisting is None:
            price = integer(fill["execution_price_units"], "成交价格", minimum=1)
            scale = integer(fill["price_scale"], "成交价格精度", minimum=0)
            _require(scale <= 9 and fill["contract_multiplier"] == 1, "现货成交计价单位无效")
            _same(notional, _rounded(Fraction(price * quantity * 100, 10 ** scale)), "账户真实成交金额不守恒")
        _require(not any(item["plan"]["old_instrument_hash"] == fill["instrument_hash"] for item in self.conversions.values()), "已注销旧证券又产生真实成交")
        if delisting is None:
            self.used_fills.add(refs[0])
        if side == "buy":
            prefix = f"spot:{self.account}:buy_fill:"
            _require(record["event_id"].startswith(prefix), "买入业务身份不符")
            lot_id = record["event_id"][len(prefix):]
            lot = self.acquisitions.get(lot_id)
            _require(lot is not None, "买入批次缺少 acquisition_lots 原始取得事实")
            _require(lot_id not in self.lots and lot_id not in self.used_acquisitions, "买入批次重复")
            self.bind(kind, lot_id, record["effective_time"], lot["source"], refs)
            _same((lot["instrument_hash"], lot["quantity"], lot["accounting_cost_units"], lot["acquired_on"]),
                  (fill["instrument_hash"], quantity, notional + fee, _time(fill["fill_time"]).date().isoformat()), "买入批次与真实成交数量成本不符")
            _require(_time(lot["sellable_at"]) >= _time(fill["fill_time"]), "买入批次在取得前可卖")
            _require(lot["predecessor_lot_id"] is None and lot["predecessor_conversion_id"] is None, "真实买入不能伪造换股继承")
            self.lots[lot_id] = deepcopy(lot)
            self.acquisition_orders[lot_id] = (fill["order_id"], _time(fill["fill_time"]))
            self.average_costs()
            self.used_acquisitions.add(lot_id)
            borrowed = 0 if self.credit_oracle is None else self.credit_oracle.buy_fill(self, fill, lot_id)
            self.cash -= notional + fee - borrowed
            return
        transfer = next((item for item in self.declared["transfers"].values() if item["sequence"] == record["sequence"]), None)
        _require(transfer is not None, "转让缺少正式业务明细")
        transfer_id = transfer["transfer_id"]
        if delisting is not None:
            _same(transfer_id, f"delisting:{delisting}", "退市转让业务键不符")
        self.bind(kind, transfer_id, transfer["effective_time"], transfer["source"], (transfer["fill_event_id"],))
        delivered = date_value(transfer["delivered_on"], "转让交割日")
        _same(delivered, _time(fill["fill_time"]).date(), "当前转让须按真实成交日交割")
        remaining = quantity
        matches = []
        candidates = sorted((item for item in self.lots.values() if item["instrument_hash"] == fill["instrument_hash"]), key=lambda item: (item["acquired_on"] or "0001-01-01", item["lot_id"]))
        instrument_cost = sum(lot["accounting_cost_units"] for lot in candidates)
        instrument_quantity = sum(lot["quantity"] for lot in candidates)
        allocated_cost = 0
        for lot in candidates:
            if not remaining:
                break
            if delisting is None and _time(lot["sellable_at"]) > _time(fill["fill_time"]):
                continue
            conversion_id = lot["predecessor_conversion_id"]
            if delisting is None and conversion_id is not None and not self.conversions[conversion_id]["released"] and lot["lot_id"] not in self.conversions[conversion_id]["released_lot_ids"] and lot["predecessor_lot_id"] not in self.conversions[conversion_id]["released_lot_ids"]:
                continue
            if delisting is None and any(lot["lot_id"] in right["lots"] for right in self.position_rights.values()):
                continue
            used = min(remaining, lot["quantity"])
            cost = _rounded(Fraction(lot["accounting_cost_units"] * used, lot["quantity"]))
            if self.cost_method == "average":
                cumulative = _rounded(Fraction(instrument_cost * (quantity - remaining + used), instrument_quantity))
                cost = cumulative - allocated_cost
                allocated_cost = cumulative
            matches.append({"lot_id": lot["lot_id"], "quantity": used, "accounting_cost_units": cost, "acquired_on": lot["acquired_on"]})
            for right in sorted(self.rights.values(), key=lambda item: item["entitlement_id"]):
                if right["lot_id"] != lot["lot_id"] or not right["remaining_quantity"]:
                    continue
                consumed = used * _fraction(right, "quantity_multiplier_numerator", "quantity_multiplier_denominator")
                remaining_right = _fraction(right, "remaining_quantity", "remaining_quantity_denominator")
                registered_right = _fraction(right, "registered_quantity", "registered_quantity_denominator")
                _require(consumed <= remaining_right, "税权消费超过剩余权利")
                before = registered_right - remaining_right
                credit = _rounded(right["prewithheld_units"] * (before + consumed) / registered_right) - _rounded(right["prewithheld_units"] * before / registered_right)
                left = remaining_right - consumed
                right.update(remaining_quantity=left.numerator, remaining_quantity_denominator=left.denominator)
                self.consumptions.append({"entitlement_id": right["entitlement_id"], "transfer_id": transfer_id, "lot_id": lot["lot_id"], "quantity": consumed.numerator, "quantity_denominator": consumed.denominator, "remaining_quantity": left.numerator, "remaining_quantity_denominator": left.denominator, "prewithheld_credit_units": credit, "assessment_id": None, "sequence": record["sequence"]})
            remaining -= used
            if used == lot["quantity"]:
                del self.lots[lot["lot_id"]]
            else:
                lot["quantity"] -= used
                if self.cost_method != "average":
                    lot["accounting_cost_units"] -= cost
        _require(remaining == 0, "真实卖出超过FIFO持仓余额")
        if self.cost_method == "average" and instrument_quantity > quantity:
            self.average_costs(fill["instrument_hash"], instrument_cost - allocated_cost)
        expected = {**transfer, "instrument_hash": fill["instrument_hash"], "quantity": quantity, "matches": matches}
        _same(transfer, expected, "FIFO转让匹配数量或会计成本不符")
        self.transfers[transfer_id] = expected
        restricted = 0 if self.credit_oracle is None or delisting is not None else self.credit_oracle.sell_fill(self, fill, matches)
        self.cash += notional - fee - restricted

    def instruction(self, kind):
        prefix = f"spot:{self.account}:{self.record['kind']}:"
        _require(self.record["event_id"].startswith(prefix), "税务业务身份不符")
        key = self.record["event_id"][len(prefix):]
        instruction = self.instructions.get(key)
        _require(instruction is not None and instruction["kind"] == kind and key not in self.used_instructions, "核定或扣收缺少明确计划或计划重复")
        _same(instruction["effective_time"], self.record["effective_time"], "税务计划与实际时点不一致")
        _same(instruction["source"], self.record["source"], "税务计划与事件来源不一致")
        _visible(instruction["source"], _time(instruction["effective_time"]))
        self.used_instructions.add(key)
        return key, instruction

    def assess(self):
        key, instruction = self.instruction("assess")
        transfer_id = instruction["transfer_id"]
        transfer = self.transfers.get(transfer_id)
        _require(transfer is not None and not any(item["transfer_id"] == transfer_id for item in self.assessments.values()), "转让尚未发生或重复核定")
        _require(key not in self.assessments and key not in self.payables, "核定与已有应付身份重复")
        self.bind("tax_assessed", key, instruction["effective_time"], instruction["source"], (transfer_id,))
        at = _time(instruction["effective_time"])
        delivered = date_value(transfer["delivered_on"], "交割日")
        _require(delivered <= at.date(), "核定使用未来交割")
        lines = []
        for consumption in self.consumptions:
            if consumption["transfer_id"] != transfer_id:
                continue
            right = self.rights[consumption["entitlement_id"]]
            rule = self.rules.get(right["tax_rule_id"])
            _require(rule is not None and right["acquired_on"] is not None, "税权缺少明确税规则或真实取得日")
            _visible(rule["source"], at)
            _require(rule["investor_tax_identity"] == self.opening["investor_tax_identity"] and right["security_class"] in rule["security_classes"] and right["acquisition_method"] in rule["acquisition_methods"], "税规则不适用于投资者证券或取得方式")
            applicable = date_value(right["record_on"] if rule["applicability_date"] == "record_date" else transfer["delivered_on"], "税规则适用日")
            _require(date_value(rule["effective_from"], "规则起日") <= applicable <= date_value(rule["effective_until"], "规则止日"), "税规则有效区间不覆盖本次权利")
            acquired = date_value(right["acquired_on"], "真实取得日")
            gross = _rounded(_fraction(consumption, "quantity", "quantity_denominator") * _fraction(right, "taxable_per_share_numerator", "taxable_per_share_denominator") * _rate(rule, acquired, delivered), rule["rounding"])
            credit = consumption["prewithheld_credit_units"]
            refund = max(credit - gross, 0)
            _require(not refund or rule["overwithholding_policy"] != "reject", "规则不允许多扣税款")
            if rule["overwithholding_policy"] == "no_refund":
                refund = 0
            lines.append({"entitlement_id": right["entitlement_id"], "transfer_id": transfer_id, "quantity": consumption["quantity"], "acquired_on": right["acquired_on"], "delivered_on": transfer["delivered_on"], "rule_id": rule["rule_id"], "rule_source": rule["source"], "gross_tax_units": gross, "prewithheld_credit_units": credit, "payable_units": max(gross - credit, 0), "refund_units": refund, "quantity_denominator": consumption["quantity_denominator"]})
            consumption["assessment_id"] = key
        total = sum(row["payable_units"] for row in lines)
        expected = {"assessment_id": key, "transfer_id": transfer_id, "assessed_at": instruction["effective_time"], "due_at": instruction["due_at"], "tax_units": total, "lines": lines, "source": instruction["source"], "opening_collected_units": 0, "sequence": self.record["sequence"]}
        _same(self.declared["assessments"].get(key), expected, "税款核定明细或自然月年金额与独立计算不符")
        self.assessments[key] = expected
        if total:
            self.payables[key] = total
            self.emit("tax_assessed", {"assessment_id": key, "transfer_id": transfer_id, "tax_units": total})
        refund = sum(row["refund_units"] for row in lines)
        if refund:
            claim = f"tax-refund:{key}"
            self.claim(claim, refund, instruction["due_at"])
            self.emit("corporate_action", {"instrument_hash": transfer["instrument_hash"], "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0, "cash_receivable_units": refund, "receivable_id": claim, "cash_due_date": _time(instruction["due_at"]).date().isoformat()}, ":refund")

    def collect(self):
        key, instruction = self.instruction("collect")
        allocations = sorted(instruction["allocations"], key=lambda row: row["assessment_id"])
        _require(bool(allocations) and len({row["assessment_id"] for row in allocations}) == len(allocations), "扣收分配为空或重复")
        self.bind("tax_collected", key, instruction["effective_time"], instruction["source"], tuple(row["assessment_id"] for row in instruction["allocations"]))
        expected = {"collection_id": key, "effective_time": instruction["effective_time"], "allocations": allocations, "source": instruction["source"], "sequence": self.record["sequence"]}
        _same(self.declared["collections"].get(key), expected, "税款扣收分配与明确计划不符")
        for allocation in allocations:
            assessment_id = allocation["assessment_id"]
            amount = integer(allocation["cash_units"], "扣款金额", minimum=1)
            _require(assessment_id in self.assessments and amount <= self.payables.get(assessment_id, 0), "分次扣款超过已核定未付税款")
            _require(amount <= self.cash, "账户可用现金不足缴税")
            self.cash -= amount
            self.payables[assessment_id] -= amount
            if not self.payables[assessment_id]:
                del self.payables[assessment_id]
            self.emit("tax_collected", {"collection_id": key, "assessment_id": assessment_id, "cash_units": amount}, f":{assessment_id}")
        self.collections[key] = expected

    def dividend(self, kind):
        prefix = f"spot:{self.account}:{kind}:"
        _require(self.record["event_id"].startswith(prefix), "分红业务身份不符")
        key = self.record["event_id"][len(prefix):]
        plan = self.dividend_plans.get(key)
        _require(plan is not None, "分红缺少封存计划")
        _visible(plan["source"], _time(self.record["effective_time"]))
        self.bind(kind, key, self.record["effective_time"], self.record["source"])
        if kind == "dividend_registered":
            _same(self.record["effective_time"], plan["record_at"], "分红登记时点与计划不一致")
            _same(self.record["source"], plan["source"], "分红登记来源不一致")
            _require(key not in self.dividends, "分红重复登记")
            held = [lot for lot in self.lots.values() if lot["instrument_hash"] == plan["instrument_hash"]]
            quantity = sum(lot["quantity"] for lot in held)
            amount = _rounded(quantity * _fraction(plan, "cash_per_share_numerator", "cash_per_share_denominator"))
            withholding = self.withholding_rules.get(key)
            withheld, rate, taxable = 0, Fraction(0), _fraction(plan, "taxable_per_share_numerator", "taxable_per_share_denominator")
            if withholding is not None:
                _visible(withholding["source"], _time(self.record["effective_time"]))
                rate = _fraction(withholding, "rate_numerator", "rate_denominator")
                withheld = _rounded(quantity * taxable * rate, withholding["rounding"])
                _require(withheld <= amount, "派息预扣超过毛现金股息")
            cumulative_quantity, allocated = 0, 0
            for lot in held:
                cumulative_quantity += lot["quantity"]
                cumulative = _rounded(cumulative_quantity * taxable * rate, withholding["rounding"] if withholding else "half_up")
                lot_withheld = cumulative-allocated
                allocated = cumulative
                right_id = f"{key}:{lot['lot_id']}"
                _require(right_id not in self.rights, "分红税权重复登记")
                right = {"entitlement_id": right_id, "dividend_id": key, "lot_id": lot["lot_id"], "instrument_hash": lot["instrument_hash"], "registered_quantity": lot["quantity"], "remaining_quantity": lot["quantity"], "taxable_per_share_numerator": plan["taxable_per_share_numerator"], "taxable_per_share_denominator": plan["taxable_per_share_denominator"], "acquired_on": lot["acquired_on"], "record_on": _time(plan["record_at"]).date().isoformat(), "tax_rule_id": plan["tax_rule_id"], "source": plan["source"], "prewithheld_units": lot_withheld, "predecessor_entitlement_id": None, "quantity_multiplier_numerator": 1, "quantity_multiplier_denominator": 1, "acquisition_method": lot["acquisition_method"], "security_class": lot["security_class"], "registered_quantity_denominator": 1, "remaining_quantity_denominator": 1}
                _require(not self.opening["exact_dividend_tax"] or right["acquired_on"] is not None, "精确分红税权缺少真实取得日")
                self.rights[right_id] = right
            self.dividends[key] = {"plan": plan, "registered_quantity": quantity, "cash_units": amount-withheld, "recognized": False, "paid": False, "sequence": self.record["sequence"]}
            return
        dividend = self.dividends.get(key)
        _require(dividend is not None, "分红尚未登记")
        amount = dividend["cash_units"]
        claim = f"dividend:{key}"
        if kind == "dividend_recognized":
            _same(self.record["effective_time"], plan["ex_at"], "分红应收确认不是计划除权时点")
            _same(self.record["source"], plan["source"], "分红除权来源不一致")
            _require(not dividend["recognized"], "分红应收重复确认")
            self.claim(claim, amount, plan["pay_at"])
            if amount:
                self.emit("corporate_action", {"instrument_hash": plan["instrument_hash"], "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0, "cash_receivable_units": amount, "receivable_id": claim, "cash_due_date": _time(plan["pay_at"]).date().isoformat()})
            dividend["recognized"] = True
        elif kind == "dividend_paid":
            _require(dividend["recognized"] and not dividend["paid"], "分红未确认或重复支付")
            self.pay_claim(claim, amount, plan["pay_at"])
            dividend["paid"] = True
        else:
            raise EvidenceContractError("未知分红业务事件")

    def conversion(self, kind):
        prefix = f"spot:{self.account}:{kind}:"
        _require(self.record["event_id"].startswith(prefix), "换股业务身份不符")
        business_id = self.record["event_id"][len(prefix):]
        refs = self.record["business_refs"]
        key = refs[0] if kind == "successor_released" and len(refs) == 1 else business_id
        plan = self.conversion_plans.get(key)
        _require(plan is not None, "换股缺少封存计划")
        self.bind(kind, business_id, self.record["effective_time"], plan["source"], refs)
        at = _time(self.record["effective_time"])
        if kind == "security_conversion":
            _require(not refs and key not in self.conversions, "换股重复或引用不符")
            _same(self.record["effective_time"], plan["cancelled_at"], "旧证券注销时点与计划不符")
            _visible(plan["valuation_source"], at)
            _require(_time(plan["valuation_observed_at"]) <= min(at, _time(plan["valuation_source"]["available_at"])), "换股期间估值使用未来事实")
            held = sorted((lot for lot in self.lots.values() if lot["instrument_hash"] == plan["old_instrument_hash"]), key=lambda item: item["lot_id"])
            _require(bool(held), "换股缺少旧持仓")
            links = []
            for lot in held:
                exact = lot["quantity"] * _fraction(plan, "ratio_numerator", "ratio_denominator")
                whole = exact.numerator // exact.denominator
                fractional = exact - whole
                _require(not fractional or plan["fractional_policy"] == "cash_in_lieu", "换股零碎权益未经允许")
                rights = sorted((right for right in self.rights.values() if right["lot_id"] == lot["lot_id"] and right["remaining_quantity"]), key=lambda row: row["entitlement_id"])
                _require(not rights or (plan["tax_right_policy"] == "carry" and not fractional), "换股税权不能按计划精确继承")
                new_cost = _rounded(lot["accounting_cost_units"] * _fraction(plan, "successor_cost_numerator", "successor_cost_denominator"))
                _require(whole > 0 or new_cost == 0, "全现金化不能保留股份成本")
                amount = _rounded(lot["quantity"] * _fraction(plan, "cash_per_old_share_numerator", "cash_per_old_share_denominator") + fractional * _fraction(plan, "fractional_cash_price_numerator", "fractional_cash_price_denominator"))
                new_id = f"{key}:{lot['lot_id']}"
                successor = None if not whole else {"lot_id": new_id, "instrument_hash": plan["new_instrument_hash"], "quantity": whole, "accounting_cost_units": new_cost, "acquired_on": lot["acquired_on"] if plan["acquisition_date_policy"] == "inherit" else _time(plan["registered_at"]).date().isoformat(), "sellable_at": max(_time(lot["sellable_at"]), _time(plan["tradable_at"])).isoformat(), "acquisition_method": plan["successor_acquisition_method"], "security_class": plan["successor_security_class"], "source": plan["source"], "predecessor_lot_id": lot["lot_id"], "predecessor_conversion_id": key}
                for right in rights:
                    multiplier = _fraction(right, "quantity_multiplier_numerator", "quantity_multiplier_denominator") / _fraction(plan, "ratio_numerator", "ratio_denominator")
                    right.update(lot_id=new_id, instrument_hash=plan["new_instrument_hash"], predecessor_entitlement_id=right["entitlement_id"], quantity_multiplier_numerator=multiplier.numerator, quantity_multiplier_denominator=multiplier.denominator)
                links.append({"old_lot_id": lot["lot_id"], "old_quantity": lot["quantity"], "old_cost_units": lot["accounting_cost_units"], "successor_lot": successor, "fractional_quantity_numerator": fractional.numerator, "fractional_quantity_denominator": fractional.denominator, "cash_consideration_units": amount, "cash_allocated_cost_units": lot["accounting_cost_units"] - new_cost, "carried_entitlement_ids": [right["entitlement_id"] for right in rights]})
                del self.lots[lot["lot_id"]]
            # 旧批次已经注销，后继解禁按继承后的可卖时点独立核验。
            predecessor_ids = {link["old_lot_id"] for link in links}
            self.opening_schedule = self.opening_schedule[:self.opening_cursor] + [
                entry for entry in self.opening_schedule[self.opening_cursor:]
                if not (entry[1] == "opening_release" and entry[2] in predecessor_ids)
            ]
            conversion = {"plan": plan, "links": links, "registered": False, "released": False, "cash_paid": False, "sequence": self.record["sequence"], "released_lot_ids": []}
            self.conversions[key] = conversion
            self.average_costs()
            quantity = sum(link["successor_lot"]["quantity"] for link in links if link["successor_lot"] is not None)
            amount = sum(link["cash_consideration_units"] for link in links)
            claim = f"conversion-cash:{key}"
            self.claim(claim, amount, plan["cash_pay_at"])
            self.emit("security_conversion", {"conversion_id": key, "old_instrument_hash": plan["old_instrument_hash"], "old_quantity": sum(link["old_quantity"] for link in links), "new_instrument_hash": plan["new_instrument_hash"], "successor_quantity": quantity, "successor_entitlement_id": f"successor:{key}", "register_at": plan["registered_at"], "tradable_at": plan["tradable_at"], "cash_receivable_units": amount, "receivable_id": claim, "cash_due_date": _time(plan["cash_pay_at"]).date().isoformat()})
            return
        conversion = self.conversions.get(key)
        _require(conversion is not None, "后继证券尚未注销登记")
        lots = [link["successor_lot"] for link in conversion["links"] if link["successor_lot"] is not None]
        quantity = sum(lot["quantity"] for lot in lots)
        if kind == "successor_registered":
            _require(not refs and not conversion["registered"] and at >= _time(plan["registered_at"]), "后继证券登记提前或重复")
            for lot in lots:
                _require(lot["lot_id"] not in self.lots, "后继批次身份重复")
                self.lots[lot["lot_id"]] = deepcopy(lot)
            if quantity:
                self.emit("successor_registered", {"conversion_id": key, "instrument_hash": plan["new_instrument_hash"], "entitlement_id": f"successor:{key}", "quantity": quantity, "cash_units": 0})
            conversion["registered"] = True
        elif kind == "successor_released":
            _same(business_id, f"{key}:{self.record['effective_time']}", "后继解禁业务键不符")
            _require(conversion["registered"] and not conversion["released"] and at >= _time(plan["tradable_at"]), "后继证券未登记或提前重复解禁")
            eligible = [lot for lot in lots if lot["lot_id"] not in conversion["released_lot_ids"] and _time(lot["sellable_at"]) <= at]
            _require(bool(eligible) or not quantity, "后继批次尚有原股份交易限制")
            released_quantity = sum(lot["quantity"] for lot in eligible)
            conversion["released_lot_ids"] = sorted([*conversion["released_lot_ids"], *(lot["lot_id"] for lot in eligible)])
            conversion["released"] = len(conversion["released_lot_ids"]) == len(lots)
            if released_quantity:
                self.emit("settlement", {"instrument_hash": plan["new_instrument_hash"], "quantity": released_quantity, "cash_units": 0})
        elif kind == "conversion_cash_paid":
            _require(not refs and not conversion["cash_paid"], "换股现金重复支付")
            self.pay_claim(f"conversion-cash:{key}", sum(link["cash_consideration_units"] for link in conversion["links"]), plan["cash_pay_at"])
            conversion["cash_paid"] = True

    def action_position(self, values, action, quantity):
        position = values.get("record_position")
        if position is not None:
            _same(position["instrument_hash"], action["instrument_hash"], "公司行动登记证券不符")
            _same(integer(position["quantity"], "登记数量", minimum=0), quantity, "公司行动登记数量与逐批事实不符")
            at = _time(position["record_time"])
            _require(at.date().isoformat() == action["record_date"] and at.hour == 15 and at.minute == 0 and at.second == 0, "公司行动登记不是实际登记日收盘")
            _require(bool(position["source_ref"]), "公司行动登记持仓没有来源")
        return position

    def corporate_event(self, key, phase, expected, event_id):
        action = self.actions[key]
        event = self.events.get(event_id)
        _require(event is not None and event_id not in self.used_events, "公司行动缺少唯一金融事件")
        _same((event["kind"], event["group_id"], event["parent_id"], event["effective_time"], event["session"]),
              ("corporate_action", self.group, key, self.record["effective_time"], self.record["effective_time"][:10]), "公司行动金融事件与业务记录不符")
        _same(dict(event["payload"]), expected, "公司行动金融事件数量金额或来源与独立计算不符")
        _require(isinstance(event["rule_hash"], str) and len(event["rule_hash"]) == 64, "公司行动缺少规则身份")
        _same(event_id, f"corporate:{self.action_hashes[key]}" + (":shares-arrival" if phase == "shares_arrival" else ""), "公司行动金融事件身份不符")
        _require(_time(action["announcement_available_time"]) <= _time(self.record["effective_time"]), "公司行动公告在使用时尚不可见")
        self.used_events.add(event_id)

    def corporate_action(self, kind):
        record = self.record
        prefix = f"spot:{self.account}:{kind}:"
        _require(record["event_id"].startswith(prefix), "公司行动业务键不符")
        key = record["event_id"][len(prefix):]
        action = self.actions.get(key)
        _require(action is not None, "公司行动业务缺少封存源载荷")
        at = _time(record["effective_time"])
        source = {"source_ref": action.get("source_ref") or f"corporate:{key}", "available_at": action["announcement_available_time"]}
        self.bind(kind, key, record["effective_time"], source, record["business_refs"])
        row = self.corporate_declared[key]
        current = sorted((deepcopy(lot) for lot in self.lots.values() if lot["instrument_hash"] == action["instrument_hash"]), key=lambda lot: lot["lot_id"])
        if kind == "corporate_action_registered":
            _require(key not in self.corporate and not record["business_refs"], "公司行动重复登记或带有错误引用")
            _same(at.date().isoformat(), action["record_date"], "公司行动登记日期与源事实不符")
            self.corporate[key] = {"action_payload": row["action_payload"], "action_id": key, "revision": action["revision"], "record_lots": current, "applied": False, "financial_event_id": None, "lot_rule": None, "source": source, "links": [], "arrival_event_id": None}
            return
        _require(len(record["business_refs"]) == 1, "公司行动投影缺少唯一金融事件引用")
        event_id = record["business_refs"][0]
        event = self.events.get(event_id)
        _require(event is not None, "公司行动引用未知金融事件")
        values = dict(event["payload"])
        existing = self.corporate.get(key)
        if kind == "corporate_action_arrived":
            _require(existing is not None and existing["applied"] and existing["arrival_event_id"] is None, "股份到账未有生效事实或重复到账")
            _same(at.date().isoformat(), action.get("shares_arrival_date"), "股份到账日期不符")
            arrived = sum(link["successor_lot"]["quantity"] for link in existing["links"] if link["successor_lot"] is not None)
            position = self.action_position(values, action, sum(lot["quantity"] for lot in existing["record_lots"]))
            expected = {"contract_version": 2, "action_hash": self.action_hashes[key], "action_kind": action["kind"], "action_revision": action["revision"], "action_phase": "shares_arrival", "source_ref": action["source_ref"], "instrument_hash": action["instrument_hash"], "arrived_quantity": arrived, "shares_arrival_date": action["shares_arrival_date"], "shares_sellable_date": action["shares_sellable_date"], "record_position": position, "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0, "frozen_delta": 0}
            self.corporate_event(key, "shares_arrival", expected, event_id)
            existing["arrival_event_id"] = event_id
            return
        _same(at.date().isoformat(), action["effective_date"], "公司行动生效日不符")
        _require(existing is None or not existing["applied"], "公司行动重复生效")
        if action["kind"] == "delisting_cash":
            _require(key in self.delist_removed, "退市注销缺少法定转让与税权消费")
            current = self.delist_removed[key]
        if existing is None:
            _require(action["record_date"] == action["effective_date"] or action["kind"] == "delisting_cash", "跨日公司行动缺少真实登记批次")
            recorded = deepcopy(current)
        else:
            recorded = existing["record_lots"]
        links = []
        rule = self.corporate_rules.get(key)
        before_quantity = sum(lot["quantity"] for lot in current)
        before_sellable = sum(lot["quantity"] for lot in current if _time(lot["sellable_at"]) <= at)
        before_unsettled = before_quantity - before_sellable
        cash_amount = 0
        new_quantity = 0
        ratio = Fraction(action["ratio_numerator"], action["ratio_denominator"])
        if action["kind"] == "delisting_cash":
            _require(action.get("contract_version") == 2, "退市现金必须声明有来源的v2事实")
            _require(_time(action["settlement_available_time"]) <= at and action["trading_termination_date"] <= at.date().isoformat(), "退市价格或终止交易事实来自未来")
            cash_amount = _rounded(Fraction(before_quantity * action["cash_per_share_microunits"], 10000)) if action["cash_per_share_microunits"] else before_quantity * action["cash_per_share_units"]
        else:
            _require(rule is not None, "送转拆股缺少明确批次规则")
            _visible(rule["source"], at)
            _require(_time(rule["sellable_at"]) >= at, "公司行动新增股份在生效前可卖")
            if action.get("contract_version") == 2:
                _same(_time(rule["sellable_at"]).date().isoformat(), action["shares_sellable_date"], "股份可卖规则与来源日期不一致")
            if action["kind"] in {"split", "reverse_split"}:
                _same(sum(lot["quantity"] for lot in recorded), before_quantity, "拆并股登记与生效股份中间变动未解释")
                _require(rule["acquisition_date_policy"] == "inherit", "拆并股必须继承真实取得日")
                incremental = action.get("contract_version") == 2 and ratio >= 1
                for lot in current:
                    scaled = lot["quantity"] * ratio
                    _require(scaled.denominator == 1, "逐批拆并股数量不是整数，缺少零碎规则")
                    born = int(scaled) - lot["quantity"] if incremental else int(scaled)
                    if not born:
                        continue
                    new_id = f"corporate:{key}:{lot['lot_id']}"
                    successor = {**lot, "lot_id": new_id, "quantity": born, "accounting_cost_units": 0 if incremental else lot["accounting_cost_units"], "sellable_at": max(_time(lot["sellable_at"]), _time(rule["sellable_at"])).isoformat(), "source": source, "predecessor_lot_id": lot["lot_id"]}
                    if not incremental:
                        self.lots.pop(lot["lot_id"])
                    _require(new_id not in self.lots, "公司行动后继批次身份重复")
                    self.lots[new_id] = deepcopy(successor)
                    carried = []
                    related = sorted((deepcopy(right) for right in self.rights.values() if right["lot_id"] == lot["lot_id"] and right["remaining_quantity"]), key=lambda right: right["entitlement_id"])
                    _require(not related or rule["tax_right_policy"] == "scale", "拆并股没有明确税权比例承接")
                    for right in related:
                        multiplier = _fraction(right, "quantity_multiplier_numerator", "quantity_multiplier_denominator") / ratio
                        if incremental:
                            remaining = _fraction(right, "remaining_quantity", "remaining_quantity_denominator")
                            registered = _fraction(right, "registered_quantity", "registered_quantity_denominator")
                            uncredited = right["prewithheld_units"] - _rounded(right["prewithheld_units"] * (registered - remaining) / registered)
                            old_part = remaining / ratio
                            old_credit = _rounded(Fraction(uncredited, 1) / ratio)
                            self.rights[right["entitlement_id"]].update(remaining_quantity=0, remaining_quantity_denominator=1)
                            for suffix, destination, part, credit in (("existing", lot["lot_id"], old_part, old_credit), ("bonus", new_id, remaining - old_part, uncredited - old_credit)):
                                identity = f"{right['entitlement_id']}:{key}:{suffix}"
                                _require(identity not in self.rights, "拆股后继税权身份重复")
                                successor_right = {**right, "entitlement_id": identity, "lot_id": destination, "registered_quantity": part.numerator, "registered_quantity_denominator": part.denominator, "remaining_quantity": part.numerator, "remaining_quantity_denominator": part.denominator, "prewithheld_units": credit, "predecessor_entitlement_id": right["entitlement_id"], "quantity_multiplier_numerator": multiplier.numerator, "quantity_multiplier_denominator": multiplier.denominator}
                                self.rights[identity] = successor_right
                                carried.append(identity)
                        else:
                            self.rights[right["entitlement_id"]].update(lot_id=new_id, quantity_multiplier_numerator=multiplier.numerator, quantity_multiplier_denominator=multiplier.denominator)
                            carried.append(right["entitlement_id"])
                    links.append({"old_lot_id": lot["lot_id"], "old_quantity": lot["quantity"], "old_cost_units": lot["accounting_cost_units"], "successor_lot": successor, "fractional_quantity_numerator": 0, "fractional_quantity_denominator": 1, "cash_consideration_units": 0, "cash_allocated_cost_units": 0, "carried_entitlement_ids": carried})
                    new_quantity += born if incremental else born - lot["quantity"]
            else:
                _require(ratio > 1 and rule["tax_right_policy"] != "scale", "送股比例或税权承接方式不符")
                _require(rule["tax_right_policy"] != "require_no_rights" or not any(right["instrument_hash"] == action["instrument_hash"] and right["remaining_quantity"] for right in self.rights.values()), "送股来源不允许现存税权")
                for lot in recorded:
                    born = lot["quantity"] * (ratio - 1)
                    _require(born.denominator == 1, "逐批送股数量不是整数，缺少零碎规则")
                    if not born:
                        continue
                    new_id = f"corporate:{key}:{lot['lot_id']}"
                    _require(new_id not in self.lots, "送股批次身份重复")
                    successor = {"lot_id": new_id, "instrument_hash": action["instrument_hash"], "quantity": int(born), "accounting_cost_units": 0, "acquired_on": lot["acquired_on"] if rule["acquisition_date_policy"] == "inherit" else action["effective_date"], "sellable_at": rule["sellable_at"], "acquisition_method": rule["acquisition_method"], "security_class": lot["security_class"], "source": source, "predecessor_lot_id": lot["lot_id"], "predecessor_conversion_id": None}
                    self.lots[new_id] = deepcopy(successor)
                    links.append({"old_lot_id": lot["lot_id"], "old_quantity": lot["quantity"], "old_cost_units": lot["accounting_cost_units"], "successor_lot": successor, "fractional_quantity_numerator": 0, "fractional_quantity_denominator": 1, "cash_consideration_units": 0, "cash_allocated_cost_units": 0, "carried_entitlement_ids": []})
                    new_quantity += int(born)
        registered_quantity = before_quantity if action["kind"] == "delisting_cash" else sum(lot["quantity"] for lot in recorded)
        expected = {"instrument_hash": action["instrument_hash"], "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0}
        version2 = action.get("contract_version") == 2
        if version2:
            expected.update(contract_version=2, action_hash=self.action_hashes[key], action_kind=action["kind"], action_revision=action["revision"], action_phase="effective", source_ref=action["source_ref"], frozen_delta=0, registered_quantity=registered_quantity, record_date=action["record_date"], ex_date=action["ex_date"], pay_date=action["pay_date"])
            if action["kind"] != "delisting_cash":
                position = self.action_position(values, action, registered_quantity)
                if position is not None:
                    expected["record_position"] = position
                expected.update(shares_arrival_date=action["shares_arrival_date"], shares_sellable_date=action["shares_sellable_date"])
        if action["kind"] == "delisting_cash":
            expected.update(sellable_delta=-before_sellable, unsettled_delta=-before_unsettled, frozen_delta=0, cancel_position_entitlements=True, trading_termination_date=action["trading_termination_date"], settlement_available_time=action["settlement_available_time"])
        elif new_quantity >= 0:
            due = action["shares_sellable_date"] if version2 else action["pay_date"]
            if due > at.date().isoformat() and new_quantity:
                expected.update(position_entitlement_quantity=new_quantity, position_due_date=due, entitlement_id=f"position:{self.action_hashes[key]}")
                born_lots = [link["successor_lot"]["lot_id"] for link in links if link["successor_lot"] is not None]
                self.position_rights[expected["entitlement_id"]] = {"quantity": new_quantity, "due": due, "lots": born_lots}
            else:
                expected["sellable_delta"] = new_quantity
        else:
            if version2 and action["shares_sellable_date"] > at.date().isoformat():
                total = sum(link["successor_lot"]["quantity"] for link in links)
                expected.update(sellable_delta=-before_sellable, unsettled_delta=-before_unsettled, position_entitlement_quantity=total, position_due_date=action["shares_sellable_date"], entitlement_id=f"position:{self.action_hashes[key]}")
                self.position_rights[expected["entitlement_id"]] = {"quantity": total, "due": action["shares_sellable_date"], "lots": [link["successor_lot"]["lot_id"] for link in links]}
            elif version2:
                buckets = {"sellable": before_sellable, "unsettled": before_unsettled, "frozen": 0}
                target = before_quantity + new_quantity
                converted = {name: int(quantity * ratio) for name, quantity in buckets.items()}
                residues = sorted(buckets, key=lambda name: (-(buckets[name] * ratio - converted[name]), name))
                for name in residues[:target - sum(converted.values())]:
                    converted[name] += 1
                expected.update({name + "_delta": converted[name] - quantity for name, quantity in buckets.items()})
            else:
                expected["sellable_delta"] = new_quantity
        if cash_amount:
            if action["pay_date"] > at.date().isoformat():
                claim = f"cash:{self.action_hashes[key]}"
                expected.update(cash_receivable_units=cash_amount, cash_due_date=action["pay_date"], receivable_id=claim)
                self.claim(claim, cash_amount, action["pay_date"] + "T00:00:00+08:00")
            else:
                expected["cash_delta_units"] = cash_amount
                self.cash += cash_amount
        self.average_costs()
        self.corporate_event(key, "effective", expected, event_id)
        self.corporate[key] = {"action_payload": row["action_payload"], "action_id": key, "revision": action["revision"], "record_lots": recorded, "applied": True, "financial_event_id": event_id, "lot_rule": rule, "source": source, "links": links, "arrival_event_id": None}

    def snapshot(self, snapshot, cash, positions, valuation):
        at = _time(snapshot["valuation_time"])
        _same(date_value(snapshot["session"], "账户会话"), at.date(), "账户快照会话与时点不符")
        for row in [cash, valuation, *positions]:
            _same(_time(row["valuation_time"]), at, "账户与canonical估值时点不一致")
        self.advance(at)
        liabilities = sum(self.payables.values())
        pending = sum(_rounded(sum(link["successor_lot"]["quantity"] for link in item["links"] if link["successor_lot"] is not None) * _fraction(item["plan"], "interim_price_numerator", "interim_price_denominator")) for item in self.conversions.values() if not item["registered"])
        _same((integer(snapshot["liabilities_units"], "快照负债", minimum=0), integer(snapshot["pending_successor_units"], "快照后继权益", minimum=0), integer(snapshot["opening_nav_units"], "快照期初NAV", minimum=0 if self.allow_zero_opening else 1)), (liabilities, pending, self.opening_nav), "账户快照负债后继权益或期初NAV与独立计算不符")
        _same(cash["total_cash_units"], self.total_cash(), "账户现金应收与canonical不闭合")
        _same(cash["receivable_cash_units"], sum(self.receivables.values()), "账户应收与canonical不闭合")
        _same(cash["available_cash_units"], self.cash, "账户可用现金与canonical不闭合")
        _same(cash["opening_cash_units"], self.opening_cash, "canonical期初现金与明细不符")
        expected_quantity = {}
        expected_sellable = {}
        for lot in self.lots.values():
            key = lot["instrument_hash"]
            expected_quantity[key] = expected_quantity.get(key, 0) + lot["quantity"]
            conversion_id = lot["predecessor_conversion_id"]
            released = conversion_id is None or self.conversions[conversion_id]["released"] or lot["lot_id"] in self.conversions[conversion_id]["released_lot_ids"] or lot["predecessor_lot_id"] in self.conversions[conversion_id]["released_lot_ids"]
            released = released and not any(lot["lot_id"] in right["lots"] for right in self.position_rights.values())
            if _time(lot["sellable_at"]) <= at and released:
                expected_sellable[key] = expected_sellable.get(key, 0) + lot["quantity"]
        actual = _index(positions, "instrument_hash")
        _same({key: row["quantity"] for key, row in actual.items() if row["quantity"]}, {key: quantity for key, quantity in expected_quantity.items() if quantity}, "账户批次数量与canonical持仓不闭合")
        for key, row in actual.items():
            _same((row["sellable_quantity"], row["unsettled_quantity"], row["frozen_quantity"]), (expected_sellable.get(key, 0), expected_quantity.get(key, 0) - expected_sellable.get(key, 0), 0), "账户可卖或未到账桶与canonical不符")
        if self.credit_oracle is not None:
            return self.credit_oracle.snapshot(self, snapshot["session"], positions, valuation, pending - liabilities)
        _same(valuation["valuation_model"], "cash_plus_positions_and_account_rights_v1", "账户估值模型不符")
        _same(valuation["nav_units"], self.total_cash() + sum(integer(row["market_value_units"], "持仓市值", minimum=0) for row in positions) + pending - liabilities, "账户负债权益与canonical NAV不闭合")
        return pending - liabilities

    def finish(self, at):
        _require(self.next_record is None, "账户含末次估值之后的业务事件")
        if self.credit_oracle is None:
            _same(self.used_fills, set(self.fills), "账户账本漏记真实成交")
        else:
            _same(len(self.used_fills), len(self.fill_source), "信用账户没有完整覆盖canonical成交")
            self.credit_oracle.finish(self)
        _same(self.used_events, set(self.events), "账户包含无业务依据的金融事件")
        _same(self.used_acquisitions, set(self.acquisitions), "账户买入取得事实没有真实成交")
        _same(self.lots, _index(self.book["lots"], "lot_id"), "期末持仓批次数量成本或取得继承不符")
        _same(self.rights, _index(self.book["entitlements"], "entitlement_id"), "期末税权数量来源或继承不符")
        _same(self.corporate, self.corporate_declared, "公司行动登记批次数量成本税权或到账链接不符")
        _same(self.transfers, self.declared["transfers"], "转让业务明细不闭合")
        _same(self.assessments, self.declared["assessments"], "税务核定业务明细不闭合")
        _same(self.collections, self.declared["collections"], "税务扣收业务明细不闭合")
        def consumption_key(row):
            return row["sequence"], row["lot_id"], row["entitlement_id"]

        _same(sorted(self.consumptions, key=consumption_key), sorted(self.book["consumptions"], key=consumption_key), "税权分次消费余额或关联不符")
        _same(sorted(self.dividends.values(), key=lambda row: row["sequence"]), sorted(self.book["dividends"], key=lambda row: row["sequence"]), "分红登记或到账事实不闭合")
        _same(sorted(self.conversions.values(), key=lambda row: row["sequence"]), sorted(self.book["conversions"], key=lambda row: row["sequence"]), "换股数量成本税权或阶段状态不闭合")
        for key, action in self.actions.items():
            item = self.corporate.get(key)
            _require(item is not None, "公司行动源事实没有业务记录")
            if action["effective_date"] <= at.date().isoformat():
                _require(item["applied"], "公司行动已生效但遗漏批次投影")
            if item["applied"] and action.get("contract_version") == 2 and action.get("shares_arrival_date") is not None and action["shares_arrival_date"] <= at.date().isoformat() and item["links"]:
                _require(item["arrival_event_id"] is not None, "公司行动股份到账事实遗漏")
        _require(all(right["due"] > at.date().isoformat() for right in self.position_rights.values()), "终态存在已到期未解禁股份权益")
        for key, instruction in self.instructions.items():
            if _time(instruction["effective_time"]) <= at:
                _require(key in self.used_instructions, "到期税务计划未执行")
        for key, plan in self.dividend_plans.items():
            for field, stage in (("record_at", None), ("ex_at", "recognized"), ("pay_at", "paid")):
                if _time(plan[field]) <= at:
                    _require(key in self.dividends and (stage is None or self.dividends[key][stage]), "分红计划存在遗漏阶段")
        for key, plan in self.conversion_plans.items():
            for field, stage in (("cancelled_at", None), ("registered_at", "registered"), ("cash_pay_at", "cash_paid")):
                if _time(plan[field]) <= at:
                    _require(key in self.conversions and (stage is None or self.conversions[key][stage]), "换股计划存在遗漏阶段")
        for row in self.assessments.values():
            if _time(row["due_at"]) <= at:
                _require(not self.payables.get(row["assessment_id"], 0), "终态存在已到期未缴税款")
        for item in self.conversions.values():
            if item["registered"]:
                eligible = {link["successor_lot"]["lot_id"] for link in item["links"] if link["successor_lot"] is not None and _time(link["successor_lot"]["sellable_at"]) <= at}
                _require(eligible.issubset(set(item["released_lot_ids"])), "后继股份已到可卖时点但遗漏解禁事件")
        _require(not self.opening["exact_dividend_tax"] or all(row["assessment_id"] is not None for row in self.consumptions), "精确税务模式遗漏已交割权利的核定")


def verify_spot_account_context(*, context, canonical, allow_zero_opening=False, cashflow_oracle=None, credit_oracle=None) -> dict[str, int]:
    """返回每会话独立计算的后继权益减负债，供六表校验调整 NAV。

    canonical 沿用金融 oracle 的六表行序列接口。价格与普通公司行动
    的原始行情复核由日频金融 oracle 承担；本函数复核账户金融事实。
    acquisition_lots 保留所有买入取得事实，包括期末已全部卖出的批次。
    """
    try:
        _require(isinstance(context, Mapping) and context.get("contract_version") == CONTRACT_VERSION, "现货账户上下文版本无效")
        _same(set(canonical), {"orders", "fills", "positions", "cash", "costs", "valuations"}, "账户canonical六表不完整")
        replay = _AccountReplay(context, canonical, allow_zero_opening=allow_zero_opening, cashflow_oracle=cashflow_oracle, credit_oracle=credit_oracle)
        snapshots = list(context["snapshots"])
        _require(bool(snapshots), "账户缺少会话快照")
        snapshot_map = _index(snapshots, "session")
        cash_rows = ordered_rows(canonical["cash"], order_by=("session", "snapshot_id"))
        valuation_rows = iter(ordered_rows(canonical["valuations"], order_by=("session", "snapshot_id")))
        positions = iter(groupby(ordered_rows(canonical["positions"], order_by=("session", "snapshot_id", "instrument_hash")), key=lambda row: date_value(row["session"], "持仓会话").isoformat()))
        current_positions = next(positions, None)
        adjustments = {}
        previous_at = replay.start
        for cash in cash_rows:
            session = date_value(cash["session"], "现金会话").isoformat()
            _require(session in snapshot_map and session not in adjustments, "账户与canonical会话不闭合或多账户未分组")
            snapshot = snapshot_map[session]
            at = _time(snapshot["valuation_time"])
            _require(at >= previous_at, "账户估值时点倒退")
            previous_at = at
            valuation = next(valuation_rows, None)
            _require(valuation is not None and date_value(valuation["session"], "估值会话").isoformat() == session, "账户与canonical估值会话不一致")
            _same((cash["portfolio_id"], cash["snapshot_id"]), (valuation["portfolio_id"], valuation["snapshot_id"]), "账户现金与估值快照键不一致")
            held = []
            if current_positions is not None and current_positions[0] == session:
                held = list(current_positions[1])
                for row in held:
                    _same((row["portfolio_id"], row["snapshot_id"]), (cash["portfolio_id"], cash["snapshot_id"]), "账户持仓快照键不一致")
                current_positions = next(positions, None)
            adjustments[session] = replay.snapshot(snapshot, cash, held, valuation)
        _same(set(adjustments), set(snapshot_map), "账户快照与canonical现金会话不闭合")
        _require(next(valuation_rows, None) is None and current_positions is None, "canonical存在无账户快照的估值或持仓")
        replay.finish(previous_at)
        return adjustments
    except EvidenceContractError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, ZeroDivisionError, OverflowError) as exc:
        raise EvidenceContractError(f"现货账户事实合同无效: {exc}") from exc


__all__ = ["verify_spot_account_context"]
