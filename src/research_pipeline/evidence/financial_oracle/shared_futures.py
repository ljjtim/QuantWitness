"""从封存输入独立重放共享期货账户、订单和真实成交。"""
from __future__ import annotations

from collections import defaultdict
from itertools import groupby
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Mapping

import pyarrow as pa

from research_pipeline.domain.shared_futures import (
    validate_shared_futures_events, validate_shared_futures_spec,
)
from research_pipeline.domain.shared_futures_result import (
    SHARED_FUTURES_COLUMNS, SHARED_FUTURES_INTEGER_COLUMNS,
    SHARED_FUTURES_KEYS, SHARED_FUTURES_TIMESTAMP_COLUMNS,
)
from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, require_unique


def _time(value):
    return aware_datetime(value, "共享期货事件时间")


def _round(value):
    """精确有理数逐事件半升到分；负损益使用同样的绝对值舍入。"""
    value = Fraction(value)
    sign = -1 if value < 0 else 1
    value = abs(value)
    return sign * ((value.numerator * 2 + value.denominator) // (value.denominator * 2))


def _rows(table, name):
    if isinstance(table, pa.Table):
        rows = table.to_pylist()
        columns = table.column_names
        for column in SHARED_FUTURES_COLUMNS[name]:
            if column not in columns:
                raise EvidenceContractError(f"共享期货 {name} schema 缺少字段")
            kind = table.schema.field(column).type
            valid = (pa.types.is_int64(kind) if column in SHARED_FUTURES_INTEGER_COLUMNS
                     else pa.types.is_timestamp(kind) and kind.tz is not None and kind.unit == "ns"
                     if column in SHARED_FUTURES_TIMESTAMP_COLUMNS
                     else pa.types.is_date32(kind) if column == "session" else pa.types.is_string(kind))
            if not valid:
                raise EvidenceContractError(f"共享期货 {name}.{column} 物理类型不符合正式 schema")
    elif hasattr(table, "to_dict"):
        rows = table.to_dict("records")
        columns = list(table.columns)
    else:
        rows = [dict(row) for row in table]
        columns = list(rows[0]) if rows else list(SHARED_FUTURES_COLUMNS[name])
    if set(columns) != set(SHARED_FUTURES_COLUMNS[name]):
        raise EvidenceContractError(f"共享期货 {name} schema 不完整")
    for row in rows:
        for field in SHARED_FUTURES_COLUMNS[name]:
            value = row[field]
            if field in SHARED_FUTURES_TIMESTAMP_COLUMNS:
                row[field] = _time(value)
            elif field == "session":
                row[field] = date_value(value, "session")
            elif field in SHARED_FUTURES_INTEGER_COLUMNS:
                if isinstance(value, bool) or not isinstance(value, int):
                    raise EvidenceContractError(f"共享期货 {name}.{field} 必须为整数")
    require_unique(rows, SHARED_FUTURES_KEYS[name], f"共享期货 {name}")
    return rows



def _effective_time(rule):
    return _time(rule.get("effective_at") or rule["effective_from"]+"T00:00:00+08:00")


_TERMINAL = {"filled", "rejected", "cancelled", "expired"}


class _Replay:
    """只依赖输入的经济重放；结果表不参与决定订单、成交或账户状态。"""

    def __init__(self, spec, events):
        self.spec, self.events = spec, events
        self.instruments = {row["instrument_id"]: row for row in spec["instruments"]}
        self.common = {key: spec[key] for key in ("portfolio_id", "account_id", "currency")}
        self.cash = spec["initial_cash_units"]
        self.positions, self.prices, self.rules = {}, {}, {}
        self.orders, self.reservations, self.targets, self.rolls = {}, {}, {}, {}
        self.exiting, self.session_activity, self.settled, self.closed_sessions = set(), set(), set(), set()
        self.sessions = {}
        self.session_facts = {(row["instrument_id"], row["trading_date"]): row for row in spec["sessions"]}
        self.risk_state, self.risk_since, self.resume_after = "normal", None, None
        self.current_time = min(_time(row["event_time"]) for row in events)
        self.session = date.fromisoformat(min(row["trading_date"] for row in events))
        self.sequence = self.ordinal = 0
        self.output = {name: [] for name in SHARED_FUTURES_COLUMNS}

    def row(self, name, **fields):
        self.output[name].append({**self.common, **fields})

    def event_row(self, name, **fields):
        self.sequence += 1
        self.row(name, event_time=self.current_time, sequence=self.sequence, session=self.session, **fields)

    def rule(self, code, session, at):
        eligible = [r for r in self.spec["rules"] if r["instrument_id"] == code
                    and r["effective_from"] <= str(session) <= r["effective_to"]
                    and _time(r["available_at"]) <= at and _effective_time(r) <= at]
        if not eligible:
            raise EvidenceContractError("共享期货事件缺少当时可见的有效规则")
        latest = max(_effective_time(row) for row in eligible)
        current = [row for row in eligible if _effective_time(row) == latest]
        if len(current) != 1:
            raise EvidenceContractError("共享期货同刻可见规则有歧义")
        return current[0]

    def refresh_rules(self):
        for code in self.prices:
            self.rules[code] = self.rule(code, self.sessions.get(code, self.session.isoformat()), self.current_time)

    def notional(self, code, quantity, price):
        instrument = self.instruments[code]
        return Fraction(price, instrument["price_scale"])*Fraction(instrument["contract_multiplier"])*quantity*100

    def margin(self, code, quantity, *, price=None, rule=None):
        value = self.notional(code, quantity, self.prices[code] if price is None else price)
        return _round(value*Fraction((self.rules[code] if rule is None else rule)["margin_rate"]))

    def fee(self, code, effect, bucket, quantity, price, rule):
        field = "open_fee" if effect == "open" else "close_today_fee" if bucket == "today" else "close_fee"
        rate = Fraction(rule[field])
        return _round(rate*quantity*100 if rule["fee_unit"] == "per_contract_cny"
                      else self.notional(code, quantity, price)*rate)

    def pnl(self, key, quantity, price):
        code, direction, _ = key
        basis = self.positions[key][1]
        value = (Fraction(price)-basis)*Fraction(self.instruments[code]["contract_multiplier"])*quantity*100/self.instruments[code]["price_scale"]
        return _round(value if direction == "long" else -value)

    def bucket_margins(self):
        result = {}
        for code, direction in sorted({key[:2] for key in self.positions}):
            count = previous = 0
            for bucket in ("yesterday", "today"):
                key = (code, direction, bucket)
                if key in self.positions:
                    count += self.positions[key][0]
                    current = self.margin(code, count)
                    result[key] = current-previous
                    previous = current
        return result

    def amounts(self):
        unrealized = sum(self.pnl(key, count, self.prices[key[0]]) for key, (count, _) in self.positions.items())
        margin = sum(self.bucket_margins().values())
        frozen = sum(row["margin"] for row in self.reservations.values())
        equity = self.cash+unrealized
        return dict(cash_units=self.cash, unrealized_pnl_units=unrealized, equity_units=equity,
                    margin_units=margin, frozen_units=frozen, available_units=equity-margin-frozen)

    def snapshot(self):
        self.sequence += 1
        self.row("cash", event_time=self.current_time, sequence=self.sequence, session=self.session,
                 **self.amounts(), cash_scale=100, risk_state=self.risk_state)
        self.row("valuations", event_time=self.current_time, sequence=self.sequence, session=self.session,
                 nav_units=self.amounts()["equity_units"], valuation_model="shared_futures_equity_v1")
        margins = self.bucket_margins()
        for key, (count, cost) in sorted(self.positions.items()):
            code, direction, bucket = key
            scale = self.instruments[code]["price_scale"]
            quote = cost/scale
            self.row("positions", event_time=self.current_time, sequence=self.sequence, session=self.session,
                instrument_id=code, direction=direction, position_bucket=bucket, quantity=count,
                base_price=str(Decimal(quote.numerator)/Decimal(quote.denominator)),
                cost_numerator=str(quote.numerator), cost_denominator=str(quote.denominator),
                price_scale=scale, margin_units=margins[key])

    def quantity(self, code, direction, bucket=None):
        return sum(count for (instrument, side, part), (count, _) in self.positions.items()
                   if (instrument, side) == (code, direction) and (bucket is None or bucket == part))

    def active(self, *, code=None, origin=None):
        return [order for order in self.orders.values() if order["status"] not in _TERMINAL
                and (code is None or code == order["instrument_id"]) and (origin is None or origin == order["origin"])]

    def allocation(self, order, count):
        code, direction, effect = order["instrument_id"], order["direction"], order["position_effect"]
        if effect.startswith("close_"):
            buckets = (effect[6:],)
        else:
            buckets = ("yesterday", "today") if self.rules[code]["close_order"] == "yesterday_first" else ("today", "yesterday")
        legs = []
        for bucket in buckets:
            frozen = sum(row[bucket] for oid, row in self.reservations.items()
                         if oid != order["order_id"] and row["instrument_id"] == code and row["direction"] == direction)
            available = self.quantity(code, direction, bucket)-frozen
            take = min(count, available)
            if take > 0:
                legs.append((bucket, take))
                count -= take
        return legs

    def roll_fact(self, plan_id, order_id=None):
        plan = self.rolls[plan_id]
        reserved = sum(row["opening_quantity"] for row in self.reservations.values() if row["roll_plan_id"] == plan_id)
        self.event_row("rolls", roll_plan_id=plan_id, old_instrument_id=plan["old_instrument_id"],
            new_instrument_id=plan["new_instrument_id"], closed_quantity=plan["closed_quantity"],
            opened_quantity=plan["opened_quantity"], reserved_new_quantity=reserved,
            available_new_quantity=max(0, min(plan["quantity"], plan["closed_quantity"])-plan["opened_quantity"]-reserved), order_id=order_id)

    def reserve(self, order, current, reason):
        oid = order["order_id"]
        previous = self.reservations.pop(oid, None) or {"margin": 0, "today": 0, "yesterday": 0}
        if current is not None:
            self.reservations[oid] = current
        after = current or {"margin": 0, "today": 0, "yesterday": 0}
        for kind in ("margin", "today", "yesterday"):
            if previous[kind] != after[kind]:
                self.event_row("reservations", order_id=oid, reservation_type=kind,
                    delta_units=after[kind]-previous[kind], remaining_units=after[kind], reason=reason)
        if order["roll_plan_id"]:
            self.roll_fact(order["roll_plan_id"], oid)

    def terminal(self, order, reason, status="cancelled"):
        if order["status"] in _TERMINAL:
            return
        if status == "rejected" and order["filled_quantity"]:
            status = "cancelled"
        order["status"], order["terminal_reason"] = status, reason
        self.reserve(order, None, reason)

    def affordable_count(self, maximum, funds, expense):
        low, high = 0, maximum
        while low < high:
            middle = (low+high+1)//2
            if expense(middle) <= funds:
                low = middle
            else:
                high = middle-1
        return low

    def reserve_remaining(self, order, rule, price, *, admission=False):
        if order["status"] in _TERMINAL:
            return
        code, effect = order["instrument_id"], order["position_effect"]
        count = order["quantity"]-order["filled_quantity"]
        current = dict(instrument_id=code, direction=order["direction"], roll_plan_id=order["roll_plan_id"],
                       margin=0, today=0, yesterday=0, opening_quantity=0)
        if effect == "open":
            own = self.reservations.get(order["order_id"], {}).get("margin", 0)
            funds = self.amounts()["available_units"]+own
            def expense(quantity):
                return self.margin(code, quantity, price=price, rule=rule)+self.fee(code, "open", "today", quantity, price, rule)
            if expense(count) > funds:
                if admission and order["funds_policy"] == "reject":
                    self.terminal(order, "insufficient_funds", "rejected")
                    return
                count = self.affordable_count(count, funds, expense)
                if not count and admission:
                    self.terminal(order, "insufficient_funds", "rejected")
                    return
            current["margin"], current["opening_quantity"] = expense(count), count
        else:
            legs = self.allocation(order, count)
            if sum(quantity for _, quantity in legs) < count and admission:
                self.terminal(order, "insufficient_close_bucket", "rejected")
                return
            for bucket, quantity in legs:
                current[bucket] = quantity
        self.reserve(order, current, "admission" if admission else "remaining")

    def submit(self, command, origin="strategy"):
        order = dict(command, origin=origin, roll_plan_id=command.get("roll_plan_id"),
                     status="accepted", filled_quantity=0, terminal_reason=None, ordinal=len(self.orders))
        oid, code = order["order_id"], order["instrument_id"]
        if oid in self.orders:
            raise EvidenceContractError("共享期货订单重复提交")
        self.orders[oid] = order
        if origin == "strategy" and (self.risk_state != "normal" or self.resume_after is not None and _time(order["event_time"]) <= self.resume_after):
            self.terminal(order, "strategy_suspended", "rejected")
        elif order["position_effect"] == "open" and (code in self.exiting or self.current_time >= _time(self.instruments[code]["exit_deadline"])):
            self.terminal(order, "contract_exiting", "rejected")
        else:
            rule = self.rule(code, order["trading_date"], self.current_time)
            self.rules[code] = rule
            self.reserve_remaining(order, rule, order["reference_price_units"], admission=True)
        return order

    def internal(self, code, direction, effect, count, decision, *, origin="strategy", tif="IOC", plan=None):
        self.ordinal += 1
        oid = f"{origin}:{self.ordinal}"
        declared = {row["order_id"] for row in self.spec["commands"]}
        while oid in self.orders or oid in declared:
            self.ordinal += 1
            oid = f"{origin}:{self.ordinal}"
        return dict(order_id=oid, instrument_id=code, direction=direction, position_effect=effect,
            quantity=count, event_time=decision.isoformat(), source_sequence=self.ordinal,
            trading_date=self.session.isoformat(), order_type="market", time_in_force=tif,
            reference_price_units=self.prices[code], limit_price_units=None, funds_policy="resize", roll_plan_id=plan)


    def risk_fact(self, action, trigger, order_id=None):
        values = self.amounts()
        self.event_row("risks", action=action, trigger=trigger, risk_order_id=order_id,
                       **values, deficit_units=max(0, -values["available_units"]))

    def risk_control(self, trigger):
        values = self.amounts()
        if values["available_units"] < 0 or values["equity_units"] < 0:
            if self.risk_state == "normal":
                self.risk_state, self.risk_since = "reducing", self.current_time
                self.risk_fact("trigger", trigger)
                for order in self.active():
                    if order["origin"] != "risk":
                        self.terminal(order, "account_risk")
                self.targets.clear()
                self.risk_fact("cancel_strategy", trigger)
            if self.amounts()["available_units"] < 0:
                self.create_risk_orders()
        if self.risk_state != "normal" and self.amounts()["available_units"] >= 0 and self.amounts()["equity_units"] >= 0:
            for order in self.active(origin="risk"):
                self.terminal(order, "risk_resolved")
            self.risk_state, self.resume_after = "normal", self.current_time
            self.risk_fact("recovered", trigger)

    def create_risk_orders(self):
        occupied = {(o["instrument_id"], o["direction"], o["position_effect"][6:]) for o in self.active(origin="risk")}
        ranked = sorted(self.positions, key=lambda key: (-self.margin(key[0], 1), key[0], key[1], key[2] != "yesterday"))
        for code, direction, bucket in ranked:
            if (code, direction, bucket) in occupied:
                continue
            probe = dict(order_id=None, instrument_id=code, direction=direction, position_effect="close_"+bucket)
            count = sum(quantity for _, quantity in self.allocation(probe, self.quantity(code, direction, bucket)))
            if count:
                command = self.internal(code, direction, "close_"+bucket, count, self.risk_since, origin="risk", tif="DAY")
                order = self.submit(command, "risk")
                self.risk_fact("submit", "insufficient_available", order["order_id"])

    def eligible(self, decision, event):
        at, start = self.current_time, _time(event["bar_start"])
        decision = _time(decision["event_time"])
        return decision < start and start == at if self.spec["frequency"] == "1d" else decision <= start and decision < at

    def allowance(self, plan_id):
        plan = self.rolls[plan_id]
        reserved = sum(row["opening_quantity"] for row in self.reservations.values() if row["roll_plan_id"] == plan_id)
        return max(0, min(plan["quantity"], plan["closed_quantity"])-plan["opened_quantity"]-reserved)

    def prepare_roll(self, event, *, opening):
        if self.risk_state != "normal":
            return
        code = event["instrument_id"]
        for plan_id, plan in self.rolls.items():
            if not self.eligible(plan, event):
                continue
            if code == plan["old_instrument_id"]:
                effect, count = "close", plan["quantity"]-plan["closed_quantity"]
            elif code == plan["new_instrument_id"]:
                effect, count = "open", self.allowance(plan_id)
            else:
                continue
            if (effect == "open") != opening:
                continue
            if count and not any(order["roll_plan_id"] == plan_id and order["position_effect"] == effect for order in self.active(code=code)):
                command = self.internal(code, plan["direction"], effect, count, _time(plan["event_time"]), tif="DAY", origin="roll", plan=plan_id)
                self.submit(command, "roll")

    def prepare_targets(self, event, *, opening):
        if self.risk_state != "normal":
            return
        code = event["instrument_id"]
        for (instrument, direction), target in self.targets.items():
            if instrument == code and self.eligible(target, event):
                delta = target["quantity"]-self.quantity(code, direction)
                if (delta > 0) != opening:
                    continue
                if delta and not (delta > 0 and code in self.exiting):
                    command = self.internal(code, direction, "open" if delta > 0 else "close", abs(delta), _time(target["event_time"]))
                    self.submit(command)

    def fillable(self, order, event, capacity):
        code, price, effect = event["instrument_id"], event["price_units"], order["position_effect"]
        rule = self.rules[code]
        buying = (order["direction"] == "long") == (effect == "open")
        if buying and event.get("limit_up_units") is not None and price >= event["limit_up_units"] or not buying and event.get("limit_down_units") is not None and price <= event["limit_down_units"]:
            return 0, "price_limit_blocked"
        limit = order["limit_price_units"]
        if limit is not None and (price > limit if buying else price < limit):
            return 0, "limit_not_crossed"
        count = min(capacity, order["quantity"]-order["filled_quantity"])
        if not count:
            return 0, "no_capacity"
        if effect == "open":
            if self.risk_state != "normal" or code in self.exiting:
                return 0, "contract_exiting" if code in self.exiting else "account_risk"
            own = self.reservations.get(order["order_id"], {})
            if order["roll_plan_id"]:
                count = min(count, self.allowance(order["roll_plan_id"])+own.get("opening_quantity", 0))
            funds = self.amounts()["available_units"]+own.get("margin", 0)
            held = self.quantity(code, order["direction"])
            before = self.margin(code, held)
            def expense(quantity):
                return self.margin(code, held+quantity)-before+self.fee(code, "open", "today", quantity, price, rule)
            if expense(count) > funds:
                if order["funds_policy"] == "reject":
                    return 0, "insufficient_funds"
                count = self.affordable_count(count, funds, expense)
            return count, None if count else "insufficient_funds"
        count = sum(quantity for _, quantity in self.allocation(order, count))
        if count and order["origin"] == "risk":
            held, available = self.quantity(code, order["direction"]), self.amounts()["available_units"]
            before = self.margin(code, held)
            def resolved(quantity):
                fee = sum(self.fee(code, effect, bucket, q, price, rule) for bucket, q in self.allocation(order, quantity))
                return available+before-self.margin(code, held-quantity)-fee >= 0
            if resolved(count):
                low, high = 1, count
                while low < high:
                    middle = (low+high)//2
                    if resolved(middle):
                        high = middle
                    else:
                        low = middle+1
                count = low
        return count, None if count else "insufficient_close_bucket"

    def book_fill(self, order, count, event):
        code, direction, effect = order["instrument_id"], order["direction"], order["position_effect"]
        price, rule = event["price_units"], self.rules[code]
        legs = [("today", count)] if effect == "open" else self.allocation(order, count)
        if sum(quantity for _, quantity in legs) != count:
            raise EvidenceContractError("共享期货平仓量超过未预占方向桶")
        facts = []
        for bucket, quantity in legs:
            key, realized = (code, direction, bucket), 0
            if effect == "open":
                old_count, old_cost = self.positions.get(key, (0, Fraction(0)))
                self.positions[key] = (old_count+quantity, (old_count*old_cost+quantity*price)/(old_count+quantity))
            else:
                old_count, old_cost = self.positions[key]
                realized = self.pnl(key, quantity, price)
                if old_count == quantity:
                    del self.positions[key]
                else:
                    self.positions[key] = (old_count-quantity, old_cost)
            fee = self.fee(code, effect, bucket, quantity, price, rule)
            self.cash += realized-fee
            facts.append(dict(position_bucket=bucket, quantity=quantity, notional_units=_round(self.notional(code, quantity, price)), fee_units=fee, realized_pnl_units=realized))
        order["filled_quantity"] += count
        order["status"] = "filled" if order["filled_quantity"] == order["quantity"] else "partially_filled"
        self.session_activity.add((code, event["trading_date"]))
        if effect == "open":
            self.settled.discard((code, event["trading_date"]))
        for fact in facts:
            fill_id = f"{order['order_id']}:{order['filled_quantity']}:{fact['position_bucket']}"
            self.event_row("fills", fill_id=fill_id, order_id=order["order_id"], instrument_id=code,
                direction=direction, position_effect=effect, execution_price_units=price,
                reference_price_units=order["reference_price_units"], price_scale=self.instruments[code]["price_scale"],
                contract_multiplier=str(self.instruments[code]["contract_multiplier"]), origin=order["origin"], **fact)
            self.event_row("costs", cost_id=fill_id+":fee", fill_id=fill_id, cost_type="fee", amount_units=fact["fee_units"])
        if order["roll_plan_id"]:
            field = "opened_quantity" if effect == "open" else "closed_quantity"
            self.rolls[order["roll_plan_id"]][field] += count
        self.reserve(order, None, "fill")
        if order["status"] not in _TERMINAL:
            self.reserve_remaining(order, rule, price)
        self.snapshot()
        if order["origin"] == "risk":
            self.risk_fact("fill", "risk_reduction", order["order_id"])
        self.risk_control("fill")

    def needs_close(self, event):
        code = event["instrument_id"]
        return (any(order["position_effect"] != "open" and self.eligible(order, event) for order in self.active(code=code))
                or any(instrument == code and target["quantity"] < self.quantity(code, direction) and self.eligible(target, event)
                       for (instrument, direction), target in self.targets.items())
                or any(plan["old_instrument_id"] == code and plan["closed_quantity"] < plan["quantity"] and self.eligible(plan, event)
                       for plan in self.rolls.values()))

    def close_priority(self, event):
        risk = any(self.eligible(order, event) for order in self.active(code=event["instrument_id"], origin="risk"))
        return (not risk, -self.margin(event["instrument_id"], 1) if risk else 0,
                event["source_sequence"], event["instrument_id"])

    def execute_bar(self, event, *, opening, capacity, marked=False):
        code = event["instrument_id"]
        if not marked:
            self.rules[code], self.prices[code] = self.rule(code, event["trading_date"], self.current_time), event["price_units"]
            if any(key[0] == code for key in self.positions):
                self.session_activity.add((code, event["trading_date"]))
            self.risk_control("visible_price")
        self.prepare_roll(event, opening=opening)
        self.prepare_targets(event, opening=opening)
        candidates = sorted(self.active(code=code), key=lambda order: (order["origin"] != "risk",
            order["position_effect"] == "close_today", _time(order["event_time"]), order["source_sequence"], order["ordinal"]))
        for order in candidates:
            if (order["position_effect"] == "open") != opening or order["status"] in _TERMINAL or not self.eligible(order, event):
                continue
            if order["origin"] == "risk" and self.risk_state == "normal":
                continue
            count, reason = self.fillable(order, event, capacity)
            if count:
                self.book_fill(order, count, event)
                capacity -= count
            if order["status"] not in _TERMINAL and order["time_in_force"] == "IOC":
                self.terminal(order, "ioc_remainder_cancelled" if count else reason or "unfilled")
            elif order["status"] in _TERMINAL and order["order_id"] in self.reservations:
                self.reserve(order, None, order["terminal_reason"] or "filled")
        self.snapshot()
        return capacity

    def execute_bars(self, events):
        remaining = {}
        for event in sorted([row for row in events if self.needs_close(row)], key=self.close_priority):
            self.market_session(event)
            key = (event["instrument_id"], event["source_sequence"])
            remaining[key] = self.execute_bar(event, opening=False, capacity=event["capacity"])
        for event in events:
            self.market_session(event)
            key = (event["instrument_id"], event["source_sequence"])
            self.execute_bar(event, opening=True, capacity=remaining.get(key, event["capacity"]), marked=key in remaining)

    def activate_roll(self, plan):
        code, direction = plan["old_instrument_id"], plan["direction"]
        eligible = sorted(((_time(fact["close_at"]), day) for (instrument, day), fact in self.session_facts.items()
                           if instrument == code and _time(fact["available_at"]) <= self.current_time
                           and _time(fact["close_at"]) >= self.current_time))
        if not eligible:
            raise EvidenceContractError("共享期货换月登记没有可见的当前或下一旧合约会话")
        self.session = date.fromisoformat(eligible[0][1])
        if self.quantity(code, direction) < plan["quantity"]:
            raise EvidenceContractError("共享期货换月量超过开始时旧合约方向仓位")
        if any(p["old_instrument_id"] == code and p["direction"] == direction for p in self.rolls.values()):
            raise EvidenceContractError("共享期货旧合约方向被重复消费换月额度")
        self.rolls[plan["roll_plan_id"]] = dict(plan, closed_quantity=0, opened_quantity=0)
        self.exiting.add(code)
        for order in self.active(code=code):
            if order["origin"] == "strategy":
                self.terminal(order, "roll_exit_started")
        self.targets = {key: row for key, row in self.targets.items() if key[0] != code}
        self.roll_fact(plan["roll_plan_id"])

    def market_session(self, event):
        code, session = event["instrument_id"], event["trading_date"]
        fact = self.session_facts.get((code, session))
        if fact is None or _time(fact["available_at"]) > self.current_time:
            raise EvidenceContractError("共享期货事件缺少当时可见的交易会话事实")
        if (code, session) in self.closed_sessions:
            raise EvidenceContractError("共享期货已结束会话不能继续 bar、结算或再次收尾")
        if event["kind"] == "bar":
            start = _time(event["bar_start"])
            if not any(_time(segment["starts_at"]) <= start <= self.current_time <= _time(segment["ends_at"])
                       for segment in fact["segments"]):
                raise EvidenceContractError("共享期货 bar 区间或夜盘归属不符合冻结会话")
        elif event["kind"] == "settlement" and _time(event["available_at"]) < _time(fact["close_at"]):
            raise EvidenceContractError("共享期货结算发布早于冻结会话收尾")
        elif event["kind"] == "session_end" and self.current_time < _time(fact["close_at"]):
            raise EvidenceContractError("共享期货 session_end 早于冻结会话结束时间")
        previous = self.sessions.get(code)
        if previous is not None and session < previous:
            raise EvidenceContractError("共享期货交易会话倒退")
        if previous is not None and session != previous and (code, previous) not in self.closed_sessions:
            if any(key[0] == code for key in self.positions) or self.active(code=code):
                raise EvidenceContractError("共享期货跨会话缺少显式 session_end")
        if event["kind"] == "bar" and (code, session) in self.closed_sessions:
            raise EvidenceContractError("共享期货已结束会话不能继续成交")
        self.sessions[code], self.session = session, date.fromisoformat(session)

    def settle(self, event):
        code, price = event["instrument_id"], event["price_units"]
        self.rules[code], self.prices[code] = self.rule(code, event["trading_date"], self.current_time), price
        if any(key[0] == code for key in self.positions):
            self.session_activity.add((code, event["trading_date"]))
        for key, (quantity, _) in tuple(self.positions.items()):
            if key[0] == code:
                self.cash += self.pnl(key, quantity, price)
                self.positions[key] = (quantity, Fraction(price))
        self.settled.add((code, event["trading_date"]))
        self.snapshot()

    def end_session(self, event):
        code, session = event["instrument_id"], event["trading_date"]
        if (code, session) in self.session_activity and (code, session) not in self.settled:
            raise EvidenceContractError("共享期货持仓会话缺少正式结算")
        for order in self.active(code=code):
            self.terminal(order, "session_end", "expired")
        for direction in ("long", "short"):
            today = self.positions.pop((code, direction, "today"), None)
            if today:
                key = (code, direction, "yesterday")
                old_quantity, old_cost = self.positions.get(key, (0, Fraction(0)))
                quantity, cost = today
                self.positions[key] = (old_quantity+quantity, (old_quantity*old_cost+quantity*cost)/(old_quantity+quantity))
        self.closed_sessions.add((code, session))
        self.snapshot()

    def deadline(self, *, inclusive=False):
        for code, _, _ in self.positions:
            at = _time(self.instruments[code]["exit_deadline"])
            if self.current_time > at or inclusive and self.current_time == at:
                raise EvidenceContractError("共享期货持仓越过合约退出截止时点")
        for plan in self.rolls.values():
            at = _time(plan["deadline"])
            if (self.current_time > at or inclusive and self.current_time == at) and plan["closed_quantity"] < plan["quantity"]:
                raise EvidenceContractError("共享期货换月旧腿未完成便越过退出截止时点")

    def run(self):
        timeline = []
        for row in self.events:
            stage = {"settlement": 0, "session_end": 1, "bar": 4}[row["kind"]]
            timeline.append((_time(row["event_time"]), stage, row["source_sequence"], row["instrument_id"], row["kind"], row))
        for row in self.spec["commands"]:
            stage = 1 if row["action"] == "cancel" else 3 if row["position_effect"] == "open" else 2
            timeline.append((_time(row["event_time"]), stage, row["source_sequence"], row["instrument_id"], "command", row))
        for row in self.spec["targets"]:
            timeline.append((_time(row["event_time"]), 2, row["source_sequence"], row["instrument_id"], "target", row))
        for row in self.spec["roll_plans"]:
            timeline.append((_time(row["event_time"]), 2, row["source_sequence"], row["old_instrument_id"], "roll", row))
        first, last = min(row[0] for row in timeline), max(_time(row["event_time"]) for row in self.events)
        for rule in self.spec["rules"]:
            at = max(_time(rule["available_at"]), _effective_time(rule))
            if first <= at <= last:
                timeline.append((at, 0, 0, rule["instrument_id"], "rule", rule))
        for at, batch in groupby(sorted(timeline, key=lambda row: row[:5]), key=lambda row: row[0]):
            self.current_time = at
            self.deadline()
            batch = list(batch)
            bar_codes = {row["instrument_id"] for _, _, _, _, kind, row in batch if kind == "bar"}
            closing_settlements = []
            session_ends = []
            for _, stage, _, _, kind, row in batch:
                if stage == 0 and kind == "settlement":
                    if row["instrument_id"] in bar_codes:
                        closing_settlements.append(row)
                    else:
                        self.market_session(row)
                        self.settle(row)
            self.refresh_rules()
            self.risk_control("visible_rule_or_settlement")
            bars = []
            for _, stage, _, _, kind, row in batch:
                if stage == 0:
                    continue
                if kind == "bar":
                    bars.append(row)
                elif kind == "session_end":
                    session_ends.append(row)
                elif kind == "command":
                    self.session = date.fromisoformat(row["trading_date"])
                    if row["action"] == "submit":
                        self.submit(row)
                    else:
                        order = self.orders.get(row["order_id"])
                        if order is None or order["instrument_id"] != row["instrument_id"] or order["origin"] != "strategy":
                            raise EvidenceContractError("共享期货策略撤单引用未知订单、错误合约或内部订单")
                        self.terminal(order, "user_cancel")
                    self.snapshot()
                elif kind == "target":
                    if self.risk_state == "normal" and (self.resume_after is None or at > self.resume_after):
                        self.targets[(row["instrument_id"], row["direction"])] = row
                elif kind == "roll":
                    self.activate_roll(row)
            self.execute_bars(bars)
            # 最后一根完整 bar 可成交，随后结算重置成本，最后收尾滚桶。
            for row in closing_settlements:
                self.market_session(row)
                self.settle(row)
            if closing_settlements:
                self.risk_control("visible_rule_or_settlement")
            for row in session_ends:
                self.market_session(row)
                self.end_session(row)
            self.deadline(inclusive=True)
        for order in self.active():
            self.terminal(order, "window_end")
        self.risk_control("window_end")
        final = self.amounts()
        if final["equity_units"] < 0 or final["available_units"] < 0 or self.risk_state != "normal":
            raise EvidenceContractError("共享期货终态负权益或资金缺口，拒绝正式结果")
        if self.session_activity-self.settled:
            raise EvidenceContractError("共享期货持仓会话缺少封存结算")
        if self.reservations or self.active():
            raise EvidenceContractError("共享期货终态仍有活动订单或冻结预占")
        self.snapshot()
        for order in self.orders.values():
            fields = {name: order[name] for name in ("order_id", "instrument_id", "direction", "position_effect", "order_type", "time_in_force", "filled_quantity", "status", "terminal_reason", "origin", "roll_plan_id")}
            self.row("orders", **fields, submitted_at=_time(order["event_time"]), decision_time=_time(order["event_time"]),
                     session=date.fromisoformat(order["trading_date"]), requested_quantity=order["quantity"])
        return self.output


def _compare_tables(actual, expected):
    """逐字段比较独立事件流；重算外层摘要不能替代这一金融复验。"""
    for name, columns in SHARED_FUTURES_COLUMNS.items():
        key_fields = SHARED_FUTURES_KEYS[name]
        def sort_key(row):
            return tuple(row[field] for field in key_fields)
        left, right = sorted(actual[name], key=sort_key), sorted(expected[name], key=sort_key)
        if len(left) != len(right):
            raise EvidenceContractError(f"共享期货 {name} 存在漏单、漏事件、漏快照或额外事实")
        for row, recomputed in zip(left, right):
            for field in columns:
                if field == "base_price":
                    # 显示字符串不作为精确成本或损益依据。
                    if not isinstance(row[field], str):
                        raise EvidenceContractError("共享期货 base_price 必须为显示文本")
                    continue
                if field in {"cost_numerator", "cost_denominator"}:
                    try:
                        value = int(row[field])
                        if not isinstance(row[field], str) or str(value) != row[field]:
                            raise ValueError("integer text")
                    except (TypeError, ValueError) as exc:
                        raise EvidenceContractError("共享期货成本必须封存规范十进制整数文本") from exc
                    if value != int(recomputed[field]):
                        raise EvidenceContractError(f"共享期货 {name}.{field} 精确成本与独立重算不一致")
                elif row[field] != recomputed[field]:
                    raise EvidenceContractError(f"共享期货 {name}.{field} 与封存输入独立重算不一致")


def verify_shared_futures(tables: Mapping, context: Mapping) -> dict:
    """从 spec、行情和命令生成完整九表，独立核对交易合法性与金融结果。"""
    if not isinstance(context, Mapping) or set(context) != {"version", "spec", "market_events"} or context.get("version") != "research-shared-futures-context-v1":
        raise EvidenceContractError("共享期货金融上下文版本或封存输入不完整")
    if set(tables) != set(SHARED_FUTURES_COLUMNS):
        raise EvidenceContractError("共享期货独立复核必须消费完整九表")
    try:
        spec = validate_shared_futures_spec(context["spec"])
        events = validate_shared_futures_events(spec, context["market_events"])
    except (TypeError, KeyError, ValueError) as exc:
        raise EvidenceContractError("共享期货封存输入违反 PIT 或规则合同") from exc
    actual = {name: _rows(table, name) for name, table in tables.items()}
    try:
        replay = _Replay(spec, events)
        expected = replay.run()
        _compare_tables(actual, expected)
    except EvidenceContractError:
        raise
    except (TypeError, KeyError, ValueError, ZeroDivisionError) as exc:
        raise EvidenceContractError("共享期货封存输入或金融事实无法独立重建") from exc
    tca = defaultdict(lambda: {"quantity": 0, "fee_units": 0, "slippage_units": 0})
    for row in expected["fills"]:
        key = (row["instrument_id"], row["direction"], row["position_bucket"], row["position_effect"])
        value = tca[key]
        value["quantity"] += row["quantity"]
        value["fee_units"] += row["fee_units"]
        buying = (row["direction"] == "long") == (row["position_effect"] == "open")
        slip = replay.notional(row["instrument_id"], row["quantity"], row["execution_price_units"]-row["reference_price_units"])
        value["slippage_units"] += _round(slip if buying else -slip)
    return {"account": replay.common, "final_amounts": replay.amounts(),
            "orders": len(expected["orders"]), "fills": len(expected["fills"]),
            "tca_by_bucket": [{"instrument_id": key[0], "direction": key[1], "position_bucket": key[2], "position_effect": key[3], **value} for key, value in sorted(tca.items())]}
