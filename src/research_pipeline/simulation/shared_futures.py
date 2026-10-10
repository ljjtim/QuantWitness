"""复用公共执行内核和 Broker 的共享期货账户事件调度。"""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from itertools import groupby
import json
from zoneinfo import ZoneInfo

from research_pipeline.domain import Price
from research_pipeline.domain.shared_futures import (
    SHARED_FUTURES_CONTEXT_VERSION, aware_time,
    validate_shared_futures_events, validate_shared_futures_spec,
)
from research_pipeline.domain.shared_futures_result import SHARED_FUTURES_COLUMNS
from research_pipeline.domain.trading import InstrumentKey
from research_pipeline.platform.canonical import typed_canonical_hash

from .engine import ExecutionEngine
from .events import ExecutionOutcome
from .orders import ORDER_TERMINAL_STATES, SimulationContractError
from .shared_futures_ledger import SharedFuturesLedger, SharedReservation
from .shared_futures_result import SharedFuturesResult, shared_futures_tables


def _time(row: dict, field: str = "event_time") -> datetime:
    return aware_time(row[field], field)


def _json_fact(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"上下文值不可序列化: {type(value).__name__}")


class _SharedFuturesExecution:
    """事件协调只持有决策和输出，金融数量由账本持有。"""

    def __init__(self, spec: dict, events: list[dict]) -> None:
        self.spec, self.events = spec, events
        self.engine = ExecutionEngine()
        self.broker = self.engine.broker
        self.ledger = SharedFuturesLedger(spec)
        self.rows = {name: [] for name in SHARED_FUTURES_COLUMNS}
        self.common = {key: spec[key] for key in ("portfolio_id", "account_id", "currency")}
        self.sequence = 0
        self.metadata: dict[str, dict] = {}
        self.targets: dict[tuple[str, str], dict] = {}
        self.sessions: dict[str, str] = {}
        self.closed_sessions: set[tuple[str, str]] = set()
        self.ordinal = 0
        self.current_time = min(_time(row) for row in events)
        self.current_session = date.fromisoformat(min(row["trading_date"] for row in events))

    def _row(self, table: str, **values) -> None:
        self.rows[table].append({**self.common, **values})

    def _event_row(self, table: str, **values) -> None:
        self.sequence += 1
        self._row(table, event_time=self.current_time, sequence=self.sequence,
                  session=self.current_session, **values)

    def _snapshot(self) -> None:
        self.sequence += 1
        amounts = self.ledger.amounts()
        self._row("cash", event_time=self.current_time, sequence=self.sequence,
                  session=self.current_session, **amounts, cash_scale=100,
                  risk_state=self.ledger.risk_state)
        self._row("valuations", event_time=self.current_time, sequence=self.sequence,
                  session=self.current_session, nav_units=amounts["equity_units"],
                  valuation_model="shared_futures_equity_v1")
        margins = self.ledger.position_margins()
        for (code, direction, bucket), position in sorted(self.ledger.positions.items()):
            scale = self.ledger.instruments[code]["price_scale"]
            basis = position.cost_price / scale
            self._row("positions", event_time=self.current_time, sequence=self.sequence,
                      session=self.current_session, instrument_id=code, direction=direction,
                      position_bucket=bucket, quantity=abs(position.contracts),
                      base_price=str(Decimal(basis.numerator) / Decimal(basis.denominator)),
                      cost_numerator=basis.numerator, cost_denominator=basis.denominator,
                      price_scale=scale, margin_units=margins[(code, direction, bucket)])

    @staticmethod
    def _effective_time(rule: dict) -> datetime:
        if rule.get("effective_at") is not None:
            return _time(rule, "effective_at")
        return datetime.combine(date.fromisoformat(rule["effective_from"]), time.min,
                                tzinfo=ZoneInfo("Asia/Shanghai"))

    def _rule(self, code: str, session: str, at: datetime) -> dict:
        candidates = [rule for rule in self.spec["rules"] if rule["instrument_id"] == code
                      and rule["effective_from"] <= session <= rule["effective_to"]
                      and _time(rule, "available_at") <= at and self._effective_time(rule) <= at]
        if not candidates:
            raise SimulationContractError(f"{code} 在 {session} 缺少当时可见的规则")
        latest = max(self._effective_time(rule) for rule in candidates)
        current = [rule for rule in candidates if self._effective_time(rule) == latest]
        if len(current) != 1:
            raise SimulationContractError(f"{code} 同刻可见规则存在歧义")
        return current[0]

    def _refresh_rules(self) -> None:
        for code in self.ledger.prices:
            session = self.sessions.get(code, self.current_session.isoformat())
            self.ledger.rules[code] = self._rule(code, session, self.current_time)

    def _active(self, *, code: str | None = None, origin: str | None = None):
        for oid, order in self.broker.orders.items():
            meta = self.metadata[oid]
            if (order.status not in ORDER_TERMINAL_STATES
                    and (code is None or meta["instrument_id"] == code)
                    and (origin is None or meta["origin"] == origin)):
                yield oid, order, meta

    def _reservation(self, oid: str, value: SharedReservation | None, reason: str) -> None:
        plan_id = self.metadata[oid].get("roll_plan_id")
        for change in self.ledger.reserve(oid, value):
            self._event_row("reservations", **change, reason=reason)
        if plan_id:
            self._roll_fact(plan_id, oid)

    def _terminal(self, oid: str, reason: str, *, action: str = "cancel") -> None:
        order = self.broker.orders[oid]
        if order.status in ORDER_TERMINAL_STATES:
            return
        if action == "reject" and order.status == "partially_filled":
            action = "cancel"
        self.broker.advance(oid, action, self.current_time, reason=reason)
        self._reservation(oid, None, reason)

    def _roll_fact(self, plan_id: str, oid: str | None = None) -> None:
        plan = self.ledger.rolls[plan_id]
        self._event_row("rolls", roll_plan_id=plan_id,
                        old_instrument_id=plan["old_instrument_id"], new_instrument_id=plan["new_instrument_id"],
                        closed_quantity=plan["closed_quantity"], opened_quantity=plan["opened_quantity"],
                        reserved_new_quantity=self.ledger.reserved_new_quantity(plan_id),
                        available_new_quantity=self.ledger.roll_allowance(plan_id), order_id=oid)

    def _risk_fact(self, action: str, trigger: str, oid: str | None = None) -> None:
        values = self.ledger.amounts()
        self._event_row("risks", action=action, trigger=trigger, risk_order_id=oid,
                        **values, deficit_units=max(0, -values["available_units"]))

    def _risk(self, trigger: str) -> None:
        amounts = self.ledger.amounts()
        if amounts["available_units"] < 0 or amounts["equity_units"] < 0:
            if self.ledger.risk_state == "normal":
                self.ledger.risk_state = "reducing"
                self.ledger.risk_triggered_at = self.current_time
                self._risk_fact("trigger", trigger)
                for oid, _, meta in tuple(self._active()):
                    if meta["origin"] != "risk":
                        self._terminal(oid, "account_risk")
                self.targets.clear()
                self._risk_fact("cancel_strategy", trigger)
            if self.ledger.amounts()["available_units"] < 0:
                self._ensure_risk_orders()
        if (self.ledger.risk_state != "normal"
                and self.ledger.amounts()["available_units"] >= 0
                and self.ledger.amounts()["equity_units"] >= 0):
            for oid, _, _ in tuple(self._active(origin="risk")):
                self._terminal(oid, "risk_resolved")
            self.ledger.risk_state = "normal"
            self.ledger.resume_after = self.current_time
            self._risk_fact("recovered", trigger)

    def _ensure_risk_orders(self) -> None:
        existing = {(meta["instrument_id"], meta["direction"], meta["position_effect"].removeprefix("close_"))
                    for _, _, meta in self._active(origin="risk")}
        candidates = sorted(self.ledger.positions, key=lambda key: (
            -self.ledger.margin(key[0], 1), key[0], key[1], key[2] != "yesterday"))
        for code, direction, bucket in candidates:
            if (code, direction, bucket) in existing:
                continue
            rule = self.ledger.rules[code]
            count = sum(q for _, q in self.ledger.close_allocation(code, direction, "close_" + bucket,
                        self.ledger.quantity(code, direction, bucket), rule))
            if not count:
                continue
            command = self._internal(code, direction, "close_" + bucket, count,
                                     self.ledger.risk_triggered_at, tif="DAY", origin="risk")
            self._submit(command, origin="risk")
            self._risk_fact("submit", "insufficient_available", command["order_id"])

    def _internal(self, code: str, direction: str, effect: str, quantity: int, decision: datetime,
                  *, tif: str = "IOC", origin: str = "strategy", plan_id: str | None = None) -> dict:
        self.ordinal += 1
        oid = f"{origin}:{self.ordinal}"
        while oid in self.broker.orders or any(row["order_id"] == oid for row in self.spec["commands"]):
            self.ordinal += 1
            oid = f"{origin}:{self.ordinal}"
        return dict(command_id=oid, order_id=oid,
                    instrument_id=code, direction=direction, position_effect=effect, quantity=quantity,
                    event_time=decision.isoformat(), available_at=decision.isoformat(),
                    source_sequence=self.ordinal, trading_date=self.current_session.isoformat(),
                    order_type="market", time_in_force=tif, limit_price_units=None,
                    reference_price_units=self.ledger.prices[code], reference_available_at=self.current_time.isoformat(),
                    funds_policy="resize", roll_plan_id=plan_id)

    def _submit(self, command: dict, *, origin: str = "strategy") -> str:
        code, oid = command["instrument_id"], command["order_id"]
        instrument = self.ledger.instruments[code]
        key = InstrumentKey(code, "cn_future", instrument["exchange"], "CNY", "future_contract")
        opening = command["position_effect"] == "open"
        buying = (command["direction"] == "long") == opening
        limit = command["limit_price_units"]
        limit_value = None
        if limit is not None:
            quote = self.ledger.quote(code, limit)
            limit_value = Price.from_decimal(quote, scale=max(0, -quote.as_tuple().exponent), currency="CNY")
        self.metadata[oid] = {**command, "origin": origin, "roll_plan_id": command.get("roll_plan_id"),
                              "decision_time": _time(command), "ordinal": len(self.metadata)}
        self.broker.submit_fields(
            order_id=oid, instrument=key, side="buy" if buying else "sell",
            quantity=command["quantity"], order_type=command["order_type"],
            time_in_force=command["time_in_force"], submitted_at=_time(command),
            session=date.fromisoformat(command["trading_date"]),
            position_effect=command["position_effect"], limit_price=limit_value,
            intent_hash=typed_canonical_hash(command),
        )
        if origin == "strategy" and (self.ledger.risk_state != "normal" or (
                self.ledger.resume_after is not None and _time(command) <= self.ledger.resume_after)):
            self._terminal(oid, "strategy_suspended", action="reject")
            return oid
        if opening and (code in self.ledger.exiting or self.current_time >= _time(instrument, "exit_deadline")):
            self._terminal(oid, "contract_exiting", action="reject")
            return oid
        rule = self._rule(code, command["trading_date"], self.current_time)
        self.broker.advance(oid, "accept", self.current_time)
        self._reserve_remaining(oid, rule, command["reference_price_units"], admission=True)
        return oid

    def _reserve_remaining(self, oid: str, rule: dict, price: int, *, admission: bool = False) -> None:
        order, meta = self.broker.orders[oid], self.metadata[oid]
        if order.status in ORDER_TERMINAL_STATES:
            return
        count = order.quantity - order.filled_quantity
        code, direction, effect = meta["instrument_id"], meta["direction"], meta["position_effect"]
        if effect == "open":
            needed = self.ledger.margin(code, count, rule, price) + self.ledger.fee(code, effect, "today", count, price, rule)
            previous = self.ledger.reservations.get(oid)
            available = self.ledger.amounts()["available_units"] + (previous.cash_units if previous else 0)
            if needed > available:
                if admission and meta["funds_policy"] == "reject":
                    self._terminal(oid, "insufficient_funds", action="reject")
                    return
                low, high = 0, count
                while low < high:
                    middle = (low + high + 1) // 2
                    expense = self.ledger.margin(code, middle, rule, price) + self.ledger.fee(code, effect, "today", middle, price, rule)
                    if expense <= available:
                        low = middle
                    else:
                        high = middle - 1
                count = low
                if not count and admission:
                    self._terminal(oid, "insufficient_funds", action="reject")
                    return
                needed = self.ledger.margin(code, count, rule, price) + self.ledger.fee(code, effect, "today", count, price, rule)
            value = SharedReservation(code, direction, cash_units=needed,
                                      roll_plan_id=meta["roll_plan_id"], opening_quantity=count)
        else:
            allocations = self.ledger.close_allocation(code, direction, effect, count, rule, order_id=oid)
            if sum(q for _, q in allocations) < count and admission:
                self._terminal(oid, "insufficient_close_bucket", action="reject")
                return
            value = SharedReservation(code, direction, today=sum(q for bucket, q in allocations if bucket == "today"),
                                      yesterday=sum(q for bucket, q in allocations if bucket == "yesterday"),
                                      roll_plan_id=meta["roll_plan_id"])
        self._reservation(oid, value, "admission" if admission else "remaining")

    def _eligible(self, meta: dict, bar: dict) -> bool:
        decision, start, at = _time(meta), _time(bar, "bar_start"), _time(bar)
        if self.spec["frequency"] == "1d":
            return decision < start and start == at
        return decision <= start and decision < at

    def _fillable(self, oid: str, bar: dict, rule: dict, capacity: int) -> tuple[int, str | None]:
        order, meta = self.broker.orders[oid], self.metadata[oid]
        price, code = bar["price_units"], meta["instrument_id"]
        buying = (meta["direction"] == "long") == (meta["position_effect"] == "open")
        if ((buying and bar.get("limit_up_units") is not None and price >= bar["limit_up_units"])
                or (not buying and bar.get("limit_down_units") is not None and price <= bar["limit_down_units"])):
            return 0, "price_limit_blocked"
        if meta["limit_price_units"] is not None and (
                (buying and price > meta["limit_price_units"]) or (not buying and price < meta["limit_price_units"])):
            return 0, "limit_not_crossed"
        count = min(capacity, order.quantity - order.filled_quantity)
        if not count:
            return 0, "no_capacity"
        if meta["position_effect"] == "open":
            expired = self.current_time >= _time(self.ledger.instruments[code], "exit_deadline")
            if self.ledger.risk_state != "normal" or code in self.ledger.exiting or expired:
                return 0, "contract_exiting" if code in self.ledger.exiting or expired else "account_risk"
            if meta["roll_plan_id"]:
                reservation = self.ledger.reservations.get(oid)
                own = reservation.opening_quantity if reservation else 0
                count = min(count, self.ledger.roll_allowance(meta["roll_plan_id"]) + own)
            reservation = self.ledger.reservations.get(oid)
            available = self.ledger.amounts()["available_units"] + (reservation.cash_units if reservation else 0)
            held = self.ledger.quantity(code, meta["direction"])
            before = self.ledger.margin(code, held, rule)
            def permitted(q):
                fee = self.ledger.fee(code, "open", "today", q, price, rule)
                return self.ledger.margin(code, held + q, rule) - before + fee <= available
            if not permitted(count):
                if meta["funds_policy"] == "reject":
                    return 0, "insufficient_funds"
                low, high = 0, count
                while low < high:
                    middle = (low + high + 1) // 2
                    if permitted(middle):
                        low = middle
                    else:
                        high = middle - 1
                count = low
            return count, None if count else "insufficient_funds"
        allocations = self.ledger.close_allocation(code, meta["direction"], meta["position_effect"], count, rule, order_id=oid)
        count = sum(q for _, q in allocations)
        if meta["origin"] == "risk" and count:
            held = self.ledger.quantity(code, meta["direction"])
            before = self.ledger.margin(code, held, rule)
            available = self.ledger.amounts()["available_units"]
            def resolved(q):
                legs = self.ledger.close_allocation(code, meta["direction"], meta["position_effect"], q, rule, order_id=oid)
                fee = sum(self.ledger.fee(code, meta["position_effect"], bucket, n, price, rule) for bucket, n in legs)
                return available + before - self.ledger.margin(code, held - q, rule) - fee >= 0
            if resolved(count):
                low, high = 1, count
                while low < high:
                    middle = (low + high) // 2
                    if resolved(middle):
                        high = middle
                    else:
                        low = middle + 1
                count = low
        return count, None if count else "insufficient_close_bucket"

    def _execute(self, oid: str, bar: dict, capacity: int) -> int:
        meta = self.metadata[oid]
        code = meta["instrument_id"]
        rule = self.ledger.rules[code]
        count, reason = self._fillable(oid, bar, rule, capacity)
        def confirm():
            legs = self.ledger.fill(code, meta["direction"], meta["position_effect"], count,
                                    bar["price_units"], rule, order_id=oid) if count else []
            return ExecutionOutcome(filled_quantity=count, reason=reason, value=legs)
        def record_fill(legs):
            self.ledger.session_activity.add((code, bar["trading_date"]))
            if meta["position_effect"] == "open":
                self.ledger.settled_sessions.discard((code, bar["trading_date"]))
            for leg in legs:
                fill_id = f"{oid}:{self.broker.orders[oid].filled_quantity}:{leg['position_bucket']}"
                self._event_row("fills", fill_id=fill_id, order_id=oid, instrument_id=code,
                                direction=meta["direction"], position_effect=meta["position_effect"],
                                execution_price_units=bar["price_units"], reference_price_units=meta["reference_price_units"],
                                price_scale=self.ledger.instruments[code]["price_scale"],
                                contract_multiplier=str(self.ledger.instruments[code]["contract_multiplier"]),
                                origin=meta["origin"], **leg)
                self._event_row("costs", cost_id=fill_id + ":fee", fill_id=fill_id,
                                cost_type="fee", amount_units=leg["fee_units"])
            plan_id = meta["roll_plan_id"]
            if plan_id:
                field = "opened_quantity" if meta["position_effect"] == "open" else "closed_quantity"
                self.ledger.rolls[plan_id][field] += count
            self._reservation(oid, None, "fill")
            if self.broker.orders[oid].status not in ORDER_TERMINAL_STATES:
                self._reserve_remaining(oid, rule, bar["price_units"])
            self._snapshot()
            if meta["origin"] == "risk":
                self._risk_fact("fill", "risk_reduction", oid)
            self._risk("fill")
        self.engine.execute_active_order(oid, event_time=self.current_time, execute=confirm, after_fill=record_fill)
        if self.broker.orders[oid].status in ORDER_TERMINAL_STATES and oid in self.ledger.reservations:
            self._reservation(oid, None, self.broker.orders[oid].rejection_code or "filled")
        return count

    def _activate_roll(self, plan: dict) -> None:
        code = plan["old_instrument_id"]
        sessions = [row for row in self.spec["sessions"] if row["instrument_id"] == code
                    and _time(row, "available_at") <= self.current_time <= _time(row, "close_at")]
        if not sessions:
            raise SimulationContractError("换月登记缺少当时已知的旧合约当前或下一交易会话")
        # 休市登记归下一已声明会话；夜盘不沿用上一条行情的交易日。
        self.current_session = date.fromisoformat(min(sessions, key=lambda row: _time(row, "close_at"))["trading_date"])
        if self.ledger.quantity(code, plan["direction"]) < plan["quantity"]:
            raise SimulationContractError("换月计划量超过开始时旧合约方向持仓")
        if any(item["old_instrument_id"] == code and item["direction"] == plan["direction"]
               for item in self.ledger.rolls.values()):
            raise SimulationContractError("同一旧合约方向不能重复消费换月额度")
        self.ledger.rolls[plan["roll_plan_id"]] = {**plan, "closed_quantity": 0, "opened_quantity": 0}
        self.ledger.exiting.add(code)
        for oid, _, meta in tuple(self._active(code=code)):
            if meta["origin"] == "strategy":
                self._terminal(oid, "roll_exit_started")
        for key in tuple(self.targets):
            if key[0] == code:
                del self.targets[key]
        self._roll_fact(plan["roll_plan_id"])

    def _prepare_roll(self, bar: dict, *, opening: bool | None = None) -> None:
        if self.ledger.risk_state != "normal":
            return
        code = bar["instrument_id"]
        for plan_id, plan in self.ledger.rolls.items():
            if not self._eligible(plan, bar):
                continue
            if code == plan["old_instrument_id"]:
                effect = "close"
                count = plan["quantity"] - plan["closed_quantity"]
            elif code == plan["new_instrument_id"]:
                effect = "open"
                count = self.ledger.roll_allowance(plan_id)
            else:
                continue
            if opening is not None and (effect == "open") != opening:
                continue
            if any(meta["roll_plan_id"] == plan_id and meta["position_effect"] == effect
                   for _, _, meta in self._active(code=code)):
                continue
            if count:
                command = self._internal(code, plan["direction"], effect, count, _time(plan),
                                         tif="DAY", origin="roll", plan_id=plan_id)
                self._submit(command, origin="roll")

    def _prepare_targets(self, bar: dict, *, opening: bool | None = None) -> None:
        if self.ledger.risk_state != "normal":
            return
        code = bar["instrument_id"]
        for (instrument, direction), target in tuple(self.targets.items()):
            if instrument != code or not self._eligible(target, bar):
                continue
            held = self.ledger.quantity(code, direction)
            delta = target["quantity"] - held
            if opening is not None and (delta > 0) != opening:
                continue
            if delta and not (delta > 0 and code in self.ledger.exiting):
                command = self._internal(code, direction, "open" if delta > 0 else "close", abs(delta), _time(target))
                self._submit(command)

    def _market_session(self, row: dict) -> None:
        code, session = row["instrument_id"], row["trading_date"]
        previous = self.sessions.get(code)
        if previous is not None and session < previous:
            raise SimulationContractError("共享期货 trading_date 倒退")
        if previous is not None and session != previous and (code, previous) not in self.closed_sessions:
            if any(key[0] == code for key in self.ledger.positions) or any(self._active(code=code)):
                raise SimulationContractError("跨会话缺少显式 session_end")
        if (code, session) in self.closed_sessions:
            raise SimulationContractError("已经结束的合约会话不能继续成交、结算或再次收尾")
        self.sessions[code] = session
        self.current_session = date.fromisoformat(session)

    def _needs_close(self, bar: dict) -> bool:
        code = bar["instrument_id"]
        if any(meta["position_effect"] != "open" and self._eligible(meta, bar)
               for _, _, meta in self._active(code=code)):
            return True
        if any(instrument == code and target["quantity"] < self.ledger.quantity(code, direction)
               and self._eligible(target, bar)
               for (instrument, direction), target in self.targets.items()):
            return True
        return any(plan["old_instrument_id"] == code and plan["closed_quantity"] < plan["quantity"]
                   and self._eligible(plan, bar) for plan in self.ledger.rolls.values())

    def _close_priority(self, bar: dict) -> tuple:
        risk = [(oid, order, meta) for oid, order, meta in self._active(code=bar["instrument_id"], origin="risk")
                if self._eligible(meta, bar)]
        release = self.ledger.margin(bar["instrument_id"], 1) if risk else 0
        return (not bool(risk), -release, bar["source_sequence"], bar["instrument_id"])

    def _bar(self, bar: dict, *, opening: bool, capacity: int,
             marked: bool = False) -> int:
        code = bar["instrument_id"]
        if not marked:
            rule = self._rule(code, bar["trading_date"], self.current_time)
            self.ledger.mark(code, bar["price_units"], rule)
            if any(key[0] == code for key in self.ledger.positions):
                self.ledger.session_activity.add((code, bar["trading_date"]))
            self._risk("visible_price")
        self._prepare_roll(bar, opening=opening)
        self._prepare_targets(bar, opening=opening)
        active = sorted(tuple(self._active(code=code)), key=lambda item: (
            item[2]["origin"] != "risk",
            item[2]["origin"] == "risk" and item[2].get("position_effect") == "close_today",
            _time(item[2]), item[2]["source_sequence"], item[2]["ordinal"]))
        for oid, _, meta in active:
            if ((meta["position_effect"] == "open") != opening
                    or self.broker.orders[oid].status in ORDER_TERMINAL_STATES or not self._eligible(meta, bar)):
                continue
            if meta["origin"] == "risk" and self.ledger.risk_state == "normal":
                continue
            capacity -= self._execute(oid, bar, capacity)
        self._snapshot()
        return capacity

    def _bars(self, bars: list[dict]) -> None:
        """共享账户先平后开；只有已经处理的合约报价进入购买力。"""
        closed = {}
        for bar in sorted((row for row in bars if self._needs_close(row)), key=self._close_priority):
            self._market_session(bar)
            key = (bar["instrument_id"], bar["source_sequence"])
            closed[key] = self._bar(bar, opening=False, capacity=bar["capacity"])
        for bar in bars:
            self._market_session(bar)
            key = (bar["instrument_id"], bar["source_sequence"])
            self._bar(bar, opening=True, capacity=closed.get(key, bar["capacity"]), marked=key in closed)

    def _settlement(self, row: dict) -> None:
        code, session = row["instrument_id"], row["trading_date"]
        rule = self._rule(code, session, self.current_time)
        if any(key[0] == code for key in self.ledger.positions):
            self.ledger.session_activity.add((code, session))
        self.ledger.settle(code, row["price_units"], rule)
        self.ledger.settled_sessions.add((code, session))
        self._snapshot()

    def _session_end(self, row: dict) -> None:
        code, session = row["instrument_id"], row["trading_date"]
        if (code, session) in self.ledger.session_activity and (code, session) not in self.ledger.settled_sessions:
            raise SimulationContractError(f"{code} 非零持仓会话缺少可见结算")
        for oid, _, _ in tuple(self._active(code=code)):
            self._terminal(oid, "session_end", action="expire")
        self.ledger.roll_session(code)
        self.closed_sessions.add((code, session))
        self._snapshot()

    def _deadline(self, *, inclusive: bool = False) -> None:
        for code, _, _ in self.ledger.positions:
            at = _time(self.ledger.instruments[code], "exit_deadline")
            if self.current_time > at or (inclusive and self.current_time == at):
                raise SimulationContractError(f"{code} 持仓越过退出 deadline，禁止发布")
        for plan in self.ledger.rolls.values():
            at = _time(plan, "deadline")
            if (self.current_time > at or (inclusive and self.current_time == at)) and plan["closed_quantity"] < plan["quantity"]:
                raise SimulationContractError("换月旧腿未完成便越过退出 deadline")

    def run(self) -> SharedFuturesResult:
        timeline = []
        for row in self.events:
            phase = {"settlement": 0, "session_end": 1, "bar": 4}[row["kind"]]
            timeline.append((_time(row), phase, row["source_sequence"], row["instrument_id"], row["kind"], row))
        for row in self.spec["commands"]:
            phase = 1 if row["action"] == "cancel" else 3 if row["position_effect"] == "open" else 2
            timeline.append((_time(row), phase, row["source_sequence"], row["instrument_id"], "command", row))
        for row in self.spec["targets"]:
            timeline.append((_time(row), 2, row["source_sequence"], row["instrument_id"], "target", row))
        for row in self.spec["roll_plans"]:
            timeline.append((_time(row), 2, row["source_sequence"], row["old_instrument_id"], "roll", row))
        first, last = min(item[0] for item in timeline), max(_time(row) for row in self.events)
        for rule in self.spec["rules"]:
            at = max(_time(rule, "available_at"), self._effective_time(rule))
            if first <= at <= last:
                timeline.append((at, 0, 0, rule["instrument_id"], "rule", rule))
        timeline.sort(key=lambda item: item[:5])
        for at, group in groupby(timeline, key=lambda item: item[0]):
            self.current_time = at
            self._deadline()
            batch = list(group)
            bar_codes = {row["instrument_id"] for _, _, _, _, kind, row in batch if kind == "bar"}
            closing_settlements = []
            session_ends = []
            for _, phase, _, _, kind, row in batch:
                if phase != 0:
                    continue
                if kind == "settlement":
                    if row["instrument_id"] in bar_codes:
                        closing_settlements.append(row)
                    else:
                        self._market_session(row)
                        self._settlement(row)
            self._refresh_rules()
            self._risk("visible_rule_or_settlement")
            bars = []
            for _, phase, _, _, kind, row in batch:
                if phase == 0:
                    continue
                if kind == "bar":
                    bars.append(row)
                elif kind == "session_end":
                    session_ends.append(row)
                elif kind == "command":
                    self.current_session = date.fromisoformat(row["trading_date"])
                    if row["action"] == "submit":
                        self._submit(row)
                    else:
                        oid = row["order_id"]
                        if oid not in self.broker.orders or self.metadata[oid]["instrument_id"] != row["instrument_id"]:
                            raise SimulationContractError("取消引用未知订单或错误合约")
                        if self.metadata[oid]["origin"] != "strategy":
                            raise SimulationContractError("策略不能取消内部风险或换月订单")
                        self._terminal(oid, "user_cancel")
                    self._snapshot()
                elif kind == "target":
                    if self.ledger.risk_state == "normal" and (
                            self.ledger.resume_after is None or _time(row) > self.ledger.resume_after):
                        self.targets[(row["instrument_id"], row["direction"])] = row
                elif kind == "roll":
                    self._activate_roll(row)
            self._bars(bars)
            for row in closing_settlements:
                self._market_session(row)
                self._settlement(row)
            if closing_settlements:
                self._risk("visible_rule_or_settlement")
            for row in session_ends:
                self._market_session(row)
                self._session_end(row)
            self._deadline(inclusive=True)
        for oid, _, _ in tuple(self._active()):
            self._terminal(oid, "window_end")
        self._risk("window_end")
        values = self.ledger.amounts()
        if values["equity_units"] < 0 or values["available_units"] < 0 or self.ledger.risk_state != "normal":
            raise SimulationContractError("共享期货终态仍有资金缺口，禁止发布")
        missing = self.ledger.session_activity - self.ledger.settled_sessions
        if missing:
            raise SimulationContractError(f"非零持仓会话缺少可见结算: {sorted(missing)}")
        if self.ledger.reservations or any(self._active()):
            raise SimulationContractError("共享期货终态仍有活动订单或预占")
        self._snapshot()
        for oid, order in self.broker.orders.items():
            meta = self.metadata[oid]
            self._row("orders", order_id=oid, instrument_id=meta["instrument_id"], direction=meta["direction"],
                      position_effect=meta["position_effect"], order_type=order.order_type,
                      time_in_force=order.time_in_force, submitted_at=order.submitted_at,
                      decision_time=meta["decision_time"], session=date.fromisoformat(meta["trading_date"]),
                      requested_quantity=order.quantity, filled_quantity=order.filled_quantity, status=order.status,
                      terminal_reason=order.rejection_code, origin=meta["origin"], roll_plan_id=meta["roll_plan_id"])
        self.engine.finished = True
        context = json.loads(json.dumps({"version": SHARED_FUTURES_CONTEXT_VERSION,
                            "spec": self.spec, "market_events": self.events}, default=_json_fact))
        return SharedFuturesResult(shared_futures_tables(self.rows), context)


def run_shared_futures_simulation(spec: dict, market_events) -> SharedFuturesResult:
    """只消费显式事实，不连接数据库；共享金融状态与旧独立账户分开。"""
    normalized = validate_shared_futures_spec(spec)
    records = [normalized, *normalized["instruments"], *normalized["rules"],
               *normalized["commands"], *normalized["targets"], *normalized["roll_plans"]]
    if any(command.get("origin", "strategy") != "strategy" or command.get("source") == "risk"
           for command in normalized["commands"]):
        raise SimulationContractError("策略命令不能指定内部风险来源")
    if any(any("slippage" in field for field in record) for record in records):
        raise SimulationContractError("共享期货合同未声明滑点字段")
    events = validate_shared_futures_events(normalized, market_events)
    if any(any("slippage" in field for field in event) for event in events):
        raise SimulationContractError("共享期货合同未声明滑点字段")
    return _SharedFuturesExecution(normalized, events).run()
