"""从原始分钟规则、行情和会话独立复核单品种期货显式订单。"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from fractions import Fraction
import json

from research_pipeline.platform import typed_canonical_hash
from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer

_EXECUTION_RULES = (
    "actual_contract_mapping", "contract_lifecycle", "contract_multiplier",
    "delivery_expiry", "fee_schedule", "margin", "price_limit", "price_tick", "session",
)
_SETTLEMENT_RULES = ("contract_multiplier", "margin", "price_tick", "session", "settlement")


def _fail(message):
    raise EvidenceContractError("显式期货订单：" + message)


def _same(actual, expected, label):
    if actual != expected:
        _fail(f"{label} 不一致")


def _dt(value):
    return aware_datetime(value, "期货事实时点")


def _day(value):
    return date_value(value, "期货交易日")


def _ceil(value):
    return (value + 999_999) // 1_000_000


def _select(bundle, code, session, at, sections):
    parameters, bindings, snapshots = {}, [], []
    sources = {row["source_id"]: typed_canonical_hash(row) for row in bundle["sources"]}
    for section in sections:
        rule_id = f"rule.cn_futures.{section}.v1"
        candidates = [row for row in bundle["rules"] if row["instrument_id"] == code
            and row["rule_id"] == rule_id and _day(row["effective_from"]) <= session <= _day(row["effective_to"])
            and row["available_at"] is not None and _dt(row["available_at"]) <= at]
        if not candidates:
            _fail(f"当时不可见或缺失规则 {rule_id}")
        if len({(row["effective_from"], row["effective_to"]) for row in candidates}) != 1:
            _fail(f"规则有效区间重叠 {rule_id}")
        revision = max(row["revision"] for row in candidates)
        selected = [row for row in candidates if row["revision"] == revision]
        if len(selected) != 1 or selected[0]["status"] != "supported":
            _fail(f"规则修订不唯一或不支持 {rule_id}")
        row = selected[0]
        snapshot = typed_canonical_hash(row)
        try:
            source_hashes = {key: sources[key] for key in row["source_ids"]}
        except KeyError:
            _fail("规则来源缺失")
        bindings.append(typed_canonical_hash({"snapshot_hash": snapshot, "source_hashes": source_hashes}))
        snapshots.append(snapshot)
        for key, value in row["parameters"].items():
            if key in parameters and parameters[key] != value:
                _fail(f"规则参数冲突 {key}")
            parameters[key] = value
    identity = typed_canonical_hash({"bundle_hash": bundle["bundle_hash"], "bindings": bindings})
    return identity, parameters, snapshots


def _validate_parameters(p, code, session, at):
    _same(p.get("actual_contract_id"), code, "真实合约映射")
    if not _day(p["listed_date"]) <= session <= _day(p["last_trade_date"]):
        _fail("订单超出合约上市或最后交易日")
    for key in ("price_scale", "contract_unit_kg", "price_tick_units", "speculative_initial_margin_ppm"):
        integer(p.get(key), key, minimum=0 if key == "price_scale" else 1)
    if p["price_scale"] != 2:
        _fail("当前分钟期货只支持分价格精度")
    for key in ("open_fee_ppm", "close_fee_ppm", "close_today_fee_ppm"):
        integer(p.get(key), key, minimum=0)
    _same(p.get("fee_unit"), "notional_permyriad", "费用单位")
    _same(p["close_fee_ppm"], p["close_today_fee_ppm"], "当前范围的平今费率")
    _same(p.get("margin_account_role"), "speculative", "保证金账户角色")
    if _dt(p["reference_price_available_at"]) > at:
        _fail("价格限制参考含未来信息")
    previous, ratio = p["reference_previous_settlement_units"], p["price_limit_ratio_ppm"]
    tick = p["price_tick_units"]
    for key, sign in (("low_limit_units", -1), ("high_limit_units", 1)):
        exact = Fraction(previous * (1_000_000 + sign * ratio), 1_000_000 * tick)
        rounded = (exact.numerator * 2 + exact.denominator) // (2 * exact.denominator) * tick
        _same(p[key], rounded, "期货涨跌停价")


def _price(command, reference, p):
    direction = 1 if command["side"] == "buy" else -1
    tick = p["price_tick_units"]
    value = Decimal(reference) * (1 + direction * Decimal(str(command["slippage_bps"])) / 10000)
    value += direction * command["slippage_ticks"] * tick
    units = int((value / tick).to_integral_value(rounding=ROUND_CEILING if direction == 1 else ROUND_FLOOR)) * tick
    if units <= 0:
        _fail("滑点后价格非正")
    return units


class _Replay:
    def __init__(self, context, canonical, bundle, bars, settlements, session_bundle, participation):
        self.context, self.canonical, self.bundle = context, canonical, bundle
        self.bars, self.settlements, self.session_bundle = bars, settlements, session_bundle
        self.participation = participation
        self.equity = integer(context.get("initial_cash_units"), "初始现金", minimum=0)
        self.initial = self.equity
        self.contracts = self.today = self.margin = 0
        self.basis, self.remainder = Fraction(0), Fraction(0)
        self.reservations, self.orders = {}, {}
        self.events = list(context["events"])
        self.cursor = self.command_cursor = 0
        self.fills, self.cancel_results, self.command_rules = [], [], []
        self.event_ids = set()
        self.commands = self._commands(context["commands"])
        instruments = {typed_canonical_hash(row["instrument"]): row["instrument"] for row in self.commands}
        if len(instruments) != 1:
            _fail("当前范围要求恰好一个真实合约")
        self.instrument_hash, self.instrument = next(iter(instruments.items()))
        self.code = self.instrument["instrument_id"]

    def _commands(self, rows):
        commands, ids, submits, previous = [], set(), set(), None
        for row in rows:
            row = dict(row)
            clock = _dt(row["submitted_at"]), integer(row["source_sequence"], "来源顺序", minimum=0)
            if previous is not None and clock < previous:
                _fail("命令来源顺序倒置")
            previous = clock
            if max(_dt(row["available_at"]), _dt(row["decision_time"])) > clock[0]:
                _fail("命令包含未来信息")
            if row["command_id"] in ids:
                _fail("command_id 重复")
            ids.add(row["command_id"])
            if not row.get("source_hashes") or list(row["source_hashes"]) != sorted(set(row["source_hashes"])):
                _fail("命令缺少有序来源")
            instrument = row["instrument"]
            if instrument.get("asset_class") != "cn_future" or instrument.get("contract_kind") not in {"future", "future_contract"}:
                _fail("只支持真实期货合约")
            _same(instrument.get("currency"), "CNY", "合约币种")
            if row["action"] == "submit":
                if row["order_id"] in submits:
                    _fail("提交 order_id 重复")
                submits.add(row["order_id"])
                integer(row["quantity"], "委托手数", minimum=1)
                if (row["side"] not in {"buy", "sell"} or row["position_effect"] not in {"open", "close", "close_today", "close_yesterday"}
                    or row["order_type"] not in {"market", "limit"} or row["time_in_force"] not in {"IOC", "DAY"}
                    or row["funds_policy"] not in {"reject", "resize"}):
                    _fail("订单交易字段无效")
                for key in ("reference_price", "limit_price"):
                    price = row.get(key)
                    if price is not None:
                        _same((price["scale"], price["currency"]), (2, "CNY"), "订单价格精度及币种")
                        integer(price["units"], "订单价格", minimum=1)
                if row["reference_price"] is None or _dt(row["reference_price_available_at"]) > clock[0]:
                    _fail("提交价格缺失或含未来信息")
                if (row["order_type"] == "limit") != (row["limit_price"] is not None):
                    _fail("订单限价声明无效")
            elif row["action"] != "cancel":
                _fail("未知命令")
            commands.append(row)
        return commands

    def event(self, kind, at, session, rule_hash, order_id, payload, fill_id=None):
        if self.cursor >= len(self.events):
            _fail(f"缺少 {kind} 正式事件")
        row = self.events[self.cursor]
        self.cursor += 1
        if row["event_id"] in self.event_ids:
            _fail("事件身份重复")
        self.event_ids.add(row["event_id"])
        expected = {"kind": kind, "effective_time": at.isoformat(), "session": session.isoformat(),
                    "group_id": "minute-default-cn-futures", "rule_hash": rule_hash,
                    "parent_id": order_id, "payload": sorted({"order_id": order_id, **payload}.items())}
        for key, value in expected.items():
            actual = row[key]
            if key == "payload":
                actual = [tuple(item) for item in actual]
            _same(actual, value, f"{kind} 事件 {key}")
        if fill_id is not None:
            _same(row["event_id"], fill_id, "fill 唯一身份")
        return typed_canonical_hash(row)

    def available(self, order_id, effect):
        total = self.today if effect == "close_today" else abs(self.contracts) - self.today
        return total - sum(value.get(effect, 0) for key, value in self.reservations.items() if key != order_id)

    def power(self, order_id):
        return self.equity - self.margin - sum(value.get("cash", 0) + value.get("margin", 0)
                                               for key, value in self.reservations.items() if key != order_id)

    def legs(self, command, quantity):
        if command["position_effect"] != "close":
            return [(command["position_effect"], quantity)]
        yesterday = min(quantity, self.available(command["order_id"], "close_yesterday"))
        return [(effect, amount) for effect, amount in (("close_yesterday", yesterday), ("close_today", quantity-yesterday)) if amount]

    def reserve(self, command, quantity, reference, at, session, rule_hash, p):
        order_id = command["order_id"]
        sign = 1 if command["side"] == "buy" else -1
        opening = command["position_effect"] == "open"
        if self.contracts and ((opening and self.contracts*sign < 0) or (not opening and self.contracts*sign > 0)):
            return False
        price = command["limit_price"]["units"] if command["limit_price"] else _price(command, reference, p)
        notional = price * quantity * p["contract_unit_kg"]
        fee = _ceil(notional * p["open_fee_ppm" if opening else "close_fee_ppm"])
        margin = _ceil(notional * p["speculative_initial_margin_ppm"]) if opening else 0
        available = self.power(order_id)
        if opening and command["funds_policy"] == "resize":
            fee, margin = min(fee, available), min(margin, max(0, available-min(fee, available)))
        if fee + margin > available:
            return False
        legs = [] if opening else self.legs(command, quantity)
        if any(amount > self.available(order_id, effect) for effect, amount in legs):
            return False
        reservation = dict(self.reservations.get(order_id, {}))
        for effect, amount in legs:
            reservation[effect] = amount
            self.event("position_reserved", at, session, rule_hash, order_id,
                       {"instrument_hash": self.instrument_hash, "position_effect": effect, "quantity": amount})
        reservation.update(cash=fee, margin=margin)
        self.event("cash_reserved", at, session, rule_hash, order_id, {"cash_units": fee, "margin_units": margin})
        self.store(order_id, reservation)
        return True

    def store(self, order_id, reservation):
        if any(reservation.values()):
            self.reservations[order_id] = reservation
        else:
            self.reservations.pop(order_id, None)

    def release(self, order_id, at, session, rule_hash):
        if order_id in self.reservations:
            self.event("cash_reserved", at, session, rule_hash, order_id, {"action": "release"})
            del self.reservations[order_id]

    def process(self, through, session):
        while self.command_cursor < len(self.commands):
            command = self.commands[self.command_cursor]
            at = _dt(command["submitted_at"])
            if at > through:
                break
            _same(_day(command["trading_date"]), session, "命令交易会话")
            self.command_cursor += 1
            order_id = command["order_id"]
            if command["action"] == "cancel":
                active = order_id in self.orders and self.orders[order_id]["active"]
                self.cancel_results.append({"command_id": command["command_id"], "order_id": order_id,
                    "status": "cancelled" if active else "rejected", "reason": "explicit_cancel" if active else "order_not_active"})
                if active:
                    self.release(order_id, at, session, typed_canonical_hash(self.orders[order_id]["command"]))
                    self.orders[order_id]["active"] = False
                continue
            rule_hash, p, _ = _select(self.bundle, self.code, session, at, _EXECUTION_RULES)
            _validate_parameters(p, self.code, session, at)
            self.command_rules.append({"command_id": command["command_id"], "rules_identity_hash": rule_hash,
                                       "parameters": p, "cash_policy": None})
            accepted = self.reserve(command, command["quantity"], command["reference_price"]["units"], at, session, rule_hash, p)
            self.orders[order_id] = {"command": command, "active": accepted, "filled": 0, "fills": 0}

    def execute(self, observation, bar, session):
        at, start = _dt(bar["available_time"]), _dt(bar["bar_start"])
        self.process(at, session)
        rule_hash, p, _ = _select(self.bundle, self.code, session, at, _EXECUTION_RULES)
        _validate_parameters(p, self.code, session, at)
        reference = bar["avg_units"] if bar.get("avg_units") is not None else bar["close_units"]
        capacity = integer(bar["volume"], "Bar 成交量", minimum=0) * self.participation // 1_000_000
        expected = {"instrument": self.instrument, "event_start": start.isoformat(), "event_time": at.isoformat(),
                    "session": session.isoformat(), "reference_price": {"units": reference, "scale": 2, "currency": "CNY"},
                    "visible_capacity": capacity, "rules_identity_hash": rule_hash, "parameters": p, "cash_policy": None}
        _same(dict(observation), expected, "执行观察与原始 Bar/规则")
        if not bar["completed"] or bar["quality_status"] != "pass" or _dt(bar["bar_end"]) > at:
            _fail("执行 Bar 未完成或不可见")
        for order_id, order in self.orders.items():
            command = order["command"]
            submitted = _dt(command["submitted_at"])
            if not order["active"] or _day(command["trading_date"]) != session or submitted > start or submitted >= at:
                continue
            price = _price(command, reference, p)
            limited = not p["low_limit_units"] <= price <= p["high_limit_units"]
            if command["limit_price"] is not None:
                limit = command["limit_price"]["units"]
                limited |= price > limit if command["side"] == "buy" else price < limit
            filled, rejected = 0, False
            if not limited and capacity > 0:
                remaining = command["quantity"] - order["filled"]
                for effect, amount in self.legs(command, min(remaining, capacity)):
                    amount = min(amount, capacity-filled)
                    if effect != "open":
                        amount = min(amount, self.available(order_id, effect))
                    if amount <= 0:
                        continue
                    def quote(quantity):
                        delta = quantity * (1 if command["side"] == "buy" else -1)
                        fee = _ceil(price * quantity * p["contract_unit_kg"] * p["open_fee_ppm" if effect == "open" else "close_fee_ppm"])
                        exact = (price-self.basis)*(-delta)*p["contract_unit_kg"] + self.remainder if effect != "open" else self.remainder
                        pnl = int(exact) if effect != "open" else 0
                        margin = _ceil(price * abs(self.contracts+delta)*p["contract_unit_kg"]*p["speculative_initial_margin_ppm"])
                        affordable = margin <= self.power(order_id)+self.margin+pnl-fee
                        return affordable, delta, fee, exact, pnl, margin
                    if not quote(amount)[0] and command["funds_policy"] == "resize" and effect == "open":
                        lo, hi = 0, amount
                        while lo < hi:
                            mid = (lo+hi+1)//2
                            if quote(mid)[0]:
                                lo = mid
                            else:
                                hi = mid-1
                        amount = lo
                    if amount <= 0 or not quote(amount)[0]:
                        rejected = True
                        break
                    _, delta, fee, exact, pnl, margin = quote(amount)
                    fill_id = f"{order_id}:fill:{order['fills']+1}"
                    source = self.event("fill", at, session, rule_hash, order_id,
                        {"instrument_hash": self.instrument_hash, "contracts_delta": delta, "position_effect": effect,
                         "settlement_price_units": price, "fee_units": fee, "position_margin_units": margin,
                         "multiplier": p["contract_unit_kg"]}, fill_id)
                    self.fills.append({"portfolio_id": "default", "session": session, "fill_id": fill_id, "order_id": order_id,
                        "instrument_id": self.code, "instrument_hash": self.instrument_hash, "asset_class": "cn_future",
                        "side": command["side"], "quantity": amount, "fill_time": at, "execution_price_units": price,
                        "price_scale": 2, "contract_multiplier": p["contract_unit_kg"],
                        "notional_units": price*amount*p["contract_unit_kg"], "fee_units": fee,
                        "realized_pnl_units": pnl, "position_effect": effect, "source_fill_hash": source})
                    reservation = dict(self.reservations.get(order_id, {}))
                    reservation["cash"] = max(0, reservation.get("cash", 0)-fee)
                    reservation["margin"] = max(0, reservation.get("margin", 0)-max(0, margin-self.margin))
                    if effect != "open":
                        reservation[effect] = max(0, reservation.get(effect, 0)-amount)
                    self.store(order_id, reservation)
                    if self.contracts*delta > 0:
                        self.basis = (self.basis*abs(self.contracts)+price*amount)/abs(self.contracts+delta)
                    elif self.contracts*(self.contracts+delta) <= 0:
                        self.basis = Fraction(price)
                    if effect == "open":
                        self.today += amount
                    elif effect == "close_today":
                        self.today -= amount
                    if effect != "open":
                        self.remainder = exact-pnl
                    self.contracts += delta
                    self.equity += pnl-fee
                    self.margin = margin
                    order["filled"] += amount
                    order["fills"] += 1
                    filled += amount
            capacity -= filled
            if order["filled"] == command["quantity"] or (rejected and command["funds_policy"] == "reject") or command["time_in_force"] == "IOC":
                self.release(order_id, at, session, rule_hash)
                order["active"] = False
            elif filled and not self.reserve(command, command["quantity"]-order["filled"], price, at, session, rule_hash, p):
                self.release(order_id, at, session, rule_hash)
                order["active"] = False

    def close(self, session, bars):
        policies = [item for item in self.session_bundle.policies if item.instrument.instrument_id == self.code and session in item.trading_dates]
        if not policies:
            _fail("缺少交易会话终点来源")
        policy = max(policies, key=lambda item: item.revision)
        calendar = policy.build_session(session, scope_binding_hash=typed_canonical_hash(self.session_bundle.scope_coverage_payload()))
        end = max(item.ends_at for item in calendar.segments if item.bar_eligible)
        _same(self.context["session_ends"].get(session.isoformat()), end.isoformat(), "DAY 会话终点")
        self.process(end, session)
        for order_id, order in self.orders.items():
            if order["active"] and _day(order["command"]["trading_date"]) == session:
                self.release(order_id, end, session, typed_canonical_hash(order["command"]))
                order["active"] = False
        settlement_pnl, settlement_hashes = 0, []
        valuation_time = max(_dt(row["available_time"]) for row in bars)
        facts = [row for row in self.settlements if _day(row["event"]["session"]) == session]
        if self.contracts:
            if len(facts) != 1:
                _fail("非零持仓缺少唯一结算事实")
            candidates = [row for row in self.bundle["rules"] if row["instrument_id"] == self.code
                and row["rule_id"] == "rule.cn_futures.settlement.v1" and _day(row["effective_from"]) <= session <= _day(row["effective_to"])]
            if len(candidates) != 1 or candidates[0]["available_at"] is None:
                _fail("结算时点缺少唯一规则")
            at = _dt(candidates[0]["available_at"])
            if at < end or sum(_dt(row["bar_end"]) == end and row["completed"] and row["quality_status"] == "pass"
                               and row["session_id"] == calendar.session_id for row in bars) != 1:
                _fail("结算缺少完整收盘 Bar 或结算早于收盘")
            rule_hash, p, snapshots = _select(self.bundle, self.code, session, at, _SETTLEMENT_RULES)
            _same(p.get("settlement_availability_semantics"), "session_close_event_not_supplier_timestamp", "结算可见语义")
            _same(calendar.calendar_policy_id, p["session_policy_id"], "结算会话规则")
            _same(policy.revision, p["session_policy_revision"], "结算会话修订")
            price, multiplier, ppm = p["settlement_price_units"], p["contract_unit_kg"], p["speculative_initial_margin_ppm"]
            exact = (price-self.basis)*self.contracts*multiplier+self.remainder
            settlement_pnl = int(exact)
            required = _ceil(price*abs(self.contracts)*multiplier*ppm)
            event = {"event_id": f"minute-settlement:{self.code}:{session.isoformat()}", "kind": "mark_to_market",
                "effective_time": at.isoformat(), "session": session.isoformat(), "group_id": "minute-default-cn-futures",
                "rule_hash": rule_hash, "payload": [["pnl_units", settlement_pnl], ["required_margin_units", required]], "parent_id": None}
            expected = {"instrument_id": self.code, "instrument_hash": self.instrument_hash, "settlement_time": at,
                "settlement_price_units": price, "price_scale": p["price_scale"], "position_contracts_before": self.contracts,
                "previous_settlement_price_units": int(self.basis), "contract_multiplier": multiplier,
                "speculative_margin_ppm": ppm, "pnl_units": settlement_pnl, "required_margin_units": required,
                "rule_hash": rule_hash, "rule_snapshot_hashes": snapshots, "aggregate_required_margin_units": required,
                "event": event, "event_hash": typed_canonical_hash(event)}
            actual = dict(facts[0], settlement_time=_dt(facts[0]["settlement_time"]))
            actual["event"] = dict(actual["event"], payload=[list(pair) for pair in actual["event"]["payload"]])
            _same(actual, expected, "独立结算事实")
            self.equity += settlement_pnl
            valuation_time = at
            self.margin = required
            self.basis, self.remainder = Fraction(price), exact-settlement_pnl
            settlement_hashes.append(expected["event_hash"])
        elif facts:
            _fail("空仓存在多余结算事实")
        if self.reservations:
            _fail("会话结束仍存在冻结")
        self.check_snapshot(session, settlement_pnl, settlement_hashes, valuation_time)

    def check_snapshot(self, session, settlement_pnl, hashes, valuation_time):
        def rows(name):
            return [dict(row) for row in self.canonical[name] if _day(row["session"]) == session]
        cash, valuations, positions = rows("cash"), rows("valuations"), rows("positions")
        if len(cash) != 1 or len(valuations) != 1:
            _fail("会话现金或估值快照缺失/重复")
        for row in (*cash, *valuations, *positions):
            _same(_dt(row["valuation_time"]), valuation_time, "会话估值时点")
            _same(row["portfolio_id"], "default", "会话账户")
            _same(row["snapshot_id"], cash[0]["snapshot_id"], "会话快照外键")
        fills = [row for row in self.fills if row["session"] == session]
        trade = sum(row["realized_pnl_units"]-row["fee_units"] for row in fills)
        expected_cash = {"total_cash_units": self.equity, "available_cash_units": self.equity-self.margin,
            "receivable_cash_units": 0, "margin_units": self.margin, "opening_cash_units": self.initial,
            "trade_cash_change_units": trade, "non_trade_cash_change_units": settlement_pnl,
            "non_trade_source_hash": typed_canonical_hash(hashes), "currency": "CNY"}
        for key, value in expected_cash.items():
            _same(cash[0][key], value, f"现金 {key}")
        for key, value in {"nav_units": self.equity, "currency": "CNY", "valuation_model": "futures_settlement_equity",
                           "snapshot_id": cash[0]["snapshot_id"], "source_state_hash": cash[0]["source_state_hash"]}.items():
            _same(valuations[0][key], value, f"估值 {key}")
        if len(positions) != (1 if self.contracts or fills else 0):
            _fail("持仓快照缺失/重复")
        if positions:
            for key, value in {"quantity": self.contracts, "sellable_quantity": 0, "unsettled_quantity": 0,
                "frozen_quantity": 0, "market_value_units": 0, "trade_quantity_change": sum(row["quantity"]*(1 if row["side"]=="buy" else -1) for row in fills),
                "non_trade_quantity_change": 0, "instrument_id": self.code, "instrument_hash": self.instrument_hash,
                "source_state_hash": cash[0]["source_state_hash"], "non_trade_source_hash": typed_canonical_hash(hashes)}.items():
                _same(positions[0][key], value, f"持仓 {key}")

    def run(self):
        observations = self.context["observations"]
        if len(observations) != len(self.bars):
            _fail("执行观察与正式 Bar 数量不一致")
        grouped = {}
        for observation, bar in zip(observations, self.bars):
            _same(bar["instrument_id"], self.code, "原始行情合约")
            session = _day(bar["trading_date"])
            grouped.setdefault(session, []).append((observation, bar))
        if list(grouped) != sorted(grouped):
            _fail("交易会话顺序倒置")
        _same(set(self.context["session_ends"]), {day.isoformat() for day in grouped}, "会话终点集合")
        if any(_day(row["event"]["session"]) not in grouped for row in self.settlements):
            _fail("结算存在未覆盖会话")
        for session, pairs in grouped.items():
            self.today = 0
            previous = None
            for observation, bar in pairs:
                at = _dt(bar["available_time"])
                if previous is not None and at <= previous:
                    _fail("行情时点重复或倒置")
                previous = at
                self.execute(observation, bar, session)
            self.close(session, [bar for _, bar in pairs])
        if self.command_cursor != len(self.commands) or self.cursor != len(self.events):
            _fail("命令或事件存在未验证事实")
        _same(self.context["command_rules"], self.command_rules, "提交当时规则")
        _same(self.context["cancel_results"], self.cancel_results, "撤单结果")
        _same(self.context.get("fee_facts", []), [], "期货费用事实范围")
        for name in ("cash", "positions", "valuations"):
            if any(_day(row["session"]) not in grouped for row in self.canonical[name]):
                _fail(f"{name} 存在额外会话")
        actual_fills = [dict(row, session=_day(row["session"]), fill_time=_dt(row["fill_time"])) for row in self.canonical["fills"]]
        _same(sorted(actual_fills, key=lambda row: row["fill_id"]), sorted(self.fills, key=lambda row: row["fill_id"]), "独立成交")
        costs = [{"portfolio_id": "default", "session": row["session"], "cost_id": row["fill_id"]+":fee",
            "fill_id": row["fill_id"], "cost_type": "transaction_fee", "amount_units": row["fee_units"],
            "currency": "CNY", "source_cost_hash": row["source_fill_hash"]} for row in self.fills]
        actual_costs = [dict(row, session=_day(row["session"])) for row in self.canonical["costs"]]
        _same(sorted(actual_costs, key=lambda row: row["cost_id"]), sorted(costs, key=lambda row: row["cost_id"]), "独立费用")
        actual_orders = {row["order_id"]: row for row in self.canonical["orders"]}
        if len(actual_orders) != len(self.orders) or len(actual_orders) != len(list(self.canonical["orders"])):
            _fail("订单事实缺失或重复")
        for order_id, value in self.orders.items():
            command, filled = value["command"], value["filled"]
            expected = {"portfolio_id": "default", "order_id": order_id, "instrument_id": self.code,
                "instrument_hash": self.instrument_hash, "asset_class": "cn_future", "side": command["side"],
                "requested_quantity": command["quantity"], "filled_quantity": filled,
                "status": "filled" if filled == command["quantity"] else "partially_filled" if filled else "rejected",
                "source_order_hash": typed_canonical_hash(command)}
            for key, expected_value in expected.items():
                _same(actual_orders[order_id][key], expected_value, f"订单 {key}")
            _same(_day(actual_orders[order_id]["session"]), _day(command["trading_date"]), "订单交易会话")
            for key in ("decision_time", "submitted_at"):
                _same(_dt(actual_orders[order_id][key]), _dt(command[key]), f"订单 {key}")
        return {row["fill_id"]: row["fee_units"] for row in self.fills}


def verify_explicit_futures_order_execution(
    *, context, canonical, rule_bundle, observations, settlement_events,
    session_policy_bundle, simulation_manifest=None, participation_cap_ppm=None,
) -> dict[str, int]:
    """复算分钟期货并返回每笔成交费用；生命周期由公共生命周期 oracle 复核。"""
    _same(context.get("contract_version"), "research-explicit-order-execution-v1", "执行上下文版本")
    if participation_cap_ppm is None and isinstance(simulation_manifest, Mapping):
        participation_cap_ppm = simulation_manifest.get("participation_cap_ppm")
    participation = integer(participation_cap_ppm, "正式容量比例", minimum=1)
    if participation > 1_000_000:
        _fail("容量比例超过成交量")
    if context.get("cash_scale") not in {None, 2}:
        _fail("分钟期货现金精度无效")
    bars = list(observations.values()) if isinstance(observations, Mapping) else list(observations)
    bars.sort(key=lambda row: (_day(row["trading_date"]), _dt(row["available_time"]), row["instrument_id"]))
    settlements = []
    for row in settlement_events:
        row = dict(row)
        if "event_json" in row:
            row["event"] = json.loads(row.pop("event_json"))
        settlements.append(row)
    bundle = rule_bundle.to_dict() if hasattr(rule_bundle, "to_dict") else rule_bundle
    return _Replay(context, canonical, bundle, bars, settlements, session_policy_bundle, participation).run()


__all__ = ["verify_explicit_futures_order_execution"]
