"""从封存日频期货显式订单事实独立复核执行与预占。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Any

from research_pipeline.platform import typed_canonical_hash

from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer

CONTEXT_VERSION = "research-explicit-futures-daily-execution-v1"


def _fail(message: str) -> None:
    raise EvidenceContractError(f"日频期货显式订单：{message}")


def _rows(value: Any, *, order_by: tuple[str, ...] = ()) -> list[dict[str, object]]:
    if value is None:
        return []
    if hasattr(value, "iter_rows"):
        return [dict(row) for row in value.iter_rows(order_by=order_by)]
    if not isinstance(value, (list, tuple)):
        _fail("支持事实必须是行列表")
    rows = [dict(row) for row in value]
    if order_by:
        rows.sort(key=lambda row: tuple(row.get(name) for name in order_by))
    return rows


def _rule_parameters(row: Mapping[str, object]) -> dict[str, object]:
    parameters = {key: value for key, value in row.items() if key not in {
        "rule_snapshot_hash", "application", "settlement_margin_available_at",
        "settlement_margin_hash", "settlement_margin_rate_pct", "product", "category",
    }}
    # 列式封存的 date 与命令 JSON 的 ISO 日期表示同一交易日。
    parameters["trading_date"] = date_value(parameters["trading_date"], "规则交易日").isoformat()
    return parameters


def _price(value: object, label: str) -> Decimal:
    if isinstance(value, Mapping) and {"units", "scale", "currency"} <= set(value):
        if value["currency"] != "CNY":
            _fail(f"{label}币种无效")
        return Decimal(int(value["units"])).scaleb(-int(value["scale"]))
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise EvidenceContractError(f"日频期货显式订单：{label}价格无效") from exc
    if not result.is_finite() or result <= 0:
        _fail(f"{label}价格无效")
    return result


def _fen(value: Decimal) -> int:
    return int((value * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _command_price(value: object) -> Decimal:
    return _price(value, "命令")


def _rule_for(command: Mapping[str, object], command_rules: Mapping[str, Mapping[str, object]], observations: Mapping[str, Mapping[str, object]], order_id: str, at: object) -> Mapping[str, object]:
    submitted = aware_datetime(command["submitted_at"], "命令提交时点")
    if aware_datetime(at, "规则选择时点") <= submitted:
        return command_rules[command["command_id"]]["parameters"]
    return observations[order_id]["parameters"]


def _fee(price: Decimal, quantity: int, rule: Mapping[str, object], effect: str) -> int:
    field = {
        "open": "open_fee_permyriad",
        "close_today": "close_today_fee_permyriad",
        "close_yesterday": "close_fee_permyriad",
    }.get(effect)
    if field is None:
        _fail("持仓效果无效")
    rate = Decimal(str(rule[field]))
    multiplier = Decimal(int(rule["multiplier"]))
    if rule["fee_unit"] == "per_lot_cny":
        return _fen(rate * quantity)
    if rule["fee_unit"] == "notional_permyriad":
        return _fen(price * multiplier * quantity * rate / Decimal(10000))
    _fail("费用单位无效")


def _margin(price: Decimal, quantity: int, rule: Mapping[str, object], effect: str) -> int:
    if effect != "open":
        return 0
    return _fen(price * int(rule["multiplier"]) * quantity * Decimal(str(rule["margin_rate_pct"])) / Decimal(100))


def _tick_and_scale(market: Mapping[str, object], command: Mapping[str, object], tick_rows: Sequence[Mapping[str, object]], session: date, code: str, at) -> tuple[Decimal | None, int]:
    matching = [row for row in tick_rows if str(row.get("code")) == code and date_value(row.get("date"), "tick.date") == session]
    if len(matching) > 1:
        _fail("tick size 来源重复")
    tick = None
    if matching:
        row = matching[0]
        if aware_datetime(row["available_time"], "tick.available_time") > at:
            _fail("tick size 在执行时点尚不可见")
        tick = _price(row["tick_size"], "tick_size")
    raw_scale = market.get("price_scale")
    if raw_scale is None:
        opening = _price(market["open"], "market.open")
        scale = max(int(command["reference_price"]["scale"]), max(0, -opening.as_tuple().exponent))
    else:
        scale = integer(raw_scale, "market.price_scale", minimum=0)
    return tick, scale


def _execution_price(command: Mapping[str, object], opening: Decimal, tick: Decimal | None, quote_scale: int, global_slippage_ticks: int) -> Decimal:
    if tick is None and (global_slippage_ticks or int(command.get("slippage_ticks", 0))):
        _fail("无 tick size 时不能使用 tick 滑点")
    direction = Decimal(1 if command["side"] == "buy" else -1)
    raw = opening * (Decimal(1) + direction * Decimal(str(command.get("slippage_bps", 0))) / Decimal(10000))
    if tick is not None:
        raw += direction * (global_slippage_ticks + int(command.get("slippage_ticks", 0))) * tick
        step = tick
    else:
        step = Decimal(1).scaleb(-quote_scale)
    units = (raw / step).to_integral_value(rounding=ROUND_CEILING if direction > 0 else ROUND_FLOOR)
    result = units * step
    if result <= 0:
        _fail("滑点复算得到非正成交价")
    return result


def _payload(event: Mapping[str, object]) -> dict[str, object]:
    raw = event.get("payload")
    if not isinstance(raw, list) or any(not isinstance(item, (list, tuple)) or len(item) != 2 for item in raw):
        _fail("金融事件 payload 无效")
    result = dict(raw)
    if len(result) != len(raw):
        _fail("金融事件 payload 字段重复")
    return result


def _market_rows(support_tables: Mapping[str, object]) -> list[dict[str, object]]:
    return _rows(support_tables.get("market_inputs"), order_by=("date", "code"))


def _canonical_rows(canonical: Mapping[str, object], name: str) -> list[dict[str, object]]:
    return _rows(canonical.get(name), order_by=("fill_id",) if name == "fills" else ("portfolio_id", "order_id"))


def _verify_commands(context: Mapping[str, object]) -> tuple[dict[str, dict[str, object]], list[dict[str, object]]]:
    required = {"contract_version", "commands", "events", "observations", "command_rules", "cancel_results", "session_ends", "fee_facts", "order_states", "initial_cash_fen", "cash_scale"}
    if context.get("contract_version") != CONTEXT_VERSION or not required <= set(context):
        _fail("显式上下文版本或字段不完整")
    if integer(context["initial_cash_fen"], "initial_cash_fen", minimum=1) <= 0 or context["cash_scale"] != 2:
        _fail("初始现金或精度无效")
    submits: dict[str, dict[str, object]] = {}
    commands = []
    previous = None
    for row in context["commands"]:
        if not isinstance(row, Mapping):
            _fail("命令行无效")
        row = dict(row)
        if row.get("action") not in {"submit", "cancel"}:
            _fail("命令动作无效")
        at = aware_datetime(row["submitted_at"], "命令提交时点")
        sequence = integer(row["source_sequence"], "source_sequence", minimum=0)
        if previous is not None and (at, sequence) < previous:
            _fail("命令来源顺序倒置")
        previous = (at, sequence)
        if not isinstance(row.get("source_hashes"), list) or not row["source_hashes"]:
            _fail("命令来源缺失")
        if row["action"] == "submit":
            if row["order_id"] in submits:
                _fail("submit order_id 重复")
            if row.get("instrument", {}).get("asset_class") != "cn_future":
                _fail("显式日频订单资产类别无效")
            if row.get("time_in_force") not in {"IOC", "DAY"} or row.get("order_type") not in {"market", "limit"}:
                _fail("显式订单类型或 TIF 无效")
            if row.get("position_effect") not in {"open", "close_today", "close_yesterday"}:
                _fail("显式订单持仓效果无效")
            if row.get("side") not in {"buy", "sell"} or integer(row.get("quantity"), "委托数量", minimum=1) <= 0:
                _fail("显式订单方向或数量无效")
            if aware_datetime(row["reference_price_available_at"], "参考价可见时点") > aware_datetime(row["decision_time"], "决策时点"):
                _fail("参考价在决策时点后才可见")
            submits[str(row["order_id"])] = row
        else:
            if row.get("order_id") not in submits:
                _fail("撤单没有对应提交命令")
        commands.append(row)
    if not submits:
        _fail("显式日频命令缺少 submit")
    return submits, commands


def verify_explicit_futures_daily_order_execution(*, context, canonical, support_tables, global_slippage_ticks=0):
    """独立复核日频期货显式用户订单，返回用户 fill 的费用映射。"""
    if not isinstance(context, Mapping):
        _fail("上下文必须为映射")
    submits, commands = _verify_commands(context)
    command_rules = {}
    for row in context["command_rules"]:
        if not isinstance(row, Mapping) or set(row) != {"command_id", "rules_identity_hash", "parameters"}:
            _fail("command_rules schema 无效")
        command_id = str(row["command_id"])
        if command_id in command_rules or command_id not in {str(item["command_id"]) for item in submits.values()}:
            _fail("command_rules 与 submit 集合不一致")
        command_rules[command_id] = dict(row)
    if set(command_rules) != {str(item["command_id"]) for item in submits.values()}:
        _fail("submit 缺少 command_rules")

    market_rows = _market_rows(support_tables)
    tick_rows = _rows(support_tables.get("tick_size_inputs"), order_by=("date", "code"))
    market_by_key = {}
    for row in market_rows:
        key = (date_value(row.get("date"), "market.date"), str(row.get("code")))
        if key in market_by_key:
            _fail("原始开盘行情主键重复")
        market_by_key[key] = row
    rule_rows = _rows(support_tables.get("rule_snapshots"), order_by=("trading_date", "contract_code"))
    rules_by_hash = {str(row["rule_snapshot_hash"]): row for row in rule_rows if row.get("rule_snapshot_hash") is not None}
    if len(rules_by_hash) != len(rule_rows):
        _fail("规则快照身份重复或缺失")

    observations = {}
    for raw in context["observations"]:
        if not isinstance(raw, Mapping) or str(raw.get("order_id")) in observations:
            _fail("执行观察重复或无效")
        row = dict(raw)
        order_id = str(row["order_id"])
        if order_id not in submits:
            _fail("执行观察引用未知订单")
        command = submits[order_id]
        session = date_value(command["trading_date"], "命令交易日")
        code = str(command["instrument"]["instrument_id"])
        market = market_by_key.get((session, code))
        if market is None:
            _fail("订单缺少封存开盘行情")
        event_start = aware_datetime(row["event_start"], "执行开始")
        event_time = aware_datetime(row["event_time"], "执行时点")
        if event_start != event_time:
            _fail("日频执行观察必须是单一开盘事件")
        if aware_datetime(market["execution_time"], "原始执行时点") != event_time:
            _fail("执行观察时点与原始开盘时点不一致")
        if _price(row["reference_price"], "观察参考价") != _price(market["open"], "原始开盘价"):
            _fail("执行观察参考价与原始开盘价不一致")
        if aware_datetime(market["open_available_at"], "原始开盘可见时点") > event_time:
            _fail("原始开盘在执行时点尚不可见")
        rule_hash = str(row["rules_identity_hash"])
        rule = rules_by_hash.get(rule_hash)
        if rule is None or dict(row["parameters"]) != _rule_parameters(rule):
            _fail("执行观察规则未绑定封存规则快照")
        if command_rules[command["command_id"]]["rules_identity_hash"] not in rules_by_hash:
            _fail("提交规则未绑定封存规则快照")
        if dict(command_rules[command["command_id"]]["parameters"]) != _rule_parameters(rules_by_hash[command_rules[command["command_id"]]["rules_identity_hash"]]):
            _fail("提交规则参数与封存规则快照不一致")
        tick, quote_scale = _tick_and_scale(market, command, tick_rows, session, code, event_time)
        expected_price = _execution_price(command, _price(market["open"], "原始开盘价"), tick, quote_scale, int(global_slippage_ticks))
        observed_price = _price(row["execution_price"], "观察成交价")
        if observed_price != expected_price:
            _fail("滑点、tick或不利方向舍入复算不一致")
        if row.get("quote_price_scale") != quote_scale or row.get("price_rounding_policy") != "adverse_direction":
            _fail("执行观察报价精度或舍入策略缺失")
        if row.get("slippage_ticks") != int(global_slippage_ticks) + int(command.get("slippage_ticks", 0)):
            _fail("执行观察 tick 滑点不一致")
        if "slippage_bps" in row and str(row["slippage_bps"]) != str(command.get("slippage_bps", 0.0)):
            _fail("执行观察 bps 滑点不一致")
        if "tick_size" in row:
            expected_tick = None if tick is None else str(tick)
            if row["tick_size"] != expected_tick:
                _fail("执行观察 tick 来源不一致")
        for name, relation in (("low_limit", lambda price, bound: price >= bound), ("high_limit", lambda price, bound: price <= bound)):
            if market.get(name) is not None and not relation(observed_price, _price(market[name], name)):
                if int(row.get("filled_quantity", 0)):
                    _fail("成交价越过封存涨跌停")
        observations[order_id] = row
    if set(observations) != set(submits):
        _fail("submit 与执行观察集合不一致")

    # 共享开盘容量来自原始 market_inputs；只允许同一会话同一合约消耗一次。
    capacity_left = {}
    for order_id, row in observations.items():
        command = submits[order_id]
        key = (date_value(command["trading_date"], "命令交易日"), str(command["instrument"]["instrument_id"]))
        market = market_by_key[key]
        supplied = market.get("visible_capacity") is not None
        if supplied:
            capacity_left.setdefault(key, integer(market["visible_capacity"], "visible_capacity", minimum=0))
            if row.get("visible_capacity") != market["visible_capacity"] or row.get("capacity_model") != "visible_capacity":
                _fail("执行观察容量没有绑定原始可见容量")
            if row.get("capacity_before") != capacity_left[key]:
                _fail("同标的共享容量顺序不一致")
            quantity = integer(row.get("filled_quantity"), "观察成交数量", minimum=0)
            if quantity > capacity_left[key]:
                _fail("成交数量超过共享可见容量")
            capacity_left[key] -= quantity
        elif row.get("visible_capacity") is not None or row.get("capacity_model") != "assumed_unbounded" or row.get("capacity_before") is not None:
            _fail("无原始容量时不能自报可见容量")

    canonical_orders = {str(row["order_id"]): row for row in _canonical_rows(canonical, "orders")}
    for order_id, command in submits.items():
        order = canonical_orders.get(order_id)
        if order is None:
            _fail("正式 orders 缺少显式订单")
        if any(order.get(name) != expected for name, expected in {
            "instrument_id": command["instrument"]["instrument_id"],
            "asset_class": "cn_future", "side": command["side"],
            "requested_quantity": command["quantity"], "source_order_hash": typed_canonical_hash(command),
        }.items()):
            _fail("正式订单身份或数量偏离显式命令")

    support_fills = _rows(support_tables.get("fills"), order_by=("fill_id",))
    user_fills = [row for row in support_fills if str(row.get("order_id")) in submits and row.get("execution_reason", "explicit_order") == "explicit_order"]
    canonical_fills = {str(row["fill_id"]): row for row in _canonical_rows(canonical, "fills")}
    fee_facts = {str(row["fill_id"]): row for row in context["fee_facts"]}
    if len(fee_facts) != len(context["fee_facts"]):
        _fail("fee_facts 重复")
    fill_events = {}
    for event in context["events"]:
        if not isinstance(event, Mapping):
            _fail("金融事件无效")
        payload = _payload(event)
        if event.get("kind") == "fill":
            fill_events[str(event["event_id"])] = (event, payload)
    verified = {}
    fills_by_order = {}
    for fill in user_fills:
        fill_id = str(fill["fill_id"])
        canonical_fill = canonical_fills.get(fill_id)
        if canonical_fill is None:
            _fail("用户成交缺少正式 canonical fill")
        event_pair = fill_events.get(fill_id)
        if event_pair is None:
            _fail("用户成交缺少同 ID 金融事件")
        event, payload = event_pair
        if payload.get("order_id") != fill["order_id"] or payload.get("contract_code") != fill["actual_contract"] or payload.get("settlement_price_units") != fill["execution_price_units"] or payload.get("fee_units") != fill["fee_fen"]:
            _fail("成交金融事件与原始成交事实不一致")
        if any(canonical_fill.get(left) != fill.get(right) for left, right in {
            "fill_id": "fill_id", "order_id": "order_id", "instrument_id": "actual_contract", "side": "side", "quantity": "quantity", "execution_price_units": "execution_price_units", "price_scale": "price_scale", "contract_multiplier": "multiplier", "fee_units": "fee_fen", "position_effect": "position_effect",
        }.items()):
            _fail("canonical fill 与封存成交事实不一致")
        order_id = str(fill["order_id"])
        observed = observations[order_id]
        if _price(observed["execution_price"], "观察成交价") != Decimal(int(fill["execution_price_units"])).scaleb(-int(fill["price_scale"])):
            _fail("成交定点价格与观察价格不一致")
        rule = observed["parameters"]
        expected_fee = _fee(_price(observed["execution_price"], "观察成交价"), int(fill["quantity"]), rule, str(fill["position_effect"]))
        if int(fill["fee_fen"]) != expected_fee:
            _fail("期货逐笔费用独立复算不一致")
        fact = fee_facts.get(fill_id)
        if fact is None or any(fact.get(name) != expected for name, expected in {
            "order_id": fill["order_id"], "quantity": fill["quantity"], "fee_fen": fill["fee_fen"], "rules_identity_hash": observed["rules_identity_hash"], "rounding_policy": "half_up_per_event",
        }.items()):
            _fail("fee_facts 与成交费用不一致")
        verified[fill_id] = expected_fee
        fills_by_order.setdefault(order_id, []).append(fill)
    if set(fee_facts) != set(verified):
        _fail("fee_facts 包含未知或缺失成交")

    states = {str(row["order_id"]): dict(row) for row in context["order_states"]}
    if set(states) != set(submits):
        _fail("order_states 与显式订单集合不一致")
    for order_id, command in submits.items():
        filled = sum(int(row["quantity"]) for row in fills_by_order.get(order_id, ()))
        state = states[order_id]
        if int(state["filled_quantity"]) != filled or int(state["remaining_quantity"]) != int(command["quantity"]) - filled:
            _fail("订单终态数量与成交事实不一致")
        expected_status = "filled" if filled == int(command["quantity"]) else "partially_filled" if filled else "rejected"
        if int(observations[order_id].get("filled_quantity")) != filled:
            _fail("执行观察成交数量与正式成交事实不一致")
        limit = command.get("limit_price")
        if limit is not None:
            limit_price = _price(limit, "订单限价")
            executable = (_price(observations[order_id]["execution_price"], "观察成交价") <= limit_price
                          if command["side"] == "buy" else
                          _price(observations[order_id]["execution_price"], "观察成交价") >= limit_price)
            if not executable and filled:
                _fail("限价未满足却产生成交")
            if not executable and observations[order_id].get("reason") not in {"limit_not_reached", None}:
                _fail("限价未达原因不一致")
        canonical_status = str(canonical_orders[order_id]["status"])
        if canonical_status != expected_status:
            _fail("订单摘要状态与显式成交事实不一致")

    # 预占守恒：逐事件验证数量、价格和费保证金，所有预占必须最终释放。
    active_cash = {}
    active_position = {}
    total_reserved = 0
    for event in context["events"]:
        values = _payload(event)
        order_id = str(values.get("order_id"))
        if order_id not in submits:
            _fail("预占或成交事件引用未知订单")
        command = submits[order_id]
        at = aware_datetime(event["effective_time"], "金融事件时点")
        if event["kind"] == "cash_reserved":
            if values.get("action") == "release":
                if order_id not in active_cash:
                    _fail("现金预占重复释放或未预占")
                total_reserved -= active_cash.pop(order_id)
                active_position.pop(order_id, None)
                continue
            quantity = integer(values.get("quantity"), "预占数量", minimum=0)
            reference = _price(values.get("reference_price"), "预占参考价")
            limit = values.get("limit_price")
            reservation_price = max(reference, _price(limit, "预占限价")) if limit is not None else reference
            rule = _rule_for(command, command_rules, observations, order_id, at)
            expected_cash = _fee(reservation_price, quantity, rule, command["position_effect"])
            expected_margin = _margin(reservation_price, quantity, rule, command["position_effect"])
            if values.get("cash_units") != expected_cash or values.get("margin_units") != expected_margin:
                _fail("现金和保证金预占未按命令、规则和余量重算")
            if values.get("reference_price") != command["reference_price"] or values.get("limit_price") != command.get("limit_price"):
                _fail("预占价格基准偏离命令")
            if order_id in active_cash:
                _fail("同一订单重复持有现金预占")
            active_cash[order_id] = expected_cash + expected_margin
            total_reserved += expected_cash + expected_margin
            if total_reserved > int(context["initial_cash_fen"]):
                _fail("订单间现金或保证金预占挪用")
        elif event["kind"] == "position_reserved":
            if values.get("action") == "release":
                if order_id not in active_position:
                    _fail("持仓预占重复释放或未预占")
                active_position.pop(order_id)
                continue
            quantity = integer(values.get("quantity"), "持仓预占数量", minimum=0)
            if (command["position_effect"] == "open"
                    or values.get("instrument_hash") != typed_canonical_hash(command["instrument"])):
                _fail("持仓预占未绑定平仓命令")
            if quantity > int(command["quantity"]):
                _fail("持仓预占超过订单余量")
            if order_id in active_position:
                _fail("同一订单重复持有持仓预占")
            active_position[order_id] = quantity
        elif event["kind"] == "fill":
            continue
        else:
            _fail("日频期货显式上下文含未知金融事件")
    if active_cash or active_position:
        _fail("订单终态后仍残留预占")

    # 取消结果必须能由命令时点前的实际生命周期事实推出。
    cancel_results = {str(row.get("command_id")): dict(row) for row in context["cancel_results"]}
    cancel_commands = [row for row in commands if row["action"] == "cancel"]
    if set(cancel_results) != {str(row["command_id"]) for row in cancel_commands}:
        _fail("撤单结果与撤单命令集合不一致")
    for request in cancel_commands:
        order_id = str(request["order_id"])
        state = states[order_id]
        at = aware_datetime(request["submitted_at"], "撤单时点")
        terminal_at = aware_datetime(state["at"], "订单终态时点")
        active = str(state["status"]) == "cancelled" and terminal_at == at and state.get("reason") == "user_cancel"
        result = cancel_results[str(request["command_id"])]
        if {"status", "reason"} <= set(result):
            expected = {"command_id": request["command_id"], "order_id": order_id, "status": "cancelled" if active else "rejected", "reason": "explicit_cancel" if active else "order_not_active"}
        else:
            expected = {"command_id": request["command_id"], "order_id": order_id, "at": request["submitted_at"], "reason": "cancelled" if active else "already_terminal"}
        if result != expected:
            _fail("撤单结果与订单实际终态不一致")
    return verified
