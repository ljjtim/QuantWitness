"""共享期货账户的正式输入与可见时间合同。"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping, Sequence
import re

from .sessions import SessionSegment, TradingSession

from .models import DomainContractError

SHARED_FUTURES_INPUT_VERSION = "research-shared-futures-input-v1"
SHARED_FUTURES_CONTEXT_VERSION = "research-shared-futures-context-v1"


def aware_time(value: object, field: str) -> datetime:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise DomainContractError(f"{field} 必须是带时区时间") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise DomainContractError(f"{field} 必须带时区")
    return result


def _text(record: Mapping, name: str) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainContractError(f"{name} 必须是非空字符串")
    return value


def _integer(record: Mapping, name: str, *, minimum: int = 0) -> int:
    value = record.get(name)
    if type(value) is not int or value < minimum:
        raise DomainContractError(f"{name} 必须是至少 {minimum} 的整数")
    return value


def _decimal(record: Mapping, name: str, *, positive: bool = False) -> Decimal:
    try:
        value = Decimal(str(record[name]))
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise DomainContractError(f"{name} 必须是十进制数") from exc
    if not value.is_finite() or value < 0 or (positive and value == 0):
        raise DomainContractError(f"{name} 数值无效")
    return value


def _choice(record: Mapping, name: str, choices: set[str]) -> str:
    value = record.get(name)
    if value not in choices:
        raise DomainContractError(f"{name} 必须属于 {sorted(choices)}")
    return value


def _records(spec: Mapping, field: str) -> list[dict]:
    value = spec.get(field)
    if not isinstance(value, (list, tuple)) or any(not isinstance(x, Mapping) for x in value):
        raise DomainContractError(f"{field} 必须为记录数组")
    return [dict(x) for x in value]


def _session(value: object) -> date:
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError) as exc:
        raise DomainContractError("trading_date 必须为交易日 YYYY-MM-DD") from exc


def validate_shared_futures_spec(spec: Mapping[str, object]) -> dict[str, object]:
    """在正式准入边界校验账户、真实合约、规则及决策声明。"""
    if not isinstance(spec, Mapping):
        raise DomainContractError("共享期货声明必须是JSON对象")
    result = deepcopy(dict(spec))
    if result.get("version") != SHARED_FUTURES_INPUT_VERSION:
        raise DomainContractError("共享期货输入版本无效")
    for field in ("portfolio_id", "account_id"):
        _text(result, field)
    if result.get("currency") != "CNY" or result.get("cash_scale") != 100:
        raise DomainContractError("共享期货账户只支持人民币分")
    _integer(result, "initial_cash_units", minimum=1)
    _choice(result, "frequency", {"1d", "1m"})
    mode = _choice(result, "execution_mode", {"target", "explicit_orders"})
    instruments = _records(result, "instruments")
    if not instruments:
        raise DomainContractError("共享期货账户必须声明真实合约")
    by_id = {}
    for instrument in instruments:
        code = _text(instrument, "instrument_id")
        if code in by_id:
            raise DomainContractError("共享期货合约重复")
        if any(token in code.upper() for token in ("8888", "9999", "CONTINUOUS")):
            raise DomainContractError("连续合约不能作为成交合约")
        _text(instrument, "exchange")
        _choice(instrument, "category", {"commodity", "index", "treasury"})
        _decimal(instrument, "contract_multiplier", positive=True)
        _integer(instrument, "price_scale", minimum=1)
        _integer(instrument, "tick_units", minimum=1)
        valid_from = aware_time(instrument.get("valid_from"), "instrument.valid_from")
        valid_until = aware_time(instrument.get("valid_until"), "instrument.valid_until")
        if valid_from >= valid_until:
            raise DomainContractError("合约规格有效期必须为非空半开区间")
        known = aware_time(instrument.get("available_at"), "instrument.available_at")
        deadline = aware_time(instrument.get("exit_deadline"), "exit_deadline")
        if known >= deadline:
            raise DomainContractError("合约退出截止时间必须晚于规则可见时间")
        _text(instrument, "source_ref")
        last_trading = aware_time(instrument.get("last_trading_at"), "last_trading_at")
        unsupported = aware_time(instrument.get("unsupported_from"), "unsupported_from")
        exit_known = aware_time(instrument.get("exit_rule_available_at"), "exit_rule_available_at")
        _text(instrument, "exit_source_ref")
        if deadline > min(last_trading, unsupported) or exit_known >= deadline:
            raise DomainContractError("退出截止不能越过最后交易或未支持业务阶段，退出规则必须事前可见")
        if instrument["category"] == "treasury":
            match = re.match(r"^(TS|TF|TL|T)[0-9]{4}(?:\.|$)", code.upper())
            if match is None:
                raise DomainContractError("国债必须绑定TS/TF/T/TL真实合约")
            face = _decimal(instrument, "face_value_cny", positive=True)
            basis = _decimal(instrument, "quote_basis", positive=True)
            expected = Decimal(2_000_000 if match.group(1) == "TS" else 1_000_000)
            if face != expected or basis != 100 or _decimal(instrument, "contract_multiplier") * basis != face:
                raise DomainContractError("国债报价乘数必须等于对应产品面值除以百元报价基准")
        by_id[code] = instrument
    rules = _records(result, "rules")
    covered = set()
    for rule in rules:
        code = rule.get("instrument_id")
        if code not in by_id:
            raise DomainContractError("费用或保证金规则引用未声明合约")
        start, end = _session(rule.get("effective_from")), _session(rule.get("effective_to"))
        if start > end:
            raise DomainContractError("共享期货规则有效区间倒置")
        aware_time(rule.get("available_at"), "rule.available_at")
        if "effective_at" in rule:
            aware_time(rule["effective_at"], "rule.effective_at")
        if not 0 < _decimal(rule, "margin_rate") <= 1:
            raise DomainContractError("保证金比例必须在(0,1]内")
        _choice(rule, "fee_unit", {"per_contract_cny", "notional_rate"})
        for field in ("open_fee", "close_fee", "close_today_fee"):
            _decimal(rule, field)
        _choice(rule, "close_order", {"yesterday_first", "today_first"})
        _text(rule, "source_ref")
        covered.add(code)
    if covered != set(by_id):
        raise DomainContractError("每个共享期货合约都必须声明费用和保证金规则")
    commands, targets, rolls = (_records(result, name) for name in ("commands", "targets", "roll_plans"))
    if (mode == "target" and commands) or (mode == "explicit_orders" and targets):
        raise DomainContractError("同一共享账户不能混用目标与显式策略订单")
    command_ids, order_ids, target_ids, roll_ids = set(), set(), set(), set()
    for command in commands:
        identifier = _text(command, "command_id")
        if identifier in command_ids:
            raise DomainContractError("command_id 重复")
        command_ids.add(identifier)
        action = _choice(command, "action", {"submit", "cancel"})
        oid = _text(command, "order_id")
        _validate_decision(command, by_id)
        if action == "submit":
            if oid in order_ids:
                raise DomainContractError("提交 order_id 重复")
            order_ids.add(oid)
            _choice(command, "direction", {"long", "short"})
            _choice(command, "position_effect", {"open", "close", "close_today", "close_yesterday"})
            _integer(command, "quantity", minimum=1)
            kind = _choice(command, "order_type", {"market", "limit"})
            _choice(command, "time_in_force", {"IOC", "DAY"})
            _choice(command, "funds_policy", {"reject", "resize"})
            _integer(command, "reference_price_units", minimum=1)
            if aware_time(command.get("reference_available_at"), "reference_available_at") > aware_time(command["event_time"], "event_time"):
                raise DomainContractError("订单参考价在提交时尚不可见")
            if kind == "limit":
                _integer(command, "limit_price_units", minimum=1)
                if command["limit_price_units"] % by_id[command["instrument_id"]]["tick_units"]:
                    raise DomainContractError("限价不符合价格tick")
            elif command.get("limit_price_units") is not None:
                raise DomainContractError("市价订单不能携带限价")
    for target in targets:
        identifier = _text(target, "target_id")
        if identifier in target_ids:
            raise DomainContractError("target_id 重复")
        target_ids.add(identifier)
        _validate_decision(target, by_id)
        _choice(target, "direction", {"long", "short"})
        _integer(target, "quantity")
    for plan in rolls:
        identifier = _text(plan, "roll_plan_id")
        if identifier in roll_ids:
            raise DomainContractError("roll_plan_id 重复")
        roll_ids.add(identifier)
        old, new = plan.get("old_instrument_id"), plan.get("new_instrument_id")
        if old not in by_id or new not in by_id or old == new:
            raise DomainContractError("换月必须关联两个已声明真实合约")
        before, after = by_id[old], by_id[new]
        if Decimal(str(before["contract_multiplier"])) != Decimal(str(after["contract_multiplier"])) or before["category"] != after["category"]:
            raise DomainContractError("1:1换月要求相同类别与报价乘数")
        _choice(plan, "direction", {"long", "short"})
        _integer(plan, "quantity", minimum=1)
        _integer(plan, "source_sequence")
        start = aware_time(plan.get("event_time"), "roll.event_time")
        if aware_time(plan.get("available_at"), "roll.available_at") > start:
            raise DomainContractError("换月映射在决策时尚不可见")
        _validate_spec_time(before, start)
        _validate_spec_time(after, start)
        deadline = aware_time(plan.get("deadline"), "roll.deadline")
        if deadline <= start or deadline > aware_time(before["exit_deadline"], "exit_deadline"):
            raise DomainContractError("换月截止时间必须晚于开始且不越过合约退出边界")
        _text(plan, "source_ref")
    session_index = _validate_sessions(result, by_id)
    for decision in (*commands, *targets):
        key = (decision["instrument_id"], _session(decision["trading_date"]))
        if key not in session_index:
            raise DomainContractError("决策交易日缺少冻结会话")
        record, _ = session_index[key]
        at = aware_time(decision["event_time"], "event_time")
        if not aware_time(record["available_at"], "session.available_at") <= at <= aware_time(record["close_at"], "close_at"):
            raise DomainContractError("决策超出已知交易会话或已过会话收尾")
    return result


def _validate_sessions(spec: Mapping, instruments: Mapping) -> dict:
    sessions = _records(spec, "sessions")
    if not sessions:
        raise DomainContractError("共享期货必须冻结明确交易会话")
    indexed = {}
    all_segments = {}
    for record in sessions:
        code = record.get("instrument_id")
        if code not in instruments:
            raise DomainContractError("交易会话引用未声明合约")
        day = _session(record.get("trading_date"))
        known = aware_time(record.get("available_at"), "session.available_at")
        close = aware_time(record.get("close_at"), "session.close_at")
        _text(record, "source_ref")
        segments = tuple(SessionSegment(
            _text(raw, "segment_id"), aware_time(raw.get("starts_at"), "starts_at"),
            aware_time(raw.get("ends_at"), "ends_at"), day,
            _choice(raw, "phase", {"day", "night"}), True,
            aware_time(raw.get("starts_at"), "starts_at"), "completed-bar-right-closed-v1",
        ) for raw in _records(record, "segments"))
        session = TradingSession(f"{code}:{day}", "cn_future", day, segments, record["source_ref"])
        if known > segments[0].starts_at or close != max(x.ends_at for x in segments):
            raise DomainContractError("会话规则必须事前可见，close_at必须是完整交易会话终点")
        key = (code, day)
        if key in indexed:
            raise DomainContractError("共享合约交易会话重复")
        indexed[key] = (record, session)
        all_segments.setdefault(code, []).extend(segments)
    for segments in all_segments.values():
        ordered = sorted(segments, key=lambda item: item.starts_at)
        if any(left.ends_at > right.starts_at for left, right in zip(ordered, ordered[1:])):
            raise DomainContractError("同一合约不同交易日会话不能重叠")
    return indexed


def _validate_spec_time(instrument: Mapping, at: datetime) -> None:
    if not (aware_time(instrument.get("valid_from"), "instrument.valid_from") <= at
            < aware_time(instrument.get("valid_until"), "instrument.valid_until")):
        raise DomainContractError("事件或决策超出合约规格有效期，必须按历史版本分开运行")


def _validate_decision(record: Mapping, instruments: Mapping) -> None:
    code = record.get("instrument_id")
    if code not in instruments:
        raise DomainContractError("决策引用未声明合约")
    time = aware_time(record.get("event_time"), "event_time")
    _validate_spec_time(instruments[code], time)
    if aware_time(record.get("available_at"), "available_at") > time:
        raise DomainContractError("决策来源在提交时尚不可见")
    if aware_time(instruments[code]["exit_rule_available_at"], "exit_rule_available_at") > time:
        raise DomainContractError("退出规则在决策时尚不可见")
    if aware_time(instruments[code]["available_at"], "instrument.available_at") > time:
        raise DomainContractError("合约规格在决策时尚不可见")
    _integer(record, "source_sequence")
    _session(record.get("trading_date"))


def validate_shared_futures_events(spec: Mapping, events: Sequence[Mapping]) -> list[dict]:
    instruments = {row["instrument_id"]: row for row in spec["instruments"]}
    result = []
    keys = set()
    daily_bars = set()
    sessions = _validate_sessions(spec, instruments)
    endings = {}
    for event in events:
        row = dict(event)
        kind = _choice(row, "kind", {"bar", "settlement", "session_end"})
        code = row.get("instrument_id")
        if code not in instruments:
            raise DomainContractError("行情引用未声明共享合约")
        time = aware_time(row.get("event_time"), "market.event_time")
        _validate_spec_time(instruments[code], time)
        known = aware_time(row.get("available_at"), "market.available_at")
        if known > time:
            raise DomainContractError("行情或结算在事件时点尚不可见")
        if aware_time(instruments[code]["exit_rule_available_at"], "exit_rule_available_at") > time:
            raise DomainContractError("退出规则在事件时尚不可见")
        if aware_time(instruments[code]["available_at"], "instrument.available_at") > time:
            raise DomainContractError("行情事件早于合约规格可见时点")
        day = _session(row.get("trading_date"))
        if (code, day) not in sessions:
            raise DomainContractError("行情交易日缺少冻结会话")
        session_record, session = sessions[(code, day)]
        if aware_time(session_record["available_at"], "session.available_at") > time:
            raise DomainContractError("交易会话规则在事件时点尚不可见")
        if kind == "settlement" and known < aware_time(session_record["close_at"], "close_at"):
            raise DomainContractError("逐日结算价不能早于完整交易会话结束发布")
        if kind == "session_end":
            if time < aware_time(session_record["close_at"], "close_at") or (code, day) in endings:
                raise DomainContractError("会话收尾不能提前或重复滚动今昨仓")
            endings[(code, day)] = time
        sequence = _integer(row, "source_sequence")
        key = (kind, code, time, sequence)
        if key in keys:
            raise DomainContractError("共享期货市场事件重复")
        keys.add(key)
        _text(row, "source_ref")
        if kind != "session_end":
            _integer(row, "price_units", minimum=1)
        if kind == "bar":
            start = aware_time(row.get("bar_start"), "bar_start")
            _validate_spec_time(instruments[code], start)
            if start > time or (spec["frequency"] == "1m" and start == time):
                raise DomainContractError("分钟bar必须先开始后完成")
            if not any(segment.starts_at <= start and time <= segment.ends_at
                       and (segment.contains_completed_bar_end(time) if spec["frequency"] == "1m"
                            else segment.contains_instant(time)) for segment in session.segments):
                raise DomainContractError("行情区间或夜盘交易日不符合冻结会话")
            if spec["frequency"] == "1d":
                daily_key = (code, row["trading_date"])
                if start != time or daily_key in daily_bars:
                    raise DomainContractError("日频每合约每会话只允许一个开盘执行事件")
                daily_bars.add(daily_key)
            if row.get("completed") is not True:
                raise DomainContractError("共享期货只消费已完成bar或已发布开盘事件")
            _integer(row, "capacity")
            if row["price_units"] % instruments[code]["tick_units"]:
                raise DomainContractError("执行价格不符合合约tick")
            for field in ("limit_up_units", "limit_down_units"):
                if row.get(field) is not None:
                    _integer(row, field, minimum=1)
        result.append(row)
    if not result:
        raise DomainContractError("共享期货市场事件不能为空")
    for row in result:
        ending = endings.get((row["instrument_id"], _session(row["trading_date"])))
        if ending is not None and row["kind"] != "session_end" and aware_time(row["event_time"], "event_time") > ending:
            raise DomainContractError("同一交易会话收尾后不能再成交或结算")
    return result
