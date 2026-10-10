"""从封存行动、分钟行情和成交独立重建登记权益与现金持仓。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import time
from itertools import groupby
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction, InstrumentKey, MinuteRuleSnapshotBundle

from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer, ordered_rows


_VERSION = "research-minute-corporate-actions-v1"
_ZONE = ZoneInfo("Asia/Shanghai")
_QUANTITY_KINDS = {"stock_dividend", "split", "reverse_split"}


def _fail(message):
    raise EvidenceContractError(f"分钟公司行动：{message}")


def _groups(rows, session_key, order):
    for session, group in groupby(ordered_rows(rows, order_by=order),
                                key=lambda row: date_value(row[session_key], session_key)):
        yield session, list(group)


def _take_session(iterator, pending, session, label):
    if pending is not None and pending[0] < session:
        _fail(f"{label} 包含没有行情会话的事实")
    if pending is not None and pending[0] == session:
        return pending[1], next(iterator, None)
    return [], pending


def _payload(event):
    raw = event.get("payload")
    if not isinstance(raw, list) or any(not isinstance(item, (list, tuple)) or len(item) != 2 for item in raw):
        _fail("事件 payload 不是字段对列表")
    value = dict(raw)
    if len(value) != len(raw):
        _fail("事件 payload 字段重复")
    return value


def _parse_context(context):
    if (not isinstance(context, Mapping)
            or set(context) != {"contract_version", "actions", "records", "events"}
            or context["contract_version"] != _VERSION
            or any(not isinstance(context[key], list) for key in ("actions", "records", "events"))):
        _fail("上下文 schema 无效")
    actions = []
    identities = set()
    for raw in context["actions"]:
        try:
            action = CorporateAction.from_dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            raise EvidenceContractError("分钟公司行动原始声明无效") from exc
        if action.contract_version != 2 or action.kind not in _QUANTITY_KINDS | {"cash_dividend", "delisting_cash"}:
            _fail("v6 只接收完整 v2 现金、送转、拆并和退市行动")
        key = action.action_id, action.revision
        if key in identities:
            _fail("行动版本重复")
        identities.add(key)
        actions.append(action)
    records = {}
    for raw in context["records"]:
        if not isinstance(raw, Mapping) or set(raw) != {"instrument_hash", "record_time", "quantity", "source_ref"}:
            _fail("登记持仓 schema 无效")
        moment = aware_datetime(raw["record_time"], "record_time").astimezone(_ZONE)
        if moment.time() != time(15) or not isinstance(raw["source_ref"], str) or not raw["source_ref"]:
            _fail("登记必须有来源及真实收盘时点")
        integer(raw["quantity"], "record.quantity", minimum=0)
        key = str(raw["instrument_hash"]), moment.date()
        if key in records:
            _fail("登记持仓重复")
        records[key] = dict(raw)
    events = {}
    event_ids = set()
    for raw in context["events"]:
        if not isinstance(raw, Mapping) or set(raw) != {
            "event_id", "kind", "effective_time", "session", "group_id", "rule_hash", "payload", "parent_id",
        }:
            _fail("金融事件 schema 无效")
        moment = aware_datetime(raw["effective_time"], "event.effective_time")
        session = date_value(raw["session"], "event.session")
        if moment.astimezone(_ZONE).date() != session or raw["kind"] not in {"corporate_action", "settlement"}:
            _fail("事件种类或会话无效")
        if not isinstance(raw["event_id"], str) or not raw["event_id"] or raw["event_id"] in event_ids:
            _fail("事件身份缺失或重复")
        event_ids.add(raw["event_id"])
        values = _payload(raw)
        events.setdefault(session, []).append((dict(raw), values))
    return actions, records, events


def _consume_event(events, *, kind, moment, expected, action=None):
    if action is not None:
        matches = [index for index, (event, payload) in enumerate(events)
                   if event["kind"] == kind and payload.get("action_hash") == action.action_hash
                   and payload.get("action_phase") == expected["action_phase"]]
    else:
        identity_key = "receivable_id" if "receivable_id" in expected else "entitlement_id"
        matches = [index for index, (event, payload) in enumerate(events)
                   if event["kind"] == kind and payload.get(identity_key) == expected[identity_key]]
    if len(matches) != 1:
        _fail("权益事件缺失或重复")
    event, payload = events.pop(matches[0])
    if payload != expected or aware_datetime(event["effective_time"], "event.effective_time") != moment:
        _fail("事件金额、数量或生效时点与原始行动不一致")
    if action is not None and event["parent_id"] != action.action_id:
        _fail("行动事件来源关联不一致")


def _cash_amount(action, quantity, multiplier):
    # 行动现金以分计量；万分币先对整笔权益半入舍位，再转换到分钟账本精度。
    cents = ((quantity * action.cash_per_share_microunits + 5000) // 10000
             if action.cash_per_share_microunits else quantity * action.cash_per_share_units)
    return cents * multiplier


def verify_minute_corporate_actions(
    *, corporate_action_context: Mapping[str, object], canonical: Mapping[str, object],
    bars: object, rule_bundle: MinuteRuleSnapshotBundle, asset_class: str, price_scale: int,
    order_index: Mapping, select_rule: Callable, verify_quantity: Callable,
) -> None:
    """从零期初持仓重放公司行动；事件不是权益金额和登记数量的计算输入。"""
    if asset_class not in {"cn_stock", "cn_etf"} or type(price_scale) is not int or not 2 <= price_scale <= 8:
        _fail("现货资产或金额精度无效")
    multiplier = 10 ** (price_scale - 2)
    actions, records, events_by_session = _parse_context(corporate_action_context)
    namespace = "cn_stock" if asset_class == "cn_stock" else "cn_fund"
    instruments = {}
    for item in rule_bundle.instruments:
        if item.asset_class != asset_class:
            continue
        code = item.instrument_id
        instrument = InstrumentKey(code, asset_class, code.rpartition(".")[2], "CNY",
                                   "stock" if asset_class == "cn_stock" else "etf")
        instruments[instrument.instrument_hash] = code
    if any(action.instrument_hash not in instruments for action in actions):
        _fail("行动标的不属于本次资产规则")
    grouped = {
        name: iter(_groups(canonical[name], "session", ("session", *order)))
        for name, order in (("fills", ("fill_time", "fill_id")), ("positions", ("instrument_id",)),
                            ("cash", ("valuation_time",)), ("valuations", ("valuation_time",)))
    }
    pending = {name: next(iterator, None) for name, iterator in grouped.items()}
    # 每个标的只保留当前桶、未到期权益和登记日状态；行情和六表按会话流式消费。
    buckets = {key: {"sellable": 0, "unsettled": 0, "frozen": 0} for key in instruments}
    cash_claims = {}
    share_claims = {}
    registered = {}
    applied = {}
    arrived = set()
    initial_cash = None
    cumulative_trade_cash = 0
    earned_cash = 0
    paid_cash = 0
    first_session = None
    last_session = None
    for session, bar_rows in _groups(bars, "trading_date", ("trading_date", "available_time", "source_sequence")):
        rows = {}
        for name, iterator in grouped.items():
            rows[name], pending[name] = _take_session(iterator, pending[name], session, name)
        eligible_bars = [row for row in bar_rows if row.get("completed") is True
                         and row.get("quality_status") == "pass" and row["instrument_id"] in instruments.values()]
        if not eligible_bars:
            if any(rows.values()) or session in events_by_session:
                _fail("金融事实缺少已完成可见行情")
            continue
        start = min(aware_datetime(row["bar_start"], "bar_start") for row in eligible_bars)
        if first_session is None:
            for (key, day), record in records.items():
                if day < session:
                    if key not in buckets or record["quantity"] != 0:
                        _fail("窗口前登记必须符合零期初持仓")
                    registered[key, day] = 0
        first_session = session if first_session is None else first_session
        last_session = session
        session_events = events_by_session.pop(session, [])
        before = {key: sum(value.values()) for key, value in buckets.items()}
        prior_trade_unsettled = {}
        for key, value in buckets.items():
            locked = sum(q for h, _due, q in share_claims.values() if h == key)
            ordinary = value["unsettled"] - locked
            if ordinary < 0:
                _fail("待上市权益超过未结算数量")
            prior_trade_unsettled[key] = ordinary
            value["sellable"] += ordinary
            value["unsettled"] -= ordinary
        for claim, (due, amount) in list(cash_claims.items()):
            if due <= session:
                _consume_event(session_events, kind="settlement", moment=start,
                               expected={"cash_units": 0, "quantity": 0, "receivable_id": claim})
                paid_cash += amount
                del cash_claims[claim]
        for claim, (key, due, quantity) in list(share_claims.items()):
            if due <= session:
                _consume_event(session_events, kind="settlement", moment=start,
                               expected={"cash_units": 0, "quantity": 0, "entitlement_id": claim})
                buckets[key]["unsettled"] -= quantity
                buckets[key]["sellable"] += quantity
                del share_claims[claim]
        latest = {}
        for action in actions:
            if action.announcement_available_time <= start:
                prior = latest.get(action.action_id)
                if prior is None or prior.revision < action.revision:
                    latest[action.action_id] = action
        for action in latest.values():
            previous = applied.get(action.action_id)
            if previous is not None and previous != action:
                _fail("已落账行动不能回填另一修订")
        earned_today = 0
        for action in sorted(latest.values(), key=lambda item: item.action_id):
            if action.effective_date != session:
                continue
            key = action.instrument_hash
            value = buckets[key]
            held = sum(value.values())
            record = None
            if action.kind == "delisting_cash":
                count = held
                if action.settlement_available_time > start:
                    _fail("退市清算价格尚不可见")
            else:
                record_key = key, action.record_date
                record = records.get(record_key)
                if record is None or record_key not in registered:
                    _fail("行动缺少由登记日成交重建的收盘持仓")
                count = registered[record_key]
            expected = {
                "contract_version": 2, "action_hash": action.action_hash,
                "action_kind": action.kind, "action_revision": action.revision,
                "action_phase": "effective", "source_ref": action.source_ref,
                "instrument_hash": key, "cash_delta_units": 0, "sellable_delta": 0,
                "unsettled_delta": 0, "frozen_delta": 0, "registered_quantity": count,
                "record_date": action.record_date.isoformat(), "ex_date": action.ex_date.isoformat(),
                "pay_date": action.pay_date.isoformat(),
            }
            if record is not None:
                expected["record_position"] = record
            amount = 0
            entitlement = 0
            if action.kind in {"cash_dividend", "delisting_cash"}:
                amount = _cash_amount(action, count, multiplier)
                if action.kind == "delisting_cash":
                    for bucket, quantity in value.items():
                        expected[f"{bucket}_delta"] = -quantity
                    expected.update(cancel_position_entitlements=True,
                                    trading_termination_date=action.trading_termination_date.isoformat(),
                                    settlement_available_time=action.settlement_available_time.isoformat())
                    share_claims = {claim: item for claim, item in share_claims.items() if item[0] != key}
            elif action.kind == "stock_dividend":
                entitlement = count * action.ratio_numerator // action.ratio_denominator - count
            else:
                if count != held or any(item[0] == key for item in share_claims.values()):
                    _fail("拆并股登记数量或既有受限权益无法闭合")
                target = count * action.ratio_numerator // action.ratio_denominator
                if target >= held:
                    entitlement = target - held
                elif action.shares_sellable_date > session:
                    for bucket, quantity in value.items():
                        expected[f"{bucket}_delta"] = -quantity
                    entitlement = target
                else:
                    converted = {bucket: quantity * action.ratio_numerator // action.ratio_denominator
                                 for bucket, quantity in value.items()}
                    remainder = target - sum(converted.values())
                    for bucket in sorted(value, key=lambda name: (
                            -(value[name] * action.ratio_numerator % action.ratio_denominator), name))[:remainder]:
                        converted[bucket] += 1
                    for bucket in value:
                        expected[f"{bucket}_delta"] = converted[bucket] - value[bucket]
            if amount:
                earned_today += amount
                if action.pay_date > session:
                    claim = f"cash:{action.action_hash}"
                    expected.update(cash_receivable_units=amount, cash_due_date=action.pay_date.isoformat(), receivable_id=claim)
                    cash_claims[claim] = action.pay_date, amount
                else:
                    expected["cash_delta_units"] = amount
                    paid_cash += amount
            if action.shares_arrival_date is not None:
                expected.update(shares_arrival_date=action.shares_arrival_date.isoformat(),
                                shares_sellable_date=action.shares_sellable_date.isoformat())
            if entitlement:
                if action.shares_sellable_date > session:
                    claim = f"position:{action.action_hash}"
                    expected.update(position_entitlement_quantity=entitlement,
                                    position_due_date=action.shares_sellable_date.isoformat(), entitlement_id=claim)
                    share_claims[claim] = key, action.shares_sellable_date, entitlement
                else:
                    expected["sellable_delta"] += entitlement
            _consume_event(session_events, kind="corporate_action", moment=start, expected=expected, action=action)
            for bucket in value:
                value[bucket] += expected[f"{bucket}_delta"]
            value["unsettled"] += expected.get("position_entitlement_quantity", 0)
            applied[action.action_id] = action
        earned_cash += earned_today
        for action in applied.values():
            if (action.shares_arrival_date is not None and action.shares_arrival_date < session
                    and action.action_id not in arrived):
                _fail("分钟流缺少股份到账会话")
            if action.shares_arrival_date != session:
                continue
            record = records[action.instrument_hash, action.record_date]
            count = registered[action.instrument_hash, action.record_date]
            arrived_quantity = count * action.ratio_numerator // action.ratio_denominator
            if action.kind == "stock_dividend" or action.ratio_numerator >= action.ratio_denominator:
                arrived_quantity -= count
            expected = {
                "contract_version": 2, "action_hash": action.action_hash, "action_kind": action.kind,
                "action_revision": action.revision, "action_phase": "shares_arrival", "source_ref": action.source_ref,
                "instrument_hash": action.instrument_hash, "arrived_quantity": arrived_quantity,
                "shares_arrival_date": action.shares_arrival_date.isoformat(),
                "shares_sellable_date": action.shares_sellable_date.isoformat(), "record_position": record,
                "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0, "frozen_delta": 0,
            }
            _consume_event(session_events, kind="corporate_action", moment=start, expected=expected, action=action)
            arrived.add(action.action_id)
        for event, payload in session_events:
            key = payload.get("instrument_hash")
            if (event["kind"] != "settlement" or set(payload) != {"cash_units", "instrument_hash", "quantity"}
                    or payload["cash_units"] != 0 or payload["quantity"] != prior_trade_unsettled.get(key)
                    or not payload["quantity"] or aware_datetime(event["effective_time"], "event.time") != start):
                _fail("封存事件包含没有原始权益或结算依据的变化")
            prior_trade_unsettled[key] = 0
        trade_changes = {key: 0 for key in instruments}
        trade_cash = 0
        traded = set()
        for fill in rows["fills"]:
            key = str(fill["instrument_hash"])
            if key not in buckets or instruments[key] != str(fill["instrument_id"]):
                _fail("成交标的身份不一致")
            quantity = integer(fill["quantity"], "fill.quantity", minimum=1)
            moment = aware_datetime(fill["fill_time"], "fill.fill_time")
            order = order_index.get(str(fill["order_id"]))
            if order is None:
                _fail("成交缺少正式订单")
            submitted = aware_datetime(order["submitted_at"], "order.submitted_at")
            if moment < start or moment.astimezone(_ZONE).date() != session:
                _fail("成交早于权益执行时点或跨越会话")
            if any(action.instrument_hash == key and action.kind == "delisting_cash"
                   and action.announcement_available_time <= submitted
                   and action.trading_termination_date <= session for action in actions):
                _fail("终止交易后仍有成交")
            settlement = select_rule(rule_bundle, f"rule.{namespace}.settlement.v1",
                                              instruments[key], session, submitted)
            days = integer(dict(settlement.parameters).get("settlement_days"), "settlement_days", minimum=0)
            if days not in {0, 1}:
                _fail("不支持的结算天数")
            value = buckets[key]
            side = str(fill["side"])
            if side == "sell":
                requested = integer(order["requested_quantity"], "requested_quantity", minimum=1)
                if max(quantity, requested) > value["sellable"]:
                    _fail("卖出超出 T+1 或公司行动可卖数量")
                lot = select_rule(rule_bundle, f"rule.{namespace}.lot_size.v1",
                                          instruments[key], session, submitted)
                parameters = dict(lot.parameters)
                if "sell_min_quantity" in parameters or "sell_quantity_step" in parameters:
                    for amount in (quantity, requested):
                        verify_quantity(parameters, side="sell", quantity=amount,
                                                    sellable=value["sellable"])
                else:
                    lot_key = "buy_lot_shares" if asset_class == "cn_stock" else "buy_lot_units"
                    step = integer(parameters.get(lot_key), lot_key, minimum=1)
                    if quantity % step and (parameters.get("sell_remainder_allowed") is not True
                                            or quantity % step != value["sellable"] % step):
                        _fail("卖出不符合零股余额")
                value["sellable"] -= quantity
                trade_changes[key] -= quantity
            elif side == "buy":
                value["unsettled" if days else "sellable"] += quantity
                trade_changes[key] += quantity
            else:
                _fail("成交方向无效")
            notional = integer(fill["notional_units"], "fill.notional_units", minimum=1)
            fee = integer(fill["fee_units"], "fill.fee_units", minimum=0)
            trade_cash += (notional if side == "sell" else -notional) - fee
            traded.add(key)
        cumulative_trade_cash += trade_cash
        for (key, record_day), record in records.items():
            if record_day != session:
                continue
            if key not in buckets:
                _fail("登记标的不在规则范围内")
            moment = aware_datetime(record["record_time"], "record_time")
            if any(aware_datetime(fill["fill_time"], "fill_time") > moment for fill in rows["fills"]):
                _fail("登记日成交晚于收盘时点")
            quantity = sum(buckets[key].values())
            if record["quantity"] != quantity:
                _fail("登记数量与原始成交及权益重建不一致")
            registered[key, record_day] = quantity
        if len(rows["cash"]) > 1 or len(rows["valuations"]) > 1:
            _fail("现货会话现金或估值快照重复")
        if not rows["cash"]:
            if rows["fills"] or rows["positions"] or rows["valuations"] or earned_today or any(before.values()):
                _fail("非空金融状态缺少现金快照")
            continue
        cash = rows["cash"][0]
        valuation_time = aware_datetime(cash["valuation_time"], "valuation_time")
        if initial_cash is None:
            initial_cash = integer(cash["opening_cash_units"], "opening_cash_units", minimum=0)
        expected_cash = {
            "opening_cash_units": initial_cash, "trade_cash_change_units": trade_cash,
            "non_trade_cash_change_units": earned_today,
            "receivable_cash_units": sum(amount for _due, amount in cash_claims.values()),
            "available_cash_units": initial_cash + cumulative_trade_cash + paid_cash,
            "total_cash_units": initial_cash + cumulative_trade_cash + earned_cash,
        }
        if any(integer(cash.get(key), key) != value for key, value in expected_cash.items()):
            _fail("应收、到账或非交易现金与登记权益不一致")
        positions = {}
        for row in rows["positions"]:
            key = str(row["instrument_hash"])
            if key not in instruments or row["instrument_id"] != instruments[key] or key in positions:
                _fail("持仓标的缺失或重复")
            positions[key] = row
        market_value = 0
        for key, value in buckets.items():
            quantity = sum(value.values())
            change = quantity - before[key] - trade_changes[key]
            position = positions.get(key)
            if position is None:
                if quantity or change or key in traded:
                    _fail("缺少公司行动或成交后的持仓快照")
                continue
            expected = {"quantity": quantity, "sellable_quantity": value["sellable"],
                        "unsettled_quantity": value["unsettled"], "frozen_quantity": value["frozen"],
                        "non_trade_quantity_change": change, "trade_quantity_change": trade_changes[key]}
            if any(integer(position.get(name), name) != amount for name, amount in expected.items()):
                _fail("持仓数量、可卖桶或非交易净变化不一致")
            visible = [row for row in eligible_bars if row["instrument_id"] == instruments[key]
                       and aware_datetime(row["available_time"], "available_time") <= valuation_time
                       and aware_datetime(row["bar_end"], "bar_end") <= valuation_time]
            if quantity and not visible:
                _fail("权益估值缺少当时可见价格")
            price = (integer(max(visible, key=lambda row: (
                aware_datetime(row["bar_end"], "bar_end"), integer(row["source_sequence"], "source_sequence")))
                ["close_units"], "close_units", minimum=1) if visible else 0)
            amount = quantity * price
            if integer(position["market_value_units"], "market_value_units") != amount:
                _fail("持仓估值与权益数量及可见行情不一致")
            market_value += amount
        if len(rows["valuations"]) != 1:
            _fail("缺少净资产快照")
        valuation = rows["valuations"][0]
        if (aware_datetime(valuation["valuation_time"], "valuation_time") != valuation_time
                or integer(valuation["nav_units"], "nav_units") != expected_cash["total_cash_units"] + market_value):
            _fail("NAV 未包含正确现金、应收或股份权益")
    if any(value is not None for value in pending.values()) or events_by_session:
        _fail("封存金融事实超出实际行情会话")
    for key in records:
        if key not in registered or not any(action.instrument_hash == key[0] and action.record_date == key[1] for action in actions):
            _fail("登记事实缺少对应行动或行情")
    if first_session is not None:
        for action in actions:
            if first_session <= action.effective_date <= last_session and action.action_id not in applied:
                _fail("窗口内行动未执行或在生效时尚不可见")
