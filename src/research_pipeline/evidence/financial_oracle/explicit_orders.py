"""从封存命令、行情和金融事件独立复核显式订单。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from research_pipeline.platform import typed_canonical_hash

from ..errors import EvidenceContractError
from .common import aware_datetime, ceil_ratio, date_value, integer, normalized, ordered_rows

EXPLICIT_ORDER_CONTEXT_VERSION = "research-explicit-order-execution-v1"


def _fail(message):
    raise EvidenceContractError(f"显式订单：{message}")


def _rows(value, label):
    if not isinstance(value, (list, tuple)) or any(not isinstance(row, Mapping) for row in value):
        _fail(f"{label} 必须为完整事实列表")
    return value


def _price(value, scale, label):
    if not isinstance(value, Mapping) or set(value) != {"units", "scale", "currency"}:
        _fail(f"{label} 必须为完整定点价格")
    if value["currency"] != "CNY" or value["scale"] != scale:
        _fail(f"{label} 精度或币种不一致")
    return integer(value["units"], label, minimum=1)


def _instrument(value, asset_class):
    if (not isinstance(value, Mapping) or value.get("asset_class") != asset_class
            or value.get("currency") != "CNY" or not value.get("instrument_id")):
        _fail("命令或观察标的无效")
    return str(value["instrument_id"]), typed_canonical_hash(dict(value))


def _commands(rows, asset_class, scale):
    submits, cancels, identities = {}, {}, set()
    previous = None
    for row in _rows(rows, "commands"):
        required = {"command_id", "action", "order_id", "instrument", "decision_time", "submitted_at",
                    "available_at", "source_sequence", "source_hashes", "trading_date"}
        fields = required | {"side", "quantity", "position_effect", "order_type", "time_in_force",
            "limit_price", "reference_price", "reference_price_available_at", "funds_policy", "slippage_bps", "slippage_ticks"}
        if set(row) != fields:
            _fail("命令缺少完整来源、交易字段或时钟")
        command_id, order_id = row["command_id"], row["order_id"]
        if not isinstance(command_id, str) or not command_id or command_id in identities:
            _fail("command_id 缺失或重复")
        if not isinstance(order_id, str) or not order_id:
            _fail("order_id 缺失")
        identities.add(command_id)
        code, instrument_hash = _instrument(row["instrument"], asset_class)
        submitted = aware_datetime(row["submitted_at"], "命令提交时点")
        decision = aware_datetime(row["decision_time"], "命令决策时点")
        available = aware_datetime(row["available_at"], "命令可见时点")
        sequence = integer(row["source_sequence"], "source_sequence", minimum=0)
        clock = submitted, sequence
        if decision > submitted or available > submitted or (previous is not None and clock < previous):
            _fail("命令含未来信息或来源顺序倒置")
        previous = clock
        hashes = row["source_hashes"]
        if (not isinstance(hashes, (list, tuple)) or not hashes
                or any(not isinstance(item, str) or len(item) != 64
                       or any(char not in "0123456789abcdef" for char in item) for item in hashes)
                or list(hashes) != sorted(set(hashes))):
            _fail("命令来源身份缺失、重复或未排序")
        session = date_value(row["trading_date"], "命令交易会话")
        value = dict(row, _code=code, _hash=instrument_hash, _submitted=submitted,
                     _decision=decision, _session=session, _clock=clock, _source_hash=typed_canonical_hash(dict(row)))
        if row["action"] == "submit":
            if order_id in submits:
                _fail("submit order_id 重复")
            if (row.get("side") not in {"buy", "sell"} or row.get("order_type") not in {"market", "limit"}
                    or row.get("time_in_force") not in {"IOC", "DAY"}
                    or row.get("funds_policy") not in {"reject", "resize"}):
                _fail("submit 交易合同无效")
            integer(row.get("quantity"), "委托数量", minimum=1)
            if asset_class in {"cn_stock", "cn_etf"} and row.get("position_effect") != "auto":
                _fail("现货不能声明期货开平仓")
            if row.get("order_type") == "limit":
                _price(row.get("limit_price"), scale, "限价")
            elif row.get("limit_price") is not None:
                _fail("市价单不能携带限价")
            if row.get("reference_price") is not None:
                _price(row["reference_price"], scale, "提交价格基准")
                if aware_datetime(row.get("reference_price_available_at"), "价格基准可见时点") > decision:
                    _fail("价格基准在决策时尚不可见")
            integer(row.get("slippage_ticks"), "slippage_ticks", minimum=0)
            try:
                bps = Decimal(str(row.get("slippage_bps")))
            except InvalidOperation:
                _fail("slippage_bps 无效")
            if not bps.is_finite() or bps < 0:
                _fail("slippage_bps 必须为非负有限数")
            submits[order_id] = value
        elif row["action"] == "cancel":
            if any(row[name] is not None for name in ("side", "quantity", "position_effect", "order_type",
                "time_in_force", "limit_price", "reference_price", "reference_price_available_at", "funds_policy")) or row["slippage_bps"] != 0 or row["slippage_ticks"] != 0:
                _fail("撤单不能携带交易字段或滑点")
            original = submits.get(order_id)
            if (original is None or original["instrument"] != row["instrument"]
                    or clock <= original["_clock"] or decision < original["_submitted"]):
                _fail("撤单不能引用未来或其他标的订单")
            cancels.setdefault(order_id, []).append(value)
        else:
            _fail("命令 action 无效")
    return submits, cancels


def _visible_parameters(bundle, observation, asset_class):
    """从正式规则中选取参数；观察只承担引用，不承担规则权威。"""
    code, _ = _instrument(observation["instrument"], asset_class)
    session = date_value(observation["session"], "观察会话")
    as_of = aware_datetime(observation.get("event_time", observation["event_start"]), "规则可见时点")
    if isinstance(bundle, Mapping):
        selected = []
        for rule in bundle.get(code, ()):
            if (date_value(rule["effective_start"], "规则生效日") <= session
                    and (rule["effective_end"] is None or session <= date_value(rule["effective_end"], "规则终止日"))
                    and aware_datetime(rule["available_time"], "规则可见时点") <= as_of):
                selected.append(rule)
        if len(selected) != 1:
            _fail("执行机会没有唯一已可见的正式规则")
        rule = selected[0]
        identity = typed_canonical_hash(dict(rule))
        parameters = dict(rule["parameters"])
    else:
        from .minute_rules import visible_minute_rule
        namespace = "cn_stock" if asset_class == "cn_stock" else "cn_fund"
        required = {f"rule.{namespace}.{kind}.v1" for kind in (
            "lot_size", "trading_fee", "settlement", "instrument_lifecycle", "price_limit", "session", "suspension")}
        if asset_class == "cn_stock":
            required.add("rule.cn_stock.adjustment_factor_snapshot.v1")
        selected = {name: visible_minute_rule(bundle, name, code, session, as_of) for name in sorted(required)}
        parameters = {}
        for rule in sorted(selected.values(), key=lambda item: item.rule_id):
            for name, value in rule.parameters:
                if name in parameters and parameters[name] != value:
                    _fail("分钟正式规则参数互相冲突")
                parameters[name] = value
        source_hashes = {source.source_id: source.source_hash for source in bundle.sources}
        ordered = [selected[name] for name in sorted(required | {
            f"rule.{namespace}.session.v1", f"rule.{namespace}.suspension.v1",
        }) if name in selected]
        bindings = [typed_canonical_hash({"snapshot_hash": rule.snapshot_hash,
                    "source_hashes": {name: source_hashes[name] for name in rule.source_ids}})
                    for rule in ordered]
        identity = typed_canonical_hash({"bundle_hash": bundle.bundle_hash, "bindings": bindings})
    declared = observation.get("parameters")
    dynamic = {"price_scale", "paused", "high_limit_units", "low_limit_units"}
    if (observation.get("rules_identity_hash") != identity or not isinstance(declared, Mapping)
            or any(declared.get(name) != value for name, value in parameters.items())
            or set(declared) - parameters.keys() - dynamic):
        _fail("观察参数或身份未绑定正式规则 bundle")
    cash_policy = observation.get("cash_policy")
    if cash_policy is None:
        _fail("现货命令或执行观察缺少完整 cash_policy.rule")
    if cash_policy is not None:
        if not isinstance(cash_policy, Mapping):
            _fail("cash_policy 必须为完整映射或 None")
        for name in ("commission_ppm", "min_commission_units", "transfer_fee_ppm", "sell_tax_ppm", "settlement_days"):
            if cash_policy.get(name) != parameters.get(name):
                _fail("cash_policy 费用或结算参数偏离正式规则")
        raw_rule = cash_policy.get("rule")
        if not isinstance(raw_rule, Mapping) or typed_canonical_hash(dict(raw_rule)) != cash_policy.get("rule_hash"):
            _fail("cash_policy.rule 内容身份无效")
        if isinstance(bundle, Mapping):
            if raw_rule != rule or cash_policy.get("rule_hash") != identity:
                _fail("cash_policy 未绑定正式规则")
        else:
            derived = dict(raw_rule.get("parameters", ()))
            if (derived.get("source_binding_hash") != identity
                    or derived.get("source_instrument_id") != code
                    or any(derived.get(name) != value for name, value in parameters.items())):
                _fail("分钟 cash_policy.rule 未绑定来源快照")
            expected_bindings = [{"rule_id": item.rule_id, "snapshot_hash": item.snapshot_hash,
                                  "source_hashes": {name: source_hashes[name] for name in item.source_ids}}
                                 for item in ordered]
            if derived.get("source_rule_bindings") != expected_bindings:
                _fail("分钟派生规则来源列表不一致")
        if not isinstance(bundle, Mapping):
            expected_rule = {"rule_id": f"minute-{asset_class}-derived-v1", "version": 1,
                "market": asset_class, "instrument_type": observation["instrument"]["contract_kind"],
                "effective_start": max(item.effective_from for item in ordered).isoformat(),
                "effective_end": min(item.effective_to for item in ordered).isoformat(),
                "available_time": max(item.available_at for item in ordered).isoformat(),
                "official_source_id": "minute-rule-bundle", "evidence_url": "research_pipeline/docs/minute_simulation.md",
                "parameters": sorted({**parameters, "lot_size": parameters["buy_quantity_step"],
                    "source_binding_hash": identity, "source_instrument_id": code,
                    "source_rule_bindings": expected_bindings}.items())}
            if _plain(raw_rule) != _plain(expected_rule):
                _fail("分钟 cash_policy.rule 与正式派生来源不一致")
        if cash_policy.get("lot_size") != parameters.get("lot_size", parameters.get("buy_quantity_step")):
            _fail("cash_policy 数量格点偏离正式规则")
    resolved = dict(declared)
    if "sell_remainder_allowed" not in resolved:
        # 旧沪深整手合同允许一次带出零股；细分格点仍以显式规则为准。
        resolved["sell_remainder_allowed"] = (
            "sell_min_quantity" not in parameters and "sell_quantity_step" not in parameters
            and "stock_board" not in parameters and cash_policy["lot_size"] == 100
            and observation["instrument"].get("venue") in {"XSHG", "XSHE"}
            and as_of.astimezone(ZoneInfo("Asia/Shanghai")).date() >= date(2006, 7, 1)
        )
    return resolved


def _execution_price(command, reference, parameters, scale):
    tick = integer(parameters.get("price_tick_units", 1), "最小价位", minimum=1)
    direction = 1 if command["side"] == "buy" else -1
    raw = (Decimal(reference) * (1 + direction * Decimal(str(command["slippage_bps"])) / 10_000)
           + direction * integer(command["slippage_ticks"], "slippage_ticks", minimum=0) * tick)
    rounding = ROUND_CEILING if direction > 0 else ROUND_FLOOR
    price = int((raw / tick).to_integral_value(rounding=rounding)) * tick
    if price <= 0:
        return None
    if command["order_type"] == "limit":
        limit = _price(command["limit_price"], scale, "限价")
        if (direction > 0 and (reference > limit or price > limit)) or (direction < 0 and (reference < limit or price < limit)):
            return None
    return price


def _fee(notional, prior_numerator, paid, parameters, side):
    commission = max(paid, integer(parameters["min_commission_units"], "最低佣金", minimum=0),
                     ceil_ratio(prior_numerator + notional * integer(parameters["commission_ppm"], "佣金比例", minimum=0), 1_000_000))
    delta = max(commission - paid, 0)
    transfer = ceil_ratio(notional * integer(parameters["transfer_fee_ppm"], "过户费比例", minimum=0), 1_000_000)
    tax = (ceil_ratio(notional * integer(parameters["sell_tax_ppm"], "卖出税比例", minimum=0), 1_000_000)
           if side == "sell" else 0)
    return delta + transfer + tax, commission, delta, transfer, tax


def _quantity_allowed(quantity, side, parameters, sellable):
    lot = parameters.get("lot_size", parameters.get("buy_lot_shares", parameters.get("buy_lot_units")))
    minimum = integer(parameters.get(f"{side}_min_quantity", lot), "最小申报数量", minimum=1)
    step = integer(parameters.get(f"{side}_quantity_step", lot), "数量步长", minimum=1)
    maximum = parameters.get(f"{side}_max_quantity")
    if maximum is not None and quantity > integer(maximum, "最大申报数量", minimum=minimum):
        return False
    if quantity >= minimum and (quantity - minimum) % step == 0:
        return True
    if side != "sell" or parameters.get("sell_remainder_allowed") is not True or quantity > sellable:
        return False
    if sellable < minimum:
        return quantity == sellable and quantity > 0
    remainder = (sellable - minimum) % step
    return remainder > 0 and quantity % step == remainder


def _payload(event):
    raw = event.get("payload")
    if (not isinstance(raw, list) or any(not isinstance(item, (list, tuple)) or len(item) != 2 for item in raw)
            or len(dict(raw)) != len(raw)):
        _fail("金融事件 payload 无效或重复")
    return dict(raw)


def verify_explicit_order_execution(*, context, canonical, rule_bundle, asset_class,
                                    price_scale, market_observations=None, participation_cap_ppm=None,
                                    session_policy_bundle=None, account_context=None, verified_account_events=(),
                                    corporate_actions=(), verified_external_cashflow_events=(), credit_oracle=None):
    """独立重算执行、累计费用与订单专属预占，返回已验证的逐笔费用。

    verified_account_events 必须先通过原账户或分钟权益 oracle；这里只按生效时点
    消费其现金、股份和结算变化，不代替权益金额复算。account_context 提供正式期初
    可用现金及待到账、待上市权益，存在时由账户结算事件推进可卖股份。
    corporate_actions 提供封存公告；终止交易须同时满足生效日期和公告可见时点。
    """
    if (not isinstance(context, Mapping) or context.get("contract_version") != EXPLICIT_ORDER_CONTEXT_VERSION
            or not {"commands", "events", "observations", "fee_facts"} <= context.keys()):
        _fail("上下文 schema 或版本无效")
    if asset_class not in {"cn_stock", "cn_etf"}:
        _fail("当前显式订单 oracle 尚未闭合期货保证金与今昨仓")
    commands = context["commands"] if credit_oracle is None else [*context["commands"], *credit_oracle.context["risk_commands"]]
    submits, cancels = _commands(commands, asset_class, price_scale)
    cash_scale = context.get("cash_scale")
    cash_scale = price_scale if cash_scale is None else integer(cash_scale, "现金精度", minimum=0)
    command_rules = {}
    for item in _rows(context.get("command_rules"), "command_rules"):
        command_id = item.get("command_id")
        matches = [command for command in submits.values() if command["command_id"] == command_id]
        if len(matches) != 1 or command_id in command_rules:
            _fail("提交规则与命令集合不一致")
        command = matches[0]
        observation = dict(item, instrument=command["instrument"], session=command["trading_date"],
                           event_start=command["submitted_at"])
        command_rules[command_id] = dict(observation, _parameters=_visible_parameters(rule_bundle, observation, asset_class))
    if set(command_rules) != {item["command_id"] for item in submits.values()}:
        _fail("提交命令缺少独立可见规则")
    formal_orders = set()
    for order in canonical["orders"]:
        order_id = str(order["order_id"])
        command = submits.get(order_id)
        if command is None or order_id in formal_orders:
            _fail("正式订单与 submit 集合不一致")
        formal_orders.add(order_id)
        if (order["instrument_id"] != command["_code"] or order["instrument_hash"] != command["_hash"]
                or order["side"] != command["side"] or order["requested_quantity"] != command["quantity"]
                or aware_datetime(order["submitted_at"], "正式提交时点") != command["_submitted"]
                or aware_datetime(order["decision_time"], "正式决策时点") != command["_decision"]
                or date_value(order["session"], "正式订单会话") != command["_session"]
                or order.get("source_order_hash") != command["_source_hash"]):
            _fail("正式订单数量、标的、方向或时钟偏离命令")
    if formal_orders != set(submits):
        _fail("submit 缺少正式订单，零成交也必须保留订单")
    trading_ends = _trading_ends(corporate_actions, verified_account_events)
    observations = {}
    previous = None
    for raw_row in _rows(context["observations"], "observations"):
        row = dict(raw_row)
        if not {"instrument", "event_start", "event_time", "session", "reference_price", "visible_capacity",
                "rules_identity_hash", "parameters", "cash_policy"} <= row.keys():
            _fail("执行观察缺少必要字段")
        code, instrument_hash = _instrument(row["instrument"], asset_class)
        start = aware_datetime(row["event_start"], "执行区间开始")
        at = aware_datetime(row["event_time"], "执行时点")
        if start > at or (previous is not None and at < previous):
            _fail("执行观察时钟倒置")
        previous = at
        identity = instrument_hash, at
        if identity in observations:
            _fail("同标的执行机会重复，会导致容量重复使用")
        parameters = _visible_parameters(rule_bundle, row, asset_class)
        if parameters.get("price_scale", price_scale) != price_scale:
            _fail("执行观察价格精度偏离规则")
        reference = _price(row["reference_price"], price_scale, "执行参考价")
        capacity = row["visible_capacity"]
        if capacity is not None:
            integer(capacity, "可见执行容量", minimum=0)
        if market_observations is not None:
            _bind_market(row, market_observations, reference, code, start, at, price_scale, participation_cap_ppm,
                         trading_ends.get(instrument_hash), rule_bundle=rule_bundle, asset_class=asset_class)
        observations[identity] = dict(row, _parameters=parameters, _reference=reference, _start=start,
                                      _time=at, _hash=instrument_hash, _code=code)
    fee_facts = {}
    for fact in _rows(context["fee_facts"], "fee_facts"):
        fill_id = fact.get("fill_id")
        if not isinstance(fill_id, str) or not fill_id or fill_id in fee_facts:
            _fail("逐笔累计费用缺少唯一 fill_id")
        fee_facts[fill_id] = fact
    used, totals, commissions, quantities = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    numerators, fees, confirmed = defaultdict(int), defaultdict(int), defaultdict(list)
    from .minute_rules import minute_index
    verified = {}
    expected_events = minute_index(canonical["fills"], ("fill_id",), "显式订单规范成交")
    fill_at = {}
    for fill in ordered_rows(canonical["fills"], order_by=("fill_time", "fill_id")):
        fill_id, order_id = str(fill["fill_id"]), str(fill["order_id"])
        command = submits.get(order_id)
        if command is None or fill_id in verified:
            _fail("成交引用未知订单或 fill_id 重复")
        at = aware_datetime(fill["fill_time"], "成交时点")
        observation = observations.get((command["_hash"], at))
        if observation is None:
            _fail("成交缺少唯一合格执行机会")
        if (observation["_start"] < command["_submitted"] or at <= command["_submitted"]
                or date_value(observation["session"], "执行会话") != command["_session"]
                or date_value(fill["session"], "成交会话") != command["_session"]):
            _fail("成交回溯同 Bar 或跨越交易会话")

        if command["time_in_force"] == "IOC":
            eligible = [item["_time"] for item in observations.values()
                        if item["_hash"] == command["_hash"] and item["_start"] >= command["_submitted"]
                        and item["_time"] > command["_submitted"]
                        and date_value(item["session"], "IOC 会话") == command["_session"]]
            if not eligible or at != min(eligible):
                _fail("IOC 余量进入后续执行机会")
        parameters = observation["_parameters"]
        expected_price = _execution_price(command, observation["_reference"], parameters, price_scale)
        if expected_price is None or fill["execution_price_units"] != expected_price:
            _fail("成交价不满足限价、tick 或 bps 滑点")
        if (fill["instrument_hash"] != command["_hash"] or fill["instrument_id"] != command["_code"]
                or fill["side"] != command["side"] or fill["price_scale"] != price_scale):
            _fail("成交标的、方向或价格精度偏离命令")
        if parameters.get("paused") is True or parameters.get("trading_status") == "suspended":
            _fail("停牌标的出现成交")
        if parameters.get("price_limit_mode", "bounded") == "bounded":
            lower, upper = _price_bounds(parameters, observation)
            if not lower <= expected_price <= upper:
                _fail("滑点后价格越过正式涨跌停限制")
        quantity = integer(fill["quantity"], "成交数量", minimum=1)
        quantities[order_id] += quantity
        if quantities[order_id] > command["quantity"]:
            _fail("累计成交超过委托数量")
        if command["side"] == "buy" and not _quantity_allowed(quantity, "buy", parameters, quantity):
            _fail("成交违反正式数量格点")
        identity = command["_hash"], at
        used[identity] += quantity
        if observation["visible_capacity"] is not None and used[identity] > observation["visible_capacity"]:
            _fail("同标的同 Bar 订单没有共享容量")
        notional = int((Decimal(expected_price * quantity) * Decimal(10) ** (cash_scale - price_scale)).to_integral_value(rounding=ROUND_HALF_UP))
        if notional != integer(fill["notional_units"], "成交金额", minimum=1):
            _fail("成交金额与价格数量不一致")
        before = {"order_id": order_id, "side": command["side"], "notional_units": totals[order_id],
                  "commission_units": commissions[order_id], "commission_numerator": numerators[order_id],
                  "fee_units": fees[order_id], "confirmed_fill_ids": list(confirmed[order_id])}
        fee, commission, delta, transfer, tax = _fee(notional, numerators[order_id], commissions[order_id], parameters, command["side"])
        numerators[order_id] += notional * parameters["commission_ppm"]
        fees[order_id] += fee
        confirmed[order_id].append(fill_id)
        totals[order_id] += notional
        commissions[order_id] = commission
        if integer(fill["fee_units"], "成交费用", minimum=0) != fee:
            _fail("逐 order 累计最低佣金或分笔税费不一致")
        fact = fee_facts.get(fill_id)
        after = {"order_id": order_id, "side": command["side"], "notional_units": totals[order_id],
                 "commission_units": commission, "commission_numerator": numerators[order_id],
                 "fee_units": fees[order_id], "confirmed_fill_ids": list(confirmed[order_id])}
        expected = {"rule_hash": observation["rules_identity_hash"], "before": before, "after": after,
                    "fee_units": fee, "commission_units": delta, "transfer_units": transfer, "tax_units": tax}
        if fact is None or any(_plain(fact.get(name)) != _plain(value) for name, value in expected.items()):
            _fail("逐 fill 累计费用事实与独立复算不一致")
        verified[fill_id] = fee
        fill_at[fill_id] = observation
    if set(fee_facts) != set(verified):
        _fail("累计费用事实包含未知成交")
    _verify_opening_positions(context, account_context)
    session_ends = explicit_order_session_ends(context, session_policy_bundle=session_policy_bundle,
                                               frequency="daily" if isinstance(rule_bundle, Mapping) else "minute")
    valid_cancels = _verify_cancel_results(context, submits, canonical, session_ends, observations)
    _verify_reservations(context, canonical, submits, valid_cancels, expected_events, fill_at, observations,
                         command_rules, price_scale, cash_scale, session_ends,
                         verified_account_events, account_context, verified_external_cashflow_events, credit_oracle)
    for fill in expected_events.values():
        if any(cancel["_submitted"] <= aware_datetime(fill["fill_time"], "成交时点")
               for cancel in valid_cancels.get(str(fill["order_id"]), ())):
            _fail("有效撤单后仍有成交")
    return verified


def _price_bounds(parameters, observation):
    if "low_limit_units" in parameters and "high_limit_units" in parameters:
        return int(parameters["low_limit_units"]), int(parameters["high_limit_units"])
    # 价格边界必须由封存行情给出，不能从自报成交价反推。
    if observation.get("_market_lower") is None or observation.get("_market_upper") is None:
        _fail("执行机会缺少正式涨跌停价格来源")
    return observation["_market_lower"], observation["_market_upper"]


def _trading_ends(corporate_actions, verified_account_events):
    """从可见公告及已独立验证的注销事件确定旧证券停止交易时点。"""
    from research_pipeline.domain.corporate_actions import CorporateAction

    ends = {}
    for raw in corporate_actions:
        try:
            action = CorporateAction.from_dict(raw)
        except (ValueError, TypeError, KeyError) as exc:
            raise EvidenceContractError("显式订单：公司行动事实无效") from exc
        if action.contract_version != 2 or action.trading_termination_date is None:
            continue
        boundary = datetime.combine(action.trading_termination_date, time.min, ZoneInfo("Asia/Shanghai"))
        at = max(boundary, action.announcement_available_time)
        ends[action.instrument_hash] = min(ends.get(action.instrument_hash, at), at)
    for event in verified_account_events:
        if event["kind"] != "security_conversion":
            continue
        key = _payload(event)["old_instrument_hash"]
        at = aware_datetime(event["effective_time"], "已验证换股注销时点")
        ends[key] = min(ends.get(key, at), at)
    return ends


def _bind_market(observation, market, reference, code, start, at, scale, participation_cap_ppm, trading_end=None, *, rule_bundle, asset_class):
    if isinstance(market, Mapping):
        row = market.get((code, date_value(observation["session"], "行情会话")))
        if row is None or reference != row.get("open_price_units"):
            _fail("日频执行参考价未绑定封存开盘行情")
        local_at = at.astimezone(ZoneInfo("Asia/Shanghai"))
        if start != at or (local_at.hour, local_at.minute, local_at.second, local_at.microsecond) != (9, 30, 0, 0):
            _fail("日频执行机会不是正式开盘时点")
        observation["_market_lower"] = row.get("low_limit_units")
        observation["_market_upper"] = row.get("high_limit_units")
        if observation.get("capacity_model", "assumed_unbounded") != row.get("capacity_model", "assumed_unbounded"):
            _fail("执行容量模型与封存行情不一致")
        if observation["visible_capacity"] != row.get("opening_capacity", 2**62):
            _fail("日频执行容量偏离事前可见行情")
        declared = observation["parameters"]
        expected_dynamic = {"paused": row["paused"] or trading_end is not None and trading_end <= at,
                            "price_scale": scale}
        if declared.get("price_limit_mode") != "unbounded":
            expected_dynamic.update(high_limit_units=row["high_limit_units"], low_limit_units=row["low_limit_units"])
        if any(declared.get(name) != value for name, value in expected_dynamic.items()):
            _fail("执行观察价格限制或停牌事实偏离封存行情")
    else:
        from .minute_rules import minute_single_bar, verify_minute_cash_bar_suspended
        row = minute_single_bar(market, {"instrument_id": code, "bar_start": normalized(start)}, "显式订单执行机会无法绑定唯一 completed/pass bar")
        if aware_datetime(row["available_time"], "行情可见时点") != at:
            _fail("分钟执行时点偏离 completed bar 可见时点")
        expected = row["avg_units"] if row.get("avg_units") is not None else row["close_units"]
        if reference != expected:
            _fail("分钟参考价格偏离封存成交行情")
        if participation_cap_ppm is None:
            _fail("分钟容量缺少正式参与率")
        capacity = integer(row["volume"], "执行行情成交量", minimum=0) * integer(participation_cap_ppm, "参与率", minimum=1) // 1_000_000
        if verify_minute_cash_bar_suspended(
            rule_bundle, asset_class=asset_class, instrument_id=code,
            session=date_value(observation["session"], "行情会话"),
            bar_start=start, bar_end=row["bar_end"], available_at=at,
        ):
            capacity = 0
        if observation["visible_capacity"] != capacity:
            _fail("分钟容量偏离封存行情与参与率")
        observation["_market_lower"] = row.get("lower_limit_units", row.get("low_limit_units"))
        observation["_market_upper"] = row.get("upper_limit_units", row.get("high_limit_units"))


def _verify_reservations(context, canonical, commands, cancels, fills, observations_by_fill, observations, command_rules, scale, cash_scale, session_ends, verified_account_events, account_context, verified_external_cashflow_events=(), credit_oracle=None):
    opening = None
    for row in canonical["cash"]:
        units = integer(row["opening_cash_units"], "期初现金", minimum=0)
        if opening is not None and units != opening:
            _fail("期初现金身份漂移")
        opening = units
    initial = context.get("initial_cash_units", opening)
    if initial is None or (opening is not None and initial != opening):
        _fail("显式订单期初现金未绑定正式现金表")
    cash = integer(initial, "显式订单期初现金", minimum=0)
    reserves, position_reserves, held, sellable, unsettled = {}, {}, defaultdict(int), defaultdict(int), defaultdict(int)
    for row in _rows(context.get("initial_positions", []), "initial_positions"):
        key = row.get("instrument_hash")
        if not isinstance(key, str) or key in held:
            _fail("期初持仓缺少唯一标的身份")
        held[key] = integer(row["quantity"], "期初持仓", minimum=0)
        sellable[key] = integer(row["sellable_quantity"], "期初可卖持仓", minimum=0)
        if sellable[key] > held[key]:
            _fail("期初可卖持仓超过总持仓")
        unsettled[key] = held[key] - sellable[key]
    cash_claims, share_claims = {}, {}
    pending_successors, successor_claims = {}, {}
    if account_context is not None:
        opening_snapshot = account_context["opening_snapshot"]
        cash = integer(opening_snapshot["cash"]["available_units"], "账户期初可用现金", minimum=0)
        cash_claims = {row["claim_id"]: row["cash_units"] for row in opening_snapshot["receivables"]}
        lots = {row["lot_id"]: row for row in opening_snapshot["lots"]}
        share_claims = {row["entitlement_id"]: (lots[row["lot_id"]]["instrument_hash"], row["quantity"])
                        for row in opening_snapshot["position_entitlements"]}
    financing_limits = {} if credit_oracle is None else {key: row["financing_limit_units"] for key, row in credit_oracle.allocations.items()}
    own_cash_limits = {} if credit_oracle is None else {key: row["own_cash_limit_units"] for key, row in credit_oracle.allocations.items()}
    withdrawal_reserves = {}
    unwithdrawable = 0
    payables = {} if account_context is None else {row["claim_id"]: row["cash_units"] for row in account_context["opening_snapshot"]["payables"]}
    if account_context is not None:
        for row in account_context["opening_snapshot"]["tax_assessments"]:
            payables[row["assessment_id"]] = row["assessed_units"] - row["collected_units"]
    order_summaries = {str(row["order_id"]): row for row in canonical["orders"]}
    previous, active_session = None, None
    auto_settled = {}
    opportunity_ends = {}
    seen, seen_events, released_at = set(), set(), {}
    reserve_seen = set()
    cumulative_quantity, fee_numerator, commission_paid = defaultdict(int), defaultdict(int), defaultdict(int)
    last_price = {}
    admission, ioc_ended = {}, set()
    for event in _event_schedule(context, commands, observations, verified_account_events, verified_external_cashflow_events,
                                 () if credit_oracle is None else credit_oracle.verified_events):
        at = aware_datetime(event["effective_time"], "调度项时点")
        session = date_value(event["session"], "调度项会话")
        if previous is not None and (at < previous or active_session is not None and session < active_session):
            _fail("预占、权益或成交事件顺序倒置")
        previous = at
        if active_session is not None and session != active_session:
            auto_settled = {}
            if account_context is None:
                for key in list(unsettled):
                    locked = sum(quantity for instrument, quantity in (*share_claims.values(), *successor_claims.values())
                                 if instrument == key)
                    quantity = unsettled[key] - locked
                    if quantity < 0:
                        _fail("待上市权益超过未结算持仓")
                    sellable[key] += quantity
                    unsettled[key] -= quantity
                    auto_settled[key] = quantity
        active_session = session
        if event["kind"] == "_credit":
            raw = event["event"]
            credit_values = _payload(raw)
            if raw["kind"] == "credit_repayment":
                cash -= credit_values["cash_units"]
                unwithdrawable = max(0, unwithdrawable - credit_values["cash_units"])
            elif raw["kind"] == "credit_sale_settled":
                if credit_values.get("cash_already_settled"):
                    if credit_values["claim_id"] in cash_claims:
                        _fail("换股现金尚未到账，不得偿还融资")
                    cash -= credit_values["cash_units"]
                else:
                    cash += cash_claims.pop(credit_values["claim_id"]) - credit_values["cash_units"]
            if cash < sum(reserves.values()):
                _fail("信用偿还挪用其他订单预占")
            continue
        if event["kind"] == "_external":
            raw = event["event"]
            values = _payload(raw)
            if raw["event_id"] in seen_events:
                _fail("外部资金事件重复消费")
            seen_events.add(raw["event_id"])
            identity = values.get("cashflow_id")
            units = integer(values["cash_units"], "外部资金金额", minimum=1)
            if raw["kind"] == "external_cashflow_settlement":
                if units != unwithdrawable:
                    _fail("卖出资金结算与尚未可提款余额不符")
                unwithdrawable = 0
            elif raw["kind"] == "external_cashflow_reserved":
                if identity in withdrawal_reserves or units > max(0, cash - sum(reserves.values()) - unwithdrawable - sum(payables.values())):
                    _fail("出金申请挪用他单预占、未结算卖款或应付款")
                cash -= units
                withdrawal_reserves[identity] = units
            elif raw["kind"] == "external_cashflow":
                own = withdrawal_reserves.pop(identity, 0)
                if values["direction"] == "deposit":
                    if own:
                        _fail("入金携带出金预占")
                    if values["status"] == "settled":
                        cash += units
                elif values["status"] == "settled":
                    if own != units or units > max(0, cash - sum(reserves.values()) + own - unwithdrawable - sum(payables.values())):
                        _fail("实际出金违反本笔冻结或他单预占隔离")
                elif own:
                    if own != units:
                        _fail("失败或取消释放金额不是本笔出金冻结")
                    cash += own
            else:
                _fail("外部资金事件类型无效")
            if raw["available_cash_units"] != cash - sum(reserves.values()) or raw["withdrawal_reserved_units"] != sum(withdrawal_reserves.values()):
                _fail("外部资金与订单合并回放的现金或冻结余额不符")
            if cash < sum(reserves.values()):
                _fail("出金释放或生效改变了他单预占")
            continue
        if event["kind"] == "_account":
            raw = event["event"]
            if raw["event_id"] in seen_events:
                _fail("已验证账户事件重复")
            seen_events.add(raw["event_id"])
            values = _payload(raw)
            key = values.get("instrument_hash")
            if raw["kind"] == "corporate_action":
                cash += integer(values.get("cash_delta_units", 0), "公司行动可用现金变化")
                if values.get("cancel_position_entitlements"):
                    share_claims = {identity: item for identity, item in share_claims.items() if item[0] != key}
                quantity = integer(values.get("position_entitlement_quantity", 0), "待上市股份", minimum=0)
                sellable_delta = integer(values.get("sellable_delta", 0), "可卖股份变化")
                unsettled_delta = integer(values.get("unsettled_delta", 0), "未结算股份变化") + quantity
                held[key] += sellable_delta + unsettled_delta
                sellable[key] += sellable_delta
                unsettled[key] += unsettled_delta
                if quantity:
                    share_claims[values["entitlement_id"]] = key, quantity
                if values.get("cash_receivable_units"):
                    cash_claims[values["receivable_id"]] = values["cash_receivable_units"]
            elif raw["kind"] == "security_conversion":
                key = values["old_instrument_hash"]
                quantity = integer(values["old_quantity"], "换股注销数量", minimum=0)
                if held[key] != quantity or any(value for oid, value in position_reserves.items()
                                               if commands[oid]["_hash"] == key):
                    _fail("换股注销持仓与已释放订单预占不符")
                held[key] = sellable[key] = unsettled[key] = 0
                share_claims = {identity: item for identity, item in share_claims.items() if item[0] != key}
                successor_claims = {identity: item for identity, item in successor_claims.items() if item[0] != key}
                auto_settled.pop(key, None)
                quantity = integer(values["successor_quantity"], "待登记后继股份", minimum=0)
                if quantity:
                    pending_successors[values["successor_entitlement_id"]] = (values["new_instrument_hash"], quantity)
                amount = integer(values["cash_receivable_units"], "换股现金应收", minimum=0)
                if amount:
                    cash_claims[values["receivable_id"]] = amount
            elif raw["kind"] == "successor_registered":
                key, quantity = pending_successors.pop(values["entitlement_id"])
                if key != values["instrument_hash"] or quantity != values["quantity"]:
                    _fail("后继登记与已验证待登记权益不符")
                held[key] += quantity
                unsettled[key] += quantity
                successor_claims[values["conversion_id"]] = key, quantity
            elif raw["kind"] == "settlement":
                if values.get("receivable_id") is not None:
                    cash += cash_claims.pop(values["receivable_id"])
                if values.get("entitlement_id") is not None:
                    instrument, quantity = share_claims.pop(values["entitlement_id"])
                    unsettled[instrument] -= quantity
                    sellable[instrument] += quantity
                quantity = integer(values.get("quantity", 0), "账户结算数量", minimum=0)
                if key is not None and quantity:
                    conversion = raw.get("parent_id")
                    if conversion in successor_claims:
                        instrument, locked = successor_claims[conversion]
                        if instrument != key or quantity > locked:
                            _fail("后继可卖结算超过本次已登记待解禁权益")
                        if quantity == locked:
                            del successor_claims[conversion]
                        else:
                            successor_claims[conversion] = key, locked - quantity
                    already = min(quantity, auto_settled.get(key, 0))
                    auto_settled[key] = auto_settled.get(key, 0) - already
                    unsettled[key] -= quantity - already
                    sellable[key] += quantity - already
                cash += integer(values.get("cash_units", 0), "账户结算现金", minimum=0)
            elif raw["kind"] == "tax_assessed":
                payables[values["assessment_id"]] = integer(values["tax_units"], "账户应付税", minimum=0)
            elif raw["kind"] == "tax_collected":
                cash -= integer(values["cash_units"], "账户实际扣税", minimum=0)
                unwithdrawable = max(0, unwithdrawable - values["cash_units"])
                if values.get("assessment_id") in payables:
                    payables[values["assessment_id"]] -= values["cash_units"]
            if cash < sum(reserves.values()) or any(sellable[key] < 0 or unsettled[key] < 0 for key in held):
                _fail("已验证权益变化与订单预占可用额不守恒")
            continue
        if event["kind"] == "_submit":
            command = commands[event["order_id"]]
            parameters = command_rules[command["command_id"]]["_parameters"]
            if command["side"] == "buy":
                reference = _price(command["reference_price"], scale, "提交预占基准")
                estimate = (_price(command["limit_price"], scale, "提交限价") if command["order_type"] == "limit"
                            else _execution_price(command, reference, parameters, scale))
                amount = _amount(estimate, command["quantity"], scale, cash_scale)
                needed = amount - min(amount, financing_limits.get(command["order_id"], 0)) + _fee(amount, 0, 0, parameters, "buy")[0]
                accepted = command["funds_policy"] == "resize" or needed <= cash - sum(reserves.values())
            else:
                available = sellable[command["_hash"]] - sum(value for oid, value in position_reserves.items()
                    if commands[oid]["_hash"] == command["_hash"])
                reference = _price(command["reference_price"], scale, "卖单费用基准")
                estimate = (_price(command["limit_price"], scale, "卖单限价") if command["order_type"] == "limit"
                            else _execution_price(command, reference, parameters, scale))
                amount = _amount(estimate, command["quantity"], scale, cash_scale)
                fee = _fee(amount, 0, 0, parameters, "sell")[0]
                accepted = command["quantity"] <= available and max(0, fee - amount) <= cash - sum(reserves.values())
            actual = any(item["kind"] in {"cash_reserved", "position_reserved"}
                         and _payload(item).get("order_id") == command["order_id"]
                         and _payload(item).get("action") != "release"
                         and aware_datetime(item["effective_time"], "受理预占时点") == command["_submitted"]
                         for item in context["events"])
            if accepted != actual:
                _fail("订单受理或拒绝未按可用现金和持仓独立重算")
            admission[command["order_id"]] = accepted
            continue
        if event["kind"] == "_opportunity":
            observation = event["observation"]
            _verify_opportunity(observation, commands, cancels, fills, cumulative_quantity,
                admission, released_at, ioc_ended, cash, reserves, position_reserves, sellable,
                fee_numerator, commission_paid, scale, cash_scale, opportunity_ends, order_summaries, financing_limits, own_cash_limits)
            continue
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id or event_id in seen_events:
            _fail("金融事件身份缺失或重复")
        seen_events.add(event_id)
        at = aware_datetime(event.get("effective_time"), "金融事件时点")
        session = date_value(event.get("session"), "金融事件会话")
        values = _payload(event)
        order_id = values.get("order_id")
        command = commands.get(order_id)
        if command is None or at < command["_submitted"] or session != command["_session"]:
            _fail("金融事件缺少有效命令或会话绑定")
        kind = event.get("kind")
        closing = session_ends[session.isoformat()]
        if at > closing or event.get("parent_id") != order_id:
            _fail("金融事件越过真实会话结束或父订单不一致")
        bindings = [row for row in observations.values() if row["_hash"] == command["_hash"] and row["_time"] == at]
        binding = command_rules[command["command_id"]] if at == command["_submitted"] else (bindings[0] if len(bindings) == 1 else None)
        permitted = {command["_source_hash"]}
        if binding is not None:
            permitted.add(binding["rules_identity_hash"])
        if event.get("rule_hash") not in permitted:
            _fail("金融事件规则身份没有正式来源")
        if kind in {"cash_reserved", "position_reserved"}:
            own = reserves.get(order_id, 0)
            position_own = position_reserves.get(order_id, 0)
            if values.get("action") == "release":
                if order_id in released_at:
                    _fail("终态重复释放预占")
                if order_id not in reserves and order_id not in position_reserves:
                    _fail("释放事件没有本单预占")
                valid_terminal = (cumulative_quantity[order_id] == command["quantity"]
                    or any(cancel["_submitted"] == at for cancel in cancels.get(order_id, ()))
                    or at == closing or opportunity_ends.get(order_id) == at)
                if not valid_terminal:
                    _fail("提前释放没有真实资金不足、IOC、成交完成或撤单依据")
                reserves.pop(order_id, None)
                position_reserves.pop(order_id, None)
                released_at[order_id] = at
                continue
            if order_id in released_at:
                _fail("释放终态预占后重新占用资金或持仓")
            if binding is None:
                _fail("预占更新缺少当时可见规则")
            parameters = binding["_parameters"]
            remaining = command["quantity"] - cumulative_quantity[order_id]
            reserve_seen.add(order_id)
            if kind == "cash_reserved":
                amount = integer(values.get("cash_units"), "订单预占现金", minimum=0)
                other = sum(reserves.values()) - own
                reference = (last_price[order_id] if order_id in last_price else
                             _price(command.get("reference_price"), scale, "预占价格基准"))
                estimate = (_price(command["limit_price"], scale, "预占限价") if command["order_type"] == "limit"
                            else _execution_price(dict(command, order_type="market"), reference, parameters, scale)
                            if order_id not in last_price else reference)
                if estimate is None:
                    _fail("预占没有合法价格基准")
                notional = int((Decimal(estimate * remaining) * Decimal(10) ** (cash_scale - scale)).to_integral_value(rounding=ROUND_HALF_UP))
                expected_fee = _fee(notional, fee_numerator[order_id], commission_paid[order_id], parameters, command["side"])[0] if remaining else 0
                required = notional - min(notional, financing_limits.get(order_id, 0)) + expected_fee
                if command["funds_policy"] == "resize":
                    required = min(required, cash - other)
                if command["side"] == "sell":
                    required = max(0, expected_fee - notional)
                if amount != required:
                    _fail("本单现金预占未按可见价格、余量和已扣佣金重算")
                if amount + other > cash:
                    _fail("现金预占超额或挪用其他订单预占")
                reserves[order_id] = amount
            else:
                key = values.get("instrument_hash")
                if key != command["_hash"]:
                    _fail("订单持仓预占更换标的")
                quantity = integer(values.get("quantity"), "订单预占数量", minimum=0)
                if quantity != remaining or command["side"] != "sell":
                    _fail("持仓预占未绑定本单剩余数量")
                other = sum(value for oid, value in position_reserves.items() if commands[oid]["_hash"] == key) - position_own
                if quantity + other > sellable[key]:
                    _fail("可卖持仓预占超额或挪用其他订单预占")
                position_reserves[order_id] = quantity
        elif kind == "fill":
            fill_id = values.get("fill_id")
            if fill_id is None:
                matches = [identity for identity, fill in fills.items() if fill["order_id"] == order_id
                           and aware_datetime(fill["fill_time"], "fill_time") == at and identity not in seen]
                if len(matches) != 1:
                    _fail("成交事件无法唯一关联规范 fill")
                fill_id = matches[0]
            fill = fills.get(fill_id)
            if fill is None or fill_id in seen or fill["order_id"] != order_id:
                _fail("成交事件重复或没有规范成交")
            if (order_id in released_at or order_id not in reserve_seen
                    or opportunity_ends.get(order_id) is not None and opportunity_ends[order_id] < at):
                _fail("成交没有本单预占或已释放终态预占")
            if aware_datetime(fill["fill_time"], "fill_time") != at:
                _fail("成交事件时点偏离规范成交")
            for name in ("quantity", "notional_units", "fee_units", "side", "instrument_hash"):
                if values.get(name) != fill.get(name):
                    _fail("成交事件金额、费用或持仓与规范成交不一致")
            if fill.get("source_fill_hash") != typed_canonical_hash(dict(event)):
                _fail("规范成交没有绑定真实金融事件")
            seen.add(fill_id)
            quantity, notional, fee = int(fill["quantity"]), int(fill["notional_units"]), int(fill["fee_units"])
            key = command["_hash"]
            parameters = observations_by_fill[fill_id]["_parameters"]
            cumulative_quantity[order_id] += quantity
            fee_numerator[order_id] += notional * parameters["commission_ppm"]
            commission_paid[order_id] = max(commission_paid[order_id], parameters["min_commission_units"], ceil_ratio(fee_numerator[order_id], 1_000_000))
            last_price[order_id] = fill["execution_price_units"]
            if command["side"] == "buy":
                borrowed = 0 if credit_oracle is None else values.get("credit_drawdown", {}).get("principal_units", 0)
                if borrowed != min(notional, financing_limits.get(order_id, 0)):
                    _fail("融资成交与独立分配额度不符")
                financing_limits[order_id] = financing_limits.get(order_id, 0) - borrowed
                if order_id in own_cash_limits:
                    own_cash_limits[order_id] -= notional - borrowed
                debit = notional + fee - borrowed
                other = sum(value for oid, value in reserves.items() if oid != order_id)
                if debit > cash - other:
                    _fail("成交购买力挪用其他订单资金预占")
                cash -= debit
                unwithdrawable = max(0, unwithdrawable - debit)
                reserves[order_id] = max(reserves.get(order_id, 0) - debit, 0)
                held[key] += quantity
                unsettled[key] += quantity
            else:
                if max(0, fee - notional) > cash - sum(value for oid, value in reserves.items() if oid != order_id):
                    _fail("卖单费用缺口挪用其他订单现金预占")
                other = sum(value for oid, value in position_reserves.items() if oid != order_id and commands[oid]["_hash"] == key)
                if quantity > sellable[key] - other:
                    _fail("成交挪用其他订单可卖持仓或违反 T+1")
                if not _quantity_allowed(quantity, "sell", observations_by_fill[fill_id]["_parameters"], sellable[key] - other):
                    _fail("卖出成交违反零股规则")
                reserves[order_id] = max(reserves.get(order_id, 0) - fee, 0)
                held[key] -= quantity
                sellable[key] -= quantity
                position_reserves[order_id] = max(position_reserves.get(order_id, 0) - quantity, 0)
                restricted = 0 if credit_oracle is None else values.get("credit_sale", {}).get("cash_units", 0)
                if restricted:
                    cash_claims[values["credit_sale"]["claim_id"]] = restricted
                cash += notional - fee - restricted
                unwithdrawable += max(0, notional - fee - restricted)
        elif kind == "credit_reserved" and credit_oracle is not None:
            if event not in credit_oracle.verified_events:
                _fail("显式融资预占没有独立信用复核")
            if values.get("action") == "release":
                financing_limits[order_id] = 0
                released_at[order_id] = at
            else:
                financing_limits[order_id] = values["reservation"]["principal_units"]
                own_cash_limits[order_id] = values["reservation"]["own_cash_units"]
        elif kind == "settlement":
            key = values.get("instrument_hash")
            quantity = integer(values.get("quantity"), "T+0 结算数量", minimum=1)
            candidates = [row for row in observations.values() if row["_hash"] == key and row["_time"] == at]
            if (len(candidates) != 1 or candidates[0]["_parameters"].get("settlement_days") != 0
                    or values.get("cash_units") != 0 or quantity > unsettled[key]):
                _fail("T+0 结算没有当时规则或持仓依据")
            unsettled[key] -= quantity
            sellable[key] += quantity
        else:
            _fail("显式订单金融事件包含未支持的账本操作")
        if min(cash - sum(reserves.values()), cash) < 0:
            _fail("预占与成交现金不守恒")
    if withdrawal_reserves:
        _fail("期末出金冻结未释放")
    if seen != set(fills):
        _fail("规范成交缺少金融事件")
    if any(reserves.values()) or any(position_reserves.values()):
        _fail("订单终结后仍有未释放预占")
    for order_id, command in commands.items():
        terminal = released_at.get(order_id)
        cancellations = cancels.get(order_id, ())
        if cancellations and terminal is not None and terminal > cancellations[0]["_submitted"]:
            _fail("撤单没有及时释放本单预占")
    for order_id, command in commands.items():
        if order_id in reserves or order_id in position_reserves:
            if order_id not in released_at and cumulative_quantity[order_id] < command["quantity"]:
                _fail("活动余量缺少终态预占释放")
    # 规范现金与持仓/NAV 的全量闭合由公共 canonical 和现货权益 oracle 继续复核。


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return normalized(value)


def explicit_order_session_ends(context, *, session_policy_bundle=None, frequency):
    """从原始日夜时段重建会话终点，再核对执行器声明。"""
    declared = context.get("session_ends")
    if not isinstance(declared, Mapping):
        _fail("缺少显式交易会话结束声明")
    expected = {}
    if frequency == "daily" and not context["commands"]:
        return {key: _daily_empty_session_end(key, value) for key, value in declared.items()}
    command_rows = _rows(context["commands"], "commands")
    instruments = {item["instrument"]["instrument_id"]: item["instrument"] for item in command_rows}
    records = [{"trading_date": key, "instrument": instrument, "submitted_at": None}
               for key in declared for instrument in instruments.values()]
    records.extend(command_rows)
    for command in records:
        session = date_value(command["trading_date"], "交易会话")
        code = command["instrument"]["instrument_id"]
        if frequency == "minute":
            if session_policy_bundle is None:
                _fail("分钟 DAY 到期缺少原始 session policy bundle")
            policies = [item for item in session_policy_bundle.policies if item.instrument.instrument_id == code
                        and item.effective_from <= session <= item.effective_to and session in item.trading_dates]
            if len(policies) != 1:
                _fail("订单没有唯一正式日夜会话")
            segments = [item.build(session) for item in policies[0].segments if item.phase in {"day", "night"}]
            if not segments:
                _fail("订单会话缺少交易时段")
            closing = max(item.ends_at for item in segments)
        else:
            if command["instrument"]["asset_class"] not in {"cn_stock", "cn_etf"}:
                _fail("日频会话缺少正式资产时段来源")
            closing = datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai"))
        key = session.isoformat()
        if key in expected and expected[key] != closing:
            _fail("同账户订单的会话收尾时点不同")
        expected[key] = closing
        if command["submitted_at"] is not None and aware_datetime(command["submitted_at"], "提交时点") > closing:
            _fail("订单提交晚于真实交易会话结束")
    if set(declared) != set(expected) or any(aware_datetime(declared[key], "声明会话终点") != value for key, value in expected.items()):
        _fail("DAY 到期声明偏离原始日夜会话来源")
    return expected


def _daily_empty_session_end(session, declared):
    closing = datetime.combine(date_value(session, "零订单交易会话"), time(15), ZoneInfo("Asia/Shanghai"))
    if aware_datetime(declared, "零订单会话终点") != closing:
        _fail("零订单会话终点偏离日频现货收盘")
    return closing


def _verify_cancel_results(context, commands, canonical, session_ends, observations):
    requests = [item for item in context["commands"] if item["action"] == "cancel"]
    results = {}
    for row in _rows(context.get("cancel_results", []), "cancel_results"):
        if set(row) != {"command_id", "order_id", "status", "reason"} or row["command_id"] in results:
            _fail("撤单结果 schema 无效或重复")
        results[row["command_id"]] = row
    if set(results) != {item["command_id"] for item in requests}:
        _fail("撤单结果与完整撤单命令集合不一致")
    fills = defaultdict(list)
    for row in canonical["fills"]:
        fills[str(row["order_id"])].append((aware_datetime(row["fill_time"], "撤单前成交时点"), int(row["quantity"])))
    accepted, releases = set(), defaultdict(list)
    for event in context["events"]:
        values = _payload(event)
        order_id = values.get("order_id")
        if order_id not in commands:
            continue
        at = aware_datetime(event["effective_time"], "撤单相关账本时点")
        if event["kind"] in {"cash_reserved", "position_reserved"}:
            if values.get("action") == "release":
                releases[order_id].append(at)
            elif at == commands[order_id]["_submitted"]:
                accepted.add(order_id)
    cancelled, valid = set(), {}
    for request in requests:
        order_id = request["order_id"]
        command = commands[order_id]
        at = aware_datetime(request["submitted_at"], "撤单请求时点")
        filled = sum(quantity for moment, quantity in fills[order_id] if moment < at)
        active = (order_id in accepted and order_id not in cancelled and filled < command["quantity"]
                  and at <= session_ends[command["_session"].isoformat()]
                  and not any(moment < at for moment in releases[order_id]))
        if command["time_in_force"] == "IOC":
            eligible = [row["_time"] for row in observations.values() if row["_hash"] == command["_hash"]
                        and row["_start"] >= command["_submitted"] and row["_time"] > command["_submitted"]
                        and date_value(row["session"], "IOC 会话") == command["_session"]]
            if eligible and min(eligible) < at:
                active = False
        expected = {"command_id": request["command_id"], "order_id": order_id,
                    "status": "cancelled" if active else "rejected",
                    "reason": "explicit_cancel" if active else "order_not_active"}
        if results[request["command_id"]] != expected:
            _fail("撤单结果与当时真实活动或终态不一致")
        if active:
            cancelled.add(order_id)
            valid.setdefault(order_id, []).append(dict(request, _submitted=at))
            if at not in releases[order_id]:
                _fail("有效撤单没有释放本单预占")
        else:
            if any(moment == at for moment in releases[order_id]):
                _fail("已终态撤单不能改变账本预占")
    return valid


def _amount(price, quantity, scale, cash_scale):
    if price is None:
        _fail("没有合法执行价格")
    return int((Decimal(price * quantity) * Decimal(10) ** (cash_scale - scale)).to_integral_value(rounding=ROUND_HALF_UP))


def _event_schedule(context, commands, observations, verified_account_events=(), verified_external_cashflow_events=(), verified_credit_events=()):
    rows = []
    command_ordinal = {item["order_id"]: index for index, item in enumerate(commands.values())}
    observation_ordinal = {(item["_hash"], item["_time"]): index for index, item in enumerate(observations.values())}
    for index, event in enumerate(_rows(context["events"], "events")):
        at = aware_datetime(event["effective_time"], "金融事件时点")
        command = commands.get(_payload(event).get("order_id"))
        if command is not None and at == command["_submitted"]:
            stage, ordinal = 0, command_ordinal[command["order_id"]]
        elif command is not None and (command["_hash"], at) in observation_ordinal:
            stage, ordinal = 1, observation_ordinal[(command["_hash"], at)]
        else:
            stage, ordinal = 2, 0
        rows.append(((at, stage, ordinal, index), event))
    for command in commands.values():
        rows.append(((command["_submitted"], 0, command_ordinal[command["order_id"]], -1),
                     {"kind": "_submit", "order_id": command["order_id"],
                      "effective_time": command["submitted_at"], "session": command["trading_date"]}))
    for index, observation in enumerate(observations.values()):
        rows.append(((observation["_time"], 1, index, -1), {"kind": "_opportunity", "observation": observation,
            "effective_time": observation["event_time"], "session": observation["session"]}))
    # 非订单事件由调用方保留原账户或权益 oracle 门禁；同一时点先到账，再受理订单。
    for index, event in enumerate(verified_account_events):
        if event["kind"] not in {"corporate_action", "security_conversion", "successor_registered", "settlement", "tax_assessed", "tax_collected"} or "order_id" in _payload(event):
            continue
        at = aware_datetime(event["effective_time"], "已验证账户事件时点")
        rows.append(((at, -1, index, 0), {"kind": "_account", "event": event,
            "effective_time": event["effective_time"], "session": event["session"]}))
    for index, event in enumerate(verified_external_cashflow_events):
        at = aware_datetime(event["effective_time"], "已验证外部资金事件时点")
        rows.append(((at, -0.5, index, 0), {"kind": "_external", "event": event,
            "effective_time": event["effective_time"], "session": event["session"]}))
    for index, event in enumerate(verified_credit_events):
        if event["kind"] in {"credit_repayment", "credit_sale_settled"}:
            at = aware_datetime(event["effective_time"], "已验证信用事件时点")
            rows.append(((at, -0.75, index, 0), {"kind": "_credit", "event": event,
                "effective_time": event["effective_time"], "session": event["session"]}))
    return (event for _, event in sorted(rows, key=lambda item: item[0]))


def _floor_quantity(capacity, side, parameters, sellable):
    lot = parameters.get("lot_size", parameters.get("buy_lot_shares", parameters.get("buy_lot_units")))
    minimum = integer(parameters.get(f"{side}_min_quantity", lot), "最小数量", minimum=1)
    step = integer(parameters.get(f"{side}_quantity_step", lot), "数量步长", minimum=1)
    maximum = parameters.get(f"{side}_max_quantity")
    cap = capacity if maximum is None else min(capacity, maximum)
    if side == "sell":
        cap = min(cap, sellable)
    normal = 0 if cap < minimum else minimum + (cap - minimum) // step * step
    if side != "sell" or parameters.get("sell_remainder_allowed") is not True:
        return normal
    if sellable < minimum:
        return sellable if cap >= sellable else 0
    remainder = (sellable - minimum) % step
    odd = 0 if not remainder or cap < remainder else (cap - remainder) // step * step + remainder
    return max(normal, odd)


def _verify_opportunity(observation, commands, cancels, fills, cumulative, admission, released,
                        ioc_ended, cash, reserves, position_reserves, sellable,
                        numerators, commissions, scale, cash_scale, opportunity_ends, order_summaries, financing_limits=None, own_cash_limits=None):
    financing_limits = {} if financing_limits is None else financing_limits
    own_cash_limits = {} if own_cash_limits is None else own_cash_limits
    capacity = observation["visible_capacity"]
    capacity = 2**62 if capacity is None else capacity
    at = observation["_time"]
    working_cash = cash
    working_reserves = dict(reserves)
    working_position_reserves = dict(position_reserves)
    working_sellable = dict(sellable)
    parameters = observation["_parameters"]
    candidates = sorted((item for item in commands.values() if item["_hash"] == observation["_hash"]),
                        key=lambda item: item["_clock"])
    for command in candidates:
        order_id = command["order_id"]
        if (not admission.get(order_id) or order_id in released or order_id in ioc_ended
                or command["_submitted"] > observation["_start"] or command["_submitted"] >= at
                or command["_session"] != date_value(observation["session"], "观察会话")
                or opportunity_ends.get(order_id) is not None and opportunity_ends[order_id] < at
                or cumulative[order_id] >= command["quantity"]
                or any(cancel["_submitted"] <= at for cancel in cancels.get(order_id, ()))):
            continue
        remaining = command["quantity"] - cumulative[order_id]
        price = _execution_price(command, observation["_reference"], parameters, scale)
        available = working_sellable.get(command["_hash"], 0) - sum(value for oid, value in working_position_reserves.items()
            if oid != order_id and commands[oid]["_hash"] == command["_hash"])
        quantity = 0
        rejected = False
        tradable = parameters.get("paused") is not True and parameters.get("trading_status") != "suspended"
        if price is not None and parameters.get("price_limit_mode", "bounded") == "bounded":
            lower, upper = _price_bounds(parameters, observation)
            tradable = tradable and (price < upper if command["side"] == "buy" else price > lower)
        quantity_allowed = _quantity_allowed(remaining, command["side"], parameters, available)
        if price is not None and tradable and quantity_allowed:
            quantity = _floor_quantity(min(remaining, capacity), command["side"], parameters, min(available, remaining))
            if command["side"] == "buy" and quantity:
                budget = working_cash - sum(value for oid, value in working_reserves.items() if oid != order_id)
                def debit(count):
                    amount = _amount(price, count, scale, cash_scale)
                    if order_id in own_cash_limits and amount > financing_limits.get(order_id, 0) + own_cash_limits[order_id]:
                        return budget + 1
                    return amount - min(amount, financing_limits.get(order_id, 0)) + _fee(amount, numerators[order_id], commissions[order_id], parameters, "buy")[0] if count else 0
                if debit(quantity) > budget:
                    if command["funds_policy"] == "reject":
                        quantity = 0
                        rejected = True
                    else:
                        lot = parameters.get("lot_size", parameters.get("buy_quantity_step"))
                        low, high = 0, quantity // lot
                        while low < high:
                            middle = (low + high + 1) // 2
                            if debit(middle * lot) <= budget:
                                low = middle
                            else:
                                high = middle - 1
                        quantity = low * lot
                        if quantity and not _quantity_allowed(quantity, "buy", parameters, 0):
                            quantity = 0
        if command["side"] == "sell" and quantity:
            amount = _amount(price, quantity, scale, cash_scale)
            fee = _fee(amount, numerators[order_id], commissions[order_id], parameters, "sell")[0]
            if max(0, fee-amount) > working_cash - sum(value for oid, value in working_reserves.items() if oid != order_id):
                quantity = 0
                rejected = command["funds_policy"] == "reject"
        if command["time_in_force"] == "IOC" and cumulative[order_id] == 0 and quantity == 0 and not rejected:
            # 可卖余额已经扣除他单预占；余股是否合法不能仅由普通数量格点判断。
            expected_reason = ("invalid_lot"
                if price is not None and capacity > 0 and not quantity_allowed
                else "ioc_remainder_cancelled")
            if order_summaries[order_id].get("terminal_reason") != expected_reason:
                _fail("IOC 零成交原因与当时可卖余额及执行机会不符")
        actual = [row for row in fills.values() if row["order_id"] == order_id
                  and aware_datetime(row["fill_time"], "合格机会成交时点") == at]
        if sum(row["quantity"] for row in actual) != quantity:
            _fail("零成交、部分成交或资金不足数量偏离合格执行机会重算")
        if quantity:
            amount = _amount(price, quantity, scale, cash_scale)
            fee = _fee(amount, numerators[order_id], commissions[order_id], parameters, command["side"])[0]
            capacity -= quantity
            if command["side"] == "buy":
                debit = amount + fee - min(amount, financing_limits.get(order_id, 0))
                working_cash -= debit
                own = max(working_reserves.get(order_id, 0) - debit, 0)
                working_reserves[order_id] = own
            else:
                if max(0, fee - amount) > working_cash - sum(value for oid, value in working_reserves.items() if oid != order_id):
                    _fail("卖单费用缺口挪用其他订单现金预占")
                working_cash += amount - fee
                working_sellable[command["_hash"]] = working_sellable.get(command["_hash"], 0) - quantity
                working_position_reserves[order_id] = max(working_position_reserves.get(order_id, 0) - quantity, 0)
                working_reserves[order_id] = max(working_reserves.get(order_id, 0) - fee, 0)
        if quantity and cumulative[order_id] + quantity < command["quantity"] and command["time_in_force"] == "DAY":
            remaining_amount = _amount(price, remaining-quantity, scale, cash_scale)
            after_numerator = numerators[order_id] + amount*parameters["commission_ppm"]
            after_commission = _fee(amount, numerators[order_id], commissions[order_id], parameters, command["side"])[1]
            next_fee = _fee(remaining_amount, after_numerator, after_commission, parameters, command["side"])[0]
            required = remaining_amount + next_fee if command["side"] == "buy" else max(0, next_fee-remaining_amount)
            budget = working_cash - sum(value for oid, value in working_reserves.items() if oid != order_id)
            if command["side"] == "buy" and command["funds_policy"] == "resize":
                required = min(required, budget)
            if required > budget:
                rejected = True
            else:
                working_reserves[order_id] = required
        if rejected or command["time_in_force"] == "IOC" or cumulative[order_id] + quantity == command["quantity"]:
            opportunity_ends[order_id] = at
            working_reserves.pop(order_id, None)
            working_position_reserves.pop(order_id, None)
            if command["time_in_force"] == "IOC":
                ioc_ended.add(order_id)


def _verify_opening_positions(context, account_context):
    declared = _rows(context.get("initial_positions", []), "期初持仓")
    if account_context is None:
        if declared:
            _fail("期初持仓缺少正式账户来源")
        return
    opening = account_context.get("opening_snapshot")
    if not isinstance(opening, Mapping):
        _fail("期初持仓缺少账户快照")
    start = aware_datetime(opening["started_at"], "账户开始时点")
    expected = {}
    for lot in _rows(opening["lots"], "账户期初批次"):
        key = lot["instrument_hash"]
        count = integer(lot["quantity"], "期初批次数量", minimum=0)
        row = expected.setdefault(key, {"instrument_hash": key, "quantity": 0, "sellable_quantity": 0})
        row["quantity"] += count
        if aware_datetime(lot["sellable_at"], "期初批次可卖时点") <= start:
            row["sellable_quantity"] += count
    if sorted(declared, key=lambda item: item["instrument_hash"]) != sorted(expected.values(), key=lambda item: item["instrument_hash"]):
        _fail("显式期初持仓未绑定原始账户批次")


__all__ = ["EXPLICIT_ORDER_CONTEXT_VERSION", "verify_explicit_order_execution", "explicit_order_session_ends"]
