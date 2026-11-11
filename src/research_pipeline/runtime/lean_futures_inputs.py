"""从已冻结历史日历和规则投影期货输入，不维护交易账户。"""
from __future__ import annotations

from collections.abc import Mapping
from bisect import bisect_right
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import json
from zoneinfo import ZoneInfo

from research_pipeline.domain.minute_rule_snapshots import MinuteRuleResolver
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION, SessionCalendarResolver
from research_pipeline.domain.time import require_aware_datetime


ZONE = ZoneInfo("Asia/Shanghai")
SETTLEMENT_AVAILABILITY = "next_session_open_after_1700"
_RULE_KINDS = ("contract_lifecycle", "contract_multiplier", "price_tick", "session",
               "margin", "fee_schedule", "price_limit", "delivery_expiry")


def _instant(value, name):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return require_aware_datetime(parsed, name).astimezone(ZONE)


def _decimal(value, name, *, positive=False):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"期货{name}缺失或不是十进制数") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError(f"期货{name}须为{'正' if positive else '非负'}十进制数")
    return result


class FuturesCalendar:
    """逐合约解析真实会话；交易日不能用行情自然日代替。"""

    def __init__(self, calendar, symbols):
        self.calendar = calendar
        self.symbols = tuple(symbols)
        self.resolver = SessionCalendarResolver(calendar)
        self.metadata = {}
        self.by_key = {}
        self.by_instrument = {}
        for code in self.symbols:
            policies = tuple(item for item in calendar.policies if item.instrument.instrument_id == code)
            if not policies:
                raise ValueError(f"期货合约缺少冻结日历：{code}")
            metadata = {item.instrument for item in policies}
            if len(metadata) != 1:
                raise ValueError(f"期货合约日历元数据不一致：{code}")
            self.metadata[code] = next(iter(metadata))
            sessions = []
            for day in sorted({day for policy in policies for day in policy.trading_dates}):
                # 复用历史选版；新版本删除的休市日不再按旧版本构造会话。
                project = calendar.contract_version == PROJECT_SESSION_BUNDLE_VERSION
                visible = (self.resolver._visible_policies(policies, day, None) if project else
                           tuple(item for item in policies if day in item.trading_dates and item.revision == 1))
                if not visible:
                    continue
                session = self.resolver.resolve_trading_date(self.metadata[code], day, policy_revision=None if project else 1)
                eligible = tuple(item for item in session.segments if item.bar_eligible)
                if not eligible:
                    raise ValueError(f"期货会话缺少可交易小节：{code} {day}")
                self.by_key[code, day] = session
                sessions.append(session)
            self.by_instrument[code] = tuple(sessions)

    def dates_between(self, start_at, end_at):
        return tuple(sorted({day.isoformat() for (code, day), session in self.by_key.items()
                             if min(item.starts_at for item in session.segments if item.bar_eligible) < end_at
                             and max(item.ends_at for item in session.segments if item.bar_eligible) > start_at}))

    def session(self, code, day):
        day = date.fromisoformat(day) if isinstance(day, str) else day
        try:
            return self.by_key[code, day]
        except KeyError as exc:
            raise ValueError(f"期货会话缺失：{code} {day}") from exc

    def next_open(self, code, day, *, after_1700=True):
        day = date.fromisoformat(day) if isinstance(day, str) else day
        session = self.session(code, day)
        cutoff = max(item.ends_at for item in session.segments if item.bar_eligible)
        if after_1700:
            cutoff = max(cutoff, datetime.combine(day, time(17), ZONE))
        return self.resolver.next_session_open(self.metadata[code], day, not_before=cutoff)

    def payload(self, code, day):
        session = self.session(code, day)
        segments = [dict(segment_id=item.segment_id, start_at=item.starts_at.isoformat(),
                         end_at=item.ends_at.isoformat(), phase=item.phase, bar_eligible=item.bar_eligible)
                    for item in session.segments if item.bar_eligible]
        return dict(instrument_id=code, trading_date=session.trading_date.isoformat(),
                    start_at=segments[0]["start_at"], end_at=segments[-1]["end_at"], segments=segments,
                    session_policy_id=session.calendar_policy_id,
                    session_policy_revision=session.calendar_policy_revision)

    def payloads(self, dates):
        days = {date.fromisoformat(day) for day in dates}
        return [self.payload(code, day) for code, day in sorted(self.by_key, key=lambda item: (item[1], item[0]))
                if day in days]


def _resolve(resolver, code, kind, day, at):
    try:
        return resolver.resolve(rule_id=f"rule.cn_futures.{kind}.v1", instrument_id=code,
                                effective_on=day, as_of=at).rule
    except ValueError as exc:
        raise ValueError(f"期货规则缺失或尚不可见：{code} {day} {kind} {at.isoformat()}: {exc}") from exc


def _contract(metadata, selected, *, at, trading_date):
    life = dict(selected["contract_lifecycle"].parameters)
    multiplier = dict(selected["contract_multiplier"].parameters)
    tick = dict(selected["price_tick"].parameters)
    expiry = life.get("last_trade_date")
    deadline = life.get("exit_deadline", dict(selected["delivery_expiry"].parameters).get("exit_deadline"))
    if expiry is None or deadline is None:
        raise ValueError(f"期货合约缺少last_trade_date或exit_deadline：{metadata.instrument_id}")
    expiry = date.fromisoformat(str(expiry))
    listed = date.fromisoformat(str(life.get("listed_date")))
    if not listed <= trading_date <= expiry:
        raise ValueError(f"期货会话不在上市至最后交易日范围：{metadata.instrument_id} {trading_date}")
    deadline = _instant(deadline, "exit_deadline")
    if deadline.date() > expiry or deadline <= at:
        raise ValueError(f"期货退出截止时间无效：{metadata.instrument_id}")
    scale = tick.get("price_scale")
    if type(scale) is not int or not 0 <= scale <= 9:
        raise ValueError(f"期货价格尺度缺失或无效：{metadata.instrument_id}")
    quantity = multiplier.get("contract_multiplier")
    if quantity is None and multiplier.get("quote_unit") == "CNY/kg":
        quantity = multiplier.get("contract_unit_kg")
    if quantity is None:
        raise ValueError(f"期货合约缺少报价到每手金额的明确乘数：{metadata.instrument_id}")
    product = life.get("product_code", multiplier.get("product_code"))
    if not isinstance(product, str) or not product.strip():
        raise ValueError(f"期货合约缺少真实品种product_code：{metadata.instrument_id}")
    tick_value = _decimal(tick.get("price_tick_units"), "price_tick_units", positive=True) / 10 ** scale
    return dict(market=metadata.exchange, product=product,
                listed_date=listed.isoformat(), expiry_date=expiry.isoformat(), multiplier=str(_decimal(quantity, "contract_multiplier", positive=True)),
                price_scale=scale, tick_size=str(tick_value), exit_deadline=deadline.isoformat())


def _fee(parameters, effect):
    rate_key, amount_key = effect + "_fee_rate_ppm", effect + "_fee_per_contract_units"
    rate = parameters.get(rate_key, parameters.get(effect + "_fee_ppm"))
    amount = parameters.get(amount_key, parameters.get(effect + "_fee_units"))
    if rate is None and amount is None:
        raise ValueError(f"期货费用缺少明确{effect}费率或每手费用")
    values = (_decimal(0 if amount is None else amount, amount_key),
              _decimal(0 if rate is None else rate, rate_key))
    if any(value != value.to_integral_value() for value in values):
        raise ValueError("期货费用分金额和ppm须为整数")
    return tuple(int(value) for value in values)


def _rule_parameters(selected, contract):
    margin = dict(selected["margin"].parameters)
    common = margin.get("speculative_initial_margin_ppm")
    long = margin.get("long_margin_ppm", common)
    short = margin.get("short_margin_ppm", common)
    if long is None or short is None:
        raise ValueError("期货缺少多空初始保证金率")
    fees = dict(selected["fee_schedule"].parameters)
    limit = dict(selected["price_limit"].parameters)
    if limit.get("price_scale") != contract["price_scale"]:
        raise ValueError("期货价格限制与合约价格尺度不一致")
    priority = dict(selected["delivery_expiry"].parameters).get("close_priority")
    if priority not in {"today_first", "yesterday_first"}:
        raise ValueError("期货必须显式声明close_priority")
    long, short = (_decimal(long, "long_margin_ppm", positive=True), _decimal(short, "short_margin_ppm", positive=True))
    if any(value != value.to_integral_value() for value in (long, short)):
        raise ValueError("期货保证金ppm须为整数")
    result = dict(long_margin_ppm=int(long), short_margin_ppm=int(short),
                  tick_size=contract["tick_size"], close_priority=priority)
    for effect in ("open", "close", "close_today"):
        amount, rate = _fee(fees, effect)
        result[effect + "_fee_per_contract_units"] = amount
        result[effect + "_fee_rate_ppm"] = rate
    scale = 10 ** contract["price_scale"]
    for output, key in (("lower_limit", "low_limit_units"), ("upper_limit", "high_limit_units")):
        if key not in limit:
            raise ValueError(f"期货规则缺少{key}")
        result[output] = str(_decimal(limit[key], key, positive=True) / scale)
    return result


def validate_roll_plans(plans, contracts):
    """冻结同规格1:1换月声明，执行额度仍由C#实际成交决定。"""
    if not isinstance(plans, (list, tuple)):
        raise ValueError("roll_plans必须为数组")
    result, identities = [], set()
    keys = {"plan_id", "old_instrument_id", "new_instrument_id", "direction", "quantity",
            "available_at", "start_at", "end_at"}
    for plan in plans:
        if not isinstance(plan, Mapping) or set(plan) != keys:
            raise ValueError("换月计划字段不完整")
        plan = dict(plan)
        if not isinstance(plan["plan_id"], str) or not plan["plan_id"] or plan["plan_id"] in identities:
            raise ValueError("换月plan_id必须非空且唯一")
        identities.add(plan["plan_id"])
        old, new = plan["old_instrument_id"], plan["new_instrument_id"]
        if old == new or old not in contracts or new not in contracts:
            raise ValueError("换月两腿须为冻结集合中的不同真实合约")
        if plan["direction"] not in {"long", "short"} or type(plan["quantity"]) is not int or plan["quantity"] <= 0:
            raise ValueError("换月须声明多空方向与正整数手数")
        for name in ("market", "product", "multiplier", "price_scale", "tick_size"):
            if contracts[old][name] != contracts[new][name]:
                raise ValueError("换月仅支持同规格合约1:1手数迁移")
        available, start, end = (_instant(plan[name], name) for name in ("available_at", "start_at", "end_at"))
        if not available <= start < end or end > _instant(contracts[old]["exit_deadline"], "exit_deadline"):
            raise ValueError("换月可见时间、执行窗口或退出截止时间无效")
        for name, value in (("available_at", available), ("start_at", start), ("end_at", end)):
            plan[name] = value.isoformat()
        result.append(plan)
    return sorted(result, key=lambda item: (item["available_at"], item["plan_id"]))


class FuturesInputs:
    """小型会话和规则事实；行情由MarketSource逐日读取。"""

    def __init__(self, scope, symbols, dates, *, calendar=None, roll_plans=()):
        self.scope = scope
        self.calendar = calendar or FuturesCalendar(scope.calendar, symbols)
        self.dates = tuple(dates)
        self.sessions = self.calendar.payloads(self.dates)
        if not self.sessions:
            raise ValueError("期货策略没有冻结交易会话")
        self.sessions_by_day = {day: [] for day in self.dates}
        self._session_starts = {}
        for item in self.sessions:
            day = item["trading_date"]
            self.sessions_by_day[day].append(item)
            start = _instant(item["start_at"], "session start_at")
            self._session_starts[day] = min(start, self._session_starts.get(day, start))
        self.resolver = MinuteRuleResolver(scope.rules)
        self.contracts = {}
        self._rules = {}
        self._settlements = {}
        for item in self.sessions:
            code, day = item["instrument_id"], date.fromisoformat(item["trading_date"])
            at = _instant(item["start_at"], "session start_at")
            end = _instant(item["end_at"], "session end_at")
            selected = {kind: _resolve(self.resolver, code, kind, day, at) for kind in _RULE_KINDS}
            contract = _contract(self.calendar.metadata[code], selected, at=at, trading_date=day)
            if code in self.contracts and self.contracts[code] != contract:
                raise ValueError(f"期货合约规格在窗口内变化，须重新声明：{code} {day}")
            self.contracts[code] = contract
            points = {at}
            points.update(rule.available_at for kind in _RULE_KINDS
                          for rule in self.resolver.candidates(rule_id=f"rule.cn_futures.{kind}.v1",
                                                              instrument_id=code, effective_on=day)
                          if rule.available_at is not None and at < rule.available_at <= end)
            previous = None
            for point in sorted(points):
                selected = {kind: _resolve(self.resolver, code, kind, day, point) for kind in _RULE_KINDS}
                if _contract(self.calendar.metadata[code], selected, at=at, trading_date=day) != contract:
                    raise ValueError(f"期货合约规格在窗口内变化：{code} {day}")
                parameters = _rule_parameters(selected, contract)
                limits = dict(selected["price_limit"].parameters)
                if "reference_price_available_at" not in limits or _instant(limits["reference_price_available_at"],
                        "reference_price_available_at") > point:
                    raise ValueError(f"期货参考结算价在规则事件时尚不可见：{code} {day}")
                if parameters == previous:
                    continue
                sources = sorted({source for rule in selected.values() for source in rule.source_ids})
                self._rules.setdefault(day.isoformat(), []).append(dict(instrument_id=code,
                    effective_from=day.isoformat(), effective_to=day.isoformat(), event_at=point.isoformat(),
                    available_at=max(rule.available_at for rule in selected.values()).isoformat(),
                    parameters=parameters, source_ids=sources, source_ref=";".join(sources)))
                previous = parameters
            visible = self.calendar.next_open(code, day)
            self._settlements[code, day] = visible
        self.start_at = min(_instant(item["start_at"], "session start_at") for item in self.sessions)
        last_end = max(_instant(item["end_at"], "session end_at") for item in self.sessions)
        self.end_at = max(self._settlements.values())
        self.settlement_only = dict(start_at=last_end.isoformat(), end_at=self.end_at.isoformat())
        self.roll_plans = validate_roll_plans(roll_plans, self.contracts)
        self._settlement_keys_by_header = {day: [] for day in self.dates}
        starts = [self._session_starts[day] for day in self.dates]
        # 半开区间以全账户最早会话为界；末日收纳未来只结算尾段。
        for key, available in sorted(self._settlements.items(), key=lambda item: (item[1], *item[0])):
            index = bisect_right(starts, available) - 1
            if index >= 0:
                self._settlement_keys_by_header[self.dates[index]].append(key)

    def settlement(self, code, day, price, *, available_at, source_ref):
        day = date.fromisoformat(day) if isinstance(day, str) else day
        expected = self._settlements[code, day]
        if _instant(available_at, "settlement available_at") != expected:
            raise ValueError(f"期货结算可见时刻与冻结下一会话不一致：{code} {day}")
        if not isinstance(source_ref, str) or not source_ref:
            raise ValueError(f"期货结算来源缺失：{code} {day}")
        return dict(instrument_id=code, trading_date=day.isoformat(), price=str(_decimal(price,
                    "settlement_price", positive=True)), available_at=expected.isoformat(), source_ref=source_ref,
                    availability_assumption=SETTLEMENT_AVAILABILITY)

    def rule_settlement(self, code, day):
        at = self._settlements[code, day]
        rule = _resolve(self.resolver, code, "settlement", day, at)
        params = dict(rule.parameters)
        if params.get("settlement_availability_semantics") != SETTLEMENT_AVAILABILITY:
            raise ValueError(f"期货结算规则须重新冻结下一会话可见性：{code} {day}")
        scale = params.get("price_scale")
        if scale != self.contracts[code]["price_scale"]:
            raise ValueError(f"期货结算价格尺度与真实合约不一致：{code} {day}")
        price = _decimal(params.get("settlement_price_units"), "settlement_price_units", positive=True) / 10 ** scale
        return self.settlement(code, day, price, available_at=rule.available_at, source_ref=";".join(rule.source_ids))


class FuturesSettlementSource:
    """结算只读既有正式列式输入或冻结规则，不连接数据库。"""

    def __init__(self, context, parameters, bars, inputs):
        self.inputs = inputs
        self.dataset = None
        self.plan = None
        self._cache = {}
        self._read = set()
        request_id = parameters.get("settlement_request_id")
        if request_id is None:
            return
        from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef
        from .adapters.common import _environment, _input_external_payload, _input_external_root
        from research_pipeline.data_plane.query_ir import DateRangeV1, QueryPurpose

        port = "bars" if parameters.get("frequency", "1m") == "1d" else "futures"
        payload = bars if port == "bars" else _input_external_payload(context, port)
        bundle = payload.get("data_bundle", payload)
        if request_id not in bundle.get("references", {}):
            raise ValueError("期货结算请求未绑定实际列式输入")
        plan = _environment(context).admitted_plans.get(request_id)
        if (plan is None or plan.query.dataset_id != "cn_futures.settlement"
                or plan.query.purpose is QueryPurpose.AUDIT or not isinstance(plan.query.time_range, DateRangeV1)
                or set(plan.query.universe.instruments) != set(inputs.contracts)
                or plan.session_close_binding is None):
            raise ValueError("期货结算须使用同真实合约范围的非AUDIT正式准入请求")
        self.columns = {}
        for logical, physical in plan.columns:
            if logical not in plan.query.field_ids:
                continue
            role = physical if physical in {"date", "code", "settle_price"} else {
                "fld_futures_settlement_date": "date", "fld_futures_settlement_code": "code",
                "fld_futures_settlement_price": "settle_price"}.get(logical)
            if role:
                self.columns[role] = logical
        if set(self.columns) != {"date", "code", "settle_price"}:
            raise ValueError("期货结算请求缺少交易日、真实合约或结算价")
        resolver = ArtifactResolver(_input_external_root(context, port) / "data",
                                    max_batch_bytes=context.effective_resource_budget.memory_bytes // 8)
        self.dataset = resolver.resolve(DatasetArtifactRef.from_dict(bundle["references"][request_id]))
        self.plan = plan
        self.source_ref = f"{request_id}:{plan.plan_hash}"

    def _read_day(self, day):
        from research_pipeline.data_plane import DatasetFilter

        if day in self._read:
            return
        self._read.add(day)
        codes = tuple(code for code in self.inputs.contracts if (code, day) in self.inputs._settlements)
        at = max(self.plan.session_close_binding.completion_time(code, day) for code in codes)
        for batch in self.dataset.iter_batches_at(plan=self.plan, consumer_time=at,
                columns=tuple(self.columns.values()),
                filters=(DatasetFilter(self.columns["date"], "eq", day),
                         DatasetFilter(self.columns["code"], "in", codes)), batch_size=4096):
            for row in batch.to_pylist():
                code = row[self.columns["code"]]
                key = (code, day)
                if key not in self.inputs._settlements:
                    raise ValueError(f"期货结算包含未冻结合约交易日：{code} {day}")
                if key in self._cache:
                    raise ValueError(f"期货结算重复：{code} {day}")
                available = self.plan.session_close_binding.completion_time(code, day)
                self._cache[key] = self.inputs.settlement(code, day, row[self.columns["settle_price"]],
                                                          available_at=available, source_ref=self.source_ref)

    def for_header(self, day):
        result = []
        for key in self.inputs._settlement_keys_by_header[day]:
            if self.dataset is None:
                result.append(self.inputs.rule_settlement(*key))
            else:
                self._read_day(key[1])
                if key not in self._cache:
                    raise ValueError(f"期货必要结算缺失：{key[0]} {key[1]}")
                result.append(self._cache.pop(key))
        return result


def futures_signals(context, declaration, instruments, sessions, *, signal_reader=None):
    """按实际会话包络读取有界信号，跨午夜仍保留核心可见时间。"""
    from dataclasses import asdict
    from .strategy_signal_inputs import open_strategy_signal_inputs, signal_payload

    start = min(_instant(item["start_at"], "session start_at") for item in sessions)
    end = max(_instant(item["end_at"], "session end_at") for item in sessions)
    result = dict(available_at=start.isoformat(), selection=[])
    current = start.date()
    retained = 0
    while current <= end.date():
        with open_strategy_signal_inputs(context, declaration=declaration, instruments=instruments, day=current,
                                         signal_reader=signal_reader) as provider:
            if provider is not None:
                while provider.next_row is not None:
                    at = provider.next_row["decision_time"]
                    references = provider.visible(at)
                    if not start <= at <= end:
                        continue
                    rows = [signal_payload(asdict(item)) for item in references if item.available_at == at]
                    retained += len(json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                    if retained > context.effective_resource_budget.memory_bytes // 8:
                        raise MemoryError("期货逐日信号超过声明内存额度")
                    result["selection"].extend(rows)
        current += timedelta(days=1)
    return result
