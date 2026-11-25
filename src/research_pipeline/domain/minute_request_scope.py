"""项目分钟请求覆盖：保留完整来源版本，只校验本次证券和时间范围。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import NamedTuple
from zoneinfo import ZoneInfo

from .minute_rule_snapshots import MinuteRuleResolver, MinuteRuleSnapshotError, PROJECT_RULE_BUNDLE_VERSION, _hash
from .session_calendar import SessionCalendarResolver, SessionCalendarError, PROJECT_SESSION_BUNDLE_VERSION
from .sessions import SessionSegment
from .time import CN_MARKET_TIMEZONE, require_aware_datetime
from .cash_price_limits import CashPriceLimitError, resolve_cash_price_limits


_STOCK_REQUIRED_FIELDS = {
    "instrument_lifecycle": {"listed_date", "delisted_date", "is_st", "listing_phase", "stock_board", "trading_status"},
    "lot_size": {"buy_min_quantity", "buy_quantity_step", "sell_min_quantity", "sell_quantity_step", "sell_remainder_allowed"},
    "price_limit": {"price_limit_mode", "price_scale"},
    "session": {"session_policy_id"},
    "settlement": {"settlement_days"},
    "suspension": {"paused"},
    "trading_fee": {"commission_ppm", "min_commission_units", "sell_tax_ppm", "transfer_fee_ppm", "cost_model_scope"},
    "adjustment_factor_snapshot": {"adjustment_snapshot_identity_hash", "corporate_action_snapshot_hash"},
}

_FUTURES_REQUIRED_FIELDS = {
    "contract_lifecycle": {"listed_date", "last_trade_date", "exit_deadline"},
    "price_tick": {"price_scale", "price_tick_units"},
    "session": {"session_policy_id"},
    "price_limit": {"price_scale", "high_limit_units", "low_limit_units", "reference_price_available_at"},
    "delivery_expiry": {"exit_deadline", "close_priority"},
    "actual_contract_mapping": {"actual_contract_id"},
}


class MinuteRequestSession(NamedTuple):
    policy_index: int
    trading_date: date
    start_at: datetime
    end_at: datetime
    segments: tuple[SessionSegment, ...]
    session_open: datetime
    session_close: datetime


class MinuteRequestCalendar:
    """同一冻结日历内，按政策内容和请求窗口共享会话展开。"""

    def __init__(self, bundle):
        self.resolver = SessionCalendarResolver(bundle)
        self._date_ids = {}
        self._date_values = {}
        self._expanded = {}

    def _date_key(self, dates):
        key = self._date_ids.get(id(dates))
        if key is None:
            key = self._date_values.setdefault(dates, len(self._date_values))
            self._date_ids[id(dates)] = key
        return key

    def sessions(self, instrument, start_at, end_at, policy_id=None):
        policies = tuple(self.resolver._policies_by_instrument.get(instrument, ()))
        if not policies:
            raise SessionCalendarError(f"session证券缺失: {instrument}")
        if policy_id is not None and any(item.policy_id != policy_id for item in policies):
            raise SessionCalendarError(f"session政策身份不一致: {instrument} {policy_id}")
        future = policies[0].instrument.asset_class == "cn_future"
        # 政策族之间只需保留相等关系，不依赖各证券使用的政策名称。
        families = {}
        shapes = tuple((families.setdefault(item.policy_id, len(families)), item.revision,
                        item.effective_from, item.effective_to, item.available_at,
                        self._date_key(item.trading_dates), item.segments) for item in policies)
        key = (start_at, end_at, future, shapes)
        if key not in self._expanded:
            self._expanded[key] = tuple(self._expand(policies, start_at, end_at, future))
        return policies, self._expanded[key]

    def _expand(self, policies, start_at, end_at, future):
        code = policies[0].instrument.instrument_id
        day = start_at.astimezone(ZoneInfo(CN_MARKET_TIMEZONE)).date()
        last = (end_at - timedelta(microseconds=1)).astimezone(ZoneInfo(CN_MARKET_TIMEZONE)).date()
        if future:
            day -= timedelta(days=max(segment.end_day_offset for policy in policies for segment in policy.segments))
            last -= timedelta(days=min(segment.start_day_offset for policy in policies for segment in policy.segments))
        resolver = self.resolver
        indices = {id(item): index for index, item in enumerate(policies)}
        while day <= last:
            if future:
                possible = (segment for policy in policies for segment in resolver._segments_for_date(policy, day)
                            if segment.bar_eligible)
                if not any(item.starts_at < end_at and item.ends_at >= start_at for item in possible):
                    day += timedelta(days=1)
                    continue
            eligible = tuple(item for item in policies if item.effective_from <= day <= item.effective_to)
            if not eligible:
                raise SessionCalendarError(f"session日期覆盖缺失: {code} {day}")
            if resolver.bundle.contract_version == PROJECT_SESSION_BUNDLE_VERSION:
                selected = resolver._visible_policies(eligible, day, None)
            else:
                selected = tuple(item for item in eligible if day in item.trading_dates and item.revision == 1)
            if len(selected) > 1:
                raise SessionCalendarError(f"session日期覆盖冲突: {code} {day}")
            if selected:
                policy = selected[0]
                segments = tuple(segment for segment in resolver._segments_for_date(policy, day)
                                 if segment.bar_eligible)
                starts = min(segment.starts_at for segment in segments)
                ends = max(segment.ends_at for segment in segments)
                if starts < end_at and ends >= start_at:
                    yield MinuteRequestSession(indices[id(policy)], day, max(starts, start_at), min(ends, end_at),
                                               segments, starts, ends)
            day += timedelta(days=1)

    @staticmethod
    def check_window(instruments, start_at, end_at):
        require_aware_datetime(start_at, "request start_at")
        require_aware_datetime(end_at, "request end_at")
        if start_at >= end_at or not instruments:
            raise SessionCalendarError("项目分钟请求必须有界且声明证券")


def validate_minute_request_sessions(bundle, *, instruments, start_at, end_at, policy_id=None):
    calendar = MinuteRequestCalendar(bundle)
    calendar.check_window(instruments, start_at, end_at)
    for code in instruments:
        calendar.sessions(code, start_at, end_at, policy_id)


def minute_request_sessions(bundle, *, instruments, start_at, end_at, policy_id=None):
    calendar = MinuteRequestCalendar(bundle)
    calendar.check_window(instruments, start_at, end_at)
    for code in instruments:
        policies, sessions = calendar.sessions(code, start_at, end_at, policy_id)
        for session in sessions:
            yield policies[session.policy_index], session.trading_date, session.start_at, session.end_at


def require_minute_request_coverage(rule_bundle, session_bundle, *, instruments, start_at, end_at, stock_raw=False):
    if rule_bundle.contract_version != PROJECT_RULE_BUNDLE_VERSION:
        raise MinuteRuleSnapshotError("扩大项目范围必须显式提供项目规则版本")
    if session_bundle.contract_version != PROJECT_SESSION_BUNDLE_VERSION:
        raise MinuteRuleSnapshotError("项目规则缺少项目日历")
    if dict(rule_bundle.capability_binding)["minute_capability_manifest_hash"] != dict(session_bundle.capability_binding)["minute_capability_manifest_hash"]:
        raise MinuteRuleSnapshotError("规则与日历来源能力身份不一致")
    sources = [item for item in rule_bundle.sources if item.source_kind == "repository_snapshot"]
    if len(sources) != 1 or not sources[0].locator.endswith("#" + session_bundle.bundle_hash):
        raise MinuteRuleSnapshotError("项目规则未绑定本次完整日历来源")
    metadata = {item.instrument_id: item for item in rule_bundle.instruments}
    _require_rule_coverage(MinuteRuleResolver(rule_bundle), metadata, session_bundle,
                           instruments=instruments, start_at=start_at, end_at=end_at, stock_raw=stock_raw)


def _require_rule_coverage(resolver, metadata, session_bundle, *, instruments, start_at, end_at, stock_raw=False):
    for code in instruments:
        if code not in metadata:
            raise MinuteRuleSnapshotError(f"规则证券缺失: {code}")
    for code in instruments:
        if stock_raw and metadata[code].asset_class != "cn_stock":
            raise MinuteRuleSnapshotError(f"股票原价覆盖只能用于股票: {code}")
        if (not stock_raw and metadata[code].asset_class == "cn_stock"
                and "rule.cn_stock.adjustment_factor_snapshot.v1" not in resolver.rule_ids(instrument_id=code)):
            raise MinuteRuleSnapshotError(f"股票请求缺少复权快照规则: {code}")
    calendar_resolver = SessionCalendarResolver(session_bundle)
    for policy, day, at, end in minute_request_sessions(session_bundle, instruments=instruments, start_at=start_at, end_at=end_at):
        code = policy.instrument.instrument_id
        future = policy.instrument.asset_class == "cn_future"
        if metadata[code] != policy.instrument:
            raise MinuteRuleSnapshotError(f"规则与日历证券身份不一致: {code}")
        points = {at}
        points.update(resolver.available_times(instrument_id=code, effective_on=day, start=at, end=end))
        for point in sorted(points):
            for rule_id in resolver.rule_ids(instrument_id=code):
                if future and rule_id == "rule.cn_futures.settlement.v1":
                    continue
                try:
                    rule = resolver.resolve_rule(rule_id=rule_id, instrument_id=code, effective_on=day, as_of=point)
                except MinuteRuleSnapshotError as exc:
                    raise MinuteRuleSnapshotError(f"请求规则覆盖失败: {code} {day} {rule_id} {point.isoformat()}: {exc}") from exc
                parameters = dict(rule.parameters)
                required = (_FUTURES_REQUIRED_FIELDS if future else _STOCK_REQUIRED_FIELDS).get(rule_id.split(".")[-2], set())
                missing = required - parameters.keys()
                if missing:
                    raise MinuteRuleSnapshotError(f"请求规则字段缺失: {code} {day} {rule_id} {sorted(missing)}")
                if rule_id.endswith(".session.v1") and parameters.get("session_policy_id") != policy.policy_id:
                    raise MinuteRuleSnapshotError(f"规则交易时段身份不一致: {code} {day}")
                if not future and rule_id.endswith(".price_limit.v1"):
                    if parameters.get("price_limit_mode") != "unbounded" and (
                        rule.effective_from != day or rule.effective_to != day
                    ):
                        raise MinuteRuleSnapshotError(f"参考价与涨跌停须逐交易日覆盖: {code} {day} {rule_id}")
                    # 生命周期和停牌事实来自同一时点的独立规则，不能由价格字段覆盖。
                    combined = dict(parameters)
                    for section in ("instrument_lifecycle", "suspension"):
                        context_id = rule_id.removesuffix("price_limit.v1") + section + ".v1"
                        context = resolver.resolve_rule(rule_id=context_id, instrument_id=code, effective_on=day, as_of=point)
                        for key, value in context.parameters:
                            if key in combined and combined[key] != value:
                                raise MinuteRuleSnapshotError(f"价格与上下文规则参数冲突: {code} {day} {key}")
                            combined[key] = value
                    if combined.get("reference_price_kind") == "issue_price" and policy.instrument.asset_class != "cn_stock":
                        raise MinuteRuleSnapshotError(f"发行价参考只适用于股票IPO阶段: {code} {day}")
                    try:
                        resolve_cash_price_limits(combined, decision_at=point)
                    except CashPriceLimitError as exc:
                        raise MinuteRuleSnapshotError(f"请求价格规则无效: {code} {day} {rule_id}: {exc}") from exc
                if rule_id.endswith(".adjustment_factor_snapshot.v1"):
                    for key in ("adjustment_snapshot_identity_hash", "corporate_action_snapshot_hash"):
                        _hash(parameters.get(key), f"{code} {day} {rule_id} {key}")
                if future:
                    _require_futures_parameters(parameters, rule_id, code, day, point)
        if future:
            cutoff = max(datetime.combine(day, time(17), tzinfo=ZoneInfo(CN_MARKET_TIMEZONE)), end)
            visible = calendar_resolver.next_session_open(policy.instrument, day, not_before=cutoff)
            rule = resolver.resolve_rule(rule_id="rule.cn_futures.settlement.v1", instrument_id=code,
                                       effective_on=day, as_of=visible)
            parameters = dict(rule.parameters)
            if (parameters.get("settlement_availability_semantics") != "next_session_open_after_1700"
                    or rule.available_at != visible):
                raise MinuteRuleSnapshotError(f"期货结算须冻结17:00之后的下一会话可见时间: {code} {day}")


def _require_futures_parameters(parameters, rule_id, code, day, point):
    kind = rule_id.split(".")[-2]
    if kind == "contract_lifecycle":
        try:
            listed = date.fromisoformat(str(parameters["listed_date"]))
            last = date.fromisoformat(str(parameters["last_trade_date"]))
        except (ValueError, TypeError) as exc:
            raise MinuteRuleSnapshotError(f"期货合约生命周期日期无效: {code} {day}") from exc
        if not listed <= day <= last:
            raise MinuteRuleSnapshotError(f"期货会话超出上市至最后交易日期间: {code} {day}")
    if kind == "contract_multiplier" and not ("contract_multiplier" in parameters or
            parameters.get("quote_unit") == "CNY/kg" and "contract_unit_kg" in parameters):
        raise MinuteRuleSnapshotError(f"期货合约乘数缺失: {code} {day}")
    if kind == "margin" and not ("speculative_initial_margin_ppm" in parameters or
            {"long_margin_ppm", "short_margin_ppm"} <= parameters.keys()):
        raise MinuteRuleSnapshotError(f"期货多空保证金率缺失: {code} {day}")
    if kind == "fee_schedule":
        for effect in ("open", "close", "close_today"):
            if not any(effect + suffix in parameters for suffix in (
                    "_fee_per_contract_units", "_fee_rate_ppm", "_fee_units", "_fee_ppm")):
                raise MinuteRuleSnapshotError(f"期货费用缺失: {code} {day} {effect}")
    if kind == "price_limit":
        at = datetime.fromisoformat(str(parameters["reference_price_available_at"]))
        require_aware_datetime(at, "reference_price_available_at")
        if at > point:
            raise MinuteRuleSnapshotError(f"期货参考结算价尚不可见: {code} {day}")
    if kind == "actual_contract_mapping" and parameters["actual_contract_id"] != code:
        raise MinuteRuleSnapshotError(f"期货成交合约不能由未来映射替换: {code} {day}")
