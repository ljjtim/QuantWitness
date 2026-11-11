"""项目分钟请求覆盖：保留完整来源版本，只校验本次证券和时间范围。"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .minute_rule_snapshots import MinuteRuleResolver, MinuteRuleSnapshotError, PROJECT_RULE_BUNDLE_VERSION, _hash
from .session_calendar import SessionCalendarResolver, SessionCalendarError, PROJECT_SESSION_BUNDLE_VERSION
from .time import CN_MARKET_TIMEZONE, require_aware_datetime


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


def minute_request_sessions(bundle, *, instruments, start_at, end_at, policy_id=None):
    require_aware_datetime(start_at, "request start_at")
    require_aware_datetime(end_at, "request end_at")
    if start_at >= end_at or not instruments:
        raise SessionCalendarError("项目分钟请求必须有界且声明证券")
    resolver = SessionCalendarResolver(bundle)
    policies_by_instrument = {}
    for policy in bundle.policies:
        policies_by_instrument.setdefault(policy.instrument.instrument_id, []).append(policy)
    for code in instruments:
        policies = tuple(policies_by_instrument.get(code, ()))
        if not policies:
            raise SessionCalendarError(f"session证券缺失: {code}")
        if policy_id is not None and any(item.policy_id != policy_id for item in policies):
            raise SessionCalendarError(f"session政策身份不一致: {code} {policy_id}")
        future = policies[0].instrument.asset_class == "cn_future"
        day = start_at.astimezone(ZoneInfo(CN_MARKET_TIMEZONE)).date()
        last = (end_at - timedelta(microseconds=1)).astimezone(ZoneInfo(CN_MARKET_TIMEZONE)).date()
        if future:
            day -= timedelta(days=max(segment.end_day_offset for policy in policies for segment in policy.segments))
            last -= timedelta(days=min(segment.start_day_offset for policy in policies for segment in policy.segments))
        while day <= last:
            if future:
                possible = [segment for policy in policies for segment in resolver._segments_for_date(policy, day)
                            if segment.bar_eligible]
                if not any(item.starts_at < end_at and item.ends_at >= start_at for item in possible):
                    day += timedelta(days=1)
                    continue
            eligible = tuple(item for item in policies if item.effective_from <= day <= item.effective_to)
            if not eligible:
                raise SessionCalendarError(f"session日期覆盖缺失: {code} {day}")
            if bundle.contract_version == PROJECT_SESSION_BUNDLE_VERSION:
                selected = resolver._visible_policies(eligible, day, None)
            else:
                selected = tuple(item for item in eligible if day in item.trading_dates and item.revision == 1)
            if len(selected) > 1:
                raise SessionCalendarError(f"session日期覆盖冲突: {code} {day}")
            if selected:
                policy = selected[0]
                # 覆盖检查只需时段边界，复用选版时已解析的同日时段。
                segments = resolver._segments_for_date(policy, day)
                starts = min(segment.starts_at for segment in segments if segment.bar_eligible)
                ends = max(segment.ends_at for segment in segments if segment.bar_eligible)
                if starts < end_at and ends >= start_at:
                    yield policy, day, max(starts, start_at), min(ends, end_at)
            day += timedelta(days=1)


def require_minute_request_coverage(rule_bundle, session_bundle, *, instruments, start_at, end_at):
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
    for code in instruments:
        if code not in metadata:
            raise MinuteRuleSnapshotError(f"规则证券缺失: {code}")
    resolver = MinuteRuleResolver(rule_bundle)
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
                    binding = resolver.resolve(rule_id=rule_id, instrument_id=code, effective_on=day, as_of=point)
                except MinuteRuleSnapshotError as exc:
                    raise MinuteRuleSnapshotError(f"请求规则覆盖失败: {code} {day} {rule_id} {point.isoformat()}: {exc}") from exc
                parameters = dict(binding.rule.parameters)
                required = (_FUTURES_REQUIRED_FIELDS if future else _STOCK_REQUIRED_FIELDS).get(rule_id.split(".")[-2], set())
                if not future and rule_id.endswith(".price_limit.v1") and parameters.get("price_limit_mode") != "unbounded":
                    required = required | {"high_limit_units", "low_limit_units",
                        "reference_previous_close_units", "reference_price_available_at"}
                    if parameters.get("price_limit_mode") != "suspended":
                        required = required | {"price_limit_ratio_ppm", "price_limit_rounding"}
                missing = required - parameters.keys()
                if missing:
                    raise MinuteRuleSnapshotError(f"请求规则字段缺失: {code} {day} {rule_id} {sorted(missing)}")
                if rule_id.endswith(".session.v1") and parameters.get("session_policy_id") != policy.policy_id:
                    raise MinuteRuleSnapshotError(f"规则交易时段身份不一致: {code} {day}")
                if not future and rule_id.endswith(".price_limit.v1") and parameters.get("price_limit_mode") != "unbounded":
                    if binding.rule.effective_from != day or binding.rule.effective_to != day:
                        raise MinuteRuleSnapshotError(f"前收盘价与涨跌停须逐交易日覆盖: {code} {day} {rule_id}")
                    value = parameters.get("reference_price_available_at")
                    if not isinstance(value, str):
                        raise MinuteRuleSnapshotError(f"前收盘价可见时间缺失: {code} {day}")
                    visible = datetime.fromisoformat(value)
                    require_aware_datetime(visible, "reference_price_available_at")
                    if visible > point:
                        raise MinuteRuleSnapshotError(f"前收盘价尚不可见: {code} {day}")
                if rule_id.endswith(".adjustment_factor_snapshot.v1"):
                    for key in ("adjustment_snapshot_identity_hash", "corporate_action_snapshot_hash"):
                        _hash(parameters.get(key), f"{code} {day} {rule_id} {key}")
                if future:
                    _require_futures_parameters(parameters, rule_id, code, day, point)
        if future:
            cutoff = max(datetime.combine(day, time(17), tzinfo=ZoneInfo(CN_MARKET_TIMEZONE)), end)
            visible = calendar_resolver.next_session_open(policy.instrument, day, not_before=cutoff)
            binding = resolver.resolve(rule_id="rule.cn_futures.settlement.v1", instrument_id=code,
                                       effective_on=day, as_of=visible)
            parameters = dict(binding.rule.parameters)
            if (parameters.get("settlement_availability_semantics") != "next_session_open_after_1700"
                    or binding.rule.available_at != visible):
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
