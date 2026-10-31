"""项目分钟请求覆盖：保留完整来源版本，只校验本次证券和时间范围。"""
from __future__ import annotations

from datetime import datetime, timedelta
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
        day = start_at.astimezone(ZoneInfo(CN_MARKET_TIMEZONE)).date()
        last = (end_at - timedelta(microseconds=1)).astimezone(ZoneInfo(CN_MARKET_TIMEZONE)).date()
        while day <= last:
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
    rules_by_code = {}
    for rule in rule_bundle.rules:
        rules_by_code.setdefault(rule.instrument_id, []).append(rule)
    for policy, day, at, end in minute_request_sessions(session_bundle, instruments=instruments, start_at=start_at, end_at=end_at):
        code = policy.instrument.instrument_id
        if metadata[code] != policy.instrument:
            raise MinuteRuleSnapshotError(f"规则与日历证券身份不一致: {code}")
        rules = rules_by_code[code]
        points = {at}
        points.update(item.available_at for item in rules if item.available_at is not None and at < item.available_at <= end)
        for point in sorted(points):
            for rule_id in sorted({item.rule_id for item in rules}):
                try:
                    binding = resolver.resolve(rule_id=rule_id, instrument_id=code, effective_on=day, as_of=point)
                except MinuteRuleSnapshotError as exc:
                    raise MinuteRuleSnapshotError(f"请求规则覆盖失败: {code} {day} {rule_id} {point.isoformat()}: {exc}") from exc
                parameters = dict(binding.rule.parameters)
                required = _STOCK_REQUIRED_FIELDS.get(rule_id.split(".")[-2], set())
                if rule_id.endswith(".price_limit.v1") and parameters.get("price_limit_mode") != "unbounded":
                    required = required | {"high_limit_units", "low_limit_units",
                        "reference_previous_close_units", "reference_price_available_at"}
                    if parameters.get("price_limit_mode") != "suspended":
                        required = required | {"price_limit_ratio_ppm", "price_limit_rounding"}
                missing = required - parameters.keys()
                if missing:
                    raise MinuteRuleSnapshotError(f"请求规则字段缺失: {code} {day} {rule_id} {sorted(missing)}")
                if rule_id.endswith(".session.v1") and parameters.get("session_policy_id") != policy.policy_id:
                    raise MinuteRuleSnapshotError(f"规则交易时段身份不一致: {code} {day}")
                if rule_id.endswith(".price_limit.v1") and parameters.get("price_limit_mode") != "unbounded":
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
