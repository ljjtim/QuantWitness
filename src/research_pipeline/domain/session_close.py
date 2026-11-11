"""从当前冻结日历生成期货开盘与下一会话可见事实。"""
from datetime import datetime, time
from zoneinfo import ZoneInfo

from .session_calendar import SessionCalendarError, SessionCalendarResolver

FUTURES_NEXT_SESSION_POLICY_RULES = {
    "available.futures.daily.next-session.v1": "next_session_open_after_1700",
    "available.futures.settlement.next-session.v1": "next_session_open_after_1700",
    "available.futures.open.v1": "session_open",
}


def futures_calendar_facts(calendar, *, instruments, start, end, available_after):
    """同一冻结日历产生开盘及下一会话可见事实，所属交易日保持独立。"""
    resolver = SessionCalendarResolver(calendar)
    facts, selected = [], []
    for code in instruments:
        policies = tuple(item for item in calendar.policies if item.instrument.instrument_id == code)
        if not policies or policies[0].instrument.asset_class != "cn_future":
            raise SessionCalendarError(f"期货日历缺少合约：{code}")
        days = sorted({day for policy in policies for day in policy.trading_dates if start <= day <= end})
        if not days:
            raise SessionCalendarError(f"期货日历缺少请求交易日：{code}")
        for day in days:
            visible = resolver._visible_policies(policies, day, None)
            if not visible:
                continue
            policy = visible[0]
            session = resolver.resolve_trading_date(policy.instrument, day, policy_revision=None)
            if available_after == "session_open":
                at = min(item.starts_at for item in session.segments if item.bar_eligible)
            else:
                cutoff = max(datetime.combine(day, time(17), ZoneInfo("Asia/Shanghai")),
                             max(item.ends_at for item in session.segments if item.bar_eligible))
                at = resolver.next_session_open(policy.instrument, day, not_before=cutoff)
            facts.append((code, day, at))
            selected.append(policy)
    return tuple(facts), tuple({(p.instrument.instrument_id, p.policy_id, p.revision): p for p in selected}.values())

