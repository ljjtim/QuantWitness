"""无行情现金清算会话的纯声明合同。"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Mapping
from zoneinfo import ZoneInfo


def parse_non_trading_sessions(value: Mapping[str, object] | None) -> dict[str, object] | None:
    """保留原始声明，只接受明确有界且事前可见的会话日期。"""
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {"dates", "available_at", "source_ref"}:
        raise ValueError("non_trading_sessions 必须精确包含 dates、available_at、source_ref")
    dates = value["dates"]
    if not isinstance(dates, (list, tuple)) or not dates:
        raise ValueError("non_trading_sessions.dates 必须为非空 ISO 日期列表")
    parsed = []
    for item in dates:
        if not isinstance(item, str):
            raise ValueError("non_trading_sessions.dates 必须为 ISO 日期字符串")
        try:
            session = date.fromisoformat(item)
        except ValueError as exc:
            raise ValueError("non_trading_sessions.dates 必须为 ISO 日期字符串") from exc
        if session.isoformat() != item:
            raise ValueError("non_trading_sessions.dates 必须使用 YYYY-MM-DD")
        parsed.append(session)
    if any(left >= right for left, right in zip(parsed, parsed[1:])):
        raise ValueError("non_trading_sessions.dates 必须严格升序且唯一")
    if not isinstance(value["source_ref"], str) or not value["source_ref"].strip():
        raise ValueError("non_trading_sessions.source_ref 必须为非空字符串")
    declaration = {"dates": list(dates), "available_at": value["available_at"],
                   "source_ref": value["source_ref"]}
    validate_non_trading_sessions_available_at(declaration)
    return declaration


def validate_non_trading_sessions_available_at(declaration: Mapping[str, object]) -> None:
    """声明须严格早于首会话上海时间 09:15 可见。"""
    raw = declaration["available_at"]
    if not isinstance(raw, str):
        raise ValueError("non_trading_sessions.available_at 必须为带时区 ISO 时间")
    try:
        available_at = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError as exc:
        raise ValueError("non_trading_sessions.available_at 必须为带时区 ISO 时间") from exc
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise ValueError("non_trading_sessions.available_at 必须为带时区 ISO 时间")
    first = date.fromisoformat(declaration["dates"][0])
    preopen = datetime.combine(first, time(9, 15), ZoneInfo("Asia/Shanghai"))
    if available_at >= preopen:
        raise ValueError("non_trading_sessions 声明必须在首会话 09:15 前可见")
