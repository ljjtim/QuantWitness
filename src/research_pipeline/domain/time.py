"""金融事件、可见性、决策与执行使用的统一时间合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from research_pipeline.platform.errors import MainlineError
from research_pipeline.platform.validation import require_aware


CN_MARKET_TIMEZONE = "Asia/Shanghai"


class TimeContractError(MainlineError):
    """时间缺少时区、顺序倒置或交易日映射不成立。"""

    error_code = "time_contract_invalid"


@dataclass(frozen=True)
class TradingSessionCalendar:
    sessions: tuple[date, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.sessions, tuple) or not self.sessions:
            raise TimeContractError("交易日历不能为空")
        if self.sessions != tuple(sorted(set(self.sessions))):
            raise TimeContractError("交易日历必须严格递增且不能重复")

    @classmethod
    def build(cls, values: object) -> "TradingSessionCalendar":
        if isinstance(values, (str, bytes)):
            raise TimeContractError("交易日历必须是日期序列")
        try:
            sessions = tuple(parse_session(value, "trading_calendar") for value in values)
        except TypeError as exc:
            raise TimeContractError("交易日历必须是日期序列") from exc
        return cls(sessions=sessions)

    def position(self, value: object, field: str = "session") -> int:
        session = parse_session(value, field)
        try:
            return self.sessions.index(session)
        except ValueError as exc:
            raise TimeContractError(f"{field} 不在交易日历中") from exc

    def session_at_offset(self, value: object, offset: int) -> date:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 1:
            raise TimeContractError("session offset 必须是正整数")
        position = self.position(value)
        target = position + offset
        if target >= len(self.sessions):
            raise TimeContractError("交易日历没有足够的后续交易日")
        return self.sessions[target]

    def next_session(self, value: object) -> date:
        return self.session_at_offset(value, 1)

    def require_next_session(self, signal: object, execution: object) -> None:
        if self.next_session(signal) != parse_session(execution, "execution_session"):
            raise TimeContractError("execution_session 不是 signal_session 的下一交易日")

    def require_horizon(
        self,
        signal: object,
        actual: object,
        horizon_sessions: int,
    ) -> None:
        expected = self.session_at_offset(signal, horizon_sessions)
        if expected != parse_session(actual, "horizon_session"):
            raise TimeContractError("horizon_session 与声明的交易日跨度不一致")


def parse_session(value: object, field: str = "session") -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        raise TimeContractError(f"{field} 必须是 ISO 日期")
    text = value.strip()
    try:
        if "T" in text or " " in text:
            return datetime.fromisoformat(text).date()
        return date.fromisoformat(text)
    except ValueError as exc:
        raise TimeContractError(f"{field} 必须是 ISO 日期") from exc


def require_aware_datetime(value: datetime, field: str) -> datetime:
    return require_aware(value, field, error=TimeContractError, requirement="必须包含明确时区")


__all__ = [
    "CN_MARKET_TIMEZONE",
    "TimeContractError",
    "TradingSessionCalendar",
    "parse_session",
    "require_aware_datetime",
]
