"""金融事件、可见性、决策与执行使用的统一时间合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from research_pipeline.platform.errors import MainlineError


CN_MARKET_TIMEZONE = "Asia/Shanghai"


class TimeContractError(MainlineError):
    """时间缺少时区、顺序倒置或交易日映射不成立。"""

    error_code = "time_contract_invalid"


@dataclass(frozen=True)
class FinancialTimeTrace:
    event_time: datetime
    observation_time: datetime
    publication_time: datetime | None
    available_time: datetime
    decision_time: datetime
    order_time: datetime | None
    fill_time: datetime | None
    settlement_time: datetime | None
    policy_id: str

    def __post_init__(self) -> None:
        values = (
            ("event_time", self.event_time),
            ("observation_time", self.observation_time),
            ("publication_time", self.publication_time),
            ("available_time", self.available_time),
            ("decision_time", self.decision_time),
            ("order_time", self.order_time),
            ("fill_time", self.fill_time),
            ("settlement_time", self.settlement_time),
        )
        for field, value in values:
            if value is not None:
                require_aware_datetime(value, field)
        if not isinstance(self.policy_id, str) or not self.policy_id.strip():
            raise TimeContractError("policy_id 必须是非空字符串")
        if self.event_time > self.observation_time:
            raise TimeContractError("event_time 晚于 observation_time")
        visible_source = self.observation_time
        if self.publication_time is not None:
            if self.observation_time > self.publication_time:
                raise TimeContractError("observation_time 晚于 publication_time")
            visible_source = self.publication_time
        if visible_source > self.available_time:
            raise TimeContractError("publication/observation_time 晚于 available_time")
        if self.available_time > self.decision_time:
            raise TimeContractError("available_time 晚于 decision_time")
        if self.order_time is not None and self.decision_time > self.order_time:
            raise TimeContractError("decision_time 晚于 order_time")
        if self.fill_time is not None:
            if self.order_time is None:
                raise TimeContractError("fill_time 存在时必须提供 order_time")
            if self.order_time > self.fill_time:
                raise TimeContractError("order_time 晚于 fill_time")
        if self.settlement_time is not None:
            if self.fill_time is None:
                raise TimeContractError("settlement_time 存在时必须提供 fill_time")
            if self.fill_time > self.settlement_time:
                raise TimeContractError("fill_time 晚于 settlement_time")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "event_time": self.event_time.isoformat(),
            "observation_time": self.observation_time.isoformat(),
            "publication_time": _iso_or_none(self.publication_time),
            "available_time": self.available_time.isoformat(),
            "decision_time": self.decision_time.isoformat(),
            "order_time": _iso_or_none(self.order_time),
            "fill_time": _iso_or_none(self.fill_time),
            "settlement_time": _iso_or_none(self.settlement_time),
            "policy_id": self.policy_id,
        }


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
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise TimeContractError(f"{field} 必须包含明确时区")
    return value


def require_strict_session_order(earlier: object, later: object) -> None:
    if parse_session(earlier, "earlier_session") >= parse_session(
        later,
        "later_session",
    ):
        raise TimeContractError("later_session 必须晚于 earlier_session")


def validate_label_session_horizon(
    *,
    calendar: TradingSessionCalendar,
    signal_session: object,
    entry_session: object,
    exit_session: object,
    horizon_sessions: int,
) -> None:
    calendar.require_next_session(signal_session, entry_session)
    calendar.require_horizon(signal_session, exit_session, horizon_sessions)


def build_cn_daily_next_open_trace(
    *,
    signal_session: object,
    execution_session: object,
    calendar: TradingSessionCalendar,
) -> FinancialTimeTrace:
    calendar.require_next_session(signal_session, execution_session)
    signal_date = parse_session(signal_session, "signal_session")
    execution_date = parse_session(execution_session, "execution_session")
    timezone = ZoneInfo(CN_MARKET_TIMEZONE)
    close = datetime.combine(signal_date, time(15, 0), tzinfo=timezone)
    return FinancialTimeTrace(
        event_time=close,
        observation_time=close,
        publication_time=None,
        available_time=close,
        decision_time=close,
        order_time=datetime.combine(execution_date, time(9, 25), tzinfo=timezone),
        fill_time=datetime.combine(execution_date, time(9, 30), tzinfo=timezone),
        settlement_time=None,
        policy_id="cn_daily_close_to_next_open_v1",
    )


def build_completed_bar_trace(
    *,
    bar_start: datetime,
    bar_end: datetime,
    decision_time: datetime,
    available_time: datetime | None = None,
) -> FinancialTimeTrace:
    return FinancialTimeTrace(
        event_time=bar_start,
        observation_time=bar_end,
        publication_time=None,
        available_time=bar_end if available_time is None else available_time,
        decision_time=decision_time,
        order_time=None,
        fill_time=None,
        settlement_time=None,
        policy_id="completed_bar_decision_v1",
    )


def _iso_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


__all__ = [
    "CN_MARKET_TIMEZONE",
    "FinancialTimeTrace",
    "TimeContractError",
    "TradingSessionCalendar",
    "build_cn_daily_next_open_trace",
    "build_completed_bar_trace",
    "parse_session",
    "require_aware_datetime",
    "require_strict_session_order",
    "validate_label_session_horizon",
]
