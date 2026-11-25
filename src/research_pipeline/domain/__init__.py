"""新研究主链的不可变金融领域语义。"""

from research_pipeline.platform.errors import DomainContractError
from .trading import (
    INSTRUMENT_KEY_VERSION,
    PORTFOLIO_TARGET_VERSION,
    InstrumentKey,
    PortfolioTarget,
    PortfolioTargetEntry,
)
from .time import (
    CN_MARKET_TIMEZONE,
    TimeContractError,
    TradingSessionCalendar,
    parse_session,
)
from .sessions import SessionSegment, TradingSession
from .session_calendar import (
    SESSION_POLICY_BUNDLE_VERSION,
    SESSION_POLICY_VERSION,
    SessionCalendarError,
    SessionCalendarResolver,
    SessionInstrumentMetadata,
    SessionPolicyBundle,
    SessionPolicyRevision,
    SessionSegmentTemplate,
    load_session_policy_bundle,
)

from .minute_rule_snapshots import (
    CURRENT_MINUTE_RULE_BUNDLE_HASH,
    CURRENT_MINUTE_RULE_COVERAGE_HASH,
    MINUTE_RULE_BUNDLE_VERSION,
    MinuteRuleBinding,
    MinuteRuleResolver,
    MinuteRuleSnapshot,
    MinuteRuleSnapshotBundle,
    MinuteRuleSnapshotError,
    MinuteRuleSource,
    load_minute_rule_snapshot_bundle,
)
from .corporate_actions import CorporateAction, resolve_corporate_actions


__all__ = [
    "CN_MARKET_TIMEZONE",
    "CorporateAction",
    "DomainContractError",
    "InstrumentKey",
    "CURRENT_MINUTE_RULE_BUNDLE_HASH",
    "CURRENT_MINUTE_RULE_COVERAGE_HASH",
    "MINUTE_RULE_BUNDLE_VERSION",
    "MinuteRuleBinding",
    "MinuteRuleResolver",
    "MinuteRuleSnapshot",
    "MinuteRuleSnapshotBundle",
    "MinuteRuleSnapshotError",
    "MinuteRuleSource",
    "SESSION_POLICY_BUNDLE_VERSION",
    "SESSION_POLICY_VERSION",
    "SessionCalendarError",
    "SessionCalendarResolver",
    "SessionInstrumentMetadata",
    "SessionPolicyBundle",
    "SessionPolicyRevision",
    "SessionSegment",
    "SessionSegmentTemplate",
    "PortfolioTarget",
    "PortfolioTargetEntry",
    "INSTRUMENT_KEY_VERSION",
    "PORTFOLIO_TARGET_VERSION",
    "TimeContractError",
    "TradingSessionCalendar",
    "TradingSession",
    "parse_session",
    "load_session_policy_bundle",
    "load_minute_rule_snapshot_bundle",
    "resolve_corporate_actions",
]
