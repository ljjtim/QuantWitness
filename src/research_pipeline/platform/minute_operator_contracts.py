"""跨层共享的分钟算子白名单合同。"""

from .asset_taxonomy import CANONICAL_ASSET_CLASSES

MINUTE_FEATURE_IDS = (
    "lagged_return",
    "rolling_volatility",
    "volume_ratio",
    "vwap_close_deviation",
)
MINUTE_ASSET_CLASSES = CANONICAL_ASSET_CLASSES
MINUTE_GAP_POLICIES = ("fail", "reset")
MINUTE_ADJUSTMENT_MODES = ("none", "pre", "post")
MINUTE_SIGNAL_RULES = ("greater_than", "less_than")
MINUTE_LABEL_EVENTS = ("bar_close", "next_bar_open")
MINUTE_TARGET_PAYLOAD_SCHEMA_ID = "research.minute-targets.payload.v1"

__all__ = [
    "MINUTE_ADJUSTMENT_MODES",
    "MINUTE_ASSET_CLASSES",
    "MINUTE_FEATURE_IDS",
    "MINUTE_GAP_POLICIES",
    "MINUTE_LABEL_EVENTS",
    "MINUTE_SIGNAL_RULES",
    "MINUTE_TARGET_PAYLOAD_SCHEMA_ID",
]
