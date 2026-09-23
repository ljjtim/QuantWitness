"""开盘成交前可见的可交易性判断。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math


@dataclass(frozen=True)
class TradeReference:
    trade_date: date
    code: str
    open: float
    close: float
    volume: float
    money: float
    paused: bool
    high_limit: float
    low_limit: float
    pre_close: float
    high: float | None = None
    low: float | None = None


@dataclass(frozen=True)
class TradabilityDecision:
    allowed: bool
    reason_code: str | None = None
    message: str = ""


def evaluate_tradability(
    *,
    side: str,
    requested_shares: int,
    reference: TradeReference,
    sellable_quantity: int,
    lot_size: int = 100,
    reject_if_open_limit: bool = True,
    reject_if_intraday_limit_touch: bool = False,
    sell_block_reason_code: str = "t1_sell_blocked",
    sell_block_message: str = "T+1 可卖数量不足",
) -> TradabilityDecision:
    if reject_if_intraday_limit_touch:
        raise ValueError("盘中触板数据在开盘成交时尚不可见，存在前视偏差")
    if requested_shares < lot_size:
        return _blocked("below_lot_size", f"订单不足 {lot_size} 股交易单位")
    if reference.paused:
        return _blocked("suspended", "标的当日停牌")
    if not _valid_price(reference.open):
        return _blocked("invalid_trade_price", "开盘价无效")
    if side == "buy" and not _valid_price(reference.high_limit):
        return _blocked("invalid_limit_price", "涨停参考价无效，买单拒绝")
    if side == "sell" and not _valid_price(reference.low_limit):
        return _blocked("invalid_limit_price", "跌停参考价无效，卖单拒绝")
    if (
        reject_if_open_limit
        and side == "buy"
        and reference.open >= reference.high_limit
    ):
        return _blocked("limit_up_buy_blocked", "开盘价达到或超过涨停价，买单拒绝")
    if (
        reject_if_open_limit
        and side == "sell"
        and reference.open <= reference.low_limit
    ):
        return _blocked("limit_down_sell_blocked", "开盘价达到或低于跌停价，卖单拒绝")
    if side == "sell" and sellable_quantity < requested_shares:
        return _blocked(sell_block_reason_code, sell_block_message)
    return TradabilityDecision(True)


def _blocked(reason_code: str, message: str) -> TradabilityDecision:
    return TradabilityDecision(False, reason_code=reason_code, message=message)


def _valid_price(value: float) -> bool:
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError):
        return False


__all__ = ["TradabilityDecision", "TradeReference", "evaluate_tradability"]
