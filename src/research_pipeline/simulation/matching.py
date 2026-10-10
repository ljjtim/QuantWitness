"""只读试算成交数量、费用和保证金；不生成事件或更新账本。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable, Mapping

from research_pipeline.domain import Price

from .costs import cash_price_amount_units, cash_fee_units as _fee_units, minute_futures_fee_units
from .ledger import SpotLedgerState, FuturesLedgerState, FuturesPosition
from .margin import required_futures_margin
from .market_rules import CashMarketPolicy, _reject_reason
from .orders import Order, SimulationContractError

from .execution_market import IntradayExecutionPolicy, MinuteExecutionBar, OpeningSnapshot


@dataclass(frozen=True)
class CashExecutionCandidate:
    execution_price: Price
    filled_quantity: int
    notional_units: int = 0
    fee_units: int = 0
    reason_code: str | None = None


def match_cash_order(
    order: Order,
    *,
    policy: CashMarketPolicy,
    snapshot: OpeningSnapshot,
    state: SpotLedgerState,
    execution_at: datetime | None = None,
    cash_scale: int | None = None,
    fee_quote: Callable[[int], int] | None = None,
) -> CashExecutionCandidate:
    quote_fee = fee_quote or (lambda amount: _fee_units(policy, order.side, amount))
    order_market = getattr(order.instrument, "asset_class", None) or getattr(
        order.instrument,
        "market",
        None,
    )
    if order_market != policy.market or state.group.market != policy.market:
        raise SimulationContractError("订单、policy 与 execution group 市场不一致")
    if order.instrument.currency != state.group.currency:
        raise SimulationContractError("订单、execution group 币种不一致")
    if state.group.currency != snapshot.open_price.currency:
        raise SimulationContractError("订单组与价格币种不一致")
    fill_time = order.submitted_at if execution_at is None else execution_at
    if fill_time < order.submitted_at:
        raise SimulationContractError("execution_at 早于订单提交")
    if snapshot.available_time > fill_time:
        raise SimulationContractError("开盘快照在成交时尚不可见")
    execution_price = _execution_price(order.side, snapshot.open_price, policy, cash_scale=cash_scale)
    reason = _reject_reason(order, policy, snapshot, state, execution_price, execution_at=fill_time)
    if reason is not None:
        return CashExecutionCandidate(execution_price, 0, reason_code=reason)
    capacity = min(order.quantity, snapshot.visible_capacity)
    sellable = next((item.sellable for item in state.positions if item.instrument_hash == snapshot.instrument_hash), 0)
    # 零股只能来自本次已通过校验的卖单，不能消费订单未请求的持仓余股。
    filled = policy.quantity_grid(order.side).floor(
        capacity, sellable=min(sellable, order.quantity),
    )
    if filled <= 0:
        return CashExecutionCandidate(execution_price, 0, reason_code="capacity_exceeded")
    if order.side == "buy":
        requested_cost = cash_price_amount_units(execution_price, filled, cash_scale=cash_scale)
        requested_cost += quote_fee(requested_cost)
        if requested_cost > state.available_cash_units:
            if policy.cash_shortage_policy == "reject_v1":
                return CashExecutionCandidate(execution_price, 0, reason_code="insufficient_cash")
            filled = _max_affordable_quantity(
                available_cash_units=state.available_cash_units,
                requested=filled,
                lot_size=policy.lot_size,
                price=execution_price,
                policy=policy,
                cash_scale=cash_scale, fee_quote=quote_fee,
            )
            if filled <= 0:
                return CashExecutionCandidate(execution_price, 0, reason_code="insufficient_cash")
    notional_units = cash_price_amount_units(execution_price, filled, cash_scale=cash_scale)
    fee = quote_fee(notional_units)
    return CashExecutionCandidate(
        execution_price, filled, notional_units, fee,
        None if filled == order.quantity else "partially_filled",
    )


def _execution_price(side: str, reference: Price, policy: CashMarketPolicy, *, cash_scale: int | None = None) -> Price:
    direction = 1 if side == "buy" else -1
    slippage = policy.slippage_units_per_share
    if cash_scale is not None:
        slippage = int(Decimal(slippage).scaleb(reference.scale - cash_scale))
    units = reference.units + direction * slippage
    if units <= 0:
        raise SimulationContractError("滑点后的成交价格必须为正")
    return Price(units, reference.scale, reference.currency)


def _max_affordable_quantity(
    *,
    available_cash_units: int,
    requested: int,
    lot_size: int,
    price: Price,
    policy: CashMarketPolicy,
    cash_scale: int | None = None,
    fee_quote: Callable[[int], int] | None = None,
) -> int:
    quote_fee = fee_quote or (lambda amount: _fee_units(policy, "buy", amount))
    grid = policy.quantity_grid("buy")
    if lot_size != grid.step:
        raise SimulationContractError("资金裁剪步长与市场规则不一致")
    maximum = grid.floor(requested)
    if maximum == 0:
        return 0
    # 0 代表不成交，其余索引从最低申报量开始，不能裁剪成低于 minimum 的数量。
    low, high = 0, (maximum - grid.minimum) // grid.step + 1
    while low < high:
        middle = (low + high + 1) // 2
        quantity = grid.minimum + (middle - 1) * grid.step
        notional = cash_price_amount_units(price, quantity, cash_scale=cash_scale)
        if notional + quote_fee(notional) <= available_cash_units:
            low = middle
        else:
            high = middle - 1
    return 0 if low == 0 else grid.minimum + (low - 1) * grid.step


def _remaining_capacity(
    bar: MinuteExecutionBar,
    policy: IntradayExecutionPolicy,
    used_capacity: Mapping[str, int],
    *,
    lot_size: int,
) -> int:
    total = bar.volume * policy.participation_ppm // 1_000_000
    remaining = max(0, total - used_capacity.get(bar.bar_hash, 0))
    return remaining // lot_size * lot_size


@dataclass(frozen=True)
class FuturesExecutionCandidate:
    filled_quantity: int
    price_units: int
    contracts_delta: int = 0
    fee_units: int = 0
    realized_pnl_units: int = 0
    position_margin_units: int = 0
    required_margin_units: int = 0
    pnl_remainder_numerator: int = 0
    pnl_remainder_denominator: int = 1
    reason_code: str | None = None


def match_minute_futures_order(
    *, instrument_hash: str, side: str, quantity: int, position_effect: str,
    visible_capacity: int, price_units: int, multiplier: int, margin_ppm: int,
    parameters: Mapping[str, object], state: FuturesLedgerState,
) -> FuturesExecutionCandidate:
    filled = min(quantity, visible_capacity)
    if filled <= 0:
        return FuturesExecutionCandidate(0, price_units, reason_code="capacity_exceeded")
    old_position = next((
        item for item in state.positions if item.instrument_hash == instrument_hash
    ), FuturesPosition(instrument_hash, 0, price_units))
    realized_pnl = 0
    remainder_numerator = old_position.pnl_remainder_numerator
    remainder_denominator = old_position.pnl_remainder_denominator
    if position_effect == "close":
        expected_sign = 1 if side == "sell" else -1
        if old_position.contracts * expected_sign <= 0 or filled > abs(old_position.contracts):
            raise SimulationContractError("期货平仓意图与当前持仓不一致")
        realized_pnl, pnl_remainder = old_position.realize(
            price_units=price_units, contracts=filled * expected_sign, multiplier=multiplier,
        )
        remainder_numerator = pnl_remainder.numerator
        remainder_denominator = pnl_remainder.denominator
    direction = 1 if side == "buy" else -1
    new_contracts = old_position.contracts + direction * filled
    new_margin = required_futures_margin(
        price_units=price_units, multiplier=multiplier,
        contracts=abs(new_contracts), margin_ppm=margin_ppm,
    )
    required_margin = max(0, state.margin_units - old_position.margin_units + new_margin)
    fee = minute_futures_fee_units(
        parameters, position_effect=position_effect,
        notional_units=price_units * filled * multiplier,
    )
    if required_margin > state.equity_units + realized_pnl - fee:
        return FuturesExecutionCandidate(0, price_units, reason_code="insufficient_margin")
    return FuturesExecutionCandidate(
        filled, price_units, direction * filled, fee, realized_pnl,
        new_margin, required_margin, remainder_numerator, remainder_denominator,
        "participation_cap" if filled < quantity else None,
    )
