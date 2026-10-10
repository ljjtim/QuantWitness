"""不硬编码资产费率的定点成本、滑点和容量接口。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import TYPE_CHECKING, Protocol

from research_pipeline.domain import Money, Price, require_integer_quantity

from .orders import SimulationContractError

if TYPE_CHECKING:
    from .market_rules import CashMarketPolicy


_FEN = Decimal("1")


class CostPolicy(Protocol):
    policy_id: str

    def fee(self, *, side: str, price: Price, quantity: int) -> Money: ...


class SlippagePolicy(Protocol):
    policy_id: str

    def execution_price(self, *, side: str, reference: Price, quantity: int) -> Price: ...


class CapacityPolicy(Protocol):
    policy_id: str

    def admitted_quantity(self, *, requested: int, visible_capacity: int) -> int: ...


@dataclass(frozen=True)
class FuturesFeePolicy:
    policy_id: str
    open_units: int
    close_today_units: int
    close_yesterday_units: int

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or min(self.open_units, self.close_today_units, self.close_yesterday_units) < 0:
            raise SimulationContractError("期货费用 policy 无效")

    def fee(self, *, offset: str, contracts: int) -> int:
        quantity = require_integer_quantity(abs(contracts), "contracts")
        rates = {"open": self.open_units, "close_today": self.close_today_units, "close_yesterday": self.close_yesterday_units}
        try:
            return quantity * rates[offset]
        except KeyError as exc:
            raise SimulationContractError("offset 必须是 open/close_today/close_yesterday") from exc


def futures_fee_fen(
    price: Decimal,
    multiplier: int,
    quantity: int,
    fee_value: Decimal,
    *,
    fee_unit: str,
) -> int:
    if fee_unit == "notional_permyriad":
        amount = price * multiplier * quantity * fee_value / 100
    elif fee_unit == "per_lot_cny":
        amount = fee_value * quantity * 100
    else:
        raise SimulationContractError("期货手续费单位未登记")
    return int(amount.quantize(_FEN, rounding=ROUND_HALF_UP))


def cash_price_amount_units(price: Price, quantity: int, *, cash_scale: int | None) -> int:
    """数量乘价格后统一换算金额；日频现金按分，分钟沿用价格单位。"""
    if cash_scale is None:
        return price.units * quantity
    return int((price.decimal * quantity).scaleb(cash_scale).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def cash_fee_units(policy: CashMarketPolicy, side: str, notional_units: int) -> int:
    commission = max(policy.min_commission_units, _ceil_ratio(notional_units * policy.commission_ppm, 1_000_000))
    transfer = _ceil_ratio(notional_units * policy.transfer_fee_ppm, 1_000_000)
    tax = _ceil_ratio(notional_units * policy.sell_tax_ppm, 1_000_000) if side == "sell" else 0
    return commission + transfer + tax


@dataclass(frozen=True)
class CashOrderCostState:
    """已确认成交的累计事实；佣金分子保留每笔当时的费率。"""

    order_id: str
    side: str
    notional_units: int = 0
    commission_units: int = 0
    commission_numerator: int = 0
    fee_units: int = 0
    confirmed_fill_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.order_id or self.side not in {"buy", "sell"}:
            raise SimulationContractError("累计费用订单身份或方向无效")
        for name in ("notional_units", "commission_units", "commission_numerator", "fee_units"):
            require_integer_quantity(getattr(self, name), name, allow_zero=True)


@dataclass(frozen=True)
class CashOrderFeeQuote:
    fill_id: str
    rule_hash: str
    before: CashOrderCostState
    after: CashOrderCostState
    fee_units: int
    commission_units: int
    transfer_units: int
    tax_units: int


def cumulative_cash_fee(
    policy: CashMarketPolicy,
    side: str,
    notional_units: int,
    prior_notional_units: int,
    prior_commission_units: int,
    *,
    prior_commission_numerator: int | None = None,
) -> tuple[int, int]:
    """返回本笔费用和累计已扣佣金；税费只使用本笔成交额。

    同一订单佣金费率变化时须传入历史未舍入佣金分子，不能重估旧成交。
    """
    if side not in {"buy", "sell"}:
        raise SimulationContractError("累计费用方向无效")
    for name, value in (("notional_units", notional_units),
                        ("prior_notional_units", prior_notional_units),
                        ("prior_commission_units", prior_commission_units)):
        require_integer_quantity(value, name, allow_zero=True)
    if prior_commission_numerator is None:
        prior_commission_numerator = prior_notional_units * policy.commission_ppm
    require_integer_quantity(prior_commission_numerator, "prior_commission_numerator", allow_zero=True)
    if notional_units == 0:
        return 0, prior_commission_units
    cumulative_commission = max(
        prior_commission_units, policy.min_commission_units,
        _ceil_ratio(prior_commission_numerator + notional_units * policy.commission_ppm, 1_000_000),
    )
    commission = cumulative_commission - prior_commission_units
    transfer = _ceil_ratio(notional_units * policy.transfer_fee_ppm, 1_000_000)
    tax = _ceil_ratio(notional_units * policy.sell_tax_ppm, 1_000_000) if side == "sell" else 0
    return commission + transfer + tax, cumulative_commission


def quote_cash_order_fee(
    policy: CashMarketPolicy,
    state: CashOrderCostState,
    *,
    fill_id: str,
    notional_units: int,
) -> CashOrderFeeQuote:
    """只试算，不更新已确认累计；规则由调用方按成交可见时点选定。"""
    if not fill_id or fill_id in state.confirmed_fill_ids:
        raise SimulationContractError("成交费用身份为空或已经确认")
    require_integer_quantity(notional_units, "notional_units")
    fee, cumulative_commission = cumulative_cash_fee(
        policy, state.side, notional_units, state.notional_units, state.commission_units,
        prior_commission_numerator=state.commission_numerator,
    )
    transfer = _ceil_ratio(notional_units * policy.transfer_fee_ppm, 1_000_000)
    tax = _ceil_ratio(notional_units * policy.sell_tax_ppm, 1_000_000) if state.side == "sell" else 0
    after = CashOrderCostState(
        state.order_id, state.side, state.notional_units + notional_units,
        cumulative_commission, state.commission_numerator + notional_units * policy.commission_ppm,
        state.fee_units + fee, (*state.confirmed_fill_ids, fill_id),
    )
    return CashOrderFeeQuote(fill_id, policy.rule.content_hash, state, after, fee,
                             cumulative_commission - state.commission_units, transfer, tax)


def confirm_cash_order_fee(state: CashOrderCostState, quote: CashOrderFeeQuote) -> CashOrderCostState:
    """账本成交确认成功后提交本笔累计，过时试算不得覆盖新状态。"""
    if state != quote.before or quote.fill_id in state.confirmed_fill_ids:
        raise SimulationContractError("费用试算状态已经变化或成交已确认")
    return quote.after


def minute_futures_fee_units(
    parameters: Mapping[str, object],
    *,
    position_effect: str,
    notional_units: int,
) -> int:
    if parameters.get("fee_unit") != "notional_permyriad":
        raise SimulationContractError("期货分钟手续费只支持成交额万分比")
    if parameters.get("opening_charge_null_semantics") != (
        "use_common_clearance_charge"
    ):
        raise SimulationContractError("期货开仓空费率缺少已批准解释")
    close_rate = minute_fee_parameter(parameters, "close_fee_ppm")
    close_today_rate = minute_fee_parameter(parameters, "close_today_fee_ppm")
    if close_rate != close_today_rate:
        raise SimulationContractError(
            "分钟期货首版不能区分平今持仓，close_today_fee_ppm 必须等于 close_fee_ppm"
        )
    key = "open_fee_ppm" if position_effect == "open" else "close_fee_ppm"
    rate = minute_fee_parameter(parameters, key)
    return _ceil_ratio(notional_units * rate, 1_000_000)


def minute_fee_parameter(parameters: Mapping[str, object], key: str) -> int:
    value = parameters.get(key)
    if type(value) is not int or value < 0:
        raise SimulationContractError(f"分钟规则缺少非负整数参数: {key}")
    return value


def _ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


__all__ = [
    "CapacityPolicy",
    "CostPolicy",
    "FuturesFeePolicy",
    "SlippagePolicy",
    "cash_price_amount_units",
    "cash_fee_units",
    "CashOrderCostState",
    "CashOrderFeeQuote",
    "cumulative_cash_fee",
    "quote_cash_order_fee",
    "confirm_cash_order_fee",
    "futures_fee_fen",
    "minute_futures_fee_units",
]
