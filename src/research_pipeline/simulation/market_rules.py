"""现货历史规则、申报数量格点与期货生命周期约束。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping

from research_pipeline.domain import MarketRuleSnapshot
from research_pipeline.platform import typed_canonical_hash

from .orders import SimulationContractError

ETF_CATEGORIES = frozenset({"equity", "bond", "commodity", "cross_border", "money_market"})
STOCK_BOARDS = frozenset({"sh_main", "sz_main", "chinext", "star", "bse"})
LISTING_PHASES = frozenset({"regular", "ipo", "relisting", "delisting"})


def _integer_parameter(parameters: Mapping[str, object], key: str, *, minimum: int = 0) -> int:
    value = parameters.get(key)
    if type(value) is not int or value < minimum:
        raise SimulationContractError(f"市场规则缺少整数参数或取值无效: {key}")
    return value


@dataclass(frozen=True)
class CashQuantityGrid:
    """最小申报量与增量分开；零股仅按明确的可卖余额消费。"""

    minimum: int
    step: int
    maximum: int | None = None
    sell_remainder_allowed: bool = False

    def __post_init__(self) -> None:
        if any(type(value) is not int or value <= 0 for value in (self.minimum, self.step)):
            raise SimulationContractError("数量格点最小值和步长必须是正整数")
        if self.maximum is not None and (type(self.maximum) is not int or self.maximum < self.minimum):
            raise SimulationContractError("数量格点上限无效")
        if type(self.sell_remainder_allowed) is not bool:
            raise SimulationContractError("零股规则必须是明确布尔值")

    def accepts(self, quantity: int, *, sellable: int = 0) -> bool:
        if type(quantity) is not int or quantity <= 0:
            return False
        if self.maximum is not None and quantity > self.maximum:
            return False
        if quantity >= self.minimum and (quantity - self.minimum) % self.step == 0:
            return True
        if not self.sell_remainder_allowed or quantity > sellable:
            return False
        if sellable < self.minimum:
            return quantity == sellable
        remainder = (sellable - self.minimum) % self.step
        return remainder > 0 and quantity % self.step == remainder

    def floor(self, capacity: int, *, sellable: int = 0) -> int:
        """容量内选择合法数量；不得把不足最低申报量的买单强凑成成交。"""
        if type(capacity) is not int or capacity < 0:
            raise SimulationContractError("可成交容量必须是非负整数")
        cap = min(capacity, self.maximum) if self.maximum is not None else capacity
        normal = 0 if cap < self.minimum else self.minimum + (cap - self.minimum) // self.step * self.step
        if not self.sell_remainder_allowed:
            return normal
        cap = min(cap, sellable)
        normal = 0 if cap < self.minimum else self.minimum + (cap - self.minimum) // self.step * self.step
        if sellable < self.minimum:
            return sellable if cap >= sellable else 0
        remainder = (sellable - self.minimum) % self.step
        odd = 0 if not remainder or cap < remainder else (cap - remainder) // self.step * self.step + remainder
        return max(normal, odd)


@dataclass(frozen=True)
class CashMarketPolicy:
    market: str
    rule: MarketRuleSnapshot
    lot_size: int
    settlement_days: int
    commission_ppm: int
    min_commission_units: int
    sell_tax_ppm: int
    transfer_fee_ppm: int
    slippage_units_per_share: int = 0
    cash_shortage_policy: str = "reject_v1"

    def __post_init__(self) -> None:
        if self.market not in {"cn_stock", "cn_etf"} or self.rule.market != self.market:
            raise SimulationContractError("现货 policy market 无效或与规则不一致")
        if type(self.lot_size) is not int or self.lot_size < 1 or type(self.settlement_days) is not int or self.settlement_days not in {0, 1}:
            raise SimulationContractError("lot/settlement policy 无效")
        if self.market == "cn_stock" and self.settlement_days != 1:
            raise SimulationContractError("A 股必须使用显式 T+1 规则")
        amounts = (self.commission_ppm, self.min_commission_units, self.sell_tax_ppm, self.transfer_fee_ppm, self.slippage_units_per_share)
        if any(type(value) is not int or value < 0 for value in amounts):
            raise SimulationContractError("费用和滑点必须是非负整数")
        if self.cash_shortage_policy not in {"reject_v1", "clip_current_lot_continue_v1"}:
            raise SimulationContractError("现金不足 policy 无效")
        parameters = dict(self.rule.parameters)
        _validate_cash_classification(parameters, market=self.market)
        required = parameters.get("market_protection_required", False)
        if type(required) is not bool:
            raise SimulationContractError("market_protection_required 必须是明确布尔值")
        for side in ("buy", "sell"):
            for order_type in ("market", "limit"):
                self.quantity_grid(side, order_type=order_type)
        if self.quantity_grid("buy").step != self.lot_size:
            raise SimulationContractError("lot_size 必须等于显式买入数量步长")

    @property
    def policy_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "rule_hash": self.rule.content_hash,
            "lot_size": self.lot_size,
            "settlement_days": self.settlement_days,
            "commission_ppm": self.commission_ppm,
            "min_commission_units": self.min_commission_units,
            "sell_tax_ppm": self.sell_tax_ppm,
            "transfer_fee_ppm": self.transfer_fee_ppm,
            "slippage_units_per_share": self.slippage_units_per_share,
            "cash_shortage_policy": self.cash_shortage_policy,
        }

    def quantity_grid(self, side: str, *, order_type: str = "limit", include_maximum: bool = True) -> CashQuantityGrid:
        if side not in {"buy", "sell"} or order_type not in {"market", "limit"}:
            raise SimulationContractError("数量格点方向无效")
        parameters = dict(self.rule.parameters)
        minimum_key, step_key = f"{side}_min_quantity", f"{side}_quantity_step"
        detailed = minimum_key in parameters or step_key in parameters
        minimum = _integer_parameter(parameters, minimum_key, minimum=1) if detailed else self.lot_size
        step = _integer_parameter(parameters, step_key, minimum=1) if detailed else self.lot_size
        maximum_key = f"{side}_max_quantity"
        if order_type == "market" and f"market_{maximum_key}" in parameters:
            maximum_key = f"market_{maximum_key}"
        maximum = (_integer_parameter(parameters, maximum_key, minimum=1)
                   if include_maximum and maximum_key in parameters else None)
        # 旧整手 policy 延续沪深零股合同；具备细分格点的规则必须显式声明。
        legacy_remainder = (
            not detailed and "stock_board" not in parameters
            and self.lot_size == 100
        )
        allowed = parameters.get("sell_remainder_allowed", legacy_remainder)
        if type(allowed) is not bool:
            raise SimulationContractError("sell_remainder_allowed 必须是明确布尔值")
        return CashQuantityGrid(minimum, step, maximum, side == "sell" and allowed)


def _validate_cash_classification(parameters: Mapping[str, object], *, market: str) -> None:
    if market == "cn_stock" and "stock_board" in parameters:
        if parameters["stock_board"] not in STOCK_BOARDS:
            raise SimulationContractError("股票板块必须由历史规则明确给出")
        if parameters.get("listing_phase") not in LISTING_PHASES or type(parameters.get("is_st")) is not bool:
            raise SimulationContractError("股票上市阶段和 ST 状态必须由历史规则明确给出")
        for side in ("buy", "sell"):
            _integer_parameter(parameters, f"{side}_min_quantity", minimum=1)
            _integer_parameter(parameters, f"{side}_quantity_step", minimum=1)
        if type(parameters.get("sell_remainder_allowed")) is not bool:
            raise SimulationContractError("股票历史规则缺少明确零股政策")
        if parameters.get("price_limit_mode") not in {"bounded", "unbounded", "suspended"}:
            raise SimulationContractError("股票历史规则缺少明确价格限制模式")
        if parameters.get("price_limit_mode") == "suspended" and (
            parameters.get("paused") is not True or parameters.get("trading_status") != "suspended"
        ):
            raise SimulationContractError("停牌价格模式必须绑定停牌事实")
        _require_cash_lifecycle(parameters, trading_date=None)


def _require_cash_lifecycle(parameters: Mapping[str, object], *, trading_date: date | None) -> None:
    dates: dict[str, date | None] = {}
    for key in ("listed_date", "delisted_date"):
        if key not in parameters:
            raise SimulationContractError(f"现货规则缺少明确日期: {key}")
        raw = parameters[key]
        if key == "delisted_date" and raw is None:
            # 当时无已知终止上市事实，用空值表达；适用区间仍受规则有效期约束。
            dates[key] = None
            continue
        if not isinstance(raw, str):
            raise SimulationContractError(f"现货规则缺少明确日期: {key}")
        try:
            dates[key] = date.fromisoformat(raw)
        except ValueError as exc:
            raise SimulationContractError(f"现货规则日期无效: {key}") from exc
    listed, delisted = dates["listed_date"], dates["delisted_date"]
    if delisted is not None and listed > delisted:
        raise SimulationContractError("现货生命周期结束日早于上市日")
    if trading_date is not None and (trading_date < listed or (delisted is not None and trading_date > delisted)):
        raise SimulationContractError("现货在成交日不处于可交易生命周期")
    status = parameters.get("trading_status")
    if status not in {"trading", "suspended"}:
        raise SimulationContractError("现货交易状态必须明确为 trading/suspended")


def require_cash_rule_applicable(rule: MarketRuleSnapshot, *, trading_date: date, decision_at: datetime) -> None:
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise SimulationContractError("现货决策时间必须带时区")
    if rule.available_time > decision_at:
        raise SimulationContractError("现货规则在决策时尚不可见")
    if trading_date < rule.effective_start or (rule.effective_end is not None and trading_date > rule.effective_end):
        raise SimulationContractError("现货规则不适用于成交日")
    parameters = dict(rule.parameters)
    if "listed_date" in parameters or "delisted_date" in parameters:
        _require_cash_lifecycle(parameters, trading_date=trading_date)


def cash_order_submission_reason(order, policy: CashMarketPolicy) -> str | None:
    """单笔申报限制在占款前校验；拆单由项目策略决定。"""
    grid = policy.quantity_grid(order.side, order_type=order.order_type)
    if grid.maximum is not None and order.quantity > grid.maximum:
        return "order_quantity_exceeded"
    parameters = dict(policy.rule.parameters)
    if order.order_type != "market":
        return None
    protection = order.protection_price
    if parameters.get("market_protection_required", False) and protection is None:
        return "market_protection_required"
    if protection is not None:
        scale = parameters.get("price_scale", protection.scale)
        tick = parameters.get("price_tick_units", 1)
        if protection.scale != scale or protection.units % tick:
            return "invalid_market_protection_price"
    return None


def stock_policy_from_rule(rule: MarketRuleSnapshot, *, slippage_per_share: object = 0, cash_shortage_policy: str = "reject_v1") -> CashMarketPolicy:
    if (rule.market, rule.instrument_type) != ("cn_stock", "stock"):
        raise SimulationContractError("A 股 adapter 收到错误资产规则")
    parameters = dict(rule.parameters)
    settlement = _integer_parameter(parameters, "settlement_days")
    if settlement != 1:
        raise SimulationContractError("A 股必须使用 T+1")
    try:
        amount = Decimal(str(slippage_per_share))
        if not amount.is_finite() or amount < 0:
            raise ValueError
        slippage_units = int(amount.scaleb(2))
        if Decimal(slippage_units).scaleb(-2) != amount:
            raise ValueError
    except (InvalidOperation, ValueError, OverflowError) as exc:
        raise SimulationContractError("每股滑点必须是非负且最多两位小数的金额") from exc
    return CashMarketPolicy(
        "cn_stock", rule, _integer_parameter(parameters, "lot_size", minimum=1), settlement,
        _integer_parameter(parameters, "commission_ppm"), _integer_parameter(parameters, "min_commission_units"),
        _integer_parameter(parameters, "sell_tax_ppm"), _integer_parameter(parameters, "transfer_fee_ppm"),
        slippage_units, cash_shortage_policy,
    )


def etf_policy_from_rule(rule: MarketRuleSnapshot) -> CashMarketPolicy:
    if (rule.market, rule.instrument_type) != ("cn_etf", "etf"):
        raise SimulationContractError("ETF adapter 收到错误资产规则")
    parameters = dict(rule.parameters)
    if parameters.get("etf_category") not in ETF_CATEGORIES:
        raise SimulationContractError("ETF 品类必须由规则快照显式给出")
    return CashMarketPolicy(
        "cn_etf", rule, _integer_parameter(parameters, "lot_size", minimum=1), _integer_parameter(parameters, "settlement_days"),
        _integer_parameter(parameters, "commission_ppm"), _integer_parameter(parameters, "min_commission_units"),
        _integer_parameter(parameters, "sell_tax_ppm"), _integer_parameter(parameters, "transfer_fee_ppm"),
    )


def _require_futures_lifecycle(parameters: Mapping[str, object], trading_date: date) -> None:
    listed_value = parameters.get("listed_date")
    if listed_value is not None:
        if not isinstance(listed_value, str):
            raise SimulationContractError("期货 listed_date 无效")
        try:
            listed_date = date.fromisoformat(listed_value)
        except ValueError as exc:
            raise SimulationContractError("期货 listed_date 无效") from exc
        if trading_date < listed_date:
            raise SimulationContractError("期货合约尚未上市")
    value = parameters.get("last_trade_date")
    if not isinstance(value, str):
        raise SimulationContractError("期货分钟规则缺少明确 last_trade_date")
    try:
        last_trade_date = date.fromisoformat(value)
    except ValueError as exc:
        raise SimulationContractError("期货 last_trade_date 无效") from exc
    if trading_date > last_trade_date:
        raise SimulationContractError("期货合约已过最后交易日")
