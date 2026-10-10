"""现货历史规则、申报数量格点与期货生命周期约束。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping, TYPE_CHECKING
from zoneinfo import ZoneInfo

from research_pipeline.domain import MarketRuleSnapshot, Price
from research_pipeline.domain.trading import InstrumentKey, instrument_key_from_legacy
from research_pipeline.platform import typed_canonical_hash

from .ledger import SpotLedgerState
from .orders import Order, SimulationContractError

if TYPE_CHECKING:
    from .execution_market import OpeningSnapshot

ETF_CATEGORIES = frozenset({"equity", "bond", "commodity", "cross_border", "money_market"})
STOCK_BOARDS = frozenset({"sh_main", "sz_main", "chinext", "star", "bse"})
LISTING_PHASES = frozenset({"regular", "ipo", "relisting", "delisting"})
_BOARD_VENUES = {"sh_main": "XSHG", "sz_main": "XSHE", "chinext": "XSHE", "star": "XSHG", "bse": "XBSE"}
_SHANGHAI = ZoneInfo("Asia/Shanghai")


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
        for side in ("buy", "sell"):
            self.quantity_grid(side)
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

    def quantity_grid(self, side: str) -> CashQuantityGrid:
        if side not in {"buy", "sell"}:
            raise SimulationContractError("数量格点方向无效")
        parameters = dict(self.rule.parameters)
        minimum_key, step_key = f"{side}_min_quantity", f"{side}_quantity_step"
        detailed = minimum_key in parameters or step_key in parameters
        minimum = _integer_parameter(parameters, minimum_key, minimum=1) if detailed else self.lot_size
        step = _integer_parameter(parameters, step_key, minimum=1) if detailed else self.lot_size
        maximum_key = f"{side}_max_quantity"
        maximum = _integer_parameter(parameters, maximum_key, minimum=1) if maximum_key in parameters else None
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
        if parameters.get("price_limit_mode") not in {"bounded", "unbounded"}:
            raise SimulationContractError("股票历史规则缺少明确价格限制模式")
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


def _sell_remainder_allowed(order: Order, policy: CashMarketPolicy) -> bool:
    return policy.quantity_grid("sell").sell_remainder_allowed


def _reject_reason(order: Order, policy: CashMarketPolicy, snapshot: OpeningSnapshot, state: SpotLedgerState, execution_price: Price, *, execution_at: datetime) -> str | None:
    require_cash_rule_applicable(policy.rule, trading_date=execution_at.astimezone(_SHANGHAI).date(), decision_at=execution_at)
    instrument = order.instrument if isinstance(order.instrument, InstrumentKey) else instrument_key_from_legacy(order.instrument)
    parameters = dict(policy.rule.parameters)
    if parameters.get("source_instrument_id", instrument.instrument_id) != instrument.instrument_id:
        raise SimulationContractError("现货规则标的与订单不一致")
    board = parameters.get("stock_board")
    if board is not None and _BOARD_VENUES[board] != instrument.venue:
        raise SimulationContractError("股票板块与交易所不一致")
    sellable = next((item.sellable for item in state.positions if item.instrument_hash == snapshot.instrument_hash), 0)
    grid = policy.quantity_grid(order.side)
    if (order.side == "sell" and "sell_remainder_allowed" not in parameters
            and "sell_min_quantity" not in parameters
            and (instrument.venue not in {"XSHG", "XSHE"}
                 or order.submitted_at.astimezone(_SHANGHAI).date() < date(2006, 7, 1))
            and order.quantity % grid.step):
        return "invalid_lot"
    if not grid.accepts(order.quantity, sellable=sellable):
        return "invalid_lot"
    if snapshot.paused or parameters.get("trading_status") == "suspended":
        return "suspended"
    limit_mode = parameters.get("price_limit_mode", "bounded")
    if limit_mode not in {"bounded", "unbounded"}:
        raise SimulationContractError("现货价格限制模式无效")
    if limit_mode == "unbounded" and (snapshot.high_limit is not None or snapshot.low_limit is not None):
        raise SimulationContractError("无涨跌幅限制的执行快照必须使用空上下界")
    if limit_mode == "bounded":
        if snapshot.high_limit is None or snapshot.low_limit is None:
            raise SimulationContractError("有涨跌幅限制的执行快照缺少上下界")
        if "price_scale" in parameters and execution_price.scale != parameters["price_scale"]:
            raise SimulationContractError("现货执行报价精度与历史规则不一致")
        if "high_limit_units" in parameters and snapshot.high_limit.units != parameters["high_limit_units"]:
            raise SimulationContractError("现货涨停快照与历史规则不一致")
        if "low_limit_units" in parameters and snapshot.low_limit.units != parameters["low_limit_units"]:
            raise SimulationContractError("现货跌停快照与历史规则不一致")
        if snapshot.low_limit.units > snapshot.high_limit.units:
            raise SimulationContractError("现货价格限制上下界倒置")
        if order.side == "buy" and execution_price.units >= snapshot.high_limit.units:
            return "limit_up_buy_blocked"
        if order.side == "sell" and execution_price.units <= snapshot.low_limit.units:
            return "limit_down_sell_blocked"
    if order.limit_price is not None:
        if order.side == "buy" and execution_price.units > order.limit_price.units:
            return "limit_price_not_reached"
        if order.side == "sell" and execution_price.units < order.limit_price.units:
            return "limit_price_not_reached"
    if order.side == "sell" and sellable < order.quantity:
        return "t1_sell_blocked" if policy.settlement_days == 1 else "insufficient_position"
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
