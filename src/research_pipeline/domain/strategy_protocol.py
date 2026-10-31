"""策略事件和交易意图的内部协议；不执行撮合或账户计算。"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from functools import cached_property
from datetime import date, datetime, time
from decimal import Decimal
import json
import math
from typing import Mapping

from .models import DomainContractError
from .time import require_aware_datetime
from .trading import InstrumentKey
from .values import Money, Price, require_integer_quantity


STRATEGY_PROTOCOL_VERSION = "research-strategy-protocol-v3"
STRATEGY_STATE_VERSION = "research-strategy-json-state-v1"
EVENT_ORDER = (
    "initialize", "before_trading", "execution_feedback", "scheduled", "bar", "after_trading",
)
INTENT_KINDS = frozenset({
    "quantity", "amount", "target_quantity", "target_amount", "target_weight", "cancel",
})


class StrategyProtocolError(DomainContractError):
    """策略协议错误；机器码用于准入或Worker诊断。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = code


def _check(condition: bool, message: str, code: str = "strategy_schema_invalid") -> None:
    if not condition:
        raise StrategyProtocolError(code, message)


def _text(value: object, name: str) -> None:
    _check(isinstance(value, str) and bool(value.strip()), f"{name} 必须是非空字符串")


def _stock(instrument: InstrumentKey) -> None:
    _check(isinstance(instrument, InstrumentKey), "instrument 必须是 InstrumentKey")
    _check(
        (instrument.asset_class, instrument.currency, instrument.contract_kind)
        in {("cn_stock", "CNY", "stock"), ("cn_etf", "CNY", "etf")},
        "策略协议仅支持人民币股票与ETF现货", "strategy_capability_unsupported",
    )


def _money(value: Money, name: str, *, signed: bool = False) -> None:
    _check(type(value) is Money and value.currency == "CNY", f"{name} 必须是人民币Money")
    _check(signed or value.units >= 0, f"{name} 不能为负")


def _tuple(value: object, name: str, item_type: type) -> None:
    _check(isinstance(value, tuple) and all(isinstance(item, item_type) for item in value),
           f"{name} 必须是不可变的 {item_type.__name__} tuple")


def _unique(values: tuple, name: str) -> None:
    _check(len(values) == len(set(values)), f"{name} 不能重复")


def _wire(value: object) -> object:
    if isinstance(value, (Money, Price, InstrumentKey)):
        return value.to_dict()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value):
        return {item.name: _wire(getattr(value, item.name)) for item in fields(value) if item.metadata.get("wire", True)}
    if isinstance(value, tuple):
        return [_wire(item) for item in value]
    return value


@dataclass(frozen=True, kw_only=True)
class StrategyLimits:
    max_state_bytes: int
    max_context_bytes: int
    max_response_bytes: int
    max_intents_per_event: int
    callback_wall_seconds: int

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            _check(type(value) is int and value > 0, f"{item.name} 必须是正整数")


@dataclass(frozen=True, kw_only=True)
class StrategySchedule:
    schedule_id: str
    basis: str
    at: str | None = None
    offset_seconds: int | None = None

    def __post_init__(self) -> None:
        _text(self.schedule_id, "schedule_id")
        _check(self.basis in {"local_time", "session_open", "session_close"}, "调度基准不受支持")
        if self.basis == "local_time":
            _check(isinstance(self.at, str) and self.offset_seconds is None,
                   "local_time需要at且不能有offset_seconds")
            try:
                parsed = time.fromisoformat(self.at)
            except ValueError as exc:
                raise StrategyProtocolError("strategy_schema_invalid", "at必须为HH:MM:SS") from exc
            _check(parsed.tzinfo is None and parsed.isoformat() == self.at and not parsed.microsecond,
                   "at必须为交易日历本地HH:MM:SS")
        else:
            _check(self.at is None and type(self.offset_seconds) is int,
                   "会话偏移需要整数offset_seconds且不能有at")


@dataclass(frozen=True, kw_only=True)
class StrategyInputBinding:
    binding_id: str
    columns: tuple[str, ...]
    lookback_bars: int
    source: str = "bars"
    delivery: str | None = None

    def __post_init__(self) -> None:
        _text(self.binding_id, "binding_id")
        _check(self.source in {"bars", "signals"}, "输入source只能为bars或signals")
        expected = "columnar" if self.source == "bars" else "inline"
        if self.delivery is None:
            object.__setattr__(self, "delivery", expected)
        _check(self.delivery == expected, "bars必须columnar交付，signals必须inline交付")
        _tuple(self.columns, "columns", str)
        _check(bool(self.columns), "输入列不能留空")
        for column in self.columns:
            _text(column, "column")
        _unique(self.columns, "columns")
        require_integer_quantity(self.lookback_bars, "lookback_bars")


@dataclass(frozen=True, kw_only=True)
class StrategyDeclaration:
    strategy_id: str
    bundle_id: str
    state_schema_id: str
    subscriptions: tuple[str, ...]
    schedules: tuple[StrategySchedule, ...]
    input_bindings: tuple[StrategyInputBinding, ...]
    limits: StrategyLimits
    asset_class: str = "cn_stock"
    frequency: str = "1m"
    currency: str = "CNY"
    protocol_version: str = STRATEGY_PROTOCOL_VERSION
    state_version: str = STRATEGY_STATE_VERSION

    def __post_init__(self) -> None:
        _check(self.protocol_version == STRATEGY_PROTOCOL_VERSION
               and self.state_version == STRATEGY_STATE_VERSION,
               "策略协议或状态版本不受支持", "strategy_version_unsupported")
        _check(self.asset_class in {"cn_stock", "cn_etf"} and self.currency == "CNY" and self.frequency in {"1m", "1d"},
               "策略仅支持人民币股票与ETF的1m或1d频率", "strategy_capability_unsupported")
        for name in ("strategy_id", "bundle_id", "state_schema_id"):
            _text(getattr(self, name), name)
        _tuple(self.subscriptions, "subscriptions", str)
        _check(bool(self.subscriptions) and set(self.subscriptions) <= set(EVENT_ORDER), "订阅事件不受支持")
        _unique(self.subscriptions, "subscriptions")
        _tuple(self.input_bindings, "input_bindings", StrategyInputBinding)
        _unique(tuple(item.binding_id for item in self.input_bindings), "binding_id")
        _tuple(self.schedules, "schedules", StrategySchedule)
        _unique(tuple(item.schedule_id for item in self.schedules), "schedule_id")
        _check(bool(self.schedules) == ("scheduled" in self.subscriptions), "scheduled订阅与调度声明不一致")
        _check(isinstance(self.limits, StrategyLimits), "limits必须是StrategyLimits")


    def to_dict(self) -> dict:
        return _wire(self)


@dataclass(frozen=True, kw_only=True)
class StrategyPositionView:
    instrument: InstrumentKey
    quantity: int
    sellable_quantity: int
    reserved_sell_quantity: int
    mark_price: Price | None
    mark_available_at: datetime | None

    def __post_init__(self) -> None:
        _stock(self.instrument)
        for name in ("quantity", "sellable_quantity", "reserved_sell_quantity"):
            require_integer_quantity(getattr(self, name), name, allow_zero=True)
        _check(self.sellable_quantity + self.reserved_sell_quantity <= self.quantity,
               "可卖与卖单预占不能超过实际持仓")
        _check((self.mark_price is None) == (self.mark_available_at is None), "估值价及可见时间必须成对")
        if self.mark_price is not None:
            _check(isinstance(self.mark_price, Price) and self.mark_price.currency == "CNY", "估值价必须是人民币Price")
            require_aware_datetime(self.mark_available_at, "mark_available_at")


@dataclass(frozen=True, kw_only=True)
class StrategyOrderView:
    order_id: str
    instrument: InstrumentKey
    side: str
    quantity: int
    filled_quantity: int
    status: str
    submitted_at: datetime
    reserved_cash: Money

    def __post_init__(self) -> None:
        _text(self.order_id, "order_id")
        _stock(self.instrument)
        _check(self.side in {"buy", "sell"}, "订单方向不受支持")
        require_integer_quantity(self.quantity)
        require_integer_quantity(self.filled_quantity, "filled_quantity", allow_zero=True)
        _check(self.status in {"submitted", "accepted", "partially_filled"}, "账户仅交付活动订单")
        _check(self.filled_quantity < self.quantity, "活动订单必须有未成交量")
        _check((self.filled_quantity > 0) == (self.status == "partially_filled"), "部分成交状态与累计量不一致")
        require_aware_datetime(self.submitted_at, "submitted_at")
        _money(self.reserved_cash, "reserved_cash")


@dataclass(frozen=True, kw_only=True)
class StrategyAccountView:
    account_seq: int
    as_of: datetime
    total_cash: Money
    available_cash: Money
    frozen_cash: Money
    withdrawable_cash: Money
    net_asset: Money
    positions: tuple[StrategyPositionView, ...]
    active_orders: tuple[StrategyOrderView, ...]

    def __post_init__(self) -> None:
        require_integer_quantity(self.account_seq, "account_seq", allow_zero=True)
        require_aware_datetime(self.as_of, "as_of")
        for name in ("total_cash", "available_cash", "frozen_cash", "withdrawable_cash", "net_asset"):
            _money(getattr(self, name), name, signed=name == "net_asset")
        _check(len({getattr(self, name).scale for name in (
            "total_cash", "available_cash", "frozen_cash", "withdrawable_cash", "net_asset",
        )}) == 1, "账户金额必须使用同一scale")
        _check(self.available_cash.units + self.frozen_cash.units <= self.total_cash.units,
               "可用与冻结现金不能超过总现金")
        _check(self.withdrawable_cash.units <= self.available_cash.units, "可取资金不能超过可买资金")
        _tuple(self.positions, "positions", StrategyPositionView)
        _tuple(self.active_orders, "active_orders", StrategyOrderView)
        _unique(tuple(item.instrument for item in self.positions), "持仓证券")
        _unique(tuple(item.order_id for item in self.active_orders), "活动订单")
        _check(all(item.reserved_cash.scale == self.frozen_cash.scale for item in self.active_orders),
               "订单预占必须使用账户scale")
        _check(sum(item.reserved_cash.units for item in self.active_orders) <= self.frozen_cash.units,
               "订单预占不能超过账户冻结现金")
        _check(all(item.mark_available_at is None or item.mark_available_at <= self.as_of
                   for item in self.positions), "账户估值使用未来价格", "strategy_input_not_visible")
        _check(all(item.submitted_at <= self.as_of for item in self.active_orders),
               "账户包含未来订单", "strategy_input_not_visible")


@dataclass(frozen=True, kw_only=True)
class StrategyInputRef:
    input_id: str
    binding_id: str
    available_at: datetime
    instrument: InstrumentKey | None = None
    price: Price | None = None
    values: tuple[tuple[str, int | None], ...] = ()
    bar_start: datetime | None = None
    bar_end: datetime | None = None

    def __post_init__(self) -> None:
        _text(self.input_id, "input_id")
        _text(self.binding_id, "binding_id")
        require_aware_datetime(self.available_at, "available_at")
        _check(self.price is None or self.instrument is not None, "参考价必须绑定证券")
        _check(isinstance(self.values, tuple) and all(isinstance(item, tuple) and len(item) == 2
               and isinstance(item[0], str) and (item[1] is None or type(item[1]) is int) for item in self.values),
               "行情列必须是不可变的整数列值")
        _unique(tuple(item[0] for item in self.values), "行情列")
        _check((self.bar_start is None) == (self.bar_end is None), "bar起止时间必须成对")
        if self.bar_start is not None:
            require_aware_datetime(self.bar_start, "bar_start")
            require_aware_datetime(self.bar_end, "bar_end")
            _check(self.bar_start < self.bar_end <= self.available_at, "只能交付完成bar")
        if self.instrument is not None:
            _stock(self.instrument)
            if self.price is not None:
                _check(isinstance(self.price, Price) and self.price.currency == "CNY", "参考价必须是人民币Price")


@dataclass(frozen=True, kw_only=True)
class StrategyFeedback:
    intent_id: str
    order_id: str | None
    kind: str
    event_time: datetime
    filled_quantity: int
    trade_ids: tuple[str, ...]
    reason: str | None = None

    def __post_init__(self) -> None:
        _text(self.intent_id, "intent_id")
        _check(self.kind in {"accepted", "rejected", "partially_filled", "filled", "cancelled", "expired", "cancel_rejected", "unchanged"},
               "反馈类型不受支持")
        if self.order_id is None:
            _check(self.kind in {"rejected", "unchanged"}, "仅无订单的拒绝或目标已满足可缺少order_id")
        else:
            _text(self.order_id, "order_id")
        require_aware_datetime(self.event_time, "event_time")
        require_integer_quantity(self.filled_quantity, "filled_quantity", allow_zero=True)
        _tuple(self.trade_ids, "trade_ids", str)
        for item in self.trade_ids:
            _text(item, "trade_id")
        _unique(self.trade_ids, "trade_ids")
        if self.kind in {"rejected", "cancel_rejected", "expired"}:
            _text(self.reason, "reason")
        if self.kind in {"accepted", "rejected", "unchanged"}:
            _check(self.filled_quantity == 0 and not self.trade_ids, "接受或拒绝回执不能携带成交")
        if self.kind in {"filled", "partially_filled"}:
            _check(self.filled_quantity > 0 and bool(self.trade_ids), "成交反馈须有累计数量与成交引用")


@dataclass(frozen=True, kw_only=True)
class StrategyEvent:
    event_seq: int
    kind: str
    trading_date: date
    event_time: datetime
    decision_time: datetime
    account: StrategyAccountView
    inputs: tuple[StrategyInputRef, ...]
    feedback: tuple[StrategyFeedback, ...]
    schedule_id: str | None = None
    missing_instruments: tuple[str, ...] = ()
    protocol_version: str = STRATEGY_PROTOCOL_VERSION
    market_frame_seq: int = -1
    market_window_bytes: int = 0
    market: object | None = field(default=None, compare=False, repr=False, metadata={"wire": False})

    def __post_init__(self) -> None:
        _check(self.protocol_version == STRATEGY_PROTOCOL_VERSION, "事件版本不受支持", "strategy_version_unsupported")
        _check(type(self.market_frame_seq) is int and self.market_frame_seq >= -1,
               "market_frame_seq必须是大于等于-1的整数")
        require_integer_quantity(self.market_window_bytes, "market_window_bytes", allow_zero=True)
        _check(self.market_frame_seq >= 0 or self.market_window_bytes == 0, "初始行情窗口必须为空")
        require_integer_quantity(self.event_seq, "event_seq", allow_zero=True)
        _check(self.kind in EVENT_ORDER and type(self.trading_date) is date, "事件类型或交易日无效")
        for name in ("event_time", "decision_time"):
            require_aware_datetime(getattr(self, name), name)
        _check(isinstance(self.account, StrategyAccountView), "account必须是只读账户视图")
        _check(self.event_time <= self.decision_time and self.account.as_of == self.decision_time,
               "事件必须已发生，账户须对应当前决策时点", "strategy_input_not_visible")
        _tuple(self.inputs, "inputs", StrategyInputRef)
        _tuple(self.feedback, "feedback", StrategyFeedback)
        _tuple(self.missing_instruments, "missing_instruments", str)
        _unique(self.missing_instruments, "missing_instruments")
        _unique(tuple(item.input_id for item in self.inputs), "input_id")
        _check(all(item.available_at <= self.decision_time for item in self.inputs)
               and all(item.event_time <= self.decision_time for item in self.feedback),
               "策略输入或反馈在决策时尚不可见", "strategy_input_not_visible")
        _check((self.kind == "scheduled") == (self.schedule_id is not None), "定时事件须引用schedule_id")
        if self.schedule_id is not None:
            _text(self.schedule_id, "schedule_id")

    @cached_property
    def wire_bytes(self) -> bytes:
        """不可变事件只编码一次；各边界复用同一控制消息。"""
        return json.dumps(_wire(self), ensure_ascii=False, allow_nan=False,
                          separators=(",", ":")).encode("utf-8")

    def to_dict(self) -> dict:
        return json.loads(self.wire_bytes)


def validate_event(declaration: StrategyDeclaration, event: StrategyEvent) -> None:
    _check(event.kind in declaration.subscriptions, "收到未订阅事件", "strategy_event_unsubscribed")
    _check({item.binding_id for item in event.inputs} <= {item.binding_id for item in declaration.input_bindings},
           "事件输入来源未声明", "strategy_input_undeclared")
    bindings = {item.binding_id: item for item in declaration.input_bindings}
    counts = {}
    for item in event.inputs:
        binding = bindings[item.binding_id]
        _check({key for key, _ in item.values} <= set(binding.columns), "行情列超出声明")
        if binding.source == "signals":
            _check(item.price is None and item.bar_start is None and item.bar_end is None, "信号不能充当行情参考价")
            key = item.binding_id
            counts.setdefault(key, set()).add(item.available_at)
            _check(len(counts[key]) <= binding.lookback_bars, "信号时点窗口超出声明", "strategy_resource_exceeded")
        else:
            _check(False, "bars必须经market列式交付，事件inputs仅接受signals")
    if event.kind == "scheduled":
        _check(event.schedule_id in {item.schedule_id for item in declaration.schedules}, "定时事件未声明")
    _check(len(event.wire_bytes) + event.market_window_bytes <= declaration.limits.max_context_bytes,
           "事件上下文超出预算", "strategy_resource_exceeded")


@dataclass(frozen=True, kw_only=True)
class StrategyIntent:
    intent_id: str
    kind: str
    instrument: InstrumentKey
    side: str | None = None
    quantity: int | None = None
    amount: Money | None = None
    weight: str | None = None
    order_type: str | None = None
    time_in_force: str | None = None
    limit_price: Price | None = None
    protection_price: Price | None = None
    funds_policy: str | None = None
    price_input_id: str | None = None
    order_id: str | None = None

    def __post_init__(self) -> None:
        _text(self.intent_id, "intent_id")
        _stock(self.instrument)
        _check(self.kind in INTENT_KINDS, "交易意图不受支持", "strategy_capability_unsupported")
        if self.kind == "cancel":
            _text(self.order_id, "order_id")
            _check(all(getattr(self, name) is None for name in (
                "side", "quantity", "amount", "weight", "order_type", "time_in_force",
                "limit_price", "protection_price", "funds_policy", "price_input_id",
            )), "撤单不能夹带交易字段")
            return
        _check(self.order_id is None, "新意图不能指定已有order_id")
        _check(self.order_type in {"market", "limit"} and self.time_in_force in {"DAY", "IOC"}
               and self.funds_policy in {"reject", "resize"}, "订单类型、有效期或资金政策不受支持",
               "strategy_capability_unsupported")
        _text(self.price_input_id, "price_input_id")
        if self.order_type == "limit":
            _check(isinstance(self.limit_price, Price) and self.limit_price.currency == "CNY", "限价需要人民币Price")
        else:
            _check(self.limit_price is None, "市价委托不能带limit_price")
        if self.protection_price is not None:
            _check(self.order_type == "market" and isinstance(self.protection_price, Price)
                   and self.protection_price.currency == "CNY", "保护价仅用于人民币市价委托")
        target = self.kind.startswith("target_")
        _check(self.side is None if target else self.side in {"buy", "sell"}, "目标意图由差额决定方向，普通委托须声明side")
        field = {"quantity": "quantity", "target_quantity": "quantity", "amount": "amount",
                 "target_amount": "amount", "target_weight": "weight"}[self.kind]
        _check(all(getattr(self, name) is None for name in {"quantity", "amount", "weight"} - {field}),
               "意图数量、金额和权重不能混用")
        if field == "quantity":
            require_integer_quantity(self.quantity, "quantity", allow_zero=target)
        elif field == "amount":
            _money(self.amount, "amount")
            _check(target or self.amount.units > 0, "金额委托必须为正，零目标可用于清仓")
        else:
            _check(isinstance(self.weight, str), "weight必须是十进制字符串")
            try:
                weight = Decimal(self.weight)
            except ArithmeticError as exc:
                raise StrategyProtocolError("strategy_schema_invalid", "weight必须是十进制字符串") from exc
            _check(weight.is_finite() and 0 <= weight <= 1, "现货目标权重必须在0到1之间")

    def to_dict(self) -> dict:
        return _wire(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StrategyIntent":
        if isinstance(payload, Mapping) and "protection_price" not in payload:
            payload = dict(payload, protection_price=None)
        _check(isinstance(payload, Mapping) and set(payload) == {item.name for item in fields(cls)},
               "StrategyIntent字段不匹配")
        values = dict(payload)
        try:
            values["instrument"] = InstrumentKey.from_dict(values["instrument"])
            for name, value_type in (("amount", Money), ("limit_price", Price), ("protection_price", Price)):
                if values[name] is not None:
                    values[name] = value_type(**values[name])
            return cls(**values)
        except (TypeError, KeyError) as exc:
            raise StrategyProtocolError("strategy_schema_invalid", "交易意图字段类型不匹配") from exc


@dataclass(frozen=True, kw_only=True)
class StrategyResponse:
    event_seq: int
    state_schema_id: str
    state_json: bytes
    intents: tuple[StrategyIntent, ...]

    def __post_init__(self) -> None:
        require_integer_quantity(self.event_seq, "event_seq", allow_zero=True)
        _text(self.state_schema_id, "state_schema_id")
        _check(type(self.state_json) is bytes, "state_json必须是不可变字节")
        _tuple(self.intents, "intents", StrategyIntent)

    def to_dict(self) -> dict:
        return {
            "protocol_version": STRATEGY_PROTOCOL_VERSION,
            "event_seq": self.event_seq,
            "state_version": STRATEGY_STATE_VERSION,
            "state_schema_id": self.state_schema_id,
            "state": json.loads(self.state_json),
            "intents": [item.to_dict() for item in self.intents],
        }


def _state_bytes(state: object) -> bytes:
    def visit(value: object) -> None:
        if type(value) is dict:
            _check(all(type(key) is str for key in value), "state对象键必须是字符串")
            for item in value.values():
                visit(item)
        elif type(value) is list:
            for item in value:
                visit(item)
        else:
            _check(value is None or type(value) in {str, int, bool}
                   or (type(value) is float and math.isfinite(value)), "state只接受有限JSON值")

    _check(type(state) is dict, "state根必须是JSON对象")
    visit(state)
    return json.dumps(state, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def parse_strategy_response(
    payload: Mapping[str, object], *, declaration: StrategyDeclaration, event: StrategyEvent,
) -> StrategyResponse:
    """解析已发布事件的应答；参考价由订单执行器在换算时读取。"""
    _check(isinstance(payload, Mapping) and set(payload) == {
        "protocol_version", "event_seq", "state_version", "state_schema_id", "state", "intents",
    }, "策略应答字段不匹配")
    _check(payload["protocol_version"] == declaration.protocol_version
           and payload["state_version"] == declaration.state_version,
           "策略应答版本不匹配", "strategy_version_unsupported")
    _check(type(payload["event_seq"]) is int and payload["event_seq"] == event.event_seq,
           "策略应答引用了其他事件", "strategy_response_mismatch")
    _check(payload["state_schema_id"] == declaration.state_schema_id,
           "状态schema身份与冻结声明不一致", "strategy_response_mismatch")
    state_json = _state_bytes(payload["state"])
    _check(len(state_json) <= declaration.limits.max_state_bytes,
           "state超出预算", "strategy_resource_exceeded")
    raw_intents = payload["intents"]
    _check(isinstance(raw_intents, (list, tuple)), "intents必须是数组")
    _check(len(raw_intents) <= declaration.limits.max_intents_per_event,
           "意图数量超出预算", "strategy_resource_exceeded")
    intents = tuple(StrategyIntent.from_dict(item) for item in raw_intents)
    _check(not intents or event.kind not in {"initialize", "before_trading", "after_trading"}, "初始化、盘前和盘后不提交交易意图")
    for index, intent in enumerate(intents):
        _check(intent.intent_id == f"{event.event_seq}:{index}", "intent_id必须由事件序号和批内顺序组成")
    response = StrategyResponse(event_seq=event.event_seq, state_schema_id=declaration.state_schema_id,
                                state_json=state_json, intents=intents)
    # state_json已规范化；占位值只计算其余字段，避免再次遍历和编码完整状态。
    envelope = {
        "protocol_version": STRATEGY_PROTOCOL_VERSION,
        "event_seq": response.event_seq,
        "state_version": STRATEGY_STATE_VERSION,
        "state_schema_id": response.state_schema_id,
        "state": None,
        "intents": [item.to_dict() for item in response.intents],
    }
    encoded = json.dumps(envelope, ensure_ascii=False, allow_nan=False,
                         separators=(",", ":")).encode("utf-8")
    response_size = len(encoded) - len(b"null") + len(state_json)
    _check(response_size <= declaration.limits.max_response_bytes,
           "应答消息超出预算", "strategy_resource_exceeded")
    return response


def validate_event_transition(previous: StrategyEvent, current: StrategyEvent) -> None:
    """核对相邻交付事件的序号与阶段，不生成事件或推进交易时钟。"""
    _check(current.event_seq == previous.event_seq + 1,
           "事件序号必须连续递增", "strategy_event_order_invalid")
    _check(current.decision_time >= previous.decision_time
           and current.trading_date >= previous.trading_date
           and current.account.account_seq >= previous.account.account_seq,
           "决策时点、交易日或账户序号不能倒退", "strategy_event_order_invalid")
    _check(current.kind != "initialize", "initialize不能重复交付", "strategy_event_order_invalid")
    if current.decision_time == previous.decision_time:
        _check(EVENT_ORDER.index(current.kind) >= EVENT_ORDER.index(previous.kind),
               "同刻事件阶段不能倒退", "strategy_event_order_invalid")
        if current.kind == previous.kind:
            _check(current.kind == "scheduled" and current.schedule_id > previous.schedule_id,
                   "同刻仅不同定时事件可按schedule_id顺序重复阶段", "strategy_event_order_invalid")
