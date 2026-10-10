"""显式提交与撤单流的不可变输入合同。"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime
import json
import math
from typing import Mapping

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.asset_taxonomy import MINUTE_TARGET_ASSET_CLASSES

from .models import DomainContractError
from .time import require_aware_datetime
from .trading import InstrumentKey
from .values import Price


@dataclass(frozen=True, kw_only=True)
class ExplicitOrderCommand:
    command_id: str
    action: str
    order_id: str
    instrument: InstrumentKey
    decision_time: datetime
    submitted_at: datetime
    available_at: datetime
    source_sequence: int
    source_hashes: tuple[str, ...]
    trading_date: date
    side: str | None = None
    quantity: int | None = None
    position_effect: str | None = None
    order_type: str | None = None
    time_in_force: str | None = None
    limit_price: Price | None = None
    reference_price: Price | None = None
    reference_price_available_at: datetime | None = None
    funds_policy: str | None = None
    slippage_bps: float = 0.0
    slippage_ticks: int = 0

    def __post_init__(self) -> None:
        for name in ("command_id", "order_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise DomainContractError(f"{name} 必须是非空字符串")
        if self.action not in ("submit", "cancel"):
            raise DomainContractError("action 只能是 submit 或 cancel")
        if not isinstance(self.instrument, InstrumentKey):
            raise DomainContractError("instrument 必须是 InstrumentKey")
        for name in ("decision_time", "submitted_at", "available_at"):
            require_aware_datetime(getattr(self, name), name)
        if self.decision_time > self.submitted_at or self.available_at > self.submitted_at:
            raise DomainContractError("decision_time/available_at 不能晚于 submitted_at")
        if type(self.trading_date) is not date:
            raise DomainContractError("trading_date 必须是显式交易会话 date")
        if type(self.source_sequence) is not int or self.source_sequence < 0:
            raise DomainContractError("source_sequence 必须是非负整数")
        if not isinstance(self.source_hashes, tuple) or not self.source_hashes:
            raise DomainContractError("source_hashes 必须是非空 tuple")
        for value in self.source_hashes:
            if (not isinstance(value, str) or len(value) != 64
                    or any(char not in "0123456789abcdef" for char in value)):
                raise DomainContractError("source_hashes 必须是 sha256 小写摘要")
        if self.source_hashes != tuple(sorted(set(self.source_hashes))):
            raise DomainContractError("source_hashes 必须排序且唯一")
        if (type(self.slippage_bps) not in {int, float}
                or not math.isfinite(self.slippage_bps) or self.slippage_bps < 0):
            raise DomainContractError("slippage_bps 必须是有限非负数")
        if type(self.slippage_ticks) is not int or self.slippage_ticks < 0:
            raise DomainContractError("slippage_ticks 必须是非负整数")
        if self.action == "cancel":
            if any(getattr(self, name) is not None for name in (
                "side", "quantity", "position_effect", "order_type", "time_in_force",
                "limit_price", "reference_price", "reference_price_available_at", "funds_policy",
            )) or self.slippage_bps != 0 or self.slippage_ticks != 0:
                raise DomainContractError("cancel 交易字段必须为 None，滑点必须为零")
            return
        if self.instrument.asset_class not in MINUTE_TARGET_ASSET_CLASSES:
            raise DomainContractError("显式订单只支持股票、ETF 和真实期货合约")
        if self.side not in ("buy", "sell"):
            raise DomainContractError("side 只能是 buy 或 sell")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise DomainContractError("quantity 必须是正整数")
        if self.instrument.asset_class == "cn_future":
            if self.instrument.contract_kind not in {"future", "future_contract"}:
                raise DomainContractError("期货订单必须绑定真实合约")
            if self.position_effect not in ("open", "close", "close_today", "close_yesterday"):
                raise DomainContractError("期货订单必须显式声明开平仓方向")
        elif self.position_effect != "auto":
            raise DomainContractError("股票和 ETF position_effect 只能是 auto")
        if self.order_type not in ("market", "limit"):
            raise DomainContractError("order_type 只能是 market 或 limit")
        if self.time_in_force not in ("IOC", "DAY"):
            raise DomainContractError("time_in_force 只能是 IOC 或 DAY")
        if self.funds_policy not in ("reject", "resize"):
            raise DomainContractError("funds_policy 只能是 reject 或 resize")
        if self.order_type == "market" and self.limit_price is not None:
            raise DomainContractError("market 不能声明 limit_price")
        if self.order_type == "limit":
            self._require_price(self.limit_price, "limit_price")
        self._require_price(self.reference_price, "reference_price")
        require_aware_datetime(self.reference_price_available_at, "reference_price_available_at")
        if self.reference_price_available_at > self.decision_time:
            raise DomainContractError("reference_price 在决策时尚不可见")

    def _require_price(self, value: object, name: str) -> None:
        if not isinstance(value, Price) or value.currency != self.instrument.currency:
            raise DomainContractError(f"{name} 必须是与标的同币种的 Price")

    @property
    def command_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        payload = {field.name: getattr(self, field.name) for field in fields(self)}
        payload["instrument"] = self.instrument.to_dict()
        payload["source_hashes"] = list(self.source_hashes)
        for name in ("decision_time", "submitted_at", "available_at", "trading_date",
                     "reference_price_available_at"):
            value = getattr(self, name)
            payload[name] = None if value is None else value.isoformat()
        for name in ("limit_price", "reference_price"):
            value = getattr(self, name)
            payload[name] = None if value is None else value.to_dict()
        payload["slippage_bps"] = float(self.slippage_bps)
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplicitOrderCommand:
        _exact_keys(payload, {field.name for field in fields(cls)}, "ExplicitOrderCommand")
        values = dict(payload)
        _exact_keys(payload["instrument"], {field.name for field in fields(InstrumentKey)}, "instrument")
        values["instrument"] = InstrumentKey.from_dict(payload["instrument"])
        for name in ("decision_time", "submitted_at", "available_at",
                     "reference_price_available_at"):
            value = payload[name]
            if value is None and name == "reference_price_available_at":
                continue
            if not isinstance(value, str):
                raise DomainContractError(f"{name} 必须是带时区的 ISO 时间字符串")
            try:
                values[name] = datetime.fromisoformat(value)
            except ValueError as exc:
                raise DomainContractError(f"{name} 必须是带时区的 ISO 时间字符串") from exc
        value = payload["trading_date"]
        if not isinstance(value, str):
            raise DomainContractError("trading_date 必须是 ISO 日期字符串")
        try:
            values["trading_date"] = date.fromisoformat(value)
        except ValueError as exc:
            raise DomainContractError("trading_date 必须是 ISO 日期字符串") from exc
        if values["trading_date"].isoformat() != value:
            raise DomainContractError("trading_date 必须是 YYYY-MM-DD")
        for name in ("limit_price", "reference_price"):
            value = payload[name]
            if value is not None:
                _exact_keys(value, {"units", "scale", "currency"}, name)
                values[name] = Price(**value)
        hashes = payload["source_hashes"]
        if not isinstance(hashes, (list, tuple)):
            raise DomainContractError("source_hashes 必须是列表")
        values["source_hashes"] = tuple(hashes)
        return cls(**values)


def _exact_keys(payload: object, expected: set[str], name: str) -> None:
    if not isinstance(payload, Mapping) or set(payload) != expected:
        raise DomainContractError(f"{name} schema 不匹配")


def parse_order_commands(payload: object) -> tuple[ExplicitOrderCommand, ...]:
    """保留声明顺序，验证提交身份、撤单引用与提交时钟。"""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (ValueError, TypeError) as exc:
            raise DomainContractError("order_commands 必须是完整 JSON 数组") from exc
    if not isinstance(payload, (list, tuple)):
        raise DomainContractError("order_commands 必须是数组")
    commands = tuple(
        item if isinstance(item, ExplicitOrderCommand) else ExplicitOrderCommand.from_dict(item)
        for item in payload
    )
    command_ids: set[str] = set()
    submitted: dict[str, ExplicitOrderCommand] = {}
    previous_clock: tuple[datetime, int] | None = None
    for command in commands:
        if command.command_id in command_ids:
            raise DomainContractError("command_id 在同一流内必须唯一")
        command_ids.add(command.command_id)
        clock = (command.submitted_at, command.source_sequence)
        if previous_clock is not None and clock < previous_clock:
            raise DomainContractError("订单流必须按 submitted_at/source_sequence 排序")
        previous_clock = clock
        if command.action == "submit":
            if command.order_id in submitted:
                raise DomainContractError("submit order_id 在同一流内必须唯一")
            submitted[command.order_id] = command
        else:
            original = submitted.get(command.order_id)
            if original is None:
                raise DomainContractError("cancel 必须引用此前的 submit")
            if command.instrument != original.instrument:
                raise DomainContractError("cancel 标的与 submit 不一致")
            if (clock <= (original.submitted_at, original.source_sequence)
                    or command.decision_time < original.submitted_at):
                raise DomainContractError("cancel 时钟必须在 submit 之后")
    return commands


__all__ = ["ExplicitOrderCommand", "parse_order_commands"]
