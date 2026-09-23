"""标的生命周期、别名和真实合约属性。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from research_pipeline.platform.canonical import typed_canonical_hash

from .models import DomainContractError, InstrumentId
from .time import parse_session


@dataclass(frozen=True)
class Instrument:
    instrument_id: InstrumentId
    listed_date: date
    delisted_date: date | None = None
    exchange: str = ""
    board: str = ""
    contract_multiplier: int | None = None
    price_tick_units: int | None = None
    continuous_signal_only: bool = False

    def __post_init__(self) -> None:
        listed = parse_session(self.listed_date, "listed_date")
        if self.delisted_date is not None and parse_session(self.delisted_date, "delisted_date") < listed:
            raise DomainContractError("delisted_date 早于 listed_date")
        if not self.exchange.strip():
            raise DomainContractError("exchange 不能为空")
        if self.instrument_id.instrument_type == "future":
            if not self.continuous_signal_only and (not self.contract_multiplier or not self.price_tick_units):
                raise DomainContractError("真实期货合约必须声明乘数和最小价位")
        elif self.contract_multiplier is not None or self.price_tick_units is not None:
            raise DomainContractError("现货标的不能声明期货合约属性")

    @property
    def instrument_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id.to_dict(),
            "listed_date": self.listed_date.isoformat(),
            "delisted_date": None if self.delisted_date is None else self.delisted_date.isoformat(),
            "exchange": self.exchange,
            "board": self.board,
            "contract_multiplier": self.contract_multiplier,
            "price_tick_units": self.price_tick_units,
            "continuous_signal_only": self.continuous_signal_only,
        }

    def require_tradable_on(self, session: object) -> None:
        current = parse_session(session)
        if current < self.listed_date or (self.delisted_date is not None and current > self.delisted_date):
            raise DomainContractError("标的不在可交易生命周期内")
        if self.continuous_signal_only:
            raise DomainContractError("连续合约只能用于信号，不能成交")


@dataclass(frozen=True)
class InstrumentAlias:
    instrument_hash: str
    code: str
    board: str
    effective_start: date
    effective_end: date | None = None

    def __post_init__(self) -> None:
        if len(self.instrument_hash) != 64:
            raise DomainContractError("instrument_hash 必须是 sha256")
        if not self.code.strip():
            raise DomainContractError("alias code 不能为空")
        if self.effective_end is not None and self.effective_end < self.effective_start:
            raise DomainContractError("alias 有效期倒置")


def resolve_instrument_alias(aliases: tuple[InstrumentAlias, ...], *, instrument_hash: str, as_of: object) -> InstrumentAlias:
    current = parse_session(as_of)
    matches = tuple(
        item for item in aliases
        if item.instrument_hash == instrument_hash
        and item.effective_start <= current
        and (item.effective_end is None or current <= item.effective_end)
    )
    if len(matches) != 1:
        raise DomainContractError("标的别名快照缺失或重叠")
    return matches[0]


__all__ = ["Instrument", "InstrumentAlias", "resolve_instrument_alias"]
