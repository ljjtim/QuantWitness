"""可跨进程稳定编码的金融定点值对象。"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from numbers import Integral

from research_pipeline.platform.canonical import typed_canonical_hash

from .models import DomainContractError


MAX_DECIMAL128_UNITS = 10**38 - 1


@dataclass(frozen=True)
class FixedValue:
    units: int
    scale: int
    currency: str

    def __post_init__(self) -> None:
        if isinstance(self.units, bool) or not isinstance(self.units, Integral):
            raise DomainContractError("units 必须是整数")
        if abs(int(self.units)) > MAX_DECIMAL128_UNITS:
            raise DomainContractError("units 超出 decimal128 范围")
        if isinstance(self.scale, bool) or not isinstance(self.scale, Integral) or not 0 <= int(self.scale) <= 18:
            raise DomainContractError("scale 必须是 0..18 的整数")
        if not isinstance(self.currency, str) or not self.currency.strip():
            raise DomainContractError("currency 必须是非空字符串")

    @classmethod
    def from_decimal(cls, value: object, *, scale: int, currency: str) -> "FixedValue":
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise DomainContractError("金融值无法转换为 Decimal") from exc
        quantum = Decimal(1).scaleb(-scale)
        if decimal_value != decimal_value.quantize(quantum):
            raise DomainContractError("金融值精度超过声明 scale，禁止隐式舍入")
        return cls(int(decimal_value.scaleb(scale)), scale, currency)

    @property
    def decimal(self) -> Decimal:
        return Decimal(self.units).scaleb(-self.scale)

    @property
    def value_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {"units": int(self.units), "scale": int(self.scale), "currency": self.currency}

    def _require_compatible(self, other: object) -> "FixedValue":
        if not isinstance(other, type(self)):
            raise DomainContractError("金融值类型不一致")
        if (self.scale, self.currency) != (other.scale, other.currency):
            raise DomainContractError("金融值 scale/currency 不一致")
        return other

    def __add__(self, other: object) -> "FixedValue":
        right = self._require_compatible(other)
        return type(self)(self.units + right.units, self.scale, self.currency)

    def __sub__(self, other: object) -> "FixedValue":
        right = self._require_compatible(other)
        return type(self)(self.units - right.units, self.scale, self.currency)

    def multiply_quantity(self, quantity: int) -> "FixedValue":
        if isinstance(quantity, bool) or not isinstance(quantity, Integral):
            raise DomainContractError("quantity 必须是整数")
        return type(self)(self.units * int(quantity), self.scale, self.currency)


@dataclass(frozen=True)
class Money(FixedValue):
    """现金或费用；币种必须显式。"""


@dataclass(frozen=True)
class Price(FixedValue):
    """未复权真实成交价格。"""

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.units <= 0:
            raise DomainContractError("Price 必须为正数")

    def notional(self, quantity: int) -> Money:
        value = self.multiply_quantity(quantity)
        return Money(value.units, value.scale, value.currency)


def require_integer_quantity(value: object, field: str = "quantity", *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise DomainContractError(f"{field} 必须是整数")
    result = int(value)
    if result < 0 or (result == 0 and not allow_zero):
        raise DomainContractError(f"{field} 必须是{'非负' if allow_zero else '正'}整数")
    return result


__all__ = ["FixedValue", "MAX_DECIMAL128_UNITS", "Money", "Price", "require_integer_quantity"]
