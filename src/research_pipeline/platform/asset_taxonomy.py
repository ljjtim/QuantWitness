"""中国市场资产分类的唯一规范值、兼容别名和最小能力矩阵。"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from .errors import MainlineError


CANONICAL_ASSET_CLASSES = (
    "cn_stock",
    "cn_etf",
    "cn_index",
    "cn_future",
)
DEPRECATED_ASSET_CLASS_ALIASES: Mapping[str, str] = MappingProxyType({
    "cn_fund": "cn_etf",
    "cn_futures": "cn_future",
})


class AssetTaxonomyError(MainlineError):
    """资产类别未知、未规范化或与声明能力不一致。"""

    error_code = "asset_taxonomy_invalid"


@dataclass(frozen=True)
class AssetCapability:
    asset_class: str
    instrument_types: tuple[str, ...]
    frequencies: tuple[str, ...]
    price_fields: tuple[str, ...]
    trading_calendar: str
    simulation_entries: tuple[str, ...]


ASSET_CAPABILITY_MATRIX: Mapping[str, AssetCapability] = MappingProxyType({
    "cn_stock": AssetCapability(
        "cn_stock",
        ("equity", "stock"),
        ("daily", "minute"),
        ("open", "high", "low", "close", "volume", "money"),
        "cn_stock",
        ("cash_daily", "minute_event", "bar_tca"),
    ),
    "cn_etf": AssetCapability(
        "cn_etf",
        ("etf",),
        ("daily", "minute"),
        ("open", "high", "low", "close", "volume", "money"),
        "cn_stock",
        ("cash_daily", "minute_event", "bar_tca"),
    ),
    "cn_index": AssetCapability(
        "cn_index",
        ("index",),
        ("daily", "minute"),
        ("open", "high", "low", "close", "volume", "money"),
        "cn_stock",
        ("minute_event",),
    ),
    "cn_future": AssetCapability(
        "cn_future",
        ("future", "future_contract", "future_continuous", "future_product"),
        ("daily", "minute"),
        (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "money",
            "open_interest",
            "settlement",
        ),
        "cn_future",
        ("futures_daily", "minute_event", "bar_tca"),
    ),
})
MINUTE_TARGET_ASSET_CLASSES = tuple(
    asset_class
    for asset_class in CANONICAL_ASSET_CLASSES
    if "minute_event" in ASSET_CAPABILITY_MATRIX[asset_class].simulation_entries
    and "bar_tca" in ASSET_CAPABILITY_MATRIX[asset_class].simulation_entries
)


def normalize_asset_class(value: object, *, field: str = "asset_class") -> str:
    """只在声明入口接收历史别名，并返回唯一规范值。"""

    if not isinstance(value, str) or not value or value != value.strip():
        raise AssetTaxonomyError(
            f"{field} 不受支持；支持={list(CANONICAL_ASSET_CLASSES)}"
        )
    canonical = DEPRECATED_ASSET_CLASS_ALIASES.get(value, value)
    if canonical not in ASSET_CAPABILITY_MATRIX:
        raise AssetTaxonomyError(
            f"{field} 不受支持；支持={list(CANONICAL_ASSET_CLASSES)}"
        )
    return canonical


def require_canonical_asset_class(
    value: object,
    *,
    field: str = "asset_class",
) -> str:
    """内部合同只接受规范值，禁止兼容别名继续向下游传播。"""

    canonical = normalize_asset_class(value, field=field)
    if canonical != value:
        raise AssetTaxonomyError(
            f"{field} 必须使用规范值 {canonical}；历史别名 {value} 仅允许出现在声明入口"
        )
    return canonical


def asset_capability(value: object) -> AssetCapability:
    canonical = require_canonical_asset_class(value)
    return ASSET_CAPABILITY_MATRIX[canonical]


def require_instrument_type(asset_class: object, instrument_type: object) -> None:
    capability = asset_capability(asset_class)
    if instrument_type not in capability.instrument_types:
        raise AssetTaxonomyError(
            "instrument_type 与 asset_class 不一致；"
            f"{capability.asset_class} 支持={list(capability.instrument_types)}"
        )


def require_frequency(asset_class: object, frequency: object) -> None:
    capability = asset_capability(asset_class)
    if frequency not in capability.frequencies:
        raise AssetTaxonomyError(
            "frequency 与 asset_class 不一致；"
            f"{capability.asset_class} 支持={list(capability.frequencies)}"
        )


def require_price_fields(asset_class: object, price_fields: object) -> None:
    capability = asset_capability(asset_class)
    if not isinstance(price_fields, (tuple, list)):
        raise AssetTaxonomyError("price_fields 必须是字段列表")
    unsupported = [field for field in price_fields if field not in capability.price_fields]
    if unsupported:
        raise AssetTaxonomyError(
            "price_fields 与 asset_class 不一致；"
            f"不支持={unsupported}，支持={list(capability.price_fields)}"
        )


def require_simulation_entry(asset_class: object, simulation_entry: object) -> None:
    capability = asset_capability(asset_class)
    if simulation_entry not in capability.simulation_entries:
        raise AssetTaxonomyError(
            "simulation_entry 与 asset_class 不一致；"
            f"{capability.asset_class} 支持={list(capability.simulation_entries)}"
        )


__all__ = [
    "ASSET_CAPABILITY_MATRIX",
    "CANONICAL_ASSET_CLASSES",
    "DEPRECATED_ASSET_CLASS_ALIASES",
    "MINUTE_TARGET_ASSET_CLASSES",
    "AssetCapability",
    "AssetTaxonomyError",
    "asset_capability",
    "normalize_asset_class",
    "require_canonical_asset_class",
    "require_frequency",
    "require_instrument_type",
    "require_price_fields",
    "require_simulation_entry",
]
