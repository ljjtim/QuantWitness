"""编译期可消费的 target/asset/backend 能力矩阵。"""

from __future__ import annotations

from typing import Mapping

from .errors import MainlineError


class TradingCapabilityError(MainlineError):
    """ResearchPackage 的交易能力声明不成立。"""

    error_code = "trading_capability_invalid"


EXECUTION_CAPABILITY_MATRIX: Mapping[str, Mapping[str, object]] = {
    "cash-daily-v1": {
        "target_types": ("weight",),
        "asset_classes": ("cn_etf", "cn_stock"),
        "position_effects": ("auto",),
        "supports_short": False,
        "maximum_leverage": 1.0,
        "supports_fx": False,
    },
    "cn-futures-daily-v1": {
        "target_types": ("quantity",),
        "asset_classes": ("cn_future",),
        "position_effects": ("close", "close_today", "close_yesterday", "open"),
        "supports_short": True,
        "maximum_leverage": 20.0,
        "supports_fx": False,
    },
}


def require_declared_trading_capability(
    *,
    backend_id: str,
    target_type: str,
    asset_classes: tuple[str, ...],
    base_currency: str,
    short_allowed: object,
    leverage_limit: object,
) -> Mapping[str, object]:
    """校验 plan 中尚未物化的 target/asset/backend 能力声明。"""
    capability = EXECUTION_CAPABILITY_MATRIX.get(backend_id)
    if capability is None:
        raise TradingCapabilityError("execution_backend 未注册")
    if target_type not in capability["target_types"]:
        raise TradingCapabilityError("execution backend 不支持声明的 target_type")
    if not asset_classes or asset_classes != tuple(sorted(set(asset_classes))):
        raise TradingCapabilityError("asset_classes 必须非空、唯一并排序")
    unsupported_assets = set(asset_classes) - set(capability["asset_classes"])
    if unsupported_assets:
        raise TradingCapabilityError(
            f"execution backend 不支持声明的资产类别: {sorted(unsupported_assets)}"
        )
    if type(short_allowed) is not bool:
        raise TradingCapabilityError("short_allowed 必须是布尔值")
    if short_allowed and not capability["supports_short"]:
        raise TradingCapabilityError("execution backend 不支持声明的 short")
    if isinstance(leverage_limit, bool) or not isinstance(leverage_limit, (int, float)):
        raise TradingCapabilityError("leverage_limit 必须是数值")
    if not 1 <= float(leverage_limit) <= float(capability["maximum_leverage"]):
        raise TradingCapabilityError("声明的 leverage_limit 超过 execution backend 上限")
    if base_currency != "CNY" and not capability["supports_fx"]:
        raise TradingCapabilityError("非 CNY plan 缺少 FX model/backend 能力")
    return capability


__all__ = [
    "EXECUTION_CAPABILITY_MATRIX",
    "TradingCapabilityError",
    "require_declared_trading_capability",
]
