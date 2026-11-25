"""现货每日价格边界：准入和执行共用参考价、比例及可见性校验。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Mapping

from .time import require_aware_datetime


EXPLICIT_PRICE_LIMIT_FIELDS = frozenset({
    "reference_price_kind", "reference_price_units",
    "price_limit_up_ratio_ppm", "price_limit_down_ratio_ppm",
})


class CashPriceLimitError(ValueError):
    """每日价格边界与声明的参考规则不一致。"""


def _integer(parameters: Mapping[str, object], key: str, minimum: int = 1) -> int:
    value = parameters.get(key)
    if type(value) is not int or value < minimum:
        raise CashPriceLimitError(f"现货价格规则缺少整数参数或取值无效: {key}")
    return value


def cash_price_limit_reference(parameters: Mapping[str, object]) -> tuple[int, int, int]:
    """解析前收对称简写或显式参考价与独立上下比例，禁止混用。"""
    if EXPLICIT_PRICE_LIMIT_FIELDS.intersection(parameters):
        if parameters.get("price_limit_mode") != "bounded":
            raise CashPriceLimitError("显式参考价与上下比例只适用于有界价格模式")
        missing = EXPLICIT_PRICE_LIMIT_FIELDS.difference(parameters)
        if missing:
            raise CashPriceLimitError(f"显式价格规则字段缺失: {sorted(missing)}")
        if {"reference_previous_close_units", "price_limit_ratio_ppm"}.intersection(parameters):
            raise CashPriceLimitError("显式价格规则不能混用前收对称简写")
        kind = parameters["reference_price_kind"]
        if kind not in ("previous_close", "issue_price"):
            raise CashPriceLimitError("现货参考价类型必须为previous_close或issue_price")
        if kind == "issue_price" and (
            parameters.get("stock_board") not in ("sh_main", "sz_main", "chinext", "star", "bse")
            or parameters.get("listing_phase") != "ipo"
            or "etf_category" in parameters
        ):
            raise CashPriceLimitError("发行价参考只适用于明确股票板块的IPO阶段")
        reference = _integer(parameters, "reference_price_units")
        up = _integer(parameters, "price_limit_up_ratio_ppm")
        down = _integer(parameters, "price_limit_down_ratio_ppm")
        if down >= 1_000_000:
            raise CashPriceLimitError("现货下跌比例必须小于100%")
        return reference, up, down
    reference = _integer(parameters, "reference_previous_close_units")
    ratio = _integer(parameters, "price_limit_ratio_ppm")
    if ratio >= 1_000_000:
        raise CashPriceLimitError("现货对称价格限制比例无效")
    return reference, ratio, ratio


@dataclass(frozen=True)
class CashPriceLimits:
    """无涨跌幅限制时保留空上下界，不能虚构极大价格。"""

    mode: str
    price_scale: int
    high_limit_units: int | None
    low_limit_units: int | None


def resolve_cash_price_limits(parameters: Mapping[str, object], *, decision_at: datetime) -> CashPriceLimits:
    require_aware_datetime(decision_at, "decision_at")
    mode = parameters.get("price_limit_mode")
    if mode not in ("bounded", "unbounded", "suspended"):
        raise CashPriceLimitError("分钟现货缺少明确价格限制模式")
    scale = _integer(parameters, "price_scale", 0)
    if scale > 8:
        raise CashPriceLimitError("分钟现货报价精度超出支持范围")
    if mode != "bounded" and EXPLICIT_PRICE_LIMIT_FIELDS.intersection(parameters):
        raise CashPriceLimitError("显式参考价与上下比例只适用于有界价格模式")
    if mode == "unbounded":
        if any(parameters.get(key) is not None for key in ("high_limit_units", "low_limit_units", "price_limit_ratio_ppm")):
            raise CashPriceLimitError("无涨跌幅限制阶段不能携带固定涨跌停值")
        return CashPriceLimits(mode, scale, None, None)
    high = _integer(parameters, "high_limit_units", 0 if mode == "suspended" else 1)
    low = _integer(parameters, "low_limit_units", 0 if mode == "suspended" else 1)
    raw = parameters.get("reference_price_available_at")
    if not isinstance(raw, str):
        raise CashPriceLimitError("分钟参考价缺少可见时间")
    try:
        visible = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        require_aware_datetime(visible, "reference_price_available_at")
    except ValueError as exc:
        raise CashPriceLimitError("分钟参考价可见时间无效") from exc
    if visible > decision_at:
        raise CashPriceLimitError("分钟参考价在决策时尚不可见")
    if low > high:
        raise CashPriceLimitError("分钟涨跌停下界高于上界")
    if mode == "suspended":
        _integer(parameters, "reference_previous_close_units")
        if parameters.get("paused") is not True or parameters.get("trading_status") != "suspended":
            raise CashPriceLimitError("停牌价格模式必须绑定当时停牌事实")
        return CashPriceLimits(mode, scale, high, low)
    reference, up, down = cash_price_limit_reference(parameters)
    if parameters.get("price_limit_rounding") != "half_up_to_quote_unit":
        raise CashPriceLimitError("分钟现货必须明确声明报价单位四舍五入规则")
    unit = Decimal(1)
    denominator = Decimal(1_000_000)
    expected_high = int((Decimal(reference) * (1 + Decimal(up) / denominator)).quantize(unit, rounding=ROUND_HALF_UP))
    expected_low = int((Decimal(reference) * (1 - Decimal(down) / denominator)).quantize(unit, rounding=ROUND_HALF_UP))
    if (high, low) != (expected_high, expected_low):
        raise CashPriceLimitError("分钟涨跌停值与可见参考价、上下比例和报价精度不一致")
    return CashPriceLimits(mode, scale, high, low)
