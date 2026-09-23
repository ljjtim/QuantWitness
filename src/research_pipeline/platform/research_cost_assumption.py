"""研究费用假设的小型跨层合同。"""

from __future__ import annotations

from datetime import date
from typing import Mapping


class ResearchCostAssumptionError(ValueError):
    """费用假设字段、单位、数值或适用窗口无效。"""


def normalize_stock_research_cost_assumption(
    value: Mapping[str, object],
    *,
    research_start: date,
    research_end: date,
) -> dict[str, object]:
    """校验 A 股费用只是带适用窗口的研究假设。"""

    expected = {
        "contract_version",
        "assumption_id",
        "currency",
        "rate_unit",
        "minimum_fee_unit",
        "slippage_unit",
        "applicable_start",
        "applicable_end",
        "commission_ppm",
        "min_commission_units",
        "sell_tax_ppm",
        "transfer_fee_ppm",
        "slippage_per_share_units",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ResearchCostAssumptionError(
            "research_cost_assumption 字段不完整或含未知字段"
        )
    if value.get("contract_version") != "research-cost-assumption-v1":
        raise ResearchCostAssumptionError("research_cost_assumption 版本不受支持")
    assumption_id = value.get("assumption_id")
    if not isinstance(assumption_id, str) or not assumption_id.strip():
        raise ResearchCostAssumptionError("research_cost_assumption 缺少 assumption_id")
    if (
        value.get("currency") != "CNY"
        or value.get("rate_unit") != "ppm_of_notional"
        or value.get("minimum_fee_unit") != "CNY_cent"
        or value.get("slippage_unit") != "CNY_cent_per_share"
    ):
        raise ResearchCostAssumptionError(
            "research_cost_assumption 单位必须为 CNY、ppm、分和分/股"
        )
    numeric_fields = (
        "commission_ppm",
        "min_commission_units",
        "sell_tax_ppm",
        "transfer_fee_ppm",
        "slippage_per_share_units",
    )
    if any(
        type(value.get(field)) is not int or int(value[field]) < 0
        for field in numeric_fields
    ):
        raise ResearchCostAssumptionError(
            "research_cost_assumption 数值必须为非负整数"
        )
    if (
        type(research_start) is not date
        or type(research_end) is not date
        or research_start > research_end
    ):
        raise ResearchCostAssumptionError("研究日期窗口无效")
    try:
        applicable_start = date.fromisoformat(str(value["applicable_start"]))
        applicable_end = date.fromisoformat(str(value["applicable_end"]))
    except ValueError as exc:
        raise ResearchCostAssumptionError(
            "research_cost_assumption 日期无效"
        ) from exc
    if applicable_start > research_start or applicable_end < research_end:
        raise ResearchCostAssumptionError(
            "research_cost_assumption 未覆盖完整研究窗口"
        )
    return {
        "contract_version": "research-cost-assumption-v1",
        "assumption_id": assumption_id,
        "currency": "CNY",
        "rate_unit": "ppm_of_notional",
        "minimum_fee_unit": "CNY_cent",
        "slippage_unit": "CNY_cent_per_share",
        "applicable_start": applicable_start.isoformat(),
        "applicable_end": applicable_end.isoformat(),
        **{field: int(value[field]) for field in numeric_fields},
    }


__all__ = [
    "ResearchCostAssumptionError",
    "normalize_stock_research_cost_assumption",
]
