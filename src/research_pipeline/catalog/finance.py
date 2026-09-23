"""财务观测值的时点可见性门禁。"""

from __future__ import annotations

from datetime import date, datetime

from .errors import CatalogFinancialSemanticsError


def require_financial_observation_visible(
    *,
    report_period: date,
    publication_date: date,
    as_of: date,
    revision_available_at: date | None = None,
    value_semantics: str = "report_period",
    ttm_transform_hash: str | None = None,
) -> None:
    """确认财务值在研究时点已经公开，且 TTM 有可追溯变换。"""

    for name, value in (
        ("report_period", report_period),
        ("publication_date", publication_date),
        ("as_of", as_of),
    ):
        if not isinstance(value, date) or isinstance(value, datetime):
            raise CatalogFinancialSemanticsError(f"{name} 必须是 date")
    if publication_date < report_period:
        raise CatalogFinancialSemanticsError("publication_date 不能早于 report_period")
    visible_at = max(
        publication_date,
        revision_available_at or publication_date,
    )
    if visible_at > as_of:
        raise CatalogFinancialSemanticsError("财务值在 as_of 时点尚不可见")
    if value_semantics == "ttm" and not ttm_transform_hash:
        raise CatalogFinancialSemanticsError("TTM 值必须绑定已审核的 transform hash")


__all__ = ["require_financial_observation_visible"]
