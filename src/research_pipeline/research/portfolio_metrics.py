"""组合研究的基础收益与风险指标。"""

from __future__ import annotations

from math import sqrt

import pandas as pd


def portfolio_metric_values(
    *,
    returns: pd.Series,
    equity: pd.Series,
    turnover: pd.Series,
    annual_factor: int,
    capital_utilization: pd.Series | None,
    cost_drag: pd.Series | None,
) -> dict[str, float]:
    clean_turnover = _clean_series(turnover)
    values = {
        "total_return": _total_return(returns),
        "max_drawdown": _max_drawdown(equity),
        "annualized_volatility": _annualized_volatility(
            returns,
            annual_factor=annual_factor,
        ),
        "sharpe": _sharpe_ratio(returns, annual_factor=annual_factor),
        "turnover_mean": (
            0.0 if clean_turnover.empty else float(clean_turnover.mean())
        ),
        "portfolio_turnover_mean": (
            0.0 if clean_turnover.empty else float(clean_turnover.mean())
        ),
    }
    if capital_utilization is not None:
        clean_utilization = _clean_series(capital_utilization)
        values["capital_utilization_mean"] = (
            0.0 if clean_utilization.empty else float(clean_utilization.mean())
        )
    if cost_drag is not None:
        clean_cost = _clean_series(cost_drag)
        values["cost_drag_total"] = (
            0.0
            if clean_cost.empty
            else float(1.0 - (1.0 - clean_cost).prod())
        )
    return values


def _clean_series(values: pd.Series) -> pd.Series:
    return pd.Series(values, dtype="float64").dropna()


def _total_return(returns: pd.Series) -> float:
    clean = _clean_series(returns)
    if clean.empty:
        return 0.0
    return float((1.0 + clean).prod() - 1.0)


def _annualized_volatility(returns: pd.Series, *, annual_factor: int) -> float:
    clean = _clean_series(returns)
    if len(clean) < 2:
        return 0.0
    value = clean.std(ddof=0) * sqrt(annual_factor)
    if pd.isna(value):
        return 0.0
    return float(value)


def _sharpe_ratio(returns: pd.Series, *, annual_factor: int) -> float:
    clean = _clean_series(returns)
    if clean.empty:
        return 0.0
    volatility = clean.std(ddof=0)
    if pd.isna(volatility) or volatility == 0:
        return 0.0
    return float(clean.mean() / volatility * sqrt(annual_factor))


def _max_drawdown(equity: pd.Series) -> float:
    clean = _clean_series(equity)
    if clean.empty:
        return 0.0
    drawdown = clean / clean.cummax() - 1.0
    value = drawdown.min()
    if pd.isna(value):
        return 0.0
    return float(value)


__all__ = ["portfolio_metric_values"]
