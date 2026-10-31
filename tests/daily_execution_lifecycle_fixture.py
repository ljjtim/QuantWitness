"""现货历史规则、目标和行情的纯测试夹具。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from research_pipeline.domain import InstrumentKey, MarketRuleSnapshot, PortfolioTarget
from research_pipeline.domain.trading import PortfolioTargetEntry
from research_pipeline.platform import canonical_json


_ZONE = ZoneInfo("Asia/Shanghai")
_SESSIONS = tuple(date(2024, 1, day) for day in (2, 3, 4))


def _rule(market: str) -> MarketRuleSnapshot:
    stock = market == "cn_stock"
    parameters = {
        "commission_ppm": 300,
        "lot_size": 100,
        "min_commission_units": 500,
        "sell_tax_ppm": 1000 if stock else 0,
        "settlement_days": 1,
        "transfer_fee_ppm": 10 if stock else 0,
    }
    if not stock:
        parameters["etf_category"] = "equity"
    return MarketRuleSnapshot(
        f"{market}-daily-lifecycle", 1, market, "stock" if stock else "etf",
        date(2020, 1, 1), None, datetime(2020, 1, 1, tzinfo=_ZONE),
        "catalog.cash.rules", "https://example.invalid/cash",
        tuple(sorted(parameters.items())),
    )


def _arguments(
    *, market_id: str = "cn_etf", weights: tuple[float, ...] = (0.5, 0.0),
    initial_cash: float = 10000.0, paused: bool = False, opening_price: float = 10.0,
) -> dict[str, object]:
    stock = market_id == "cn_stock"
    code = "600000.XSHG" if stock else "510300.XSHG"
    instrument = InstrumentKey(code, market_id, "XSHG", "CNY", "stock" if stock else "etf")
    plans = []
    for session, weight in zip(_SESSIONS, weights):
        target = PortfolioTarget(
            decision_time=datetime.combine(session - timedelta(days=1), time(15), _ZONE),
            target_type="weight",
            entries=(PortfolioTargetEntry(instrument, "weight", weight),) if weight else (),
            base_currency="CNY", cash_weight=1.0 - weight,
            short_allowed=False, leverage_limit=1.0, source_hashes=("d" * 64,),
        )
        plans.append({
            "order_time": datetime.combine(session, time(9, 30), _ZONE),
            "target_json": canonical_json(target.to_dict()),
            "intent_plan_hash": "e" * 64,
        })
    market = pd.DataFrame([
        {"date": session, "code": code, "open": opening_price, "close": opening_price,
         "high_limit": 11.0, "low_limit": 9.0, "paused": paused}
        for session in _SESSIONS
    ])
    return {
        "market": market, "intent_plans": pd.DataFrame(plans),
        "corporate_actions": (), "rule_by_code": {code: _rule(market_id)},
        "initial_cash_cny": initial_cash, "market_data_artifact_hash": "c" * 64,
        "columns": {name: name for name in market.columns},
    }


