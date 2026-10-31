"""股票历史规则与合成行情的纯测试夹具。"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.domain import InstrumentKey, PortfolioTarget
from research_pipeline.domain.trading import PortfolioTargetEntry
from research_pipeline.platform import canonical_json

ZONE = ZoneInfo("Asia/Shanghai")


def _rule(code, board, start, end, tax, *, available="2024-01-01T00:00:00+08:00"):
    minimum = 200 if board == "star" else 100
    step = 1 if board in {"star", "bse"} else 100
    parameters = {
        "source_instrument_id": code, "stock_board": board, "listing_phase": "regular",
        "is_st": False, "listed_date": "2020-01-01", "delisted_date": "2030-12-31",
        "trading_status": "trading", "price_limit_mode": "bounded", "lot_size": step,
        "buy_min_quantity": minimum, "buy_quantity_step": step,
        "sell_min_quantity": minimum, "sell_quantity_step": step,
        "sell_remainder_allowed": True, "settlement_days": 1,
        "commission_ppm": 300, "min_commission_units": 500,
        "sell_tax_ppm": tax, "transfer_fee_ppm": 10,
    }
    return {
        "rule_id": f"fixture.{board}.{start}", "version": 1, "market": "cn_stock",
        "instrument_type": "stock", "effective_start": start, "effective_end": end,
        "available_time": available, "official_source_id": "fixture.historical-market-rules",
        "evidence_url": "fixture://p6/historical-market-rules",
        "parameters": [[key, value] for key, value in sorted(parameters.items())],
    }


def _case(root, *, code="600000.XSHG", board="sh_main", late=None):
    instrument = InstrumentKey(code, "cn_stock", code.rsplit(".", 1)[-1], "CNY", "stock")
    targets, benchmarks, market = [], [], []
    for day, weight in ((2, .5), (3, 0.)):
        decision = datetime(2024, 1, day - 1, 15, tzinfo=ZONE)
        target = PortfolioTarget(
            decision_time=decision, target_type="weight",
            entries=(PortfolioTargetEntry(instrument, "weight", weight),) if weight else (),
            base_currency="CNY", cash_weight=1-weight, short_allowed=False,
            leverage_limit=1., source_hashes=("d"*64,),
        )
        targets.append({"decision_time": decision, "order_time": datetime(2024, 1, day, 9, 30, tzinfo=ZONE),
                        "target_json": canonical_json(target.to_dict()), "intent_plan_hash": "e"*64})
        benchmarks.append({"code": code, "decision_time": decision, "available_at": decision,
                           "price_units": 10000, "price_scale": 3})
        market.append({"date": date(2024, 1, day), "code": code, "open": 10., "close": 10.,
                       "high_limit": 11., "low_limit": 9., "paused": False})
    for name, rows in (("targets", targets), ("benchmarks", benchmarks), ("market", market)):
        folder = root / name
        folder.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows), folder / "data.parquet")
    rules = [_rule(code, board, "2024-01-02", "2024-01-02", 1000),
             _rule(code, board, "2024-01-03", "2024-01-03", 500,
                   available=late or "2024-01-01T00:00:00+08:00")]
    return {"instrument_codes": [code], "market_rules": {code: rules}, "corporate_actions": [],
            "initial_cash_cny": 4000. if board == "star" else 2000. if board == "bse" else 2100.,
            "calendar_id": "fixture.cn-trading-sessions", "policy_available_at": "2024-01-01T00:00:00+08:00"}


