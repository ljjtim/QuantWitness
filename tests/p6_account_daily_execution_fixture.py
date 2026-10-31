"""账户起点、持仓批次和开盘估值的纯测试夹具。"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.spot_account import AccountSource, OpeningCash, SpotAcquisitionLot, SpotOpeningMark, SpotOpeningSnapshot

ZONE = ZoneInfo("Asia/Shanghai")


def payload(*, lots=100, restricted=False):
    start = datetime(2024, 1, 2, 9, tzinfo=ZONE)
    source = AccountSource("fixture:opening", start - timedelta(days=1))
    key = InstrumentKey("600000.XSHG", "cn_stock", "XSHG", "CNY", "stock").instrument_hash
    lot = SpotAcquisitionLot("opening-lot", key, lots, lots * 800,
        date(2023, 12, 1), start + timedelta(days=2) if restricted else start,
        "market_buy", "equity", source)
    opening = SpotOpeningSnapshot("initial", "default", "CNY", start, start, "declared-investor",
        False, OpeningCash(1000000, 0, 0, source), (lot,), (), (), (), source)
    marks = (SpotOpeningMark(key, 1000, 1, source.available_at, source),)
    return {"opening_snapshot": opening.to_dict(), "opening_marks": [item.to_dict() for item in marks],
            "tax_rules": [], "dividend_plans": [], "conversion_plans": [], "tax_instructions": []}


