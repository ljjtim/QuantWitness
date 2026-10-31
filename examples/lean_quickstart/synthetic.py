"""同一证券与日历的确定性合成行情。"""
from datetime import date, datetime, timedelta
CODE = "599999.XSHG"
DAYS = ("2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11")
PRICES = (10., 10., 10.5, 11., 11.)
CLOCK = "2024-01-12T09:00:00+08:00"
def rows(include_factor_fields=True):
    return [dict(fld_equity_daily_date=date.fromisoformat(day), fld_equity_daily_code=CODE,
        fld_equity_daily_close=price, fld_demo_open=price, fld_demo_high=price,
        fld_demo_low=price, fld_demo_volume=1000000.) for day, price in zip(DAYS, PRICES)]
def minute_rows():
    result=[]
    for day, price in zip(DAYS[1:], PRICES[1:]):
        for first, count in (("09:31:00",120),("13:01:00",120)):
            start=datetime.fromisoformat(day+"T"+first)
            for index in range(count):
                result.append(dict(dt=start+timedelta(minutes=index), code=CODE, open=price,
                    high=price, low=price, close=price, volume=1000000., money=price*1000000., avg=price))
    return result
