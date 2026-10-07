"""公开合成ETF行情；价格、交易日和证券都不对应真实市场历史。"""
from datetime import date, timedelta
import math


def sessions():
    result = []
    current = date(2024, 1, 2)
    while len(result) < 102:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return tuple(result)


def instruments():
    return tuple(f"SYN{i:03d}.XSHG" for i in range(10))


def rows(*, include_factor_fields=False):
    """100个价格会话；额外两个日历会话用于标签成熟和次日执行。"""
    result = []
    for number, code in enumerate(instruments()):
        previous = 8.0 + number * 0.7
        for index, session in enumerate(sessions()[:100]):
            overnight = 0.002 * math.sin(index / 3 + number)
            opening = round(previous * (1 + overnight), 2)
            close = round(opening * (1 + 0.004 * math.sin(index / 8 + number / 2) + 0.0005 * (number - 4)), 2)
            result.append({
                "fld_equity_daily_date": session,
                "fld_equity_daily_code": code,
                "fld_equity_daily_close": close,
                "fld_demo_open": opening,
                "fld_demo_high_limit": round(previous * 1.1, 2),
                "fld_demo_low_limit": round(previous * 0.9, 2),
                "fld_demo_paused": False,
            })
            if include_factor_fields:
                result[-1].update(
                    fld_demo_high=round(max(opening, close) + 0.02 + 0.01 * (index % 3), 3),
                    fld_demo_low=round(min(opening, close) - 0.02 - 0.01 * (index % 2), 3),
                    fld_demo_vwap=round(0.4 * opening + 0.6 * close, 3),
                    fld_demo_volume=float(10000 + number * 400 + index * 80 + round(500 * math.sin(index / 3 + number))),
                )
            previous = close
    return tuple(sorted(result, key=lambda row: (row["fld_equity_daily_date"], row["fld_equity_daily_code"])))
