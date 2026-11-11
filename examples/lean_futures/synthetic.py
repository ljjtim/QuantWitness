"""双合约合成期货市场，价格与规则均为事先定义的研究输入。"""
from datetime import date, datetime, timedelta

DAYS = ('2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05')
RUN_DAYS = DAYS[1:3]
CODES = ('IF2401.CCFX', 'IF2403.CCFX')
CLOCK = '2024-01-05T23:59:59+08:00'
AS_OF = '2024-01-04T23:59:59+08:00'


def daily_rows():
    return [dict(date=date.fromisoformat(day), code=code, open=float(100+10*i+100*j),
                 high=float(106+10*i+100*j), low=float(99+10*i+100*j),
                 close=float(105+10*i+100*j), volume=1000.)
            for i, day in enumerate(RUN_DAYS) for j, code in enumerate(CODES)]


def minute_rows():
    rows = []
    for i, day in enumerate(RUN_DAYS):
        for begin, end in (('09:30', '11:30'), ('13:00', '15:00')):
            stamp = datetime.fromisoformat(day+'T'+begin)
            finish = datetime.fromisoformat(day+'T'+end)
            while stamp < finish:
                stamp += timedelta(minutes=1)
                for j, code in enumerate(CODES):
                    price = float(100+10*i+100*j)
                    rows.append(dict(dt=stamp, code=code, open=price, high=price, low=price,
                                     close=price, volume=100., money=price*100*10, avg=price, open_interest=1000.))
    return rows
