"""ETF 时间序列项目的确定性虚构记录。"""

from __future__ import annotations


def rows() -> tuple[dict[str, object], ...]:
    sessions = (
        ("2024-01-02", "2024-01-03T09:30:00+08:00"),
        ("2024-01-03", "2024-01-04T09:30:00+08:00"),
        ("2024-01-04", "2024-01-05T09:30:00+08:00"),
        ("2024-01-05", "2024-01-08T09:30:00+08:00"),
    )
    prices = {
        "SYN.ETF.A": ((10.0, 10.0), (10.2, 11.0), (11.1, 12.0), (12.0, 12.6)),
        "SYN.ETF.B": ((20.0, 20.0), (19.7, 19.5), (19.4, 19.0), (19.0, 19.3)),
    }
    return tuple(
        {
            "session": session,
            "instrument": instrument,
            "open": bar[0],
            "close": bar[1],
            "available_at": available_at,
        }
        for instrument, bars in sorted(prices.items())
        for (session, available_at), bar in zip(sessions, bars)
    )
