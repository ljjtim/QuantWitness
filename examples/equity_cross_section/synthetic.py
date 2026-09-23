"""股票横截面项目的确定性虚构输入。"""

from __future__ import annotations


def rows() -> tuple[dict[str, object], ...]:
    prices = {
        "SYN.A": (10.0, 11.0, 12.0, 12.3),
        "SYN.B": (10.0, 10.5, 10.2, 10.1),
        "SYN.C": (10.0, 9.5, 9.0, 8.8),
        "SYN.D": (10.0, 10.0, 10.4, 10.6),
    }
    sessions = (
        ("2024-01-02", "2024-01-03T09:30:00+08:00"),
        ("2024-01-03", "2024-01-04T09:30:00+08:00"),
        ("2024-01-04", "2024-01-05T09:30:00+08:00"),
        ("2024-01-05", "2024-01-08T09:30:00+08:00"),
    )
    return tuple(
        {
            "session": session,
            "instrument": instrument,
            "close": closes[index],
            "available_at": available_at,
        }
        for index, (session, available_at) in enumerate(sessions)
        for instrument, closes in sorted(prices.items())
    )
