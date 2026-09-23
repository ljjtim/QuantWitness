"""期货期限结构项目的确定性虚构合约快照。"""

from __future__ import annotations


def contracts() -> tuple[dict[str, object], ...]:
    return (
        {"contract": "SYN2402", "expiry": "2024-02-20", "settlement": 100.0,
         "volume": 2000, "available_at": "2024-01-04T16:00:00+08:00"},
        {"contract": "SYN2403", "expiry": "2024-03-20", "settlement": 103.0,
         "volume": 1500, "available_at": "2024-01-04T16:00:00+08:00"},
        {"contract": "SYN2404", "expiry": "2024-04-22", "settlement": 105.0,
         "volume": 600, "available_at": "2024-01-04T16:00:00+08:00"},
    )
