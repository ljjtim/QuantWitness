"""事件研究项目的确定性虚构公告与日线。"""

from __future__ import annotations


def events() -> tuple[dict[str, object], ...]:
    return (
        {"event_id": "E1", "instrument": "SYN.A", "event_day": "2024-01-03", "revision": 1,
         "decision_at": "2024-01-04T09:30:00+08:00",
         "available_at": "2024-01-03T18:00:00+08:00", "surprise": 0.2},
        {"event_id": "E1", "instrument": "SYN.A", "event_day": "2024-01-03", "revision": 2,
         "decision_at": "2024-01-04T09:30:00+08:00",
         "available_at": "2024-01-08T09:00:00+08:00", "surprise": -0.4},
        {"event_id": "E2", "instrument": "SYN.A", "event_day": "2024-01-04", "revision": 1,
         "decision_at": "2024-01-05T09:30:00+08:00",
         "available_at": "2024-01-04T18:00:00+08:00", "surprise": 0.1},
        {"event_id": "E3", "instrument": "SYN.B", "event_day": "2024-01-04", "revision": 1,
         "decision_at": "2024-01-05T09:30:00+08:00",
         "available_at": "2024-01-04T18:00:00+08:00", "surprise": -0.1},
    )


def prices() -> tuple[dict[str, object], ...]:
    return (
        {"instrument": "SYN.A", "session": "2024-01-03", "close": 10.0,
         "available_at": "2024-01-04T09:30:00+08:00"},
        {"instrument": "SYN.A", "session": "2024-01-04", "close": 10.5,
         "available_at": "2024-01-05T09:30:00+08:00"},
        {"instrument": "SYN.A", "session": "2024-01-05", "close": 10.8,
         "available_at": "2024-01-08T09:30:00+08:00"},
        {"instrument": "SYN.B", "session": "2024-01-04", "close": 20.0,
         "available_at": "2024-01-05T09:30:00+08:00"},
        {"instrument": "SYN.B", "session": "2024-01-05", "close": 19.8,
         "available_at": "2024-01-08T09:30:00+08:00"},
    )
