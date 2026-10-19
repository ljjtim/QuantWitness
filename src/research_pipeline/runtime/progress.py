"""仅供运行观察的真实计数和时间估计，不进入研究身份。"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import time


class ProgressCounter:
    """累计确认完成的单位；心跳和阶段切换不推进计数。"""

    def __init__(self, unit: str, total: int, completed: int = 0) -> None:
        if type(total) is not int or type(completed) is not int or not 0 <= completed <= total:
            raise ValueError("进度计数必须满足 0 <= completed <= total")
        self.unit = unit
        self.total = total
        self.completed = completed
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.last_progress_at: str | None = None
        self._started = time.monotonic()
        self._last = self._started
        self._initial = completed
        self._samples: deque[float] = deque(maxlen=8)

    def advance(self, completed: int) -> None:
        if type(completed) is not int or not self.completed <= completed <= self.total:
            raise ValueError("进度只能递增且不能超过总数")
        if completed == self.completed:
            return
        now = time.monotonic()
        self._samples.append(max(0.0, now - self._last) / (completed - self.completed))
        self._last = now
        self.completed = completed
        self.last_progress_at = datetime.now(timezone.utc).isoformat()

    def snapshot(self, *, estimate: bool = False) -> dict[str, object]:
        elapsed = max(0.0, time.monotonic() - self._started)
        advanced = self.completed - self._initial
        remaining = self.total - self.completed
        eta = None
        if estimate and len(self._samples) >= 2 and remaining:
            pending = max(0.0, time.monotonic() - self._last)
            # 当前批已用时间只抵扣一个单位，避免把长批误报为即将完成。
            lower, upper = min(self._samples), max(self._samples)
            if pending <= upper:
                eta = [round(max(0.0, lower - pending) + lower * (remaining - 1), 3),
                       round(max(0.0, upper - pending) + upper * (remaining - 1), 3)]
        return {
            "unit": self.unit, "completed": self.completed, "total": self.total,
            "started_at": self.started_at, "last_progress_at": self.last_progress_at,
            "throughput_per_second": round(advanced / elapsed, 6) if advanced and elapsed > 0 else None,
            "eta_seconds": eta,
            "sample_count": len(self._samples),
            "stalled_after_seconds": max(60.0, 3 * max(self._samples, default=0.0)),
        }


def project_progress(progress, *, phase: str, process_alive: bool, now=None):
    """为 inspect 添加观察状态；没有单位完成事件时不推断停滞。"""
    if progress is None:
        return None
    result = dict(progress)
    counter = result.get("work")
    now = now or datetime.now(timezone.utc)
    last = counter.get("last_progress_at") if counter else None
    age = None if last is None else max(0.0, (now - datetime.fromisoformat(last)).total_seconds())
    if phase == "stopped" or not process_alive:
        status = "inactive"
    elif phase == "waiting_for_resources":
        status = "waiting"
    elif counter and counter["completed"] == counter["total"]:
        status = "completed"
    elif counter and age is not None and phase == "executing":
        status = "stalled" if age >= counter["stalled_after_seconds"] else "advancing"
    else:
        status = "unknown"
    # 停滞或不再执行时，不展示失去依据的剩余时间。
    if counter is not None:
        result["work"] = {**counter, "eta_seconds": (
            counter["eta_seconds"] if status == "advancing" else None
        )}
    last_observed = result.get("last_progress_at") or last
    observed_age = (None if last_observed is None else max(
        0.0, (now - datetime.fromisoformat(last_observed)).total_seconds()
    ))
    return {**result, "status": status, "last_progress_age_seconds": observed_age}
