"""节点资源向量及进程内采样，用于单机及共享资源池。"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time
from typing import Mapping

import psutil

from research_pipeline.platform.process_resources import (
    _measure_scratch_scopes, _nonoverlapping_roots, _project_process_usage,
    _register_attempt_sampler, _unregister_attempt_sampler,
    _owned_process_descendants,
)

from .contracts import ResourceBudget
from .errors import RuntimeAdmissionError, RuntimeIntegrityError, RuntimeWorkerError


@dataclass(frozen=True)
class ResourceVector:
    memory_bytes: int
    cpu_slots: int
    scratch_bytes: int | None
    process_slots: int

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for key, value in self.to_dict().items() if not (key == "scratch_bytes" and value is None)):
            raise RuntimeAdmissionError("资源向量必须由非负整数组成")
        if not any(self.to_dict().values()):
            raise RuntimeAdmissionError("资源向量不能全为零")

    @classmethod
    def from_budget(cls, budget: ResourceBudget, *, process_slots: int = 1) -> "ResourceVector":
        return cls(budget.memory_bytes, budget.cpu_slots, budget.temp_bytes, process_slots)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResourceVector":
        if set(payload) != {"memory_bytes", "cpu_slots", "scratch_bytes", "process_slots"}:
            raise RuntimeIntegrityError("资源向量 schema 无效")
        return cls(*(payload[key] for key in (  # type: ignore[arg-type]
            "memory_bytes", "cpu_slots", "scratch_bytes", "process_slots"
        )))

    def to_dict(self) -> dict[str, int]:
        return {
            "memory_bytes": self.memory_bytes,
            "cpu_slots": self.cpu_slots,
            "scratch_bytes": self.scratch_bytes,
            "process_slots": self.process_slots,
        }

    def plus(self, other: "ResourceVector") -> "ResourceVector":
        return ResourceVector(*(None if self.to_dict()[key] is None or other.to_dict()[key] is None else self.to_dict()[key] + other.to_dict()[key] for key in self.to_dict()))

    def fits_within(self, capacity: "ResourceVector") -> bool:
        return all(capacity.to_dict()[key] is None or (self.to_dict()[key] is not None and self.to_dict()[key] <= capacity.to_dict()[key]) for key in self.to_dict())


class ResourceUsageSampler:
    """独立采样 attempt 进程树和 scratch，不信任算子自报峰值。"""

    def __init__(
        self, scratch_root: str | Path, *,
        additional_scratch_roots: tuple[str | Path, ...] = (),
        interval_seconds: float = 0.05,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("资源采样间隔必须为正数")
        self.scratch_root = Path(scratch_root).resolve()
        self.scratch_roots = (self.scratch_root, *(Path(root).resolve() for root in additional_scratch_roots))
        self.interval_seconds = interval_seconds
        self.peak_rss_bytes = 0
        self.peak_scratch_bytes = 0
        self.max_processes = 1
        self.measurement_status = "available"
        self.wall_milliseconds = 0
        self._started = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._baseline_descendants: frozenset[tuple[int, float]] | None = None
        self._sampling_lock = threading.RLock()
        self._last_project_sample_at = float("-inf")
        self._last_generic_sample_at = float("-inf")
        self._project_sample_key = None
        self._project_scratch_key = None
        self._project_process_sample = None
        self._project_scratch_sample = None
        self._last_scratch_sample_at = float("-inf")
        self._governor_scratch_sizes = None
        self._last_worker_scratch_at = float("-inf")

    def __enter__(self) -> "ResourceUsageSampler":
        self._started = time.monotonic()
        self._sample()
        self._thread = threading.Thread(target=self._run, name="resource-usage-sampler", daemon=True)
        self._thread.start()
        _register_attempt_sampler(self)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self._stop.set()
            if self._thread is not None:
                self._thread.join(timeout=max(1.0, self.interval_seconds * 4))
            with self._sampling_lock:
                self._last_project_sample_at = float("-inf")
                self._last_generic_sample_at = float("-inf")
                self._sample(force_scratch=True)
            self.wall_milliseconds = max(1, round((time.monotonic() - self._started) * 1000))
        finally:
            _unregister_attempt_sampler(self)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            # 同周期读数已计入峰值时，后台不再争用监督器的采样锁。
            if time.monotonic() - max(self._last_project_sample_at, self._last_generic_sample_at) >= self.interval_seconds:
                self._sample()

    def sample_project_usage(self, worker_pid: int | None, observed_descendants: dict[int, float],
                             scratch_roots: tuple[Path, ...], *,
                             windows_job_name: str | None = None, force: bool = False,
                             force_scratch: bool = False) -> tuple[int, int, int]:
        """复用监督器本轮真实测量，治理峰值和 Worker 硬门禁共享同一读数。"""
        with self._sampling_lock:
            try:
                roots = tuple(Path(root).resolve() for root in scratch_roots)
                worker_started = None
                if worker_pid is not None:
                    try:
                        worker_started = psutil.Process(worker_pid).create_time()
                    except psutil.NoSuchProcess:
                        pass
                key = (worker_pid, worker_started, id(observed_descendants), windows_job_name)
                scratch_key = (key, roots)
                now = time.monotonic()
                changed = key != self._project_sample_key
                if force or changed or self._project_process_sample is None or now - self._last_project_sample_at >= self.interval_seconds:
                    if windows_job_name is None:
                        self._project_process_sample = _project_process_usage(worker_pid, observed_descendants)
                    else:
                        self._project_process_sample = _project_process_usage(
                            worker_pid, observed_descendants, windows_job_name=windows_job_name)
                    self._last_project_sample_at = now
                rss, count = self._project_process_sample
                # 节点历史目录按秒采样；子任务边界只强制当前实际工作范围。
                governor_due = self._governor_scratch_sizes is None or now - self._last_scratch_sample_at >= 1.0
                worker_due = force or force_scratch or scratch_key != self._project_scratch_key or self._project_scratch_sample is None or now - self._last_worker_scratch_at >= 1.0
                if governor_due:
                    sizes = _measure_scratch_scopes((*self.scratch_roots, *roots))
                    self._governor_scratch_sizes = {path: sizes[path] for path in self.scratch_roots}
                    self._last_scratch_sample_at = now
                    self._project_scratch_sample = {path: sizes[path] for path in roots}
                    self._last_worker_scratch_at = now
                elif worker_due:
                    self._project_scratch_sample = _measure_scratch_scopes(roots)
                    self._last_worker_scratch_at = now
                    for path in self.scratch_roots:
                        if path in self._project_scratch_sample:
                            self._governor_scratch_sizes[path] = self._project_scratch_sample[path]
                sizes = self._project_scratch_sample
                self._project_sample_key = key
                self._project_scratch_key = scratch_key
                governor_size = sum(self._governor_scratch_sizes[path] for path in _nonoverlapping_roots(self.scratch_roots))
                worker_size = sum(sizes[path] for path in _nonoverlapping_roots(roots))
            except (psutil.Error, OSError, RuntimeError):
                self.measurement_status = "measurement_unavailable"
                raise
            self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
            self.max_processes = max(self.max_processes, count)
            self.peak_scratch_bytes = max(self.peak_scratch_bytes, governor_size)
            return rss, count, worker_size

    def _sample(self, *, force_scratch=False) -> None:
        with self._sampling_lock:
            self._sample_unlocked(force_scratch=force_scratch)

    def _sample_scratch_unlocked(
        self,
        *,
        force_scratch: bool = False,
        roots_changed: bool = False,
        measure_scratch=None,
        monotonic=None,
    ) -> bool:
        clock = time.monotonic if monotonic is None else monotonic
        measure = _measure_scratch_scopes if measure_scratch is None else measure_scratch
        now = clock()
        if not force_scratch and not roots_changed and now - self._last_scratch_sample_at < 1.0:
            return False
        roots = tuple(self.scratch_roots)
        try:
            sizes = measure(roots)
            self._last_scratch_sample_at = clock()
            self._governor_scratch_sizes = sizes
            current_scratch_bytes = sum(sizes[path] for path in _nonoverlapping_roots(roots))
            self.peak_scratch_bytes = max(self.peak_scratch_bytes, current_scratch_bytes)
            return True
        except (OSError, RuntimeError):
            self.measurement_status = "measurement_unavailable"
            return False

    def _sample_unlocked(self, *, force_scratch=False) -> None:
        if time.monotonic() - max(self._last_project_sample_at, self._last_generic_sample_at) >= self.interval_seconds:
            try:
                root = psutil.Process(os.getpid())
                descendants = _owned_process_descendants(root)
                identities = {}
                for item in descendants:
                    try:
                        identities[(item.pid, item.create_time())] = item
                    except psutil.NoSuchProcess:
                        continue
                if self._baseline_descendants is None:
                    self._baseline_descendants = frozenset(identities)
                processes = [
                    root,
                    *(
                        item
                        for identity, item in identities.items()
                        if identity not in self._baseline_descendants
                    ),
                ]
                rss = root.memory_info().rss
                for item in processes[1:]:
                    try:
                        rss += item.memory_info().rss
                    except psutil.NoSuchProcess:
                        continue
                self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
                self.max_processes = max(self.max_processes, len(processes))
                self._last_generic_sample_at = time.monotonic()
            except (psutil.Error, OSError, RuntimeError):
                self.measurement_status = "measurement_unavailable"
        self._sample_scratch_unlocked(force_scratch=force_scratch)



def _require_sampled_resources(
    sampler: ResourceUsageSampler, *, budget: ResourceBudget,
    reservation: ResourceVector,
) -> None:
    if sampler.measurement_status != "available":
        raise RuntimeWorkerError(
            "节点资源测量不可用，不能发布成功 checkpoint",
            error_code="resource_measurement_unavailable",
            failure_payload={"code": "resource_measurement_unavailable"},
        )
    limits = {
        "memory_bytes": (sampler.peak_rss_bytes, min(budget.memory_bytes, reservation.memory_bytes)),
        "temp_bytes": (sampler.peak_scratch_bytes, min((value for value in (budget.temp_bytes, reservation.scratch_bytes) if value is not None), default=None)),
        "process_slots": (sampler.max_processes, reservation.process_slots),
    }
    if budget.wall_seconds:
        limits["wall_milliseconds"] = (sampler.wall_milliseconds, budget.wall_seconds * 1000)
    exceeded = {name: {"actual": actual, "limit": limit} for name, (actual, limit) in limits.items() if limit is not None and actual > limit}
    if exceeded:
        raise RuntimeWorkerError(
            "节点资源使用超过执行预算或租约",
            error_code="resource_exceeded",
            failure_payload={"code": "resource_exceeded", "exceeded": exceeded},
        )


__all__ = ["ResourceUsageSampler", "ResourceVector"]
