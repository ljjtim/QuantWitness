"""资源准入与确定性 ready queue。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import ResourceBudget
from .errors import RuntimeAdmissionError


DEFAULT_RUNTIME_MEMORY_BYTES = 16 * 1024**3
DEFAULT_RUNTIME_SCRATCH_BYTES = 64 * 1024**3


def available_cpu_slots() -> int:
    """返回当前进程可用的逻辑 CPU 数，无法读取 affinity 时退回系统计数。"""

    affinity_reader = getattr(os, "sched_getaffinity", None)
    if callable(affinity_reader):
        try:
            affinity = affinity_reader(0)
        except (OSError, NotImplementedError):
            affinity = None
        if affinity:
            return len(affinity)
    count = os.cpu_count()
    return count if type(count) is int and count > 0 else 1


class ExecutionMode(str, Enum):
    DETERMINISTIC_SERIAL = "deterministic_serial"
    BOUNDED_PARALLEL = "bounded_parallel"
    PARTITIONED_BATCH = "partitioned_batch"


@dataclass(frozen=True)
class ResourceCapacity:
    memory_bytes: int
    cpu_slots: int
    temp_bytes: int
    max_workers: int

    def __post_init__(self) -> None:
        positive = (self.memory_bytes, self.cpu_slots, self.max_workers)
        if any(type(value) is not int or value <= 0 for value in positive):
            raise RuntimeAdmissionError("运行内存、CPU 和 worker 容量必须是正整数")
        if type(self.temp_bytes) is not int or self.temp_bytes < 0:
            raise RuntimeAdmissionError("运行 scratch/temp 容量必须是非负整数")

    def to_dict(self) -> dict[str, int]:
        return {
            "memory_bytes": self.memory_bytes,
            "cpu_slots": self.cpu_slots,
            "temp_bytes": self.temp_bytes,
            "max_workers": self.max_workers,
        }


@dataclass(frozen=True, order=True)
class ReadyCandidate:
    topological_level: int
    node_id: str
    partition_key: str | None
    budget: ResourceBudget

    @property
    def key(self) -> str:
        return self.node_id if self.partition_key is None else f"{self.node_id}#{self.partition_key}"


@dataclass(frozen=True, order=True)
class ProjectReadyCandidate:
    project_id: str
    run_id: str
    block_id: str
    candidate: ReadyCandidate

    @property
    def key(self) -> str:
        return f"{self.project_id}/{self.run_id}/{self.block_id}/{self.candidate.key}"


class ProjectFairQueue:
    """以 parameter block 为最小单元，在项目间做确定性轮转。"""

    def __init__(self) -> None:
        self._last_project: str | None = None

    def order(self, candidates: tuple[ProjectReadyCandidate, ...]) -> tuple[ProjectReadyCandidate, ...]:
        grouped: dict[str, list[ProjectReadyCandidate]] = {}
        for item in candidates:
            grouped.setdefault(item.project_id, []).append(item)
        projects = sorted(grouped)
        if self._last_project in projects:
            pivot = (projects.index(self._last_project) + 1) % len(projects)
            projects = projects[pivot:] + projects[:pivot]
        for values in grouped.values():
            values.sort(key=lambda item: (item.candidate.topological_level, item.run_id, item.block_id, item.candidate.node_id))
        ordered: list[ProjectReadyCandidate] = []
        while any(grouped.values()):
            for project in projects:
                if grouped[project]:
                    ordered.append(grouped[project].pop(0))
        if ordered:
            # 调用方可以只取前 N 个填满固定 worker 池；下轮从首个已获机会项目之后开始。
            self._last_project = ordered[0].project_id
        return tuple(ordered)


class ReservationLedger:
    def __init__(self, capacity: ResourceCapacity, mode: ExecutionMode) -> None:
        self.capacity = capacity
        self.mode = mode
        self._active: dict[str, ResourceBudget] = {}

    @property
    def active_keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._active))

    def _limit_workers(self) -> int:
        return 1 if self.mode is ExecutionMode.DETERMINISTIC_SERIAL else self.capacity.max_workers

    def _fits(self, budget: ResourceBudget) -> bool:
        used_memory = sum(item.memory_bytes for item in self._active.values())
        used_cpu = sum(item.cpu_slots for item in self._active.values())
        used_temp = sum(item.temp_bytes for item in self._active.values())
        return (
            len(self._active) < self._limit_workers()
            and used_memory + budget.memory_bytes <= self.capacity.memory_bytes
            and used_cpu + budget.cpu_slots <= self.capacity.cpu_slots
            and used_temp + budget.temp_bytes <= self.capacity.temp_bytes
        )

    def require_single_node_admission(self, budget: ResourceBudget) -> None:
        if budget.memory_bytes > self.capacity.memory_bytes or budget.cpu_slots > self.capacity.cpu_slots or budget.temp_bytes > self.capacity.temp_bytes:
            raise RuntimeAdmissionError("节点声明预算超过运行总容量")

    def reserve(self, candidate: ReadyCandidate) -> None:
        self.require_single_node_admission(candidate.budget)
        if candidate.key in self._active:
            raise RuntimeAdmissionError("资源 reservation 重复")
        if not self._fits(candidate.budget):
            raise RuntimeAdmissionError("当前资源不足，拒绝超卖")
        self._active[candidate.key] = candidate.budget

    def release(self, key: str) -> None:
        if key not in self._active:
            raise RuntimeAdmissionError("释放了不存在的 reservation")
        del self._active[key]

    def select(self, candidates: tuple[ReadyCandidate, ...]) -> tuple[ReadyCandidate, ...]:
        selected: list[ReadyCandidate] = []
        shadow = ReservationLedger(self.capacity, self.mode)
        shadow._active = dict(self._active)
        for candidate in sorted(candidates, key=lambda item: (item.topological_level, item.node_id, item.partition_key or "")):
            shadow.require_single_node_admission(candidate.budget)
            if shadow._fits(candidate.budget):
                shadow.reserve(candidate)
                selected.append(candidate)
        return tuple(selected)


@dataclass(frozen=True)
class ResourceToken:
    token_id: str
    reservation_key: str
    budget: ResourceBudget


class MemoryGovernor:
    """为固定 worker 池签发内存、CPU 和临时空间联合令牌。"""

    def __init__(self, capacity: ResourceCapacity, mode: ExecutionMode) -> None:
        self._ledger = ReservationLedger(capacity, mode)
        self._tokens: dict[str, ResourceToken] = {}

    @property
    def active_tokens(self) -> tuple[ResourceToken, ...]:
        return tuple(self._tokens[key] for key in sorted(self._tokens))

    def issue(self, candidate: ReadyCandidate) -> ResourceToken:
        self._ledger.reserve(candidate)
        token_id = typed_canonical_hash(
            {
                "reservation_key": candidate.key,
                "memory_bytes": candidate.budget.memory_bytes,
                "cpu_slots": candidate.budget.cpu_slots,
                "temp_bytes": candidate.budget.temp_bytes,
                "wall_seconds": candidate.budget.wall_seconds,
            }
        )
        token = ResourceToken(token_id, candidate.key, candidate.budget)
        self._tokens[token_id] = token
        return token

    def release(self, token: ResourceToken) -> None:
        active = self._tokens.get(token.token_id)
        if active != token:
            raise RuntimeAdmissionError("资源令牌未知或已释放")
        self._ledger.release(token.reservation_key)
        del self._tokens[token.token_id]


def stable_partition_keys(values: dict[str, object]) -> tuple[str, ...]:
    return tuple(sorted(values))


def stable_partition_merge(values: dict[str, object]) -> tuple[tuple[str, object], ...]:
    return tuple((key, values[key]) for key in stable_partition_keys(values))


__all__ = [
    "DEFAULT_RUNTIME_MEMORY_BYTES", "DEFAULT_RUNTIME_SCRATCH_BYTES", "ExecutionMode",
    "MemoryGovernor", "ProjectFairQueue", "ProjectReadyCandidate", "ReadyCandidate",
    "ReservationLedger", "ResourceCapacity", "ResourceToken", "available_cpu_slots",
    "stable_partition_keys", "stable_partition_merge",
]
