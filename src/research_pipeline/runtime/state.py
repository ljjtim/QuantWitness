"""事件日志的唯一状态投影器。"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import RuntimeIntegrityError, RuntimeStateError
from .events import RuntimeEvent


RUN_TRANSITIONS = {
    None: {"created"}, "created": {"planned", "cancelled"},
    "planned": {"running", "cancelled"}, "running": {"paused", "succeeded", "failed", "cancelled"},
    "paused": {"running", "failed", "cancelled"}, "succeeded": set(), "failed": set(), "cancelled": set(),
}
NODE_TRANSITIONS = {
    None: {"pending"}, "pending": {"ready", "blocked", "cancelled"},
    "ready": {"waiting_for_resources", "running", "cancelled", "blocked"},
    "waiting_for_resources": {"running", "retryable_failed", "exhausted", "cancelled"},
    "running": {"retryable_failed", "succeeded", "exhausted", "cancelled"},
    "retryable_failed": {"ready", "exhausted", "cancelled"}, "succeeded": set(), "exhausted": set(), "cancelled": set(), "blocked": set(),
}
ATTEMPT_TRANSITIONS = {
    None: {"pending"}, "pending": {"admitted", "cancelled"}, "admitted": {"ready", "cancelled"},
    "ready": {"waiting_for_resources", "running", "cancelled"},
    "waiting_for_resources": {"running", "failed", "cancelled", "lost"},
    "running": {"succeeded", "failed", "cancelled", "lost"},
    "succeeded": set(), "failed": set(), "cancelled": set(), "lost": set(),
}


@dataclass(frozen=True)
class RuntimeProjection:
    run_id: str | None = None
    run_status: str | None = None
    node_statuses: dict[str, str] = field(default_factory=dict)
    attempt_statuses: dict[str, str] = field(default_factory=dict)
    last_seq: int = 0
    chain_head: str = ""
    command_ids: frozenset[str] = frozenset()

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id, "run_status": self.run_status, "node_statuses": dict(sorted(self.node_statuses.items())), "attempt_statuses": dict(sorted(self.attempt_statuses.items())), "last_seq": self.last_seq, "chain_head": self.chain_head, "command_ids": sorted(self.command_ids)}


def _transition(current: str | None, target: str, table: dict[str | None, set[str]], label: str) -> str:
    if target not in table.get(current, set()):
        raise RuntimeStateError(f"{label} 非法状态转移: {current} -> {target}")
    return target


class _ProjectionAccumulator:
    """受控写者内部可变；对外只返回隔离的状态快照。"""

    def __init__(self, projection: RuntimeProjection | None = None) -> None:
        projection = projection or RuntimeProjection()
        self.run_id = projection.run_id
        self.run_status = projection.run_status
        self.node_statuses = dict(projection.node_statuses)
        self.attempt_statuses = dict(projection.attempt_statuses)
        self.last_seq = projection.last_seq
        self.chain_head = projection.chain_head
        self.command_ids = set(projection.command_ids)

    def validate(self, event: RuntimeEvent, *, validate_chain: bool = True) -> str:
        event.verify()
        if validate_chain:
            if event.seq != self.last_seq + 1 or event.previous_hash != self.chain_head:
                raise RuntimeIntegrityError("事件序号或前序 hash 不连续")
        if self.run_id is not None and event.run_id != self.run_id:
            raise RuntimeStateError("同一事件库不能混入其他 run")
        target = str(event.payload.get("status", ""))
        if event.kind == "run_status_changed":
            _transition(self.run_status, target, RUN_TRANSITIONS, "run")
        elif event.kind == "node_status_changed":
            if not event.node_id:
                raise RuntimeStateError("node 状态事件缺 node_id")
            _transition(self.node_statuses.get(event.node_id), target, NODE_TRANSITIONS, "node")
        elif event.kind == "attempt_status_changed":
            if not event.node_id or not event.attempt_id:
                raise RuntimeStateError("attempt 状态事件缺关联 ID")
            _transition(self.attempt_statuses.get(event.attempt_id), target, ATTEMPT_TRANSITIONS, "attempt")
        elif event.kind not in {"execution_completed", "checkpoint_prepared", "checkpoint_committed", "diagnostic"}:
            raise RuntimeStateError(f"未知事件类型: {event.kind}")
        return target

    def accept(self, event: RuntimeEvent, target: str) -> None:
        if event.kind == "run_status_changed":
            self.run_status = target
        elif event.kind == "node_status_changed":
            self.node_statuses[event.node_id] = target
        elif event.kind == "attempt_status_changed":
            self.attempt_statuses[event.attempt_id] = target
        self.run_id = event.run_id
        self.last_seq = event.seq
        self.chain_head = event.event_hash
        self.command_ids.add(event.command_id)

    def snapshot(self) -> RuntimeProjection:
        return RuntimeProjection(
            self.run_id, self.run_status, dict(self.node_statuses),
            dict(self.attempt_statuses), self.last_seq, self.chain_head,
            frozenset(self.command_ids),
        )


def apply_event(projection: RuntimeProjection, event: RuntimeEvent, *, validate_chain: bool = True) -> RuntimeProjection:
    state = _ProjectionAccumulator(projection)
    target = state.validate(event, validate_chain=validate_chain)
    state.accept(event, target)
    return state.snapshot()


__all__ = ["ATTEMPT_TRANSITIONS", "NODE_TRANSITIONS", "RUN_TRANSITIONS", "RuntimeProjection", "apply_event"]
