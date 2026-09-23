"""事件日志的唯一状态投影器。"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from .errors import RuntimeIntegrityError, RuntimeStateError
from .events import RuntimeEvent


RUN_TRANSITIONS = {
    None: {"created"}, "created": {"planned", "cancelled"},
    "planned": {"running", "cancelled"}, "running": {"paused", "succeeded", "failed", "cancelled"},
    "paused": {"running", "failed", "cancelled"}, "succeeded": set(), "failed": set(), "cancelled": set(),
}
NODE_TRANSITIONS = {
    None: {"pending"}, "pending": {"ready", "blocked", "cancelled"},
    "ready": {"running", "cancelled", "blocked"}, "running": {"retryable_failed", "succeeded", "exhausted", "cancelled"},
    "retryable_failed": {"ready", "exhausted", "cancelled"}, "succeeded": set(), "exhausted": set(), "cancelled": set(), "blocked": set(),
}
ATTEMPT_TRANSITIONS = {
    None: {"pending"}, "pending": {"admitted", "cancelled"}, "admitted": {"ready", "cancelled"},
    "ready": {"running", "cancelled"}, "running": {"succeeded", "failed", "cancelled", "lost"},
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


def apply_event(projection: RuntimeProjection, event: RuntimeEvent, *, validate_chain: bool = True) -> RuntimeProjection:
    event.verify()
    if validate_chain:
        if event.seq != projection.last_seq + 1 or event.previous_hash != projection.chain_head:
            raise RuntimeIntegrityError("事件序号或前序 hash 不连续")
    if projection.run_id is not None and event.run_id != projection.run_id:
        raise RuntimeStateError("同一事件库不能混入其他 run")
    run_status = projection.run_status
    nodes = dict(projection.node_statuses)
    attempts = dict(projection.attempt_statuses)
    target = str(event.payload.get("status", ""))
    if event.kind == "run_status_changed":
        run_status = _transition(run_status, target, RUN_TRANSITIONS, "run")
    elif event.kind == "node_status_changed":
        if not event.node_id:
            raise RuntimeStateError("node 状态事件缺 node_id")
        nodes[event.node_id] = _transition(nodes.get(event.node_id), target, NODE_TRANSITIONS, "node")
    elif event.kind == "attempt_status_changed":
        if not event.node_id or not event.attempt_id:
            raise RuntimeStateError("attempt 状态事件缺关联 ID")
        attempts[event.attempt_id] = _transition(attempts.get(event.attempt_id), target, ATTEMPT_TRANSITIONS, "attempt")
    elif event.kind not in {"execution_completed", "checkpoint_prepared", "checkpoint_committed", "diagnostic"}:
        raise RuntimeStateError(f"未知事件类型: {event.kind}")
    return replace(
        projection, run_id=event.run_id, run_status=run_status, node_statuses=nodes,
        attempt_statuses=attempts, last_seq=event.seq, chain_head=event.event_hash,
        command_ids=projection.command_ids | {event.command_id},
    )


__all__ = ["ATTEMPT_TRANSITIONS", "NODE_TRANSITIONS", "RUN_TRANSITIONS", "RuntimeProjection", "apply_event"]
