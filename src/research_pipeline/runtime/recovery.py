"""resume、retry-node 与 rerun-from 的纯恢复计划。"""

from __future__ import annotations

from dataclasses import dataclass

from research_pipeline.platform import typed_canonical_hash

from .contracts import RetryPolicy
from .errors import RuntimeStateError
from .graph import DagSpec
from .state import RuntimeProjection


RECOVERY_PLAN_VERSION = "research-runtime-recovery-v1"


@dataclass(frozen=True)
class RecoveryPlan:
    command: str
    run_id: str
    parent_run_id: str | None
    reuse_nodes: tuple[str, ...]
    recompute_nodes: tuple[str, ...]
    blocked_nodes: tuple[str, ...]
    force_recompute_nodes: tuple[str, ...] = ()
    reason: str = ""
    contract_version: str = RECOVERY_PLAN_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "command": self.command, "run_id": self.run_id, "parent_run_id": self.parent_run_id,
            "reuse_nodes": list(self.reuse_nodes), "recompute_nodes": list(self.recompute_nodes),
            "blocked_nodes": list(self.blocked_nodes), "force_recompute_nodes": list(self.force_recompute_nodes),
            "reason": self.reason, "contract_version": self.contract_version,
        }

    @property
    def plan_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


def _descendants(dag: DagSpec, seeds: frozenset[str]) -> frozenset[str]:
    known = {node.node_id for node in dag.nodes}
    if not seeds <= known:
        raise RuntimeStateError("恢复目标不在 DAG 中")
    result = set(seeds)
    changed = True
    while changed:
        changed = False
        for edge in dag.edges:
            if edge.source_node in result and edge.target_node not in result:
                result.add(edge.target_node)
                changed = True
    return frozenset(result)


def _ordered(dag: DagSpec, values: frozenset[str] | set[str]) -> tuple[str, ...]:
    selected = set(values)
    return tuple(node for node in dag.topological_order() if node in selected)


def plan_resume(dag: DagSpec, run_id: str, projection: RuntimeProjection, *, verified_nodes: frozenset[str], invalid_nodes: frozenset[str]) -> RecoveryPlan:
    if projection.run_id != run_id or projection.run_status not in {"planned", "running", "paused"}:
        raise RuntimeStateError("resume 只接受同一非终态 run")
    invalidated = _descendants(dag, invalid_nodes) if invalid_nodes else frozenset()
    successful = frozenset(node for node, status in projection.node_statuses.items() if status == "succeeded")
    reusable = successful & verified_nodes - invalidated
    all_nodes = frozenset(node.node_id for node in dag.nodes)
    recompute = all_nodes - reusable
    return RecoveryPlan("resume", run_id, None, _ordered(dag, reusable), _ordered(dag, recompute), (), reason="只复用已验证成功 checkpoint")


def plan_retry_node(dag: DagSpec, run_id: str, projection: RuntimeProjection, node_id: str, *, attempts_used: int, retry_policy: RetryPolicy, error_code: str) -> RecoveryPlan:
    if projection.run_id != run_id or projection.run_status != "paused" or projection.node_statuses.get(node_id) != "retryable_failed":
        raise RuntimeStateError("retry-node 只接受 paused run 的 retryable_failed 节点")
    if attempts_used >= retry_policy.max_attempts or error_code not in retry_policy.retryable_codes:
        raise RuntimeStateError("错误不可重试或重试次数已耗尽")
    recompute = _descendants(dag, frozenset({node_id}))
    return RecoveryPlan("retry-node", run_id, None, (), _ordered(dag, recompute), (), reason=f"retryable:{error_code}")


def plan_rerun_from(dag: DagSpec, new_run_id: str, parent_run_id: str, projection: RuntimeProjection, node_id: str, *, verified_nodes: frozenset[str]) -> RecoveryPlan:
    if projection.run_id != parent_run_id or projection.run_status not in {"succeeded", "failed", "cancelled"}:
        raise RuntimeStateError("rerun-from 只接受终态父 run")
    forced = _descendants(dag, frozenset({node_id}))
    reuse = verified_nodes - forced
    return RecoveryPlan("rerun-from", new_run_id, parent_run_id, _ordered(dag, reuse), _ordered(dag, forced), (), _ordered(dag, forced), reason=f"force-from:{node_id}")


__all__ = ["RECOVERY_PLAN_VERSION", "RecoveryPlan", "plan_rerun_from", "plan_resume", "plan_retry_node"]
