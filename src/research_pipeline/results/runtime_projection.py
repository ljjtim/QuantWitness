"""Runtime 合同的只读解析与事件投影，供上层可信消费者复用。"""

from __future__ import annotations

from dataclasses import dataclass
import json
from types import MappingProxyType
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash

from .errors import ResultContractError


OPERATOR_DAG_RUN_VERSION = "research-runtime-operator-dag-run-v3"
EVENT_VERSION = "research-runtime-event-v1"

_RUN_TRANSITIONS = {
    None: {"created"},
    "created": {"planned", "cancelled"},
    "planned": {"running", "cancelled"},
    "running": {"paused", "succeeded", "failed", "cancelled"},
    "paused": {"running", "failed", "cancelled"},
    "succeeded": set(),
    "failed": set(),
    "cancelled": set(),
}
_NODE_TRANSITIONS = {
    None: {"pending"},
    "pending": {"ready", "blocked", "cancelled"},
    "ready": {"running", "cancelled", "blocked"},
    "running": {"retryable_failed", "succeeded", "exhausted", "cancelled"},
    "retryable_failed": {"ready", "exhausted", "cancelled"},
    "succeeded": set(),
    "exhausted": set(),
    "cancelled": set(),
    "blocked": set(),
}
_ATTEMPT_TRANSITIONS = {
    None: {"pending"},
    "pending": {"admitted", "cancelled"},
    "admitted": {"ready", "cancelled"},
    "ready": {"running", "cancelled"},
    "running": {"succeeded", "failed", "cancelled", "lost"},
    "succeeded": set(),
    "failed": set(),
    "cancelled": set(),
    "lost": set(),
}
_EVENT_FIELDS = {
    "seq", "event_id", "run_id", "kind", "payload", "previous_hash", "event_hash",
    "occurred_at", "command_id", "node_id", "attempt_id", "contract_version",
}


@dataclass(frozen=True)
class RuntimeRunProjection:
    project_id: str
    run_id: str
    parent_run_id: str | None
    dag_id: str
    status: str
    mode: str
    fixed_clock: str
    event_chain_head: str
    node_statuses: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "node_statuses",
            MappingProxyType(dict(sorted(self.node_statuses.items()))),
        )


def load_runtime_run_projection(
    *,
    record_content: bytes,
    event_content: bytes,
) -> RuntimeRunProjection:
    """按 Runtime 自身合同解析已由调用方完成内容绑定的两个文件。"""

    try:
        record = json.loads(record_content)
        if not isinstance(record, dict):
            raise ResultContractError("runtime run record 必须是对象")
        projection = _replay_runtime_events(event_content)
        version = record.get("contract_version")
        if version != OPERATOR_DAG_RUN_VERSION:
            raise ResultContractError("runtime run record 版本不受支持")
        required = ("project_id", "run_id", "dag_id", "status", "mode", "fixed_clock")
        if any(not isinstance(record.get(field), str) or not record[field] for field in required):
            raise ResultContractError("runtime run record 身份字段无效")
        dag_payload = record.get("dag")
        if not isinstance(dag_payload, dict):
            raise ResultContractError("operator DAG run record 缺少 DAG 合同")
        if record["dag_id"] != typed_canonical_hash(dag_payload):
            raise ResultContractError("runtime run record 的 DAG 身份不一致")
        nodes = dag_payload.get("nodes")
        if not isinstance(nodes, list) or not nodes:
            raise ResultContractError("operator DAG run record 的节点合同无效")
        node_ids = []
        for node in nodes:
            if not isinstance(node, Mapping) or not isinstance(node.get("node_id"), str):
                raise ResultContractError("operator DAG run record 的节点合同无效")
            node_ids.append(str(node["node_id"]))
        if len(node_ids) != len(set(node_ids)):
            raise ResultContractError("operator DAG run record 的 node_id 重复")
        if set(projection.node_statuses or {}) != set(node_ids):
            raise ResultContractError("runtime 事件链节点集合与 DAG 不一致")
        parent_run_id = record.get("parent_run_id")
        if parent_run_id is not None and not isinstance(parent_run_id, str):
            raise ResultContractError("runtime parent_run_id 无效")
        if (
            record["status"] != "succeeded"
            or projection.run_status != "succeeded"
            or projection.run_id != record["run_id"]
            or not projection.node_statuses
            or set(projection.node_statuses.values()) != {"succeeded"}
        ):
            raise ResultContractError("可信运行必须由完整 succeeded 事件链证明")
        recorded_chain_head = record.get("event_chain_head")
        if recorded_chain_head is not None and recorded_chain_head != projection.chain_head:
            raise ResultContractError("runtime run record 与事件链摘要不一致")
        return RuntimeRunProjection(
            project_id=str(record["project_id"]),
            run_id=str(record["run_id"]),
            parent_run_id=parent_run_id,
            dag_id=str(record["dag_id"]),
            status="succeeded",
            mode=str(record["mode"]),
            fixed_clock=str(record["fixed_clock"]),
            event_chain_head=projection.chain_head,
            node_statuses=projection.node_statuses,
        )
    except ResultContractError:
        raise
    except Exception as exc:
        raise ResultContractError("可信 runtime 工件无法解析或重放") from exc


@dataclass
class _RuntimeProjection:
    run_id: str | None = None
    run_status: str | None = None
    node_statuses: dict[str, str] | None = None
    attempt_statuses: dict[str, str] | None = None
    last_seq: int = 0
    chain_head: str = ""

    def __post_init__(self) -> None:
        if self.node_statuses is None:
            self.node_statuses = {}
        if self.attempt_statuses is None:
            self.attempt_statuses = {}


def _transition(
    current: str | None,
    target: str,
    table: Mapping[str | None, set[str]],
    label: str,
) -> str:
    if target not in table.get(current, set()):
        raise ResultContractError(f"runtime {label} 非法状态转移: {current} -> {target}")
    return target


def _replay_runtime_events(content: bytes) -> _RuntimeProjection:
    projection = _RuntimeProjection()
    for line_number, line in enumerate(content.splitlines(keepends=True), 1):
        if not line.endswith(b"\n"):
            raise ResultContractError(
                f"runtime event log 尾部截断: line {line_number}"
            )
        payload = json.loads(line)
        if not isinstance(payload, dict) or set(payload) != _EVENT_FIELDS:
            raise ResultContractError("runtime event 必须是对象")
        if payload.get("contract_version") != EVENT_VERSION:
            raise ResultContractError("runtime event 版本不受支持")
        identity = {key: payload[key] for key in _EVENT_FIELDS if key != "event_hash"}
        if payload.get("event_hash") != typed_canonical_hash(identity):
            raise ResultContractError("runtime event 内容 hash 校验失败")
        seq = payload.get("seq")
        previous_hash = payload.get("previous_hash")
        if type(seq) is not int or seq != projection.last_seq + 1:
            raise ResultContractError("runtime event 序号不连续")
        if previous_hash != projection.chain_head:
            raise ResultContractError("runtime event 前序 hash 不连续")
        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ResultContractError("runtime event run_id 无效")
        if projection.run_id is not None and projection.run_id != run_id:
            raise ResultContractError("runtime event log 混入其他 run")
        event_payload = payload.get("payload")
        if not isinstance(event_payload, dict):
            raise ResultContractError("runtime event payload 必须是对象")
        target = event_payload.get("status")
        kind = payload.get("kind")
        node_id = payload.get("node_id")
        attempt_id = payload.get("attempt_id")
        if kind == "run_status_changed":
            if not isinstance(target, str):
                raise ResultContractError("runtime run 状态无效")
            projection.run_status = _transition(
                projection.run_status, target, _RUN_TRANSITIONS, "run",
            )
        elif kind == "node_status_changed":
            if not isinstance(node_id, str) or not node_id or not isinstance(target, str):
                raise ResultContractError("runtime node 状态事件无效")
            assert projection.node_statuses is not None
            projection.node_statuses[node_id] = _transition(
                projection.node_statuses.get(node_id), target, _NODE_TRANSITIONS, "node",
            )
        elif kind == "attempt_status_changed":
            if (
                not isinstance(node_id, str)
                or not node_id
                or not isinstance(attempt_id, str)
                or not attempt_id
                or not isinstance(target, str)
            ):
                raise ResultContractError("runtime attempt 状态事件无效")
            assert projection.attempt_statuses is not None
            projection.attempt_statuses[attempt_id] = _transition(
                projection.attempt_statuses.get(attempt_id),
                target,
                _ATTEMPT_TRANSITIONS,
                "attempt",
            )
        elif kind not in {
            "execution_completed", "checkpoint_prepared", "checkpoint_committed", "diagnostic",
        }:
            raise ResultContractError("runtime event kind 不受支持")
        projection.run_id = run_id
        projection.last_seq = seq
        projection.chain_head = str(payload["event_hash"])
    return projection


__all__ = [
    "RuntimeRunProjection",
    "load_runtime_run_projection",
]
