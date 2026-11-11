"""从终态失败运行显式导入已经成功的节点 checkpoint。"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Mapping

from research_pipeline.platform.canonical import canonical_json

from .checkpoint import CheckpointStore
from .contracts import DeterminismContext
from .errors import RuntimeIntegrityError
from .execution_service import OPERATOR_DAG_RUN_VERSION, RuntimeExecutionService
from .external_artifact import ExternalArtifactStore
from .graph import DagSpec
from .identity import derive_run_id
from .operator_registry import NODE_IDENTITY_PROJECTION_CURRENT
from .recovery import RecoveryPlan, plan_rerun_from
from .required_run_reuse import _ancestor_closure
from .scheduler import ExecutionMode
from .store import EventStore, process_identity_alive
from .liveness import read_runtime_liveness


FAILED_RUN_REUSE_MODE = "failed_run_successful_checkpoints"


def prepare_failed_run_reuse(
    *,
    service: RuntimeExecutionService,
    dag: DagSpec,
    source_run_root: str | Path,
    target_run_root: str | Path,
    project_id: str,
    root_seed: int,
    fixed_clock: str,
    mode: ExecutionMode,
    required_node_ids: tuple[str, ...] = (),
    node_identity_projection: str = NODE_IDENTITY_PROJECTION_CURRENT,
) -> dict[str, object]:
    """完整复验失败运行的成功 checkpoint，并导入独立目标运行。"""

    source_root = Path(source_run_root).resolve(strict=True)
    target_root = Path(target_run_root).resolve()
    if (
        source_root == target_root
        or source_root in target_root.parents
        or target_root in source_root.parents
    ):
        raise RuntimeIntegrityError("失败 run 复用来源与目标不得相同或相互包含")

    projection = EventStore(source_root).replay()
    record = _read_run_record(source_root)
    parent_run_id = record.get("run_id")
    expected_audit = service.audit_environment.to_dict()
    strict = bool(required_node_ids)
    if strict and node_identity_projection != NODE_IDENTITY_PROJECTION_CURRENT:
        raise RuntimeIntegrityError("要求节点复用只接受现行节点局部身份计划")
    terminal_failed = record.get("status") == "failed" and projection.run_status == "failed"
    interrupted = strict and not terminal_failed and record.get("status") in {"created", "planned", "running"} and projection.run_status in {"created", "planned", "running"}
    stopped_source = _interrupted_source_snapshot(source_root, parent_run_id) if interrupted else None
    if (
        record.get("contract_version") != OPERATOR_DAG_RUN_VERSION
        or not (terminal_failed or interrupted)
        or not isinstance(parent_run_id, str)
        or not parent_run_id
        or projection.run_id != parent_run_id
        or record.get("node_identity_projection")
        != NODE_IDENTITY_PROJECTION_CURRENT
        or (not strict and record.get("dag") != dag.to_dict())
        or record.get("root_seed") != root_seed
        or record.get("fixed_clock") != fixed_clock
        or record.get("mode") != mode.value
        or (not strict and record.get("audit_environment") != expected_audit)
        or (
            not strict
            and record.get("audit_manifest_digest") != service.audit_environment.manifest_digest
        )
    ):
        raise RuntimeIntegrityError(
            "失败 run 复用要求来源终态、clock 和 seed 一致"
            if strict else
            "失败 run 复用要求来源终态、DAG、节点身份环境、clock 和 seed 完全一致"
        )
    recorded_chain_head = record.get("event_chain_head")
    if recorded_chain_head is not None and recorded_chain_head != projection.chain_head:
        raise RuntimeIntegrityError("失败 run 记录的事件链与当前内容不一致")

    ordered_nodes = dag.topological_order()
    required = frozenset(required_node_ids)
    needed = frozenset()
    if strict:
        if len(required) != len(required_node_ids):
            raise RuntimeIntegrityError("要求复用的节点不得重复")
        unknown = required - set(ordered_nodes)
        if unknown:
            raise RuntimeIntegrityError("要求复用的节点不在当前 DAG: " + ", ".join(sorted(unknown)))
        needed = _ancestor_closure(dag, required)
        unsuccessful = sorted(
            node_id for node_id in needed
            if projection.node_statuses.get(node_id) != "succeeded"
        )
        if unsuccessful:
            raise RuntimeIntegrityError("要求复用的来源节点未成功: " + ", ".join(unsuccessful))
        source_dag = DagSpec.from_dict(dict(record["dag"]))
        source_nodes = {node.node_id: node for node in source_dag.nodes}
        for node in dag.nodes:
            if node.node_id in needed and source_nodes.get(node.node_id) != node:
                raise RuntimeIntegrityError(f"要求复用节点的局部合同发生变化: {node.node_id}")
    first_unfinished = next(
        (
            node_id
            for node_id in ordered_nodes
            if (
                node_id not in needed
                if strict
                else projection.node_statuses.get(node_id) != "succeeded"
            )
        ),
        None,
    )
    if first_unfinished is None:
        raise RuntimeIntegrityError("失败 run 没有需要继续执行的节点")

    child_run_id = derive_run_id(
        dag,
        dag.dag_id,
        DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock)),
        mode.value,
        audit_manifest_digest=service.audit_environment.manifest_digest,
        project_id=project_id,
        parent_run_id=parent_run_id,
    )
    if strict:
        recovery = RecoveryPlan(
            "rerun-from", child_run_id, parent_run_id,
            tuple(node_id for node_id in ordered_nodes if node_id in needed),
            tuple(node_id for node_id in ordered_nodes if node_id not in needed),
            (), reason="显式要求复用失败运行中的成功节点及其上游",
        )
    else:
        recovery = plan_rerun_from(
            dag,
            child_run_id,
            parent_run_id,
            projection,
            first_unfinished,
            verified_nodes=frozenset(
                node_id for node_id, status in projection.node_statuses.items()
                if status == "succeeded"
            ),
        )
    if any(
        projection.node_statuses.get(node_id) != "succeeded"
        for node_id in recovery.reuse_nodes
    ):
        raise RuntimeIntegrityError("失败 run 的可复用范围包含未成功节点")

    recovery_path = target_root / "recovery-plan.json"
    if (
        not recovery_path.exists()
        and any(
            (target_root / name).exists()
            for name in ("events.jsonl", "operator-dag-run.json")
        )
    ):
        raise RuntimeIntegrityError("目标 run 已有执行状态但缺少失败 run 复用计划")

    source_checkpoints = CheckpointStore(source_root, create=False)
    source_external = ExternalArtifactStore(
        source_root / "external-artifacts",
        create=False,
    )
    target_checkpoints = CheckpointStore(target_root)
    target_external = ExternalArtifactStore(target_root / "external-artifacts")
    context = DeterminismContext(
        root_seed,
        datetime.fromisoformat(fixed_clock),
    )
    values = {}
    copied_external_nodes: list[str] = []
    reused_checkpoints: dict[str, str] = {}
    committed = {
        (event.node_id, event.payload.get("node_execution_id"))
        for event in EventStore(source_root).read_events()
        if event.kind == "checkpoint_committed"
    }
    node_map = {node.node_id: node for node in dag.nodes}
    sources = (
        (
            source_root,
            parent_run_id,
            source_checkpoints,
            source_external,
        ),
    )
    for node_id in recovery.reuse_nodes:
        node = node_map[node_id]
        inputs = service._inputs(dag, node, values)
        identity, expectation = service._identity(
            node,
            tuple(value.artifact_ref for _, value in sorted(inputs.items())),
            context,
        )
        if strict and (node_id, expectation.node_execution_id) not in committed:
            raise RuntimeIntegrityError(f"要求复用节点缺少当前身份的来源提交事件: {node_id}")
        reused = service._reuse_cross_run_checkpoint(
            node=node,
            expectation=expectation,
            identity=identity,
            sources=sources,
            target_checkpoints=target_checkpoints,
            target_external=target_external,
            root_seed=root_seed,
            fixed_clock=fixed_clock,
        )
        if reused is None:
            raise RuntimeIntegrityError(
                f"失败 run 的成功节点缺少当前身份 checkpoint: {node_id}"
            )
        _source_run_id, _manifest, outputs = reused
        values[node_id] = outputs
        reused_checkpoints[node_id] = expectation.node_execution_id
        if any(value.external_commit is not None for value in outputs.values.values()):
            copied_external_nodes.append(node_id)

    payload = {
        **recovery.to_dict(),
        "recovery_plan_hash": recovery.plan_hash,
        "rerun_from_node": first_unfinished,
        "reuse_mode": FAILED_RUN_REUSE_MODE,
        "source_run_root": str(source_root),
        "copied_external_nodes": copied_external_nodes,
    }
    if strict:
        payload["required_nodes"] = sorted(required)
        payload["source_checkpoints_by_node"] = reused_checkpoints
    if stopped_source is not None:
        if (_interrupted_source_snapshot(source_root, parent_run_id) != stopped_source
                or EventStore(source_root).replay().chain_head != projection.chain_head
                or _read_run_record(source_root) != record):
            raise RuntimeIntegrityError("中断来源在复用期间发生变化，拒绝交接")
    _write_or_verify_recovery_plan(recovery_path, payload)
    return payload


def _interrupted_source_snapshot(source_root: Path, run_id: object):
    """只读确认中断来源的实际运行者和事件写者均已退出。"""
    owner = read_runtime_liveness(source_root)
    if owner is None or owner.get("run_id") != run_id:
        raise RuntimeIntegrityError("中断来源缺少绑定当前run的有效owner身份")
    def require_dead(identity, name):
        pid, started = identity.get("pid"), identity.get("process_started_at")
        if (type(pid) is not int or pid <= 0 or type(started) not in (int, float)
                or not math.isfinite(started) or started <= 0):
            raise RuntimeIntegrityError(f"中断来源{name}身份无效")
        if process_identity_alive(pid, started):
            raise RuntimeIntegrityError(f"中断来源{name}仍存活，拒绝复用")
    require_dead(owner, "owner")
    writer_path = source_root / ".writer.lock"
    writer = None
    if writer_path.exists():
        try:
            writer = json.loads(writer_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RuntimeIntegrityError("中断来源事件写者锁身份无法读取") from exc
        if not isinstance(writer, dict) or not isinstance(writer.get("token"), str) or not writer["token"]:
            raise RuntimeIntegrityError("中断来源事件写者锁身份无效")
        require_dead(writer, "事件写者")
    return owner, writer


def _read_run_record(source_root: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(
            (source_root / "operator-dag-run.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeIntegrityError("失败 run record 无法读取") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeIntegrityError("失败 run record schema 无效")
    return payload


def _write_or_verify_recovery_plan(
    path: Path,
    payload: Mapping[str, object],
) -> None:
    document = dict(payload)
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("失败 run 复用计划无法读取") from exc
        if existing != document:
            raise RuntimeIntegrityError("失败 run 复用计划与当前输入不一致")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(canonical_json(document), encoding="utf-8")
    temporary.replace(path)


__all__ = ["FAILED_RUN_REUSE_MODE", "prepare_failed_run_reuse"]
