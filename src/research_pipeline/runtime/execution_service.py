"""正式 OperatorDefinition DAG 的统一、可恢复本地 Runtime。"""

from __future__ import annotations

import base64
from contextlib import ExitStack
from datetime import datetime
import importlib
import json
from pathlib import Path
import shutil
import time
from typing import Mapping

from research_pipeline.extensions import (
    AdmittedProjectOperatorRegistry,
    ProjectOperatorImplementationToken,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.data_plane import (
    ArtifactResolver,
    DatasetArtifactRef,
    PartitionedDatasetRef,
    QueryIRInvalidError,
    resolve_as_of_cutoff,
)
from research_pipeline.data_plane.verification_lifecycle import (
    RunScopedArtifactVerification,
    activate_artifact_verification,
)

from .artifacts import CheckpointExpectation
from .checkpoint import CheckpointStore
from .contracts import ArtifactRef, DeterminismContext, NodeSpec, ResourceBudget
from .diagnostics import read_finalize_status, safe_error_summary
from .errors import RuntimeAdmissionError, RuntimeIntegrityError, RuntimeWorkerError
from .events import RuntimeEvent
from .external_artifact import ExternalArtifactStore
from .graph import DagSpec
from .identity import (
    AuditEnvironmentManifest,
    CacheCompatibilityMode,
    CacheCompatibilityProfile,
    ExecutionIdentity,
    derive_run_id,
)
from .operator_definitions import (
    build_mainline_operator_manifest,
    production_runtime_adapter_code_hash,
)
from .operator_runtime import (
    RUNTIME_NODE_VALUE_VERSION,
    OperatorRuntimeContext,
    RuntimeCompletionMetadata,
    RuntimeNodeContext,
    RuntimeNodeOutputs,
    RuntimeNodeValue,
)
from .project_operator_runtime import (
    ProjectRuntimeInput,
    ProjectWorkerState,
    _declared_request_ids,
    execute_project_worker_attempt,
    project_runtime_identity,
)
from .operator_registry import NODE_IDENTITY_PROJECTION_CURRENT
from .liveness import RuntimeLiveness
from .partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointExpectation,
    PartitionCheckpointStore,
)
from .recovery import plan_rerun_from
from .scheduler import (
    ExecutionMode,
    ReadyCandidate,
    ReservationLedger,
    ResourceCapacity,
)
from .resource_governor import (
    ResourceGovernor,
    ResourceLease,
    ResourceObservation,
    ResourceUsageSampler,
    ResourceVector,
    resource_data_bucket,
)
from .store import EventStore


OPERATOR_DAG_RUN_VERSION = "research-runtime-operator-dag-run-v3"


class RuntimeExecutionService:
    """按 Typed DAG 管理正式节点状态、重试、checkpoint 和外部工件引用。"""

    def __init__(
        self,
        *,
        audit_environment: AuditEnvironmentManifest,
        numerical_backend_names: tuple[str, ...],
        resource_capacity: ResourceCapacity | None = None,
        resource_governor: ResourceGovernor | None = None,
        resource_timeout_seconds: float | None = None,
        project_registry: AdmittedProjectOperatorRegistry | None = None,
        project_parameters_by_node: Mapping[str, Mapping[str, object]] | None = None,
        result_table_requirements_by_node: Mapping[
            str, tuple[Mapping[str, str], ...]
        ] | None = None,
    ) -> None:
        self.audit_environment = audit_environment
        self.numerical_backend_names = numerical_backend_names
        self.resource_capacity = resource_capacity
        if resource_governor is not None and (
            resource_timeout_seconds is None or resource_timeout_seconds <= 0
        ):
            raise ValueError("启用资源治理时必须显式提供正数 timeout")
        self.resource_governor = resource_governor
        self.resource_timeout_seconds = resource_timeout_seconds
        self.project_registry = project_registry
        self.project_parameters_by_node = {
            key: dict(value)
            for key, value in (project_parameters_by_node or {}).items()
        }
        self.result_table_requirements_by_node = {
            node_id: tuple(dict(item) for item in requirements)
            for node_id, requirements in (
                result_table_requirements_by_node or {}
            ).items()
        }
        self._definitions = {
            item.implementation_ref.implementation_id: item
            for item in build_mainline_operator_manifest().definitions
        }

    def execute_minute_partitions(self, **kwargs: object) -> object:
        """复用统一 Runtime 服务入口执行分钟分区 profile。"""
        from .minute_profile import MinutePartitionRuntime

        if self.resource_governor is None:
            return MinutePartitionRuntime().execute(**kwargs)  # type: ignore[arg-type]
        budget = kwargs.get("budget")
        manifest = kwargs.get("manifest")
        worker_count = kwargs.get("worker_count", 1)
        if budget is None or manifest is None or type(worker_count) is not int:
            raise RuntimeIntegrityError("分钟资源治理缺少 manifest/budget/worker_count")
        vector = ResourceVector(
            budget.max_rss_bytes,  # type: ignore[union-attr]
            worker_count,
            budget.max_temp_bytes,  # type: ignore[union-attr]
            1,
        )
        lease = self.resource_governor.acquire(
            owner_id=f"minute/{manifest.manifest_hash}",  # type: ignore[union-attr]
            vector=vector,
            timeout_seconds=float(self.resource_timeout_seconds),
        )
        try:
            with self.resource_governor.maintained_lease(lease):
                return MinutePartitionRuntime().execute(  # type: ignore[arg-type]
                    **kwargs,
                    resource_governor=self.resource_governor,
                    parent_resource_lease=lease,
                    resource_timeout_seconds=self.resource_timeout_seconds,
                )
        finally:
            self.resource_governor.release(lease)

    def prepare_rerun_from(
        self,
        *,
        dag: DagSpec,
        parent_run_root: str | Path,
        child_run_root: str | Path,
        project_id: str,
        root_seed: int,
        fixed_clock: str,
        mode: ExecutionMode,
        node_id: str,
    ) -> dict[str, object]:
        """复验并把目标节点之前的 checkpoint/外部对象导入独立 child。"""
        parent_root = Path(parent_run_root).resolve()
        child_root = Path(child_run_root).resolve()
        if (
            parent_root == child_root
            or parent_root in child_root.parents
            or child_root in parent_root.parents
        ):
            raise RuntimeIntegrityError("rerun-from 父子 run root 不得相同或相互包含")
        parent_projection = EventStore(parent_root).replay()
        parent_record = json.loads(
            (parent_root / "operator-dag-run.json").read_text(encoding="utf-8")
        )
        if (
            parent_record.get("contract_version") != OPERATOR_DAG_RUN_VERSION
            or
            parent_record.get("status") not in {"succeeded", "failed", "cancelled"}
            or parent_projection.run_id != parent_record.get("run_id")
            or parent_projection.run_status != parent_record.get("status")
            or parent_record.get("dag") != dag.to_dict()
        ):
            raise RuntimeIntegrityError("rerun-from 父运行终态、事件链或 DAG 不一致")
        parent_run_id = str(parent_record["run_id"])
        child_run_id = derive_run_id(
            dag,
            dag.dag_id,
            DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock)),
            mode.value,
            audit_manifest_digest=self.audit_environment.manifest_digest,
            parent_run_id=parent_run_id,
            project_id=project_id,
        )
        successful_nodes = frozenset(node_id for node_id, status in parent_projection.node_statuses.items() if status == "succeeded")
        preliminary = plan_rerun_from(
            dag,
            child_run_id,
            parent_run_id,
            parent_projection,
            node_id,
            verified_nodes=successful_nodes,
        )
        child_root.mkdir(parents=True, exist_ok=True)
        if any(
            (child_root / name).exists()
            for name in (
                "checkpoints",
                "events.jsonl",
                "operator-dag-run.json",
                "external-artifacts",
            )
        ):
            raise RuntimeIntegrityError("rerun-from child run 已含 Runtime 可写状态")
        parent_checkpoints = CheckpointStore(parent_root, create=False)
        parent_external = ExternalArtifactStore(
            parent_root / "external-artifacts", create=False
        )
        child_checkpoints = CheckpointStore(child_root)
        child_external = ExternalArtifactStore(child_root / "external-artifacts")
        context = DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock))
        values: dict[str, RuntimeNodeOutputs] = {}
        copied: list[str] = []
        node_map = {item.node_id: item for item in dag.nodes}
        for current_id in preliminary.reuse_nodes:
            node = node_map[current_id]
            inputs = self._inputs(dag, node, values)
            identity, expectation = self._identity(
                node,
                tuple(value.artifact_ref for _, value in sorted(inputs.items())),
                context,
            )
            manifest = parent_checkpoints.verify(expectation)
            checkpoint_path = (
                parent_checkpoints.checkpoints_root / expectation.node_execution_id
            )
            raw = (checkpoint_path / manifest.content_path).read_bytes()
            node_outputs = self._decode_outputs(raw, parent_external)
            imported_values = {}
            for port, value in node_outputs.values.items():
                if value.external_commit is not None:
                    value = RuntimeNodeValue.external(
                        child_external.import_verified(
                            parent_external, value.external_commit
                        )
                    )
                imported_values[port] = value
            node_outputs = RuntimeNodeOutputs(
                imported_values,
                completion_metadata=node_outputs.completion_metadata,
            )
            if any(value.external_commit is not None for value in node_outputs.values.values()):
                copied.append(current_id)
            child_checkpoints.commit_bytes(
                expectation=expectation,
                attempt_id=f"rerun-reuse-{current_id}",
                content=node_outputs.checkpoint_bytes(),
                outputs=node_outputs.artifact_refs,
                audit_environment_digest=self.audit_environment.manifest_digest,
                execution_identity_digest=identity.identity_digest,
                root_seed=root_seed,
                fixed_clock=fixed_clock,
            )
            values[current_id] = node_outputs
        plan = plan_rerun_from(
            dag,
            child_run_id,
            parent_run_id,
            parent_projection,
            node_id,
            verified_nodes=frozenset(values),
        )
        recovery = {
            **plan.to_dict(),
            "recovery_plan_hash": plan.plan_hash,
            "rerun_from_node": node_id,
            "copied_external_nodes": copied,
        }
        self._write_record(child_root / "recovery-plan.json", recovery)
        return recovery

    def execute(
        self,
        *,
        dag: DagSpec,
        environment: object,
        run_root: str | Path,
        project_id: str,
        root_seed: int,
        fixed_clock: str,
        mode: ExecutionMode = ExecutionMode.DETERMINISTIC_SERIAL,
        resume: bool = False,
        retry_node_id: str | None = None,
        process_slots_by_node: Mapping[str, int] | None = None,
        parent_run_id: str | None = None,
        rerun_from_node: str | None = None,
        recovery_plan_hash: str | None = None,
        reuse_run_roots: tuple[str | Path, ...] = (),
        node_identity_projection: str = NODE_IDENTITY_PROJECTION_CURRENT,
        owner: RuntimeLiveness | None = None,
    ) -> dict[str, object]:
        owns_lifetime = owner is None
        run_id = derive_run_id(
            dag, dag.dag_id,
            DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock)),
            mode.value, audit_manifest_digest=self.audit_environment.manifest_digest,
            project_id=project_id, parent_run_id=parent_run_id,
        )
        if owner is None:
            owner = RuntimeLiveness(run_root, run_id=run_id, phase="starting").start()
        else:
            owner.bind_run(run_root, run_id)
        try:
            return self._execute(
                dag=dag,
                environment=environment,
                run_root=run_root,
                project_id=project_id,
                root_seed=root_seed,
                fixed_clock=fixed_clock,
                mode=mode,
                resume=resume,
                retry_node_id=retry_node_id,
                process_slots_by_node=process_slots_by_node,
                parent_run_id=parent_run_id,
                rerun_from_node=rerun_from_node,
                recovery_plan_hash=recovery_plan_hash,
                reuse_run_roots=reuse_run_roots,
                node_identity_projection=node_identity_projection,
                owner=owner,
            )
        finally:
            if owns_lifetime:
                owner.stop()

    def _execute(
        self,
        *,
        dag: DagSpec,
        environment: object,
        run_root: str | Path,
        project_id: str,
        root_seed: int,
        fixed_clock: str,
        mode: ExecutionMode = ExecutionMode.DETERMINISTIC_SERIAL,
        resume: bool = False,
        retry_node_id: str | None = None,
        process_slots_by_node: Mapping[str, int] | None = None,
        parent_run_id: str | None = None,
        rerun_from_node: str | None = None,
        recovery_plan_hash: str | None = None,
        reuse_run_roots: tuple[str | Path, ...] = (),
        node_identity_projection: str = NODE_IDENTITY_PROJECTION_CURRENT,
        owner: RuntimeLiveness,
    ) -> dict[str, object]:
        root = Path(run_root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        record_path = root / "operator-dag-run.json"
        if record_path.exists() and not resume:
            raise RuntimeIntegrityError("operator DAG run 已存在；必须显式 resume")
        context = DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock))
        process_slots = dict(process_slots_by_node or {})
        if set(process_slots) - {node.node_id for node in dag.nodes} or any(
            type(value) is not int or value <= 0 for value in process_slots.values()
        ):
            raise RuntimeIntegrityError("节点 process slots 配置无效")
        effective_budgets = {
            node.node_id: _effective_resource_budget(
                node.resource_budget,
                capacity=self.resource_capacity,
            )
            for node in dag.nodes
        }
        reservation_budgets = {
            node.node_id: _reservation_budget(
                node.resource_budget,
                process_slots=process_slots.get(node.node_id, 1),
            )
            for node in dag.nodes
        }
        resource_ledger = (
            None
            if self.resource_capacity is None
            else ReservationLedger(self.resource_capacity, mode)
        )
        if resource_ledger is not None:
            for node in dag.nodes:
                resource_ledger.require_single_node_admission(node.resource_budget)
            for budget in reservation_budgets.values():
                resource_ledger.require_single_node_admission(budget)
        governance_hash = (
            None
            if self.resource_governor is None
            else self.resource_governor.identity_hash
        )
        run_id = derive_run_id(
            dag,
            dag.dag_id,
            context,
            mode.value,
            audit_manifest_digest=self.audit_environment.manifest_digest,
            project_id=project_id,
            parent_run_id=parent_run_id,
        )
        events = EventStore(root)
        checkpoints = CheckpointStore(root)
        verification_session = RunScopedArtifactVerification(run_id)
        external = ExternalArtifactStore(
            root / "external-artifacts",
            verification_session=verification_session,
        )
        projection = events.replay()
        if projection.run_id is not None and projection.run_id != run_id:
            raise RuntimeIntegrityError("operator DAG run identity 漂移")
        base_record = {
            "contract_version": OPERATOR_DAG_RUN_VERSION,
            "status": projection.run_status or "created",
            "project_id": project_id,
            "run_id": run_id,
            "dag_id": dag.dag_id,
            "dag": dag.to_dict(),
            "root_seed": root_seed,
            "fixed_clock": fixed_clock,
            "mode": mode.value,
            "audit_environment": self.audit_environment.to_dict(),
            "audit_manifest_digest": self.audit_environment.manifest_digest,
            "reused_nodes": [],
            "outputs": {},
        }
        if node_identity_projection == NODE_IDENTITY_PROJECTION_CURRENT:
            base_record["node_identity_projection"] = node_identity_projection
        elif reuse_run_roots:
            raise RuntimeIntegrityError("旧节点身份投影不能启用跨运行复用")
        reuse_sources = self._open_reuse_sources(
            reuse_run_roots,
            target_root=root,
            node_identity_projection=node_identity_projection,
        )
        if reuse_sources:
            base_record["reuse_source_run_ids"] = [
                source[1] for source in reuse_sources
            ]
        if self.resource_capacity is not None:
            base_record.update(
                {
                    "resource_capacity": self.resource_capacity.to_dict(),
                    "process_slots_by_node": dict(sorted(process_slots.items())),
                }
            )
        if parent_run_id is not None:
            if not rerun_from_node or not recovery_plan_hash:
                raise RuntimeIntegrityError("child run 缺少 rerun lineage")
            base_record.update(
                {
                    "parent_run_id": parent_run_id,
                    "rerun_from_node": rerun_from_node,
                    "recovery_plan_hash": recovery_plan_hash,
                }
            )
        if self.resource_governor is not None:
            base_record.update(
                {
                    "resource_governance": self.resource_governor.config.identity_payload,
                    "resource_governance_hash": governance_hash,
                }
            )
        if record_path.exists():
            existing = json.loads(record_path.read_text(encoding="utf-8"))
            immutable = (
                "contract_version",
                "project_id",
                "run_id",
                "dag_id",
                "dag",
                "root_seed",
                "fixed_clock",
                "mode",
                "audit_environment",
                "audit_manifest_digest",
            )
            if parent_run_id is not None:
                immutable += ("parent_run_id", "rerun_from_node", "recovery_plan_hash")
            if self.resource_capacity is not None:
                immutable += ("resource_capacity", "process_slots_by_node")
            if self.resource_governor is not None:
                immutable += (
                    "resource_governance",
                    "resource_governance_hash",
                )
            if node_identity_projection == NODE_IDENTITY_PROJECTION_CURRENT:
                immutable += ("node_identity_projection",)
            if reuse_sources:
                immutable += ("reuse_source_run_ids",)
            if any(existing.get(key) != base_record[key] for key in immutable):
                raise RuntimeIntegrityError("operator DAG run record 与当前输入不一致")
        else:
            self._write_record(record_path, base_record)
        if projection.run_status in {None, "created", "planned"}:
            startup = (None, "created", "planned", "running")
            for status in startup[startup.index(projection.run_status) + 1:]:
                self._status(events, run_id, "run", status, command_id=f"run:{status}")
        elif projection.run_status == "paused":
            if retry_node_id is None:
                raise RuntimeIntegrityError(
                    "paused run 必须通过 retry-node 指定失败节点"
                )
            self._status(
                events,
                run_id,
                "run",
                "running",
                command_id=f"run:retry:{retry_node_id}:{len(events.read_events())}",
            )
        elif projection.run_status == "succeeded":
            if not resume:
                raise RuntimeIntegrityError("succeeded run 不允许覆盖")
        elif projection.run_status != "running":
            raise RuntimeIntegrityError("operator DAG run 状态不可恢复")

        outputs: dict[str, RuntimeNodeOutputs] = {}
        reused: list[str] = []
        node_map = {node.node_id: node for node in dag.nodes}
        for topological_level, node_id in enumerate(dag.topological_order()):
            node = node_map[node_id]
            values_by_port = self._inputs(dag, node, outputs)
            semantic_inputs = tuple(
                value.artifact_ref for _, value in sorted(values_by_port.items())
            )
            identity, expectation = self._identity(node, semantic_inputs, context)
            checkpoint_path = (
                checkpoints.checkpoints_root / expectation.node_execution_id
            )
            if (
                (
                    events.replay().node_statuses.get(node_id) == "succeeded"
                    or any(
                        event.kind == "checkpoint_committed" and event.node_id == node_id
                        and event.payload.get("node_execution_id") == expectation.node_execution_id
                        for event in events.read_events()
                    )
                )
                and not (checkpoint_path / "COMMITTED").is_file()
            ):
                raise RuntimeIntegrityError(f"已提交节点 checkpoint 缺少提交标记: {node_id}")
            if checkpoint_path.exists() and (checkpoint_path / "COMMITTED").is_file():
                manifest = checkpoints.verify(expectation)
                node_outputs = self._decode_outputs(
                    (checkpoint_path / manifest.content_path).read_bytes(),
                    external,
                )
                self._validate_outputs(node, node_outputs)
                if dict(manifest.outputs) != dict(node_outputs.artifact_refs):
                    raise RuntimeIntegrityError(
                        f"节点 {node_id} checkpoint outputs 与内容引用不一致"
                    )
                outputs[node_id] = node_outputs
                reused.append(node_id)
                self._reconcile_success(
                    events, run_id, node_id, expectation.node_execution_id
                )
                continue
            reused_from = None
            if node.cacheable and node.pure:
                reused_from = self._reuse_cross_run_checkpoint(
                    node=node,
                    expectation=expectation,
                    identity=identity,
                    sources=reuse_sources,
                    target_checkpoints=checkpoints,
                    target_external=external,
                    root_seed=root_seed,
                    fixed_clock=fixed_clock,
                )
            if reused_from is not None:
                source_run_id, source_manifest, node_outputs = reused_from
                outputs[node_id] = node_outputs
                reused.append(node_id)
                events.append(
                    run_id,
                    "diagnostic",
                    {
                        "diagnostic_type": "cross_run_checkpoint_reused",
                        "node_execution_id": expectation.node_execution_id,
                        "reused_from_run_id": source_run_id,
                        "source_checkpoint_manifest_hash": source_manifest.manifest_hash,
                    },
                    command_id=f"{node_id}:cross-run-reused:{source_run_id}",
                    node_id=node_id,
                )
                self._reconcile_success(
                    events, run_id, node_id, expectation.node_execution_id
                )
                continue
            current = events.replay().node_statuses.get(node_id)
            recovered_interruption = False
            if current in {"waiting_for_resources", "running"}:
                recovered_interruption = self._recover_interrupted_attempt(
                    events,
                    run_id,
                    node_id,
                )
                current = events.replay().node_statuses.get(node_id)
            if (
                current == "retryable_failed"
                and retry_node_id != node_id
                and not recovered_interruption
            ):
                raise RuntimeIntegrityError(f"节点 {node_id} 失败；必须显式 retry-node")
            attempts_used = sum(
                event.node_id == node_id
                and event.kind == "attempt_status_changed"
                and event.payload.get("status") == "pending"
                for event in events.read_events()
            )
            if (
                current == "retryable_failed"
                and attempts_used >= node.retry_policy.max_attempts
            ):
                self._status(
                    events,
                    run_id,
                    "node",
                    "exhausted",
                    command_id=f"{node_id}:exhausted:{attempts_used}",
                    node_id=node_id,
                )
                self._status(
                    events, run_id, "run", "failed", command_id=f"run:failed:{node_id}"
                )
                self._write_record(record_path, {**base_record, "status": "failed"})
                raise RuntimeWorkerError(f"operator 节点重试次数耗尽: {node_id}")
            if current is None:
                self._status(
                    events,
                    run_id,
                    "node",
                    "pending",
                    command_id=f"{node_id}:pending",
                    node_id=node_id,
                )
                current = "pending"
            transition_suffix = len(events.read_events())
            if current in {"pending", "retryable_failed"}:
                self._status(
                    events,
                    run_id,
                    "node",
                    "ready",
                    command_id=f"{node_id}:ready:{transition_suffix}",
                    node_id=node_id,
                )
            elif current != "ready":
                raise RuntimeIntegrityError(
                    f"节点 {node_id} 当前状态不可执行: {current}"
                )
            attempt_number = attempts_used + 1
            attempt_id = f"{node_id}-attempt-{attempt_number}"
            for status in ("pending", "admitted", "ready"):
                self._status(
                    events,
                    run_id,
                    "attempt",
                    status,
                    command_id=f"{attempt_id}:{status}",
                    node_id=node_id,
                    attempt_id=attempt_id,
                )
            reservation_vector = ResourceVector.from_budget(
                reservation_budgets[node_id],
                process_slots=process_slots.get(node_id, 1),
            )
            queued_at = datetime.now().astimezone().isoformat()
            queue_started = time.monotonic()
            waiting_payload = {
                "queued_at": queued_at,
                "requested_resources": reservation_vector.to_dict(),
            }
            self._status(
                events,
                run_id,
                "node",
                "waiting_for_resources",
                command_id=f"{node_id}:waiting:{transition_suffix}",
                node_id=node_id,
                details=waiting_payload,
            )
            self._status(
                events,
                run_id,
                "attempt",
                "waiting_for_resources",
                command_id=f"{attempt_id}:waiting",
                node_id=node_id,
                attempt_id=attempt_id,
                details=waiting_payload,
            )
            work_dir = root / "work" / attempt_id
            lease: ResourceLease | None = None
            local_reservation: ReadyCandidate | None = None
            sampler = None
            sampled = False
            checkpoint_committed = False
            liveness: RuntimeLiveness | None = None
            estimate_components = {
                "scan_bytes": None,
                "intermediate_bytes": node.resource_budget.memory_bytes,
                "output_bytes": None,
                "scratch_bytes": node.resource_budget.temp_bytes,
                "parallelism": process_slots.get(node_id, 1),
                "wall_seconds": node.resource_budget.wall_seconds,
            }
            try:
                liveness = owner
                liveness.set_node(
                    node_id=node_id, attempt_id=attempt_id,
                    requested_resources=reservation_vector.to_dict(),
                )
                if resource_ledger is not None:
                    local_reservation = ReadyCandidate(
                        topological_level,
                        node_id,
                        None,
                        reservation_budgets[node_id],
                    )
                    resource_ledger.reserve(local_reservation)
                if self.resource_governor is not None:
                    lease = self.resource_governor.acquire(
                        owner_id=f"{project_id}/{run_id}/{node_id}/{attempt_id}",
                        vector=reservation_vector,
                        timeout_seconds=float(self.resource_timeout_seconds),
                    )
                acquired_at = datetime.now().astimezone().isoformat()
                wait_milliseconds = max(
                    0,
                    round((time.monotonic() - queue_started) * 1000),
                )
                running_payload = {
                    "acquired_at": acquired_at,
                    "queued_at": queued_at,
                    "requested_resources": reservation_vector.to_dict(),
                    "wait_milliseconds": wait_milliseconds,
                }
                self._status(
                    events,
                    run_id,
                    "node",
                    "running",
                    command_id=f"{node_id}:running:{transition_suffix}",
                    node_id=node_id,
                    details=running_payload,
                )
                liveness.update(
                    "executing",
                    reserved_resources=reservation_vector.to_dict(),
                )
                self._status(
                    events,
                    run_id,
                    "attempt",
                    "running",
                    command_id=f"{attempt_id}:running",
                    node_id=node_id,
                    attempt_id=attempt_id,
                    details=running_payload,
                )
                work_dir.mkdir(parents=True, exist_ok=False)
                attempt_deadline = time.monotonic() + node.resource_budget.wall_seconds

                def phase_hook(phase: str) -> None:
                    if phase == "checkpoint_prepared":
                        events.append(
                            run_id,
                            "checkpoint_prepared",
                            {"node_execution_id": expectation.node_execution_id},
                            command_id=f"{attempt_id}:checkpoint_prepared",
                            node_id=node_id,
                            attempt_id=attempt_id,
                        )

                with ExitStack() as stack:
                    if lease is not None:
                        stack.enter_context(
                            self.resource_governor.maintained_lease(lease)
                        )
                    if self.resource_governor is not None:
                        sampler_context = stack.enter_context(ExitStack())
                        sampler = sampler_context.enter_context(ResourceUsageSampler(work_dir))
                        sampled = True
                    node_context = RuntimeNodeContext(
                        node=node,
                        inputs=values_by_port,
                        work_dir=work_dir,
                        external_store=external,
                        root_seed=root_seed,
                        fixed_clock=fixed_clock,
                        effective_resource_budget=effective_budgets[node_id],
                        resource_governor=self.resource_governor,
                        resource_lease=lease,
                        attempt_deadline_monotonic=attempt_deadline,
                        process_slots=process_slots.get(node_id, 1),
                    )
                    definition = self._definitions.get(node.implementation_id)
                    if definition is not None:
                        adapter_ref = definition.runtime_adapter_ref
                        if (
                            production_runtime_adapter_code_hash(
                                adapter_ref.adapter_id
                            )
                            != adapter_ref.code_hash
                        ):
                            raise RuntimeIntegrityError(
                                f"正式 Runtime adapter 源码摘要漂移: {adapter_ref.adapter_id}"
                            )
                        module = importlib.import_module(adapter_ref.module_name)
                        adapter = getattr(module, adapter_ref.symbol_name, None)
                        if not callable(adapter):
                            raise RuntimeIntegrityError(
                                f"正式 Runtime adapter 不可调用: {adapter_ref.adapter_id}"
                            )
                        with activate_artifact_verification(verification_session):
                            raw_outputs = adapter(
                                OperatorRuntimeContext(node_context, environment)
                            )
                    else:
                        with activate_artifact_verification(verification_session):
                            raw_outputs = self._execute_project_node(
                                node_context=node_context,
                                run_id=run_id,
                                attempt_id=attempt_id,
                                node_execution_id=expectation.node_execution_id,
                                environment=environment,
                            )
                    node_context.remaining_resource_budget()
                    node_outputs = self._coerce_outputs(node, raw_outputs)
                    self._validate_outputs(node, node_outputs)
                    output_bytes = (
                        _runtime_outputs_size(node_outputs, external)
                        if sampler is not None else 0
                    )
                    liveness.update("checkpointing")
                    events.append(
                        run_id,
                        "execution_completed",
                        {
                            "result_hash": typed_canonical_hash({
                                port: value.artifact_ref.to_dict()
                                for port, value in node_outputs.values.items()
                            })
                        },
                        command_id=f"{attempt_id}:execution_completed",
                        node_id=node_id,
                        attempt_id=attempt_id,
                    )
                    checkpoints.commit_bytes(
                        expectation=expectation,
                        attempt_id=attempt_id,
                        content=node_outputs.checkpoint_bytes(),
                        outputs=node_outputs.artifact_refs,
                        audit_environment_digest=self.audit_environment.manifest_digest,
                        execution_identity_digest=identity.identity_digest,
                        root_seed=root_seed,
                        fixed_clock=fixed_clock,
                        phase_hook=phase_hook,
                    )
                    try:
                        if sampler is not None:
                            sampler_context.close()
                            _require_sampled_resources(
                                sampler, budget=effective_budgets[node_id],
                                reservation=reservation_vector,
                            )
                        node_context.remaining_resource_budget()
                    except BaseException:
                        # 本 attempt 的完整复验尚未通过资源门禁，不能留下可恢复的提交。
                        shutil.rmtree(checkpoint_path)
                        raise
                    checkpoint_committed = True
                    events.append(
                        run_id,
                        "checkpoint_committed",
                        {"node_execution_id": expectation.node_execution_id},
                        command_id=f"{attempt_id}:checkpoint_committed",
                        node_id=node_id,
                        attempt_id=attempt_id,
                    )
                if self.resource_governor is not None:
                    operator_id, operator_version, profile_id = (
                        self._observation_identity(node)
                    )
                    self.resource_governor.record_observation(
                        ResourceObservation(
                            operator_id=operator_id,
                            operator_version=operator_version,
                            profile_id=profile_id,
                            environment_hash=self.audit_environment.manifest_digest,
                            data_bucket=resource_data_bucket(estimate_components),
                            run_id=run_id,
                            node_id=node_id,
                            attempt_id=attempt_id,
                            status="succeeded",
                            fixture=False,
                            estimate_components=estimate_components,
                            actual_components={
                                "peak_rss_bytes": sampler.peak_rss_bytes,
                                "peak_scratch_bytes": sampler.peak_scratch_bytes,
                                "output_bytes": output_bytes,
                                "max_processes": sampler.max_processes,
                                "wall_milliseconds": sampler.wall_milliseconds,
                            },
                            observed_at=datetime.now().astimezone().isoformat(),
                            measurement_status=sampler.measurement_status,
                        )
                    )
            except BaseException as exc:
                diagnostic = safe_error_summary(
                    exc,
                    default_error_code="operator_execution_failed",
                )
                if (
                    self.resource_governor is not None
                    and lease is not None
                    and sampled
                    and not checkpoint_committed
                ):
                    operator_id, operator_version, profile_id = (
                        self._observation_identity(node)
                    )
                    self.resource_governor.record_observation(
                        ResourceObservation(
                            operator_id=operator_id,
                            operator_version=operator_version,
                            profile_id=profile_id,
                            environment_hash=self.audit_environment.manifest_digest,
                            data_bucket=resource_data_bucket(estimate_components),
                            run_id=run_id,
                            node_id=node_id,
                            attempt_id=attempt_id,
                            status=(
                                "cancelled"
                                if isinstance(exc, (KeyboardInterrupt, SystemExit))
                                else "failed"
                            ),
                            fixture=False,
                            estimate_components=estimate_components,
                            actual_components={
                                "peak_rss_bytes": sampler.peak_rss_bytes,
                                "peak_scratch_bytes": sampler.peak_scratch_bytes,
                                "output_bytes": 0,
                                "max_processes": sampler.max_processes,
                                "wall_milliseconds": sampler.wall_milliseconds,
                            },
                            observed_at=datetime.now().astimezone().isoformat(),
                            measurement_status=sampler.measurement_status,
                        )
                    )
                self._status(
                    events,
                    run_id,
                    "attempt",
                    "failed",
                    command_id=f"{attempt_id}:failed",
                    node_id=node_id,
                    attempt_id=attempt_id,
                )
                events.append(
                    run_id,
                    "diagnostic",
                    diagnostic,
                    command_id=f"{attempt_id}:diagnostic",
                    node_id=node_id,
                    attempt_id=attempt_id,
                )
                retry_allowed = (
                    diagnostic["error_code"] in node.retry_policy.retryable_codes
                    and attempt_number < node.retry_policy.max_attempts
                )
                node_failure_status = (
                    "retryable_failed" if retry_allowed else "exhausted"
                )
                run_failure_status = "paused" if retry_allowed else "failed"
                self._status(
                    events,
                    run_id,
                    "node",
                    node_failure_status,
                    command_id=f"{attempt_id}:node-failed",
                    node_id=node_id,
                )
                self._status(
                    events,
                    run_id,
                    "run",
                    run_failure_status,
                    command_id=f"run:{run_failure_status}:{attempt_id}",
                )
                self._write_record(
                    record_path, {**base_record, "status": run_failure_status}
                )
                failure_payload = getattr(exc, "failure_payload", None)
                raise RuntimeWorkerError(
                    f"operator 节点执行失败: {node_id}",
                    error_code=str(diagnostic["error_code"]),
                    failure_payload=(
                        failure_payload
                        if isinstance(failure_payload, Mapping)
                        else None
                    ),
                ) from exc
            finally:
                if lease is not None:
                    self.resource_governor.release(lease)
                if local_reservation is not None:
                    resource_ledger.release(local_reservation.key)
            self._status(
                events,
                run_id,
                "attempt",
                "succeeded",
                command_id=f"{attempt_id}:succeeded",
                node_id=node_id,
                attempt_id=attempt_id,
            )
            self._status(
                events,
                run_id,
                "node",
                "succeeded",
                command_id=f"{node_id}:succeeded",
                node_id=node_id,
            )
            outputs[node_id] = node_outputs
        result = {
            **base_record,
            "status": "succeeded",
            "reused_nodes": reused,
            "outputs": {
                node_id: {
                    port: value.artifact_ref.to_dict()
                    for port, value in node_outputs.values.items()
                }
                for node_id, node_outputs in sorted(outputs.items())
            },
            "node_completion_metadata": {
                node_id: node_outputs.completion_metadata.to_dict()
                for node_id, node_outputs in sorted(outputs.items())
                if node_outputs.completion_metadata is not None
            },
            "completion_metadata": RuntimeCompletionMetadata.merge(
                {
                    node_id: node_outputs.completion_metadata
                    for node_id, node_outputs in outputs.items()
                    if node_outputs.completion_metadata is not None
                }
            ).to_dict(),
            "event_chain_head": events.replay().chain_head,
        }
        def persist_completion(event: RuntimeEvent) -> None:
            result["event_chain_head"] = event.event_hash
            self._write_record(record_path, result)

        if events.replay().run_status != "succeeded":
            events.append(
                run_id, "run_status_changed", {"status": "succeeded"},
                command_id="run:succeeded", before_append=persist_completion,
            )
        else:
            self._write_record(record_path, result)
        return result

    @staticmethod
    def _open_reuse_sources(
        roots: tuple[str | Path, ...],
        *,
        target_root: Path,
        node_identity_projection: str,
    ) -> tuple[tuple[Path, str, CheckpointStore, ExternalArtifactStore], ...]:
        if not roots:
            return ()
        if node_identity_projection != NODE_IDENTITY_PROJECTION_CURRENT:
            raise RuntimeIntegrityError("跨运行复用只接受现行节点局部身份计划")
        sources = []
        seen: set[Path] = set()
        for raw_root in roots:
            source_root = Path(raw_root).resolve(strict=True)
            if source_root in seen:
                raise RuntimeIntegrityError("跨运行复用来源不得重复")
            seen.add(source_root)
            if (
                source_root == target_root
                or source_root in target_root.parents
                or target_root in source_root.parents
            ):
                raise RuntimeIntegrityError("跨运行复用来源与目标 run 不得相同或相互包含")
            projection = EventStore(source_root).replay()
            try:
                record = json.loads(
                    (source_root / "operator-dag-run.json").read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeIntegrityError("跨运行复用来源 run record 无法读取") from exc
            finalize = read_finalize_status(source_root)
            if (
                record.get("contract_version") != OPERATOR_DAG_RUN_VERSION
                or record.get("status") != "succeeded"
                or record.get("node_identity_projection")
                != NODE_IDENTITY_PROJECTION_CURRENT
                or projection.run_status != "succeeded"
                or projection.run_id != record.get("run_id")
                or projection.chain_head != record.get("event_chain_head")
                or finalize.get("status") != "succeeded"
                or finalize.get("result_published") is not True
            ):
                raise RuntimeIntegrityError("跨运行复用来源不是现行身份下已发布 Result 的成功 run")
            run_id = record.get("run_id")
            if not isinstance(run_id, str) or not run_id:
                raise RuntimeIntegrityError("跨运行复用来源 run_id 无效")
            sources.append(
                (
                    source_root,
                    run_id,
                    CheckpointStore(source_root, create=False),
                    ExternalArtifactStore(
                        source_root / "external-artifacts", create=False
                    ),
                )
            )
        return tuple(sources)

    def _reuse_cross_run_checkpoint(
        self,
        *,
        node: NodeSpec,
        expectation: CheckpointExpectation,
        identity: ExecutionIdentity,
        sources: tuple[
            tuple[Path, str, CheckpointStore, ExternalArtifactStore], ...
        ],
        target_checkpoints: CheckpointStore,
        target_external: ExternalArtifactStore,
        root_seed: int,
        fixed_clock: str,
    ):
        for _source_root, source_run_id, source_checkpoints, source_external in sources:
            checkpoint_path = (
                source_checkpoints.checkpoints_root / expectation.node_execution_id
            )
            if not checkpoint_path.exists():
                continue
            manifest = source_checkpoints.verify(expectation)
            source_outputs = self._decode_outputs(
                (checkpoint_path / manifest.content_path).read_bytes(),
                source_external,
            )
            self._validate_outputs(node, source_outputs)
            if dict(manifest.outputs) != dict(source_outputs.artifact_refs):
                raise RuntimeIntegrityError(
                    f"节点 {node.node_id} 跨运行 checkpoint outputs 与内容引用不一致"
                )
            imported_values = {}
            for port, value in source_outputs.values.items():
                if value.external_commit is None:
                    imported_values[port] = value
                    continue
                imported_values[port] = RuntimeNodeValue.external(
                    target_external.import_verified(
                        source_external,
                        value.external_commit,
                    )
                )
            imported_outputs = RuntimeNodeOutputs(
                imported_values,
                completion_metadata=source_outputs.completion_metadata,
            )
            target_checkpoints.commit_bytes(
                expectation=expectation,
                attempt_id=(
                    f"reuse-{expectation.node_execution_id[:16]}-{source_run_id[:12]}"
                ),
                content=imported_outputs.checkpoint_bytes(),
                outputs=imported_outputs.artifact_refs,
                audit_environment_digest=self.audit_environment.manifest_digest,
                execution_identity_digest=identity.identity_digest,
                root_seed=root_seed,
                fixed_clock=fixed_clock,
            )
            return source_run_id, manifest, imported_outputs
        return None

    def retry_node(self, **kwargs) -> dict[str, object]:
        node_id = kwargs.pop("node_id")
        return self.execute(**kwargs, resume=True, retry_node_id=node_id)

    def inspect_checkpoints(
        self,
        *,
        dag: DagSpec,
        run_root: str | Path,
        root_seed: int,
        fixed_clock: str,
    ) -> dict[str, dict[str, object]]:
        """用正式恢复身份只读复验各节点 checkpoint。"""

        root = Path(run_root).resolve()
        events = EventStore(root).read_events()
        committed_by_node = {
            event.node_id: str(event.payload["node_execution_id"])
            for event in events
            if event.kind == "checkpoint_committed"
            and event.node_id is not None
            and isinstance(event.payload.get("node_execution_id"), str)
        }
        checkpoints_root = root / "checkpoints"
        context = DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock))
        values: dict[str, RuntimeNodeOutputs] = {}
        result: dict[str, dict[str, object]] = {}
        node_map = {node.node_id: node for node in dag.nodes}
        for topological_level, node_id in enumerate(dag.topological_order()):
            node = node_map[node_id]
            incoming_nodes = {
                edge.source_node
                for edge in dag.edges
                if edge.target_node == node_id
            }
            unavailable = sorted(incoming_nodes - set(values))
            if unavailable:
                result[node_id] = {
                    "status": "unavailable",
                    "reusable": False,
                    "reason": f"上游 checkpoint 不可复用: {', '.join(unavailable)}",
                }
                continue
            try:
                inputs = self._inputs(dag, node, values)
                _, expectation = self._identity(
                    node,
                    tuple(
                        value.artifact_ref
                        for _, value in sorted(inputs.items())
                    ),
                    context,
                )
                expected_path = checkpoints_root / expectation.node_execution_id
                candidate_id = (
                    expectation.node_execution_id
                    if expected_path.is_dir()
                    else committed_by_node.get(node_id)
                )
                if candidate_id is None:
                    result[node_id] = {
                        "status": "missing",
                        "reusable": False,
                        "reason": "没有已提交 checkpoint",
                    }
                    continue
                if not checkpoints_root.is_dir():
                    raise RuntimeIntegrityError("只读 checkpoint store 缺少 checkpoints 目录")
                checkpoints = CheckpointStore(root, create=False)
                manifest = checkpoints.verify_stored(candidate_id)
                manifest.require_expectation(expectation)
                external = ExternalArtifactStore(
                    root / "external-artifacts", create=False
                )
                checkpoint_path = checkpoints.checkpoints_root / candidate_id
                node_outputs = self._decode_outputs(
                    (checkpoint_path / manifest.content_path).read_bytes(),
                    external,
                )
                self._validate_outputs(node, node_outputs)
                if dict(manifest.outputs) != dict(node_outputs.artifact_refs):
                    raise RuntimeIntegrityError(
                        f"节点 {node_id} checkpoint outputs 与内容引用不一致"
                    )
                values[node_id] = node_outputs
                result[node_id] = {
                    "status": "reusable",
                    "reusable": True,
                    "reason": "checkpoint 通过当前恢复身份和内容复验",
                }
            except (OSError, ValueError, RuntimeIntegrityError) as exc:
                error = safe_error_summary(
                    exc,
                    default_error_code="runtime_checkpoint_invalid",
                )
                result[node_id] = {
                    "status": "rejected",
                    "reusable": False,
                    "reason": error["message"],
                }
        return result

    def _identity(
        self,
        node: NodeSpec,
        inputs: tuple[ArtifactRef, ...],
        context: DeterminismContext,
    ) -> tuple[ExecutionIdentity, CheckpointExpectation]:
        definition = self._definitions.get(node.implementation_id)
        if definition is not None:
            implementation = definition.implementation_ref
            implementation_id = implementation.implementation_id
            implementation_digest = implementation.code_hash
            definition_digest = definition.definition_hash
            mode = CacheCompatibilityMode(definition.cache_compatibility_mode)
        else:
            registry = self.project_registry
            if registry is None:
                raise RuntimeIntegrityError(
                    f"正式 Runtime 缺少已准入项目实现: {node.implementation_id}"
                )
            token = registry.project_token_by_implementation(node.implementation_id)
            if token is None:
                raise RuntimeIntegrityError(
                    f"正式 Runtime 缺少已准入项目实现: {node.implementation_id}"
                )
            implementation_id = token.implementation_id
            definition_digest = token.manifest.operator_spec.spec_hash
            _, identity = self._project_token_and_identity(
                node.implementation_id
            )
            implementation_digest = identity.identity_hash
            mode = CacheCompatibilityMode.BYTE_EXACT
        profile = CacheCompatibilityProfile.from_audit(
            self.audit_environment,
            mode=mode,
            implementation_digest=implementation_digest,
            numerical_backend_names=self.numerical_backend_names
            if mode is CacheCompatibilityMode.NUMERICAL
            else (),
        )
        identity = ExecutionIdentity.build(
            node=node,
            inputs=inputs,
            context=context,
            policy_payload={"configuration_hash": node.configuration_hash},
            audit_manifest=self.audit_environment,
            cache_profile=profile,
            operator_definition_digest=definition_digest,
        )
        return identity, CheckpointExpectation(
            identity.node_execution_id,
            inputs,
            implementation_id,
            implementation_digest,
            definition_digest,
            profile.profile_digest,
        )

    def _project_token_and_identity(self, implementation_id: str):
        registry = self.project_registry
        if registry is None:
            raise RuntimeIntegrityError(
                f"正式 Runtime 缺少 OperatorDefinition: {implementation_id}"
            )
        token = registry.project_token_by_implementation(implementation_id)
        if not isinstance(token, ProjectOperatorImplementationToken):
            raise RuntimeIntegrityError(
                f"正式 Runtime 缺少已准入 Python Worker 实现: {implementation_id}"
            )
        spec = token.manifest.operator_spec
        return token, project_runtime_identity(
            registry,
            spec.operator_id,
            spec.operator_version,
        )

    def _execute_project_node(
        self,
        *,
        node_context: RuntimeNodeContext,
        run_id: str,
        attempt_id: str,
        node_execution_id: str,
        environment: object,
    ) -> RuntimeNodeOutputs:
        registry = self.project_registry
        if registry is None:
            raise RuntimeIntegrityError("正式 Runtime 未注入项目组合注册表")
        node = node_context.node
        token = registry.project_token_by_implementation(node.implementation_id)
        if token is None:
            raise RuntimeIntegrityError("项目实现未被当前组合注册表准入")
        if "causal_plan" in self.project_parameters_by_node.get(node.node_id, {}):
            from .project_causal_execution import execute_project_causal_node

            return execute_project_causal_node(
                self, node_context=node_context, run_id=run_id,
                attempt_id=attempt_id, environment=environment,
            )
        project_parameters = self.project_parameters_by_node.get(node.node_id, {})
        declared_request_ids = _declared_request_ids(project_parameters)
        project_inputs = tuple(
            self._project_input(
                port, value, node_context.external_store,
                admitted_plans=getattr(environment, "admitted_plans", {}),
                declared_request_ids=declared_request_ids,
                fixed_clock=node_context.fixed_clock,
            )
            for port, value in sorted(node_context.inputs.items())
        )
        partitioned = self._partitioned_project_dataset(project_inputs)
        if partitioned is not None:
            return self._execute_partitioned_project_node(
                node_context=node_context,
                run_id=run_id,
                attempt_id=attempt_id,
                node_execution_id=node_execution_id,
                environment=environment,
                project_inputs=project_inputs,
                dataset=partitioned,
            )
        result = execute_project_worker_attempt(
            registry=registry,
            implementation_id=node.implementation_id,
            node_id=node.node_id,
            run_id=run_id,
            attempt_id=attempt_id,
            attempt_root=node_context.work_dir,
            inputs=project_inputs,
            parameters=project_parameters,
            fixed_clock=node_context.fixed_clock,
            root_seed=node_context.root_seed,
            budget=node_context.remaining_resource_budget(),
            process_slots=node_context.process_slots,
        )
        self._verify_project_request_consumption(
            project_inputs, result.request_traces,
            project_parameters,
        )
        values = {}
        for output in result.outputs:
            values[output.port] = RuntimeNodeValue.external(
                self._commit_project_worker_output(
                    node_context.external_store,
                    output,
                )
            )
        return RuntimeNodeOutputs(values)

    def _execute_partitioned_project_node(
        self,
        *,
        node_context: RuntimeNodeContext,
        run_id: str,
        attempt_id: str,
        node_execution_id: str,
        environment: object,
        project_inputs: tuple[ProjectRuntimeInput, ...],
        dataset,
    ) -> RuntimeNodeOutputs:
        """由 Supervisor 逐月运行项目算子，并只复用有效 checkpoint。"""

        node = node_context.node
        parameters = self.project_parameters_by_node.get(node.node_id, {})
        parameters_hash = typed_canonical_hash(parameters)
        store = PartitionCheckpointStore(
            node_context.external_store.root.parent,
            node_execution_id,
        )
        minute_root = getattr(environment, "minute_data_root", None)
        roots = {"runtime_artifacts": node_context.external_store.root}
        if minute_root is not None:
            roots["minute_data"] = Path(minute_root)
        minute_input = next(
            (
                item
                for item in project_inputs
                if item.artifact.artifact_type == "data.minute-bars.v1"
            ),
            None,
        )
        if minute_input is not None:
            roots["runtime_artifact"] = (
                node_context.external_store.objects_root
                / minute_input.artifact.artifact_key
            )
        outputs_by_partition: dict[str, Mapping[str, object]] = {}
        state: ProjectWorkerState | None = None
        stateful: bool | None = None
        continuous_prefix = True
        for partition in dataset.partitions:
            expectation = PartitionCheckpointExpectation(
                node_execution_id=node_execution_id,
                partition_key=partition.partition_key,
                input_partition_id=typed_canonical_hash(partition.to_dict()),
                implementation_id=node.implementation_id,
                parameters_hash=parameters_hash,
                fixed_clock=node_context.fixed_clock,
                root_seed=node_context.root_seed,
                state_in_semantic_hash=(
                    None
                    if state is None
                    else str(state.commit["semantic_hash"])
                ),
            )
            reusable = store.exists(partition.partition_key) and (
                stateful is not True or continuous_prefix
            )
            if reusable:
                checkpoint = store.load(
                    expectation,
                    external_store=node_context.external_store,
                )
                checkpoint_stateful = checkpoint.state_out is not None
                if stateful is not None and checkpoint_stateful != stateful:
                    raise RuntimeIntegrityError("项目分区 state 合同在月份间发生变化")
                stateful = checkpoint_stateful
                outputs_by_partition[partition.partition_key] = checkpoint.outputs
                state = self._project_state_from_checkpoint(
                    checkpoint,
                    node_context.external_store,
                )
                continue
            if stateful is True:
                continuous_prefix = False
            partition_attempt = self._next_partition_attempt_root(
                node_context.work_dir,
                partition.partition_key,
            )
            from research_pipeline.data_plane import PartitionedDatasetResolver

            verified_partition = PartitionedDatasetResolver(roots).resolve_partition(
                dataset,
                partition.partition_key,
            )
            result = execute_project_worker_attempt(
                registry=self.project_registry,
                implementation_id=node.implementation_id,
                node_id=node.node_id,
                run_id=run_id,
                attempt_id=f"{attempt_id}-{partition.partition_key}",
                attempt_root=partition_attempt,
                inputs=project_inputs,
                parameters=parameters,
                fixed_clock=node_context.fixed_clock,
                root_seed=node_context.root_seed,
                budget=node_context.remaining_resource_budget(),
                process_slots=node_context.process_slots,
                partition_key=partition.partition_key,
                dataset_roots=roots,
                verified_partition_id=typed_canonical_hash(
                    verified_partition.reference.to_dict()
                ),
                state_in=state,
            )
            current_stateful = result.state is not None
            if stateful is not None and current_stateful != stateful:
                raise RuntimeIntegrityError("项目分区 state 合同在月份间发生变化")
            stateful = current_stateful
            committed_outputs = {
                output.port: self._commit_project_worker_output(
                    node_context.external_store,
                    output,
                )
                for output in result.outputs
            }
            state_commit = (
                None
                if result.state is None
                else self._commit_project_worker_state(
                    node_context.external_store,
                    result.state,
                )
            )
            checkpoint = PartitionCheckpoint(
                expectation,
                committed_outputs,
                state_commit,
                (
                    None
                    if result.state is None
                    else str(result.state.commit["schema_hash"])
                ),
            )
            store.commit(checkpoint)
            outputs_by_partition[partition.partition_key] = committed_outputs
            state = (
                None
                if result.state is None
                else ProjectWorkerState(
                    result.state.path,
                    {
                        **dict(result.state.commit),
                        "semantic_hash": state_commit.semantic_hash,
                    },
                )
            )
        return RuntimeNodeOutputs(
            self._assemble_partitioned_project_outputs(
                node_context.external_store,
                outputs_by_partition,
            )
        )

    @staticmethod
    def _partitioned_project_dataset(inputs: tuple[ProjectRuntimeInput, ...]):
        from research_pipeline.data_plane import PartitionedDatasetRef

        candidates = []
        for item in inputs:
            if item.artifact.artifact_type != "data.minute-bars.v1":
                continue
            try:
                payload = json.loads(item.content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeIntegrityError("项目分钟输入 manifest 无法解析") from exc
            raw = payload.get("partitioned_dataset") if isinstance(payload, Mapping) else None
            if not isinstance(raw, Mapping):
                raise RuntimeIntegrityError("项目分钟输入缺少分区 manifest")
            candidates.append(PartitionedDatasetRef.from_dict(raw))
        if not candidates:
            return None
        if len(candidates) != 1:
            raise RuntimeIntegrityError("项目分区执行当前要求恰好一个分钟数据输入")
        return candidates[0]

    @staticmethod
    def _next_partition_attempt_root(work_root: Path, partition_key: str) -> Path:
        base = work_root / "partitions" / partition_key
        base.mkdir(parents=True, exist_ok=True)
        number = 1
        while (base / f"attempt-{number}").exists():
            number += 1
        target = base / f"attempt-{number}"
        target.mkdir()
        return target

    @staticmethod
    def _commit_project_worker_output(external, output):
        staging = external.prepare()
        if output.path.is_dir():
            if output.commit.get("publish_at_artifact_root", False):
                shutil.copytree(output.path, staging, dirs_exist_ok=True)
            else:
                shutil.copytree(output.path, staging / output.port)
            return external.commit(
                staging,
                artifact_name=output.port,
                artifact_type=output.artifact_type,
                producer_scope="project",
            )
        with output.path.open("rb") as handle:
            header = handle.read(4)
            handle.seek(max(0, output.path.stat().st_size - 4))
            footer = handle.read(4)
        suffix = (
            "data.parquet"
            if header == footer == b"PAR1"
            else "content.bin"
        )
        target = staging / output.port / suffix
        target.parent.mkdir()
        shutil.copyfile(output.path, target)
        return external.commit(
            staging,
            artifact_name=output.port,
            artifact_type=output.artifact_type,
            producer_scope="project",
        )

    @staticmethod
    def _commit_project_worker_state(external, state: ProjectWorkerState):
        staging = external.prepare()
        target = staging / "runtime_state" / "content.bin"
        target.parent.mkdir()
        shutil.copyfile(state.path, target)
        return external.commit(
            staging,
            artifact_name="runtime_state",
            artifact_type="runtime.project-state",
            producer_scope="project",
        )

    @staticmethod
    def _project_state_from_checkpoint(
        checkpoint: PartitionCheckpoint,
        external: ExternalArtifactStore,
    ) -> ProjectWorkerState | None:
        if checkpoint.state_out is None:
            return None
        commit = checkpoint.state_out
        if len(commit.files) != 1 or checkpoint.state_out_schema_hash is None:
            raise RuntimeIntegrityError("项目分区 state checkpoint 文件合同无效")
        relative_path = next(iter(commit.files))
        path = external.objects_root / commit.semantic_hash / relative_path
        return ProjectWorkerState(
            path,
            {
                "schema_hash": checkpoint.state_out_schema_hash,
                "semantic_hash": commit.semantic_hash,
                "content_hash": commit.files[relative_path],
                "byte_size": path.stat().st_size,
            },
        )

    @staticmethod
    def _assemble_partitioned_project_outputs(external, by_partition):
        if not by_partition:
            raise RuntimeIntegrityError("项目分区执行没有输出")
        ports = set(next(iter(by_partition.values())))
        if any(set(outputs) != ports for outputs in by_partition.values()):
            raise RuntimeIntegrityError("项目分区输出端口在月份间不一致")
        values = {}
        for port in sorted(ports):
            staging = external.prepare()
            artifact_type = None
            manifest = []
            for partition_key, outputs in sorted(by_partition.items()):
                commit = outputs[port]
                if artifact_type is None:
                    artifact_type = commit.artifact_type
                elif artifact_type != commit.artifact_type:
                    raise RuntimeIntegrityError("项目分区输出类型在月份间不一致")
                source_root = external.objects_root / commit.semantic_hash
                data_files = [
                    path for path in commit.files if path.endswith((".parquet", ".bin"))
                ]
                if len(data_files) != 1:
                    raise RuntimeIntegrityError("项目分区输出必须恰好包含一个数据文件")
                source_relative = data_files[0]
                suffix = Path(source_relative).name
                target = staging / port / partition_key / suffix
                target.parent.mkdir(parents=True)
                shutil.copyfile(source_root / source_relative, target)
                manifest.append({
                    "partition_key": partition_key,
                    "artifact_key": commit.semantic_hash,
                    "source_file": source_relative,
                })
            (staging / "partition_manifest.json").write_text(
                canonical_json({"partitions": manifest}),
                encoding="utf-8",
            )
            committed = external.commit(
                staging,
                artifact_name=port,
                artifact_type=str(artifact_type),
                producer_scope="project",
            )
            values[port] = RuntimeNodeValue.external(committed)
        return values

    @staticmethod
    def _project_input(
        port: str,
        value: RuntimeNodeValue,
        external: ExternalArtifactStore,
        *,
        admitted_plans: Mapping[str, object] | None = None,
        declared_request_ids: frozenset[str] = frozenset(),
        fixed_clock: str | None = None,
    ) -> ProjectRuntimeInput:
        if value.inline_content is not None:
            artifact = ArtifactRef(
                port,
                value.artifact_ref.artifact_type,
                value.artifact_ref.artifact_key,
                value.artifact_ref.content_hash,
            )
            return ProjectRuntimeInput.from_verified(
                artifact,
                value.inline_content,
                typed_canonical_hash({"artifact_type": artifact.artifact_type}),
            )
        commit = value.external_commit
        if commit is None or external.verify(commit.semantic_hash) != commit:
            raise RuntimeIntegrityError("项目算子 external 输入引用漂移")
        if commit.artifact_type == "data.minute-bars.1m.v1":
            relative_path = "result.json"
            content_hash = commit.files.get(relative_path)
            if content_hash is None:
                raise RuntimeIntegrityError("项目分钟输入缺少 result.json")
            content = (
                external.objects_root / commit.semantic_hash / relative_path
            ).read_bytes()
            schema_hash = typed_canonical_hash({"artifact_type": commit.artifact_type})
            artifact = ArtifactRef(
                port,
                commit.artifact_type,
                commit.semantic_hash,
                content_hash,
            )
            request_admissions = RuntimeExecutionService._minute_request_admissions(
                content,
                artifact=artifact,
                schema_hash=schema_hash,
                admitted_plans=admitted_plans,
                declared_request_ids=declared_request_ids,
                fixed_clock=fixed_clock,
            )
            if request_admissions:
                return ProjectRuntimeInput.from_verified(
                    artifact,
                    content,
                    schema_hash,
                    request_admissions=request_admissions,
                )
        if commit.artifact_type == "data.minute-bars.v1":
            relative_path = "result.json"
            content_hash = commit.files.get(relative_path)
            if content_hash is None:
                raise RuntimeIntegrityError("项目分钟输入缺少 result.json")
            content = (
                external.objects_root / commit.semantic_hash / relative_path
            ).read_bytes()
            schema_hash = typed_canonical_hash({"artifact_type": commit.artifact_type})
            artifact = ArtifactRef(
                port,
                commit.artifact_type,
                commit.semantic_hash,
                content_hash,
            )
            return ProjectRuntimeInput.from_verified(
                artifact,
                content,
                schema_hash,
            )
        if commit.artifact_type == "data.columnar-bundle.v1":
            source_root = external.objects_root / commit.semantic_hash
            request_tables = RuntimeExecutionService._request_tables_from_data_bundle(
                source_root
            )
            if admitted_plans is not None:
                if fixed_clock is None:
                    raise RuntimeIntegrityError("项目 request admission 缺少固定时钟")
                for request_id, descriptor in request_tables.items():
                    plan = admitted_plans.get(request_id)
                    if plan is None or plan.plan_hash != descriptor["admitted_plan_hash"]:
                        raise RuntimeIntegrityError(
                            f"项目 request 与正式准入计划不一致: {request_id}"
                        )
                    descriptor["admission"] = RuntimeExecutionService._project_request_admission(
                        plan,
                        descriptor,
                        fixed_clock=fixed_clock,
                    )
            artifact = ArtifactRef(
                port,
                commit.artifact_type,
                commit.semantic_hash,
                commit.artifact_ref.content_hash,
            )
            return ProjectRuntimeInput.from_verified(
                artifact,
                b"",
                typed_canonical_hash({
                    request_id: {
                        "source_identity": item["source_identity"],
                        "schema_hash": item["schema_hash"],
                    }
                    for request_id, item in sorted(request_tables.items())
                }),
                source_root=source_root,
                request_tables=request_tables,
            )
        schema_hash = typed_canonical_hash(dict(commit.schema_hashes))
        artifact = ArtifactRef(
            port,
            commit.artifact_type,
            commit.semantic_hash,
            commit.artifact_ref.content_hash,
        )
        return ProjectRuntimeInput.from_verified(
            artifact,
            b"",
            schema_hash,
            source_root=external.objects_root / commit.semantic_hash,
            files=commit.files,
        )

    @staticmethod
    def _minute_request_admissions(
        content: bytes,
        *,
        artifact: ArtifactRef,
        schema_hash: str,
        admitted_plans: Mapping[str, object] | None,
        declared_request_ids: frozenset[str],
        fixed_clock: str | None,
    ) -> dict[str, Mapping[str, object]]:
        """从已验证分钟工件投影显式绑定 request 的只读准入事实。"""

        if not declared_request_ids:
            return {}
        try:
            payload = json.loads(content.decode("utf-8"))
            request_id = payload.get("request_id")
            raw_dataset = payload.get("partitioned_dataset")
            if not isinstance(request_id, str) or not request_id:
                raise ValueError
            if request_id not in declared_request_ids:
                return {}
            if not isinstance(raw_dataset, Mapping):
                raise ValueError
            dataset = PartitionedDatasetRef.from_dict(raw_dataset)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise RuntimeIntegrityError("项目分钟输入缺少有效 request lineage") from exc
        lineage = dataset.lineage
        plan_hash = lineage.get("admitted_plan_hash")
        source_revision_hash = lineage.get("source_revision_hash")
        if admitted_plans is None:
            raise RuntimeIntegrityError("项目分钟 admission 缺少正式准入计划")
        if fixed_clock is None:
            raise RuntimeIntegrityError("项目分钟 admission 缺少固定时钟")
        plan = admitted_plans.get(request_id)
        if (
            plan is None
            or plan.plan_hash != plan_hash
            or not isinstance(source_revision_hash, str)
            or len(source_revision_hash) != 64
        ):
            raise RuntimeIntegrityError(
                f"项目分钟 request 与正式准入计划不一致: {request_id}"
            )
        source_snapshot_hash = payload.get("source_snapshot_hash")
        if (
            not isinstance(source_snapshot_hash, str)
            or len(source_snapshot_hash) != 64
            or any(char not in "0123456789abcdef" for char in source_snapshot_hash)
        ):
            raise RuntimeIntegrityError("项目分钟输入缺少有效 source snapshot")
        descriptor = {
            "source_identity": artifact.artifact_key,
            "schema_hash": schema_hash,
            "source_revision_hash": source_revision_hash,
            "physical_snapshot_id": source_snapshot_hash,
        }
        descriptor["admission"] = RuntimeExecutionService._project_request_admission(
            plan,
            descriptor,
            fixed_clock=fixed_clock,
        )
        return {request_id: descriptor}

    @staticmethod
    def _request_tables_from_data_bundle(
        source_root: Path,
    ) -> dict[str, Mapping[str, object]]:
        """从已验证外部工件建立 request 到冻结 Parquet 文件的索引。"""

        from research_pipeline.data_plane.research_data_bundle import (
            validate_research_data_bundle,
        )

        try:
            payload = json.loads((source_root / "result.json").read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("项目数据 bundle 缺少有效 result.json") from exc
        raw_bundle = payload.get("data_bundle") if isinstance(payload, Mapping) else None
        if not isinstance(raw_bundle, Mapping):
            raise RuntimeIntegrityError("项目数据 bundle 缺少 request 引用")
        try:
            bundle = validate_research_data_bundle(raw_bundle)
        except ValueError as exc:
            raise RuntimeIntegrityError("项目数据 bundle 合同或身份无效") from exc
        references = bundle["references"]
        admitted_plan_hashes = bundle["admitted_plan_hashes"]
        if not references:
            raise RuntimeIntegrityError("项目数据 bundle 缺少 request 引用")
        resolver = ArtifactResolver(source_root / "data")
        result: dict[str, Mapping[str, object]] = {}
        for request_id, raw_reference in sorted(references.items()):
            if not isinstance(request_id, str) or not isinstance(raw_reference, Mapping):
                raise RuntimeIntegrityError("项目数据 bundle 的 request 引用无效")
            try:
                reference = DatasetArtifactRef.from_dict(raw_reference)
                resolver.resolve(reference)
            except Exception as exc:
                raise RuntimeIntegrityError(
                    f"项目数据 bundle 的 request={request_id} 工件无效"
                ) from exc
            files = tuple(
                {
                    "relative_path": (
                        Path("data") / reference.relative_path / relative_path
                    ).as_posix(),
                    "byte_size": (
                        source_root / "data" / reference.relative_path / relative_path
                    ).stat().st_size,
                }
                for relative_path in reference.partitions
            )
            result[request_id] = {
                "admitted_plan_hash": str(admitted_plan_hashes[request_id]),
                "source_identity": reference.manifest_hash,
                "schema_hash": reference.schema_hash,
                "source_revision_hash": reference.source_revision_hash,
                "physical_snapshot_id": reference.physical_snapshot_id,
                "files": files,
            }
        return result

    @staticmethod
    def _project_request_admission(
        plan,
        descriptor,
        *,
        fixed_clock: str,
    ) -> dict[str, object]:
        """将真实计划与已验证工件投影为项目只读事实。"""
        query = plan.query
        try:
            reference_clock = datetime.fromisoformat(fixed_clock)
            as_of_cutoff = resolve_as_of_cutoff(
                query.as_of,
                reference_clock=reference_clock,
                field="QueryIR as_of",
            )
        except (QueryIRInvalidError, ValueError) as exc:
            raise RuntimeIntegrityError("项目 request as_of 无法按固定时钟解释") from exc
        return {
            "plan_hash": plan.plan_hash,
            "dataset_id": query.dataset_id,
            "dataset_version": query.dataset_version,
            "field_ids": list(query.field_ids),
            "time_range": {
                "start": query.time_range.start.isoformat(),
                "end": query.time_range.end.isoformat(),
            },
            "universe": {
                "instruments": list(query.universe.instruments),
                "snapshot_id": query.universe.snapshot_id,
            },
            "as_of": str(query.as_of),
            "as_of_cutoff": as_of_cutoff.isoformat(),
            "object_name": plan.object_name,
            "source_profile": plan.source_profile,
            "input_claim_ceiling": plan.input_claim_ceiling,
            "availability_policy_hash": plan.availability_policy_hash,
            "daily_availability_policy_ref": plan.daily_availability_policy_ref,
            "daily_availability_rule": plan.daily_availability_rule,
            "publication_id": (
                None if plan.factor_publication is None
                else plan.factor_publication.publication_id
            ),
            "source_revision_hash": descriptor["source_revision_hash"],
            "physical_snapshot_id": descriptor["physical_snapshot_id"],
        }

    @staticmethod
    def _verify_project_request_consumption(inputs, traces, parameters) -> None:
        bound = {
            item.artifact.name: item
            for item in inputs
            if (
                getattr(item, "request_tables", {})
                or getattr(item, "request_admissions", {})
            )
        }
        if not bound:
            return
        if not isinstance(traces, Mapping) or set(traces) != set(bound):
            raise RuntimeIntegrityError("项目 request 消费轨迹缺失")
        declared = _declared_request_ids(parameters)
        for port, item in bound.items():
            request_tables = getattr(item, "request_tables", {})
            request_admissions = getattr(item, "request_admissions", {})
            requests = request_tables or request_admissions
            selected = {
                key: value
                for key, value in requests.items()
                if key in declared
            }
            trace = traces[port]
            if not isinstance(trace, Mapping) or set(trace) != set(selected):
                raise RuntimeIntegrityError("项目 request 消费集合不闭合")
            for request_id, descriptor in selected.items():
                row = trace[request_id]
                if (
                    not isinstance(row, Mapping)
                    or row.get("source_identity") != descriptor["source_identity"]
                    or row.get("schema_hash") != descriptor["schema_hash"]
                ):
                    raise RuntimeIntegrityError("项目 request 消费身份或字段漂移")
                if row.get("metadata_only") is True:
                    if set(row) != {
                        "source_identity", "schema_hash", "metadata_only",
                    }:
                        raise RuntimeIntegrityError("项目 request 元数据消费轨迹无效")
                    continue
                if request_admissions:
                    raise RuntimeIntegrityError("项目 admission 输入不得报告数据行消费")
                if (
                    set(row) != {
                        "source_identity", "schema_hash", "columns", "row_count",
                    }
                    or not isinstance(row.get("columns"), list)
                    or not set(row["columns"]) <= set(
                        descriptor["admission"]["field_ids"]
                    )
                ):
                    raise RuntimeIntegrityError("项目 request 消费身份或字段漂移")
                import pyarrow.parquet as pq

                expected_rows = sum(
                    pq.read_metadata(item.source_root / file["relative_path"]).num_rows
                    for file in descriptor["files"]
                )
                if row.get("row_count") != expected_rows:
                    raise RuntimeIntegrityError("项目 request 未完整消费已验证行")

    def _observation_identity(self, node: NodeSpec) -> tuple[str, str, str]:
        definition = self._definitions.get(node.implementation_id)
        if definition is not None:
            return definition.name, definition.version, definition.resource_hint_ref
        registry = self.project_registry
        if registry is None:
            raise RuntimeIntegrityError(
                f"正式 Runtime 缺少已准入项目实现: {node.implementation_id}"
            )
        token = registry.project_token_by_implementation(node.implementation_id)
        if token is None:
            raise RuntimeIntegrityError(
                f"正式 Runtime 缺少已准入项目实现: {node.implementation_id}"
            )
        spec = token.manifest.operator_spec
        return spec.operator_id, spec.operator_version, f"project:{spec.spec_hash}"

    @staticmethod
    def _inputs(
        dag: DagSpec,
        node: NodeSpec,
        outputs: Mapping[str, RuntimeNodeOutputs],
    ) -> dict[str, RuntimeNodeValue]:
        incoming = sorted(
            (edge for edge in dag.edges if edge.target_node == node.node_id),
            key=lambda edge: edge.target_port,
        )
        try:
            return {
                edge.target_port: outputs[edge.source_node].values[edge.source_port]
                for edge in incoming
            }
        except KeyError as exc:
            raise RuntimeIntegrityError("DAG 输入引用缺少 source node/port 输出") from exc

    @staticmethod
    def _decode_outputs(
        content: bytes, external: ExternalArtifactStore
    ) -> RuntimeNodeOutputs:
        try:
            payload = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("checkpoint node value 无法解析") from exc
        if (
            not isinstance(payload, Mapping)
            or set(payload) != {
                "completion_metadata",
                "contract_version",
                "outputs",
            }
            or payload.get("contract_version") != RUNTIME_NODE_VALUE_VERSION
            or not isinstance(payload.get("outputs"), Mapping)
            or not payload["outputs"]
        ):
            raise RuntimeIntegrityError("checkpoint node value schema 无效")
        values = {}
        for port, raw_value in payload["outputs"].items():
            if not isinstance(port, str) or not isinstance(raw_value, Mapping):
                raise RuntimeIntegrityError("checkpoint output 端口 schema 无效")
            expected = {"artifact_ref", "kind"}
            if raw_value.get("kind") == "inline":
                expected.add("content_base64")
            if set(raw_value) != expected:
                raise RuntimeIntegrityError("checkpoint output value schema 无效")
            artifact = ArtifactRef.from_dict(dict(raw_value["artifact_ref"]))
            if artifact.name != port:
                raise RuntimeIntegrityError("checkpoint output 端口与引用名称不一致")
            if raw_value["kind"] == "inline":
                try:
                    raw = base64.b64decode(raw_value["content_base64"], validate=True)
                except (ValueError, TypeError) as exc:
                    raise RuntimeIntegrityError(
                        "checkpoint inline value base64 无效"
                    ) from exc
                values[port] = RuntimeNodeValue(artifact, inline_content=raw)
                continue
            if raw_value["kind"] != "external":
                raise RuntimeIntegrityError("checkpoint node value kind 无效")
            commit = external.verify(artifact.artifact_key)
            values[port] = RuntimeNodeValue.external(commit)
        raw_metadata = payload["completion_metadata"]
        if raw_metadata is not None and not isinstance(raw_metadata, Mapping):
            raise RuntimeIntegrityError("checkpoint completion metadata schema 无效")
        metadata = (
            None
            if raw_metadata is None
            else RuntimeCompletionMetadata.from_dict(raw_metadata)
        )
        return RuntimeNodeOutputs(values, completion_metadata=metadata)

    @staticmethod
    def _coerce_outputs(node: NodeSpec, value: object) -> RuntimeNodeOutputs:
        if isinstance(value, RuntimeNodeOutputs):
            return value
        if isinstance(value, RuntimeNodeValue) and len(node.output_types) == 1:
            return RuntimeNodeOutputs.single(value)
        raise RuntimeIntegrityError(f"节点 {node.node_id} 未返回按端口索引的 typed outputs")

    def _validate_outputs(
        self,
        node: NodeSpec,
        outputs: RuntimeNodeOutputs,
    ) -> None:
        expected = dict(node.output_types)
        actual = {
            port: value.artifact_ref.artifact_type
            for port, value in outputs.values.items()
        }
        if actual != expected:
            raise RuntimeIntegrityError(
                f"节点 {node.node_id} 输出端口或 artifact type 不匹配"
            )
        for requirement in self.result_table_requirements_by_node.get(
            node.node_id, ()
        ):
            port = requirement["source_port"]
            value = outputs.values.get(port)
            if value is None or value.external_commit is None:
                raise RuntimeIntegrityError(
                    f"ResultSpec 表 {requirement['table_id']} 要求外部 Parquet 输出: "
                    f"{node.node_id}/{port}"
                )
            prefix = f"{requirement['path_prefix'].strip('/')}/"
            matched = tuple(
                path
                for path in value.external_commit.files
                if path.startswith(prefix) and path.endswith(".parquet")
            )
            if matched:
                continue
            available = tuple(
                path
                for path in value.external_commit.files
                if path.endswith(".parquet")
            )
            raise RuntimeIntegrityError(
                f"ResultSpec 表 {requirement['table_id']} 未匹配正式 Parquet 前缀 "
                f"{requirement['path_prefix']!r}; 可用 Parquet: {list(available[:20])}"
            )

    @staticmethod
    def _status(
        store: EventStore,
        run_id: str,
        scope: str,
        status: str,
        *,
        command_id: str,
        node_id: str | None = None,
        attempt_id: str | None = None,
        details: Mapping[str, object] | None = None,
    ) -> RuntimeEvent:
        payload = {"status": status}
        if details:
            payload.update(details)
        return store.append(
            run_id,
            f"{scope}_status_changed",
            payload,
            command_id=command_id,
            node_id=node_id,
            attempt_id=attempt_id,
        )

    @classmethod
    def _reconcile_success(
        cls,
        events: EventStore,
        run_id: str,
        node_id: str,
        node_execution_id: str,
    ) -> None:
        projection = events.replay()
        if projection.node_statuses.get(node_id) == "succeeded":
            return
        current = projection.node_statuses.get(node_id)
        all_events = events.read_events()
        active_attempt = next(
            (
                event.attempt_id
                for event in reversed(all_events)
                if event.node_id == node_id and event.attempt_id is not None
            ),
            None,
        )
        active_status = (
            projection.attempt_statuses.get(active_attempt)
            if active_attempt is not None
            else None
        )
        if current is None:
            cls._status(
                events,
                run_id,
                "node",
                "pending",
                command_id=f"{node_id}:recovered-pending",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "ready",
                command_id=f"{node_id}:recovered-ready",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "waiting_for_resources",
                command_id=f"{node_id}:recovered-waiting",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "running",
                command_id=f"{node_id}:recovered-running",
                node_id=node_id,
            )
        elif current == "pending":
            cls._status(
                events,
                run_id,
                "node",
                "ready",
                command_id=f"{node_id}:recovered-ready",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "waiting_for_resources",
                command_id=f"{node_id}:recovered-waiting",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "running",
                command_id=f"{node_id}:recovered-running",
                node_id=node_id,
            )
        elif current == "ready":
            cls._status(
                events,
                run_id,
                "node",
                "waiting_for_resources",
                command_id=f"{node_id}:recovered-waiting",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "running",
                command_id=f"{node_id}:recovered-running",
                node_id=node_id,
            )
        elif current == "retryable_failed":
            suffix = len(events.read_events())
            cls._status(
                events,
                run_id,
                "node",
                "ready",
                command_id=f"{node_id}:recovered-ready:{suffix}",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "waiting_for_resources",
                command_id=f"{node_id}:recovered-waiting:{suffix}",
                node_id=node_id,
            )
            cls._status(
                events,
                run_id,
                "node",
                "running",
                command_id=f"{node_id}:recovered-running:{suffix}",
                node_id=node_id,
            )
        elif current != "running":
            raise RuntimeIntegrityError(
                f"节点 {node_id} 不能从当前状态复用 checkpoint: {current}"
            )
        if active_status in {
            "pending",
            "admitted",
            "ready",
            "waiting_for_resources",
        }:
            raise RuntimeIntegrityError(
                f"节点 {node_id} 的 checkpoint 与 attempt 状态冲突"
            )
        if not any(
            event.kind == "checkpoint_committed"
            and event.node_id == node_id
            and event.payload.get("node_execution_id") == node_execution_id
            for event in all_events
        ):
            events.append(
                run_id,
                "checkpoint_committed",
                {"node_execution_id": node_execution_id},
                command_id=f"{node_id}:recovered-checkpoint",
                node_id=node_id,
                attempt_id=active_attempt,
            )
        if active_attempt is not None and active_status == "running":
            cls._status(
                events,
                run_id,
                "attempt",
                "succeeded",
                command_id=f"{active_attempt}:recovered-succeeded",
                node_id=node_id,
                attempt_id=active_attempt,
            )
        cls._status(
            events,
            run_id,
            "node",
            "succeeded",
            command_id=f"{node_id}:recovered-succeeded",
            node_id=node_id,
        )

    @classmethod
    def _recover_interrupted_attempt(
        cls,
        events: EventStore,
        run_id: str,
        node_id: str,
    ) -> bool:
        """把上次进程遗留的非终态 attempt 收敛为可恢复节点。"""
        projection = events.replay()
        node_status = projection.node_statuses.get(node_id)
        if node_status not in {"waiting_for_resources", "running"}:
            return False
        attempt_id = next(
            (
                event.attempt_id
                for event in reversed(events.read_events())
                if event.node_id == node_id and event.attempt_id is not None
            ),
            None,
        )
        if attempt_id is not None:
            attempt_status = projection.attempt_statuses.get(attempt_id)
            if attempt_status == "running":
                terminal_status = "lost"
            elif attempt_status in {
                "pending",
                "admitted",
                "ready",
                "waiting_for_resources",
            }:
                terminal_status = "cancelled"
            else:
                raise RuntimeIntegrityError(
                    f"节点 {node_id} 的 running 状态与 attempt 终态冲突: {attempt_status}"
                )
            cls._status(
                events,
                run_id,
                "attempt",
                terminal_status,
                command_id=f"{attempt_id}:recovered-{terminal_status}",
                node_id=node_id,
                attempt_id=attempt_id,
            )
        cls._status(
            events,
            run_id,
            "node",
            "retryable_failed",
            command_id=f"{node_id}:recovered-retryable:{projection.last_seq}",
            node_id=node_id,
        )
        return True

    @staticmethod
    def _write_record(path: Path, payload: Mapping[str, object]) -> None:
        temporary = path.with_suffix(".tmp")
        temporary.write_text(canonical_json(dict(payload)), encoding="utf-8")
        temporary.replace(path)


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
        "temp_bytes": (sampler.peak_scratch_bytes, min(budget.temp_bytes, reservation.scratch_bytes)),
        "process_slots": (sampler.max_processes, reservation.process_slots),
        "wall_milliseconds": (sampler.wall_milliseconds, budget.wall_seconds * 1000),
    }
    exceeded = {name: {"actual": actual, "limit": limit} for name, (actual, limit) in limits.items() if actual > limit}
    if exceeded:
        raise RuntimeWorkerError(
            "节点资源使用超过执行预算或租约",
            error_code="resource_exceeded",
            failure_payload={"code": "resource_exceeded", "exceeded": exceeded},
        )


def _runtime_value_size(value: RuntimeNodeValue, store: ExternalArtifactStore) -> int:
    if value.inline_content is not None:
        return len(value.inline_content)
    if value.external_commit is None:
        return 0
    root = store.objects_root / value.external_commit.semantic_hash
    total = 0
    for relative_path in value.external_commit.files:
        try:
            total += (root / relative_path).stat().st_size
        except OSError as exc:
            raise RuntimeIntegrityError("资源观测无法读取已提交输出大小") from exc
    return total


def _runtime_outputs_size(
    outputs: RuntimeNodeOutputs,
    store: ExternalArtifactStore,
) -> int:
    return sum(_runtime_value_size(value, store) for value in outputs.values.values())


def _reservation_budget(
    budget: ResourceBudget,
    *,
    process_slots: int,
) -> ResourceBudget:
    """保持节点声明的 CPU/内存/scratch 包络；进程槽单独治理。"""

    if type(process_slots) is not int or process_slots <= 0:
        raise RuntimeIntegrityError("节点进程槽声明无效")
    return budget


def _effective_resource_budget(
    budget: ResourceBudget,
    *,
    capacity: ResourceCapacity | None,
) -> ResourceBudget:
    """验证节点声明可被本次运行容量承载，并保持声明上限。"""

    if capacity is None:
        return budget
    if (
        budget.memory_bytes > capacity.memory_bytes
        or budget.cpu_slots > capacity.cpu_slots
        or budget.temp_bytes > capacity.temp_bytes
    ):
        raise RuntimeAdmissionError("节点声明预算超过运行总容量")
    return budget


__all__ = [
    "OPERATOR_DAG_RUN_VERSION",
    "RuntimeExecutionService",
    "RuntimeNodeContext",
    "RuntimeNodeOutputs",
    "RuntimeNodeValue",
]
