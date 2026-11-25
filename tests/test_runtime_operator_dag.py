from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.runtime import (
    AuditEnvironmentManifest,
    CheckpointPolicy,
    DagSpec,
    DeterminismContext,
    Edge,
    EventStore,
    ExecutionMode,
    NodeSpec,
    PartitionSpec,
    ResourceBudget,
    ResourceCapacity,
    RetryPolicy,
    RuntimeAdmissionError,
    RuntimeExecutionService,
    RuntimeIntegrityError,
    RuntimeNodeOutputs,
    RuntimeNodeValue,
    RuntimeWorkerError,
    derive_run_id,
)
from research_pipeline.runtime.operator_definitions import (
    build_mainline_operator_manifest,
)
from research_pipeline.cli.parser import build_parser
from research_pipeline.application import research_run, runtime_lifecycle
from research_pipeline.research.validation import (
    PersistentHoldoutLedger,
    ValidationError,
    persistent_holdout_ledger_root,
)
from test_generic_lifecycle import _patched_runtime_adapters
from test_research_locked_holdout import _plan as _holdout_plan


pytestmark = pytest.mark.integration


def test_research_run_completion_does_not_branch_on_domain_output_shape() -> None:
    source = Path(research_run.__file__).read_text(encoding="utf-8")
    completion_source = source[
        source.index("def _execute_unified_operator_runtime("):
        source.index("def _write_or_verify_runtime_invocation(")
    ]
    for domain_literal in (
        "research.minute-statistics.v1",
        "research.factor-analysis.v1",
        "research.event-study.v1",
        "research.daily-event-study.v1",
        "research.daily-simulation.v1",
        "research.validity-gates.v1",
    ):
        assert domain_literal not in completion_source
    assert "output_types" not in completion_source


def test_runtime_reuses_validated_recipe_parameters_without_reopening_plan(tmp_path, monkeypatch) -> None:
    from types import SimpleNamespace
    from research_pipeline.platform.operator_contracts import OperatorNodeRecipe, OperatorGraphRecipe

    node = OperatorNodeRecipe.from_dict({
        "node_id": "data", "operator_id": "data.columnar.materialize", "operator_version": "1.0.0",
        "inputs": [], "parameters": {"rules": [{"instrument_id": "000001.XSHE", "limits": [1, 2]}]},
        "strategies": [],
    })
    recipe = OperatorGraphRecipe.build(graph_id="memory_test", nodes=(node,))
    captured = {}
    monkeypatch.setattr(research_run, "_write_or_verify_runtime_invocation", lambda *args: None)
    class CapturedEnvironment(Exception):
        pass
    def capture(**kwargs):
        captured.update(kwargs)
        raise CapturedEnvironment()
    monkeypatch.setattr(research_run, "ResearchRunEnvironment", capture)
    monkeypatch.setattr(research_run, "_holdout_ledger_anchor", lambda args: tmp_path)
    args = SimpleNamespace(run_root=tmp_path, plan=tmp_path / "unopened-plan", clock="2026-10-24T00:00:00+08:00", root_seed=1, workers=1)
    with pytest.raises(CapturedEnvironment):
        research_run._execute_unified_operator_runtime(
            args=args, manifest={}, admitted={}, dag=None, recipe=recipe,
            registry=SimpleNamespace(strategy_specs=()), database=None, source_databases={},
            minute_data_root=None, resource_capacity=None,
        )
    assert captured["node_parameters"] == {"data": node.to_dict()["parameters"]}
    captured["node_parameters"]["data"]["rules"][0]["limits"].append(3)
    assert tuple(node.parameters["rules"][0]["limits"]) == (1, 2)


def _runtime_node(node_id: str, operator_id: str) -> NodeSpec:
    definition = build_mainline_operator_manifest().require_operator(
        operator_id, "1.0.0"
    )
    return NodeSpec(
        node_id,
        definition.implementation_ref.implementation_id,
        tuple((item.port, item.artifact_type) for item in definition.input_schema),
        tuple((item.port, item.artifact_type) for item in definition.output_schema),
        ResourceBudget(1024 * 1024, 1, 1024 * 1024, 30),
        RetryPolicy(2, ("operator_execution_failed",)),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        configuration_hash="f" * 64,
    )


def _dag() -> DagSpec:
    admission = _runtime_node("admission", "data.catalog.admission")
    data = _runtime_node("data", "data.columnar.materialize")
    panel = NodeSpec(
        "panel",
        "research.features.intraday.v1",
        (("data", "data.columnar-bundle.v1"),),
        (("features", "research.feature-set.v1"),),
        ResourceBudget(1024 * 1024, 1, 1024 * 1024, 30),
        RetryPolicy(2, ("operator_execution_failed",)),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        configuration_hash="f" * 64,
    )
    return DagSpec(
        "operator_runtime_fixture",
        (admission, data, panel),
        (
            Edge(
                "admission",
                "admission",
                "data",
                "admission",
                "data.catalog-admission.v1",
            ),
            Edge("data", "data", "panel", "data", "data.columnar-bundle.v1"),
        ),
    )


def _service(
    resource_capacity: ResourceCapacity | None = None,
) -> RuntimeExecutionService:
    audit = AuditEnvironmentManifest.capture(
        build_artifact_digest="a" * 64,
        dependency_distribution_digests={"numpy": "b" * 64, "runtime": "c" * 64},
    )
    return RuntimeExecutionService(
        audit_environment=audit,
        numerical_backend_names=("numpy",),
        resource_capacity=resource_capacity,
    )


def _execute_with_actions(
    service: RuntimeExecutionService,
    *,
    actions,
    **kwargs,
):
    dag = kwargs["dag"]

    def dispatch(context):
        return actions[context.node.node_id](context)

    with _patched_runtime_adapters(dag, dispatch):
        return service.execute(environment=None, **kwargs)


def _retry_with_actions(
    service: RuntimeExecutionService,
    *,
    actions,
    node_id: str,
    **kwargs,
):
    dag = kwargs["dag"]

    def dispatch(context):
        return actions[context.node.node_id](context)

    with _patched_runtime_adapters(dag, dispatch):
        return service.retry_node(environment=None, node_id=node_id, **kwargs)


def _multi_output_dag() -> DagSpec:
    producer = NodeSpec(
        "producer",
        "data.columnar.materialize.v1",
        (),
        (
            ("left", "data.columnar-bundle.v1"),
            ("right", "data.columnar-bundle.v1"),
        ),
        ResourceBudget(1024 * 1024, 1, 1024 * 1024, 30),
        RetryPolicy(2, ("operator_execution_failed",)),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        configuration_hash="e" * 64,
    )
    left_consumer = NodeSpec(
        "left_consumer",
        "research.features.intraday.v1",
        (("data", "data.columnar-bundle.v1"),),
        (("features", "research.feature-set.v1"),),
        ResourceBudget(1024 * 1024, 1, 1024 * 1024, 30),
        RetryPolicy(2, ("operator_execution_failed",)),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        configuration_hash="d" * 64,
    )
    right_consumer = NodeSpec(
        "right_consumer",
        "research.features.intraday.v1",
        (("data", "data.columnar-bundle.v1"),),
        (("features", "research.feature-set.v1"),),
        ResourceBudget(1024 * 1024, 1, 1024 * 1024, 30),
        RetryPolicy(2, ("operator_execution_failed",)),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        configuration_hash="c" * 64,
    )
    return DagSpec(
        "multi_output_runtime_fixture",
        (producer, left_consumer, right_consumer),
        (
            Edge(
                "producer",
                "left",
                "left_consumer",
                "data",
                "data.columnar-bundle.v1",
            ),
            Edge(
                "producer",
                "right",
                "right_consumer",
                "data",
                "data.columnar-bundle.v1",
            ),
        ),
    )


def test_process_local_capacity_is_enforced_and_does_not_change_run_identity(
    tmp_path,
) -> None:
    dag = _dag()
    seen_effective_memory: list[int] = []

    def action(context):
        seen_effective_memory.append(context.effective_resource_budget.memory_bytes)
        port, artifact_type = context.node.output_types[0]
        return RuntimeNodeValue.inline(
            name=port,
            artifact_type=artifact_type,
            content=context.node.node_id.encode("utf-8"),
        )

    actions = {node.node_id: action for node in dag.nodes}
    first_capacity = ResourceCapacity(2 * 1024**2, 2, 2 * 1024**2, 2)
    second_capacity = ResourceCapacity(4 * 1024**2, 4, 4 * 1024**2, 4)
    first = _execute_with_actions(
        _service(first_capacity),
        actions=actions,
        dag=dag,
        run_root=tmp_path / "first",
        project_id="capacity-test",
        root_seed=7,
        fixed_clock="2026-09-03T00:00:00+08:00",
    )
    second = _execute_with_actions(
        _service(second_capacity),
        actions=actions,
        dag=dag,
        run_root=tmp_path / "second",
        project_id="capacity-test",
        root_seed=7,
        fixed_clock="2026-09-03T00:00:00+08:00",
    )

    assert first["run_id"] == second["run_id"]
    assert first["contract_version"] == "research-runtime-operator-dag-run-v3"
    assert first["resource_capacity"] == first_capacity.to_dict()
    assert second["resource_capacity"] == second_capacity.to_dict()
    assert set(seen_effective_memory) == {1024 * 1024}
    assert all(node.resource_budget.memory_bytes == 1024 * 1024 for node in dag.nodes)

    with pytest.raises(RuntimeAdmissionError, match="节点声明预算超过运行总容量"):
        _execute_with_actions(
            _service(ResourceCapacity(512 * 1024, 1, 2 * 1024**2, 1)),
            actions=actions,
            dag=dag,
            run_root=tmp_path / "too-small",
            project_id="capacity-test",
            root_seed=7,
            fixed_clock="2026-09-03T00:00:00+08:00",
        )


def test_process_slots_do_not_inflate_declared_cpu_reservation() -> None:
    from research_pipeline.runtime.execution_service import _reservation_budget

    declared = ResourceBudget(1024, 1, 1024, 30)
    assert _reservation_budget(declared, process_slots=3) == declared


def test_operator_dag_routes_multiple_output_ports_and_resume_keeps_identity(
    tmp_path,
) -> None:
    service = _service()
    seen: dict[str, str] = {}

    def producer(_context):
        return RuntimeNodeOutputs(
            {
                "left": RuntimeNodeValue.inline(
                    name="left",
                    artifact_type="data.columnar-bundle.v1",
                    content=b"left",
                ),
                "right": RuntimeNodeValue.inline(
                    name="right",
                    artifact_type="data.columnar-bundle.v1",
                    content=b"right",
                ),
            }
        )

    def consumer(context):
        seen[context.node.node_id] = context.inputs["data"].inline_content.decode()
        return RuntimeNodeValue.inline(
            name="features",
            artifact_type="research.feature-set.v1",
            content=context.node.node_id.encode(),
        )

    run_root = tmp_path / "run"
    kwargs = {
        "dag": _multi_output_dag(),
        "actions": {
            "producer": producer,
            "left_consumer": consumer,
            "right_consumer": consumer,
        },
        "run_root": run_root,
        "project_id": "fixture",
        "root_seed": 7,
        "fixed_clock": "2026-07-18T00:00:00+08:00",
    }
    result = _execute_with_actions(service, **kwargs)
    assert seen == {"left_consumer": "left", "right_consumer": "right"}
    assert set(result["outputs"]["producer"]) == {"left", "right"}
    original_outputs = result["outputs"]

    def should_not_run(_context):
        raise AssertionError("resume 不应执行已提交的多端口节点")

    resumed = _execute_with_actions(
        service,
        **{
            **kwargs,
            "actions": {
                node_id: should_not_run for node_id in kwargs["actions"]
            },
        },
        resume=True,
    )
    assert resumed["outputs"] == original_outputs
    assert resumed["reused_nodes"] == [
        "producer",
        "left_consumer",
        "right_consumer",
    ]


def test_operator_dag_rejects_missing_or_unknown_output_port(tmp_path) -> None:
    service = _service()
    dag = _multi_output_dag()

    def missing(_context):
        return RuntimeNodeOutputs.single(
            RuntimeNodeValue.inline(
                name="left",
                artifact_type="data.columnar-bundle.v1",
                content=b"left",
            )
        )

    with pytest.raises(RuntimeWorkerError, match="producer"):
        _execute_with_actions(
            service,
            dag=dag,
            actions={
                "producer": missing,
                "left_consumer": lambda _: None,
                "right_consumer": lambda _: None,
            },
            run_root=tmp_path / "missing",
            project_id="fixture",
            root_seed=7,
            fixed_clock="2026-07-18T00:00:00+08:00",
        )


def test_operator_dag_failure_retry_resume_and_external_artifact(tmp_path) -> None:
    service = _service()
    calls = {"admission": 0, "data": 0, "panel": 0}
    fail_data = {"enabled": True}

    def admission(context):
        calls["admission"] += 1
        return RuntimeNodeValue.inline(
            name="admission",
            artifact_type=context.node.output_types[0][1],
            content=b"admitted",
        )

    def data(context):
        calls["data"] += 1
        assert set(context.inputs) == {"admission"}
        staging = context.external_store.prepare()
        pq.write_table(
            pa.table({"code": ["000001.XSHE"], "value": [1.0]}),
            staging / "part.parquet",
        )

        def kill(phase: str) -> None:
            if fail_data["enabled"] and phase == "before_commit":
                raise RuntimeError("kill-before-commit")

        commit = context.external_store.commit(
            staging,
            artifact_name="data",
            artifact_type=context.node.output_types[0][1],
            phase_hook=kill,
        )
        return RuntimeNodeValue.external(commit)

    def panel(context):
        calls["panel"] += 1
        assert context.inputs["data"].external_commit is not None
        return RuntimeNodeValue.inline(
            name="features",
            artifact_type=context.node.output_types[0][1],
            content=b"panel-result",
        )

    actions = {"admission": admission, "data": data, "panel": panel}
    run_root = tmp_path / "run"
    kwargs = {
        "dag": _dag(),
        "actions": actions,
        "run_root": run_root,
        "project_id": "fixture",
        "root_seed": 7,
        "fixed_clock": "2026-07-18T00:00:00+08:00",
        "mode": ExecutionMode.DETERMINISTIC_SERIAL,
    }
    with pytest.raises(RuntimeWorkerError, match="data"):
        _execute_with_actions(service, **kwargs)
    assert not any((run_root / "external-artifacts" / "objects").iterdir())
    assert EventStore(run_root).replay().run_status == "paused"

    fail_data["enabled"] = False
    result = _retry_with_actions(service, **kwargs, node_id="data")
    assert result["status"] == "succeeded"
    assert result["reused_nodes"] == ["admission"]
    assert calls == {"admission": 1, "data": 2, "panel": 1}
    assert set(EventStore(run_root).replay().node_statuses.values()) == {"succeeded"}

    def should_not_run(_context):
        raise AssertionError("resume 不应重新执行已提交节点")

    resumed = _execute_with_actions(
        service,
        **{**kwargs, "actions": {node_id: should_not_run for node_id in actions}},
        resume=True,
    )
    assert resumed["reused_nodes"] == ["admission", "data", "panel"]

    record = json.loads(
        (run_root / "operator-dag-run.json").read_text(encoding="utf-8")
    )
    external_key = record["outputs"]["data"]["data"]["artifact_key"]
    parquet = (
        run_root / "external-artifacts" / "objects" / external_key / "part.parquet"
    )
    parquet.write_bytes(b"tampered")
    with pytest.raises(RuntimeIntegrityError):
        _execute_with_actions(
            service,
            **{**kwargs, "actions": {node_id: should_not_run for node_id in actions}},
            resume=True,
        )


def test_operator_dag_resume_recovers_uncommitted_running_attempt(tmp_path) -> None:
    service = _service()
    node = _runtime_node("admission", "data.catalog.admission")
    dag = DagSpec("interrupted_attempt", (node,), ())
    run_root = tmp_path / "run"
    calls = {"count": 0}

    def admission(context):
        calls["count"] += 1
        return RuntimeNodeValue.inline(
            name="admission",
            artifact_type=context.node.output_types[0][1],
            content=b"admitted",
        )

    kwargs = {
        "dag": dag,
        "actions": {"admission": admission},
        "run_root": run_root,
        "project_id": "fixture",
        "root_seed": 7,
        "fixed_clock": "2026-07-18T00:00:00+08:00",
    }
    run_id = derive_run_id(
        dag,
        dag.dag_id,
        DeterminismContext(7, datetime.fromisoformat(kwargs["fixed_clock"])),
        ExecutionMode.DETERMINISTIC_SERIAL.value,
        audit_manifest_digest=service.audit_environment.manifest_digest,
        project_id="fixture",
    )
    events = EventStore(run_root)
    for status in ("created", "planned", "running"):
        service._status(events, run_id, "run", status, command_id=f"run:{status}")
    service._status(
        events, run_id, "node", "pending", command_id="admission:pending",
        node_id="admission",
    )
    service._status(
        events, run_id, "node", "ready", command_id="admission:ready",
        node_id="admission",
    )
    for status in ("pending", "admitted", "ready"):
        service._status(
            events, run_id, "attempt", status,
            command_id=f"admission-attempt-1:{status}",
            node_id="admission", attempt_id="admission-attempt-1",
        )
    service._status(
        events, run_id, "node", "waiting_for_resources",
        command_id="admission:waiting", node_id="admission",
    )
    service._status(
        events, run_id, "attempt", "waiting_for_resources",
        command_id="admission-attempt-1:waiting", node_id="admission",
        attempt_id="admission-attempt-1",
    )
    service._status(
        events, run_id, "node", "running", command_id="admission:running",
        node_id="admission",
    )
    service._status(
        events, run_id, "attempt", "running",
        command_id="admission-attempt-1:running", node_id="admission",
        attempt_id="admission-attempt-1",
    )
    interrupted = events.replay()
    assert interrupted.run_status == "running"
    assert interrupted.node_statuses == {"admission": "running"}
    assert interrupted.attempt_statuses == {"admission-attempt-1": "running"}

    resumed = _execute_with_actions(service, **kwargs, resume=True)
    projection = EventStore(run_root).replay()
    assert resumed["status"] == "succeeded"
    assert calls == {"count": 1}
    assert projection.run_status == "succeeded"
    assert projection.node_statuses == {"admission": "succeeded"}
    assert projection.attempt_statuses == {
        "admission-attempt-1": "lost",
        "admission-attempt-2": "succeeded",
    }


def test_operator_dag_resume_reconciles_committed_running_attempt(
    tmp_path,
    monkeypatch,
) -> None:
    service = _service()
    node = _runtime_node("admission", "data.catalog.admission")
    dag = DagSpec("committed_interruption", (node,), ())
    run_root = tmp_path / "run"
    original_status = service._status
    interrupt_once = {"enabled": True}

    def interrupt_after_checkpoint(store, run_id, scope, status, **kwargs):
        if interrupt_once["enabled"] and scope == "attempt" and status == "succeeded":
            interrupt_once["enabled"] = False
            raise RuntimeError("模拟进程在 checkpoint 提交后中断")
        return original_status(store, run_id, scope, status, **kwargs)

    monkeypatch.setattr(service, "_status", interrupt_after_checkpoint)

    def admission(context):
        return RuntimeNodeValue.inline(
            name="admission",
            artifact_type=context.node.output_types[0][1],
            content=b"admitted",
        )

    kwargs = {
        "dag": dag,
        "actions": {"admission": admission},
        "run_root": run_root,
        "project_id": "fixture",
        "root_seed": 7,
        "fixed_clock": "2026-07-18T00:00:00+08:00",
    }
    with pytest.raises(RuntimeError, match="checkpoint 提交后中断"):
        _execute_with_actions(service, **kwargs)
    interrupted = EventStore(run_root).replay()
    assert interrupted.node_statuses == {"admission": "running"}
    assert interrupted.attempt_statuses == {"admission-attempt-1": "running"}

    monkeypatch.setattr(service, "_status", original_status)

    def should_not_run(_context):
        raise AssertionError("有效 checkpoint 必须直接复用")

    resumed = _execute_with_actions(
        service,
        **{**kwargs, "actions": {"admission": should_not_run}},
        resume=True,
    )
    projection = EventStore(run_root).replay()
    assert resumed["reused_nodes"] == ["admission"]
    assert projection.run_status == "succeeded"
    assert projection.node_statuses == {"admission": "succeeded"}
    assert projection.attempt_statuses == {"admission-attempt-1": "succeeded"}


def test_operator_dag_rejects_wrong_output_type(tmp_path) -> None:
    service = _service()
    dag = _dag()

    def wrong(context):
        return RuntimeNodeValue.inline(
            name="wrong", artifact_type="wrong.v1", content=b"wrong"
        )

    actions = {node.node_id: wrong for node in dag.nodes}
    with pytest.raises(RuntimeWorkerError, match="admission"):
        _execute_with_actions(
            service,
            dag=dag,
            actions=actions,
            run_root=tmp_path / "wrong",
            project_id="fixture",
            root_seed=7,
            fixed_clock="2026-07-18T00:00:00+08:00",
        )


def test_operator_dag_retry_policy_exhaustion_is_terminal(tmp_path) -> None:
    service = _service()
    node = _runtime_node("admission", "data.catalog.admission")
    dag = DagSpec("retry_exhaustion", (node,), ())

    def fail(_context):
        raise RuntimeError("always fails")

    kwargs = {
        "dag": dag,
        "actions": {"admission": fail},
        "run_root": tmp_path / "exhausted",
        "project_id": "fixture",
        "root_seed": 7,
        "fixed_clock": "2026-07-18T00:00:00+08:00",
    }
    with pytest.raises(RuntimeWorkerError):
        _execute_with_actions(service, **kwargs)
    with pytest.raises(RuntimeWorkerError):
        _retry_with_actions(service, **kwargs, node_id="admission")
    projection = EventStore(tmp_path / "exhausted").replay()
    assert projection.run_status == "failed"
    assert projection.node_statuses["admission"] == "exhausted"


def test_run_cli_has_only_unified_runtime() -> None:
    required = [
        "run",
        "--plan",
        "plan",
        "--data-db",
        "data.duckdb",
        "--artifact-root",
        "artifacts",
        "--handoff-out",
        "handoff.json",
        "--run-root",
        "run",
        "--result-store",
        "results",
        "--clock",
        "2026-07-18T00:00:00+08:00",
    ]
    parsed = build_parser().parse_args(required)
    assert not hasattr(parsed, "execution_engine")
    assert not hasattr(parsed, "contract_fixture")
    assert not hasattr(parsed, "draft_out")
    with pytest.raises(SystemExit):
        build_parser().parse_args([*required, "--execution-engine", "legacy"])


def test_run_cli_uses_process_local_capacity_without_shared_state(
    monkeypatch,
) -> None:
    required = [
        "run",
        "--plan",
        "plan",
        "--data-db",
        "data.duckdb",
        "--artifact-root",
        "artifacts",
        "--handoff-out",
        "handoff.json",
        "--run-root",
        "run",
        "--result-store",
        "results",
        "--clock",
        "2026-07-18T00:00:00+08:00",
    ]
    defaults = build_parser().parse_args(required)
    assert research_run._validate_resource_governance_arguments(defaults).to_dict() == {
        "memory_bytes": 16 * 1024**3,
        "cpu_slots": 4,
        "temp_bytes": 64 * 1024**3,
        "max_workers": 4,
    }
    explicit = build_parser().parse_args([*required, "--resource-cpu-slots", "2"])
    assert research_run._validate_resource_governance_arguments(explicit).cpu_slots == 2
    local = build_parser().parse_args(
        [
            *required,
            "--resource-memory-bytes",
            str(12 * 1024**3),
            "--resource-scratch-bytes",
            "0",
        ]
    )
    capacity = research_run._validate_resource_governance_arguments(local)
    assert capacity.to_dict() == {
        "memory_bytes": 12 * 1024**3,
        "cpu_slots": 4,
        "temp_bytes": 0,
        "max_workers": 4,
    }


def test_formal_worker_count_is_serial_within_runtime_capacity() -> None:
    dag = SimpleNamespace(nodes=())
    capacity = ResourceCapacity(2 * 1024**3, 8, 64 * 1024**3, 6)

    for workers in (None, 1):
        args = SimpleNamespace(mode="deterministic_serial", workers=workers)
        research_run._require_formal_execution_arguments(args)
        assert research_run._resolve_worker_count(
            requested=workers, mode=args.mode, dag=dag, capacity=capacity,
        ) == 1
    for mode, workers in (
        ("bounded_parallel", 0), ("partitioned_batch", -1), ("deterministic_serial", 2),
    ):
        with pytest.raises(ValueError):
            research_run._execute_operator_graph(SimpleNamespace(mode=mode, workers=workers))
    with pytest.raises(ValueError, match="CPU.*max_workers"):
        research_run._resolve_worker_count(
            requested=7, mode="deterministic_serial", dag=dag, capacity=capacity,
        )


def test_shared_resource_governance_requires_process_slots_and_timeout() -> None:
    required = [
        "run",
        "--plan",
        "plan",
        "--data-db",
        "data.duckdb",
        "--artifact-root",
        "artifacts",
        "--handoff-out",
        "handoff.json",
        "--run-root",
        "run",
        "--result-store",
        "results",
        "--clock",
        "2026-07-18T00:00:00+08:00",
    ]
    incomplete = build_parser().parse_args(
        [
            *required,
            "--resource-state-dir",
            "external-state",
        ]
    )
    local = build_parser().parse_args(required)
    research_run._validate_resource_governance_arguments(local, required_process_slots=6)
    with pytest.raises(ValueError, match="真实进程槽要求"):
        research_run._validate_resource_governance_arguments(local, required_process_slots=7)
    local.resource_process_slots = 7
    research_run._validate_resource_governance_arguments(local, required_process_slots=7)

    with pytest.raises(ValueError, match="process-slots.*timeout"):
        research_run._validate_resource_governance_arguments(incomplete)
    complete = build_parser().parse_args(
        [
            *required,
            "--mode",
            "deterministic_serial",
            "--workers",
            "1",
            "--resource-state-dir",
            "external-state",
            "--resource-memory-bytes",
            str(16 * 1024**3),
            "--resource-cpu-slots",
            "8",
            "--resource-scratch-bytes",
            str(64 * 1024**3),
            "--resource-process-slots",
            "3",
            "--resource-timeout-seconds",
            "300",
        ]
    )
    research_run._validate_resource_governance_arguments(complete, required_process_slots=3)
    complete.resource_process_slots = 2
    with pytest.raises(ValueError, match="真实进程槽要求"):
        research_run._validate_resource_governance_arguments(complete, required_process_slots=3)


def test_formal_runtime_invocation_is_hash_bound_and_resumable(
    tmp_path, monkeypatch
) -> None:
    root = tmp_path / "run"
    args = SimpleNamespace(
        plan=str(tmp_path / "plan"),
        data_db=str(tmp_path / "data.duckdb"),
        artifact_root=str(tmp_path / "artifacts"),
        handoff_out=str(tmp_path / "handoff.json"),
        run_root=str(root),
        result_store=str(tmp_path / "results"),
        mode="deterministic_serial",
        workers=1,
        root_seed=7,
        clock="2026-07-18T00:00:00+08:00",
        acceptance_proof=None,
    )
    research_run._write_or_verify_runtime_invocation(root, args)
    monkeypatch.setattr(
        research_run,
        "_execute_operator_graph",
        lambda recovered: {
            "resumed": recovered.runtime_resume,
            "retry_node": recovered.runtime_retry_node,
            "engine": recovered.execution_engine,
        },
    )
    assert research_run.resume_operator_graph(
        run_root=root, retry_node_id="simulation"
    ) == {
        "resumed": True,
        "retry_node": "simulation",
        "engine": "unified",
    }

    invocation = root / "operator-dag-invocation.json"
    payload = json.loads(invocation.read_text(encoding="utf-8"))
    assert payload["holdout_ledger_anchor"] == str((tmp_path / "artifacts").resolve())
    assert payload["local_process_slots"] == 6
    assert research_run._load_operator_invocation(root).resource_process_slots == 6
    payload["workers"] = 2
    invocation.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="invocation"):
        research_run.resume_operator_graph(run_root=root)


def test_governed_runtime_invocation_restores_exact_resource_contract(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "run-governed"
    args = SimpleNamespace(
        plan=str(tmp_path / "plan"),
        data_db=str(tmp_path / "data.duckdb"),
        source_db=[],
        artifact_root=str(tmp_path / "artifacts"),
        handoff_out=str(tmp_path / "handoff.json"),
        run_root=str(root),
        result_store=str(tmp_path / "results"),
        mode="deterministic_serial",
        workers=1,
        root_seed=7,
        clock="2026-07-18T00:00:00+08:00",
        acceptance_proof=None,
        resource_state_dir=str(tmp_path / "external-governor"),
        resource_memory_bytes=8 * 1024**3,
        resource_cpu_slots=8,
        resource_scratch_bytes=32 * 1024**3,
        resource_process_slots=4,
        resource_timeout_seconds=12.5,
        resource_stale_seconds=30.0,
    )
    research_run._write_or_verify_runtime_invocation(root, args)
    invocation = json.loads(
        (root / "operator-dag-invocation.json").read_text(encoding="utf-8")
    )
    assert invocation["contract_version"] == "research-operator-dag-invocation-v17"
    assert invocation["resource_capacity"] == {
        "memory_bytes": 8 * 1024**3,
        "cpu_slots": 8,
        "temp_bytes": 32 * 1024**3,
        "max_workers": 8,
    }
    assert invocation["resource_governance"]["process_slots"] == 4
    assert "memory_bytes" not in invocation["resource_governance"]
    def recovered_runtime(recovered):
        research_run._require_formal_execution_arguments(recovered)
        return {
            "state_dir": recovered.resource_state_dir,
            "timeout": recovered.resource_timeout_seconds,
            "workers": recovered.workers,
        }

    monkeypatch.setattr(research_run, "_execute_operator_graph", recovered_runtime)
    assert research_run.resume_operator_graph(run_root=root) == {
        "state_dir": str((tmp_path / "external-governor").resolve()),
        "timeout": 12.5,
        "workers": 1,
    }


def test_runtime_lifecycle_dispatches_formal_dag_without_fixture_arguments(
    tmp_path, monkeypatch
) -> None:
    root = tmp_path / "run"
    root.mkdir()
    (root / "operator-dag-run.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        research_run,
        "resume_operator_graph",
        lambda **kwargs: {
            "run_root": str(kwargs["run_root"]),
            "retry": kwargs["retry_node_id"],
        },
    )
    result = runtime_lifecycle._execute(
        Namespace(
            command="retry-node",
            run_root=str(root),
            node="simulation",
            handoff=None,
            artifact_root=None,
        )
    )
    assert result == {"run_root": str(root), "retry": "simulation"}


def test_formal_rerun_parser_does_not_require_fixture_paths(tmp_path) -> None:
    parsed = build_parser().parse_args(
        [
            "rerun-from",
            "--run-root",
            str(tmp_path / "parent"),
            "--output-run-root",
            str(tmp_path / "child"),
            "--node",
            "simulation",
        ]
    )
    assert not hasattr(parsed, "contract_fixture")
    assert not hasattr(parsed, "handoff")
    assert not hasattr(parsed, "artifact_root")


def test_formal_rerun_writes_current_child_invocation_with_independent_outputs(
    tmp_path,
    monkeypatch,
) -> None:
    parent = tmp_path / "parent"
    child = tmp_path / "child"
    plan = tmp_path / "plan"
    minute_data = tmp_path / "minute-data"
    plan.mkdir()
    minute_data.mkdir()
    database = tmp_path / "data.duckdb"
    database.write_bytes(b"read-only-probe")
    args = SimpleNamespace(
        plan=str(plan),
        data_db=str(database),
        source_db=[],
        minute_data_root=str(minute_data),
        artifact_root=str(tmp_path / "parent-artifacts"),
        handoff_out=str(tmp_path / "parent-handoff.json"),
        run_root=str(parent),
        result_store=str(tmp_path / "parent-results"),
        mode="deterministic_serial",
        workers=1,
        root_seed=7,
        clock="2026-07-18T00:00:00+08:00",
        acceptance_proof=None,
        execution_engine="unified",
        resource_state_dir=None,
        runtime_rerun_parent_root=None,
    )
    research_run._write_or_verify_runtime_invocation(parent, args)

    def execute_child(child_args):
        research_run._write_or_verify_runtime_invocation(
            Path(child_args.run_root),
            child_args,
        )
        return {"status": "result_finalized"}

    monkeypatch.setattr(research_run, "_execute_operator_graph", execute_child)
    result = research_run.rerun_operator_graph(
        parent_run_root=parent,
        child_run_root=child,
        node_id="simulation",
    )
    invocation = json.loads(
        (child / "operator-dag-invocation.json").read_text(encoding="utf-8")
    )
    assert invocation["contract_version"] == "research-operator-dag-invocation-v17"
    assert invocation["parent_run_root"] == str(parent.resolve())
    assert invocation["rerun_from_node"] == "simulation"
    assert invocation["holdout_ledger_anchor"] == str(
        Path(args.artifact_root).resolve()
    )
    assert invocation["run_root"] == str(child.resolve())
    assert invocation["artifact_root"] != args.artifact_root
    assert "handoff_out" not in invocation
    assert invocation["result_store"] != args.result_store
    assert result == {"status": "result_finalized"}


def test_rerun_holdout_ledger_anchor_rejects_missing_parent(tmp_path) -> None:
    args = SimpleNamespace(
        run_root=str(tmp_path / "child"),
        artifact_root=str(tmp_path / "child-artifacts"),
        runtime_rerun_parent_root=str(tmp_path / "missing-parent"),
        holdout_ledger_anchor=str(tmp_path / "root-artifacts"),
    )

    with pytest.raises(ValueError, match="lineage 无法加载父运行"):
        research_run._holdout_ledger_anchor(args)


def test_rerun_holdout_ledger_anchor_rejects_lineage_cycle(
    tmp_path,
    monkeypatch,
) -> None:
    root_anchor = tmp_path / "root-artifacts"
    first = tmp_path / "first"
    second = tmp_path / "second"
    args = SimpleNamespace(
        run_root=str(tmp_path / "child"),
        artifact_root=str(tmp_path / "child-artifacts"),
        runtime_rerun_parent_root=str(first),
        holdout_ledger_anchor=str(root_anchor),
    )
    invocations = {
        first.resolve(): SimpleNamespace(
            run_root=str(first),
            artifact_root=str(tmp_path / "first-artifacts"),
            runtime_rerun_parent_root=str(second),
            holdout_ledger_anchor=str(root_anchor),
        ),
        second.resolve(): SimpleNamespace(
            run_root=str(second),
            artifact_root=str(tmp_path / "second-artifacts"),
            runtime_rerun_parent_root=str(first),
            holdout_ledger_anchor=str(root_anchor),
        ),
    }
    monkeypatch.setattr(
        research_run,
        "_load_operator_invocation",
        lambda run_root: invocations[Path(run_root).resolve()],
    )

    with pytest.raises(ValueError, match="lineage 存在循环"):
        research_run._holdout_ledger_anchor(args)


def test_rerun_holdout_ledger_anchor_rejects_conflicting_anchor(
    tmp_path,
    monkeypatch,
) -> None:
    parent = tmp_path / "parent"
    args = SimpleNamespace(
        run_root=str(tmp_path / "child"),
        artifact_root=str(tmp_path / "child-artifacts"),
        runtime_rerun_parent_root=str(parent),
        holdout_ledger_anchor=str(tmp_path / "declared-root-artifacts"),
    )
    monkeypatch.setattr(
        research_run,
        "_load_operator_invocation",
        lambda _run_root: SimpleNamespace(
            run_root=str(parent),
            artifact_root=str(tmp_path / "actual-root-artifacts"),
            runtime_rerun_parent_root=None,
            holdout_ledger_anchor=str(tmp_path / "actual-root-artifacts"),
        ),
    )

    with pytest.raises(ValueError, match="ledger anchor 冲突"):
        research_run._holdout_ledger_anchor(args)


@pytest.mark.parametrize(
    ("terminal_status", "terminal_reason"),
    (("committed", None), ("consumed_failed", "fixture_failure")),
)
def test_rerun_output_path_cannot_reset_terminal_holdout(
    tmp_path,
    terminal_status,
    terminal_reason,
) -> None:
    parent = tmp_path / "parent"
    root_artifacts = tmp_path / "parent-artifacts"
    args = SimpleNamespace(
        plan=str(tmp_path / "plan"),
        data_db=str(tmp_path / "data.duckdb"),
        source_db=[],
        minute_data_root=None,
        artifact_root=str(root_artifacts),
        handoff_out=str(tmp_path / "parent-handoff.json"),
        run_root=str(parent),
        result_store=str(tmp_path / "parent-results"),
        mode="deterministic_serial",
        workers=1,
        root_seed=7,
        clock="2026-07-18T00:00:00+08:00",
        acceptance_proof=None,
        execution_engine="unified",
        resource_state_dir=None,
        runtime_rerun_parent_root=None,
    )
    research_run._write_or_verify_runtime_invocation(parent, args)

    child = tmp_path / "independent-child" / "child"
    child_args = research_run._load_operator_invocation(parent)
    child_args.run_root = str(child)
    child_args.artifact_root = str(child.with_name("child-artifacts"))
    child_args.handoff_out = str(child.with_name("child-handoff.json"))
    child_args.result_store = str(child.with_name("child-results"))
    child_args.runtime_rerun_parent_root = str(parent)
    child_args.runtime_rerun_node = "statistics"
    research_run._write_or_verify_runtime_invocation(child, child_args)
    child_args = research_run._load_operator_invocation(child)

    parent_anchor = research_run._holdout_ledger_anchor(
        research_run._load_operator_invocation(parent)
    )
    child_anchor = research_run._holdout_ledger_anchor(child_args)
    assert child_anchor == parent_anchor == root_artifacts.resolve()
    assert child_anchor != Path(child_args.artifact_root).resolve()

    plan = _holdout_plan()
    ledger_root = persistent_holdout_ledger_root(
        parent_anchor,
        scope="family",
        research_identity_hash="e" * 64,
        holdout_identity_hash=plan.holdout_identity_hash,
    )
    ledger = PersistentHoldoutLedger(ledger_root, plan)
    fixed_clock = datetime(2024, 3, 1, tzinfo=timezone.utc)
    prepared = ledger.prepare(
        prepared_at=fixed_clock,
        preflight=lambda: {"format": "parquet", "schema": "fixture-v1"},
    )
    opened = ledger.open(
        opening_id="parent-open",
        token_hash=plan.token_hash,
        actor=plan.actor,
        reason=plan.reason,
        fixed_clock=fixed_clock,
        prepared_hash=str(prepared["prepared_hash"]),
    )
    ledger.finish(
        opened_hash=str(opened["opened_hash"]),
        status=terminal_status,
        result_hash="f" * 64,
        reason=terminal_reason,
    )

    child_ledger_root = persistent_holdout_ledger_root(
        child_anchor,
        scope="family",
        research_identity_hash="e" * 64,
        holdout_identity_hash=plan.holdout_identity_hash,
    )
    assert child_ledger_root == ledger_root
    child_preflight_called = False

    def child_preflight() -> dict[str, object]:
        nonlocal child_preflight_called
        child_preflight_called = True
        return {"format": "parquet", "schema": "fixture-v1"}

    with pytest.raises(ValidationError, match="已经 opened|不能重新预检"):
        PersistentHoldoutLedger(child_ledger_root, plan).prepare(
            prepared_at=fixed_clock,
            preflight=child_preflight,
        )
    assert child_preflight_called is False


def test_runtime_lifecycle_requires_current_invocation(tmp_path) -> None:
    with pytest.raises(ValueError, match="operator DAG invocation"):
        runtime_lifecycle._execute(
            Namespace(
                command="resume",
                run_root=str(tmp_path / "missing-formal-run"),
                handoff="handoff.json",
                artifact_root="artifacts",
            )
        )


def test_formal_rerun_imports_verified_upstream_into_independent_child(
    tmp_path, monkeypatch,
) -> None:
    service = _service()
    dag = _dag()
    parent = tmp_path / "parent"
    child = tmp_path / "child"

    def admission(context):
        return RuntimeNodeValue.inline(
            name="admission",
            artifact_type=context.node.output_types[0][1],
            content=b"admitted",
        )

    def data(context):
        staging = context.external_store.prepare()
        (staging / "result.json").write_text('{"value":1}', encoding="utf-8")
        return RuntimeNodeValue.external(
            context.external_store.commit(
                staging,
                artifact_name="data",
                artifact_type=context.node.output_types[0][1],
            )
        )

    def panel(context):
        return RuntimeNodeValue.inline(
            name="features",
            artifact_type=context.node.output_types[0][1],
            content=b"panel",
        )

    actions = {"admission": admission, "data": data, "panel": panel}
    parent_result = _execute_with_actions(
        service,
        dag=dag,
        actions=actions,
        run_root=parent,
        project_id="fixture",
        root_seed=7,
        fixed_clock="2026-07-18T00:00:00+08:00",
    )
    import research_pipeline.runtime.external_artifact as external_module
    source_reads = []
    original_hash = external_module._sha256
    def hash_file(path):
        if parent in path.parents:
            source_reads.append(path)
        return original_hash(path)
    monkeypatch.setattr(external_module, "_sha256", hash_file)
    recovery = service.prepare_rerun_from(
        dag=dag,
        parent_run_root=parent,
        child_run_root=child,
        project_id="fixture",
        root_seed=7,
        fixed_clock="2026-07-18T00:00:00+08:00",
        mode=ExecutionMode.DETERMINISTIC_SERIAL,
        node_id="panel",
    )
    assert recovery["reuse_nodes"] == ["admission", "data"]
    assert recovery["recompute_nodes"] == ["panel"]
    assert recovery["copied_external_nodes"] == ["data"]
    artifact_key = parent_result["outputs"]["data"]["data"]["artifact_key"]
    assert source_reads == [parent / "external-artifacts" / "objects" / artifact_key / "result.json"]

    calls = {"panel": 0}

    def rerun_panel(context):
        calls["panel"] += 1
        return panel(context)

    result = _execute_with_actions(
        service,
        dag=dag,
        actions={
            "admission": lambda _: (_ for _ in ()).throw(AssertionError()),
            "data": lambda _: (_ for _ in ()).throw(AssertionError()),
            "panel": rerun_panel,
        },
        run_root=child,
        project_id="fixture",
        root_seed=7,
        fixed_clock="2026-07-18T00:00:00+08:00",
        resume=True,
        parent_run_id=parent_result["run_id"],
        rerun_from_node="panel",
        recovery_plan_hash=recovery["recovery_plan_hash"],
    )
    assert result["run_id"] != parent_result["run_id"]
    assert result["reused_nodes"] == ["admission", "data"]
    assert calls["panel"] == 1


def test_formal_rerun_rejects_corrupt_external_before_child_execution(tmp_path) -> None:
    service = _service()
    dag = _dag()
    parent = tmp_path / "parent"

    def admission(context):
        return RuntimeNodeValue.inline(
            name="admission",
            artifact_type=context.node.output_types[0][1],
            content=b"admitted",
        )

    def data(context):
        staging = context.external_store.prepare()
        (staging / "result.json").write_text('{"value":1}', encoding="utf-8")
        return RuntimeNodeValue.external(
            context.external_store.commit(
                staging,
                artifact_name="data",
                artifact_type=context.node.output_types[0][1],
            )
        )

    def panel(context):
        return RuntimeNodeValue.inline(
            name="features",
            artifact_type=context.node.output_types[0][1],
            content=b"panel",
        )

    result = _execute_with_actions(
        service,
        dag=dag,
        actions={"admission": admission, "data": data, "panel": panel},
        run_root=parent,
        project_id="fixture",
        root_seed=7,
        fixed_clock="2026-07-18T00:00:00+08:00",
    )
    artifact_key = result["outputs"]["data"]["data"]["artifact_key"]
    (parent / "external-artifacts/objects" / artifact_key / "result.json").write_text(
        '{"value":2}',
        encoding="utf-8",
    )

    with pytest.raises(RuntimeIntegrityError, match="漂移"):
        service.prepare_rerun_from(
            dag=dag,
            parent_run_root=parent,
            child_run_root=tmp_path / "child",
            project_id="fixture",
            root_seed=7,
            fixed_clock="2026-07-18T00:00:00+08:00",
            mode=ExecutionMode.DETERMINISTIC_SERIAL,
            node_id="panel",
        )
