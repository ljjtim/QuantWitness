"""运行收尾、checkpoint 发布与恢复的独立边界回归。"""
from argparse import Namespace
import hashlib
import json
from types import SimpleNamespace

import pytest

from research_pipeline.application import research_run
from research_pipeline.cli.parser import build_parser
from research_pipeline.runtime import (
    ArtifactRef, CheckpointPolicy, CheckpointStore, DagSpec, Edge, EventStore, NodeSpec,
    PartitionSpec, ResourceBudget, RetryPolicy, RuntimeIntegrityError,
    RuntimeProjection, RuntimeWorkerError, plan_rerun_from, plan_resume,
)
from research_pipeline.runtime.execution_service import _require_sampled_resources
from research_pipeline.runtime.liveness import (
    RuntimeLiveness, read_runtime_liveness, require_inactive_owner, runtime_owner_alive,
)
from research_pipeline.runtime.operator_runtime import RuntimeCompletionMetadata
from research_pipeline.runtime.resource_governor import ResourceVector
from test_runtime_checkpoint import _commit, _expectation


def _diamond():
    def node(name, inputs=()):
        return NodeSpec(
            name, "runtime.fixture.v1", inputs, (("out", "x"),),
            ResourceBudget(1024, 1, 1024, 10), RetryPolicy(2, ("worker_crash",)),
            CheckpointPolicy.REQUIRED, PartitionSpec(False),
        )
    return DagSpec("diamond", (
        node("a"), node("b", (("in", "x"),)), node("c", (("in", "x"),)),
        node("d", (("left", "x"), ("right", "x"))),
    ), (
        Edge("a", "out", "b", "in", "x"), Edge("a", "out", "c", "in", "x"),
        Edge("b", "out", "d", "left", "x"), Edge("c", "out", "d", "right", "x"),
    ))


def test_metadata_preserves_independent_nodes_and_fields():
    short = RuntimeCompletionMetadata(artifact_hashes={"features": "1" * 64}, counts={"lookback": 5})
    long = RuntimeCompletionMetadata(artifact_hashes={"features": "2" * 64}, counts={"lookback": 20})
    result = RuntimeCompletionMetadata.merge({"short": short, "long": long})
    assert result.artifact_hashes == {"short/features": "1" * 64, "long/features": "2" * 64}
    assert result.counts == {"short/lookback": 5, "long/lookback": 20}
    assert result == RuntimeCompletionMetadata.merge({"long": long, "short": short})
    renamed = RuntimeCompletionMetadata.merge({"x": short, "y": long})
    assert sorted(renamed.artifact_hashes.values()) == sorted(result.artifact_hashes.values())
    assert sorted(renamed.counts.values()) == sorted(result.counts.values())


@pytest.mark.parametrize("phase", ["checkpoint_prepared", "marker_written", "renamed"])
def test_checkpoint_interrupt_boundaries_are_atomic(tmp_path, phase):
    store = CheckpointStore(tmp_path)
    def interrupt(current):
        if current == phase:
            raise OSError("模拟发布中断")
    with pytest.raises(OSError, match="发布中断"):
        store.commit_bytes(
            expectation=_expectation(), attempt_id="attempt", content=b"result",
            outputs={
                "result": ArtifactRef(
                    "result",
                    "runtime.fixture.v1",
                    _expectation().node_execution_id,
                    hashlib.sha256(b"result").hexdigest(),
                ),
            },
            audit_environment_digest="e" * 64, execution_identity_digest="f" * 64,
            root_seed=7, fixed_clock="2026-07-13T00:00:00+00:00", phase_hook=interrupt,
        )
    target = store.checkpoints_root / _expectation().node_execution_id
    assert target.exists() is (phase == "renamed")
    if target.exists():
        assert (target / "COMMITTED").is_file()
        store.verify(_expectation())
    recovered = _commit(store, attempt_id="attempt")
    assert recovered.content_size == len(b"result")


def test_uncommitted_target_can_recompute_but_committed_corruption_rejects(tmp_path):
    store = CheckpointStore(tmp_path)
    target = store.checkpoints_root / _expectation().node_execution_id
    target.mkdir()
    (target / "content.bin").write_bytes(b"partial")
    _commit(store, attempt_id="attempt")
    (target / "content.bin").write_bytes(b"corrupt")
    with pytest.raises(RuntimeIntegrityError, match="hash"):
        _commit(store, attempt_id="attempt-2")


@pytest.mark.parametrize("branch_status", [None, "pending", "succeeded", "exhausted"])
def test_rerun_only_reuses_successful_diamond_branch(branch_status):
    statuses = {"a": "succeeded", "b": "exhausted"}
    if branch_status is not None:
        statuses["c"] = branch_status
    projection = RuntimeProjection(run_id="parent", run_status="failed", node_statuses=statuses)
    plan = plan_rerun_from(_diamond(), "child", "parent", projection, "b", verified_nodes=frozenset("abcd"))
    assert set(plan.reuse_nodes) == ({"a", "c"} if branch_status == "succeeded" else {"a"})
    assert set(plan.recompute_nodes) == set("abcd") - set(plan.reuse_nodes)
    assert set(plan.force_recompute_nodes) == {"b", "d"}


def test_unverified_upstream_invalidates_successful_descendants():
    projection = RuntimeProjection(run_id="parent", run_status="succeeded", node_statuses=dict.fromkeys("abcd", "succeeded"))
    plan = plan_rerun_from(_diamond(), "child", "parent", projection, "b", verified_nodes=frozenset("bcd"))
    assert plan.reuse_nodes == ()
    assert set(plan.recompute_nodes) == set("abcd")


@pytest.mark.parametrize("status", ["created", "planned"])
def test_startup_states_are_resumable(status):
    plan = plan_resume(_diamond(), "run", RuntimeProjection(run_id="run", run_status=status), verified_nodes=frozenset(), invalid_nodes=frozenset())
    assert set(plan.recompute_nodes) == set("abcd")


def test_active_owner_rejects_takeover_and_releases_cleanly(tmp_path):
    owner = RuntimeLiveness(tmp_path, run_id="run", phase="starting").start()
    try:
        assert runtime_owner_alive(read_runtime_liveness(tmp_path))
        with pytest.raises(RuntimeIntegrityError, match="owner"):
            require_inactive_owner(tmp_path)
        owner.set_node(node_id="n", attempt_id="n-attempt-1", requested_resources={})
        assert read_runtime_liveness(tmp_path)["node_id"] == "n"
        payload = dict(read_runtime_liveness(tmp_path))
        payload["process_started_at"] -= 100
        assert not runtime_owner_alive(payload)
    finally:
        owner.stop()
    require_inactive_owner(tmp_path)


def test_dead_owner_is_resumable(tmp_path):
    owner = RuntimeLiveness(tmp_path, run_id="run", phase="starting").start()
    owner.stop()
    path = tmp_path / "runtime-liveness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.update(phase="executing", pid=2**30)
    path.write_text(json.dumps(payload), encoding="utf-8")
    require_inactive_owner(tmp_path)


def test_completion_persistence_failure_precedes_terminal_event(tmp_path):
    store = EventStore(tmp_path)
    for status in ("created", "planned", "running"):
        store.append("run", "run_status_changed", {"status": status}, command_id=status)
    def fail(_event):
        raise OSError("模拟收尾写入失败")
    with pytest.raises(OSError, match="收尾"):
        store.append("run", "run_status_changed", {"status": "succeeded"}, command_id="success", before_append=fail)
    assert store.replay().run_status == "running"


@pytest.mark.parametrize("field,value,code", [
    ("peak_rss_bytes", 101, "resource_exceeded"),
    ("peak_scratch_bytes", 101, "resource_exceeded"),
    ("max_processes", 3, "resource_exceeded"),
    ("wall_milliseconds", 10001, "resource_exceeded"),
    ("measurement_status", "measurement_unavailable", "resource_measurement_unavailable"),
])
def test_final_resource_sample_blocks_success(field, value, code):
    values = dict(peak_rss_bytes=100, peak_scratch_bytes=100, max_processes=2,
                  wall_milliseconds=10000, measurement_status="available")
    budget = ResourceBudget(200, 1, 200, 10)
    reservation = ResourceVector(100, 1, 100, 2)
    _require_sampled_resources(SimpleNamespace(**values), budget=budget, reservation=reservation)
    values[field] = value
    with pytest.raises(RuntimeWorkerError) as error:
        _require_sampled_resources(SimpleNamespace(**values), budget=budget, reservation=reservation)
    assert error.value.failure_payload["code"] == code
    assert error.value.error_code == code


def test_proof_rejected_before_plan_or_database_access():
    with pytest.raises(ValueError, match="acceptance-proof"):
        research_run._execute_operator_graph(Namespace(acceptance_proof="old.json"))


@pytest.mark.parametrize("prefix", ["run", "workspace run"])
def test_cli_does_not_accept_proof_option(prefix):
    parser = build_parser()
    command = parser._subparsers._group_actions[0].choices[prefix.split()[0]]
    if prefix == "workspace run":
        command = command._subparsers._group_actions[0].choices["run"]
    assert "--acceptance-proof" not in command._option_string_actions
    with pytest.raises(SystemExit):
        command.parse_args(["--acceptance-proof", "old.json"])


def test_workspace_rejects_proof_before_creating_execution(tmp_path):
    from research_pipeline.workspace import WorkspaceError, run_workspace_execution
    with pytest.raises(WorkspaceError, match="acceptance-proof"):
        run_workspace_execution(
            tmp_path / "missing", execution_id="example", plan="missing", data_db="missing",
            clock="2026-01-01T00:00:00+00:00", root_seed=0, acceptance_proof="old.json",
        )
    assert not (tmp_path / "missing").exists()


def test_saved_proof_invocation_rejected_before_loading_plan(tmp_path):
    path = tmp_path / "operator-dag-invocation.json"
    path.write_text(json.dumps({"acceptance_proof": "old.json"}), encoding="utf-8")
    with pytest.raises(ValueError, match="acceptance-proof"):
        research_run._load_operator_invocation(tmp_path)


def test_owner_claim_allows_only_one_concurrent_caller(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from research_pipeline.runtime.errors import RuntimeErrorBase

    barrier = Barrier(2)
    def claim():
        owner = RuntimeLiveness(tmp_path, run_id="run", phase="starting")
        barrier.wait()
        try:
            return owner.start()
        except RuntimeErrorBase:
            return None
    with ThreadPoolExecutor(max_workers=2) as executor:
        owners = list(executor.map(lambda _: claim(), range(2)))
    active = [owner for owner in owners if owner is not None]
    try:
        assert len(active) == 1
        assert runtime_owner_alive(read_runtime_liveness(tmp_path))
    finally:
        for owner in active:
            owner.stop()


def test_releasing_old_owner_does_not_stop_later_same_process_call(tmp_path):
    first = RuntimeLiveness(tmp_path, run_id="run", phase="starting").start()
    first.stop()
    second = RuntimeLiveness(tmp_path, run_id="run", phase="starting").start()
    try:
        first.stop()
        assert runtime_owner_alive(read_runtime_liveness(tmp_path))
    finally:
        second.stop()


def test_heartbeat_start_exception_releases_owner(tmp_path, monkeypatch):
    import threading
    def fail(_thread):
        raise RuntimeError("模拟心跳线程启动失败")
    monkeypatch.setattr(threading.Thread, "start", fail)
    with pytest.raises(RuntimeError, match="启动失败"):
        RuntimeLiveness(tmp_path, run_id="run", phase="starting").start()
    require_inactive_owner(tmp_path)


@pytest.mark.parametrize("fails", [False, True])
def test_cli_owner_covers_preparation_and_releases_on_return(tmp_path, monkeypatch, fails):
    monkeypatch.setattr(research_run, "_validate_run_paths", lambda _args: None)
    def execute(_args, owner, *, prepared_plan=None):
        assert prepared_plan is None
        assert runtime_owner_alive(read_runtime_liveness(tmp_path))
        owner.bind_run(tmp_path, "run")
        owner.update("finalizing")
        with pytest.raises(RuntimeIntegrityError, match="owner"):
            RuntimeLiveness(tmp_path, run_id="run", phase="starting").start()
        if fails:
            raise ValueError("模拟收尾失败")
        return {"status": "result_finalized"}
    monkeypatch.setattr(research_run, "_execute_owned_operator_graph", execute)
    args = Namespace(run_root=tmp_path)
    if fails:
        with pytest.raises(ValueError, match="收尾失败"):
            research_run._execute_operator_graph(args)
    else:
        assert research_run._execute_operator_graph(args)["status"] == "result_finalized"
    require_inactive_owner(tmp_path)


def test_finalize_failure_keeps_call_owner_until_caller_releases(tmp_path):
    owner = RuntimeLiveness(tmp_path, run_id="run", phase="checkpointing").start()
    try:
        with pytest.raises(ValueError, match="completion metadata"):
            research_run._finalize_operator_graph_result(
                args=Namespace(run_root=tmp_path), manifest={}, admitted={},
                runtime_result={"run_id": "run"}, implementation_manifest_hash="a" * 64,
                database=tmp_path / "unused", database_probe=(0, 0),
                source_databases={}, source_database_probes={}, owner=owner,
            )
        assert runtime_owner_alive(read_runtime_liveness(tmp_path))
        assert read_runtime_liveness(tmp_path)["phase"] == "finalizing"
    finally:
        owner.stop()


def test_store_lock_distinguishes_reused_process_id(tmp_path):
    import os
    import psutil
    from research_pipeline.runtime.store import _StoreLock
    from research_pipeline.runtime.errors import RuntimeStateError
    path = tmp_path / ".owner.lock"
    payload = {"pid": os.getpid(), "token": "old", "process_started_at": psutil.Process().create_time()}
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeStateError, match="活动写者"):
        with _StoreLock(path):
            pass
    payload["process_started_at"] -= 100
    path.write_text(json.dumps(payload), encoding="utf-8")
    with _StoreLock(path):
        assert json.loads(path.read_text(encoding="utf-8"))["token"] != "old"
    assert not path.exists()


def test_writer_lock_without_process_timestamp_is_rejected(tmp_path):
    from research_pipeline.runtime.store import _StoreLock
    from research_pipeline.runtime.errors import RuntimeStateError
    path = tmp_path / ".writer.lock"
    payload = {"pid": 2**30, "token": "old"}
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeStateError, match="锁损坏"):
        with _StoreLock(path):
            pass
    assert json.loads(path.read_text(encoding="utf-8")) == payload
