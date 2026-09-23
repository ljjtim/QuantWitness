from __future__ import annotations

import json

import pytest

from research_pipeline.runtime import (
    ArtifactRef,
    CheckpointExpectation,
    CheckpointManifest,
    CheckpointStore,
    RuntimeIntegrityError,
)
from research_pipeline.runtime.artifacts import CHECKPOINT_MANIFEST_VERSION


def _input(*, content_hash: str = "data-hash") -> ArtifactRef:
    return ArtifactRef("data", "data.columnar-bundle.v1", "data-key", content_hash)


def _expectation(
    *,
    input_content_hash: str = "data-hash",
    implementation_digest: str = "b" * 64,
    operator_definition_digest: str = "c" * 64,
    cache_profile_digest: str = "d" * 64,
) -> CheckpointExpectation:
    return CheckpointExpectation(
        "a" * 64,
        (_input(content_hash=input_content_hash),),
        "runtime.fixture.identity.v1",
        implementation_digest,
        operator_definition_digest,
        cache_profile_digest,
    )


def _commit(store: CheckpointStore, *, attempt_id: str, content: bytes = b"result"):
    return store.commit_bytes(
        expectation=_expectation(),
        attempt_id=attempt_id,
        content=content,
        output_name="result",
        output_type="runtime.fixture.v1",
        audit_environment_digest="e" * 64,
        execution_identity_digest="f" * 64,
        root_seed=7,
        fixed_clock="2026-07-13T00:00:00+00:00",
    )


def test_checkpoint_commit_verify_and_idempotency(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "run")
    first = _commit(store, attempt_id="attempt-1")
    second = _commit(store, attempt_id="attempt-2")
    assert first.output.content_hash == second.output.content_hash
    assert first.contract_version == CHECKPOINT_MANIFEST_VERSION
    assert store.verify(_expectation()).node_execution_id == "a" * 64


def test_checkpoint_rejects_tamper_missing_marker_and_conflict(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "run")
    _commit(store, attempt_id="attempt-1")
    with pytest.raises(RuntimeIntegrityError, match="冲突"):
        _commit(store, attempt_id="attempt-2", content=b"changed")
    content = tmp_path / "run" / "checkpoints" / ("a" * 64) / "content.bin"
    content.write_bytes(b"tampered")
    with pytest.raises(RuntimeIntegrityError, match="内容"):
        store.verify(_expectation())
    content.write_bytes(b"result")
    (content.parent / "COMMITTED").unlink()
    with pytest.raises(RuntimeIntegrityError, match="marker"):
        store.verify(_expectation())


def test_checkpoint_manifest_path_is_not_trusted(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "run")
    _commit(store, attempt_id="attempt-1")
    manifest_path = tmp_path / "run" / "checkpoints" / ("a" * 64) / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["content_path"] = "../../outside"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeIntegrityError):
        store.verify(_expectation())


def test_checkpoint_rejects_current_identity_mismatch_and_legacy_schema(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "run")
    _commit(store, attempt_id="attempt-1")
    with pytest.raises(RuntimeIntegrityError, match="当前预期"):
        store.verify(_expectation(implementation_digest="9" * 64))

    manifest_path = tmp_path / "run" / "checkpoints" / ("a" * 64) / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["contract_version"] = "research-runtime-checkpoint-v1"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeIntegrityError, match="旧 checkpoint"):
        store.verify_stored("a" * 64)
    with pytest.raises(RuntimeIntegrityError, match="旧 checkpoint"):
        CheckpointManifest.from_dict({"contract_version": "research-runtime-checkpoint-v1"})
    with pytest.raises(RuntimeIntegrityError, match="旧 checkpoint"):
        CheckpointManifest.from_dict({"contract_version": "research-runtime-checkpoint-v2"})


def test_partial_staging_is_never_reused_and_can_be_recomputed(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "run")

    def interrupt_after_prepare(phase: str) -> None:
        if phase == "checkpoint_prepared":
            raise OSError("模拟 staging 写完但尚未提交")

    with pytest.raises(OSError, match="尚未提交"):
        store.commit_bytes(
            expectation=_expectation(),
            attempt_id="attempt-partial",
            content=b"partial",
            output_name="result",
            output_type="runtime.fixture.v1",
            audit_environment_digest="e" * 64,
            execution_identity_digest="f" * 64,
            root_seed=7,
            fixed_clock="2026-07-13T00:00:00+00:00",
            phase_hook=interrupt_after_prepare,
        )

    assert [path.name for path in store.list_uncommitted_staging()] == ["attempt-partial"]
    with pytest.raises(RuntimeIntegrityError, match="marker"):
        store.verify(_expectation())

    committed = _commit(store, attempt_id="attempt-partial", content=b"recomputed")
    clean = _commit(
        CheckpointStore(tmp_path / "clean"),
        attempt_id="attempt-clean",
        content=b"recomputed",
    )
    assert committed.output.content_hash == clean.output.content_hash
    assert store.list_uncommitted_staging() == ()


@pytest.mark.parametrize(
    "changed_expectation",
    [
        {"input_content_hash": "changed-input"},
        {"implementation_digest": "9" * 64},
        {"operator_definition_digest": "8" * 64},
        {"cache_profile_digest": "7" * 64},
    ],
    ids=("input", "implementation", "operator-definition", "environment-profile"),
)
def test_checkpoint_rejects_input_implementation_and_environment_drift(
    tmp_path,
    changed_expectation,
) -> None:
    store = CheckpointStore(tmp_path / "run")
    _commit(store, attempt_id="attempt-1")
    with pytest.raises(RuntimeIntegrityError, match="当前预期"):
        store.verify(_expectation(**changed_expectation))
