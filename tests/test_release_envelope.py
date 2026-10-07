from __future__ import annotations

from dataclasses import replace
import json
import hashlib
import platform
from pathlib import Path
import runpy
import subprocess
import sys

import pytest

from research_pipeline.platform import (
    BuildManifest,
    DEPENDENCY_LOCK_VERSION,
    LOCAL_SUPPLY_CHAIN_STATUS,
    REQUIRED_GATE_IDS,
    ReleaseAcceptanceInput,
    ReleaseEnvelope,
    ReleaseEnvelopeError,
    ReleaseGateReceipt,
    canonical_json,
    installed_distribution_digest,
    verify_release_envelope,
)


ROOT = Path(__file__).resolve().parents[1]


def _manifest(*, dirty: bool = False, wheel: str = "4" * 64) -> BuildManifest:
    return BuildManifest.build(
        source_commit="a" * 40,
        source_dirty=dirty,
        source_tree_digest="1" * 64,
        build_toolchain={"python": "3.10.0", "build": "1.5.0"},
        input_digests={"pyproject.toml": "2" * 64, "src/module.py": "3" * 64},
        wheel_digest=wheel,
        sdist_digest="5" * 64,
        dependency_distribution_digests={"numpy==1.0": "6" * 64},
    )


def _receipts(manifest: BuildManifest, *, candidate: str = "rc-20260726"):
    return tuple(
        ReleaseGateReceipt.build(
            gate_id=gate_id,
            release_candidate_id=candidate,
            build_manifest_hash=manifest.manifest_hash,
            evidence_hashes={"acceptance": str(index) * 64},
            issued_at="2026-07-26T12:00:00+08:00",
            expires_at="2026-08-25T12:00:00+08:00",
        )
        for index, gate_id in enumerate(REQUIRED_GATE_IDS, start=1)
    )


def _envelope(*, manifest: BuildManifest | None = None) -> ReleaseEnvelope:
    selected = manifest or _manifest()
    receipts = _receipts(selected)
    acceptance = ReleaseAcceptanceInput.build(
        release_candidate_id="rc-20260726",
        build_manifest_hash=selected.manifest_hash,
        receipts=receipts,
    )
    return ReleaseEnvelope.build(
        release_candidate_id="rc-20260726",
        profile="local",
        manifest=selected,
        capabilities_digest="7" * 64,
        dependency_lock_digest="8" * 64,
        platform="windows",
        python_cache_tag="cpython-310",
        receipts=receipts,
        acceptance=acceptance,
        issued_at="2026-07-26T12:00:00+08:00",
        expires_at="2026-08-25T12:00:00+08:00",
        revocation_policy="not_applicable_no_distribution",
    )


def test_local_release_envelope_binds_clean_manifest_gates_and_truthful_profile() -> None:
    envelope = _envelope()
    assert envelope.gate_ids == REQUIRED_GATE_IDS
    assert dict(envelope.supply_chain_status) == dict(LOCAL_SUPPLY_CHAIN_STATUS)
    assert envelope.profile == "local"
    assert envelope.revocation_policy == "not_applicable_no_distribution"
    assert "envelope_hash" not in envelope.to_dict()
    assert "acceptance_hash" not in envelope.to_dict()
    assert "gate_receipt_hashes" not in envelope.to_dict()
    assert ReleaseEnvelope.from_dict(envelope.to_dict()) == envelope
    envelope.require_valid_at("2026-07-27T00:00:00+08:00")


def test_release_envelope_rejects_dirty_source_old_receipts_and_missing_gate() -> None:
    dirty = _manifest(dirty=True)
    dirty_receipts = _receipts(dirty)
    dirty_acceptance = ReleaseAcceptanceInput.build(
        release_candidate_id="rc-20260726",
        build_manifest_hash=dirty.manifest_hash,
        receipts=dirty_receipts,
    )
    with pytest.raises(ReleaseEnvelopeError, match="source_dirty=false"):
        ReleaseEnvelope.build(
            release_candidate_id="rc-20260726",
            profile="local",
            manifest=dirty,
            capabilities_digest="7" * 64,
            dependency_lock_digest="8" * 64,
            platform="windows",
            python_cache_tag="cpython-310",
            receipts=dirty_receipts,
            acceptance=dirty_acceptance,
            issued_at="2026-07-26T12:00:00+08:00",
            expires_at="2026-08-25T12:00:00+08:00",
            revocation_policy="not_applicable_no_distribution",
        )

    clean = _manifest()
    old_receipts = _receipts(clean, candidate="old-rc")
    with pytest.raises(ReleaseEnvelopeError, match="旧 candidate"):
        ReleaseAcceptanceInput.build(
            release_candidate_id="rc-20260726",
            build_manifest_hash=clean.manifest_hash,
            receipts=old_receipts,
        )
    with pytest.raises(ReleaseEnvelopeError, match="不完整"):
        ReleaseAcceptanceInput.build(
            release_candidate_id="rc-20260726",
            build_manifest_hash=clean.manifest_hash,
            receipts=_receipts(clean)[:-1],
        )


def test_release_envelope_rejects_false_supply_claim_invalid_digest_and_expiry() -> None:
    envelope = _envelope()
    with pytest.raises(ReleaseEnvelopeError, match="candidate/profile 无效"):
        replace(
            envelope,
            profile="distributable",
            supply_chain_status={
                "additional_platforms": "pass",
                "artifact_signature": "pass",
                "sbom": "pass",
            },
        )
    with pytest.raises(ReleaseEnvelopeError, match="不得冒充"):
        replace(
            envelope,
            supply_chain_status={
                "additional_platforms": "pass",
                "artifact_signature": "pass",
                "sbom": "pass",
            },
        )
    with pytest.raises(ReleaseEnvelopeError, match="sha256"):
        replace(envelope, capabilities_digest="invalid")
    with pytest.raises(ReleaseEnvelopeError, match="已过期"):
        envelope.require_valid_at("2026-08-25T12:00:00+08:00")


def test_release_envelope_verifier_recomputes_actual_receipt_closure() -> None:
    manifest = _manifest()
    receipts = _receipts(manifest)
    acceptance = ReleaseAcceptanceInput.build(
        release_candidate_id="rc-20260726",
        build_manifest_hash=manifest.manifest_hash,
        receipts=receipts,
    )
    envelope = _envelope(manifest=manifest)
    verify_release_envelope(
        envelope,
        manifest=manifest,
        receipts=receipts,
        acceptance=acceptance,
        as_of="2026-07-27T00:00:00+08:00",
    )
    with pytest.raises(ReleaseEnvelopeError, match="旧 candidate"):
        verify_release_envelope(
            envelope,
            manifest=manifest,
            receipts=_receipts(manifest, candidate="old-rc"),
            acceptance=acceptance,
            as_of="2026-07-27T00:00:00+08:00",
        )
    expired_receipts = tuple(
        ReleaseGateReceipt.build(
            gate_id=item.gate_id,
            release_candidate_id=item.release_candidate_id,
            build_manifest_hash=item.build_manifest_hash,
            evidence_hashes=item.evidence_hashes,
            issued_at=item.issued_at,
            expires_at="2026-07-26T18:00:00+08:00",
        )
        for item in receipts
    )
    with pytest.raises(ReleaseEnvelopeError, match="receipt 在验收时刻无效"):
        verify_release_envelope(
            envelope,
            manifest=manifest,
            receipts=expired_receipts,
            acceptance=acceptance,
            as_of="2026-07-27T00:00:00+08:00",
        )


def test_build_release_envelope_tool_writes_closed_offline_bundle(tmp_path: Path) -> None:
    dependency_name, dependency_version, dependency_digest = installed_distribution_digest("numpy")
    dependency_key = f"{dependency_name}=={dependency_version}"
    base = _manifest()
    capabilities = tmp_path / "capabilities.json"
    capabilities.write_text('{"contract_version":"test"}', encoding="utf-8")
    lock = tmp_path / "dependency-lock.json"
    lock.write_text(json.dumps({
        "contract_version": DEPENDENCY_LOCK_VERSION,
        "platform": platform.system().lower(),
        "python_cache_tag": sys.implementation.cache_tag,
        "distributions": [{
            "name": dependency_name,
            "version": dependency_version,
            "distribution_digest": dependency_digest,
        }],
    }), encoding="utf-8")
    manifest = BuildManifest.build(
        source_commit=base.source_commit,
        source_dirty=base.source_dirty,
        source_tree_digest=base.source_tree_digest,
        build_toolchain=base.build_toolchain,
        input_digests={
            **base.input_digests,
            "src/research_pipeline/capabilities.json": hashlib.sha256(capabilities.read_bytes()).hexdigest(),
            "release/dependency-distributions.json": hashlib.sha256(lock.read_bytes()).hexdigest(),
            "release/gate-test-protocol.json": hashlib.sha256((ROOT / "release/gate-test-protocol.json").read_bytes()).hexdigest(),
        },
        wheel_digest=base.wheel_digest,
        sdist_digest=base.sdist_digest,
        dependency_distribution_digests={dependency_key: dependency_digest},
    )
    manifest_path = tmp_path / "build-manifest.json"
    manifest_path.write_text(canonical_json(manifest.to_dict()), encoding="utf-8")
    gate_arguments: list[str] = []
    for gate_id in REQUIRED_GATE_IDS:
        path = tmp_path / f"{gate_id}.json"
        protocol = json.loads((ROOT / "release/gate-test-protocol.json").read_text(encoding="utf-8"))
        detail = {}
        version = "research-release-gate-test-evidence-v2"
        if gate_id in {"gate-d", "gate-f", "gate-l"}:
            detail = {"pytest": {"tests": 1, "failures": 0, "errors": 0, "skipped": 0},
                      "selectors": protocol["gates"][gate_id]["selectors"],
                      "protocol_hash": hashlib.sha256((ROOT / "release/gate-test-protocol.json").read_bytes()).hexdigest()}
        elif gate_id == "gate-c":
            version = "research-gate-c-evidence-v2"
            detail = {"reference_count": 4, "independent_oracle_count": 4,
                      "dag_family_count": 4, "each_successful_run_has_one_result_bundle": True}
        else:
            version = "research-release-workflow-evidence-v1"
            names = ["installed_origin", "installed_candidate_content", "discovery_and_neutral_draft",
                     "checkpoint_recovery", "result_tamper_rejected", "database_unchanged"]
            names += ["workflow:" + name for name in ("equity_cross_section", "etf_time_series", "event_study", "futures_term_structure")]
            detail = {"checks": {name: {"status": "pass"} for name in names}}
        path.write_text(json.dumps({
            **detail, "contract_version": version,
            "status": "pass",
            "gate_id": gate_id,
            "evidence_scope": "release_candidate",
            "release_candidate_id": "rc-20260726",
            "build_manifest_hash": manifest.manifest_hash,
        }), encoding="utf-8")
        gate_arguments.extend(("--gate-evidence", f"{gate_id}={path}"))
    output = tmp_path / "envelope"
    command = [
        sys.executable,
        str(ROOT / "tools/build_release_envelope.py"),
        "--candidate-id", "rc-20260726",
        "--build-manifest", str(manifest_path),
        "--capabilities", str(capabilities),
        "--dependency-lock", str(lock),
        *gate_arguments,
        "--issued-at", "2026-07-26T12:00:00+08:00",
        "--expires-at", "2026-08-25T12:00:00+08:00",
        "--output", str(output),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["status"] == "pass"
    assert (output / "release-envelope.json").is_file()
    assert len(tuple((output / "receipts").glob("*.json"))) == len(REQUIRED_GATE_IDS)
    source = (ROOT / "tools/build_release_envelope.py").read_text(encoding="utf-8")
    assert "Invoke-WebRequest" not in source
    assert "requests" not in source


    namespace = runpy.run_path(str(ROOT / "tools/build_release_envelope.py"))
    namespace["verify_release_envelope_files"](output, as_of="2026-07-26T12:00:00+08:00")
    evidence = output / "evidence/gate-a.json"
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    payload["additional_fact"] = "changed"
    evidence.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="证据内容不一致"):
        namespace["verify_release_envelope_files"](output, as_of="2026-07-26T12:00:00+08:00")


def test_release_envelope_is_not_back_referenced_by_result_or_evidence() -> None:
    envelope_source = (
        ROOT / "src/research_pipeline/platform/release_envelope.py"
    ).read_text(encoding="utf-8")
    assert "research_pipeline.results" not in envelope_source
    assert "research_pipeline.evidence" not in envelope_source
    for directory in ("results", "evidence"):
        for path in (ROOT / f"src/research_pipeline/{directory}").glob("*.py"):
            assert "ReleaseEnvelope" not in path.read_text(encoding="utf-8"), path


def test_release_envelope_tool_rejects_unbound_or_old_gate_evidence(tmp_path: Path) -> None:
    namespace = runpy.run_path(str(ROOT / "tools/build_release_envelope.py"))
    evidence = tmp_path / "gate.json"
    evidence.write_text(json.dumps({
        "contract_version": "test-gate-v1",
        "status": "pass",
        "evidence_scope": "diagnostic_unbound",
        "release_candidate_id": None,
        "build_manifest_hash": None,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="未绑定当前 clean RC"):
        namespace["_load_pass_evidence"](
            evidence,
            release_candidate_id="rc-current",
            build_manifest_hash="1" * 64,
        )


def test_release_envelope_rejects_gate_swapped_between_receipts(tmp_path: Path) -> None:
    namespace = runpy.run_path(str(ROOT / "tools/build_release_envelope.py"))
    evidence = tmp_path / "gate.json"
    evidence.write_text(json.dumps({
        "contract_version": "test-gate-v1", "status": "pass", "gate_id": "gate-c",
        "evidence_scope": "release_candidate", "release_candidate_id": "rc-current",
        "build_manifest_hash": "1" * 64,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="类型与收据不一致"):
        namespace["_load_pass_evidence"](
            evidence, release_candidate_id="rc-current", build_manifest_hash="1" * 64,
            gate_id="gate-a",
        )


@pytest.mark.parametrize("counts", [
    {"tests": 0, "failures": 0, "errors": 0, "skipped": 0},
    {"tests": 1, "failures": 1, "errors": 0, "skipped": 0},
    {"tests": 1, "failures": 0, "errors": 0, "skipped": 1},
])
def test_release_envelope_rejects_failed_counts_before_sealing(tmp_path: Path, counts) -> None:
    namespace = runpy.run_path(str(ROOT / "tools/build_release_envelope.py"))
    path = tmp_path / "gate-d.json"
    path.write_text(json.dumps({
        "contract_version": "research-release-gate-test-evidence-v2", "status": "pass",
        "gate_id": "gate-d", "evidence_scope": "release_candidate",
        "release_candidate_id": "current", "build_manifest_hash": "a" * 64, "pytest": counts,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="实际执行"):
        namespace["_load_pass_evidence"](path, release_candidate_id="current",
                                        build_manifest_hash="a" * 64, gate_id="gate-d")
