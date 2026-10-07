"""公开晋级门禁的合成合同样本，不构成能力晋级证明。"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from research_pipeline.cli.capability_containment import evaluate_public_capability_release
from research_pipeline.operations.capabilities import load_capability_manifest
from research_pipeline.platform import (
    BuildManifest, REQUIRED_GATE_IDS, ReleaseAcceptanceInput, ReleaseEnvelope,
    ReleaseGateReceipt, typed_canonical_hash,
)


CATEGORIES = ("formal_execution_test", "negative_test", "independent_verification")
AS_OF = "2026-09-30T12:00:00+00:00"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _reference(root: Path, path: Path) -> dict[str, str]:
    return {"anchor": path.relative_to(root).as_posix(), "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _seal_baseline(baseline: dict[str, object]) -> None:
    baseline["baseline_hash"] = typed_canonical_hash({key: value for key, value in baseline.items() if key != "baseline_hash"})


@pytest.fixture
def promotion(tmp_path):
    project = tmp_path / "public-source"
    evidence = tmp_path / "independent-evidence"
    manifest = deepcopy(load_capability_manifest())
    descriptor = manifest["capabilities"][0]
    descriptor.update(state="sealed", trust_level="sealed", evidence_level="sealed_release_acceptance")
    source = "research_pipeline/tests/test_promotion_sample.py"
    names = ("test_formal", "test_negative", "test_independent")
    descriptor["verification_anchors"] = [source + "::" + name for name in names]
    capabilities = project / "src/research_pipeline/capabilities.json"
    _write(capabilities, manifest)
    test_file = project / "tests/test_promotion_sample.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("\n".join(f"def {name}():\n    assert 1 + 1 == 2\n" for name in names), encoding="utf-8")
    _write(evidence / "capabilities.json", manifest)
    _write(evidence / "dependency-lock.json", {"sample": "synthetic"})
    dependency_digest = hashlib.sha256((evidence / "dependency-lock.json").read_bytes()).hexdigest()
    inputs = {
        "release/dependency-distributions.json": dependency_digest,
        "src/research_pipeline/capabilities.json": hashlib.sha256(capabilities.read_bytes()).hexdigest(),
        "tests/test_promotion_sample.py": hashlib.sha256(test_file.read_bytes()).hexdigest(),
    }
    build = BuildManifest.build(
        source_commit="a" * 40, source_dirty=False,
        source_tree_digest=typed_canonical_hash(inputs), build_toolchain={"python": "3.10"},
        input_digests=inputs, wheel_digest="b" * 64, sdist_digest="c" * 64,
        dependency_distribution_digests={"pytest": "d" * 64},
    )
    _write(evidence / "build-manifest.json", build.to_dict())
    gate_receipts = []
    for gate_id in REQUIRED_GATE_IDS:
        gate = {
            "gate_id": gate_id, "status": "pass", "evidence_scope": "release_candidate",
            "release_candidate_id": "sample-rc", "build_manifest_hash": build.manifest_hash,
            "pytest": {"tests": 3, "failures": 0, "errors": 0, "skipped": 0},
            "test_file_hashes": {"tests/test_promotion_sample.py": inputs["tests/test_promotion_sample.py"]},
        }
        gate_file = evidence / "evidence" / f"{gate_id}.json"
        _write(gate_file, gate)
        receipt = ReleaseGateReceipt.build(
            gate_id=gate_id, release_candidate_id="sample-rc", build_manifest_hash=build.manifest_hash,
            evidence_hashes={"acceptance": _reference(evidence, gate_file)["artifact_sha256"]},
            issued_at="2026-09-30T00:00:00+00:00", expires_at="2026-10-01T00:00:00+00:00",
        )
        gate_receipts.append(receipt)
        _write(evidence / "receipts" / f"{gate_id}.json", receipt.to_dict())
    acceptance = ReleaseAcceptanceInput.build(
        release_candidate_id="sample-rc", build_manifest_hash=build.manifest_hash, receipts=gate_receipts,
    )
    envelope = ReleaseEnvelope.build(
        release_candidate_id="sample-rc", profile="local", manifest=build,
        capabilities_digest=inputs["src/research_pipeline/capabilities.json"],
        dependency_lock_digest=dependency_digest, platform="windows", python_cache_tag="cpython-310",
        receipts=gate_receipts, acceptance=acceptance,
        issued_at="2026-09-30T00:00:00+00:00", expires_at="2026-10-01T00:00:00+00:00",
        revocation_policy="not_applicable_no_distribution",
    )
    _write(evidence / "release-envelope.json", envelope.to_dict())
    _write(evidence / "acceptance-input.json", acceptance.to_dict())
    classname = "tests.test_promotion_sample"
    junit = evidence / "gate-d.junit.xml"
    junit.write_text('<testsuite tests="3">' + ''.join(f'<testcase classname="{classname}" name="{name}"/>' for name in names) + '</testsuite>', encoding="utf-8")
    references = {}
    for category, name in zip(CATEGORIES, names):
        proof = {
            "contract_version": "research-capability-promotion-evidence-v2",
            "capability_id": descriptor["id"], "category": category, "status": "pass",
            "subject_manifest_hash": typed_canonical_hash(manifest),
            "release_candidate_id": "sample-rc", "build_manifest_hash": build.manifest_hash,
            "producer_id": category, "trust_domain": category,
            "gate_id": "gate-d", "gate_evidence": _reference(evidence, evidence / "evidence/gate-d.json"),
            "junit": _reference(evidence, junit),
            "test_cases": [{"source": source, "classname": classname, "name": name}],
        }
        proof_file = evidence / f"{category}.json"
        _write(proof_file, proof)
        references[category] = [_reference(evidence, proof_file)]
    baseline = {
        "contract_version": "research-remediation-baseline-v1",
        "audit_sources": [{"path": "sample", "role": "test"}],
        "audit_findings": [{"finding_id": "sample", "summary": "合成授权"}],
        "containment": [{
            "capability_id": descriptor["id"], "maximum_state": "sealed",
            "finding_ids": ["sample"], "remediation_tasks": ["sample"],
            "promotion_evidence": references,
        }],
        "snapshot": {"capability_manifest": {"payload_hash": typed_canonical_hash(manifest)}},
    }
    _seal_baseline(baseline)
    return dict(project_root=project, release_evidence_root=evidence, manifest=manifest,
                baseline=baseline, release_candidate_id="sample-rc", as_of=AS_OF)


def _change_proof(promotion, category, change):
    root = promotion["release_evidence_root"]
    path = root / f"{category}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    change(payload)
    _write(path, payload)
    promotion["baseline"]["containment"][0]["promotion_evidence"][category] = [_reference(root, path)]
    _seal_baseline(promotion["baseline"])


def test_local_only_public_source_needs_no_private_baseline(tmp_path):
    observed = evaluate_public_capability_release(project_root=tmp_path)
    assert observed["status"] == "pass", observed


def test_sealed_claim_without_independent_authorization_is_rejected(promotion):
    promotion["baseline"] = None
    observed = evaluate_public_capability_release(**promotion)
    assert observed["status"] == "fail"
    assert "promotion_authorization_missing" in {issue["code"] for issue in observed["issues"]}


def test_actual_promotion_evidence_is_consumed_without_private_repository(promotion):
    observed = evaluate_public_capability_release(**promotion)
    assert observed["status"] == "pass", observed


@pytest.mark.parametrize("mutation", [
    "wrong_candidate", "missing_category", "unauthorized", "tampered_junit", "failed_case",
    "skipped_case", "missing_case", "duplicate_case", "same_domain", "same_producer",
    "source_changed", "pass_only", "gate_changed", "wrong_build", "unregistered_case", "expired",
    "sealed_capabilities_changed", "sealed_lock_changed", "unselected_gate_changed",
])
def test_promotion_evidence_rejects_invalid_claims(promotion, mutation):
    root = promotion["release_evidence_root"]
    if mutation == "wrong_candidate":
        promotion["release_candidate_id"] = "different-rc"
    elif mutation == "missing_category":
        promotion["baseline"]["containment"][0]["promotion_evidence"]["negative_test"] = []
        _seal_baseline(promotion["baseline"])
    elif mutation == "unauthorized":
        promotion["baseline"]["containment"][0]["maximum_state"] = "local_only"
        _seal_baseline(promotion["baseline"])
    elif mutation in {"tampered_junit", "failed_case", "skipped_case", "duplicate_case"}:
        path = root / "gate-d.junit.xml"
        text = path.read_text(encoding="utf-8")
        if mutation == "tampered_junit":
            text += " "
        elif mutation == "duplicate_case":
            text = text.replace('name="test_negative"', 'name="test_formal"')
        else:
            tag = "failure" if mutation == "failed_case" else "skipped"
            text = text.replace('/>', f'><{tag}/></testcase>', 1)
        path.write_text(text, encoding="utf-8")
        if mutation != "tampered_junit":
            for category in CATEGORIES:
                _change_proof(promotion, category, lambda proof: proof.update(junit=_reference(root, path)))
    elif mutation in {"same_domain", "same_producer"}:
        field = "trust_domain" if mutation == "same_domain" else "producer_id"
        _change_proof(promotion, "independent_verification", lambda proof: proof.update({field: "formal_execution_test"}))
    elif mutation == "source_changed":
        (promotion["project_root"] / "tests/test_promotion_sample.py").write_text("assert False", encoding="utf-8")
    elif mutation == "gate_changed":
        _write(root / "evidence/gate-d.json", {"status": "pass"})
    elif mutation in {"sealed_capabilities_changed", "sealed_lock_changed", "unselected_gate_changed"}:
        relative = {
            "sealed_capabilities_changed": "capabilities.json",
            "sealed_lock_changed": "dependency-lock.json",
            "unselected_gate_changed": "evidence/gate-c.json",
        }[mutation]
        _write(root / relative, {"status": "pass"})
    elif mutation == "expired":
        promotion["as_of"] = "2026-10-02T00:00:00+00:00"
    else:
        def change(proof):
            if mutation == "pass_only":
                proof.clear()
                proof["status"] = "pass"
            elif mutation == "wrong_build":
                proof["build_manifest_hash"] = "f" * 64
            elif mutation == "missing_case":
                proof["test_cases"][0]["name"] = "test_missing"
            else:
                proof["test_cases"][0]["source"] = "research_pipeline/tests/unregistered.py"
        _change_proof(promotion, "formal_execution_test", change)
    observed = evaluate_public_capability_release(**promotion)
    assert observed["status"] == "fail", (mutation, observed)


def test_partial_sealed_claim_is_rejected():
    manifest = deepcopy(load_capability_manifest())
    manifest["capabilities"][0]["trust_level"] = "sealed"
    result = evaluate_public_capability_release(project_root=Path.cwd(), manifest=manifest)
    assert result["issues"][0]["code"] == "capability_claim_invalid"


def test_authorized_sealed_without_active_release_evidence_is_rejected(promotion):
    promotion["release_evidence_root"] = None
    observed = evaluate_public_capability_release(**promotion)
    assert observed["status"] == "fail"
    assert {issue["code"] for issue in observed["issues"]} == {"promotion_release_evidence_missing"}


def test_project_relative_test_sources_match_repository_capability_anchors(promotion):
    for category in CATEGORIES:
        _change_proof(promotion, category, lambda proof: proof["test_cases"][0].update(source="tests/test_promotion_sample.py"))
    result = evaluate_public_capability_release(**promotion)
    assert result["status"] == "pass", result
