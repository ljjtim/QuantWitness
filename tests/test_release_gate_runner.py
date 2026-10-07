"""发布 Gate 拒绝无效测试与跨候选前置证据。"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

TOOLS = Path(__file__).resolve().parents[1] / "tools"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))
import run_release_gate_tests as gates  # noqa: E402


def test_gate_prerequisite_rejects_other_candidate(tmp_path: Path) -> None:
    path = tmp_path / "gate-c.json"
    path.write_text(json.dumps({
        "contract_version": "research-gate-c-evidence-v2", "status": "pass",
        "evidence_scope": "release_candidate", "release_candidate_id": "old",
        "build_manifest_hash": "a" * 64,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="同一候选"):
        gates._pass_evidence_hash(path, {
            "evidence_scope": "release_candidate", "release_candidate_id": "current",
            "build_manifest_hash": "a" * 64,
        })


def test_gate_junit_counts_errors_and_skips(tmp_path: Path) -> None:
    path = tmp_path / "result.xml"
    path.write_text('<testsuites><testsuite tests="2" errors="1" failures="0" skipped="1" /></testsuites>', encoding="utf-8")
    assert gates._junit_counts(path) == {"tests": 2, "failures": 0, "errors": 1, "skipped": 1}


def test_gate_runner_rejects_changed_frozen_protocol_before_execution(tmp_path: Path) -> None:
    project = tmp_path / "project"
    (project / "release").mkdir(parents=True)
    (project / "pyproject.toml").write_text("", encoding="utf-8")
    (project / "release/gate-test-protocol.json").write_text("{}", encoding="utf-8")
    protocol = tmp_path / "protocol.json"
    protocol.write_text(json.dumps({
        "contract_version": gates.PROTOCOL_VERSION,
        "gates": {key: {} for key in gates.REQUIRED_GATES},
    }), encoding="utf-8")
    inputs = tmp_path / "input.json"
    inputs.write_text(json.dumps({"contract_version": gates.INPUT_VERSION, "prerequisite_evidence": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="冻结选择器"):
        gates.run_gate_tests(python=Path(sys.executable), protocol_path=protocol,
                             input_path=inputs, repository=project, output=tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_gate_l_rejects_wrong_prerequisite_type(tmp_path: Path) -> None:
    binding = {"evidence_scope": "release_candidate", "release_candidate_id": "current",
               "build_manifest_hash": "a" * 64}
    path = tmp_path / "gate-d.json"
    path.write_text(json.dumps({**binding, "contract_version": gates.EVIDENCE_VERSION,
                                "status": "pass", "gate_id": "gate-d"}), encoding="utf-8")
    with pytest.raises(ValueError, match="当前 Gate C"):
        gates._pass_evidence_hash(path, binding)


def test_installed_inventory_rejects_same_version_old_content(tmp_path: Path, monkeypatch) -> None:
    from types import SimpleNamespace
    import wheel_source_inventory as inventory

    project = tmp_path / "project"
    package = project / "src/research_pipeline"
    package.mkdir(parents=True)
    (project / "src/factor_contracts").mkdir()
    (project / "pyproject.toml").write_text("", encoding="utf-8")
    (package / "__init__.py").write_text("CURRENT = True", encoding="utf-8")
    monkeypatch.setattr(inventory.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=0, stdout=json.dumps({"files": {"research_pipeline/__init__.py": "0" * 64},
                                       "module": str(tmp_path / "venv/research_pipeline/__init__.py"),
                                       "prefix": str(tmp_path / "venv")}), stderr="",
    ))
    with pytest.raises(ValueError, match="包内容与当前候选不一致"):
        inventory.verify_installed_source_inventory(python=Path(sys.executable), project=project, cwd=tmp_path)
