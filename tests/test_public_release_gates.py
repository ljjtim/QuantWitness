from __future__ import annotations

from pathlib import Path

import yaml

from research_pipeline.operations.capabilities import load_capability_manifest


ROOT = Path(__file__).resolve().parents[1]


def test_initial_public_release_does_not_claim_sealed_capabilities() -> None:
    capabilities = load_capability_manifest()["capabilities"]
    assert capabilities
    assert all(item["state"] in {"planned", "local_only"} for item in capabilities)
    assert all(item["trust_level"] != "sealed" for item in capabilities)
    assert all(item["evidence_level"] != "sealed_release_acceptance" for item in capabilities)


def test_public_ci_uses_example_dependency_lock_and_current_build_receipt() -> None:
    projects = (
        "equity_cross_section", "etf_time_series", "event_study", "futures_term_structure",
    )
    locks = {
        yaml.safe_load((ROOT / "examples" / name / "extension/operator.yaml").read_text(encoding="utf-8"))["dependency_lock"]["pyarrow"]
        for name in projects
    }
    assert len(locks) == 1
    version = locks.pop()
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["public-boundary"]
    assert job["strategy"]["matrix"]["python-version"] == ["3.10", "3.13"]
    steps = {item["name"]: item for item in job["steps"]}
    source_install = steps["Install"]["run"]
    build = steps["Build release wheel"]["run"]
    installed = steps["Test isolated wheel and four formal example workflows"]["run"]
    assert f"pyarrow=={version}" in source_install
    assert f"pyarrow=={version}" in installed
    assert "pip check" in source_install and "pip check" in installed
    assert "quantwitness-build.json" in build and "GITHUB_OUTPUT" in build
    assert "steps.release.outputs.wheel" in installed
    assert "quantwitness-release/*.whl" not in installed
