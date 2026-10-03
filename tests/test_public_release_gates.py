from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
import yaml

from research_pipeline.cli.capability_containment import (
    evaluate_public_capability_release, load_remediation_baseline,
)


ROOT = Path(__file__).resolve().parents[1]


def test_public_release_requires_actual_evidence_for_sealed_capabilities() -> None:
    baseline_path = os.environ.get("QUANTWITNESS_CAPABILITY_BASELINE")
    result = evaluate_public_capability_release(
        project_root=ROOT,
        baseline=load_remediation_baseline(baseline_path) if baseline_path else None,
        release_evidence_root=os.environ.get("QUANTWITNESS_RELEASE_EVIDENCE_ROOT"),
        release_candidate_id=os.environ.get("QUANTWITNESS_RELEASE_CANDIDATE_ID"),
    )
    assert result["status"] == "pass", result["issues"]


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


def test_public_publish_workflow_uses_tag_bound_trusted_publishing() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    )
    assert workflow["on"] == {"workflow_run": {
        "workflows": ["ci"], "types": ["completed"], "branches": ["main"],
    }}
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["concurrency"]["cancel-in-progress"] is False
    assert workflow["concurrency"]["group"] == "publish-main"

    plan = workflow["jobs"]["plan"]
    assert "workflow_run.conclusion == 'success'" in plan["if"]
    assert "workflow_run.event == 'push'" in plan["if"]
    assert "head_repository.full_name == github.repository" in plan["if"]
    assert plan["permissions"] == {"contents": "read", "actions": "read"}
    plan_steps = {item["name"]: item for item in plan["steps"]}
    assert plan_steps["Checkout tested commit"]["with"]["ref"] == "${{ github.event.workflow_run.head_sha }}"
    request = plan_steps["Download CI version request"]["with"]
    assert request["run-id"] == "${{ github.event.workflow_run.id }}"
    assert request["name"] == "version-release-request-${{ github.event.workflow_run.run_attempt }}"
    assert "--request" in plan_steps["Check version change"]["run"]
    assert plan_steps["Upload release notes"]["if"] == "${{ steps.plan.outputs.publish == 'true' }}"

    build = workflow["jobs"]["build"]
    assert build["needs"] == "plan"
    assert build["if"] == "${{ needs.plan.outputs.publish == 'true' }}"
    build_steps = {item["name"]: item for item in build["steps"]}
    tag_check = build_steps["Verify release tag"]["run"]
    release_build = build_steps["Build release artifacts"]["run"]
    isolated = build_steps["Verify isolated wheel"]["run"]
    assert "RELEASE_TAG" in tag_check and "pyproject.toml" in tag_check
    assert 'git rev-parse HEAD' in tag_check
    assert '"$CANDIDATE_SHA"' in tag_check
    assert "git cat-file" not in tag_check
    assert "merge-base --is-ancestor HEAD origin/main" in tag_check
    assert build_steps["Checkout"]["with"]["ref"] == "${{ needs.plan.outputs.candidate_commit }}"
    assert "tools/build_release_artifacts.py" in release_build
    assert "GITHUB_OUTPUT" in release_build
    for name in ("wheel", "sdist", "source_archive"):
        assert build["outputs"][f"{name}_name"] == f"${{{{ steps.release.outputs.{name}_name }}}}"
    assert 'print(f"{name}_name={path.name}", file=output)' in release_build
    assert "steps.release.outputs.wheel" in isolated

    publish = workflow["jobs"]["publish-pypi"]
    assert publish["needs"] == "build"
    assert publish["environment"]["name"] == "pypi"
    assert publish["permissions"] == {"contents": "read", "id-token": "write"}
    publish_steps = {item["name"]: item for item in publish["steps"]}
    publisher = publish_steps["Publish Python distributions"]
    assert publisher["with"] == {"packages-dir": "dist"}
    assert "password" not in publisher["with"]

    finalize = workflow["jobs"]["finalize-release"]
    assert finalize["needs"] == ["plan", "build", "publish-pypi"]
    assert finalize["permissions"] == {"contents": "write"}
    finalize_steps = {item["name"]: item for item in finalize["steps"]}
    assert finalize_steps["Set up minimum supported Python"]["with"]["python-version"] == "3.10"
    install = finalize_steps["Verify clean PyPI installation"]["run"]
    assert '--index-url https://pypi.org/simple "quantwitness==${RELEASE_TAG#v}"' in install
    assert "pip check" in install and "quantwitness\" --help" in install
    assert "is_relative_to(Path(sys.prefix).resolve())" in install
    assert finalize_steps["Download Python distributions"]["with"] == {
        "name": "python-distributions", "path": "dist",
    }
    assert finalize_steps["Download source archive"]["with"] == {
        "name": "source-archive", "path": "dist",
    }
    upload = finalize_steps["Create verified GitHub Release"]
    assert 'gh release create "$RELEASE_TAG"' in upload["run"]
    assert '--target "$CANDIDATE_SHA"' in upload["run"]
    assert "--notes-file release-metadata/quantwitness-release-notes.md" in upload["run"]
    assert "--clobber" not in upload["run"]
    for name in ("wheel", "sdist", "source_archive"):
        assert upload["env"][f"{name.upper()}_PATH"] == f"dist/${{{{ needs.build.outputs.{name}_name }}}}"

    for job in workflow["jobs"].values():
        for step in job["steps"]:
            action = step.get("uses")
            if action is not None:
                assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action), action


@pytest.mark.parametrize("tag", ("v1.1.0", "v1.0.0", "release-1.1.0"))
def test_publish_tag_version_check_rejects_mismatches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tag: str,
) -> None:
    import sys

    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    monkeypatch.setitem(sys.modules, "tomllib", tomllib)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RELEASE_TAG", tag)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nversion = "1.1.0"\n', encoding="utf-8",
    )
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    )
    steps = {item["name"]: item for item in workflow["jobs"]["build"]["steps"]}
    script = re.search(
        r"python - <<'PY'\n(?P<code>.*?)\nPY\n",
        steps["Verify release tag"]["run"], flags=re.DOTALL,
    )
    assert script is not None
    code = compile(script.group("code"), "publish-tag-check", "exec")
    if tag == "v1.1.0":
        exec(code, {})
    else:
        with pytest.raises(SystemExit, match="不一致"):
            exec(code, {})


def test_ci_records_original_push_versions_only_after_main_checks() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    )
    request = workflow["jobs"]["release-request"]
    assert request["needs"] == ["public-boundary", "rdagent-distribution", "ml-distribution"]
    assert request["if"] == "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}"
    steps = {item["name"]: item for item in request["steps"]}
    assert steps["Checkout tested commit"]["with"]["fetch-depth"] == 0
    record = steps["Record version release request"]
    assert record["env"] == {
        "PREVIOUS_SHA": "${{ github.event.before }}", "CANDIDATE_SHA": "${{ github.sha }}",
    }
    assert '--previous-commit "$PREVIOUS_SHA"' in record["run"]
    assert steps["Upload version release request"]["with"]["name"] == "version-release-request-${{ github.run_attempt }}"
