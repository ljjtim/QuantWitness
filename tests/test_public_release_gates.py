from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import pytest
import yaml

from tools.capability_policy import (
    evaluate_public_capability_release, load_remediation_baseline,
)


ROOT = Path(__file__).resolve().parents[1]


def _step(
    job: dict, *, command: str = "", action: str = "",
    step_id: str = "", artifact: str = "",
) -> dict:
    matches = [
        step for step in job["steps"]
        if (not command or command in step.get("run", ""))
        and (not action or step.get("uses", "").split("@")[0] == action)
        and (not step_id or step.get("id") == step_id)
        and (not artifact or step.get("with", {}).get("name") == artifact)
    ]
    assert len(matches) == 1, (command, action, step_id, artifact, matches)
    return matches[0]


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
    source_install = _step(job, command='pip install ".[test]"')["run"]
    build = _step(job, step_id="release")["run"]
    installed = _step(job, command="-m venv")["run"]
    assert "tests/test_turnover_factor_example.py" in _step(job, command="examples/equity_cross_section/test_operator.py")["run"]
    assert "tests/test_turnover_factor_example.py" in installed
    core_tests = _step(job, command="tests/test_current_documentation.py")["run"]
    for path in ("tests/test_generic_run_admission.py", "tests/test_minute_project_scope.py",
                 "tests/test_minute_runtime_adapters.py"):
        assert path in core_tests
    rd_steps = workflow["jobs"]["rdagent-distribution"]["steps"]
    assert any("tests/test_factor_space.py" in item.get("run", "") for item in rd_steps)
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
    assert _step(plan, action="actions/checkout")["with"]["ref"] == "${{ github.event.workflow_run.head_sha }}"
    request = _step(plan, action="actions/download-artifact")["with"]
    assert request["run-id"] == "${{ github.event.workflow_run.id }}"
    assert request["name"] == "version-release-request-${{ github.event.workflow_run.run_attempt }}"
    assert "--request" in _step(plan, step_id="plan")["run"]
    assert _step(plan, action="actions/upload-artifact", artifact="release-notes")["if"] == "${{ steps.plan.outputs.publish == 'true' }}"

    build = workflow["jobs"]["build"]
    assert build["needs"] == "plan"
    assert build["if"] == "${{ needs.plan.outputs.publish == 'true' }}"
    tag_check = _step(build, command="git rev-parse HEAD")["run"]
    release_build = _step(build, step_id="release")["run"]
    isolated = _step(build, command="-m venv")["run"]
    assert "RELEASE_TAG" in tag_check and "pyproject.toml" in tag_check
    assert 'git rev-parse HEAD' in tag_check
    assert '"$CANDIDATE_SHA"' in tag_check
    assert "git cat-file" not in tag_check
    assert "merge-base --is-ancestor HEAD origin/main" in tag_check
    assert _step(build, action="actions/checkout")["with"]["ref"] == "${{ needs.plan.outputs.candidate_commit }}"
    assert "tools/build_release_artifacts.py" in release_build
    assert "GITHUB_OUTPUT" in release_build
    for name in ("wheel", "sdist", "source_archive"):
        assert build["outputs"][f"{name}_name"] == f"${{{{ steps.release.outputs.{name}_name }}}}"
    assert "steps.release.outputs.wheel" in isolated

    publish = workflow["jobs"]["publish-pypi"]
    assert publish["needs"] == "build"
    assert publish["environment"]["name"] == "pypi"
    assert publish["permissions"] == {"contents": "read", "id-token": "write"}
    publisher = _step(publish, action="pypa/gh-action-pypi-publish")
    assert publisher["with"] == {"packages-dir": "dist"}
    assert "password" not in publisher["with"]

    finalize = workflow["jobs"]["finalize-release"]
    assert finalize["needs"] == ["plan", "build", "publish-pypi"]
    assert finalize["permissions"] == {"contents": "write"}
    assert _step(finalize, action="actions/setup-python")["with"]["python-version"] == "3.10"
    install = _step(finalize, command="-m venv")["run"]
    assert '--index-url https://pypi.org/simple "quantwitness==${RELEASE_TAG#v}"' in install
    assert "pip check" in install and "quantwitness\" --help" in install
    assert "is_relative_to(Path(sys.prefix).resolve())" in install
    assert _step(finalize, action="actions/download-artifact", artifact="python-distributions")["with"] == {
        "name": "python-distributions", "path": "dist",
    }
    assert _step(finalize, action="actions/download-artifact", artifact="source-archive")["with"] == {
        "name": "source-archive", "path": "dist",
    }
    upload = _step(finalize, command="gh release create")
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
    script = re.search(
        r"python - <<'PY'\n(?P<code>.*?)\nPY\n",
        _step(workflow["jobs"]["build"], command="git rev-parse HEAD")["run"], flags=re.DOTALL,
    )
    assert script is not None
    code = compile(script.group("code"), "publish-tag-check", "exec")
    if tag == "v1.1.0":
        exec(code, {})
    else:
        with pytest.raises(SystemExit):
            exec(code, {})


def test_ci_records_original_push_versions_only_after_main_checks() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    )
    request = workflow["jobs"]["release-request"]
    assert request["needs"] == ["public-boundary", "rdagent-distribution", "ml-distribution"]
    assert request["if"] == "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}"
    assert _step(request, action="actions/checkout")["with"]["fetch-depth"] == 0
    record = _step(request, command="tools/plan_version_release.py")
    assert record["env"] == {
        "PREVIOUS_SHA": "${{ github.event.before }}", "CANDIDATE_SHA": "${{ github.sha }}",
    }
    assert '--previous-commit "$PREVIOUS_SHA"' in record["run"]
    assert _step(request, action="actions/upload-artifact")["with"]["name"] == "version-release-request-${{ github.run_attempt }}"


@pytest.mark.parametrize("failure", (None, "failed_build", "missing_artifact", "extra_artifact"))
def test_publish_build_receipt_controls_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str | None,
) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8"))
    script = _step(workflow["jobs"]["build"], step_id="release")["run"]
    embedded = re.search(r"<<'PY'\n(?P<code>.*?)\nPY(?:\n|$)", script, re.DOTALL)
    assert embedded is not None
    artifacts = tmp_path / "dist"
    artifacts.mkdir()
    receipt = {"status": "pass"}
    for name, filename in (("wheel", "package.whl"), ("sdist", "package.tar.gz"), ("source_archive", "source.zip")):
        path = artifacts / filename
        path.touch()
        receipt[name] = str(path)
    if failure == "failed_build":
        receipt["status"] = "fail"
    elif failure == "missing_artifact":
        Path(receipt["wheel"]).unlink()
    elif failure == "extra_artifact":
        (artifacts / "stale.whl").touch()
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    output = tmp_path / "github-output"
    monkeypatch.setattr(sys, "argv", ["build-receipt", str(receipt_path), str(output)])
    code = compile(embedded.group("code"), "build-receipt", "exec")
    if failure:
        expected_error = FileNotFoundError if failure == "missing_artifact" else SystemExit
        with pytest.raises(expected_error):
            exec(code, {})
        assert not output.exists()
    else:
        exec(code, {})
        values = dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())
        assert values == {
            key: value
            for name in ("wheel", "sdist", "source_archive")
            for key, value in ((name, receipt[name]), (f"{name}_name", Path(receipt[name]).name))
        }
