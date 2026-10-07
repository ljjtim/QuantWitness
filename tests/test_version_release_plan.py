"""版本变化、候选提交和发布说明控制自动发布。"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import subprocess
import sys

import pytest


TOOLS = Path(__file__).resolve().parents[1] / "tools"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))
planner = importlib.import_module("plan_version_release")


def _git(project: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=project, check=True,
        capture_output=True, encoding="utf-8",
    ).stdout.strip()


def _commit(project: Path) -> str:
    _git(project, "add", ".")
    _git(
        project, "-c", "user.name=QuantWitness tests", "-c",
        "user.email=tests@example.invalid", "commit", "-qm", "测试候选",
    )
    return _git(project, "rev-parse", "HEAD")


def _candidate(tmp_path: Path, version: str) -> tuple[Path, str, str]:
    project = tmp_path / "project"
    project.mkdir()
    _git(project, "init", "-qb", "main")
    (project / "pyproject.toml").write_text(
        '[project]\nversion = "1.1.0"\n', encoding="utf-8",
    )
    (project / "CHANGELOG.md").write_text(
        "# 变更记录\n\n## 1.1.0\n\n- 现有版本。\n", encoding="utf-8",
    )
    previous = _commit(project)
    _git(project, "tag", "v1.1.0")
    (project / "pyproject.toml").write_text(
        f'[project]\nversion = "{version}"\n', encoding="utf-8",
    )
    (project / "CHANGELOG.md").write_text(
        f"# 变更记录\n\n## {version}（2026-10-01）\n\n- 新版本变化。\n\n"
        "## 1.1.0\n\n- 历史版本变化。\n", encoding="utf-8",
    )
    candidate = _commit(project)
    return project, previous, candidate


def test_ordinary_changes_do_not_publish(tmp_path: Path) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.0")
    result = planner.plan_release(project, previous, candidate)
    assert result["publish"] is False
    assert result["reason"] == "版本号未改变"


@pytest.mark.parametrize("version", ("1.1.1", "1.2.0", "2.0.0"))
def test_increased_version_selects_exact_release_notes(tmp_path: Path, version: str) -> None:
    project, previous, candidate = _candidate(tmp_path, version)
    result = planner.plan_release(project, previous, candidate)
    assert result["publish"] is True
    assert result["release_tag"] == f"v{version}"
    assert result["candidate_commit"] == candidate
    assert result["release_notes"] == "- 新版本变化。\n"


@pytest.mark.parametrize("version", ("1.0.9", "1.1.1rc1", "1.1.1.dev1", "01.1.1"))
def test_downgrades_and_nonstable_versions_do_not_publish(tmp_path: Path, version: str) -> None:
    project, previous, candidate = _candidate(tmp_path, version)
    with pytest.raises(ValueError, match="必须高于|X.Y.Z"):
        planner.plan_release(project, previous, candidate)


@pytest.mark.parametrize("annotated", (False, True))
def test_existing_version_tag_prevents_duplicate_publication(tmp_path: Path, annotated: bool) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.1")
    if annotated:
        _git(
            project, "-c", "user.name=QuantWitness tests", "-c",
            "user.email=tests@example.invalid", "tag", "-a", "v1.1.1", "-m", "1.1.1",
        )
    else:
        _git(project, "tag", "v1.1.1")
    result = planner.plan_release(project, previous, candidate)
    assert result["publish"] is False
    assert result["reason"] == "版本 Tag 已存在"


def test_new_version_must_exceed_historical_release_tags(tmp_path: Path) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.1")
    _git(project, "tag", "v1.2.0", previous)
    with pytest.raises(ValueError, match="已有正式版本 Tag"):
        planner.plan_release(project, previous, candidate)


@pytest.mark.parametrize("content", ("## 1.1.0\n- 旧版本。\n", "## 1.1.1\n\n## 1.1.0\n- 旧版本。\n"))
def test_release_notes_are_required(tmp_path: Path, content: str) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.1")
    (project / "CHANGELOG.md").write_text(content, encoding="utf-8")
    candidate = _commit(project)
    with pytest.raises(ValueError, match="缺少|不能为空"):
        planner.plan_release(project, previous, candidate)


def test_multi_commit_push_compares_against_push_before_commit(tmp_path: Path) -> None:
    project, previous, bumped = _candidate(tmp_path, "1.1.1")
    (project / "README.md").write_text("# 当前说明\n", encoding="utf-8")
    candidate = _commit(project)
    assert planner.plan_release(project, previous, candidate)["publish"] is True
    assert planner.plan_release(project, bumped, candidate)["publish"] is False


def test_checked_out_commit_must_be_the_ci_candidate(tmp_path: Path) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.1")
    with pytest.raises(ValueError, match="源码 HEAD"):
        planner.plan_release(project, previous, previous)


def test_release_request_cannot_select_a_different_ci_commit(tmp_path: Path) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.1")
    request = tmp_path / "request.json"
    request.write_text(json.dumps({
        "previous_commit": previous, "candidate_commit": previous,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="本次通过 CI"):
        planner.main([
            "--project", str(project), "--candidate-commit", candidate,
            "--request", str(request),
        ])


def test_request_rechecks_existing_tag_before_build(tmp_path: Path) -> None:
    project, previous, candidate = _candidate(tmp_path, "1.1.1")
    request = tmp_path / "request.json"
    output = tmp_path / "github-output"
    notes = tmp_path / "release-notes.md"
    planner.main([
        "--project", str(project), "--candidate-commit", candidate,
        "--previous-commit", previous, "--output", str(request),
    ])
    assert json.loads(request.read_text(encoding="utf-8"))["publish"] is True
    _git(project, "tag", "v1.1.1")
    planner.main([
        "--project", str(project), "--candidate-commit", candidate,
        "--request", str(request), "--github-output", str(output),
        "--notes-output", str(notes),
    ])
    assert "publish=false\n" in output.read_text(encoding="utf-8")
    assert not notes.exists()


@pytest.mark.parametrize("version", ("1.1.0", "1.1.1"))
def test_cli_rechecks_request_and_writes_conditional_outputs(tmp_path: Path, version: str) -> None:
    project, previous, candidate = _candidate(tmp_path, version)
    request = tmp_path / "request.json"
    github_output = tmp_path / "github-output"
    notes = tmp_path / "release-notes.md"
    assert planner.main([
        "--project", str(project), "--candidate-commit", candidate,
        "--previous-commit", previous, "--output", str(request),
    ]) == 0
    assert planner.main([
        "--project", str(project), "--candidate-commit", candidate,
        "--request", str(request), "--github-output", str(github_output),
        "--notes-output", str(notes),
    ]) == 0
    publish = version != "1.1.0"
    assert f"publish={str(publish).lower()}\n" in github_output.read_text(encoding="utf-8")
    assert notes.exists() == publish
    if publish:
        assert notes.read_text(encoding="utf-8") == "- 新版本变化。\n"
