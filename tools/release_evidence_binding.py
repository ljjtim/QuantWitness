"""最终 Gate 证据与 clean RC 的最小绑定；诊断预跑保持显式未绑定。"""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
import subprocess

from release_allowlist import (
    allowlisted_source_dirty,
    build_input_paths,
    file_digests,
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_input_digests(project: Path, dependency_lock: Path) -> dict[str, str]:
    result = file_digests(project, build_input_paths(project, dependency_lock))
    if (
        "pyproject.toml" not in result
        or "README.md" not in result
        or not any(key.startswith("src/") for key in result)
        or not any(key.startswith("tests/") for key in result)
    ):
        raise ValueError("构建输入不闭合")
    return result


def source_identity(project: Path) -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "-C", str(project), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dependency_lock = project / "release" / "dependency-distributions.json"
    paths = build_input_paths(project, dependency_lock)
    return commit, allowlisted_source_dirty(project, paths)


def release_evidence_binding(
    *,
    release_candidate_id: str | None,
    build_manifest_path: Path | None,
    project: Path | None = None,
) -> dict[str, object]:
    if release_candidate_id is None and build_manifest_path is None:
        return {
            "evidence_scope": "diagnostic_unbound",
            "release_candidate_id": None,
            "build_manifest_hash": None,
        }
    if not release_candidate_id or build_manifest_path is None:
        raise ValueError("最终 Gate 必须同时提供 candidate ID 和 BuildManifest")
    try:
        payload = json.loads(build_manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("最终 Gate BuildManifest 无法读取") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("contract_version") != "research-build-manifest-v1"
        or payload.get("source_dirty") is not False
        or not isinstance(payload.get("manifest_hash"), str)
    ):
        raise ValueError("最终 Gate 只接受 source_dirty=false 的 BuildManifest")
    if project is not None:
        dependency_lock = project / "release" / "dependency-distributions.json"
        current_commit, current_dirty = source_identity(project)
        if current_dirty or current_commit != payload.get("source_commit"):
            raise ValueError("最终 Gate 源码不是 BuildManifest 对应的 clean commit")
        current_inputs = build_input_digests(project, dependency_lock)
        if current_inputs != payload.get("input_digests"):
            raise ValueError("最终 Gate 构建输入已偏离 BuildManifest")
    return {
        "evidence_scope": "release_candidate",
        "release_candidate_id": release_candidate_id,
        "build_manifest_hash": payload["manifest_hash"],
    }


__all__ = [
    "build_input_digests",
    "file_sha256",
    "release_evidence_binding",
    "source_identity",
]
