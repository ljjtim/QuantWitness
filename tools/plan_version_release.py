"""只为 main 推送中递增的正式版本生成发布请求。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def _git(project: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=project, check=True,
        capture_output=True, encoding="utf-8",
    ).stdout.strip()


def _version(content: str) -> str:
    version = tomllib.loads(content)["project"]["version"]
    if not isinstance(version, str):
        raise ValueError("project.version 必须是字符串")
    return version


def _stable_version(version: str) -> tuple[int, ...]:
    if re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version) is None:
        raise ValueError("自动发布只接受 X.Y.Z 格式的正式版本号")
    return tuple(int(part) for part in version.split("."))


def _release_notes(project: Path, version: str) -> str:
    content = (project / "CHANGELOG.md").read_text(encoding="utf-8")
    heading = re.search(
        rf"^##[ \t]+{re.escape(version)}(?=[ \t（(]|$)[^\n]*$",
        content, flags=re.MULTILINE,
    )
    if heading is None:
        raise ValueError(f"CHANGELOG.md 缺少 {version} 章节")
    following = content[heading.end():]
    next_heading = re.search(r"^##[ \t]+", following, flags=re.MULTILINE)
    notes = following[:next_heading.start()] if next_heading else following
    if not notes.strip():
        raise ValueError(f"CHANGELOG.md 的 {version} 章节不能为空")
    return notes.strip() + "\n"


def plan_release(project: Path, previous_commit: str, candidate_commit: str) -> dict:
    if _git(project, "rev-parse", "HEAD") != candidate_commit:
        raise ValueError("源码 HEAD 必须与通过 CI 的候选提交一致")
    _git(project, "merge-base", "--is-ancestor", previous_commit, candidate_commit)
    previous_version = _version(_git(project, "show", f"{previous_commit}:pyproject.toml"))
    version = _version((project / "pyproject.toml").read_text(encoding="utf-8"))
    result = {
        "publish": False,
        "previous_commit": previous_commit,
        "candidate_commit": candidate_commit,
        "previous_version": previous_version,
        "version": version,
        "release_tag": f"v{version}",
        "release_notes": "",
    }
    if version == previous_version:
        return {**result, "reason": "版本号未改变"}
    current = _stable_version(version)
    if current <= _stable_version(previous_version):
        raise ValueError("新版本号必须高于推送前的版本号")
    tags = _git(project, "tag", "--list", "v*").splitlines()
    if result["release_tag"] in tags:
        return {**result, "reason": "版本 Tag 已存在"}
    released = [
        _stable_version(tag[1:]) for tag in tags
        if re.fullmatch(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", tag)
    ]
    if released and current <= max(released):
        raise ValueError("新版本号必须高于已有正式版本 Tag")
    return {
        **result, "publish": True, "reason": "正式版本号递增",
        "release_notes": _release_notes(project, version),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=".")
    parser.add_argument("--candidate-commit", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--previous-commit")
    source.add_argument("--request")
    parser.add_argument("--output")
    parser.add_argument("--github-output")
    parser.add_argument("--notes-output")
    args = parser.parse_args(argv)
    previous_commit = args.previous_commit
    if args.request:
        request = json.loads(Path(args.request).read_text(encoding="utf-8"))
        if request["candidate_commit"] != args.candidate_commit:
            raise ValueError("发布请求必须属于本次通过 CI 的提交")
        previous_commit = request["previous_commit"]
    result = plan_release(Path(args.project).resolve(), previous_commit, args.candidate_commit)
    content = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        Path(args.output).write_text(content, encoding="utf-8")
    if args.github_output:
        with Path(args.github_output).open("a", encoding="utf-8") as output:
            output.write(f"publish={str(result['publish']).lower()}\n")
            for name in ("release_tag", "version", "candidate_commit", "previous_commit"):
                output.write(f"{name}={result[name]}\n")
    if args.notes_output and result["publish"]:
        Path(args.notes_output).write_text(result["release_notes"], encoding="utf-8")
    print(content, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
