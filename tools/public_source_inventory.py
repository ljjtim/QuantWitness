"""检查或列出 QuantWitness 独立公开仓库允许出现的源码文件。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import re
import subprocess

from release_allowlist import copy_paths, public_source_paths


_FORBIDDEN_PATH_PARTS = {
    ".trellis",
    "replacement_acceptance",
    "research_packages",
    "retirement_migration",
}
_FORBIDDEN_SUFFIXES = (
    ".db",
    ".duckdb",
    ".duckdb.wal",
    ".log",
    ".parquet",
    ".pickle",
    ".pkl",
    ".pyc",
    ".sqlite",
    ".sqlite3",
)
_FORBIDDEN_TEXT = (
    re.compile(
        r"(?i)(?:"
        r"[A-Z]:[\\/](?:Users|data)[\\/][A-Za-z0-9._ -]+(?:[\\/][^\s\"'<>]+)?"
        r"|(?<![\w:])/(?:home|Users)/[A-Za-z0-9._-]+/[^\s\"'<>]+"
        r")"
    ),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(re.escape(".trellis" + "/tasks/")),
)
_INTERNAL_REFERENCE = re.compile(
    r"(?:research_pipeline/)?(?:research_packages|replacement_acceptance|retirement_migration)/"
)
_TEXT_SUFFIXES = {
    ".cfg",
    ".json",
    ".md",
    ".ps1",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}


def _validate_path(relative: str) -> None:
    path = PurePosixPath(relative)
    lowered = path.as_posix().lower()
    if (
        path.is_absolute()
        or ".." in path.parts
        or any(part.lower() in _FORBIDDEN_PATH_PARTS for part in path.parts)
        or lowered.endswith(_FORBIDDEN_SUFFIXES)
    ):
        raise ValueError(f"公开源码清单包含禁止路径: {relative}")


def _tracked_checkout_paths(root: Path) -> set[str]:
    repository = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=root, capture_output=True, text=True, check=True,
    )
    if Path(repository.stdout.strip()).resolve() != root:
        raise ValueError("公开源码清单检查必须在独立仓库根目录运行")
    tracked = subprocess.run(
        ["git", "ls-files", "--cached", "--full-name", "-z"],
        cwd=root, capture_output=True, check=True,
    )
    return {path.decode("utf-8") for path in tracked.stdout.split(bytes([0])) if path}


def inspect_public_source(project: Path, *, check_tracked: bool = False) -> dict[str, object]:
    root = project.resolve(strict=True)
    paths = public_source_paths(root)
    issues: list[dict[str, str]] = []
    if check_tracked:
        try:
            tracked = _tracked_checkout_paths(root)
        except (OSError, subprocess.CalledProcessError) as exc:
            raise ValueError("无法读取独立仓库的受跟踪文件") from exc
        if ".github/CODEOWNERS" not in tracked:
            issues.append({"kind": "missing_governance_file", "path": ".github/CODEOWNERS"})
        for relative in sorted(tracked - set(paths)):
            issues.append({"kind": "unexpected_tracked_file", "path": relative})
        for relative in sorted(set(paths) - tracked):
            issues.append({"kind": "untracked_public_file", "path": relative})
    for relative in paths:
        try:
            _validate_path(relative)
        except ValueError as exc:
            issues.append({"kind": "forbidden_path", "path": relative, "detail": str(exc)})
            continue
        source = root / relative
        if source.suffix.lower() not in _TEXT_SUFFIXES and relative != ".github/CODEOWNERS":
            continue
        try:
            text = source.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            issues.append({"kind": "non_utf8_text", "path": relative})
            continue
        for pattern in _FORBIDDEN_TEXT:
            if pattern.search(text):
                issues.append(
                    {
                        "kind": "forbidden_text",
                        "path": relative,
                        "pattern": pattern.pattern,
                    }
                )
        if source.suffix.lower() in {".json", ".md", ".yaml", ".yml"}:
            if _INTERNAL_REFERENCE.search(text):
                issues.append({
                    "kind": "internal_reference",
                    "path": relative,
                })
    return {
        "schema_version": "quantwitness-public-source-inventory-v1",
        "status": "pass" if not issues else "fail",
        "file_count": len(paths),
        "paths": list(paths),
        "issues": issues,
    }


def export_public_source(project: Path, output: Path) -> dict[str, object]:
    """把唯一清单原样复制到一个必须不存在的公开候选目录。"""

    root = project.resolve(strict=True)
    destination = output.resolve()
    if destination.exists():
        raise ValueError("公开源码输出目录已存在")
    inspection = inspect_public_source(root)
    if inspection["status"] != "pass":
        raise ValueError("公开源码检查未通过，拒绝生成候选目录")
    paths = tuple(str(item) for item in inspection["paths"])
    copy_paths(root, destination, paths)
    actual = tuple(
        sorted(
            path.relative_to(destination).as_posix()
            for path in destination.rglob("*")
            if path.is_file()
        )
    )
    if actual != paths:
        raise ValueError("公开源码候选目录与唯一清单不一致")
    return {
        "schema_version": "quantwitness-public-source-export-v1",
        "status": "pass",
        "file_count": len(paths),
        "output": str(destination),
        "paths": list(paths),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="检查 QuantWitness 公开源码清单")
    parser.add_argument("--project", default=".")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--output", help="复制到一个必须不存在的公开候选目录")
    args = parser.parse_args(argv)
    try:
        payload = (
            export_public_source(Path(args.project), Path(args.output))
            if args.output
            else inspect_public_source(Path(args.project), check_tracked=args.check)
        )
    except ValueError as exc:
        payload = {
            "schema_version": "quantwitness-public-source-export-v1",
            "status": "fail",
            "issues": [{"kind": "export_rejected", "detail": str(exc)}],
        }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    if args.output or args.check:
        return 0 if payload["status"] == "pass" else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
