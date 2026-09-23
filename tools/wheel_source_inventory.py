"""核对 wheel 中的代码与 package data 是否和当前源码树完全一致。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
from zipfile import BadZipFile, ZipFile

from release_allowlist import package_file_paths, reject_release_package_path


_ALLOWED_DIST_INFO_FILES = {
    "METADATA",
    "WHEEL",
    "entry_points.txt",
    "top_level.txt",
    "RECORD",
}
_ALLOWED_DIST_INFO_PREFIXES = ("licenses/",)
_FORBIDDEN_SUFFIXES = (
    ".db",
    ".duckdb",
    ".duckdb.wal",
    ".sqlite",
    ".sqlite3",
    ".parquet",
    ".pickle",
    ".pkl",
    ".pyc",
    ".log",
)
_FORBIDDEN_NAMES = {"cookies.json"}
_FORBIDDEN_PARTS = {"__pycache__", ".pytest_cache"}


def _digest(items: dict[str, str]) -> str:
    payload = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _file_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _source_package_files(project: Path) -> dict[str, str]:
    source_root = project / "src" / "research_pipeline"
    if not source_root.is_dir():
        raise ValueError(f"research_pipeline 源码目录不存在: {source_root}")
    files: dict[str, str] = {}
    for project_relative in package_file_paths(project):
        path = project / project_relative
        relative = path.relative_to(project / "src").as_posix()
        _reject_forbidden_package_path(PurePosixPath(relative))
        files[relative] = _file_sha256(path.read_bytes())
    if not files:
        raise ValueError("research_pipeline 源码包文件清单为空")
    return files


def _wheel_package_files(wheel: Path, *, forbidden_roots: tuple[bytes, ...]) -> dict[str, str]:
    if not wheel.is_file():
        raise ValueError(f"wheel 不存在: {wheel}")
    try:
        with ZipFile(wheel) as archive:
            files: dict[str, str] = {}
            wheel_name_parts = wheel.name.split("-")
            if len(wheel_name_parts) < 5 or wheel.suffix.lower() != ".whl":
                raise ValueError(f"wheel 文件名无效: {wheel.name}")
            expected_dist_info = f"{wheel_name_parts[0]}-{wheel_name_parts[1]}.dist-info"
            dist_info_files: set[str] = set()
            for item in archive.infolist():
                if item.is_dir():
                    continue
                normalized = item.filename.replace("\\", "/")
                path = PurePosixPath(normalized)
                if path.is_absolute() or ".." in path.parts:
                    raise ValueError(f"wheel Python 路径越界: {item.filename}")
                if path.parts and path.parts[0] in {"research_pipeline", "factor_contracts"}:
                    name = path.as_posix()
                    if name in files:
                        raise ValueError(f"wheel 含重复包文件: {name}")
                    _reject_forbidden_package_path(path)
                    content = archive.read(item)
                    if any(root and root in content for root in forbidden_roots):
                        raise ValueError(f"wheel 包文件泄露本机构建路径: {name}")
                    files[name] = _file_sha256(content)
                elif path.parts and path.parts[0] == expected_dist_info:
                    relative = PurePosixPath(*path.parts[1:]).as_posix()
                    if (
                        relative not in _ALLOWED_DIST_INFO_FILES
                        and not relative.startswith(_ALLOWED_DIST_INFO_PREFIXES)
                    ):
                        raise ValueError(f"wheel dist-info 含非必要文件: {path.as_posix()}")
                    if relative in dist_info_files:
                        raise ValueError(f"wheel 含重复 dist-info 文件: {path.as_posix()}")
                    dist_info_files.add(relative)
                else:
                    raise ValueError(f"wheel 含非正式包文件: {path.as_posix()}")
    except BadZipFile as exc:
        raise ValueError(f"wheel 不是有效 ZIP: {wheel}") from exc
    if not files:
        raise ValueError("wheel 中没有 research_pipeline 包文件")
    required_dist_info = {"METADATA", "WHEEL", "RECORD"}
    if not required_dist_info.issubset(dist_info_files):
        raise ValueError(
            "wheel 缺少必要 dist-info 文件: "
            f"{sorted(required_dist_info - dist_info_files)}"
        )
    return dict(sorted(files.items()))


def _reject_forbidden_package_path(path: PurePosixPath) -> None:
    lowered = path.as_posix().lower()
    if (
        path.name.lower() in _FORBIDDEN_NAMES
        or any(part.lower() in _FORBIDDEN_PARTS for part in path.parts)
        or lowered.endswith(_FORBIDDEN_SUFFIXES)
    ):
        raise ValueError(f"包清单含禁止的运行产物或敏感文件: {path.as_posix()}")
    reject_release_package_path(path)


def verify_wheel_source_inventory(*, project: Path, wheel: Path) -> dict[str, object]:
    """拒绝缺失源码模块或由旧 build 缓存带入的幽灵模块。"""

    project = project.resolve()
    wheel = wheel.resolve()
    expected = _source_package_files(project)
    project_roots = {
        str(project).encode("utf-8"),
        str(project).replace("\\", "/").encode("utf-8"),
    }
    actual = _wheel_package_files(wheel, forbidden_roots=tuple(sorted(project_roots)))
    missing = tuple(sorted(expected.keys() - actual.keys()))
    stale = tuple(sorted(actual.keys() - expected.keys()))
    changed = tuple(
        name
        for name in sorted(expected.keys() & actual.keys())
        if expected[name] != actual[name]
    )
    if missing or stale or changed:
        details = []
        if missing:
            details.append(f"缺少源码包文件: {', '.join(missing[:10])}")
        if stale:
            details.append(f"包含源码已删除的幽灵包文件: {', '.join(stale[:10])}")
        if changed:
            details.append(f"包文件字节与源码不一致: {', '.join(changed[:10])}")
        raise ValueError("wheel/source 包清单不一致；" + "；".join(details))
    python_count = sum(name.endswith(".py") for name in expected)
    return {
        "status": "pass",
        "package_file_count": len(expected),
        "python_file_count": python_count,
        "package_data_file_count": len(expected) - python_count,
        "inventory_sha256": _digest(expected),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="核对 wheel 与源码代码/package data 清单")
    parser.add_argument("--project", required=True)
    parser.add_argument("--wheel", required=True)
    args = parser.parse_args()
    result = verify_wheel_source_inventory(
        project=Path(args.project),
        wheel=Path(args.wheel),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
