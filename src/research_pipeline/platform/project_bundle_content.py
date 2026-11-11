"""受控项目bundle的纯文件闭包及内容身份复验。"""
from pathlib import Path, PurePosixPath
from collections.abc import Mapping
from types import SimpleNamespace
import hashlib
import json
import os
from .canonical import typed_canonical_hash


def _canonical_bytes(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(data):
    return hashlib.sha256(data).hexdigest()


def _assert_regular_file(path: Path, *, root: Path) -> None:
    if not path.is_file():
        raise ValueError(f"项目源码包含非普通文件: {path}")
    try:
        path.resolve(strict=True).relative_to(root)
    except ValueError as exc:
        raise ValueError(f"项目源码逃逸受信根: {path}") from exc


def _source_paths(root: Path) -> tuple[Path, ...]:
    paths = []
    for directory, directories, names in os.walk(root, followlinks=True):
        current = Path(directory)
        directories[:] = [name for name in directories if name != "__pycache__"]
        ancestors = {current.resolve()}
        ancestors.update(parent.resolve() for parent in current.parents
                         if parent == root or root in parent.parents)
        for name in directories:
            target = (current / name).resolve(strict=True)
            if not target.is_relative_to(root) or target in ancestors:
                raise ValueError("项目源码目录越界或循环")
        paths.extend(current / name for name in (*directories, *names))
    return tuple(sorted(paths, key=lambda path: path.as_posix()))


def verify_project_bundle_content(path, *, manifest_validator=None):
    root = Path(path).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("项目 bundle 必须是已存在目录")
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("项目 bundle 缺少 manifest.json")
    expected_top = {"sources", "dependency-lock.json", "manifest.json", "COMMITTED"}
    if {item.name for item in root.iterdir()} != expected_top:
        raise ValueError("项目 bundle 顶层文件集合漂移")
    for name in ("dependency-lock.json", "manifest.json", "COMMITTED"):
        _assert_regular_file(root / name, root=root)
    if not (root / "sources").is_dir() or not (root / "sources").resolve().is_relative_to(root):
        raise ValueError("项目 bundle sources 必须位于 bundle 根目录内")
    try:
        raw = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("项目 bundle manifest 无法读取") from exc
    if not isinstance(raw, Mapping):
        raise ValueError("项目 bundle manifest 必须是对象")
    if manifest_validator is not None:
        manifest_validator(raw)
    manifest = SimpleNamespace(**raw)
    if typed_canonical_hash({key: value for key, value in raw.items() if key != "bundle_hash"}) != manifest.bundle_hash:
        raise ValueError("项目 bundle hash 不一致")
    if root.name != manifest.bundle_hash:
        raise ValueError("项目 bundle 目录名与身份不一致")
    if (root / "COMMITTED").read_text(encoding="utf-8") != manifest.bundle_hash + "\n":
        raise ValueError("项目 bundle COMMITTED 不一致")
    lock_bytes = _canonical_bytes(dict(manifest.dependency_lock)) + b"\n"
    if (root / "dependency-lock.json").read_bytes() != lock_bytes:
        raise ValueError("项目 bundle dependency lock 漂移")
    source_root = root / "sources"
    descendants = _source_paths(source_root)
    actual_paths = tuple(sorted(item.relative_to(source_root).as_posix() for item in descendants if item.is_file()))
    declared_paths = tuple(str(item["path"]) for item in manifest.source_files)
    if declared_paths != tuple(sorted(declared_paths)) or len(set(declared_paths)) != len(declared_paths):
        raise ValueError("项目 bundle 源码清单未规范排序")
    if actual_paths != declared_paths:
        raise ValueError("项目 bundle 源码文件集合漂移")
    allowed_directories = {""}
    for relative in declared_paths:
        parent = PurePosixPath(relative).parent
        while parent.as_posix() != ".":
            allowed_directories.add(parent.as_posix())
            parent = parent.parent
    actual_directories = {item.relative_to(source_root).as_posix() for item in descendants if item.is_dir()}
    if actual_directories != allowed_directories - {""}:
        raise ValueError("项目 bundle 源码目录集合漂移")
    for item in manifest.source_files:
        file_path = source_root / str(item["path"])
        _assert_regular_file(file_path, root=source_root.resolve(strict=True))
        data = file_path.read_bytes()
        if len(data) != item["size"] or _sha256(data) != item["sha256"]:
            raise ValueError(f"项目 bundle 源码漂移: {item['path']}")
    if typed_canonical_hash({"source_files": [dict(item) for item in manifest.source_files]}) != manifest.source_tree_hash:
        raise ValueError("项目 bundle source tree hash 不一致")
    if typed_canonical_hash(dict(manifest.dependency_lock)) != manifest.dependency_lock_hash:
        raise ValueError("项目 bundle dependency lock hash 不一致")
    return raw
