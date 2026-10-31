"""LEAN 执行文件与市场元数据的显式参数锁。"""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path, PurePosixPath
import re


LEAN_RUNTIME_VERSION = "research-lean-runtime-v1"
_RELATIVE_FIELDS = (
    "dotnet_relative", "launcher_relative", "algorithm_relative",
    "python_environment_relative", "python_library_relative",
    "dotnet_shared_relative", "dotnet_host_relative", "python_base_relative",
    "config_template_relative", "metadata_relative",
)
_FIELDS = {"contract_version", "installation_root", "files", "upstream_revision", *_RELATIVE_FIELDS}
_PLUGINS = (
    "QuantWitnessLeanHost.dll", "ChinaRules.dll", "CorporateActions.dll",
    "ParquetInput.dll", "ParquetSharp.dll", "ParquetSharpNative.dll",
    "Apache.Arrow.dll", "Python.Runtime.dll",
)
_CACHE_DIRECTORIES = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
_LAUNCHER_SUFFIXES = {".dll", ".json", ".py", ".pyd", ".exe"}
_UNUSED_LAUNCHER_CONFIGS = {"config.json", "config.example.json"}


def _relative(value: object, field: str) -> str:
    if (not isinstance(value, str) or not value or "\\" in value or ":" in value
            or PurePosixPath(value).is_absolute()
            or any(part in {"", ".", ".."} for part in value.split("/"))):
        raise ValueError(f"LEAN {field} 必须是安装根内的规范相对文件名")
    return value


def _contained(root: Path, relative: str, resolved: dict[str, Path] | None = None) -> Path:
    if resolved is not None and relative in resolved:
        return resolved[relative]
    target = (root / relative).resolve()
    if not target.is_relative_to(root):
        raise ValueError(f"LEAN 文件不在安装根内: {relative}")
    if resolved is not None:
        resolved[relative] = target
    return target


def _required_files(values: Mapping[str, object]) -> set[str]:
    launcher = PurePosixPath(values["launcher_relative"])
    environment = PurePosixPath(values["python_environment_relative"])
    base = PurePosixPath(values["python_base_relative"])
    metadata = PurePosixPath(values["metadata_relative"])
    return {
        values["dotnet_relative"], values["launcher_relative"],
        values["algorithm_relative"], values["python_library_relative"],
        values["config_template_relative"],
        str(launcher.with_suffix(".deps.json")),
        str(launcher.with_suffix(".runtimeconfig.json")),
        *(str(launcher.parent / name) for name in _PLUGINS),
        str(environment / "pyvenv.cfg"), str(environment / "Scripts/python.exe"),
        str(base / "python.exe"), str(base / "Lib/os.py"),
        str(base / "Lib/encodings/__init__.py"),
        str(metadata / "market-hours/market-hours-database.json"),
        str(metadata / "symbol-properties/symbol-properties-database.csv"),
    }


def _tree_files(root: Path, relative: str, *, launcher: bool = False, resolved=None):
    directory = _contained(root, relative, resolved)
    if not directory.is_dir():
        raise ValueError(f"LEAN 依赖目录缺失: {relative}")
    pending = [relative]
    while pending:
        current = pending.pop()
        directory = _contained(root, current, resolved)
        with os.scandir(directory) as entries:
            children = sorted(entries, key=lambda entry: entry.name)
        for entry in children:
            name = entry.name
            relative_name = str(PurePosixPath(current) / name)
            if entry.is_dir():
                if (name not in _CACHE_DIRECTORIES and not entry.is_symlink()
                        and (not launcher or name.lower() not in {"logs", "results", "runs"})):
                    pending.append(relative_name)
                continue
            source = directory / name
            if source.suffix.lower() in {".pyc", ".pyo", ".log"}:
                continue
            if launcher and (source.suffix.lower() not in _LAUNCHER_SUFFIXES
                    or name.lower() in _UNUSED_LAUNCHER_CONFIGS
                    or name.lower().endswith(("-order-events.json", "-logs.json", "-summary.json"))):
                continue
            if not entry.is_file():
                raise ValueError(f"LEAN 依赖文件缺失: {relative_name}")
            # 普通文件复用已解析父目录；链接仍核对实际目标。
            target = _contained(root, relative_name, resolved) if entry.is_symlink() else source
            if resolved is not None:
                resolved[relative_name] = target
            yield relative_name


def _installation_files(root: Path, values: Mapping[str, object], *, resolved=None) -> set[str]:
    required = _required_files(values)
    for name in sorted(required):
        if not _contained(root, name, resolved).is_file():
            raise ValueError(f"LEAN 必需文件缺失: {name}")
    files = set(required)
    files.update(_tree_files(root, str(PurePosixPath(values["launcher_relative"]).parent), launcher=True, resolved=resolved))
    for field in ("dotnet_shared_relative", "dotnet_host_relative", "metadata_relative"):
        files.update(_tree_files(root, values[field], resolved=resolved))
    shared = values["dotnet_shared_relative"] + "/"
    host = values["dotnet_host_relative"] + "/"
    for prefix, basename in ((shared, "coreclr.dll"), (shared, "hostpolicy.dll"),
                             (shared, "System.Private.CoreLib.dll"), (host, "hostfxr.dll")):
        if not any(name.startswith(prefix) and PurePosixPath(name).name == basename for name in files):
            raise ValueError(f"LEAN .NET 运行依赖缺失: {basename}")
    base_relative = values["python_base_relative"]
    base = _contained(root, base_relative, resolved)
    for source in base.iterdir():
        if source.is_file() and (source.suffix.lower() in {".dll", ".exe", ".zip", "._pth"}):
            files.add(source.relative_to(root).as_posix())
    files.update(_tree_files(root, base_relative + "/Lib", resolved=resolved))
    if (base / "DLLs").is_dir():
        files.update(_tree_files(root, base_relative + "/DLLs", resolved=resolved))
    environment_relative = values["python_environment_relative"]
    files.update(_tree_files(root, environment_relative + "/Lib", resolved=resolved))
    scripts = _contained(root, environment_relative + "/Scripts", resolved)
    for source in scripts.iterdir():
        if source.is_file() and (source.suffix.lower() == ".dll"
                or re.fullmatch(r"python(?:w|[0-9.]*)?\.exe", source.name, re.IGNORECASE)):
            files.add(source.relative_to(root).as_posix())
    revision = root / "upstream-revision.txt"
    if revision.is_file():
        files.add("upstream-revision.txt")
    return files


def _sha256(source: Path) -> str:
    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _file_digests(root: Path, names, resolved):
    # 只并发读取独立文件，按原排序交付身份；每路至多保留一个读取块。
    with ThreadPoolExecutor(max_workers=4) as readers:
        paths = (_contained(root, name, resolved) for name in names)
        yield from zip(names, readers.map(_sha256, paths))


def freeze_lean_runtime(
    installation_root: str | Path, *, layout: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """冻结已安装的执行闭包；layout 只替换明确入口和目录的相对位置。"""
    root = Path(installation_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("LEAN installation_root 必须是已存在的安装目录")
    overrides = dict(layout or {})
    if set(overrides) - {*_RELATIVE_FIELDS, "upstream_revision"}:
        raise ValueError("LEAN layout 包含未支持字段")
    revision = overrides.pop("upstream_revision", None)
    if revision is None:
        revision = (root / "upstream-revision.txt").read_text(encoding="utf-8").strip()
    values = {
        "contract_version": LEAN_RUNTIME_VERSION,
        "installation_root": root.as_posix(),
        "upstream_revision": revision,
        "dotnet_relative": "sdk/dotnet.exe",
        "launcher_relative": f"src/Lean-{revision}/Launcher/bin/Release/QuantConnect.Lean.Launcher.dll",
        "algorithm_relative": "rp-integration/ResearchLeanAlgorithm.py",
        "python_environment_relative": "python-env",
        "dotnet_shared_relative": "sdk/shared",
        "dotnet_host_relative": "sdk/host",
        "config_template_relative": "rp-integration/config-template.json",
        "metadata_relative": "rp-integration/metadata",
        **overrides,
    }
    for field in _RELATIVE_FIELDS:
        if field in values:
            _contained(root, _relative(values[field], field))
    if "python_base_relative" not in values:
        if "python_library_relative" in values:
            values["python_base_relative"] = str(PurePosixPath(values["python_library_relative"]).parent)
        else:
            configuration = _contained(root, values["python_environment_relative"] + "/pyvenv.cfg")
            entries = dict(line.split("=", 1) for line in configuration.read_text(encoding="utf-8").splitlines()
                           if "=" in line)
            entries = {key.strip(): value.strip() for key, value in entries.items()}
            home = Path(entries.get("home", "")).resolve()
            if not home.is_dir() or not home.is_relative_to(root):
                raise ValueError("LEAN Python base 必须位于安装根内")
            values["python_base_relative"] = home.relative_to(root).as_posix()
    if "python_library_relative" not in values:
        base = _contained(root, _relative(values["python_base_relative"], "python_base_relative"))
        libraries = sorted(source for source in base.glob("python*.dll")
                           if re.fullmatch(r"python[0-9]{2,3}\.dll", source.name, re.IGNORECASE))
        if len(libraries) != 1:
            raise ValueError("LEAN Python base 必须有唯一版本化 Python DLL")
        values["python_library_relative"] = libraries[0].relative_to(root).as_posix()
    values["files"] = {name: "0" * 64 for name in sorted(_required_files(values))}
    normalized = validate_lean_runtime(values, verify_files=False)
    resolved = {}
    names = sorted(_installation_files(root, normalized, resolved=resolved))
    normalized["files"] = dict(_file_digests(root, names, resolved))
    return normalized


def validate_lean_runtime(payload: object, verify_files: bool = True) -> dict[str, object]:
    """复验参数锁及实际文件；关闭文件复验仅用于离线结构检查。"""
    if not isinstance(payload, Mapping) or set(payload) != _FIELDS:
        raise ValueError("LEAN runtime lock schema 无效")
    if payload["contract_version"] != LEAN_RUNTIME_VERSION:
        raise ValueError("LEAN runtime lock 版本不受支持")
    revision = payload["upstream_revision"]
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("LEAN upstream_revision 必须是完整提交身份")
    installation = payload["installation_root"]
    if not isinstance(installation, str) or not Path(installation).is_absolute():
        raise ValueError("LEAN installation_root 必须是绝对目录")
    root = Path(installation).resolve()
    # 实际文件检查在本次调用内复用路径；结构检查不逐个访问依赖文件。
    resolved = {}
    values = {"contract_version": LEAN_RUNTIME_VERSION, "installation_root": root.as_posix(),
              "upstream_revision": revision}
    for field in _RELATIVE_FIELDS:
        values[field] = _relative(payload[field], field)
        if verify_files:
            _contained(root, values[field], resolved)
    expected_names = {"dotnet_relative": "dotnet.exe", "launcher_relative": "QuantConnect.Lean.Launcher.dll",
                      "algorithm_relative": "ResearchLeanAlgorithm.py"}
    if any(PurePosixPath(values[field]).name != name for field, name in expected_names.items()):
        raise ValueError("LEAN runtime lock 入口名称不受支持")
    if not PurePosixPath(values["python_library_relative"]).is_relative_to(values["python_base_relative"]):
        raise ValueError("LEAN Python DLL 必须位于声明的 Python base 内")
    files = payload["files"]
    if not isinstance(files, Mapping) or not files:
        raise ValueError("LEAN runtime lock files 必须是非空文件身份映射")
    normalized_files = {}
    for name, digest in files.items():
        _relative(name, "files")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(f"LEAN 文件身份无效: {name}")
        normalized_files[name] = digest
    values["files"] = dict(sorted(normalized_files.items()))
    missing = _required_files(values) - set(files)
    if missing:
        raise ValueError(f"LEAN runtime lock 未冻结必需文件: {', '.join(sorted(missing))}")
    if type(verify_files) is not bool:
        raise ValueError("LEAN verify_files 必须是布尔值")
    if verify_files:
        current = _installation_files(root, values, resolved=resolved)
        missing = set(files) - current
        added = current - set(files)
        if missing or added:
            raise ValueError(f"LEAN 执行文件闭包变化: 缺失={sorted(missing)}, 新增={sorted(added)}")
        for name, digest in _file_digests(root, list(values["files"]), resolved):
            if digest != values["files"][name]:
                raise ValueError(f"LEAN 执行文件内容变化: {name}")
    return values


__all__ = ["LEAN_RUNTIME_VERSION", "freeze_lean_runtime", "validate_lean_runtime"]
