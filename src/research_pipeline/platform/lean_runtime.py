"""LEAN 执行文件与市场元数据的显式参数锁。"""

from __future__ import annotations


from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
from contextvars import ContextVar
import csv
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re

from .directory_watch import watch_directory as _watch_installation
from .validation import file_sha256 as _sha256


LEAN_RUNTIME_VERSION = "research-lean-runtime-v3"
# 中国市场规则、策略宿主、输入或成交行为发生变化时更新此版本。
LEAN_ADAPTER_SEMANTIC_VERSION = "research-lean-adapter-v2"
_RELATIVE_FIELDS = (
    "dotnet_relative", "launcher_relative",
    "python_environment_relative", "python_library_relative",
    "dotnet_shared_relative", "dotnet_host_relative", "python_base_relative",
    "config_template_relative", "metadata_relative",
)
_FIELDS = {"contract_version", "installation_root", "audit_hash", "semantic_versions", "upstream_revision", *_RELATIVE_FIELDS}
_PLUGINS = (
    "QuantWitnessLeanHost.dll", "ChinaRules.dll", "CorporateActions.dll",
    "ParquetInput.dll", "ParquetSharp.dll", "ParquetSharpNative.dll",
    "Apache.Arrow.dll", "Python.Runtime.dll",
)
_CACHE_DIRECTORIES = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
_LAUNCHER_SUFFIXES = {".dll", ".json", ".py", ".pyd", ".exe"}
_UNUSED_LAUNCHER_CONFIGS = {"config.json", "config.example.json"}
_AUDIT_RELATIVE = "rp-integration/runtime-audit.json"
_AUDIT_VERSION = "research-lean-installation-audit-v2"
_DOTNET_KEY_FILES = {"coreclr.dll", "hostpolicy.dll", "hostfxr.dll", "System.Private.CoreLib.dll"}


_run_checks = ContextVar("lean_run_checks", default=None)


@contextmanager
def lean_runtime_check_scope():
    """在一次执行内监视安装目录；独立运行与恢复各自重新读取文件。"""
    if _run_checks.get() is not None:
        yield
        return
    with ExitStack() as resources:
        token = _run_checks.set(({}, resources))
        try:
            yield
        finally:
            _run_checks.reset(token)


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
        values["python_library_relative"],
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
            if launcher and not _launcher_file(source):
                continue
            if not entry.is_file():
                raise ValueError(f"LEAN 依赖文件缺失: {relative_name}")
            # 普通文件复用已解析父目录；链接仍核对实际目标。
            target = _contained(root, relative_name, resolved) if entry.is_symlink() else source
            if resolved is not None:
                resolved[relative_name] = target
            yield relative_name


def _launcher_file(path):
    name = path.name.lower()
    return (path.suffix.lower() in _LAUNCHER_SUFFIXES and name not in _UNUSED_LAUNCHER_CONFIGS
            and not name.endswith(("-order-events.json", "-logs.json", "-summary.json")))


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


def _file_digests(root: Path, names, resolved):
    # 按小批调度，避免为安装中的每个小文件创建一个 Future。
    def digest_group(group):
        return [(name, _sha256(_contained(root, name, resolved))) for name in group]

    with ThreadPoolExecutor(max_workers=4) as readers:
        groups = (names[start:start + 16] for start in range(0, len(names), 16))
        for group in readers.map(digest_group, groups):
            yield from group


def freeze_lean_runtime(
    installation_root: str | Path, *, layout: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """引用安装审计记录，允许研究包绑定自己的市场元数据目录。"""
    from research_pipeline.platform.audit_mode import strict_audit_enabled

    root = Path(installation_root).resolve(strict=True)
    overrides = dict(layout or {})
    if set(overrides) - {"metadata_relative"}:
        raise ValueError("LEAN 入口布局须在记录安装审计时声明")
    audit, audit_hash = _load_installation_audit(root)
    runtime = _runtime_from_audit(root, audit, audit_hash, overrides.get("metadata_relative"))
    _verify_installation(root, runtime, audit["runtime"], strict=strict_audit_enabled())
    return _runtime_reference(runtime)


def _freeze_lean_runtime(root, *, layout=None):
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
    resolved = {}
    names = sorted(_installation_files(root, values, resolved=resolved))
    values["files"] = dict(_file_digests(root, names, resolved))
    return values


def _key_file_names(runtime):
    launcher = runtime["launcher_relative"].rpartition("/")[0]
    return _required_files(runtime) | {
        name for name in runtime["files"]
        if name == "upstream-revision.txt"
        or name.rpartition("/")[0] == launcher
        or name.rpartition("/")[2] in _DOTNET_KEY_FILES
        or name.endswith(".dist-info/METADATA")
    }


def _load_installation_audit(root):
    try:
        content = (root / _AUDIT_RELATIVE).read_bytes()
    except FileNotFoundError as exc:
        raise ValueError("LEAN 安装审计凭证缺失，请通过安装或同步流程生成") from exc
    audit = json.loads(content)
    if (not isinstance(audit, dict) or audit.get("contract_version") != _AUDIT_VERSION
            or not isinstance(audit.get("runtime"), dict)
            or audit["runtime"].get("installation_root") != root.as_posix()):
        raise ValueError("LEAN 安装审计凭证无效，请同步安装")
    return audit, hashlib.sha256(content).hexdigest()


def read_lean_config_template(template: Path) -> dict[str, object]:
    text = template.read_text(encoding="utf-8-sig")
    # 保留网址和转义字符串，只去掉 LEAN 配置注释。
    text = re.sub(r'"(?:\\.|[^"\\])*"|//[^\r\n]*|/\*.*?\*/',
                  lambda match: "" if match[0].startswith(("//", "/*")) else match[0], text, flags=re.S)
    return json.loads(text)


def _execution_inputs_identity(root, runtime):
    from research_pipeline.platform import typed_canonical_hash

    configuration = read_lean_config_template(root / runtime["config_template_relative"])
    metadata = runtime["metadata_relative"]
    with (root / metadata / "market-hours/market-hours-database.json").open(encoding="utf-8-sig") as stream:
        hours = json.load(stream)
    with (root / metadata / "symbol-properties/symbol-properties-database.csv").open(
        encoding="utf-8-sig", newline=""
    ) as stream:
        symbols = list(csv.reader(stream))
    return typed_canonical_hash({"config": configuration, "hours": hours, "symbols": symbols})


def _runtime_with_metadata(root, installed, metadata_relative, *, execution_inputs=None):
    relative = _relative(metadata_relative, "metadata_relative")
    previous_prefix = installed["metadata_relative"] + "/"
    files = {name: digest for name, digest in installed["files"].items() if not name.startswith(previous_prefix)}
    resolved = {}
    names = sorted(_tree_files(root, relative, resolved=resolved))
    files.update(_file_digests(root, names, resolved))
    runtime = {**installed, "metadata_relative": relative, "files": dict(sorted(files.items()))}
    missing = _required_files(runtime) - files.keys()
    if missing:
        raise ValueError(f"LEAN 自定义元数据缺失必需文件: {', '.join(sorted(missing))}")
    runtime["semantic_versions"] = {
        **installed["semantic_versions"], "execution_inputs": execution_inputs or _execution_inputs_identity(root, runtime),
    }
    return runtime


def _semantic_versions(root, runtime):
    from research_pipeline.platform import typed_canonical_hash

    launcher_deps = str(PurePosixPath(runtime["launcher_relative"]).with_suffix(".deps.json"))
    with (root / launcher_deps).open(encoding="utf-8-sig") as stream:
        dotnet_libraries = sorted(json.load(stream)["libraries"])
    files = runtime["files"]
    return {
        "adapter_version": LEAN_ADAPTER_SEMANTIC_VERSION,
        "dotnet_libraries": typed_canonical_hash(dotnet_libraries),
        "dotnet_runtime": sorted(name.split("/")[-2] for name in files if name.endswith("/coreclr.dll")),
        "python_runtime": PurePosixPath(runtime["python_base_relative"]).name,
        "python_packages": typed_canonical_hash(
            sorted(name.split("/")[-2] for name in files if name.endswith(".dist-info/METADATA"))
        ),
        "execution_inputs": _execution_inputs_identity(root, runtime),
    }


def lean_runtime_semantic_identity(runtime: Mapping[str, object]) -> dict[str, object]:
    return {"contract_version": runtime["contract_version"],
            "upstream_revision": runtime["upstream_revision"], "versions": runtime["semantic_versions"]}


def _runtime_reference(runtime):
    return {name: value for name, value in runtime.items() if name != "files"}


def _runtime_from_audit(root, audit, audit_hash, metadata_relative=None, *, execution_inputs=None):
    from research_pipeline.platform import typed_canonical_hash

    installed = audit["runtime"]
    runtime = {**installed, "audit_hash": audit_hash}
    if metadata_relative is not None and metadata_relative != installed["metadata_relative"]:
        runtime = _runtime_with_metadata(root, runtime, metadata_relative, execution_inputs=execution_inputs)
        prefix = runtime["metadata_relative"] + "/"
        runtime["audit_hash"] = typed_canonical_hash(
            {"installation": audit_hash, "execution_inputs": runtime["semantic_versions"]["execution_inputs"], "metadata": {
                name: digest for name, digest in runtime["files"].items() if name.startswith(prefix)
            }}
        )
    return runtime


def _verify_installation(root, runtime, installed, *, strict):
    resolved = {}
    files = runtime["files"]
    if strict:
        current = _installation_files(root, runtime, resolved=resolved)
        missing, added = files.keys() - current, current - files.keys()
        if missing or added:
            raise ValueError(f"LEAN 执行文件闭包变化: 缺失={sorted(missing)}, 新增={sorted(added)}")
        names = files
    else:
        names = _key_file_names(runtime)
        if runtime["metadata_relative"] != installed["metadata_relative"]:
            # 自定义元数据刚在读取审计凭证时逐文件计算了 SHA-256。
            names = {name for name in names if not name.startswith(runtime["metadata_relative"] + "/")}
        launcher = PurePosixPath(runtime["launcher_relative"]).parent
        with os.scandir(_contained(root, launcher.as_posix(), resolved)) as entries:
            added = sorted(entry.name for entry in entries if entry.is_file() and _launcher_file(Path(entry.name))
                           and str(launcher / entry.name) not in files)
        if added:
            raise ValueError(f"LEAN Launcher 新增执行文件: {', '.join(added)}")
    try:
        for name, digest in _file_digests(root, sorted(names), resolved):
            if digest != files[name]:
                raise ValueError(f"LEAN 执行文件内容变化: {name}")
    except FileNotFoundError as exc:
        raise ValueError(f"LEAN 必需文件缺失: {exc.filename}") from exc


def record_lean_installation(installation_root: str | Path, *, engine_source_id: str,
                             layout: Mapping[str, str] | None = None) -> None:
    """安装和同步完成后在安装目录发布完整文件清单。"""
    root = Path(installation_root).resolve(strict=True)
    runtime = _freeze_lean_runtime(root, layout=layout)
    runtime["semantic_versions"] = _semantic_versions(root, runtime)
    audit = {
        "contract_version": _AUDIT_VERSION,
        "engine_source_id": engine_source_id,
        "runtime": runtime,
    }
    target = root / _AUDIT_RELATIVE
    temporary = target.with_suffix(".tmp")
    try:
        temporary.write_text(json.dumps(audit, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def audit_lean_installation(installation_root: str | Path) -> dict[str, object]:
    """用安装时的凭证逐个复验执行依赖，并返回只读审计摘要。"""
    root = Path(installation_root).resolve(strict=True)
    audit, audit_hash = _load_installation_audit(root)
    runtime = _runtime_from_audit(root, audit, audit_hash)
    _verify_installation(root, runtime, audit["runtime"], strict=True)
    return {"installation_root": runtime["installation_root"], "upstream_revision": runtime["upstream_revision"],
            "verified_files": len(runtime["files"])}


def validate_lean_runtime(payload: object, verify_files: bool = True, *, strict: bool | None = None) -> dict[str, object]:
    """复验参数锁；同一调用内监视安装目录，仅在内容未变化时复用。"""
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
    audit_hash = payload["audit_hash"]
    if not isinstance(audit_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", audit_hash):
        raise ValueError("LEAN 安装审计身份无效")
    versions = payload["semantic_versions"]
    scalars = {"adapter_version", "python_runtime", "execution_inputs", "dotnet_libraries", "python_packages"}
    if (not isinstance(versions, Mapping) or set(versions) != scalars | {"dotnet_runtime"}
            or any(not isinstance(versions[name], str) or not versions[name] for name in scalars)
            or not isinstance(versions["dotnet_runtime"], (tuple, list))
            or any(not isinstance(item, str) or not item for item in versions["dotnet_runtime"])):
        raise ValueError("LEAN 执行版本声明无效")
    values = {**payload, "installation_root": root.as_posix(),
              "semantic_versions": {**versions, "dotnet_runtime": list(versions["dotnet_runtime"])}}
    for field in _RELATIVE_FIELDS:
        values[field] = _relative(payload[field], field)
    expected_names = {"dotnet_relative": "dotnet.exe", "launcher_relative": "QuantConnect.Lean.Launcher.dll"}
    if any(PurePosixPath(values[field]).name != name for field, name in expected_names.items()):
        raise ValueError("LEAN runtime lock 入口名称不受支持")
    if not PurePosixPath(values["python_library_relative"]).is_relative_to(values["python_base_relative"]):
        raise ValueError("LEAN Python DLL 必须位于声明的 Python base 内")
    if type(verify_files) is not bool:
        raise ValueError("LEAN verify_files 必须是布尔值")
    if not verify_files:
        return values

    if strict is None:
        from research_pipeline.platform.audit_mode import strict_audit_enabled
        strict = strict_audit_enabled()
    scope = _run_checks.get()
    unchanged = None
    if scope is not None:
        checked, resources = scope
        previous = checked.get((root, strict))
        if previous is not None:
            frozen, prior_unchanged = previous
            if frozen == values and prior_unchanged():
                return values
        # 在读取凭证前订阅；无法监视目录时逐次复验。
        unchanged = _watch_installation(root, resources)
    audit, base_hash = _load_installation_audit(root)
    runtime = _runtime_from_audit(root, audit, base_hash, values["metadata_relative"],
        execution_inputs=None if strict else versions["execution_inputs"])
    if _runtime_reference(runtime) != values:
        raise ValueError("LEAN 已安装版本与冻结计划不一致，请重新准入")
    _verify_installation(root, runtime, audit["runtime"], strict=strict)
    if unchanged is not None:
        checked[(root, strict)] = (
            {**values, "semantic_versions": {**values["semantic_versions"],
                "dotnet_runtime": list(values["semantic_versions"]["dotnet_runtime"])}},
            unchanged,
        )
    return values


__all__ = [
    "LEAN_ADAPTER_SEMANTIC_VERSION", "LEAN_RUNTIME_VERSION",
    "audit_lean_installation", "freeze_lean_runtime", "lean_runtime_semantic_identity",
    "record_lean_installation", "validate_lean_runtime",
]
