"""在新研究准备前同步本地源码，未变化时跳过安装和编译。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from xml.etree import ElementTree


PACKAGE_INPUTS = (
    "pyproject.toml",
    "src",
    "integrations/lean/refresh-package.ps1",
)
ENGINE_ROOT = Path("integrations/lean/engine")
ENGINE_BUILD_INPUTS = (
    ENGINE_ROOT / "build.ps1",
    ENGINE_ROOT / "Directory.Build.props",
    ENGINE_ROOT / "build_pythonnet.py",
    ENGINE_ROOT / "install_metadata.py",
    ENGINE_ROOT.parent / "patch_native_lifecycle.py",
)
ENGINE_PROJECTS = (
    ENGINE_ROOT / "QuantWitnessLeanHost.csproj",
    ENGINE_ROOT / "parquet-direct/ParquetInput.csproj",
)
AUDIT_RECORD = "rp-integration/runtime-audit.json"


def source_digest(project, names):
    """摘要用于跳过远比读取源码昂贵的 wheel 安装和插件编译。"""
    digest = hashlib.sha256()
    for name in names:
        root = project / name
        paths = sorted(root.rglob("*")) if root.is_dir() else [root]
        for path in paths:
            if not path.is_file() or any(
                part in {"__pycache__", "bin", "obj", "tests"} or part.endswith(".egg-info")
                for part in path.relative_to(project).parts
            ) or path.suffix in {".pyc", ".pyo"}:
                continue
            relative = path.relative_to(project).as_posix().encode("utf-8")
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            digest.update(path.stat().st_size.to_bytes(8, "big"))
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
    return digest.hexdigest()


def require_idle(lean_home):
    """使用中的安装不能被新启动的研究覆盖。"""
    import psutil

    own = psutil.Process()
    excluded = {own.pid, *(parent.pid for parent in own.parents())}
    prefix = os.path.normcase(str(lean_home)) + os.sep
    for process in psutil.process_iter(["pid", "exe", "cmdline"]):
        if process.pid in excluded:
            continue
        arguments = process.info["cmdline"] or []
        values = [process.info["exe"] or "", *arguments]
        uses_installation = any(os.path.normcase(value.strip('"')).startswith(prefix) for value in values)
        if uses_installation or is_research_execution(arguments):
            raise RuntimeError(f"LEAN安装正在使用，进程{process.pid}退出后再同步：{lean_home}")


def is_research_execution(arguments):
    """独立CLI在上游阶段也持有冻结安装；不读取其大型持久计划。"""
    for index, argument in enumerate(arguments):
        if argument == "research_pipeline" and index > 0 and arguments[index - 1] == "-m":
            tail = arguments[index + 1:]
            break
        if Path(argument).name.lower() in {"quantwitness", "quantwitness.exe"}:
            tail = arguments[index + 1:]
            break
    else:
        return False
    return bool(tail) and (
        tail[0] in {"run", "resume", "retry-node", "rerun-from"}
        or (tail[0] == "workspace" and len(tail) > 1 and tail[1] in {"run", "execute"})
    )


def engine_inputs(project):
    """沿生产工程的显式编译项和项目引用识别构建输入。"""
    inputs = set(ENGINE_BUILD_INPUTS)
    pending = list(ENGINE_PROJECTS)
    while pending:
        relative = pending.pop()
        if relative in inputs:
            continue
        inputs.add(relative)
        path = project / relative
        if not path.is_file():
            continue
        definition = ElementTree.parse(path)
        for item_kind in ("Compile", "ProjectReference", "EmbeddedResource", "Content", "None"):
            for item in definition.iterfind(f"./ItemGroup/{item_kind}[@Include]"):
                target = (path.parent / item.attrib["Include"]).resolve().relative_to(project.resolve())
                if item_kind == "ProjectReference":
                    pending.append(target)
                else:
                    inputs.add(target)
    return sorted(inputs)


def source_state(project):
    return {
        "package": source_digest(project, PACKAGE_INPUTS),
        "engine": source_digest(project, engine_inputs(project)),
    }


def publish_installation_audit(lean_home, state):
    from research_pipeline.platform.lean_runtime import record_lean_installation

    record_lean_installation(lean_home, engine_source_id=state["engine"])


def record_installed(project, lean_home):
    state = source_state(Path(project))
    publish_installation_audit(lean_home, state)
    (Path(lean_home) / "source-sync.json").write_text(
        json.dumps(state, indent=2) + "\n", encoding="utf-8")


def synchronize(project, lean_home, *, force_package=False, nuget_cache=None):
    project, lean_home = Path(project).resolve(), Path(lean_home).resolve()
    nuget_cache = Path(nuget_cache).resolve() if nuget_cache is not None else None
    state_path = lean_home / "source-sync.json"
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    current = source_state(project)
    changed = [name for name, value in current.items()
               if state.get(name) != value or (name == "package" and force_package)]
    if not changed:
        return {"status": "unchanged", "updated": [], "lean_home": str(lean_home)}
    require_idle(lean_home)
    # 失败不能留下可用于跳过安装的旧记录。
    state_path.unlink(missing_ok=True)
    (lean_home / AUDIT_RECORD).unlink(missing_ok=True)
    integration = project / "integrations/lean"
    for name in changed:
        script = integration / ("refresh-package.ps1" if name == "package" else "engine/build.ps1")
        command = ["pwsh", "-NoProfile", "-File", str(script), "-LeanHome", str(lean_home)]
        if name == "engine":
            command.append("-Install")
            if nuget_cache:
                command.extend(["-NugetCache", str(nuget_cache)])
        subprocess.run(command, check=True, cwd=project)
    publish_installation_audit(lean_home, current)
    state_path.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")
    return {"status": "synchronized", "updated": changed, "lean_home": str(lean_home)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lean-home", type=Path, required=True)
    parser.add_argument("--force-package", action="store_true")
    parser.add_argument("--nuget-cache", type=Path)
    args = parser.parse_args()
    print(json.dumps(synchronize(Path(__file__).resolve().parents[2], args.lean_home,
                                 force_package=args.force_package, nuget_cache=args.nuget_cache),
                     ensure_ascii=False))
