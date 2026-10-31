"""固定LEAN启动入口；Windows子进程随宿主退出。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    args = parser.parse_args()
    request = json.loads(args.request.read_text(encoding="utf-8"))
    from research_pipeline.runtime.project_operator_worker import _bind_windows_descendants_to_worker_lifetime
    if sys.platform == "win32" and not _bind_windows_descendants_to_worker_lifetime():
        raise RuntimeError("LEAN宿主无法绑定Windows进程生命周期")
    import psutil
    process = psutil.Process()
    if hasattr(process, "cpu_affinity"):
        allowed = process.cpu_affinity()
        process.cpu_affinity(allowed[:request["cpu_slots"]])
    runtime = request["runtime"]
    root = Path(runtime["installation_root"])
    launcher = root / runtime["launcher_relative"]
    python_root = (root / runtime["python_library_relative"]).parent
    venv = root / runtime["python_environment_relative"]
    output = Path(request["output_root"])
    environment = os.environ.copy()
    environment.update({
        "DOTNET_ROOT": str((root / runtime["dotnet_relative"]).parent),
        "DOTNET_CLI_TELEMETRY_OPTOUT": "1", "DOTNET_NOLOGO": "1",
        "DOTNET_CLI_HOME": str(output / "temp/dotnet"), "DOTNET_SKIP_FIRST_TIME_EXPERIENCE": "1",
        "PYTHONNET_PYDLL": str(root / runtime["python_library_relative"]),
        "PYTHONHOME": str(python_root), "PYTHONNOUSERSITE": "1",
        "PYTHONPATH": str(venv / "Lib/site-packages"), "PYTHONDONTWRITEBYTECODE": "1",
        "TEMP": str(output / "temp"), "TMP": str(output / "temp"),
        "MPLCONFIGDIR": str(output / "temp/matplotlib"),
    })
    environment["PATH"] = os.pathsep.join((str(python_root), str(python_root / "DLLs"),
        str((root / runtime["dotnet_relative"]).parent), environment.get("PATH", "")))
    (output / "temp").mkdir(exist_ok=True)
    with (output / "engine.log").open("wb") as log:
        result = subprocess.run([str(root / runtime["dotnet_relative"]), str(launcher),
            "--config", str(output / "config.json")], cwd=launcher.parent,
            env=environment, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            check=False, creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
