"""封存原生LEAN输出、算法源码和ObjectStore。"""
from pathlib import Path
import json
import shutil

from .lean_contract import LEAN_JSONL_FILES, LEAN_SUPPORT_PATHS


def copy_lean_support(root, staging):
    """保留引擎实际产物；行情交接和运行过程文件不进入原生结果目录。"""
    root, staging = Path(root), Path(staging)
    for relative in LEAN_SUPPORT_PATHS:
        source = root / Path(relative).relative_to("lean")
        destination = staging / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    excluded = set(LEAN_JSONL_FILES.values()) | {
        "input.json", "native-bindings.json", "actions.json", "callback-events.jsonl",
        "metadata", "strategy-source", "storage", "temp", "native-data", "input", "inputs", "evidence",
    } | {str(Path(name).relative_to("lean")) for name in LEAN_SUPPORT_PATHS}
    copied = []
    for source in sorted(root.iterdir(), key=lambda item: item.name):
        if source.name in excluded:
            continue
        if source.is_dir() and staging.resolve().is_relative_to(source.resolve()):
            continue
        destination = staging / "lean" / "native-output" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, destination)
        else:
            shutil.copyfile(source, destination)
        copied.append(source.name)
    for name in ("strategy-source", "storage"):
        source = root / name
        if source.is_dir():
            shutil.copytree(source, staging / "lean" / name)
    manifest = {
        "version": "research-lean-native-support-v1",
        "native_output": copied,
        "algorithm_source": "lean/strategy-source" if (root / "strategy-source").is_dir() else None,
        "object_store": "lean/storage" if (root / "storage").is_dir() else None,
    }
    (staging / "lean" / "native-support.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
