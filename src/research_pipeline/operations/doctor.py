"""只读检查当前扩展清单与 operator DAG 运行目录。"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Callable

from research_pipeline.runtime import CheckpointStore, EventStore


@dataclass(frozen=True)
class DoctorFinding:
    status: str
    code: str
    object_id: str
    message: str
    next_action: str


@dataclass(frozen=True)
class DoctorReport:
    status: str
    findings: tuple[DoctorFinding, ...]


def run_research_doctor(
    *,
    run_roots: tuple[str | Path, ...] = (),
    lean_homes: tuple[str | Path, ...] = (),
    invocation_verifier: Callable[[Path], None] | None = None,
) -> DoctorReport:
    findings: list[DoctorFinding] = []
    for path_value in run_roots:
        root = Path(path_value).resolve()
        try:
            record = json.loads(
                (root / "operator-dag-run.json").read_text(encoding="utf-8")
            )
            store = EventStore(root)
            events = store.read_events()
            projection = store.replay()
            if record.get("run_id") != projection.run_id:
                raise ValueError("run identity mismatch")
            checkpoint_root = root / "checkpoints"
            committed = {
                str(event.payload["node_execution_id"])
                for event in events
                if event.kind == "checkpoint_committed"
                and "node_execution_id" in event.payload
            }
            actual = (
                {path.name for path in checkpoint_root.iterdir() if path.is_dir()}
                if checkpoint_root.is_dir()
                else set()
            )
            if committed != actual:
                raise ValueError("checkpoint event mismatch")
            checkpoints = CheckpointStore(root, create=False)
            for node_execution_id in sorted(actual):
                checkpoints.verify_stored(node_execution_id)
            from research_pipeline.platform.lean_runtime import validate_lean_runtime
            for lock_path in root.glob("work/*/lean/runtime-lock.json"):
                validate_lean_runtime(json.loads(lock_path.read_text(encoding="utf-8")), strict=True)
            invocation = root / "operator-dag-invocation.json"
            if invocation.is_file():
                if invocation_verifier is None:
                    raise ValueError("运行包含冻结 invocation，请使用 research doctor CLI 核验计划身份")
                invocation_verifier(root)
            findings.append(DoctorFinding(
                "pass",
                "doctor.runtime_valid",
                str(record["run_id"]),
                "冻结计划、事件链、checkpoint 与 run identity 通过核验",
                "无需操作",
            ))
        except Exception as exc:
            findings.append(DoctorFinding(
                "fail",
                "doctor.runtime_invalid",
                str(root),
                f"当前运行目录或冻结计划核验失败：{exc}",
                "检查计划清单、operator-dag-run.json、events.jsonl 和 checkpoints",
            ))
    for home in lean_homes:
        from research_pipeline.platform.lean_runtime import audit_lean_installation
        try:
            audit_lean_installation(Path(home))
        except Exception as exc:
            findings.append(DoctorFinding("fail", "doctor.lean_install_invalid", str(home), str(exc),
                                          "检查冻结凭证与当前安装"))
        else:
            findings.append(DoctorFinding("pass", "doctor.lean_install_valid", str(home),
                                          "安装文件已通过完整内容核验", "无需操作"))
    status = "fail" if any(item.status == "fail" for item in findings) else (
        "warn" if not findings else "pass"
    )
    if not findings:
        findings.append(DoctorFinding(
            "warn",
            "doctor.no_targets",
            "doctor",
            "没有提供检查目标",
            "提供当前 operator DAG run root",
        ))
    return DoctorReport(status, tuple(findings))


__all__ = ["DoctorFinding", "DoctorReport", "run_research_doctor"]
