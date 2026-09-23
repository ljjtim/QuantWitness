"""只读检查当前扩展清单与 operator DAG 运行目录。"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

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
            findings.append(DoctorFinding(
                "pass",
                "doctor.runtime_valid",
                str(record["run_id"]),
                "事件链、checkpoint 与 run identity 一致",
                "无需操作",
            ))
        except Exception:
            findings.append(DoctorFinding(
                "fail",
                "doctor.runtime_invalid",
                root.name,
                "当前运行目录、事件链或 run identity 无效",
                "检查 operator-dag-run.json、events.jsonl 和 checkpoints",
            ))
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
