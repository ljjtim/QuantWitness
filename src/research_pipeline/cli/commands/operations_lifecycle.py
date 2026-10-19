"""只读诊断与默认 dry-run 的研究工件回收命令。"""

from __future__ import annotations

from dataclasses import asdict
import time

from research_pipeline.operations import (
    apply_artifact_gc,
    plan_artifact_gc,
    run_research_doctor,
)

from research_pipeline.platform import MainlineError

from ..result import execute_guarded


class DoctorFailedError(MainlineError):
    """保留各检查目标的诊断事实和处理建议。"""

    error_code = "doctor_failed"

    def __init__(self, findings: list[dict[str, object]]) -> None:
        super().__init__("research doctor 未通过，请按 findings 定位检查目标")
        self.failure_payload = {"doctor_status": "fail", "findings": findings}


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    if args.command == "gc":
        now_ns = args.now_ns if args.now_ns is not None else time.time_ns()
        plan = plan_artifact_gc(args.root, ttl_seconds=args.ttl_seconds, now_ns=now_ns)
        quarantined = apply_artifact_gc(plan, root=args.root) if args.apply else ()
        return {
            "dry_run": not args.apply,
            "plan_hash": plan.plan_hash,
            "candidates": [asdict(item) for item in plan.candidates],
            "candidate_bytes": sum(item.size_bytes for item in plan.candidates),
            "candidate_files": sum(item.file_count for item in plan.candidates),
            "protected": [asdict(item) for item in plan.protected],
            "quarantined": list(quarantined),
        }
    report = run_research_doctor(
        run_roots=tuple(args.run_root),
    )
    findings = [asdict(item) for item in report.findings]
    if report.status == "fail":
        raise DoctorFailedError(findings)
    return {"doctor_status": report.status, "findings": findings}


__all__ = ["execute"]
