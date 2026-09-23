"""只读诊断与默认 dry-run 的研究工件回收命令。"""

from __future__ import annotations

from dataclasses import asdict
import time

from research_pipeline.operations import (
    apply_artifact_gc,
    plan_artifact_gc,
    run_research_doctor,
)

from ..result import execute_guarded


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
            "quarantined": list(quarantined),
        }
    report = run_research_doctor(
        run_roots=tuple(args.run_root),
    )
    if report.status == "fail":
        raise ValueError(f"research doctor 未通过: {[item.code for item in report.findings]}")
    return {"doctor_status": report.status, "findings": [asdict(item) for item in report.findings]}


__all__ = ["execute"]
