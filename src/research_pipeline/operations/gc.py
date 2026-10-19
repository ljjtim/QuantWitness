"""默认 dry-run、先 quarantine 的研究工件 GC。"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import uuid

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.runtime.artifact_lifecycle import protection_reasons


class ArtifactGcError(ValueError):
    pass


@dataclass(frozen=True)
class GcCandidate:
    category: str
    relative_path: str
    age_ns: int
    size_bytes: int = 0
    file_count: int = 0


@dataclass(frozen=True)
class GcProtection:
    category: str
    relative_path: str
    size_bytes: int
    file_count: int
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ArtifactGcPlan:
    root: str
    ttl_seconds: int
    created_at_ns: int
    candidates: tuple[GcCandidate, ...]
    plan_hash: str
    protected: tuple[GcProtection, ...] = ()


def _tree_size(path: Path) -> tuple[int, int]:
    size_bytes = file_count = 0
    for item in path.rglob("*"):
        if item.is_file():
            size_bytes += item.stat().st_size
            file_count += 1
    return size_bytes, file_count


def _candidate_protection(base: Path, path: Path, category: str) -> tuple[str, ...]:
    reasons = list(protection_reasons(path, base))
    if (path / "COMMITTED").exists():
        reasons.append("committed_artifact")
    attempt_id = path.name
    if category == "external_staging":
        owner_dir = path.parent.parent / ".staging-owners" / path.name
        try:
            payload = json.loads((owner_dir / "owner.json").read_text(encoding="utf-8"))
            if (payload.get("contract_version") != "research-external-staging-owner-v1"
                or payload.get("run_root") != str(base)
                or payload.get("stage_name") != path.name
                or not payload.get("node_id") or not payload.get("attempt_id")):
                reasons.append("staging_owner_not_bound")
            attempt_id = payload.get("attempt_id")
            if payload.get("status") != "stopped" or (owner_dir / ".active").exists():
                reasons.append("external_producer_active")
        except (OSError, ValueError, TypeError, AttributeError):
            reasons.append("producer_activity_and_terminal_owner_evidence_required")
        if not (base / "runtime-liveness.json").exists():
            reasons.append("runtime_owner_not_proven_stopped")
    if (base / "runtime-liveness.json").exists() or (base / "events.jsonl").exists():
        from research_pipeline.runtime.liveness import read_runtime_liveness, runtime_owner_alive
        from research_pipeline.runtime import EventStore

        try:
            owner = read_runtime_liveness(base)
            if runtime_owner_alive(owner):
                reasons.append("runtime_owner_active")
            elif owner is None or owner.get("phase") != "stopped":
                reasons.append("runtime_owner_not_proven_stopped")
            if category in {"staging", "external_staging"}:
                status = EventStore(base).replay().attempt_statuses.get(attempt_id)
                if status not in {"failed", "cancelled", "succeeded"}:
                    reasons.append("attempt_not_terminal")
        except (OSError, ValueError, RuntimeError):
            reasons.append("owner_or_attempt_evidence_unavailable")
    return tuple(reasons)


def plan_artifact_gc(root: str | Path, *, ttl_seconds: int, now_ns: int) -> ArtifactGcPlan:
    base = Path(root).resolve()
    if not base.is_dir() or type(ttl_seconds) is not int or ttl_seconds < 0 or type(now_ns) is not int:
        raise ArtifactGcError("gc 必须提供有效根目录、TTL 和时钟")
    candidates: list[GcCandidate] = []
    protected: list[GcProtection] = []
    for category in ("staging", "cache", "external_staging"):
        category_root = base / ("external-artifacts/.staging" if category == "external_staging" else category)
        if not category_root.exists():
            continue
        if not category_root.is_dir():
            raise ArtifactGcError("gc 已知类别必须是目录")
        for path in sorted(category_root.iterdir()):
            resolved = path.resolve()
            if not resolved.is_relative_to(category_root.resolve()):
                raise ArtifactGcError("gc candidate 路径逃逸")
            if not path.is_dir():
                raise ArtifactGcError("gc 拒绝不明文件")
            age_ns = now_ns - path.stat().st_mtime_ns
            size_bytes, file_count = _tree_size(path)
            reasons = list(_candidate_protection(base, path, category))
            if age_ns < ttl_seconds * 1_000_000_000:
                reasons.append("diagnostic_retention_ttl")
            if reasons:
                protected.append(GcProtection(category, path.relative_to(base).as_posix(), size_bytes, file_count, tuple(reasons)))
            else:
                candidates.append(GcCandidate(category, path.relative_to(base).as_posix(), age_ns, size_bytes, file_count))
    # 未接入 owner 生命周期的生产者只展示保护原因，不扩大可执行回收范围。
    retained = {
        "request-recovery": "partial_request_or_successful_handoff_required",
        "work": "attempt_receipts_and_unproven_payloads",
        "input-artifacts": "frozen_input_reference",
        "external-artifacts/objects": "formal_external_and_recovery_reference",
        "checkpoints": "formal_checkpoint_and_reuse_reference",
        "results": "self_contained_result",
        "verifier": "independent_verifier",
        "holdout": "persistent_holdout_ledger",
    }
    for relative, reason in retained.items():
        path = base / relative
        if path.is_dir():
            size_bytes, file_count = _tree_size(path)
            protected.append(GcProtection(relative, relative, size_bytes, file_count,
                                           (reason, *protection_reasons(path, base))))
    staging_roots = [base / ".staging"]
    staging_roots.extend(path for path in base.iterdir() if path.name.startswith(".staging-") and path.is_dir())
    for staging_root in staging_roots:
        if not staging_root.is_dir():
            continue
        paths = (staging_root,) if staging_root.name.startswith(".staging-") else sorted(staging_root.iterdir())
        for path in paths:
            if not path.is_dir():
                continue
            size_bytes, file_count = _tree_size(path)
            protected.append(GcProtection("unowned_staging", path.relative_to(base).as_posix(), size_bytes, file_count,
                ("producer_activity_and_terminal_owner_evidence_required", *protection_reasons(path, base))))
    normalized = tuple(sorted(candidates, key=lambda item: (item.category, item.relative_path)))
    payload = {"root": str(base), "ttl_seconds": ttl_seconds, "created_at_ns": now_ns, "candidates": [item.__dict__ for item in normalized]}
    return ArtifactGcPlan(
        str(base), ttl_seconds, now_ns, normalized, typed_canonical_hash(payload),
        tuple(sorted(protected, key=lambda item: (item.category, item.relative_path))),
    )


def apply_artifact_gc(plan: ArtifactGcPlan, *, root: str | Path) -> tuple[str, ...]:
    base = Path(root).resolve()
    if str(base) != plan.root:
        raise ArtifactGcError("gc apply 根目录与计划不一致")
    expected_hash = typed_canonical_hash({"root": plan.root, "ttl_seconds": plan.ttl_seconds, "created_at_ns": plan.created_at_ns, "candidates": [item.__dict__ for item in plan.candidates]})
    if plan.plan_hash != expected_hash:
        raise ArtifactGcError("gc plan hash 不一致")
    lock = base / ".gc.lock"
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise ArtifactGcError("gc 已有活动执行者") from exc
    quarantined: list[str] = []
    try:
        os.close(descriptor)
        quarantine_root = base / "quarantine"
        quarantine_root.mkdir(exist_ok=True)
        for candidate in plan.candidates:
            source = (base / candidate.relative_path).resolve()
            if candidate.category not in {"staging", "cache", "external_staging"}:
                raise ArtifactGcError("gc apply 未知类别")
            category_root = (base / ("external-artifacts/.staging" if candidate.category == "external_staging" else candidate.category)).resolve()
            try:
                source.relative_to(category_root)
            except ValueError as exc:
                raise ArtifactGcError("gc apply candidate 路径逃逸") from exc
            if source == category_root or not source.is_dir() or _candidate_protection(base, source, candidate.category):
                raise ArtifactGcError("gc candidate 已变化或受到保护")
            target = quarantine_root / f"{candidate.category}-{source.name}-{uuid.uuid4().hex}"
            try:
                source.replace(target)
            except OSError as exc:
                raise ArtifactGcError(
                    "gc_candidate_busy：candidate 被占用或无法 quarantine"
                ) from exc
            quarantined.append(target.relative_to(base).as_posix())
    finally:
        try:
            lock.unlink()
        except FileNotFoundError:
            pass
    return tuple(quarantined)


def purge_artifact_quarantine(
    root: str | Path,
    *,
    ttl_seconds: int,
    now_ns: int,
    apply: bool = False,
) -> tuple[str, ...]:
    """默认只列出过期 quarantine；显式 apply 才删除。"""

    base = Path(root).resolve()
    quarantine = base / "quarantine"
    if not quarantine.exists():
        return ()
    if not quarantine.is_dir() or type(ttl_seconds) is not int or ttl_seconds < 0 or type(now_ns) is not int:
        raise ArtifactGcError("quarantine purge 参数无效")
    candidates: list[Path] = []
    for path in sorted(quarantine.iterdir()):
        resolved = path.resolve()
        try:
            resolved.relative_to(quarantine.resolve())
        except ValueError as exc:
            raise ArtifactGcError("quarantine 路径逃逸") from exc
        if not path.is_dir() or not path.name.startswith(("staging-", "cache-", "external_staging-")):
            raise ArtifactGcError("quarantine 含不明对象")
        if (path / "COMMITTED").exists() or protection_reasons(path, base):
            continue
        if now_ns - path.stat().st_mtime_ns >= ttl_seconds * 1_000_000_000:
            candidates.append(path)
    if apply:
        for path in candidates:
            shutil.rmtree(path)
    return tuple(path.relative_to(base).as_posix() for path in candidates)


__all__ = [
    "ArtifactGcError",
    "ArtifactGcPlan",
    "GcCandidate",
    "apply_artifact_gc",
    "plan_artifact_gc",
    "purge_artifact_quarantine",
]
