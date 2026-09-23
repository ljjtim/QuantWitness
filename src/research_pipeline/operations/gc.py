"""默认 dry-run、先 quarantine 的研究工件 GC。"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import uuid

from research_pipeline.platform.canonical import typed_canonical_hash


class ArtifactGcError(ValueError):
    pass


@dataclass(frozen=True)
class GcCandidate:
    category: str
    relative_path: str
    age_ns: int


@dataclass(frozen=True)
class ArtifactGcPlan:
    root: str
    ttl_seconds: int
    created_at_ns: int
    candidates: tuple[GcCandidate, ...]
    plan_hash: str


def plan_artifact_gc(root: str | Path, *, ttl_seconds: int, now_ns: int) -> ArtifactGcPlan:
    base = Path(root).resolve()
    if not base.is_dir() or type(ttl_seconds) is not int or ttl_seconds < 0 or type(now_ns) is not int:
        raise ArtifactGcError("gc 必须提供有效根目录、TTL 和时钟")
    candidates: list[GcCandidate] = []
    for category in ("staging", "cache"):
        category_root = base / category
        if not category_root.exists():
            continue
        if not category_root.is_dir():
            raise ArtifactGcError("gc 已知类别必须是目录")
        for path in sorted(category_root.iterdir()):
            resolved = path.resolve()
            try:
                resolved.relative_to(category_root.resolve())
            except ValueError as exc:
                raise ArtifactGcError("gc candidate 路径逃逸") from exc
            if not path.is_dir():
                raise ArtifactGcError("gc 拒绝不明文件")
            if any(
                (path / marker).exists()
                for marker in ("COMMITTED", ".pin", ".lease", ".active")
            ):
                continue
            age_ns = now_ns - path.stat().st_mtime_ns
            if age_ns >= ttl_seconds * 1_000_000_000:
                candidates.append(GcCandidate(category, path.relative_to(base).as_posix(), age_ns))
    normalized = tuple(sorted(candidates, key=lambda item: (item.category, item.relative_path)))
    payload = {"root": str(base), "ttl_seconds": ttl_seconds, "created_at_ns": now_ns, "candidates": [item.__dict__ for item in normalized]}
    return ArtifactGcPlan(
        str(base), ttl_seconds, now_ns, normalized, typed_canonical_hash(payload)
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
            category_root = (base / candidate.category).resolve()
            try:
                source.relative_to(category_root)
            except ValueError as exc:
                raise ArtifactGcError("gc apply candidate 路径逃逸") from exc
            if not source.is_dir() or any(
                (source / marker).exists()
                for marker in ("COMMITTED", ".pin", ".lease", ".active")
            ):
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
        if not path.is_dir() or not (path.name.startswith("staging-") or path.name.startswith("cache-")):
            raise ArtifactGcError("quarantine 含不明对象")
        if any(
            (path / marker).exists()
            for marker in ("COMMITTED", ".pin", ".lease", ".active")
        ):
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
