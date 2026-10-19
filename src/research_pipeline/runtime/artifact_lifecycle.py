"""成功节点交接后的载荷释放；正式工件和失败恢复材料保持原位。"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Mapping

from research_pipeline.platform import canonical_json

PROTECTION_MARKERS = (".pin", ".lease", ".active")
_CURRENT_OWNER: ContextVar = ContextVar("artifact_lifecycle_owner", default=None)


def external_staging_started(staging: Path) -> None:
    """由 ExternalArtifactStore.prepare 在创建暂存后调用。"""
    owner = _CURRENT_OWNER.get()
    if owner is not None:
        try:
            owner._start_external_staging(Path(staging))
        except Exception as exc:
            owner._record_staging_error(exc)


def external_staging_finished(staging: Path) -> None:
    """由 ExternalArtifactStore.commit 的 finally 调用。"""
    owner = _CURRENT_OWNER.get()
    if owner is not None:
        owner._safe_finish_external_staging(Path(staging))


def protection_reasons(path: Path, boundary: Path) -> tuple[str, ...]:
    """保护标记对其所在目录及后代生效。"""
    path, boundary = path.resolve(), boundary.resolve()
    if not path.is_relative_to(boundary):
        return ("path_outside_owner",)
    reasons = []
    for parent in (path, *path.parents):
        for marker in PROTECTION_MARKERS:
            if (parent / marker).exists():
                reasons.append(f"{parent.relative_to(boundary).as_posix()}/{marker}")
        if parent == boundary:
            break
    if path.is_dir():
        for child in path.rglob("*"):
            if child.name in PROTECTION_MARKERS:
                reasons.append(child.relative_to(boundary).as_posix())
    return tuple(sorted(set(reasons)))


@dataclass(frozen=True)
class PayloadHandoff:
    category: str
    source: Path
    commit: object
    files: Mapping[str, str]
    worker_exited: bool


class AttemptArtifactLifecycle:
    """只登记当前调用完成的复制；不从旧目录名称推断交接成功。"""

    def __init__(self, run_root: Path, node_id: str, attempt_id: str, external):
        self.root = run_root.resolve()
        self.node_id = node_id
        self.attempt_id = attempt_id
        self.external = external
        self.handoffs: list[PayloadHandoff] = []
        self.resources_accepted = False
        self._external_staging: set[Path] = set()
        self.staging_errors: list[str] = []

    @contextmanager
    def external_staging_scope(self):
        token = _CURRENT_OWNER.set(self)
        try:
            yield
        finally:
            try:
                for staging in tuple(self._external_staging):
                    self._safe_finish_external_staging(staging)
            finally:
                _CURRENT_OWNER.reset(token)

    def _record_staging_error(self, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"
        self.staging_errors.append(message)
        logging.getLogger(__name__).warning("暂存生命周期登记未完成，保留保护: %s", message)

    def _safe_finish_external_staging(self, staging: Path) -> None:
        try:
            self._finish_external_staging(staging)
        except Exception as exc:
            self._record_staging_error(exc)

    def _owner_directory(self, staging: Path) -> Path:
        staging = staging.resolve()
        if staging.parent != (self.external.root / ".staging").resolve():
            raise ValueError("external staging 不属于当前 store")
        return self.external.root / ".staging-owners" / staging.name

    def _start_external_staging(self, staging: Path) -> None:
        if staging.resolve() in self._external_staging:
            return
        owner_dir = self._owner_directory(staging)
        owner_dir.mkdir(parents=True, exist_ok=False)
        payload = {"contract_version": "research-external-staging-owner-v1",
                   "run_root": str(self.root), "node_id": self.node_id,
                   "attempt_id": self.attempt_id, "stage_name": staging.name,
                   "status": "active"}
        (owner_dir / "owner.json").write_text(canonical_json(payload), encoding="utf-8")
        (owner_dir / ".active").touch()
        self._external_staging.add(staging.resolve())

    def _finish_external_staging(self, staging: Path) -> None:
        staging = staging.resolve()
        if staging not in self._external_staging:
            return
        owner_dir = self._owner_directory(staging)
        if staging.exists():
            path = owner_dir / "owner.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["status"] = "stopped"
            temporary = owner_dir / "owner.tmp"
            temporary.write_text(canonical_json(payload), encoding="utf-8")
            temporary.replace(path)
        else:
            (owner_dir / "owner.json").unlink(missing_ok=True)
        (owner_dir / ".active").unlink(missing_ok=True)
        if not staging.exists():
            owner_dir.rmdir()
        self._external_staging.discard(staging)

    def register(self, *, category: str, source: Path, commit,
                 files: Mapping[str, str], worker_exited: bool = True) -> None:
        source = source.resolve()
        if category == "worker_output":
            boundary = self.root / "work" / self.attempt_id
            if not source.is_relative_to(boundary):
                raise ValueError("Worker 释放载荷不属于当前 attempt")
            if "outputs" not in source.relative_to(boundary).parts:
                raise ValueError("Worker 释放范围必须是 outputs 载荷")
        elif category == "request_recovery":
            if source != self.root / "request-recovery" / self.node_id / "data":
                raise ValueError("请求恢复释放范围必须是当前节点的 data")
        else:
            raise ValueError("未知成功副本类别")
        self.handoffs.append(PayloadHandoff(category, source, commit, dict(files), worker_exited))

    def register_worker(self, output, commit, *, worker_exited: bool) -> None:
        source = output.path.resolve()
        if source.is_dir():
            prefix = "" if output.commit.get("publish_at_artifact_root", False) else output.port + "/"
            files = {p.relative_to(source).as_posix(): prefix + p.relative_to(source).as_posix()
                     for p in source.rglob("*") if p.is_file()}
        else:
            destinations = tuple(commit.files)
            if len(destinations) != 1:
                return
            files = {"": destinations[0]}
        self.register(category="worker_output", source=source, commit=commit,
                      files=files, worker_exited=worker_exited)

    def register_request_data(self, outputs) -> None:
        source = self.root / "request-recovery" / self.node_id / "data"
        if not source.is_dir():
            return
        for value in outputs.values.values():
            commit = value.external_commit
            if commit is None or commit.artifact_type != "data.columnar-bundle.v1":
                continue
            target = self.external.objects_root / commit.semantic_hash
            if "result.json" not in commit.files:
                continue
            payload = json.loads((target / "result.json").read_text(encoding="utf-8"))
            if payload.get("status") != "data_succeeded":
                continue
            files = {p.relative_to(source).as_posix(): "data/" + p.relative_to(source).as_posix()
                     for p in source.rglob("*") if p.is_file()}
            self.register(category="request_recovery", source=source, commit=commit, files=files)
            return

    def release(self, *, checkpoint_committed: bool) -> dict[str, object]:
        """清理失败只形成诊断，不改变已提交节点的成功状态。"""
        from .store import EventStore

        report: dict[str, object] = {
            "node_id": self.node_id, "attempt_id": self.attempt_id,
            "released_files": 0, "released_bytes": 0, "items": [],
            "staging_errors": self.staging_errors,
        }
        if not self.handoffs and not self.staging_errors:
            return report
        try:
            projection = EventStore(self.root).replay()
            ready = (checkpoint_committed and self.resources_accepted
                     and projection.node_statuses.get(self.node_id) == "succeeded"
                     and projection.attempt_statuses.get(self.attempt_id) == "succeeded")
            for handoff in self.handoffs:
                item = self._release_handoff(handoff, ready=ready)
                report["items"].append(item)
                report["released_files"] += item["released_files"]
                report["released_bytes"] += item["released_bytes"]
        except Exception as exc:
            report["error"] = f"{type(exc).__name__}: {exc}"
        if self.handoffs or self.staging_errors:
            path = self.root / "work" / self.attempt_id / "artifact-lifecycle.json"
            try:
                path.write_text(canonical_json(report), encoding="utf-8")
            except OSError as exc:
                logging.getLogger(__name__).warning("载荷释放记录无法保存: %s", exc)
        return report

    def _release_handoff(self, handoff: PayloadHandoff, *, ready: bool) -> dict[str, object]:
        source = handoff.source
        item = {"category": handoff.category, "source": source.relative_to(self.root).as_posix(),
                "replacement": str(self.external.objects_root / handoff.commit.semantic_hash),
                "released_files": 0, "released_bytes": 0, "protection_reasons": []}
        reasons = item["protection_reasons"]
        if not ready:
            reasons.append("node_checkpoint_or_resource_not_accepted")
        if not handoff.worker_exited:
            reasons.append("worker_not_exited")
        reasons.extend(protection_reasons(source, self.root))
        if reasons or not source.exists():
            return item
        try:
            commit = self.external.verify(handoff.commit.semantic_hash)
            if commit != handoff.commit:
                raise ValueError("正式替代工件与交接引用不一致")
            target = (self.external.objects_root / commit.semantic_hash).resolve()
            sources = {p.relative_to(source).as_posix(): p for p in source.rglob("*") if p.is_file()} if source.is_dir() else {"": source}
            if not sources or set(sources) != set(handoff.files):
                reasons.append("source_file_set_changed")
                return item
            sizes = {}
            for relative, path in sources.items():
                replacement_relative = handoff.files[relative]
                replacement = (target / replacement_relative).resolve()
                if (not path.resolve().is_relative_to(source if source.is_dir() else source.parent)
                    or not replacement.is_relative_to(target)
                    or replacement_relative not in commit.files
                    or not replacement.is_file()
                    or replacement.stat().st_size != path.stat().st_size):
                    reasons.append("replacement_not_closed")
                    return item
                sizes[relative] = path.stat().st_size
            # 仅删除本次登记且已经逐项找到正式替代位置的载荷文件。
            for relative, path in sources.items():
                path.unlink()
                item["released_files"] += 1
                item["released_bytes"] += sizes[relative]
        except Exception as exc:
            item["error"] = f"{type(exc).__name__}: {exc}"
        return item
