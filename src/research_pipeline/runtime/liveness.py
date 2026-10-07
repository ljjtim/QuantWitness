"""不参与研究身份的 Runtime 存活投影。"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
from typing import Mapping
import uuid

import psutil

from research_pipeline.platform import canonical_json

from .errors import RuntimeIntegrityError
from .store import _StoreLock, process_identity_alive


RUNTIME_LIVENESS_VERSION = "research-runtime-liveness-v1"
RUNTIME_LIVENESS_FILE = "runtime-liveness.json"
_PHASES = frozenset({
    "starting",
    "stopped",
    "waiting_for_resources",
    "executing",
    "checkpointing",
    "finalizing",
})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RuntimeLiveness:
    """原子维护当前进程、节点和阶段的轻量心跳。"""

    def __init__(
        self,
        run_root: str | Path,
        *,
        run_id: str | None,
        phase: str,
        node_id: str | None = None,
        attempt_id: str | None = None,
        requested_resources: Mapping[str, int] | None = None,
        reserved_resources: Mapping[str, int] | None = None,
        interval_seconds: float = 2.0,
    ) -> None:
        if phase not in _PHASES:
            raise ValueError("Runtime 存活阶段无效")
        self.path = Path(run_root).resolve() / RUNTIME_LIVENESS_FILE
        self.interval_seconds = float(interval_seconds)
        if self.interval_seconds <= 0:
            raise ValueError("Runtime 存活心跳间隔必须为正数")
        now = _utc_now()
        process = psutil.Process(os.getpid())
        self._payload: dict[str, object] = {
            "contract_version": RUNTIME_LIVENESS_VERSION,
            "run_id": run_id,
            "node_id": node_id,
            "attempt_id": attempt_id,
            "phase": phase,
            "pid": process.pid,
            "process_started_at": process.create_time(),
            "started_at": now,
            "phase_started_at": now,
            "heartbeat_at": now,
            "requested_resources": dict(requested_resources or {}),
            "reserved_resources": dict(reserved_resources or {}),
            "progress": None,
        }
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._active = False

    def start(self) -> "RuntimeLiveness":
        with _StoreLock(self.path.parent / ".owner.lock"):
            require_inactive_owner(self.path.parent)
            with self._lock:
                self._write_locked()
                self._active = True
        self._thread = threading.Thread(
            target=self._maintain,
            name="runtime-liveness-heartbeat",
            daemon=True,
        )
        try:
            self._thread.start()
        except BaseException:
            self._thread = None
            self.stop()
            raise
        return self

    def bind_run(self, run_root: str | Path, run_id: str) -> None:
        """同一次调用在准入完成后绑定正式运行身份。"""
        if Path(run_root).resolve() != self.path.parent or not self._active:
            raise RuntimeIntegrityError("Runtime owner 不属于当前活动调用")
        with self._lock:
            self._payload["run_id"] = run_id
            self._write_locked()

    def update(
        self,
        phase: str,
        *,
        reserved_resources: Mapping[str, int] | None = None,
    ) -> None:
        if phase not in _PHASES:
            raise ValueError("Runtime 存活阶段无效")
        with self._lock:
            if self._payload["phase"] != phase:
                self._payload["phase"] = phase
                self._payload["phase_started_at"] = _utc_now()
            if reserved_resources is not None:
                self._payload["reserved_resources"] = dict(reserved_resources)
            self._write_locked()

    def set_node(
        self, *, node_id: str, attempt_id: str,
        requested_resources: Mapping[str, int],
    ) -> None:
        with self._lock:
            self._payload.update(
                node_id=node_id, attempt_id=attempt_id,
                requested_resources=dict(requested_resources), reserved_resources={},
                phase="waiting_for_resources", phase_started_at=_utc_now(),
            )
            self._write_locked()

    def stop(self) -> None:
        if not self._active:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds * 2))
        with self._lock:
            if self._active:
                self._payload["phase"] = "stopped"
                self._payload["phase_started_at"] = _utc_now()
                self._write_locked()
                self._active = False

    def _maintain(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            with self._lock:
                self._write_locked()

    def _write_locked(self) -> None:
        self._payload["heartbeat_at"] = _utc_now()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        )
        temporary.write_text(canonical_json(self._payload), encoding="utf-8")
        os.replace(temporary, self.path)


def read_runtime_liveness(run_root: str | Path) -> dict[str, object] | None:
    """只读并校验存活投影；缺失表示没有可用心跳。"""

    path = Path(run_root).resolve() / RUNTIME_LIVENESS_FILE
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeIntegrityError("Runtime 存活投影无法读取") from exc
    expected = {
        "contract_version", "run_id", "node_id", "attempt_id", "phase",
        "pid", "process_started_at", "started_at", "phase_started_at",
        "heartbeat_at", "requested_resources", "reserved_resources", "progress",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise RuntimeIntegrityError("Runtime 存活投影 schema 无效")
    if (
        payload["contract_version"] != RUNTIME_LIVENESS_VERSION
        or payload["phase"] not in _PHASES
        or type(payload["pid"]) is not int
        or not isinstance(payload["process_started_at"], (int, float))
        or not isinstance(payload["requested_resources"], Mapping)
        or not isinstance(payload["reserved_resources"], Mapping)
        or payload["progress"] is not None
    ):
        raise RuntimeIntegrityError("Runtime 存活投影内容无效")
    return payload


def runtime_owner_alive(payload: Mapping[str, object] | None) -> bool:
    return bool(
        payload is not None and payload["phase"] != "stopped"
        and process_identity_alive(int(payload["pid"]), float(payload["process_started_at"]))
    )


def require_inactive_owner(run_root: str | Path) -> None:
    if runtime_owner_alive(read_runtime_liveness(run_root)):
        raise RuntimeIntegrityError("Runtime owner 仍存活，不能接管运行；请等待原进程退出")


__all__ = [
    "RUNTIME_LIVENESS_FILE",
    "RUNTIME_LIVENESS_VERSION",
    "RuntimeLiveness",
    "process_identity_alive",
    "read_runtime_liveness",
    "runtime_owner_alive",
    "require_inactive_owner",
]
