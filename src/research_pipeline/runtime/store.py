"""追加事件日志和可丢弃状态投影。"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path
from typing import Callable
import uuid

import psutil
from research_pipeline.platform.canonical import canonical_json

from .errors import RuntimeIntegrityError, RuntimeStateError
from .events import RuntimeEvent
from .state import RuntimeProjection, _ProjectionAccumulator


def process_identity_alive(pid: int, process_started_at: float) -> bool:
    try:
        process = psutil.Process(pid)
        return process.is_running() and abs(process.create_time() - process_started_at) < 1e-6
    except (psutil.Error, OSError, ValueError):
        return False


class _StoreLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.token = str(uuid.uuid4())

    def __enter__(self) -> _StoreLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json({
            "pid": os.getpid(), "token": self.token,
            "process_started_at": psutil.Process().create_time(),
        })
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                return self
            except FileExistsError:
                try:
                    owner = json.loads(self.path.read_text(encoding="utf-8"))
                    started_at = owner.get("process_started_at")
                    alive = (
                        psutil.pid_exists(int(owner["pid"])) if started_at is None
                        else process_identity_alive(int(owner["pid"]), float(started_at))
                    )
                    if alive:
                        raise RuntimeStateError("事件库已有活动写者")
                    self.path.unlink()
                except RuntimeStateError:
                    raise
                except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                    raise RuntimeStateError("事件库锁损坏，拒绝自动回收") from exc
        raise RuntimeStateError("无法取得事件库写锁")

    def __exit__(self, *_args: object) -> None:
        try:
            owner = json.loads(self.path.read_text(encoding="utf-8"))
            if owner.get("token") == self.token:
                self.path.unlink()
        except FileNotFoundError:
            return


class _EventSession:
    def __init__(self, events: tuple[RuntimeEvent, ...]) -> None:
        self.state = _ProjectionAccumulator()
        self.events: list[RuntimeEvent] = []
        self.by_command: dict[str, RuntimeEvent] = {}
        self.by_node: dict[str, list[RuntimeEvent]] = {}
        for event in events:
            self.accept(event, self.state.validate(event))

    def accept(self, event: RuntimeEvent, target: str) -> None:
        self.state.accept(event, target)
        self.events.append(event)
        self.by_command.setdefault(event.command_id, event)
        if event.node_id is not None:
            self.by_node.setdefault(event.node_id, []).append(event)


def _copy_event(event: RuntimeEvent) -> RuntimeEvent:
    return replace(event, payload=deepcopy(event.payload))


class EventStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.events_path = self.root / "events.jsonl"
        self.projection_path = self.root / "projection.json"
        self.lock_path = self.root / ".writer.lock"
        self._writer_active = False
        self._append_active = False
        self._session: _EventSession | None = None

    @contextmanager
    def writer_session(self):
        """独占写者期间只重建一次；退出后不保留日志信任。"""
        if self._writer_active:
            raise RuntimeStateError("事件写者会话不能嵌套")
        with _StoreLock(self.lock_path):
            self._writer_active = True
            try:
                self._current_session()
                yield self
            finally:
                self._session = None
                self._writer_active = False

    def _current_session(self) -> _EventSession:
        if self._session is None:
            self._session = _EventSession(self._read_events())
        return self._session

    def _read_events(self) -> tuple[RuntimeEvent, ...]:
        if not self.events_path.exists():
            return ()
        events: list[RuntimeEvent] = []
        try:
            with self.events_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.endswith("\n"):
                        raise RuntimeIntegrityError(f"事件日志尾部截断: line {line_number}")
                    events.append(RuntimeEvent.from_dict(json.loads(line)))
        except RuntimeIntegrityError:
            raise
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("事件日志损坏或截断") from exc
        return tuple(events)

    def read_events(self) -> tuple[RuntimeEvent, ...]:
        if self._writer_active:
            return tuple(_copy_event(event) for event in self._current_session().events)
        return self._read_events()

    @property
    def event_count(self) -> int:
        if self._writer_active:
            return self._current_session().state.last_seq
        return self.replay().last_seq

    def events_for_node(self, node_id: str) -> tuple[RuntimeEvent, ...]:
        if self._writer_active:
            return tuple(_copy_event(event) for event in self._current_session().by_node.get(node_id, ()))
        return tuple(event for event in self._read_events() if event.node_id == node_id)

    def replay(self) -> RuntimeProjection:
        session = self._current_session() if self._writer_active else _EventSession(self._read_events())
        return session.state.snapshot()

    def append(self, run_id: str, kind: str, payload: dict[str, object], *, command_id: str, node_id: str | None = None, attempt_id: str | None = None, before_append: Callable[[RuntimeEvent], None] | None = None) -> RuntimeEvent:
        if self._append_active:
            raise RuntimeStateError("事件追加不能重入当前写者")
        if not self._writer_active:
            with self.writer_session():
                return self.append(run_id, kind, payload, command_id=command_id,
                                   node_id=node_id, attempt_id=attempt_id, before_append=before_append)
        self._append_active = True
        try:
            session = self._current_session()
            existing = session.by_command.get(command_id)
            if existing is not None:
                if (existing.run_id, existing.kind, existing.payload, existing.node_id, existing.attempt_id) != (run_id, kind, payload, node_id, attempt_id):
                    raise RuntimeStateError("command_id 已用于不同事件")
                return _copy_event(existing)
            event = RuntimeEvent.build(
                session.state.last_seq + 1, run_id, kind, deepcopy(payload),
                previous_hash=session.state.chain_head, command_id=command_id,
                node_id=node_id, attempt_id=attempt_id,
            )
            target = session.state.validate(event)
            if before_append is not None:
                before_append(_copy_event(event))
            try:
                with self.events_path.open("a", encoding="utf-8", newline="") as handle:
                    handle.write(canonical_json(event.to_dict()) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                session.accept(event, target)
                temporary = self.projection_path.with_suffix(".tmp")
                temporary.write_text(canonical_json(session.state.snapshot().to_dict()), encoding="utf-8")
                os.replace(temporary, self.projection_path)
            except BaseException:
                # 日志写入是否完成以再次重放为准，不沿用不确定的内存状态。
                self._session = None
                raise
            return _copy_event(event)
        finally:
            self._append_active = False


__all__ = ["EventStore"]
