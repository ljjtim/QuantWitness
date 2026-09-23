"""追加事件日志和可丢弃状态投影。"""

from __future__ import annotations

import json
import os
from pathlib import Path
import uuid

import psutil
from research_pipeline.platform.canonical import canonical_json

from .errors import RuntimeIntegrityError, RuntimeStateError
from .events import RuntimeEvent
from .state import RuntimeProjection, apply_event


class _StoreLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.token = str(uuid.uuid4())

    def __enter__(self) -> _StoreLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json({"pid": os.getpid(), "token": self.token})
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
                    if psutil.pid_exists(int(owner["pid"])):
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


class EventStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.events_path = self.root / "events.jsonl"
        self.projection_path = self.root / "projection.json"
        self.lock_path = self.root / ".writer.lock"

    def read_events(self) -> tuple[RuntimeEvent, ...]:
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

    def replay(self) -> RuntimeProjection:
        projection = RuntimeProjection()
        for event in self.read_events():
            projection = apply_event(projection, event)
        return projection

    def append(self, run_id: str, kind: str, payload: dict[str, object], *, command_id: str, node_id: str | None = None, attempt_id: str | None = None) -> RuntimeEvent:
        with _StoreLock(self.lock_path):
            events = self.read_events()
            for event in events:
                if event.command_id == command_id:
                    if (event.kind, event.payload, event.node_id, event.attempt_id) != (kind, payload, node_id, attempt_id):
                        raise RuntimeStateError("command_id 已用于不同事件")
                    return event
            projection = RuntimeProjection()
            for existing in events:
                projection = apply_event(projection, existing)
            event = RuntimeEvent.build(projection.last_seq + 1, run_id, kind, payload, previous_hash=projection.chain_head, command_id=command_id, node_id=node_id, attempt_id=attempt_id)
            updated = apply_event(projection, event)
            with self.events_path.open("a", encoding="utf-8", newline="") as handle:
                handle.write(canonical_json(event.to_dict()) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            temporary = self.projection_path.with_suffix(".tmp")
            temporary.write_text(canonical_json(updated.to_dict()), encoding="utf-8")
            os.replace(temporary, self.projection_path)
            return event


__all__ = ["EventStore"]
