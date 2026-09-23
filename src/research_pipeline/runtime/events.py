"""追加事件的版本化 codec 与 hash chain。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import uuid

from research_pipeline.platform.canonical import typed_canonical_hash

from .contracts import _require_fields
from .errors import RuntimeIntegrityError


EVENT_VERSION = "research-runtime-event-v1"


@dataclass(frozen=True)
class RuntimeEvent:
    seq: int
    event_id: str
    run_id: str
    kind: str
    payload: dict[str, object]
    previous_hash: str
    event_hash: str
    occurred_at: str
    command_id: str
    node_id: str | None = None
    attempt_id: str | None = None
    contract_version: str = EVENT_VERSION

    @classmethod
    def build(cls, seq: int, run_id: str, kind: str, payload: dict[str, object], *, previous_hash: str, command_id: str, node_id: str | None = None, attempt_id: str | None = None, occurred_at: str | None = None, event_id: str | None = None) -> RuntimeEvent:
        timestamp = occurred_at or datetime.now(timezone.utc).isoformat()
        identifier = event_id or str(uuid.uuid4())
        base = {
            "seq": seq, "event_id": identifier, "run_id": run_id, "kind": kind,
            "payload": payload, "previous_hash": previous_hash, "occurred_at": timestamp,
            "command_id": command_id, "node_id": node_id, "attempt_id": attempt_id,
            "contract_version": EVENT_VERSION,
        }
        return cls(event_hash=typed_canonical_hash(base), **base)

    def identity_payload(self) -> dict[str, object]:
        return {
            "seq": self.seq, "event_id": self.event_id, "run_id": self.run_id,
            "kind": self.kind, "payload": self.payload, "previous_hash": self.previous_hash,
            "occurred_at": self.occurred_at, "command_id": self.command_id,
            "node_id": self.node_id, "attempt_id": self.attempt_id,
            "contract_version": self.contract_version,
        }

    def verify(self) -> None:
        if self.contract_version != EVENT_VERSION or self.event_hash != typed_canonical_hash(self.identity_payload()):
            raise RuntimeIntegrityError("事件内容 hash 校验失败")

    def to_dict(self) -> dict[str, object]:
        return {**self.identity_payload(), "event_hash": self.event_hash}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RuntimeEvent:
        fields = {"seq", "event_id", "run_id", "kind", "payload", "previous_hash", "event_hash", "occurred_at", "command_id", "node_id", "attempt_id", "contract_version"}
        _require_fields(payload, fields, "RuntimeEvent")
        event = cls(
            int(payload["seq"]), str(payload["event_id"]), str(payload["run_id"]),
            str(payload["kind"]), dict(payload["payload"]), str(payload["previous_hash"]),
            str(payload["event_hash"]), str(payload["occurred_at"]), str(payload["command_id"]),
            None if payload["node_id"] is None else str(payload["node_id"]),
            None if payload["attempt_id"] is None else str(payload["attempt_id"]),
            str(payload["contract_version"]),
        )
        event.verify()
        return event


__all__ = ["EVENT_VERSION", "RuntimeEvent"]
