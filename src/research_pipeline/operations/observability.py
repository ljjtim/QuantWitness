"""可关联元数据、结构化脱敏日志与可重建索引。"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.redaction import redact_text
from research_pipeline.runtime import EventStore


OBSERVABILITY_VERSION = "research-observability-v1"
METADATA_FIELDS = frozenset({
    "research.run.id", "research.run.parent_id", "research.plan.hash", "research.dag.id",
    "research.node.id", "research.attempt.id", "research.snapshot.hash", "research.artifact.id",
    "research.artifact.hash", "research.extension.id", "research.claim.hash",
    "research.result.id", "research.verification_result.id",
})
_DENY_KEY = re.compile(
    r"(password|passwd|secret|token|cookie|private.?key|pem|credential|database.?url|sql|env)",
    re.I,
)


def safe_metadata(values: Mapping[str, object]) -> Mapping[str, str | int | float | bool | None]:
    result: dict[str, str | int | float | bool | None] = {}
    for key, value in values.items():
        if key not in METADATA_FIELDS or _DENY_KEY.search(key):
            continue
        if value is None:
            result[key] = None
        elif type(value) is bool:
            result[key] = bool(value)
        elif type(value) is int:
            result[key] = int(value)
        elif type(value) is float:
            result[key] = float(value)
        elif isinstance(value, str):
            result[key] = redact_text(value)
    return MappingProxyType(result)


@dataclass(frozen=True)
class StructuredLogRecord:
    event_type: str
    status: str
    error_code: str | None
    metadata: Mapping[str, str | int | float | bool | None]
    resource_usage: Mapping[str, int]
    message: str
    record_hash: str


def build_structured_log(*, event_type: str, status: str, metadata: Mapping[str, object], message: str = "", error_code: str | None = None, resource_usage: Mapping[str, int] | None = None) -> StructuredLogRecord:
    safe = safe_metadata(metadata)
    resources = {key: value for key, value in sorted((resource_usage or {}).items()) if key in {"peak_rss_bytes", "peak_temp_bytes", "elapsed_ms", "cpu_ms"} and type(value) is int and value >= 0}
    safe_message = redact_text(message)
    payload = {"event_type": event_type, "status": status, "error_code": error_code, "metadata": dict(safe), "resource_usage": resources, "message": safe_message, "contract_version": OBSERVABILITY_VERSION}
    return StructuredLogRecord(event_type, status, error_code, safe, MappingProxyType(resources), safe_message, typed_canonical_hash(payload))


@dataclass(frozen=True)
class ObservabilityIndexRecord:
    record_type: str
    object_id: str
    metadata: Mapping[str, object]
    record_hash: str


def rebuild_observability_index(
    *,
    run_roots: tuple[str | Path, ...] = (),
) -> tuple[ObservabilityIndexRecord, ...]:
    records: list[ObservabilityIndexRecord] = []
    for run_root in sorted(Path(path).resolve() for path in run_roots):
        record = json.loads(
            (run_root / "operator-dag-run.json").read_text(encoding="utf-8")
        )
        projection = EventStore(run_root).replay()
        metadata = {"run_id": record["run_id"], "parent_run_id": record.get("parent_run_id"), "dag_id": record["dag_id"], "status": projection.run_status, "event_chain_head": projection.chain_head, "nodes": sorted(projection.node_statuses)}
        records.append(_index_record("run", str(record["run_id"]), metadata))
    return tuple(sorted(records, key=lambda item: (item.record_type, item.object_id)))


def _index_record(record_type: str, object_id: str, metadata: Mapping[str, object]) -> ObservabilityIndexRecord:
    normalized = dict(sorted(metadata.items()))
    return ObservabilityIndexRecord(record_type, object_id, MappingProxyType(normalized), typed_canonical_hash({"record_type": record_type, "object_id": object_id, "metadata": normalized, "contract_version": OBSERVABILITY_VERSION}))


__all__ = ["METADATA_FIELDS", "OBSERVABILITY_VERSION", "ObservabilityIndexRecord", "StructuredLogRecord", "build_structured_log", "rebuild_observability_index", "redact_text", "safe_metadata"]
