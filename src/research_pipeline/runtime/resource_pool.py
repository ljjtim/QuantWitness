"""显式共享资源池及跨进程租约。"""

from __future__ import annotations

from functools import cached_property

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from typing import Iterator, Mapping
import uuid
import weakref

import psutil

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.file_lock import ProcessFileLock
from research_pipeline.platform.redaction import redact_text

from .errors import RuntimeAdmissionError, RuntimeIntegrityError, RuntimeWorkerError
from .resource_governor import ResourceVector


RESOURCE_GOVERNOR_VERSION = "research-resource-governor-v4"

_LOCAL_STATE_GUARD = threading.RLock()
_LOCAL_STATE_LOCKS = weakref.WeakValueDictionary()
_MAINTAINED_LEASES: dict[tuple[str, str], tuple[ResourceLease, list[BaseException]]] = {}


@contextmanager
def _local_state_lock(path: Path):
    with _LOCAL_STATE_GUARD:
        lock = _LOCAL_STATE_LOCKS.setdefault(os.path.normcase(str(path)), threading.RLock())
    if not lock.acquire(timeout=10.0):
        raise RuntimeAdmissionError("资源治理进程内状态锁超时")
    try:
        yield
    finally:
        lock.release()

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ResourceGovernorConfig:
    state_dir: Path
    capacity: ResourceVector
    stale_after_seconds: float = 30.0
    poll_interval_seconds: float = 0.05

    def __post_init__(self) -> None:
        resolved = Path(self.state_dir).expanduser().resolve()
        timings = (self.stale_after_seconds, self.poll_interval_seconds)
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
            for value in timings
        ):
            raise RuntimeAdmissionError("资源治理 stale/poll 参数必须为正数")
        repositories = {
            item
            for item in (
                _find_repository_root(Path.cwd().resolve()),
                _find_repository_root(Path(__file__).resolve()),
            )
            if item is not None
        }
        if any(resolved == repository or repository in resolved.parents for repository in repositories):
            raise RuntimeAdmissionError("资源治理状态目录必须位于仓库外")
        object.__setattr__(self, "state_dir", resolved)
        object.__setattr__(self, "stale_after_seconds", float(self.stale_after_seconds))
        object.__setattr__(self, "poll_interval_seconds", float(self.poll_interval_seconds))

    @property
    def identity_payload(self) -> dict[str, object]:
        return {
            "contract_version": RESOURCE_GOVERNOR_VERSION,
            "state_dir": str(self.state_dir),
            "capacity": self.capacity.to_dict(),
            "stale_after_seconds": self.stale_after_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
        }

    @cached_property
    def identity_hash(self) -> str:
        return typed_canonical_hash(self.identity_payload)


@dataclass(frozen=True)
class ResourceLease:
    lease_id: str
    request_id: str
    owner_id: str
    vector: ResourceVector
    parent_lease_id: str | None
    pid: int
    process_started_at: float
    sequence: int


def read_resource_governor_state(
    config: ResourceGovernorConfig,
) -> dict[str, object] | None:
    """在状态事务锁内只读共享资源池，不清理租约或写回文件。"""

    reader = ResourceGovernor.__new__(ResourceGovernor)
    reader.config = config
    reader._state_path = config.state_dir / "resource-governor.json"
    if not reader._state_path.is_file():
        return None
    # Windows读取句柄与原子替换互斥；与写事务共用锁，不执行写回。
    with _local_state_lock(config.state_dir), ProcessFileLock(config.state_dir / "resource-governor.lock", timeout_seconds=10.0,
                         timeout_error=RuntimeAdmissionError("资源治理状态锁超时")):
        state = reader._read_state()
        reader._validate_or_initialize(state)
        return reader._snapshot_state(state)


class ResourceGovernor:
    """通过仓库外 JSON 状态和进程锁协调同机多个独立 CLI。"""

    def __init__(self, config: ResourceGovernorConfig) -> None:
        self.config = config
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self._state_path = self.config.state_dir / "resource-governor.json"
        self._lock_path = self.config.state_dir / "resource-governor.lock"
        with self._locked_state():
            pass

    @property
    def identity_hash(self) -> str:
        return self.config.identity_hash

    def acquire(
        self,
        *,
        owner_id: str,
        vector: ResourceVector,
        timeout_seconds: float,
        parent_lease_id: str | None = None,
    ) -> ResourceLease:
        if not owner_id or timeout_seconds <= 0:
            raise RuntimeAdmissionError("资源申请 owner 和 timeout 必须显式有效")
        request_id = uuid.uuid4().hex
        pid = os.getpid()
        process_started_at = psutil.Process(pid).create_time()
        deadline = time.monotonic() + timeout_seconds
        sequence: int | None = None
        while True:
            with self._locked_state() as state:
                self._reap_stale_in_state(state)
                requests = state["requests"]
                if request_id not in requests:
                    sequence = int(state["next_sequence"])
                    state["next_sequence"] = sequence + 1
                    requests[request_id] = {
                        "request_id": request_id,
                        "sequence": sequence,
                        "owner_id": owner_id,
                        "vector": vector.to_dict(),
                        "parent_lease_id": parent_lease_id,
                        "pid": pid,
                        "process_started_at": process_started_at,
                        "created_at": _utc_now(),
                    }
                request = requests[request_id]
                if self._is_fifo_head(state, request) and self._can_admit(state, request):
                    lease_id = typed_canonical_hash({
                        "governor": self.identity_hash,
                        "request_id": request_id,
                        "sequence": request["sequence"],
                        "owner_id": owner_id,
                        "pid": pid,
                        "process_started_at": process_started_at,
                    })
                    now = time.time()
                    state["leases"][lease_id] = {
                        **request,
                        "lease_id": lease_id,
                        "status": "active",
                        "acquired_at": _utc_now(),
                        "heartbeat_epoch": now,
                        "released_at": None,
                        "release_reason": None,
                    }
                    del requests[request_id]
                    return ResourceLease(
                        lease_id, request_id, owner_id, vector, parent_lease_id,
                        pid, process_started_at, int(request["sequence"]),
                    )
            if time.monotonic() >= deadline:
                with self._locked_state() as state:
                    state["requests"].pop(request_id, None)
                raise RuntimeAdmissionError(
                    f"资源申请超时，FIFO 序号 {sequence} 未获得额度"
                )
            time.sleep(self.config.poll_interval_seconds)

    def heartbeat(self, lease: ResourceLease) -> None:
        self.check_lease_health(lease)
        with self._locked_state() as state:
            record = state["leases"].get(lease.lease_id)
            if record is None or record.get("status") != "active":
                raise RuntimeAdmissionError("资源租约未知或已失效")
            if not _same_lease_identity(record, lease):
                raise RuntimeIntegrityError("资源租约进程身份不一致")
            now = time.time()
            while record is not None:
                if not _process_identity_alive(int(record["pid"]), float(record["process_started_at"])):
                    raise RuntimeAdmissionError("资源租约祖先进程已退出")
                record["heartbeat_epoch"] = now
                parent_id = record["parent_lease_id"]
                record = state["leases"].get(parent_id) if parent_id is not None else None

    def check_lease_health(self, lease: ResourceLease) -> None:
        """在计算交接和监督轮询时传播本进程已发现的租约故障。"""
        with _LOCAL_STATE_GUARD:
            current = lease
            while current is not None:
                entry = _MAINTAINED_LEASES.get((str(self.config.state_dir), current.lease_id))
                if entry is not None and entry[1]:
                    error = entry[1][0]
                    detail = redact_text(" ".join(str(error).split()))[:1000]
                    raise RuntimeWorkerError(
                        f"资源租约 heartbeat 失败: {type(error).__name__}: {detail}",
                        error_code="resource_lease_failed",
                    ) from error
                parent_id = current.parent_lease_id
                parent = _MAINTAINED_LEASES.get((str(self.config.state_dir), parent_id))
                current = parent[0] if parent is not None else None

    def release(self, lease: ResourceLease, *, reason: str = "released") -> bool:
        with self._locked_state() as state:
            record = state["leases"].get(lease.lease_id)
            if record is None:
                record = next((item for item in self._read_history(state)
                               if item["lease_id"] == lease.lease_id), None)
                if record is not None and not _same_lease_identity(record, lease):
                    raise RuntimeIntegrityError("资源租约释放身份不一致")
                return False
            if not _same_lease_identity(record, lease):
                raise RuntimeIntegrityError("资源租约释放身份不一致")
            if record.get("status") != "active":
                return False
            self._close_lease_tree(state, lease.lease_id, reason)
            return True

    def reap_stale(self) -> tuple[str, ...]:
        with self._locked_state() as state:
            return self._reap_stale_in_state(state)

    def snapshot(self) -> dict[str, object]:
        with self._locked_state() as state:
            self._reap_stale_in_state(state)
            return self._snapshot_state(state)

    def _snapshot_state(self, state: Mapping[str, object]) -> dict[str, object]:
        leases = {item["lease_id"]: item for item in self._read_history(state)}
        if set(leases).intersection(state["leases"]):
            raise RuntimeIntegrityError("资源租约同时存在于活动状态和已提交历史")
        leases.update(state["leases"])
        return {
            "contract_version": state["contract_version"],
            "governor_config_hash": state["governor_config_hash"],
            "governor_config_payload": dict(state["governor_config_payload"]),
            "capacity": dict(state["capacity"]),
            "next_sequence": state["next_sequence"],
            "requests": [dict(item) for item in sorted(
                state["requests"].values(), key=lambda value: value["sequence"]
            )],
            "leases": [dict(item) for item in sorted(
                leases.values(), key=lambda value: value["sequence"]
            )],
        }

    @contextmanager
    def maintained_lease(self, lease: ResourceLease) -> Iterator[ResourceLease]:
        stop = threading.Event()
        error: list[BaseException] = []
        key = (str(self.config.state_dir), lease.lease_id)
        with _LOCAL_STATE_GUARD:
            _MAINTAINED_LEASES[key] = (lease, error)

        def maintain() -> None:
            interval = min(self.config.stale_after_seconds / 3, 5.0)
            while not stop.wait(interval):
                try:
                    self.heartbeat(lease)
                except BaseException as exc:  # 后台失效必须由主线程看见。
                    with _LOCAL_STATE_GUARD:
                        error.append(exc)
                    logging.getLogger(__name__).error(
                        "资源租约续约失败: %s: %s", type(exc).__name__,
                        redact_text(" ".join(str(exc).split()))[:1000],
                    )
                    stop.set()

        thread = threading.Thread(target=maintain, name=f"resource-heartbeat-{lease.sequence}", daemon=True)
        thread.start()
        try:
            try:
                yield lease
            finally:
                stop.set()
                # 等待正在进行的有界本地锁与进程锁事务完成，再释放租约。
                thread.join(timeout=21.0)
                if thread.is_alive():
                    with _LOCAL_STATE_GUARD:
                        error.append(RuntimeIntegrityError("资源租约 heartbeat 线程未在事务限时内退出"))
        except BaseException as exc:
            if error:
                # 主执行异常仍为最终错误，心跳原错作为明确原因保留。
                raise exc from error[0]
            raise
        finally:
            with _LOCAL_STATE_GUARD:
                _MAINTAINED_LEASES.pop(key, None)
        if error:
            detail = redact_text(" ".join(str(error[0]).split()))[:1000]
            raise RuntimeIntegrityError(f"资源租约 heartbeat 失败: {type(error[0]).__name__}: {detail}") from error[0]

    @contextmanager
    def _locked_state(self) -> Iterator[dict[str, object]]:
        with _local_state_lock(self.config.state_dir), ProcessFileLock(self._lock_path, timeout_seconds=10.0, timeout_error=RuntimeAdmissionError("资源治理状态锁超时")):
            state = self._read_state()
            self._validate_or_initialize(state)
            yield state
            self._write_state(state)

    def _read_state(self) -> dict[str, object]:
        if not self._state_path.exists():
            return {}
        try:
            import json
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RuntimeIntegrityError("资源治理状态损坏；请保留目录并人工检查") from exc
        if not isinstance(payload, dict):
            raise RuntimeIntegrityError("资源治理状态不是对象")
        return payload

    def _write_state(self, state: dict[str, object]) -> None:
        ended = [item for item in state["leases"].values() if item["status"] != "active"]
        if ended:
            self._append_history(state, ended)
            state["leases"] = {key: item for key, item in state["leases"].items()
                               if item["status"] == "active"}
        temporary = self._state_path.with_name(f".{self._state_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8", newline="") as handle:
                handle.write(canonical_json(dict(state)))
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(self._state_path)
        finally:
            temporary.unlink(missing_ok=True)

    def _history_path(self) -> Path:
        return self.config.state_dir / "lease-history.jsonl"

    def _append_history(self, state: dict[str, object], records: list[dict[str, object]]) -> None:
        cursor = state["history"]
        path = self._history_path()
        committed = cursor["bytes"]
        if committed and not path.is_file():
            raise RuntimeIntegrityError("资源治理已提交冷历史缺失")
        mode = "r+b" if path.exists() else "w+b"
        with path.open(mode) as handle:
            if os.fstat(handle.fileno()).st_size < committed:
                raise RuntimeIntegrityError("资源治理已提交冷历史被截断")
            # 仅移除未被热状态提交的尾部，原有效前缀保持原样。
            handle.truncate(committed)
            handle.seek(committed)
            for index, record in enumerate(records, start=cursor["records"] + 1):
                payload = {"governor_config_hash": self.identity_hash,
                           "sequence": index, "record": record}
                handle.write((canonical_json(payload) + "\n").encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
            state["history"] = {"bytes": handle.tell(), "records": cursor["records"] + len(records)}

    def _read_history(self, state: Mapping[str, object]) -> list[dict[str, object]]:
        cursor = state["history"]
        remaining = cursor["bytes"]
        records: list[dict[str, object]] = []
        if remaining:
            try:
                with self._history_path().open("rb") as handle:
                    while remaining:
                        line = handle.readline(remaining)
                        if not line or not line.endswith(b"\n"):
                            raise RuntimeIntegrityError("资源治理已提交冷历史被截断或记录不完整")
                        remaining -= len(line)
                        item = json.loads(line)
                        if (not isinstance(item, dict)
                                or set(item) != {"governor_config_hash", "sequence", "record"}
                                or item["governor_config_hash"] != self.identity_hash
                                or type(item["sequence"]) is not int
                                or item["sequence"] != len(records) + 1
                                or not isinstance(item["record"], dict)):
                            raise RuntimeIntegrityError("资源治理冷历史身份或顺序损坏")
                        records.append(item["record"])
            except (OSError, UnicodeError, ValueError) as exc:
                raise RuntimeIntegrityError("资源治理已提交冷历史损坏") from exc
        if len(records) != cursor["records"]:
            raise RuntimeIntegrityError("资源治理冷历史记录数与提交位置不一致")
        for item in records:
            self._validate_lease_record(item, active=False)
        identities = [item["lease_id"] for item in records]
        if len(set(identities)) != len(identities):
            raise RuntimeIntegrityError("资源治理冷历史身份重复")
        return records

    def _validate_or_initialize(self, state: dict[str, object]) -> None:
        if not state:
            state.update({
                "contract_version": RESOURCE_GOVERNOR_VERSION,
                "governor_config_hash": self.identity_hash,
                "governor_config_payload": self.config.identity_payload,
                "capacity": self.config.capacity.to_dict(),
                "next_sequence": 1,
                "requests": {},
                "leases": {},
                "history": {"bytes": 0, "records": 0},
            })
            return
        if state.get("contract_version") != RESOURCE_GOVERNOR_VERSION:
            raise RuntimeIntegrityError("资源治理状态版本不受支持；请使用新的仓库外 state dir，旧目录保持原样")
        if set(state) != {
            "contract_version", "governor_config_hash", "governor_config_payload",
            "capacity", "next_sequence", "requests", "leases", "history",
        }:
            raise RuntimeIntegrityError("资源治理状态 schema 无效；准入已关闭")
        config_payload = state["governor_config_payload"]
        if not isinstance(config_payload, Mapping):
            raise RuntimeIntegrityError("资源治理配置身份 schema 无效；准入已关闭")
        if (
            state["governor_config_hash"] != self.identity_hash
            or config_payload != self.config.identity_payload
            or typed_canonical_hash(config_payload) != state["governor_config_hash"]
        ):
            raise RuntimeIntegrityError("资源治理配置身份与现有状态不一致；准入已关闭")
        if ResourceVector.from_dict(state["capacity"]) != self.config.capacity:  # type: ignore[arg-type]
            raise RuntimeIntegrityError("资源治理容量与现有状态不一致")
        if type(state["next_sequence"]) is not int or state["next_sequence"] < 1:
            raise RuntimeIntegrityError("资源治理申请序号损坏")
        if not isinstance(state["requests"], dict) or not isinstance(state["leases"], dict):
            raise RuntimeIntegrityError("资源治理集合状态损坏")
        cursor = state["history"]
        if (not isinstance(cursor, dict) or set(cursor) != {"bytes", "records"}
                or any(type(value) is not int or value < 0 for value in cursor.values())
                or bool(cursor["bytes"]) != bool(cursor["records"])):
            raise RuntimeIntegrityError("资源治理租约历史提交位置损坏")
        for identity, record in state["requests"].items():
            self._validate_request_record(record)
            if identity != record["request_id"]:
                raise RuntimeIntegrityError("资源申请身份损坏")
        for identity, record in state["leases"].items():
            self._validate_lease_record(record, active=True)
            if identity != record["lease_id"]:
                raise RuntimeIntegrityError("活动租约身份损坏")
        self._assert_conservation(state)

    @staticmethod
    def _validate_request_record(record: object) -> None:
        if not isinstance(record, Mapping):
            raise RuntimeIntegrityError("资源申请记录损坏")
        if (any(not isinstance(record.get(key), str) or not record[key]
                for key in ("request_id", "owner_id", "created_at"))
                or type(record.get("sequence")) is not int or record["sequence"] < 1
                or type(record.get("pid")) is not int or record["pid"] < 1
                or type(record.get("process_started_at")) not in (int, float)
                or not math.isfinite(record["process_started_at"])
                or record["process_started_at"] <= 0
                or (record.get("parent_lease_id") is not None
                    and not isinstance(record["parent_lease_id"], str))
                or not isinstance(record.get("vector"), Mapping)):
            raise RuntimeIntegrityError("资源申请进程身份或序号损坏")
        ResourceVector.from_dict(record["vector"])

    def _validate_lease_record(self, record: object, *, active: bool) -> None:
        self._validate_request_record(record)
        expected = {"request_id", "sequence", "owner_id", "vector", "parent_lease_id", "pid",
                    "process_started_at", "created_at", "lease_id", "status", "acquired_at",
                    "heartbeat_epoch", "released_at", "release_reason"}
        if set(record) != expected:
            raise RuntimeIntegrityError("资源租约 schema 损坏")
        identity = {"governor": self.identity_hash, **{key: record[key] for key in
                    ("request_id", "sequence", "owner_id", "pid", "process_started_at")}}
        if (record["lease_id"] != typed_canonical_hash(identity)
                or record["status"] not in ({"active"} if active else {"released", "reaped"})
                or type(record["heartbeat_epoch"]) not in (int, float)
                or not math.isfinite(record["heartbeat_epoch"])
                or not isinstance(record["acquired_at"], str)
                or (active and (record["released_at"] is not None or record["release_reason"] is not None))
                or (not active and (not isinstance(record["released_at"], str)
                                    or not isinstance(record["release_reason"], str)))):
            raise RuntimeIntegrityError("资源租约身份或状态损坏")

    def _is_fifo_head(self, state: Mapping[str, object], request: Mapping[str, object]) -> bool:
        scope = request["parent_lease_id"]
        queue = [item for item in state["requests"].values() if item["parent_lease_id"] == scope]  # type: ignore[union-attr]
        return request["sequence"] == min(item["sequence"] for item in queue)

    def _can_admit(self, state: Mapping[str, object], request: Mapping[str, object]) -> bool:
        vector = ResourceVector.from_dict(request["vector"])  # type: ignore[arg-type]
        parent_id = request["parent_lease_id"]
        if parent_id is None:
            used = _sum_vectors(
                ResourceVector.from_dict(item["vector"])
                for item in state["leases"].values()  # type: ignore[union-attr]
                if item["status"] == "active" and item["parent_lease_id"] is None
            )
            return vector.fits_within(self.config.capacity) and _add_optional(used, vector).fits_within(self.config.capacity)
        parent = state["leases"].get(parent_id)  # type: ignore[union-attr]
        if parent is None or parent["status"] != "active":
            raise RuntimeAdmissionError("父资源租约不存在或已失效")
        parent_vector = ResourceVector.from_dict(parent["vector"])
        children = [
            ResourceVector.from_dict(item["vector"])
            for item in state["leases"].values()  # type: ignore[union-attr]
            if item["status"] == "active" and item["parent_lease_id"] == parent_id
        ]
        used = _sum_vectors(children)
        combined = _add_optional(used, vector)
        if combined.process_slots + 1 > parent_vector.process_slots:
            return False
        return all(
            parent_vector.to_dict()[key] is None or (combined.to_dict()[key] is not None and combined.to_dict()[key] <= parent_vector.to_dict()[key])
            for key in ("memory_bytes", "cpu_slots", "scratch_bytes")
        )

    def _reap_stale_in_state(self, state: dict[str, object]) -> tuple[str, ...]:
        now = time.time()
        stale: list[str] = []
        dead_requests = [
            request_id
            for request_id, item in state["requests"].items()  # type: ignore[union-attr]
            if not _process_identity_alive(int(item["pid"]), float(item["process_started_at"]))
        ]
        for request_id in dead_requests:
            del state["requests"][request_id]  # type: ignore[index]
        for lease_id, item in sorted(state["leases"].items(), key=lambda pair: pair[1]["sequence"]):  # type: ignore[union-attr]
            if item["status"] != "active":
                continue
            alive = _process_identity_alive(int(item["pid"]), float(item["process_started_at"]))
            heartbeat_stale = now - float(item["heartbeat_epoch"]) > self.config.stale_after_seconds
            parent_id = item["parent_lease_id"]
            parent_active = parent_id is None or (
                parent_id in state["leases"] and state["leases"][parent_id]["status"] == "active"  # type: ignore[index,operator]
            )
            if not alive or heartbeat_stale or not parent_active:
                self._close_lease_tree(state, lease_id, "stale_reaped")
                stale.append(lease_id)
        self._assert_conservation(state)
        return tuple(stale)

    @staticmethod
    def _close_lease_tree(state: dict[str, object], lease_id: str, reason: str) -> None:
        descendants = [
            key for key, item in state["leases"].items()  # type: ignore[union-attr]
            if item["status"] == "active" and item["parent_lease_id"] == lease_id
        ]
        for child_id in descendants:
            ResourceGovernor._close_lease_tree(state, child_id, "parent_released")
        record = state["leases"][lease_id]  # type: ignore[index]
        if record["status"] == "active":
            record["status"] = "released" if reason == "released" else "reaped"
            record["released_at"] = _utc_now()
            record["release_reason"] = reason

    def _assert_conservation(self, state: Mapping[str, object]) -> None:
        roots = [
            ResourceVector.from_dict(item["vector"])
            for item in state["leases"].values()  # type: ignore[union-attr]
            if item.get("status") == "active" and item.get("parent_lease_id") is None
        ]
        used = _sum_vectors(roots)
        if used is not None and not used.fits_within(self.config.capacity):
            raise RuntimeIntegrityError("资源治理全局守恒失败；准入已关闭")
        for lease_id, item in state["leases"].items():  # type: ignore[union-attr]
            if item.get("status") != "active":
                continue
            parent_id = item.get("parent_lease_id")
            if parent_id is not None:
                parent = state["leases"].get(parent_id)  # type: ignore[union-attr]
                if parent is None or parent.get("status") != "active":
                    raise RuntimeIntegrityError(f"活动子租约缺少活动父租约: {lease_id}")
        for parent_id, parent in state["leases"].items():  # type: ignore[union-attr]
            if parent.get("status") != "active":
                continue
            children = [
                ResourceVector.from_dict(item["vector"])
                for item in state["leases"].values()  # type: ignore[union-attr]
                if item.get("status") == "active" and item.get("parent_lease_id") == parent_id
            ]
            child_total = _sum_vectors(children)
            if child_total is None:
                continue
            parent_vector = ResourceVector.from_dict(parent["vector"])
            scalar_fits = all(
                parent_vector.to_dict()[key] is None or (child_total.to_dict()[key] is not None and child_total.to_dict()[key] <= parent_vector.to_dict()[key])
                for key in ("memory_bytes", "cpu_slots", "scratch_bytes")
            )
            # 父进程自身占一个 process slot，子进程只能使用剩余额度。
            if not scalar_fits or child_total.process_slots + 1 > parent_vector.process_slots:
                raise RuntimeIntegrityError(
                    f"资源治理父子守恒失败；准入已关闭: {parent_id}"
                )


def _same_lease_identity(record: Mapping[str, object], lease: ResourceLease) -> bool:
    return (
        record.get("request_id") == lease.request_id
        and record.get("owner_id") == lease.owner_id
        and record.get("pid") == lease.pid
        and abs(float(record.get("process_started_at", -1)) - lease.process_started_at) < 0.01
    )


def _process_identity_alive(pid: int, started_at: float) -> bool:
    try:
        process = psutil.Process(pid)
        return process.is_running() and abs(process.create_time() - started_at) < 0.01
    except psutil.Error:
        return False


def _sum_vectors(values: Iterator[ResourceVector] | list[ResourceVector]) -> ResourceVector | None:
    items = list(values)
    if not items:
        return None
    totals = {key: None if any(item.to_dict()[key] is None for item in items) else sum(item.to_dict()[key] for item in items) for key in items[0].to_dict()}
    return ResourceVector(**totals)


def _add_optional(left: ResourceVector | None, right: ResourceVector) -> ResourceVector:
    return right if left is None else left.plus(right)


def _find_repository_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists() or (candidate / "AGENTS.md").is_file():
            return candidate
    return None



__all__ = [
    "RESOURCE_GOVERNOR_VERSION", "ResourceGovernor", "ResourceGovernorConfig",
    "ResourceLease", "read_resource_governor_state",
]
