"""单机跨进程资源租约、父子令牌和真实运行校准。"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import threading
import time
from typing import Iterator, Mapping
import uuid

import psutil

from research_pipeline.platform import canonical_json, typed_canonical_hash

from .contracts import ResourceBudget
from .errors import RuntimeAdmissionError, RuntimeIntegrityError


RESOURCE_GOVERNOR_VERSION = "research-resource-governor-v2"
RESOURCE_OBSERVATION_VERSION = "research-resource-observation-v3"
RESOURCE_CALIBRATION_VERSION = "research-resource-calibration-v2"

_DATA_SCALE_COMPONENTS = (
    "scan_bytes",
    "intermediate_bytes",
    "output_bytes",
    "scratch_bytes",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ResourceVector:
    memory_bytes: int
    cpu_slots: int
    scratch_bytes: int
    process_slots: int

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for value in self.to_dict().values()):
            raise RuntimeAdmissionError("资源向量必须由非负整数组成")
        if not any(self.to_dict().values()):
            raise RuntimeAdmissionError("资源向量不能全为零")

    @classmethod
    def from_budget(cls, budget: ResourceBudget, *, process_slots: int = 1) -> "ResourceVector":
        return cls(budget.memory_bytes, budget.cpu_slots, budget.temp_bytes, process_slots)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResourceVector":
        if set(payload) != {"memory_bytes", "cpu_slots", "scratch_bytes", "process_slots"}:
            raise RuntimeIntegrityError("资源向量 schema 无效")
        return cls(*(payload[key] for key in (  # type: ignore[arg-type]
            "memory_bytes", "cpu_slots", "scratch_bytes", "process_slots"
        )))

    def to_dict(self) -> dict[str, int]:
        return {
            "memory_bytes": self.memory_bytes,
            "cpu_slots": self.cpu_slots,
            "scratch_bytes": self.scratch_bytes,
            "process_slots": self.process_slots,
        }

    def plus(self, other: "ResourceVector") -> "ResourceVector":
        return ResourceVector(*(self.to_dict()[key] + other.to_dict()[key] for key in self.to_dict()))

    def fits_within(self, capacity: "ResourceVector") -> bool:
        return all(self.to_dict()[key] <= capacity.to_dict()[key] for key in self.to_dict())


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

    @property
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


@dataclass(frozen=True)
class ResourceObservation:
    operator_id: str
    operator_version: str
    profile_id: str
    environment_hash: str
    data_bucket: str
    run_id: str
    node_id: str
    attempt_id: str
    status: str
    fixture: bool
    estimate_components: Mapping[str, int | None]
    actual_components: Mapping[str, int]
    observed_at: str
    measurement_status: str = "available"
    contract_version: str = RESOURCE_OBSERVATION_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != RESOURCE_OBSERVATION_VERSION:
            raise ValueError("资源观测版本无效")
        if self.status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("资源观测状态无效")
        identities = (
            self.operator_id, self.operator_version, self.profile_id,
            self.environment_hash, self.data_bucket,
            self.run_id, self.node_id, self.attempt_id,
        )
        if any(not isinstance(value, str) or not value for value in identities):
            raise ValueError("资源观测身份不能为空")
        if type(self.fixture) is not bool:
            raise ValueError("资源观测 fixture 标记必须为布尔值")
        if self.measurement_status not in {"available", "measurement_unavailable"}:
            raise ValueError("资源观测测量状态无效")
        _validate_digest(self.environment_hash, field="资源观测环境摘要")
        allowed_estimates = {"scan_bytes", "intermediate_bytes", "output_bytes", "scratch_bytes", "parallelism", "wall_seconds"}
        allowed_actuals = {"peak_rss_bytes", "peak_scratch_bytes", "output_bytes", "max_processes", "wall_milliseconds"}
        if set(self.estimate_components) != allowed_estimates or set(self.actual_components) != allowed_actuals:
            raise ValueError("资源观测分量 schema 无效")
        if any(value is not None and (type(value) is not int or value < 0) for value in self.estimate_components.values()):
            raise ValueError("资源估算分量无效")
        if self.data_bucket != resource_data_bucket(self.estimate_components):
            raise ValueError("资源观测数据规模桶与估算分量不一致")
        if any(type(value) is not int or value < 0 for value in self.actual_components.values()):
            raise ValueError("资源实际分量无效")
        try:
            observed = datetime.fromisoformat(self.observed_at)
        except ValueError as exc:
            raise ValueError("资源观测时间无效") from exc
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise ValueError("资源观测时间必须显式带时区")

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResourceObservation":
        expected = {
            "contract_version", "operator_id", "operator_version", "profile_id",
            "environment_hash", "data_bucket",
            "run_id", "node_id", "attempt_id", "status", "fixture",
            "estimate_components", "actual_components", "observed_at",
            "measurement_status",
        }
        if set(payload) != expected:
            raise ValueError("资源观测 schema 无效")
        return cls(**payload)  # type: ignore[arg-type]

    @property
    def observation_id(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "operator_id": self.operator_id,
            "operator_version": self.operator_version,
            "profile_id": self.profile_id,
            "environment_hash": self.environment_hash,
            "data_bucket": self.data_bucket,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "status": self.status,
            "fixture": self.fixture,
            "estimate_components": dict(self.estimate_components),
            "actual_components": dict(self.actual_components),
            "observed_at": self.observed_at,
            "measurement_status": self.measurement_status,
        }


class ResourceUsageSampler:
    """独立采样当前进程树和 attempt scratch，不信任算子自报峰值。"""

    def __init__(self, scratch_root: str | Path, *, interval_seconds: float = 0.05) -> None:
        if interval_seconds <= 0:
            raise ValueError("资源采样间隔必须为正数")
        self.scratch_root = Path(scratch_root).resolve()
        self.interval_seconds = interval_seconds
        self.peak_rss_bytes = 0
        self.peak_scratch_bytes = 0
        self.max_processes = 1
        self.measurement_status = "available"
        self.wall_milliseconds = 0
        self._started = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "ResourceUsageSampler":
        self._started = time.monotonic()
        self._sample()
        self._thread = threading.Thread(target=self._run, name="resource-usage-sampler", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds * 4))
        self._sample()
        self.wall_milliseconds = max(1, round((time.monotonic() - self._started) * 1000))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def _sample(self) -> None:
        try:
            root = psutil.Process(os.getpid())
            processes = [root, *root.children(recursive=True)]
            rss = sum(item.memory_info().rss for item in processes if item.is_running())
            self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
            self.max_processes = max(self.max_processes, len(processes))
        except (psutil.Error, OSError, RuntimeError):
            self.measurement_status = "measurement_unavailable"
        self.peak_scratch_bytes = max(self.peak_scratch_bytes, _directory_size(self.scratch_root))


class _ProcessFileLock:
    def __init__(self, path: Path, timeout_seconds: float) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.handle = None

    def __enter__(self) -> "_ProcessFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.handle.write(b"0")
            self.handle.flush()
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self.handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    self.handle.close()
                    self.handle = None
                    raise RuntimeAdmissionError("资源治理状态锁超时")
                time.sleep(0.01)

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is None:
            return
        self.handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()


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
        with self._locked_state() as state:
            record = state["leases"].get(lease.lease_id)
            if record is None or record.get("status") != "active":
                raise RuntimeAdmissionError("资源租约未知或已失效")
            if not _same_lease_identity(record, lease):
                raise RuntimeIntegrityError("资源租约进程身份不一致")
            record["heartbeat_epoch"] = time.time()

    def release(self, lease: ResourceLease, *, reason: str = "released") -> bool:
        with self._locked_state() as state:
            record = state["leases"].get(lease.lease_id)
            if record is None:
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
                    state["leases"].values(), key=lambda value: value["sequence"]
                )],
            }

    def record_observation(self, observation: ResourceObservation) -> str:
        payload = {**observation.to_dict(), "observation_id": observation.observation_id}
        with self._locked_state() as state:
            self._validate_observations(state["observations"])
            observations = state["observations"]
            existing = next((item for item in observations if item["observation_id"] == observation.observation_id), None)
            if existing is None:
                observations.append(payload)
            elif existing != payload:
                raise RuntimeIntegrityError("资源观测身份冲突")
        return observation.observation_id

    def calibration_summary(
        self,
        *,
        operator_id: str,
        operator_version: str,
        profile_id: str,
        environment_hash: str,
        data_bucket: str,
    ) -> dict[str, object]:
        if any(not isinstance(value, str) or not value for value in (
            operator_id, operator_version, profile_id, data_bucket,
        )):
            raise ValueError("资源校准身份不能为空")
        _validate_digest(environment_hash, field="资源校准环境摘要")
        calibration_key = {
            "operator_id": operator_id,
            "operator_version": operator_version,
            "profile_id": profile_id,
            "environment_hash": environment_hash,
            "data_bucket": data_bucket,
        }
        with self._locked_state() as state:
            self._validate_observations(state["observations"])
            matched = [item for item in state["observations"] if (
                item["operator_id"] == operator_id
                and item["operator_version"] == operator_version
                and item["profile_id"] == profile_id
                and item["environment_hash"] == environment_hash
                and item["data_bucket"] == data_bucket
            )]
        eligible = [
            item
            for item in matched
            if item["status"] == "succeeded"
            and not item["fixture"]
            and item["measurement_status"] == "available"
        ]
        metrics: dict[str, object] = {}
        pairs = {
            "memory_ratio": ("intermediate_bytes", "peak_rss_bytes"),
            "scratch_ratio": ("scratch_bytes", "peak_scratch_bytes"),
            "output_ratio": ("output_bytes", "output_bytes"),
            "parallelism_ratio": ("parallelism", "max_processes"),
            "wall_ratio": ("wall_seconds", "wall_milliseconds"),
        }
        for name, (estimated_key, actual_key) in pairs.items():
            values: list[float] = []
            for item in eligible:
                estimated = item["estimate_components"][estimated_key]
                actual = item["actual_components"][actual_key]
                if name == "wall_ratio":
                    estimated = None if estimated is None else int(estimated) * 1000
                if estimated is not None and estimated > 0 and actual >= 0:
                    values.append(actual / estimated)
            metrics[name] = {
                "sample_count": len(values),
                "p50": _publish_quantile(values, 0.50, minimum=20),
                "p90": _publish_quantile(values, 0.90, minimum=50),
            }
        payload = {
            "contract_version": RESOURCE_CALIBRATION_VERSION,
            "governor_config_hash": self.identity_hash,
            "calibration_key": calibration_key,
            "calibration_key_hash": typed_canonical_hash(calibration_key),
            "recorded_sample_count": len(matched),
            "eligible_real_success_count": len(eligible),
            "excluded_sample_count": len(matched) - len(eligible),
            "source_observation_ids": sorted(item["observation_id"] for item in matched),
            "eligible_observation_ids": sorted(item["observation_id"] for item in eligible),
            "quantile_method": "nearest_rank",
            "metrics": metrics,
        }
        return {**payload, "calibration_hash": typed_canonical_hash(payload)}

    @contextmanager
    def maintained_lease(self, lease: ResourceLease) -> Iterator[ResourceLease]:
        stop = threading.Event()
        error: list[BaseException] = []

        def maintain() -> None:
            interval = min(self.config.stale_after_seconds / 3, 5.0)
            while not stop.wait(interval):
                try:
                    self.heartbeat(lease)
                except BaseException as exc:  # 后台失效必须由主线程看见。
                    error.append(exc)
                    stop.set()

        thread = threading.Thread(target=maintain, name=f"resource-heartbeat-{lease.sequence}", daemon=True)
        thread.start()
        try:
            yield lease
            if error:
                raise RuntimeIntegrityError("资源租约 heartbeat 失败") from error[0]
        finally:
            stop.set()
            thread.join(timeout=max(1.0, self.config.poll_interval_seconds * 4))

    @contextmanager
    def _locked_state(self) -> Iterator[dict[str, object]]:
        with _ProcessFileLock(self._lock_path, timeout_seconds=10.0):
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

    def _write_state(self, state: Mapping[str, object]) -> None:
        temporary = self._state_path.with_name(f".{self._state_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(canonical_json(dict(state)), encoding="utf-8")
        temporary.replace(self._state_path)

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
                "observations": [],
            })
            return
        if set(state) != {
            "contract_version", "governor_config_hash", "governor_config_payload",
            "capacity", "next_sequence", "requests", "leases", "observations",
        }:
            raise RuntimeIntegrityError("资源治理状态 schema 无效；准入已关闭")
        if state["contract_version"] != RESOURCE_GOVERNOR_VERSION:
            raise RuntimeIntegrityError("资源治理状态版本不受支持")
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
        if not isinstance(state["requests"], dict) or not isinstance(state["leases"], dict) or not isinstance(state["observations"], list):
            raise RuntimeIntegrityError("资源治理集合状态损坏")
        self._validate_observations(state["observations"])
        self._assert_conservation(state)

    @staticmethod
    def _validate_observations(observations: object) -> None:
        if not isinstance(observations, list):
            raise RuntimeIntegrityError("资源校准观测集合损坏")
        for item in observations:
            if not isinstance(item, Mapping):
                raise RuntimeIntegrityError("资源校准观测状态损坏")
            observation_id = item.get("observation_id")
            try:
                observation = ResourceObservation.from_dict({
                    key: value for key, value in item.items() if key != "observation_id"
                })
            except (TypeError, ValueError) as exc:
                raise RuntimeIntegrityError("资源校准观测状态损坏") from exc
            if observation_id != observation.observation_id:
                raise RuntimeIntegrityError("资源校准观测摘要漂移；准入已关闭")

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
            combined.to_dict()[key] <= parent_vector.to_dict()[key]
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
                child_total.to_dict()[key] <= parent_vector.to_dict()[key]
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
    totals = {key: sum(item.to_dict()[key] for item in items) for key in items[0].to_dict()}
    return ResourceVector(**totals)


def _add_optional(left: ResourceVector | None, right: ResourceVector) -> ResourceVector:
    return right if left is None else left.plus(right)


def _publish_quantile(values: list[float], quantile: float, *, minimum: int) -> dict[str, object]:
    if len(values) < minimum:
        return {"status": "unknown", "reason": "insufficient_samples", "value": None}
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return {"status": "available", "reason": None, "value": ordered[rank - 1]}


def resource_data_bucket(estimate_components: Mapping[str, int | None]) -> str:
    """把关键字节规模压成稳定的二进制区间，不把运行路径或时间混入校准身份。"""
    parts: list[str] = []
    for name in _DATA_SCALE_COMPONENTS:
        value = estimate_components.get(name)
        if value is None:
            bucket = "unknown"
        elif type(value) is not int or value < 0:
            raise ValueError("资源规模桶输入必须是非负整数或 null")
        elif value == 0:
            bucket = "zero"
        else:
            bucket = f"2^{value.bit_length() - 1}"
        parts.append(f"{name}={bucket}")
    return "|".join(parts)


def _validate_digest(value: object, *, field: str) -> None:
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError(f"{field}必须是 SHA-256")
    try:
        int(value, 16)
    except ValueError as exc:
        raise ValueError(f"{field}必须是 SHA-256") from exc


def _find_repository_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists() or (candidate / "AGENTS.md").is_file():
            return candidate
    return None


def _directory_size(root: Path) -> int:
    total = 0
    if not root.exists():
        return total

    def directory_error(error: OSError) -> None:
        # 正式提交会原子移动 staging；下一次采样会从新目录计入文件。
        if not isinstance(error, FileNotFoundError):
            raise error

    for directory, _, files in os.walk(root, onerror=directory_error):
        for name in files:
            try:
                total += (Path(directory) / name).stat().st_size
            except OSError:
                continue
    return total


__all__ = [
    "RESOURCE_CALIBRATION_VERSION",
    "RESOURCE_GOVERNOR_VERSION",
    "RESOURCE_OBSERVATION_VERSION",
    "ResourceGovernor",
    "ResourceGovernorConfig",
    "ResourceLease",
    "ResourceObservation",
    "ResourceUsageSampler",
    "ResourceVector",
    "resource_data_bucket",
]
