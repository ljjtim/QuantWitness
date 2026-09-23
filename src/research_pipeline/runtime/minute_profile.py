"""分钟执行 profile：稳定分区、硬预算、原子工件与可验证恢复。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Callable, Mapping, Sequence

import psutil

from research_pipeline.platform import canonical_json, typed_canonical_hash

from .errors import RuntimeAdmissionError, RuntimeIntegrityError, RuntimeWorkerError
from .external_artifact import ExternalArtifactCommit, ExternalArtifactStore
from .resource_governor import ResourceGovernor, ResourceLease, ResourceVector


MINUTE_EXECUTION_PROFILE_VERSION = "minute-execution-profile-v1"
MINUTE_PARTITION_ALGORITHM_VERSION = "minute-partition-sha256-v1"
MINUTE_PARTITION_MANIFEST_VERSION = "minute-partition-manifest-v1"
MINUTE_PARTITION_CHECKPOINT_VERSION = "minute-partition-checkpoint-v1"
MINUTE_PARTITION_METRICS_VERSION = "minute-partition-metrics-v2"
MINUTE_RUNTIME_RESULT_VERSION = "minute-runtime-result-v1"


class MinuteRuntimeBudgetError(RuntimeAdmissionError):
    """分钟运行的稳定预算拒绝；调用方可按 error_code 给出缩小范围提示。"""

    error_code = "minute_runtime_budget_exceeded"


def _require_digest(value: str, field: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RuntimeIntegrityError(f"分钟执行身份字段不是 sha256: {field}")


def _safe_component(value: str, field: str) -> None:
    if not value or value in {".", ".."} or any(token in value for token in ("/", "\\")):
        raise RuntimeIntegrityError(f"分钟分区字段不是安全路径组件: {field}")


def _directory_size(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stage = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with stage.open("w", encoding="utf-8") as handle:
        handle.write(canonical_json(dict(payload)))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(stage, path)


@dataclass(frozen=True)
class MinuteExecutionIdentity:
    """恢复所需的完整语义身份；任何字段变化都拒绝复用。"""

    catalog_hash: str
    data_snapshot_hash: str
    query_plan_hash: str
    session_policy_hash: str
    operator_definitions_hash: str
    cache_profile_hash: str
    environment_hash: str
    root_seed: int
    fixed_clock: str
    contract_version: str = MINUTE_EXECUTION_PROFILE_VERSION

    def __post_init__(self) -> None:
        for field in (
            "catalog_hash", "data_snapshot_hash", "query_plan_hash", "session_policy_hash",
            "operator_definitions_hash", "cache_profile_hash", "environment_hash",
        ):
            _require_digest(getattr(self, field), field)
        if type(self.root_seed) is not int or self.root_seed < 0:
            raise RuntimeIntegrityError("分钟执行 root_seed 必须是非负整数")
        if not self.fixed_clock:
            raise RuntimeIntegrityError("分钟执行 fixed_clock 不能为空")
        if self.contract_version != MINUTE_EXECUTION_PROFILE_VERSION:
            raise RuntimeIntegrityError("分钟执行 profile 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "catalog_hash": self.catalog_hash,
            "data_snapshot_hash": self.data_snapshot_hash,
            "query_plan_hash": self.query_plan_hash,
            "session_policy_hash": self.session_policy_hash,
            "operator_definitions_hash": self.operator_definitions_hash,
            "cache_profile_hash": self.cache_profile_hash,
            "environment_hash": self.environment_hash,
            "root_seed": self.root_seed,
            "fixed_clock": self.fixed_clock,
        }

    @property
    def identity_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


@dataclass(frozen=True)
class MinuteRuntimeBudget:
    max_source_bytes: int
    max_returned_rows: int
    max_batch_bytes: int
    max_rss_bytes: int
    max_temp_bytes: int
    max_output_bytes: int
    max_workers: int
    max_wall_seconds: float

    def __post_init__(self) -> None:
        values = (
            self.max_source_bytes, self.max_returned_rows, self.max_batch_bytes,
            self.max_rss_bytes, self.max_temp_bytes, self.max_output_bytes, self.max_workers,
        )
        if any(type(value) is not int or value <= 0 for value in values):
            raise RuntimeAdmissionError("minute_runtime_budget_invalid: 整数预算必须为正数")
        if not isinstance(self.max_wall_seconds, (int, float)) or self.max_wall_seconds <= 0:
            raise RuntimeAdmissionError("minute_runtime_budget_invalid: wall time 预算必须为正数")

    def to_dict(self) -> dict[str, object]:
        return {
            "max_source_bytes": self.max_source_bytes,
            "max_returned_rows": self.max_returned_rows,
            "max_batch_bytes": self.max_batch_bytes,
            "max_rss_bytes": self.max_rss_bytes,
            "max_temp_bytes": self.max_temp_bytes,
            "max_output_bytes": self.max_output_bytes,
            "max_workers": self.max_workers,
            "max_wall_seconds": self.max_wall_seconds,
        }


@dataclass(frozen=True, order=True)
class MinutePartitionKey:
    trading_date: str
    instrument_bucket: int
    parameter_block: int

    def __post_init__(self) -> None:
        try:
            date.fromisoformat(self.trading_date)
        except ValueError as exc:
            raise RuntimeIntegrityError("分钟分区 trading_date 无效") from exc
        if self.instrument_bucket < 0 or self.parameter_block < 0:
            raise RuntimeIntegrityError("分钟分区 bucket/block 不能为负数")

    @property
    def partition_id(self) -> str:
        return f"d={self.trading_date}__b={self.instrument_bucket:04d}__p={self.parameter_block:04d}"

    def to_dict(self) -> dict[str, object]:
        return {
            "trading_date": self.trading_date,
            "instrument_bucket": self.instrument_bucket,
            "parameter_block": self.parameter_block,
        }


@dataclass(frozen=True)
class MinutePartitionSpec:
    key: MinutePartitionKey
    instruments: tuple[str, ...]
    parameter_ids: tuple[str, ...]
    estimated_source_bytes: int
    estimated_rows: int
    execution_identity_hash: str

    def __post_init__(self) -> None:
        if not self.instruments or tuple(sorted(set(self.instruments))) != self.instruments:
            raise RuntimeIntegrityError("分钟分区 instruments 必须非空、唯一且稳定排序")
        if not self.parameter_ids or tuple(sorted(set(self.parameter_ids))) != self.parameter_ids:
            raise RuntimeIntegrityError("分钟分区 parameter_ids 必须非空、唯一且稳定排序")
        if self.estimated_source_bytes < 0 or self.estimated_rows < 0:
            raise RuntimeIntegrityError("分钟分区估算不能为负数")
        _require_digest(self.execution_identity_hash, "execution_identity_hash")

    @property
    def partition_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key.to_dict(),
            "partition_id": self.key.partition_id,
            "instruments": list(self.instruments),
            "parameter_ids": list(self.parameter_ids),
            "estimated_source_bytes": self.estimated_source_bytes,
            "estimated_rows": self.estimated_rows,
            "execution_identity_hash": self.execution_identity_hash,
        }


@dataclass(frozen=True)
class MinutePartitionManifest:
    execution_identity: MinuteExecutionIdentity
    bucket_count: int
    parameter_block_size: int
    partitions: tuple[MinutePartitionSpec, ...]
    algorithm_version: str = MINUTE_PARTITION_ALGORITHM_VERSION
    contract_version: str = MINUTE_PARTITION_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if self.bucket_count <= 0 or self.parameter_block_size <= 0:
            raise RuntimeIntegrityError("分钟分区数量与参数块大小必须为正数")
        if self.algorithm_version != MINUTE_PARTITION_ALGORITHM_VERSION:
            raise RuntimeIntegrityError("分钟分区算法版本不受支持")
        if self.contract_version != MINUTE_PARTITION_MANIFEST_VERSION:
            raise RuntimeIntegrityError("分钟分区 manifest 版本不受支持")
        expected = tuple(sorted(self.partitions, key=lambda item: item.key))
        if expected != self.partitions or len({item.key for item in self.partitions}) != len(self.partitions):
            raise RuntimeIntegrityError("分钟分区清单顺序或唯一性无效")
        if any(item.execution_identity_hash != self.execution_identity.identity_hash for item in self.partitions):
            raise RuntimeIntegrityError("分钟分区未绑定同一执行身份")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "algorithm_version": self.algorithm_version,
            "execution_identity": self.execution_identity.to_dict(),
            "execution_identity_hash": self.execution_identity.identity_hash,
            "bucket_count": self.bucket_count,
            "parameter_block_size": self.parameter_block_size,
            "partitions": [item.to_dict() for item in self.partitions],
        }

    @property
    def manifest_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


def stable_minute_instrument_bucket(instrument: str, bucket_count: int) -> int:
    if not instrument or bucket_count <= 0:
        raise RuntimeIntegrityError("分钟分区 instrument/bucket_count 无效")
    digest = hashlib.sha256(
        f"{MINUTE_PARTITION_ALGORITHM_VERSION}\0{instrument}".encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big") % bucket_count


def compile_minute_partition_manifest(
    *,
    execution_identity: MinuteExecutionIdentity,
    trading_dates: Sequence[str],
    instruments: Sequence[str],
    bucket_count: int,
    parameter_ids: Sequence[str] = ("default",),
    parameter_block_size: int = 128,
    estimated_source_bytes: int = 0,
    estimated_rows: int = 0,
) -> MinutePartitionManifest:
    dates = tuple(sorted(set(trading_dates)))
    codes = tuple(sorted(set(instruments)))
    parameters = tuple(sorted(set(parameter_ids)))
    if not dates or not codes or not parameters:
        raise RuntimeIntegrityError("分钟分区编译输入不能为空")
    if bucket_count <= 0 or parameter_block_size <= 0:
        raise RuntimeIntegrityError("分钟分区 bucket_count/block_size 必须为正数")
    for value in codes:
        _safe_component(value, "instrument")
    for value in parameters:
        _safe_component(value, "parameter_id")
    buckets: dict[int, tuple[str, ...]] = {}
    for bucket in range(bucket_count):
        members = tuple(code for code in codes if stable_minute_instrument_bucket(code, bucket_count) == bucket)
        if members:
            buckets[bucket] = members
    blocks = tuple(
        parameters[offset: offset + parameter_block_size]
        for offset in range(0, len(parameters), parameter_block_size)
    )
    raw = [(day, bucket, members, block_index, block) for day in dates for bucket, members in buckets.items() for block_index, block in enumerate(blocks)]
    source_share, source_remainder = divmod(estimated_source_bytes, len(raw))
    row_share, row_remainder = divmod(estimated_rows, len(raw))
    specs = tuple(
        MinutePartitionSpec(
            MinutePartitionKey(day, bucket, block_index),
            members,
            block,
            source_share + (index < source_remainder),
            row_share + (index < row_remainder),
            execution_identity.identity_hash,
        )
        for index, (day, bucket, members, block_index, block) in enumerate(raw)
    )
    return MinutePartitionManifest(execution_identity, bucket_count, parameter_block_size, specs)


@dataclass(frozen=True)
class MinutePartitionWorkResult:
    rows_scanned: int
    bytes_scanned: int
    pruned_partitions: int
    peak_batch_bytes: int

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for value in (
            self.rows_scanned, self.bytes_scanned, self.pruned_partitions, self.peak_batch_bytes,
        )):
            raise RuntimeIntegrityError("分钟分区工作指标必须是非负整数")


@dataclass(frozen=True)
class MinutePartitionMetrics:
    partition_id: str
    rows_scanned: int
    bytes_scanned: int
    pruned_partitions: int
    peak_batch_bytes: int
    peak_rss_bytes: int
    rss_measurement_status: str
    temp_bytes: int
    output_bytes: int
    wall_seconds: float
    worker_count: int
    contract_version: str = MINUTE_PARTITION_METRICS_VERSION

    def __post_init__(self) -> None:
        _safe_component(self.partition_id, "partition_id")
        integers = (
            self.rows_scanned, self.bytes_scanned, self.pruned_partitions,
            self.peak_batch_bytes, self.peak_rss_bytes, self.temp_bytes,
            self.output_bytes, self.worker_count,
        )
        if any(type(value) is not int or value < 0 for value in integers) or self.worker_count == 0:
            raise RuntimeIntegrityError("分钟分区 metrics 数值无效")
        if self.rss_measurement_status not in {"available", "measurement_unavailable"}:
            raise RuntimeIntegrityError("分钟分区 RSS 测量状态无效")
        if self.rss_measurement_status == "measurement_unavailable" and self.peak_rss_bytes != 0:
            raise RuntimeIntegrityError("分钟分区 RSS 未测得时不能写入伪测量值")
        if not isinstance(self.wall_seconds, (int, float)) or self.wall_seconds < 0:
            raise RuntimeIntegrityError("分钟分区 metrics wall_seconds 无效")
        if self.contract_version != MINUTE_PARTITION_METRICS_VERSION:
            raise RuntimeIntegrityError("分钟分区 metrics 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "partition_id": self.partition_id,
            "rows_scanned": self.rows_scanned,
            "bytes_scanned": self.bytes_scanned,
            "pruned_partitions": self.pruned_partitions,
            "peak_batch_bytes": self.peak_batch_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "rss_measurement_status": self.rss_measurement_status,
            "temp_bytes": self.temp_bytes,
            "output_bytes": self.output_bytes,
            "wall_seconds": self.wall_seconds,
            "worker_count": self.worker_count,
        }


@dataclass(frozen=True)
class MinutePartitionExecutionResult:
    manifest_hash: str
    commits: tuple[ExternalArtifactCommit, ...]
    metrics: tuple[MinutePartitionMetrics, ...]
    reused_partition_ids: tuple[str, ...]
    semantic_hash: str


MinutePartitionAction = Callable[[MinutePartitionSpec, Path], MinutePartitionWorkResult]
MinutePartitionPhaseHook = Callable[[str, MinutePartitionSpec], None]


class MinutePartitionRuntime:
    """在小而稳定的分区上运行分钟节点；成功以 commit+checkpoint 双重确认。"""

    def execute(
        self,
        *,
        manifest: MinutePartitionManifest,
        action: MinutePartitionAction,
        run_root: str | Path,
        budget: MinuteRuntimeBudget,
        worker_count: int = 1,
        resume: bool = False,
        phase_hook: MinutePartitionPhaseHook | None = None,
        resource_governor: ResourceGovernor | None = None,
        parent_resource_lease: ResourceLease | None = None,
        resource_timeout_seconds: float | None = None,
    ) -> MinutePartitionExecutionResult:
        if worker_count <= 0 or worker_count > budget.max_workers:
            self._budget_failure("workers", worker_count, budget.max_workers)
        governance_values = (resource_governor, parent_resource_lease, resource_timeout_seconds)
        if any(item is not None for item in governance_values) and any(
            item is None for item in governance_values
        ):
            raise RuntimeIntegrityError("分钟父子资源治理参数必须完整")
        source_estimate = sum(item.estimated_source_bytes for item in manifest.partitions)
        row_estimate = sum(item.estimated_rows for item in manifest.partitions)
        if source_estimate > budget.max_source_bytes:
            self._budget_failure("estimated_source_bytes", source_estimate, budget.max_source_bytes)
        if row_estimate > budget.max_returned_rows:
            self._budget_failure("estimated_rows", row_estimate, budget.max_returned_rows)

        root = Path(run_root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        record_path = root / "minute-run.json"
        immutable = {
            "contract_version": MINUTE_RUNTIME_RESULT_VERSION,
            "manifest_hash": manifest.manifest_hash,
            "execution_identity_hash": manifest.execution_identity.identity_hash,
            "partition_algorithm_version": manifest.algorithm_version,
        }
        if record_path.exists():
            if not resume:
                raise RuntimeIntegrityError("分钟运行已存在；必须显式 resume")
            try:
                existing = json.loads(record_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                raise RuntimeIntegrityError("分钟运行记录无法读取") from exc
            if any(existing.get(key) != value for key, value in immutable.items()):
                raise RuntimeIntegrityError("分钟运行身份漂移，拒绝复用 checkpoint")
        else:
            _write_json_atomic(record_path, {**immutable, "status": "running"})

        store = ExternalArtifactStore(root / "external-artifacts")
        checkpoint_root = root / "minute-checkpoints"
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        commits: dict[str, ExternalArtifactCommit] = {}
        metrics: dict[str, MinutePartitionMetrics] = {}
        reused: list[str] = []
        pending: list[MinutePartitionSpec] = []
        for spec in manifest.partitions:
            checkpoint = checkpoint_root / f"{spec.key.partition_id}.json"
            if checkpoint.exists():
                commit, item_metrics = self._read_checkpoint(
                    checkpoint, manifest=manifest, spec=spec, store=store,
                )
                commits[spec.key.partition_id] = commit
                metrics[spec.key.partition_id] = item_metrics
                reused.append(spec.key.partition_id)
            else:
                pending.append(spec)

        started = time.monotonic()
        try:
            if worker_count == 1:
                for spec in pending:
                    commit, item_metrics = self._run_one(
                        spec, manifest, action, store, checkpoint_root, budget,
                        worker_count, phase_hook, resource_governor,
                        parent_resource_lease, resource_timeout_seconds,
                    )
                    commits[spec.key.partition_id] = commit
                    metrics[spec.key.partition_id] = item_metrics
            else:
                with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="minute-runtime") as pool:
                    futures = {
                        pool.submit(
                            self._run_one, spec, manifest, action, store,
                            checkpoint_root, budget, worker_count, phase_hook,
                            resource_governor, parent_resource_lease,
                            resource_timeout_seconds,
                        ): spec
                        for spec in pending
                    }
                    for future in as_completed(futures):
                        spec = futures[future]
                        commit, item_metrics = future.result()
                        commits[spec.key.partition_id] = commit
                        metrics[spec.key.partition_id] = item_metrics
            elapsed = time.monotonic() - started
            self._require_limit("wall_seconds", elapsed, budget.max_wall_seconds)
            self._require_limit("rows_scanned", sum(item.rows_scanned for item in metrics.values()), budget.max_returned_rows)
            self._require_limit("bytes_scanned", sum(item.bytes_scanned for item in metrics.values()), budget.max_source_bytes)
            self._require_limit("output_bytes", sum(item.output_bytes for item in metrics.values()), budget.max_output_bytes)
        except BaseException:
            _write_json_atomic(record_path, {**immutable, "status": "paused"})
            raise

        ordered_commits = tuple(commits[item.key.partition_id] for item in manifest.partitions)
        ordered_metrics = tuple(metrics[item.key.partition_id] for item in manifest.partitions)
        semantic_hash = typed_canonical_hash({
            "contract_version": MINUTE_RUNTIME_RESULT_VERSION,
            "manifest_hash": manifest.manifest_hash,
            "partitions": [
                {
                    "partition_hash": spec.partition_hash,
                    "artifact_semantic_hash": commit.semantic_hash,
                }
                for spec, commit in zip(manifest.partitions, ordered_commits, strict=True)
            ],
        })
        _write_json_atomic(record_path, {
            **immutable,
            "status": "succeeded",
            "semantic_hash": semantic_hash,
            "partitions": len(ordered_commits),
        })
        return MinutePartitionExecutionResult(
            manifest.manifest_hash,
            ordered_commits,
            ordered_metrics,
            tuple(sorted(reused)),
            semantic_hash,
        )

    def _run_one(
        self,
        spec: MinutePartitionSpec,
        manifest: MinutePartitionManifest,
        action: MinutePartitionAction,
        store: ExternalArtifactStore,
        checkpoint_root: Path,
        budget: MinuteRuntimeBudget,
        worker_count: int,
        phase_hook: MinutePartitionPhaseHook | None,
        resource_governor: ResourceGovernor | None,
        parent_resource_lease: ResourceLease | None,
        resource_timeout_seconds: float | None,
    ) -> tuple[ExternalArtifactCommit, MinutePartitionMetrics]:
        if resource_governor is None or worker_count == 1:
            return self._run_one_unleased(
                spec, manifest, action, store, checkpoint_root, budget,
                worker_count, phase_hook,
            )
        child = ResourceVector(
            max(1, parent_resource_lease.vector.memory_bytes // worker_count),
            max(1, parent_resource_lease.vector.cpu_slots // worker_count),
            max(1, parent_resource_lease.vector.scratch_bytes // worker_count),
            0,
        )
        lease = resource_governor.acquire(
            owner_id=f"minute-partition/{spec.key.partition_id}",
            vector=child,
            parent_lease_id=parent_resource_lease.lease_id,
            timeout_seconds=float(resource_timeout_seconds),
        )
        try:
            with resource_governor.maintained_lease(lease):
                return self._run_one_unleased(
                    spec, manifest, action, store, checkpoint_root, budget,
                    worker_count, phase_hook,
                )
        finally:
            resource_governor.release(lease)

    def _run_one_unleased(
        self,
        spec: MinutePartitionSpec,
        manifest: MinutePartitionManifest,
        action: MinutePartitionAction,
        store: ExternalArtifactStore,
        checkpoint_root: Path,
        budget: MinuteRuntimeBudget,
        worker_count: int,
        phase_hook: MinutePartitionPhaseHook | None,
    ) -> tuple[ExternalArtifactCommit, MinutePartitionMetrics]:
        staging = store.prepare()
        start = time.monotonic()
        process = None
        rss_before = 0
        rss_measurement_status = "available"
        try:
            process = psutil.Process()
            rss_before = process.memory_info().rss
        except (psutil.Error, OSError, RuntimeError):
            rss_measurement_status = "measurement_unavailable"
        try:
            work = action(spec, staging)
            if not isinstance(work, MinutePartitionWorkResult):
                raise RuntimeIntegrityError("分钟分区 action 返回类型无效")
            temp_bytes = _directory_size(staging)
            elapsed = time.monotonic() - start
            peak_rss = 0
            if process is not None and rss_measurement_status == "available":
                try:
                    peak_rss = max(rss_before, process.memory_info().rss)
                except (psutil.Error, OSError, RuntimeError):
                    rss_measurement_status = "measurement_unavailable"
                    peak_rss = 0
            self._require_limit("partition_source_bytes", work.bytes_scanned, budget.max_source_bytes)
            self._require_limit("partition_rows", work.rows_scanned, budget.max_returned_rows)
            self._require_limit("record_batch_bytes", work.peak_batch_bytes, budget.max_batch_bytes)
            if rss_measurement_status == "available":
                self._require_limit("peak_rss_bytes", peak_rss, budget.max_rss_bytes)
            self._require_limit("temp_bytes", temp_bytes, budget.max_temp_bytes)
            self._require_limit("partition_wall_seconds", elapsed, budget.max_wall_seconds)
            commit = store.commit(
                staging,
                artifact_name=f"minute-partition-{spec.key.partition_id}",
                artifact_type="minute_partition",
            )
            if phase_hook:
                phase_hook("artifact_committed", spec)
            output_root = store.objects_root / commit.semantic_hash
            output_bytes = _directory_size(output_root)
            self._require_limit("output_bytes", output_bytes, budget.max_output_bytes)
            item_metrics = MinutePartitionMetrics(
                spec.key.partition_id,
                work.rows_scanned,
                work.bytes_scanned,
                work.pruned_partitions,
                work.peak_batch_bytes,
                peak_rss,
                rss_measurement_status,
                temp_bytes,
                output_bytes,
                elapsed,
                worker_count,
            )
            checkpoint = {
                "contract_version": MINUTE_PARTITION_CHECKPOINT_VERSION,
                "manifest_hash": manifest.manifest_hash,
                "execution_identity": manifest.execution_identity.to_dict(),
                "execution_identity_hash": manifest.execution_identity.identity_hash,
                "partition": spec.to_dict(),
                "partition_hash": spec.partition_hash,
                "external_commit": commit.to_dict(),
                "metrics": item_metrics.to_dict(),
            }
            checkpoint["checkpoint_hash"] = typed_canonical_hash(checkpoint)
            _write_json_atomic(checkpoint_root / f"{spec.key.partition_id}.json", checkpoint)
            if phase_hook:
                phase_hook("checkpoint_committed", spec)
            return commit, item_metrics
        except (RuntimeAdmissionError, RuntimeIntegrityError):
            raise
        except Exception as exc:
            raise RuntimeWorkerError(
                f"minute_partition_worker_failed: {spec.key.partition_id}; 可使用 resume 重试"
            ) from exc

    def _read_checkpoint(
        self,
        path: Path,
        *,
        manifest: MinutePartitionManifest,
        spec: MinutePartitionSpec,
        store: ExternalArtifactStore,
    ) -> tuple[ExternalArtifactCommit, MinutePartitionMetrics]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("分钟分区 checkpoint 无法读取") from exc
        checkpoint_hash = payload.pop("checkpoint_hash", None)
        if checkpoint_hash != typed_canonical_hash(payload):
            raise RuntimeIntegrityError("分钟分区 checkpoint hash 漂移")
        if (
            payload.get("contract_version") != MINUTE_PARTITION_CHECKPOINT_VERSION
            or payload.get("manifest_hash") != manifest.manifest_hash
            or payload.get("execution_identity") != manifest.execution_identity.to_dict()
            or payload.get("execution_identity_hash") != manifest.execution_identity.identity_hash
            or payload.get("partition") != spec.to_dict()
            or payload.get("partition_hash") != spec.partition_hash
        ):
            raise RuntimeIntegrityError("分钟分区 checkpoint 身份漂移")
        commit_payload = payload.get("external_commit")
        metrics_payload = payload.get("metrics")
        if not isinstance(commit_payload, Mapping) or not isinstance(metrics_payload, Mapping):
            raise RuntimeIntegrityError("分钟分区 checkpoint 内容无效")
        commit = ExternalArtifactCommit.from_dict(commit_payload)
        if store.verify(commit.semantic_hash) != commit:
            raise RuntimeIntegrityError("分钟分区 checkpoint 工件引用漂移")
        expected_metrics = {
            "contract_version", "partition_id", "rows_scanned", "bytes_scanned",
            "pruned_partitions", "peak_batch_bytes", "peak_rss_bytes",
            "rss_measurement_status", "temp_bytes",
            "output_bytes", "wall_seconds", "worker_count",
        }
        if set(metrics_payload) != expected_metrics or metrics_payload.get("partition_id") != spec.key.partition_id:
            raise RuntimeIntegrityError("分钟分区 checkpoint 指标 schema 无效")
        metrics = MinutePartitionMetrics(
            str(metrics_payload["partition_id"]),
            int(metrics_payload["rows_scanned"]),
            int(metrics_payload["bytes_scanned"]),
            int(metrics_payload["pruned_partitions"]),
            int(metrics_payload["peak_batch_bytes"]),
            int(metrics_payload["peak_rss_bytes"]),
            str(metrics_payload["rss_measurement_status"]),
            int(metrics_payload["temp_bytes"]),
            int(metrics_payload["output_bytes"]),
            float(metrics_payload["wall_seconds"]),
            int(metrics_payload["worker_count"]),
            str(metrics_payload["contract_version"]),
        )
        return commit, metrics

    @staticmethod
    def _budget_failure(metric: str, actual: int | float, limit: int | float) -> None:
        raise MinuteRuntimeBudgetError(
            f"minute_runtime_budget_exceeded: metric={metric}; actual={actual}; limit={limit}; "
            "请缩小日期/标的/参数范围或显式提高预算"
        )

    @classmethod
    def _require_limit(cls, metric: str, actual: int | float, limit: int | float) -> None:
        if actual > limit:
            cls._budget_failure(metric, actual, limit)


__all__ = [
    "MINUTE_EXECUTION_PROFILE_VERSION",
    "MINUTE_PARTITION_ALGORITHM_VERSION",
    "MINUTE_PARTITION_CHECKPOINT_VERSION",
    "MINUTE_PARTITION_MANIFEST_VERSION",
    "MINUTE_PARTITION_METRICS_VERSION",
    "MINUTE_RUNTIME_RESULT_VERSION",
    "MinuteExecutionIdentity",
    "MinutePartitionAction",
    "MinutePartitionExecutionResult",
    "MinutePartitionKey",
    "MinutePartitionManifest",
    "MinutePartitionPhaseHook",
    "MinutePartitionMetrics",
    "MinutePartitionRuntime",
    "MinutePartitionSpec",
    "MinutePartitionWorkResult",
    "MinuteRuntimeBudget",
    "MinuteRuntimeBudgetError",
    "compile_minute_partition_manifest",
    "stable_minute_instrument_bucket",
]
