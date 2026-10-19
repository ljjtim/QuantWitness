"""不可变 Parquet 快照的写入、原子发布与校验。"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import uuid
from typing import Any, Callable, Iterable

from research_pipeline.platform.canonical import canonical_json, typed_canonical_hash

from .admission import AdmittedQueryPlan
from .errors import SnapshotIntegrityError
from .gates import QualityGateSession, gate_policy_hash, validate_publish_manifest
from .path_policy import PathRolePolicy
from .query_ir import InstantRangeV2, canonical_minute_instant
from .snapshot_identity import LogicalSnapshot
from .snapshot_layout import PhysicalSnapshotPolicy
from .verification_lifecycle import current_artifact_verification


PHYSICAL_SNAPSHOT_VERSION = "physical-snapshot-v1"
_PATH_POLICY = PathRolePolicy()
_MAX_PENDING_BATCHES = 64


class _SnapshotLayoutWriter:
    """缓冲仅持有已复制的有界 Arrow 数据，同时只打开一个文件。"""

    def __init__(self, staging: Path, schema: Any, policy: PhysicalSnapshotPolicy,
                 event_time_field: str, temporal_source: bool) -> None:
        self.staging = staging
        self.schema = schema
        self.policy = policy
        self.event_time_field = event_time_field
        self.temporal_source = temporal_source
        self.small = True
        self.pending: list[Any] = []
        self.buffer_rows = 0
        self.buffer_bytes = 0
        self.peak_buffer_rows = 0
        self.peak_buffer_bytes = 0
        self.peak_buffer_batches = 0
        self.writer: Any = None
        self.partition: str | None = None
        self.counts: dict[str, int] = {}
        self.file_rows = 0
        self.file_bytes = 0

    def _piece_rows(self, batch: Any, rows: int, byte_limit: int) -> int:
        if batch.slice(0, rows).nbytes <= byte_limit:
            return rows
        low, high = 0, rows
        while low < high:
            middle = (low + high + 1) // 2
            if batch.slice(0, middle).nbytes <= byte_limit:
                low = middle
            else:
                high = middle - 1
        return low

    def _append(self, batch: Any, count: int) -> None:
        import pyarrow as pa

        piece = batch.slice(0, count)
        # slice 会保留输入整块缓冲；复制后才可把当前输入与待写工作集分开计量。
        piece = pa.RecordBatch.from_arrays(
            [pa.concat_arrays([column]) for column in piece.columns], schema=self.schema
        )
        self.pending.append(piece)
        self.peak_buffer_batches = max(self.peak_buffer_batches, len(self.pending))
        if len(self.pending) >= _MAX_PENDING_BATCHES:
            # 极小输入批次也限制元数据对象数；合并仍在同一行数/字节包络内。
            self.pending = pa.Table.from_batches(self.pending, schema=self.schema).combine_chunks().to_batches()
        self.buffer_rows += count
        self.buffer_bytes += int(piece.nbytes)
        self.peak_buffer_rows = max(self.peak_buffer_rows, self.buffer_rows)
        self.peak_buffer_bytes = max(self.peak_buffer_bytes, self.buffer_bytes)

    def _take_buffer(self) -> Any:
        import pyarrow as pa

        table = pa.Table.from_batches(self.pending, schema=self.schema).combine_chunks()
        self.pending.clear()
        self.buffer_rows = 0
        self.buffer_bytes = 0
        return table

    def _open(self, partition: str) -> None:
        import pyarrow.parquet as pq

        if self.writer is not None and self.partition == partition:
            return
        self.close_file()
        directory = self.staging / partition
        directory.mkdir(parents=True, exist_ok=True)
        index = self.counts.get(partition, 0)
        self.counts[partition] = index + 1
        self.writer = pq.ParquetWriter(
            directory / f"part-{index:012d}.parquet", self.schema,
            compression=self.policy.compression,
        )
        self.partition = partition
        self.file_rows = 0
        self.file_bytes = 0

    def _flush(self) -> None:
        if not self.pending:
            return
        table = self._take_buffer()
        if self.small and not self.temporal_source:
            import pyarrow as pa

            # 与原月份目录扫描一致：仅按月份聚合，月内维持来源顺序。
            groups: dict[tuple[int, int], list[int]] = {}
            for index, value in enumerate(table.column(self.event_time_field).to_pylist()):
                groups.setdefault((value.year, value.month), []).append(index)
            if len(groups) > 1:
                indices = [index for key in sorted(groups) for index in groups[key]]
                table = table.take(pa.array(indices, type=pa.int64()))
        self.writer.write_table(table, row_group_size=self.policy.row_group_size)
        self.file_rows += table.num_rows
        self.file_bytes += int(table.nbytes)

    def close_file(self) -> None:
        if self.writer is not None:
            try:
                self._flush()
            finally:
                self.writer.close()
                self.writer = None

    def _feed_partition(self, batch: Any, partition: str) -> None:
        self._open(partition)
        while batch.num_rows:
            row_room = min(
                self.policy.row_group_size - self.buffer_rows,
                self.policy.target_file_rows - self.file_rows - self.buffer_rows,
            )
            byte_room = min(
                self.policy.max_buffer_bytes - self.buffer_bytes,
                self.policy.target_file_bytes - self.file_bytes - self.buffer_bytes,
            )
            count = self._piece_rows(batch, min(batch.num_rows, row_room), byte_room)
            if count == 0:
                if self.pending:
                    self._flush()
                    continue
                if self.file_rows:
                    self.close_file()
                    self._open(partition)
                    continue
                raise SnapshotIntegrityError("单行 Arrow 数据超过布局缓冲字节上限")
            self._append(batch, count)
            batch = batch.slice(count)
            if (self.file_rows + self.buffer_rows >= self.policy.target_file_rows
                    or self.file_bytes + self.buffer_bytes >= self.policy.target_file_bytes):
                self.close_file()
                if batch.num_rows:
                    self._open(partition)
            elif (self.buffer_rows >= self.policy.row_group_size
                  or self.buffer_bytes >= self.policy.max_buffer_bytes):
                self._flush()

    def _write_month_block(self, table: Any) -> None:
        import pyarrow as pa

        groups: dict[tuple[int, int], list[int]] = {}
        for index, value in enumerate(table.column(self.event_time_field).to_pylist()):
            groups.setdefault((value.year, value.month), []).append(index)
        for key in sorted(groups):
            # 块内按月份聚合，月内下标保持来源顺序；不随证券切换开新文件。
            month = table if len(groups) == 1 else table.take(
                pa.array(groups[key], type=pa.int64())
            )
            for batch in month.to_batches():
                self._feed_partition(batch, f"year={key[0]:04d}/month={key[1]:02d}")
            # 月份副本不跨块积压，使块、月份副本和 writer 输入最多三份共存。
            self._flush()

    def _drain_month_block(self) -> None:
        if self.pending:
            self._write_month_block(self._take_buffer())

    def _feed_large(self, batch: Any) -> None:
        if self.temporal_source:
            self._feed_partition(batch, "primary-key-stream")
            return
        while batch.num_rows:
            count = self._piece_rows(
                batch, min(batch.num_rows, self.policy.row_group_size - self.buffer_rows),
                self.policy.max_buffer_bytes - self.buffer_bytes,
            )
            if count == 0:
                if self.pending:
                    self._drain_month_block()
                    continue
                raise SnapshotIntegrityError("单行 Arrow 数据超过布局缓冲字节上限")
            self._append(batch, count)
            batch = batch.slice(count)
            if (self.buffer_rows >= self.policy.row_group_size
                    or self.buffer_bytes >= self.policy.max_buffer_bytes):
                self._drain_month_block()

    def feed(self, batch: Any) -> None:
        while self.small and batch.num_rows:
            count = self._piece_rows(
                batch, min(batch.num_rows, self.policy.small_table_max_rows - self.buffer_rows),
                self.policy.max_buffer_bytes - self.buffer_bytes,
            )
            if count:
                self._append(batch, count)
                batch = batch.slice(count)
            if batch.num_rows:
                self.small = False
                if self.temporal_source:
                    table = self._take_buffer()
                    for piece in table.to_batches():
                        self._feed_large(piece)
                else:
                    self._drain_month_block()
        if batch.num_rows:
            self._feed_large(batch)

    def finish(self) -> None:
        if self.small and self.pending:
            self._open("primary-key-stream" if self.temporal_source else "small")
        elif not self.temporal_source:
            self._drain_month_block()
        self.close_file()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_identity_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in manifest.items() if key != "physical_snapshot_id"}


def publish_parquet_snapshot(
    *,
    batches: Iterable[Any],
    schema: Any,
    plan: AdmittedQueryPlan,
    logical_snapshot: LogicalSnapshot,
    root: str | Path,
    policy: PhysicalSnapshotPolicy | None = None,
    before_commit: Callable[[], None] | None = None,
    temporal_source: bool = False,
) -> Path:
    import pyarrow.parquet as pq

    frozen_policy = plan.snapshot_layout
    if frozen_policy is None:
        raise SnapshotIntegrityError("旧计划未冻结布局策略，重新准入后才能物化快照")
    policy = policy or frozen_policy
    if policy != frozen_policy:
        raise SnapshotIntegrityError("物理布局与准入计划冻结策略不一致")
    if logical_snapshot.admitted_plan_hash != plan.plan_hash:
        raise SnapshotIntegrityError("逻辑快照与准入计划身份不一致")
    if temporal_source != plan.temporal_selection.requires_consumer_binding:
        raise SnapshotIntegrityError("逐决策时态来源模式与 admitted plan 不一致")
    _PATH_POLICY.validate({"snapshot_output_root": root})
    root_path = Path(root).resolve(strict=False)
    root_path.mkdir(parents=True, exist_ok=True)
    root_path = _PATH_POLICY.resolve_root(root_path, role="snapshot_output_root")
    staging = root_path / f".staging-{uuid.uuid4().hex}"
    staging.mkdir()
    rows = 0
    minimum: str | None = None
    maximum: str | None = None
    gate = QualityGateSession(plan, temporal_source=temporal_source)
    layout = _SnapshotLayoutWriter(staging, schema, policy, plan.event_time_field, temporal_source)
    try:
        for batch in batches:
            gate.validate_batch(batch)
            if batch.num_rows == 0:
                continue
            rows += int(batch.num_rows)
            dates = batch.column(plan.event_time_field).to_pylist()
            batch_minimum = min(dates)
            batch_maximum = max(dates)
            if isinstance(plan.query.time_range, InstantRangeV2):
                timezone_info = plan.query.time_range.start_at.tzinfo
                if timezone_info is None:  # pragma: no cover - InstantRangeV2 已验证
                    raise SnapshotIntegrityError("分钟范围缺少时区")
                minimum_value = canonical_minute_instant(
                    batch_minimum.replace(tzinfo=timezone_info)
                )
                maximum_value = canonical_minute_instant(
                    batch_maximum.replace(tzinfo=timezone_info)
                )
            else:
                minimum_value = batch_minimum.isoformat()
                maximum_value = batch_maximum.isoformat()
            minimum = min(filter(None, (minimum, minimum_value)))
            maximum = max(filter(None, (maximum, maximum_value)))
            layout.feed(batch)
        layout.finish()
        if rows == 0:
            if plan.result_cardinality != "zero_or_more":
                raise SnapshotIntegrityError("当前 dataset 合同不允许发布空 Parquet 快照")
            directory = staging / "empty"
            directory.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(
                directory / "part-00000.parquet",
                schema,
                compression=policy.compression,
            )
            writer.close()
        if before_commit is not None:
            before_commit()
        file_entries = []
        discovered = _PATH_POLICY.discover_files(
            staging,
            suffix=".parquet",
            root_role="snapshot_staging_root",
            file_role="snapshot_staging_file",
        )
        for relative_path, file_path in discovered.items():
            file_entries.append(
                {
                    "relative_path": relative_path,
                    "size": file_path.stat().st_size,
                    "sha256": _sha256(file_path),
                }
            )
        manifest: dict[str, Any] = {
            "contract_version": PHYSICAL_SNAPSHOT_VERSION,
            "logical_snapshot_id": logical_snapshot.logical_snapshot_id,
            "admitted_plan_hash": plan.plan_hash,
            "gate_policy_hash": gate_policy_hash(plan),
            "schema_hash": typed_canonical_hash(str(schema)),
            "source_revision_hash": typed_canonical_hash(
                [
                    item.to_dict()
                    for item in sorted(
                        logical_snapshot.source_revisions,
                        key=lambda value: value.source_id,
                    )
                ]
            ),
            "row_count": rows,
            "event_min": minimum,
            "event_max": maximum,
            "policy": policy.to_dict(),
            "files": file_entries,
        }
        if temporal_source:
            manifest["temporal_source"] = True
        if plan.minute_dataset_semantics_hash is not None:
            manifest["minute_time_contract"] = {
                "timezone": plan.minute_timezone,
                "timestamp_storage": plan.minute_timestamp_storage,
                "timestamp_role": plan.minute_timestamp_role,
                "bar_interval": plan.minute_bar_interval,
                "availability_rule": plan.minute_availability_rule,
                "normalization_version": plan.minute_time_normalization_version,
            }
        physical_id = typed_canonical_hash(manifest)
        manifest["physical_snapshot_id"] = physical_id
        validate_publish_manifest(
            plan,
            manifest,
            logical_snapshot_id=logical_snapshot.logical_snapshot_id,
        )
        (staging / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
        (staging / "COMMITTED").write_text(physical_id, encoding="utf-8")
        target = root_path / logical_snapshot.logical_snapshot_id[:2] / logical_snapshot.logical_snapshot_id / physical_id
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            verify_parquet_snapshot(target)
            existing = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
            if existing != manifest:
                raise SnapshotIntegrityError("同 physical identity 的 manifest 不一致")
            shutil.rmtree(staging)
            return target
        os.replace(staging, target)
        session = current_artifact_verification()
        if session is None:
            verify_parquet_snapshot(target)
        else:
            session.remember("parquet_snapshot", str(target), manifest)
        return target
    except BaseException:
        if layout.writer is not None:
            layout.writer.close()
            layout.writer = None
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_parquet_snapshot(path: str | Path) -> dict[str, Any]:
    """完整验证快照；Runtime 同一 run 内复用首次冻结的 manifest。"""

    snapshot = Path(path).resolve()
    session = current_artifact_verification()
    if session is None:
        return _verify_parquet_snapshot_uncached(snapshot)
    return session.resolve(
        "parquet_snapshot",
        str(snapshot),
        lambda: _verify_parquet_snapshot_uncached(snapshot),
        descriptor={"path": snapshot},
    )


def _verify_parquet_snapshot_uncached(path: str | Path) -> dict[str, Any]:
    snapshot = _PATH_POLICY.resolve_root(path, role="parquet_snapshot_root")
    if not (snapshot / "manifest.json").is_file() or not (snapshot / "COMMITTED").is_file():
        raise SnapshotIntegrityError("Parquet 快照缺少 manifest 或提交标记")
    manifest_path = _PATH_POLICY.resolve_contained_path(
        allowed_root=snapshot,
        candidate="manifest.json",
        root_role="parquet_snapshot_root",
        path_role="parquet_manifest",
        expected_kind="file",
    )
    marker_path = _PATH_POLICY.resolve_contained_path(
        allowed_root=snapshot,
        candidate="COMMITTED",
        root_role="parquet_snapshot_root",
        path_role="parquet_commit_marker",
        expected_kind="file",
    )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotIntegrityError("Parquet manifest 无法读取") from exc
    if not isinstance(manifest, dict):
        raise SnapshotIntegrityError("Parquet manifest 必须是对象")
    physical_id = str(manifest.get("physical_snapshot_id", ""))
    if marker_path.read_text(encoding="utf-8").strip() != physical_id:
        raise SnapshotIntegrityError("提交标记与 physical identity 不一致")
    if typed_canonical_hash(_manifest_identity_payload(manifest)) != physical_id:
        raise SnapshotIntegrityError("physical snapshot identity 校验失败")
    raw_files = manifest.get("files", [])
    if not isinstance(raw_files, list) or any(not isinstance(item, dict) for item in raw_files):
        raise SnapshotIntegrityError("Parquet manifest files 无效")
    declared = {str(item.get("relative_path", "")): item for item in raw_files}
    if len(declared) != len(raw_files):
        raise SnapshotIntegrityError("Parquet manifest 文件路径重复")
    actual = _PATH_POLICY.discover_files(
        snapshot,
        suffix=".parquet",
        root_role="parquet_snapshot_root",
        file_role="parquet_partition",
    )
    if not declared:
        raise SnapshotIntegrityError("Parquet manifest 文件声明不能为空")
    for relative in declared:
        _PATH_POLICY._safe_relative_path(relative, "parquet_partition")
    if set(declared) != set(actual):
        raise SnapshotIntegrityError("Parquet 文件集合与 manifest 不一致")
    for relative, item in declared.items():
        file_path = actual[relative]
        if file_path.stat().st_size != int(item["size"]) or _sha256(file_path) != item["sha256"]:
            raise SnapshotIntegrityError(f"Parquet 文件损坏: {relative}")
    return manifest


__all__ = [
    "PHYSICAL_SNAPSHOT_VERSION",
    "PhysicalSnapshotPolicy",
    "publish_parquet_snapshot",
    "verify_parquet_snapshot",
]
