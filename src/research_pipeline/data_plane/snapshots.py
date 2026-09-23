"""不可变 Parquet 快照的写入、原子发布与校验。"""

from __future__ import annotations

from dataclasses import dataclass
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
from .verification_lifecycle import current_artifact_verification


PHYSICAL_SNAPSHOT_VERSION = "physical-snapshot-v1"
_PATH_POLICY = PathRolePolicy()


@dataclass(frozen=True)
class PhysicalSnapshotPolicy:
    policy_id: str = "daily-year-month-v1"
    compression: str = "zstd"
    row_group_size: int = 65_536
    partitioning: str = "event-year-month"
    writer_version: str = "pyarrow-parquet-v1"

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "compression": self.compression,
            "row_group_size": self.row_group_size,
            "partitioning": self.partitioning,
            "writer_version": self.writer_version,
        }


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
    import pyarrow as pa
    import pyarrow.parquet as pq

    policy = policy or PhysicalSnapshotPolicy()
    if temporal_source:
        policy = PhysicalSnapshotPolicy(
            policy_id="temporal-primary-key-stream-v1",
            compression=policy.compression,
            row_group_size=policy.row_group_size,
            partitioning="primary-key-stream",
            writer_version=policy.writer_version,
        )
    _PATH_POLICY.validate({"snapshot_output_root": root})
    root_path = Path(root).resolve(strict=False)
    root_path.mkdir(parents=True, exist_ok=True)
    root_path = _PATH_POLICY.resolve_root(root_path, role="snapshot_output_root")
    staging = root_path / f".staging-{uuid.uuid4().hex}"
    staging.mkdir()
    partition_file_counts: dict[tuple[int, int], int] = {}
    temporal_part_index = 0
    rows = 0
    minimum: str | None = None
    maximum: str | None = None
    if temporal_source != plan.temporal_selection.requires_consumer_binding:
        raise SnapshotIntegrityError("逐决策时态来源模式与 admitted plan 不一致")
    gate = QualityGateSession(plan, temporal_source=temporal_source)
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
            if temporal_source:
                # 时态消费者依赖已验证的主键顺序做单遍选择；按事件月重排会
                # 打散跨月实体顺序。连续编号仍允许 Parquet 统计量裁剪文件。
                directory = staging / "primary-key-stream"
                directory.mkdir(parents=True, exist_ok=True)
                file_path = directory / f"part-{temporal_part_index:012d}.parquet"
                temporal_part_index += 1
                with pq.ParquetWriter(
                    file_path,
                    schema,
                    compression=policy.compression,
                ) as writer:
                    writer.write_batch(
                        batch,
                        row_group_size=policy.row_group_size,
                    )
                continue
            groups: dict[tuple[int, int], list[int]] = {}
            for index, value in enumerate(dates):
                groups.setdefault((value.year, value.month), []).append(index)
            for key, indices in sorted(groups.items()):
                year, month = key
                directory = staging / f"year={year:04d}" / f"month={month:02d}"
                directory.mkdir(parents=True, exist_ok=True)
                part_index = partition_file_counts.get(key, 0)
                file_path = directory / f"part-{part_index:05d}.parquet"
                partition_file_counts[key] = part_index + 1
                # 单次只保留一个 writer；不让跨年/月 writer 数随总分区数增长。
                with pq.ParquetWriter(
                    file_path,
                    schema,
                    compression=policy.compression,
                ) as writer:
                    writer.write_batch(
                        batch.take(pa.array(indices)),
                        row_group_size=policy.row_group_size,
                    )
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
    _PATH_POLICY.resolve_manifest_files(
        allowed_root=snapshot,
        relative_paths=tuple(declared),
        root_role="parquet_snapshot_root",
        file_role="parquet_partition",
    )
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
