"""分区 Parquet 引用与有界扫描。

该合同只保存分区位置和 Parquet footer 身份，不复制原始行，也不对原始
内容再计算一遍哈希。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Iterator, Mapping

from research_pipeline.platform import typed_canonical_hash

from .errors import SnapshotIntegrityError
from .path_policy import PathRolePolicy
from .query_ir import source_local_naive
from .verification_lifecycle import current_artifact_verification


PARTITIONED_DATASET_CONTRACT = "partitioned-dataset"
PARTITIONED_DATASET_REF_CONTRACT = "partitioned-dataset-ref"
_HASH_LENGTH = 64


def _require_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SnapshotIntegrityError(f"{field} 必须是非空字符串")
    return value


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != _HASH_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SnapshotIntegrityError(f"{field} 必须是 sha256 小写摘要")
    return value


def _require_relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise SnapshotIntegrityError("分区路径必须是 POSIX 相对路径")
    path = PurePosixPath(value)
    if path.is_absolute() or ":" in path.parts[0] or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise SnapshotIntegrityError("分区路径越界或不规范")
    return path.as_posix()


def _freeze_mapping(value: Mapping[str, object], field: str) -> Mapping[str, object]:
    if any(not isinstance(key, str) or not key for key in value):
        raise SnapshotIntegrityError(f"{field} 键必须是非空字符串")
    normalized = dict(sorted(value.items()))
    try:
        typed_canonical_hash(normalized)
    except (TypeError, ValueError) as exc:
        raise SnapshotIntegrityError(f"{field} 不是可封存的确定性数据") from exc
    return MappingProxyType(normalized)


@dataclass(frozen=True)
class DatasetPartitionRef:
    """一个可独立提交和恢复的 Parquet 分区。"""

    partition_key: str
    logical_start: datetime
    logical_end: datetime
    root_role: str
    relative_path: str
    source_kind: str
    size: int
    mtime_ns: int
    row_count: int
    row_groups: int
    schema_hash: str
    sort_keys: tuple[str, ...]
    lineage: Mapping[str, object]

    def __post_init__(self) -> None:
        _require_text(self.partition_key, "partition_key")
        _require_text(self.root_role, "root_role")
        object.__setattr__(self, "relative_path", _require_relative_path(self.relative_path))
        if self.source_kind not in {"catalog_raw", "runtime_derived"}:
            raise SnapshotIntegrityError("分区 source_kind 不受支持")
        if (
            self.logical_start.tzinfo is None
            or self.logical_start.utcoffset() is None
            or self.logical_end.tzinfo is None
            or self.logical_end.utcoffset() is None
            or self.logical_start >= self.logical_end
        ):
            raise SnapshotIntegrityError("分区逻辑时间必须是有时区的非空半开区间")
        if any(type(value) is not int or value < 0 for value in (self.size, self.mtime_ns)):
            raise SnapshotIntegrityError("分区文件大小和 mtime 必须是非负整数")
        if any(
            type(value) is not int or value < 0
            for value in (self.row_count, self.row_groups)
        ):
            raise SnapshotIntegrityError("分区行数和 row group 数必须是非负整数")
        if self.row_count > 0 and self.row_groups == 0:
            raise SnapshotIntegrityError("非空分区必须包含 row group")
        _require_hash(self.schema_hash, "partition.schema_hash")
        if not self.sort_keys or self.sort_keys != tuple(dict.fromkeys(self.sort_keys)):
            raise SnapshotIntegrityError("分区排序键必须非空且唯一")
        if any(not isinstance(value, str) or not value for value in self.sort_keys):
            raise SnapshotIntegrityError("分区排序键无效")
        object.__setattr__(self, "lineage", _freeze_mapping(self.lineage, "partition.lineage"))

    def to_dict(self) -> dict[str, object]:
        return {
            "partition_key": self.partition_key,
            "logical_start": self.logical_start.isoformat(timespec="microseconds"),
            "logical_end": self.logical_end.isoformat(timespec="microseconds"),
            "root_role": self.root_role,
            "relative_path": self.relative_path,
            "source_kind": self.source_kind,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "row_count": self.row_count,
            "row_groups": self.row_groups,
            "schema_hash": self.schema_hash,
            "sort_keys": list(self.sort_keys),
            "lineage": dict(self.lineage),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DatasetPartitionRef":
        expected = {
            "partition_key",
            "logical_start",
            "logical_end",
            "root_role",
            "relative_path",
            "source_kind",
            "size",
            "mtime_ns",
            "row_count",
            "row_groups",
            "schema_hash",
            "sort_keys",
            "lineage",
        }
        if (
            set(payload) != expected
            or not isinstance(payload["sort_keys"], (list, tuple))
            or not isinstance(payload["lineage"], Mapping)
        ):
            raise SnapshotIntegrityError("DatasetPartitionRef schema 无效")
        try:
            logical_start = datetime.fromisoformat(str(payload["logical_start"]))
            logical_end = datetime.fromisoformat(str(payload["logical_end"]))
        except ValueError as exc:
            raise SnapshotIntegrityError("分区逻辑时间无效") from exc
        return cls(
            partition_key=str(payload["partition_key"]),
            logical_start=logical_start,
            logical_end=logical_end,
            root_role=str(payload["root_role"]),
            relative_path=str(payload["relative_path"]),
            source_kind=str(payload["source_kind"]),
            size=payload["size"],
            mtime_ns=payload["mtime_ns"],
            row_count=payload["row_count"],
            row_groups=payload["row_groups"],
            schema_hash=str(payload["schema_hash"]),
            sort_keys=tuple(payload["sort_keys"]),
            lineage=dict(payload["lineage"]),
        )


@dataclass(frozen=True)
class PartitionedDatasetRef:
    """小型分区 manifest；数据行仍位于已批准的只读根目录。"""

    dataset_id: str
    timestamp_field: str
    instrument_field: str
    instruments: tuple[str, ...]
    allowed_columns: tuple[str, ...]
    partitions: tuple[DatasetPartitionRef, ...]
    lineage: Mapping[str, object]
    universe_snapshot_id: str | None = None
    contract_version: str = PARTITIONED_DATASET_REF_CONTRACT

    def __post_init__(self) -> None:
        _require_text(self.dataset_id, "dataset_id")
        _require_text(self.timestamp_field, "timestamp_field")
        _require_text(self.instrument_field, "instrument_field")
        if (
            not self.allowed_columns
            or self.allowed_columns != tuple(dict.fromkeys(self.allowed_columns))
            or {self.timestamp_field, self.instrument_field} - set(self.allowed_columns)
        ):
            raise SnapshotIntegrityError("分区数据列合同无效")
        if any(not isinstance(value, str) or not value for value in self.allowed_columns):
            raise SnapshotIntegrityError("分区数据列合同无效")
        if self.instruments != tuple(sorted(set(self.instruments))) or any(
            not isinstance(value, str) or not value for value in self.instruments
        ):
            raise SnapshotIntegrityError("分区数据标的必须唯一且排序")
        if bool(self.instruments) == bool(self.universe_snapshot_id):
            raise SnapshotIntegrityError(
                "分区数据必须且只能声明代码集合或 Universe snapshot"
            )
        if not self.partitions or self.partitions != tuple(
            sorted(self.partitions, key=lambda value: value.partition_key)
        ):
            raise SnapshotIntegrityError("分区 manifest 必须非空且按 key 排序")
        if len({item.partition_key for item in self.partitions}) != len(self.partitions):
            raise SnapshotIntegrityError("分区 manifest 存在重复 key")
        schema_hashes = {item.schema_hash for item in self.partitions}
        if len(schema_hashes) != 1:
            raise SnapshotIntegrityError("同一数据集分区 schema 不一致")
        for previous, current in zip(self.partitions, self.partitions[1:]):
            if previous.logical_end > current.logical_start:
                raise SnapshotIntegrityError("分区逻辑时间重叠")
        if any(set(item.sort_keys) - set(self.allowed_columns) for item in self.partitions):
            raise SnapshotIntegrityError("分区排序键不在批准列中")
        object.__setattr__(self, "lineage", _freeze_mapping(self.lineage, "dataset.lineage"))
        if self.contract_version != PARTITIONED_DATASET_REF_CONTRACT:
            raise SnapshotIntegrityError("分区数据引用合同不受支持")

    @property
    def reference_id(self) -> str:
        return typed_canonical_hash(self.to_dict())

    @property
    def schema_hash(self) -> str:
        return self.partitions[0].schema_hash

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "dataset_id": self.dataset_id,
            "timestamp_field": self.timestamp_field,
            "instrument_field": self.instrument_field,
            "instruments": list(self.instruments),
            "allowed_columns": list(self.allowed_columns),
            "partitions": [item.to_dict() for item in self.partitions],
            "lineage": dict(self.lineage),
            "universe_snapshot_id": self.universe_snapshot_id,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PartitionedDatasetRef":
        expected = {
            "contract_version",
            "dataset_id",
            "timestamp_field",
            "instrument_field",
            "instruments",
            "allowed_columns",
            "partitions",
            "lineage",
            "universe_snapshot_id",
        }
        if (
            set(payload) != expected
            or not isinstance(payload["instruments"], (list, tuple))
            or not isinstance(payload["allowed_columns"], (list, tuple))
            or not isinstance(payload["partitions"], (list, tuple))
            or any(not isinstance(item, Mapping) for item in payload["partitions"])
            or not isinstance(payload["lineage"], Mapping)
        ):
            raise SnapshotIntegrityError("PartitionedDatasetRef schema 无效")
        return cls(
            dataset_id=str(payload["dataset_id"]),
            timestamp_field=str(payload["timestamp_field"]),
            instrument_field=str(payload["instrument_field"]),
            instruments=tuple(payload["instruments"]),
            allowed_columns=tuple(payload["allowed_columns"]),
            partitions=tuple(DatasetPartitionRef.from_dict(item) for item in payload["partitions"]),
            lineage=dict(payload["lineage"]),
            universe_snapshot_id=(
                None
                if payload["universe_snapshot_id"] is None
                else str(payload["universe_snapshot_id"])
            ),
            contract_version=str(payload["contract_version"]),
        )


class VerifiedDatasetPartition:
    """只暴露当前分区的列裁剪、谓词下推和拉取式 RecordBatch。"""

    def __init__(
        self,
        *,
        reference: DatasetPartitionRef,
        path: Path,
        timestamp_field: str,
        instrument_field: str,
        instruments: tuple[str, ...],
        universe_snapshot_id: str | None,
        allowed_columns: tuple[str, ...],
        max_batch_rows: int,
        max_batch_bytes: int,
        verifier,
    ) -> None:
        self.reference = reference
        self.path = path
        self.timestamp_field = timestamp_field
        self.instrument_field = instrument_field
        self.instruments = instruments
        self.universe_snapshot_id = universe_snapshot_id
        self.allowed_columns = allowed_columns
        self.max_batch_rows = max_batch_rows
        self.max_batch_bytes = max_batch_bytes
        self._verifier = verifier

    def iter_batches(
        self,
        *,
        columns: tuple[str, ...],
        batch_size: int = 65_536,
    ) -> Iterator[object]:
        import pyarrow as pa
        import pyarrow.dataset as ds

        if not columns or columns != tuple(dict.fromkeys(columns)):
            raise SnapshotIntegrityError("分区扫描列必须非空且唯一")
        unknown = set(columns) - set(self.allowed_columns)
        if unknown:
            raise SnapshotIntegrityError(f"分区扫描包含未批准列: {sorted(unknown)}")
        if type(batch_size) is not int or not 0 < batch_size <= self.max_batch_rows:
            raise SnapshotIntegrityError("分区扫描 batch_size 超过批准上限")
        dataset = ds.dataset(str(self.path), format="parquet")
        timestamp = ds.field(self.timestamp_field)
        instrument = ds.field(self.instrument_field)
        timestamp_type = dataset.schema.field(self.timestamp_field).type
        if pa.types.is_timestamp(timestamp_type) and timestamp_type.tz is not None:
            logical_start = self.reference.logical_start
            logical_end = self.reference.logical_end
        else:
            logical_start = source_local_naive(self.reference.logical_start)
            logical_end = source_local_naive(self.reference.logical_end)
        expression = (
            (timestamp >= logical_start)
            & (timestamp < logical_end)
        )
        if self.instruments:
            expression &= instrument.isin(list(self.instruments))
        scanner = dataset.scanner(
            columns=list(columns),
            filter=expression,
            batch_size=batch_size,
            batch_readahead=2,
            fragment_readahead=1,
            use_threads=False,
        )
        for batch in scanner.to_batches():
            if int(batch.nbytes) > self.max_batch_bytes:
                raise SnapshotIntegrityError("分区 RecordBatch 超过批准内存上限")
            yield batch
        self._verifier()

    def __reduce__(self):
        raise TypeError("VerifiedDatasetPartition 不允许 pickle")


class VerifiedPartitionedDataset:
    """已验证的分区集合；Supervisor 可按 key 只解析一个分区。"""

    def __init__(self, reference: PartitionedDatasetRef, resolver: "PartitionedDatasetResolver") -> None:
        self.reference = reference
        self._resolver = resolver

    def iter_partitions(self) -> Iterator[VerifiedDatasetPartition]:
        for partition in self.reference.partitions:
            yield self._resolver.resolve_partition(self.reference, partition.partition_key)

    def partition(self, partition_key: str) -> VerifiedDatasetPartition:
        return self._resolver.resolve_partition(self.reference, partition_key)


class PartitionedDatasetResolver:
    """根据 Supervisor 批准的角色根目录解析分区。"""

    def __init__(
        self,
        allowed_roots: Mapping[str, str | Path],
        *,
        max_batch_rows: int = 65_536,
        max_batch_bytes: int = 32 * 1024 * 1024,
    ) -> None:
        if not allowed_roots:
            raise SnapshotIntegrityError("分区 resolver 必须有至少一个批准根目录")
        self.path_policy = PathRolePolicy()
        self.allowed_roots = MappingProxyType({
            _require_text(role, "allowed_root.role"): self.path_policy.resolve_root(
                path, role=role
            )
            for role, path in sorted(allowed_roots.items())
        })
        if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_bytes)):
            raise SnapshotIntegrityError("batch 行数和字节上限必须是正整数")
        self.max_batch_rows = max_batch_rows
        self.max_batch_bytes = max_batch_bytes

    def resolve(self, reference: PartitionedDatasetRef) -> VerifiedPartitionedDataset:
        for partition in reference.partitions:
            self._resolve_and_verify(partition)
        return VerifiedPartitionedDataset(reference, self)

    def resolve_partition(
        self,
        reference: PartitionedDatasetRef,
        partition_key: str,
    ) -> VerifiedDatasetPartition:
        matches = [item for item in reference.partitions if item.partition_key == partition_key]
        if len(matches) != 1:
            raise SnapshotIntegrityError("分区 key 不存在或不唯一")
        expected = matches[0]
        path = self._resolve_and_verify(expected)
        return VerifiedDatasetPartition(
            reference=expected,
            path=path,
            timestamp_field=reference.timestamp_field,
            instrument_field=reference.instrument_field,
            instruments=reference.instruments,
            universe_snapshot_id=reference.universe_snapshot_id,
            allowed_columns=reference.allowed_columns,
            max_batch_rows=self.max_batch_rows,
            max_batch_bytes=self.max_batch_bytes,
            verifier=lambda: self._resolve_and_verify(expected),
        )

    def _resolve_and_verify(self, expected: DatasetPartitionRef) -> Path:
        root = self.allowed_roots.get(expected.root_role)
        if root is None:
            raise SnapshotIntegrityError(f"分区 root_role 未经 Supervisor 批准: {expected.root_role}")
        session = current_artifact_verification()
        if session is not None:
            identity = (
                f"{root}|{typed_canonical_hash(expected.to_dict())}"
            )
            return session.resolve(
                "partitioned_dataset",
                identity,
                lambda: self._resolve_and_verify_uncached(expected, root),
            )
        return self._resolve_and_verify_uncached(expected, root)

    def _resolve_and_verify_uncached(
        self,
        expected: DatasetPartitionRef,
        root: Path,
    ) -> Path:
        path = self.path_policy.resolve_contained_path(
            allowed_root=root,
            candidate=expected.relative_path,
            root_role=expected.root_role,
            path_role="partitioned_dataset_file",
            expected_kind="file",
        )
        current = inspect_parquet_partition(
            path,
            allowed_root=root,
            partition_key=expected.partition_key,
            logical_start=expected.logical_start,
            logical_end=expected.logical_end,
            root_role=expected.root_role,
            source_kind=expected.source_kind,
            sort_keys=expected.sort_keys,
            lineage=expected.lineage,
        )
        if current != expected:
            raise SnapshotIntegrityError(f"分区来源身份发生漂移: {expected.partition_key}")
        return path


def inspect_parquet_partition(
    path: str | Path,
    *,
    allowed_root: str | Path,
    partition_key: str,
    logical_start: datetime,
    logical_end: datetime,
    root_role: str,
    source_kind: str,
    sort_keys: tuple[str, ...],
    lineage: Mapping[str, object],
) -> DatasetPartitionRef:
    """只读取文件系统元数据和 Parquet footer，不扫描数据行。"""

    import pyarrow.parquet as pq

    policy = PathRolePolicy()
    root = policy.resolve_root(allowed_root, role=root_role)
    resolved = policy.resolve_contained_path(
        allowed_root=root,
        candidate=path,
        root_role=root_role,
        path_role="partitioned_dataset_file",
        expected_kind="file",
    )
    stat = resolved.stat()
    parquet = pq.ParquetFile(resolved)
    schema = parquet.schema_arrow
    return DatasetPartitionRef(
        partition_key=partition_key,
        logical_start=logical_start,
        logical_end=logical_end,
        root_role=root_role,
        relative_path=resolved.relative_to(root).as_posix(),
        source_kind=source_kind,
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        row_count=parquet.metadata.num_rows,
        row_groups=parquet.metadata.num_row_groups,
        schema_hash=typed_canonical_hash(str(schema)),
        sort_keys=sort_keys,
        lineage=lineage,
    )


__all__ = [
    "PARTITIONED_DATASET_CONTRACT",
    "PARTITIONED_DATASET_REF_CONTRACT",
    "DatasetPartitionRef",
    "PartitionedDatasetRef",
    "PartitionedDatasetResolver",
    "VerifiedDatasetPartition",
    "VerifiedPartitionedDataset",
    "inspect_parquet_partition",
]
