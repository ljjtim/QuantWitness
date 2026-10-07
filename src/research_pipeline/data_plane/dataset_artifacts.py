"""不可变 Parquet 工件引用、校验与有界批扫描。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
import math
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Callable, Iterator, Mapping

from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import SnapshotIntegrityError
from .admission import AdmittedQueryPlan
from .path_policy import PathRolePolicy
from .query_ir import FilterOperator
from .snapshots import verify_parquet_snapshot
from .verification_lifecycle import current_artifact_verification


DATASET_ARTIFACT_REF_VERSION = "dataset-artifact-ref-v1"
_HASH_LENGTH = 64


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != _HASH_LENGTH or any(
        char not in "0123456789abcdef" for char in value
    ):
        raise SnapshotIntegrityError(f"{field} 必须是 sha256 小写摘要")
    return value


def _require_relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise SnapshotIntegrityError("dataset artifact path 必须是 POSIX 相对路径")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or ":" in path.parts[0]
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise SnapshotIntegrityError("dataset artifact path 越界或不规范")
    return path.as_posix()


@dataclass(frozen=True)
class DatasetArtifactRef:
    relative_path: str
    physical_snapshot_id: str
    manifest_hash: str
    schema_hash: str
    source_revision_hash: str
    partitions: tuple[str, ...]
    contract_version: str = DATASET_ARTIFACT_REF_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "relative_path", _require_relative_path(self.relative_path))
        for field in (
            "physical_snapshot_id",
            "manifest_hash",
            "schema_hash",
            "source_revision_hash",
        ):
            _require_hash(getattr(self, field), field)
        normalized = tuple(_require_relative_path(item) for item in self.partitions)
        if not normalized or normalized != tuple(sorted(set(normalized))):
            raise SnapshotIntegrityError("dataset artifact partitions 必须非空、唯一并排序")
        object.__setattr__(self, "partitions", normalized)
        if self.contract_version != DATASET_ARTIFACT_REF_VERSION:
            raise SnapshotIntegrityError("dataset artifact ref version 不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "physical_snapshot_id": self.physical_snapshot_id,
            "manifest_hash": self.manifest_hash,
            "schema_hash": self.schema_hash,
            "source_revision_hash": self.source_revision_hash,
            "partitions": list(self.partitions),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DatasetArtifactRef":
        expected = {
            "relative_path", "physical_snapshot_id", "manifest_hash", "schema_hash",
            "source_revision_hash", "partitions", "contract_version",
        }
        if set(payload) != expected or not isinstance(payload["partitions"], (list, tuple)):
            raise SnapshotIntegrityError("DatasetArtifactRef schema 无效")
        strings = {field: payload[field] for field in expected - {"partitions"}}
        if any(not isinstance(value, str) for value in strings.values()):
            raise SnapshotIntegrityError("DatasetArtifactRef 字符串字段类型无效")
        return cls(
            relative_path=payload["relative_path"],
            physical_snapshot_id=payload["physical_snapshot_id"],
            manifest_hash=payload["manifest_hash"],
            schema_hash=payload["schema_hash"],
            source_revision_hash=payload["source_revision_hash"],
            partitions=tuple(payload["partitions"]),
            contract_version=payload["contract_version"],
        )


@dataclass(frozen=True)
class DatasetFilter:
    field: str
    operator: str
    value: object = None

    def __post_init__(self) -> None:
        if not isinstance(self.field, str) or not self.field:
            raise SnapshotIntegrityError("dataset filter field 无效")
        if self.operator not in {
            "eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_valid",
        }:
            raise SnapshotIntegrityError("dataset filter operator 不受支持")
        if self.operator in {"is_null", "is_valid"} and self.value is not None:
            raise SnapshotIntegrityError("空值过滤不接受 value")
        if self.operator == "in":
            if (
                not isinstance(self.value, (list, tuple))
                or not self.value
                or len(self.value) != len(set(self.value))
                or any(not _is_filter_scalar(item) for item in self.value)
            ):
                raise SnapshotIntegrityError("in 过滤必须是非空、唯一的确定性标量序列")
        elif self.operator not in {"is_null", "is_valid"}:
            if not _is_filter_scalar(self.value):
                raise SnapshotIntegrityError("dataset filter value 只接受确定性标量")


def _is_filter_scalar(value: object) -> bool:
    return isinstance(value, (bool, int, float, str, date, datetime)) and not (
        isinstance(value, float) and not math.isfinite(value)
    )


def _filter_expression(
    filters: tuple[DatasetFilter, ...],
    *,
    allowed_columns: tuple[str, ...],
    schema: object = None,
):
    import pyarrow as pa
    import pyarrow.dataset as ds

    expression = None
    for item in filters:
        if item.field not in allowed_columns:
            raise SnapshotIntegrityError(f"过滤请求包含未批准列: {item.field}")
        field = ds.field(item.field)
        value = item.value
        field_type = None if schema is None else schema.field(item.field).type
        if field_type is not None and pa.types.is_decimal(field_type):
            if item.operator == "in":
                value = pa.array([Decimal(str(entry)) for entry in value], type=field_type)
            elif item.operator not in {"is_null", "is_valid"}:
                value = pa.scalar(Decimal(str(value)), type=field_type)
        current = {
            "eq": lambda: field == value,
            "ne": lambda: field != value,
            "lt": lambda: field < value,
            "le": lambda: field <= value,
            "gt": lambda: field > value,
            "ge": lambda: field >= value,
            "in": lambda: field.isin(value),
            "is_null": field.is_null,
            "is_valid": field.is_valid,
        }[item.operator]()
        expression = current if expression is None else expression & current
    return expression


def _query_dataset_filters(plan: AdmittedQueryPlan) -> tuple[DatasetFilter, ...]:
    """沿用 Arrow 过滤表达式，保留 QueryIR 的闭区间和空值语义。"""

    filters: list[DatasetFilter] = []
    for predicate in plan.query.filters:
        field, values = predicate.field_id, predicate.values
        if predicate.operator == FilterOperator.RANGE:
            filters.extend((DatasetFilter(field, "ge", values[0]), DatasetFilter(field, "le", values[1])))
        elif predicate.operator == FilterOperator.IS_NULL:
            filters.append(DatasetFilter(field, "is_null" if values[0] else "is_valid"))
        elif predicate.operator == FilterOperator.IN:
            filters.append(DatasetFilter(field, "in", values))
        else:
            filters.append(DatasetFilter(field, "eq", values[0]))
    return tuple(filters)


class VerifiedDataset:
    """只暴露列裁剪、过滤下推后的 RecordBatch 迭代。"""

    def __init__(
        self,
        *,
        files: tuple[Path, ...],
        file_resolver: Callable[[], tuple[Path, ...]],
        schema: object,
        allowed_columns: tuple[str, ...],
        max_batch_rows: int,
        max_batch_bytes: int,
        manifest: Mapping[str, object],
        reference: DatasetArtifactRef,
    ) -> None:
        self._files = files
        self._file_resolver = file_resolver
        self.schema = schema
        self.allowed_columns = allowed_columns
        self.max_batch_rows = max_batch_rows
        self.max_batch_bytes = max_batch_bytes
        self.manifest = MappingProxyType(dict(manifest))
        self._reference = reference

    def iter_batches(
        self,
        *,
        columns: tuple[str, ...],
        filters: tuple[DatasetFilter, ...] = (),
        batch_size: int = 65_536,
    ) -> Iterator[object]:
        import pyarrow.dataset as ds

        if self.manifest.get("temporal_source") is True:
            raise SnapshotIntegrityError(
                "逐决策时态来源必须通过 iter_batches_at 传入 decision/observation time"
            )

        if not columns or len(columns) != len(set(columns)):
            raise SnapshotIntegrityError("扫描列必须非空且唯一")
        unknown = set(columns) - set(self.allowed_columns)
        if unknown:
            raise SnapshotIntegrityError(f"扫描请求包含未批准列: {sorted(unknown)}")
        if type(batch_size) is not int or batch_size <= 0 or batch_size > self.max_batch_rows:
            raise SnapshotIntegrityError("扫描 batch_size 超过批准上限")
        expression = _filter_expression(
            filters,
            allowed_columns=self.allowed_columns,
            schema=self.schema,
        )
        files = self._require_current_binding()
        dataset = ds.dataset([str(path) for path in files], format="parquet")
        scanner = dataset.scanner(
            columns=list(columns),
            filter=expression,
            batch_size=batch_size,
            batch_readahead=0,
            fragment_readahead=1,
            use_threads=False,
            fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False),
        )
        for batch in scanner.to_batches():
            if int(batch.nbytes) > self.max_batch_bytes:
                raise SnapshotIntegrityError("扫描 RecordBatch 超过批准内存上限")
            yield batch

    def parquet_uncompressed_bytes(self, *, columns: tuple[str, ...]) -> int:
        """只读 footer，返回完整物化所选列前可知的工作集下界。"""

        import pyarrow.parquet as pq

        if not columns or columns != tuple(dict.fromkeys(columns)):
            raise SnapshotIntegrityError("完整矩阵预检列必须非空且唯一")
        unknown = set(columns) - set(self.allowed_columns)
        if unknown:
            raise SnapshotIntegrityError(
                f"完整矩阵预检包含未批准列: {sorted(unknown)}"
            )
        total = 0
        for path in self._require_current_binding():
            parquet = pq.ParquetFile(path)
            names = tuple(parquet.schema.names)
            indexes = tuple(names.index(name) for name in columns)
            for row_group_index in range(parquet.metadata.num_row_groups):
                row_group = parquet.metadata.row_group(row_group_index)
                total += sum(
                    int(row_group.column(index).total_uncompressed_size)
                    for index in indexes
                )
        return total

    def iter_batches_at(
        self,
        *,
        plan: AdmittedQueryPlan,
        consumer_time: str | date | datetime,
        columns: tuple[str, ...],
        filters: tuple[DatasetFilter, ...] = (),
        batch_size: int = 65_536,
    ) -> Iterator[object]:
        """在正式消费边界按单个样本时点选择版本，并裁剪回公开列。"""

        import pyarrow as pa
        import pyarrow.dataset as ds

        if self.manifest.get("temporal_source") is not True:
            raise SnapshotIntegrityError("普通数据工件不接受逐决策时态选择")
        if self.manifest.get("admitted_plan_hash") != plan.plan_hash:
            raise SnapshotIntegrityError("逐决策时态来源与 admitted plan 不一致")
        if not columns or len(columns) != len(set(columns)):
            raise SnapshotIntegrityError("扫描列必须非空且唯一")
        if not set(columns) <= set(plan.query.field_ids):
            raise SnapshotIntegrityError("逐决策扫描只能返回 QueryIR 公开列")
        if type(batch_size) is not int or batch_size <= 0 or batch_size > self.max_batch_rows:
            raise SnapshotIntegrityError("扫描 batch_size 超过批准上限")
        temporal = plan.temporal_selection
        all_filters = (*_query_dataset_filters(plan), *filters)
        expression = _filter_expression(
            all_filters, allowed_columns=temporal.required_scan_fields, schema=self.schema,
        )
        stable_filters = tuple(
            item for item in all_filters if item.field in temporal.stable_filter_fields
        )
        records = (
            row
            for batch in self._iter_ordered_file_batches(
                columns=temporal.required_scan_fields,
                filters=stable_filters,
                allowed_columns=temporal.required_scan_fields,
                batch_size=batch_size,
            )
            for row in batch.to_pylist()
        )
        # 内部保留过滤和排序列，完成选择后才裁剪公开输出。
        selection = replace(temporal, public_projection=temporal.required_scan_fields)
        selected_records = selection.iter_grouped_selected_records(
            records,
            consumer_time=consumer_time,
            primary_key=plan.primary_key,
            order_by=tuple((item.field_id, item.descending) for item in plan.query.sort),
        )
        scan_schema = pa.schema([self.schema.field(field) for field in temporal.required_scan_fields])
        remaining = plan.query.limit

        def output_batches(rows: list[dict[str, object]]) -> Iterator[object]:
            nonlocal remaining
            table = pa.Table.from_batches([self._record_batch(rows, scan_schema)])
            if expression is not None:
                table = ds.dataset(table).scanner(
                    filter=expression,
                    batch_size=batch_size,
                    batch_readahead=0,
                    fragment_readahead=1,
                    use_threads=False,
                    fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False),
                ).to_table()
            table = table.select(list(columns))
            if remaining is not None:
                table = table.slice(0, remaining)
                remaining -= table.num_rows
            yield from table.to_batches(max_chunksize=batch_size)

        output_rows: list[dict[str, object]] = []
        for row in selected_records:
            output_rows.append(dict(row))
            if len(output_rows) == batch_size:
                yield from output_batches(output_rows)
                output_rows = []
                if remaining == 0:
                    return
        if output_rows:
            yield from output_batches(output_rows)

    def iter_temporal_fact_batches(
        self,
        *,
        plan: AdmittedQueryPlan,
        columns: tuple[str, ...],
        filters: tuple[DatasetFilter, ...] = (),
        batch_size: int = 65_536,
    ) -> Iterator[object]:
        """只向核心消费者提供构造真实业务时钟所需的原始事实列。"""

        if self.manifest.get("temporal_source") is not True:
            raise SnapshotIntegrityError("普通数据工件没有原始时态事实")
        if self.manifest.get("admitted_plan_hash") != plan.plan_hash:
            raise SnapshotIntegrityError("原始时态事实与 admitted plan 不一致")
        if not columns or len(columns) != len(set(columns)):
            raise SnapshotIntegrityError("时态事实列必须非空且唯一")
        if not set(columns) <= set(plan.query.field_ids):
            raise SnapshotIntegrityError("时态事实只能用于公开业务时钟字段")
        if type(batch_size) is not int or batch_size <= 0 or batch_size > self.max_batch_rows:
            raise SnapshotIntegrityError("扫描 batch_size 超过批准上限")
        yield from self._iter_ordered_file_batches(
            columns=columns,
            filters=filters,
            allowed_columns=plan.temporal_selection.required_scan_fields,
            batch_size=batch_size,
        )

    def _iter_ordered_file_batches(
        self,
        *,
        columns: tuple[str, ...],
        filters: tuple[DatasetFilter, ...],
        allowed_columns: tuple[str, ...],
        batch_size: int,
    ) -> Iterator[object]:
        """按 manifest 文件顺序单线程扫描，保持时态主键流顺序。"""

        import pyarrow.dataset as ds

        expression = _filter_expression(
            filters, allowed_columns=allowed_columns, schema=self.schema,
        )
        for path in self._require_current_binding():
            scanner = ds.dataset(str(path), format="parquet").scanner(
                columns=list(columns),
                filter=expression,
                batch_size=batch_size,
                batch_readahead=0,
                fragment_readahead=1,
                use_threads=False,
                fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False),
            )
            for batch in scanner.to_batches():
                if int(batch.nbytes) > self.max_batch_bytes:
                    raise SnapshotIntegrityError("扫描 RecordBatch 超过批准内存上限")
                yield batch

    def _require_current_binding(self) -> tuple[Path, ...]:
        """复用冻结文件列表，同时保留引用和分区选择约束。"""

        files = self._file_resolver()
        declared = tuple(
            sorted(str(item["relative_path"]) for item in self.manifest["files"])
        )
        if (
            files != self._files
            or declared != self._reference.partitions
            or typed_canonical_hash(self.manifest) != self._reference.manifest_hash
            or self.manifest.get("physical_snapshot_id")
            != self._reference.physical_snapshot_id
            or self.manifest.get("schema_hash") != self._reference.schema_hash
            or self.manifest.get("source_revision_hash")
            != self._reference.source_revision_hash
        ):
            raise SnapshotIntegrityError("dataset artifact run-scoped binding 漂移")
        return files

    def _record_batch(self, rows: list[dict[str, object]], schema: object):
        import pyarrow as pa

        batch = pa.RecordBatch.from_pylist(rows, schema=schema)
        if int(batch.nbytes) > self.max_batch_bytes:
            raise SnapshotIntegrityError("扫描 RecordBatch 超过批准内存上限")
        return batch

    def __reduce__(self):
        raise TypeError("VerifiedDataset 不允许 pickle；worker 必须重新解析不可变工件引用")
class ArtifactResolver:
    """由 supervisor 固定允许根，项目声明不能改变。"""

    def __init__(
        self,
        allowed_root: str | Path,
        *,
        max_batch_rows: int = 65_536,
        max_batch_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self.path_policy = PathRolePolicy()
        self.allowed_root = self.path_policy.resolve_root(
            allowed_root,
            role="dataset_artifact_allowed_root",
        )
        if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_bytes)):
            raise SnapshotIntegrityError("batch 行数和字节上限必须是正整数")
        self.max_batch_rows = max_batch_rows
        self.max_batch_bytes = max_batch_bytes

    def resolve(self, reference: DatasetArtifactRef) -> VerifiedDataset:
        session = current_artifact_verification()
        if session is None:
            manifest, files, schema = self._resolve_reference(reference)

            def file_resolver() -> tuple[Path, ...]:
                return self._resolve_reference(reference)[1]
        else:
            identity = "|".join((
                str(self.allowed_root),
                reference.relative_path,
                reference.physical_snapshot_id,
                reference.manifest_hash,
                reference.schema_hash,
                reference.source_revision_hash,
                *reference.partitions,
            ))
            manifest, files, schema = session.resolve(
                "dataset_artifact",
                identity,
                lambda: self._resolve_reference(reference),
            )

            def file_resolver() -> tuple[Path, ...]:
                return files
        return VerifiedDataset(
            files=files,
            file_resolver=file_resolver,
            schema=schema,
            allowed_columns=tuple(schema.names),
            max_batch_rows=self.max_batch_rows,
            max_batch_bytes=self.max_batch_bytes,
            manifest=manifest,
            reference=reference,
        )

    def _resolve_reference(
        self,
        reference: DatasetArtifactRef,
    ) -> tuple[Mapping[str, object], tuple[Path, ...], object]:
        import pyarrow.dataset as ds

        snapshot = self.path_policy.resolve_contained_path(
            allowed_root=self.allowed_root,
            candidate=reference.relative_path,
            root_role="dataset_artifact_allowed_root",
            path_role="dataset_snapshot_root",
            expected_kind="directory",
        )
        manifest = verify_parquet_snapshot(snapshot)
        if manifest.get("physical_snapshot_id") != reference.physical_snapshot_id:
            raise SnapshotIntegrityError("dataset artifact physical identity 漂移")
        if typed_canonical_hash(manifest) != reference.manifest_hash:
            raise SnapshotIntegrityError("dataset artifact manifest hash 漂移")
        for field in ("schema_hash", "source_revision_hash"):
            if manifest.get(field) != getattr(reference, field):
                raise SnapshotIntegrityError(f"dataset artifact {field} 漂移")
        declared = tuple(sorted(str(item["relative_path"]) for item in manifest["files"]))
        if declared != reference.partitions:
            raise SnapshotIntegrityError("dataset artifact 分区声明漂移")
        files = self.path_policy.resolve_manifest_files(
            allowed_root=snapshot,
            relative_paths=reference.partitions,
            root_role="dataset_snapshot_root",
            file_role="dataset_partition",
        )
        dataset = ds.dataset([str(path) for path in files], format="parquet")
        if typed_canonical_hash(str(dataset.schema)) != reference.schema_hash:
            raise SnapshotIntegrityError("dataset artifact 实际 schema 漂移")
        return manifest, files, dataset.schema


def build_dataset_artifact_ref(snapshot: str | Path, *, allowed_root: str | Path) -> DatasetArtifactRef:
    policy = PathRolePolicy()
    root = policy.resolve_root(allowed_root, role="dataset_artifact_allowed_root")
    path = policy.resolve_contained_path(
        allowed_root=root,
        candidate=snapshot,
        root_role="dataset_artifact_allowed_root",
        path_role="dataset_snapshot_root",
        expected_kind="directory",
    )
    relative = path.relative_to(root).as_posix()
    manifest = verify_parquet_snapshot(path)
    return DatasetArtifactRef(
        relative_path=relative,
        physical_snapshot_id=str(manifest["physical_snapshot_id"]),
        manifest_hash=typed_canonical_hash(manifest),
        schema_hash=str(manifest["schema_hash"]),
        source_revision_hash=str(manifest["source_revision_hash"]),
        partitions=tuple(sorted(str(item["relative_path"]) for item in manifest["files"])),
    )


__all__ = [
    "DATASET_ARTIFACT_REF_VERSION",
    "ArtifactResolver",
    "DatasetArtifactRef",
    "DatasetFilter",
    "VerifiedDataset",
    "build_dataset_artifact_ref",
]
