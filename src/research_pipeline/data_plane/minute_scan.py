"""分钟 Parquet 的准入分区计划与只读引用发布。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Iterator

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.domain.session_calendar import CURRENT_SESSION_POLICY_BUNDLE_HASH

from .admission import AdmittedQueryPlan
from .errors import ProviderExecutionError, QueryIRInvalidError
from .partitioned_artifacts import DatasetPartitionRef, PartitionedDatasetRef
from .path_policy import PathRolePolicy
from .query_ir import InstantRangeV2


MINUTE_SCAN_PLAN_VERSION = "minute-scan-plan-v1"
STANDARD_MINUTE_COLUMNS = (
    "code",
    "dt",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "avg",
)
FUTURES_MINUTE_COLUMNS = (*STANDARD_MINUTE_COLUMNS, "open_interest")
_ASSET_SOURCE_DIRECTORIES = {
    "cn_stock": "stock",
    "cn_etf": "fund",
    "cn_index": "index",
    "cn_future": "futures",
}
_PARTITION_PATH = re.compile(
    r"^(stock|fund|index|futures)/year=(\d{4})/month=(\d{2})/data\.parquet$"
)


@dataclass(frozen=True)
class MinuteScanBudget:
    max_selected_file_bytes: int = 64 * 1024**3
    max_batch_bytes: int = 64 * 1024**2

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value <= 0
            for value in (
                self.max_selected_file_bytes,
                self.max_batch_bytes,
            )
        ):
            raise QueryIRInvalidError("分钟扫描预算必须是正整数")

    def to_dict(self) -> dict[str, int]:
        return {
            "max_selected_file_bytes": self.max_selected_file_bytes,
            "max_batch_bytes": self.max_batch_bytes,
        }


@dataclass(frozen=True)
class MinuteSourcePartition:
    relative_path: str
    size: int
    mtime_ns: int
    row_count: int
    row_groups: int
    schema_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "row_count": self.row_count,
            "row_groups": self.row_groups,
            "schema_hash": self.schema_hash,
            "evidence_strength": "parquet-footer-and-filesystem-metadata",
        }


@dataclass(frozen=True)
class MinuteScanPlan:
    admitted_plan_hash: str
    source_relative_path: str
    source_columns: tuple[str, ...]
    instruments: tuple[str, ...]
    start_at: datetime
    end_at: datetime
    as_of: datetime
    partitions: tuple[MinuteSourcePartition, ...]
    query_max_rows: int
    query_max_bytes: int
    batch_size: int
    budget: MinuteScanBudget
    minute_asset_class: str
    minute_session_policy_ref: str
    minute_quality_policy_refs: tuple[str, ...]
    minute_capability_manifest_hash: str
    universe_snapshot_id: str | None = None
    contract_version: str = MINUTE_SCAN_PLAN_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != MINUTE_SCAN_PLAN_VERSION:
            raise QueryIRInvalidError("MinuteScanPlan version 不受支持")
        if not self.partitions:
            raise QueryIRInvalidError("分钟扫描分区为空")
        if sum(item.size for item in self.partitions) > self.budget.max_selected_file_bytes:
            raise QueryIRInvalidError("分钟扫描所选源文件超过字节上限")
        physical_columns = (
            FUTURES_MINUTE_COLUMNS
            if self.minute_asset_class == "cn_future"
            else STANDARD_MINUTE_COLUMNS
        )
        if (
            len(set(self.source_columns)) != len(self.source_columns)
            or not {"code", "dt"}.issubset(self.source_columns)
            or not set(self.source_columns).issubset(physical_columns)
        ):
            raise QueryIRInvalidError("分钟扫描投影必须包含 code/dt 且只能使用标准列")
        if bool(self.instruments) == bool(self.universe_snapshot_id):
            raise QueryIRInvalidError(
                "分钟扫描必须且只能声明代码集合或 Universe snapshot"
            )
        if self.start_at >= self.end_at or self.as_of < self.start_at:
            raise QueryIRInvalidError("分钟扫描没有可见的非空时点范围")
        expected_source = _ASSET_SOURCE_DIRECTORIES.get(self.minute_asset_class)
        if self.source_relative_path != expected_source:
            raise QueryIRInvalidError("分钟扫描资产类别与物理目录不一致")
        if (
            not self.minute_quality_policy_refs
            or self.minute_quality_policy_refs
            != tuple(sorted(set(self.minute_quality_policy_refs)))
        ):
            raise QueryIRInvalidError("分钟扫描 quality policy refs 必须排序且非空")
        expected_months = set(
            _months(
                self.start_at,
                min(self.end_at - timedelta(microseconds=1), self.as_of),
            )
        )
        observed_months: set[tuple[int, int]] = set()
        for partition in self.partitions:
            match = _PARTITION_PATH.fullmatch(partition.relative_path)
            if match is None or match.group(1) != expected_source:
                raise QueryIRInvalidError("分钟扫描分区路径与资产目录不一致")
            observed_months.add((int(match.group(2)), int(match.group(3))))
        if observed_months != expected_months:
            raise QueryIRInvalidError("分钟扫描分区集合与查询月份不一致")

    @property
    def source_revision_hash(self) -> str:
        return typed_canonical_hash([item.to_dict() for item in self.partitions])

    @property
    def plan_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "admitted_plan_hash": self.admitted_plan_hash,
            "source_relative_path": self.source_relative_path,
            "source_columns": list(self.source_columns),
            "instruments": list(self.instruments),
            "start_at": self.start_at.isoformat(timespec="microseconds"),
            "end_at": self.end_at.isoformat(timespec="microseconds"),
            "as_of": self.as_of.isoformat(timespec="microseconds"),
            "partitions": [item.to_dict() for item in self.partitions],
            "query_max_rows": self.query_max_rows,
            "query_max_bytes": self.query_max_bytes,
            "batch_size": self.batch_size,
            "budget": self.budget.to_dict(),
            "minute_asset_class": self.minute_asset_class,
            "minute_session_policy_ref": self.minute_session_policy_ref,
            "minute_quality_policy_refs": list(self.minute_quality_policy_refs),
            "minute_capability_manifest_hash": self.minute_capability_manifest_hash,
            "universe_snapshot_id": self.universe_snapshot_id,
            "source_revision_hash": self.source_revision_hash,
        }


def build_minute_scan_plan(
    admitted_plan: AdmittedQueryPlan,
    *,
    source: str | Path,
    allowed_root: str | Path,
    budget: MinuteScanBudget | None = None,
) -> MinuteScanPlan:
    """从已准入 QueryIR 选择精确 year/month 文件并记录来源身份。"""

    import pyarrow as pa
    import pyarrow.parquet as pq

    budget = budget or MinuteScanBudget()
    if not isinstance(admitted_plan.query.time_range, InstantRangeV2):
        raise QueryIRInvalidError("MinuteScanPlan 只接受分钟 QueryIR")
    required_identity = (
        admitted_plan.minute_asset_class,
        admitted_plan.minute_session_policy_ref,
        admitted_plan.minute_quality_policy_refs,
        admitted_plan.minute_capability_manifest_hash,
        admitted_plan.minute_timestamp_role,
        admitted_plan.minute_timestamp_storage,
        admitted_plan.minute_bar_interval,
    )
    if any(item is None for item in required_identity) or required_identity[4:] != (
        "completed_bar_end",
        "naive_local_wall_clock",
        "1m",
    ):
        raise QueryIRInvalidError("分钟扫描缺少 Catalog session/时点合同")
    inverse_columns = {physical: logical for logical, physical in admitted_plan.columns}
    physical_columns = (
        FUTURES_MINUTE_COLUMNS
        if admitted_plan.minute_asset_class == "cn_future"
        else STANDARD_MINUTE_COLUMNS
    )
    source_columns = tuple(
        physical for physical in physical_columns if physical in inverse_columns
    )
    missing = {"code", "dt"} - set(source_columns)
    if missing:
        raise QueryIRInvalidError(f"分钟扫描投影缺少必要列: {sorted(missing)}")
    unsupported = set(source_columns) - set(physical_columns)
    if unsupported:
        raise QueryIRInvalidError(f"分钟扫描投影包含非标准列: {sorted(unsupported)}")
    field_types = dict(admitted_plan.field_types)
    expected_types = {
        physical: _arrow_type(field_types[inverse_columns[physical]], pa)
        for physical in source_columns
    }
    policy = PathRolePolicy()
    root = policy.resolve_root(allowed_root, role="minute_scan_allowed_root")
    source_root = policy.resolve_contained_path(
        allowed_root=root,
        candidate=source,
        root_role="minute_scan_allowed_root",
        path_role="minute_scan_source_root",
        expected_kind="directory",
    )
    start = admitted_plan.query.time_range.start_at
    end = admitted_plan.query.time_range.end_at
    as_of = admitted_plan.query.as_of_instant
    if as_of is None or as_of < start:
        raise QueryIRInvalidError("分钟扫描 as_of 早于查询起点")
    visible_last = min(end - timedelta(microseconds=1), as_of)
    partitions: list[MinuteSourcePartition] = []
    for year, month in _months(start, visible_last):
        relative_to_source = Path(f"year={year:04d}") / f"month={month:02d}" / "data.parquet"
        file_path = policy.resolve_contained_path(
            allowed_root=source_root,
            candidate=relative_to_source,
            root_role="minute_scan_source_root",
            path_role="minute_scan_partition",
            expected_kind="file",
        )
        schema = pq.read_schema(file_path)
        if len(schema.names) != len(physical_columns) or set(schema.names) != set(physical_columns) or any(
            schema.field(name).type != expected_types[name] for name in source_columns
        ):
            raise ProviderExecutionError("分钟源 Parquet schema 与 Catalog 漂移")
        metadata = pq.ParquetFile(file_path).metadata
        stat = file_path.stat()
        relative = file_path.relative_to(root).as_posix()
        schema_hash = typed_canonical_hash(str(schema))
        partitions.append(
            MinuteSourcePartition(
                relative,
                stat.st_size,
                stat.st_mtime_ns,
                metadata.num_rows,
                metadata.num_row_groups,
                schema_hash,
            )
        )
    source_relative = source_root.relative_to(root).as_posix()
    return MinuteScanPlan(
        admitted_plan.plan_hash,
        source_relative,
        source_columns,
        admitted_plan.query.universe.instruments,
        start,
        end,
        as_of,
        tuple(sorted(partitions, key=lambda item: item.relative_path)),
        admitted_plan.query.budget.max_rows,
        admitted_plan.query.budget.max_bytes,
        admitted_plan.query.budget.batch_size,
        budget,
        str(admitted_plan.minute_asset_class),
        str(admitted_plan.minute_session_policy_ref),
        tuple(str(item) for item in admitted_plan.minute_quality_policy_refs or ()),
        str(admitted_plan.minute_capability_manifest_hash),
        admitted_plan.query.universe.snapshot_id,
    )


def build_minute_partitioned_dataset(plan: MinuteScanPlan) -> PartitionedDatasetRef:
    """把已准入扫描计划转成不包含 raw rows 的月分区引用。"""

    visible_end = min(plan.end_at, plan.as_of + timedelta(microseconds=1))
    lineage = {
        "admitted_plan_hash": plan.admitted_plan_hash,
        "source_revision_hash": plan.source_revision_hash,
        "minute_asset_class": plan.minute_asset_class,
        "interval_minutes": 1,
        "minute_session_policy_ref": plan.minute_session_policy_ref,
        "minute_quality_policy_refs": list(plan.minute_quality_policy_refs),
        "minute_capability_manifest_hash": plan.minute_capability_manifest_hash,
    }
    if plan.minute_asset_class == "cn_future":
        lineage["minute_session_bundle_hash"] = CURRENT_SESSION_POLICY_BUNDLE_HASH
    partitions = []
    for source in plan.partitions:
        match = _PARTITION_PATH.fullmatch(source.relative_path)
        if match is None:
            raise QueryIRInvalidError("分钟分区路径无法提取月份")
        year = int(match.group(2))
        month = int(match.group(3))
        month_start = datetime(year, month, 1, tzinfo=plan.start_at.tzinfo)
        next_month = (
            datetime(year + 1, 1, 1, tzinfo=plan.start_at.tzinfo)
            if month == 12
            else datetime(year, month + 1, 1, tzinfo=plan.start_at.tzinfo)
        )
        logical_start = max(plan.start_at, month_start)
        logical_end = min(visible_end, next_month)
        if logical_start >= logical_end:
            raise QueryIRInvalidError("分钟分区没有可见的非空时间区间")
        partitions.append(DatasetPartitionRef(
            partition_key=f"{year:04d}-{month:02d}",
            logical_start=logical_start,
            logical_end=logical_end,
            root_role="minute_data",
            relative_path=source.relative_path,
            source_kind="catalog_raw",
            size=source.size,
            mtime_ns=source.mtime_ns,
            row_count=source.row_count,
            row_groups=source.row_groups,
            schema_hash=source.schema_hash,
            sort_keys=("code", "dt"),
            lineage=lineage,
        ))
    return PartitionedDatasetRef(
        dataset_id=f"minute/{plan.admitted_plan_hash}",
        timestamp_field="dt",
        instrument_field="code",
        instruments=tuple(sorted(set(plan.instruments))),
        allowed_columns=plan.source_columns,
        partitions=tuple(partitions),
        lineage=lineage,
        universe_snapshot_id=plan.universe_snapshot_id,
    )


def _months(start: datetime, end: datetime) -> Iterator[tuple[int, int]]:
    current_year, current_month = start.year, start.month
    while (current_year, current_month) <= (end.year, end.month):
        yield current_year, current_month
        if current_month == 12:
            current_year, current_month = current_year + 1, 1
        else:
            current_month += 1


def _arrow_type(value: str, pa: Any) -> Any:
    mapping = {
        "string": pa.string(),
        "timestamp[us]": pa.timestamp("us"),
        "float64": pa.float64(),
    }
    try:
        return mapping[value.lower()]
    except KeyError as exc:
        raise QueryIRInvalidError(f"分钟扫描字段类型不受支持: {value}") from exc


__all__ = [
    "FUTURES_MINUTE_COLUMNS",
    "MINUTE_SCAN_PLAN_VERSION",
    "STANDARD_MINUTE_COLUMNS",
    "MinuteScanBudget",
    "MinuteScanPlan",
    "MinuteSourcePartition",
    "build_minute_partitioned_dataset",
    "build_minute_scan_plan",
]
