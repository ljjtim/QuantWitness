"""随准入计划冻结的 Parquet 物理布局策略。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping

from .errors import SnapshotIntegrityError


@dataclass(frozen=True)
class PhysicalSnapshotPolicy:
    policy_id: str = "adaptive-bounded-v1"
    compression: str = "zstd"
    row_group_size: int = 65_536
    partitioning: str = "small-or-event-year-month"
    writer_version: str = "pyarrow-parquet-v2"
    small_table_max_rows: int = 65_536
    max_buffer_bytes: int = 8 * 1024 * 1024
    target_file_rows: int = 262_144
    target_file_bytes: int = 32 * 1024 * 1024

    def __post_init__(self) -> None:
        if (self.policy_id, self.partitioning, self.writer_version) != (
            "adaptive-bounded-v1", "small-or-event-year-month", "pyarrow-parquet-v2"
        ):
            raise SnapshotIntegrityError("不支持的快照布局策略身份")
        for name in ("row_group_size", "small_table_max_rows", "max_buffer_bytes",
                     "target_file_rows", "target_file_bytes"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise SnapshotIntegrityError(f"布局 {name} 必须是正整数")
        if (self.small_table_max_rows > self.row_group_size
                or self.row_group_size > self.target_file_rows
                or self.max_buffer_bytes > self.target_file_bytes):
            raise SnapshotIntegrityError("布局缓冲、行组和文件上限不一致")
        if not isinstance(self.compression, str) or not self.compression:
            raise SnapshotIntegrityError("布局 compression 必须非空")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> PhysicalSnapshotPolicy:
        if not isinstance(payload, Mapping) or set(payload) != set(cls().to_dict()):
            raise SnapshotIntegrityError("快照布局策略字段不完整")
        return cls(**dict(payload))
