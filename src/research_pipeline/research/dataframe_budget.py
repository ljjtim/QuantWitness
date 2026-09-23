"""在节点预算内把 Arrow 批次收集为单个 pandas DataFrame。"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa


class PandasFrameBudgetError(ValueError):
    """Arrow→pandas 完整工作集超过当前节点内存预算。"""


def pandas_frame_bytes(frame: pd.DataFrame) -> int:
    """返回 DataFrame 当前实际持有的索引和列数据字节数。"""

    return int(frame.memory_usage(index=True, deep=True).sum())


def parquet_uncompressed_bytes(
    paths: Iterable[str | Path],
    *,
    columns: Iterable[str] | None = None,
) -> int:
    """只读 Parquet footer，返回所选列的未压缩列块字节数。"""

    import pyarrow.parquet as pq

    selected = None if columns is None else tuple(dict.fromkeys(columns))
    total = 0
    found = False
    for raw_path in paths:
        found = True
        parquet = pq.ParquetFile(Path(raw_path))
        names = tuple(parquet.schema.names)
        if selected is None:
            indexes = range(len(names))
        else:
            missing = set(selected) - set(names)
            if missing:
                raise PandasFrameBudgetError(
                    f"Parquet footer 缺少列: {sorted(missing)}"
                )
            indexes = tuple(names.index(name) for name in selected)
        for row_group_index in range(parquet.metadata.num_row_groups):
            row_group = parquet.metadata.row_group(row_group_index)
            total += sum(
                int(row_group.column(index).total_uncompressed_size)
                for index in indexes
            )
    if not found:
        raise PandasFrameBudgetError("Parquet 完整矩阵预检没有输入文件")
    return total


@dataclass
class PandasFrameBudget:
    """跟踪同一节点内同时存活的 pandas 输入和合并副本。"""

    max_memory_bytes: int
    retained_bytes: int = 0
    peak_required_bytes: int = 0

    def __post_init__(self) -> None:
        if type(self.max_memory_bytes) is not int or self.max_memory_bytes <= 0:
            raise PandasFrameBudgetError("pandas 工作集内存预算必须是正整数")
        if self.retained_bytes != 0 or self.peak_required_bytes != 0:
            raise PandasFrameBudgetError("pandas 工作集计数必须从零开始")

    def collect_arrow_batches(
        self,
        batches: Iterable[pa.RecordBatch],
        *,
        label: str,
    ) -> pd.DataFrame:
        """收集一个完整矩阵；预算不足时在 concat 前拒绝。"""

        frames: list[pd.DataFrame] = []
        input_bytes = 0
        for batch in batches:
            batch_bytes = int(batch.nbytes)
            # 转换时 Arrow batch 与 pandas 列可能同时存活。先用 Arrow 字节给出
            # 可计算下界，避免明知单批装不下仍调用 to_pandas。
            self._require(
                self.retained_bytes + input_bytes + 2 * batch_bytes,
                f"{label} 的 Arrow→pandas 单批转换",
            )
            frame = batch.to_pandas()
            frame_bytes = pandas_frame_bytes(frame)
            self._require(
                self.retained_bytes
                + input_bytes
                + batch_bytes
                + frame_bytes,
                f"{label} 的 Arrow→pandas 单批转换",
            )
            frames.append(frame)
            input_bytes += frame_bytes

        if not frames:
            raise PandasFrameBudgetError(f"{label} 没有 Arrow 批次")
        if len(frames) == 1:
            result = frames[0]
        else:
            # concat 执行时输入块和完整输出会同时存活。忽略索引重建只会使
            # 估算更保守，因为 input_bytes 已逐块包含各自 RangeIndex。
            self._require(
                self.retained_bytes + 2 * input_bytes,
                f"{label} 的完整 DataFrame 合并",
            )
            result = pd.concat(frames, ignore_index=True, sort=False)

        result_bytes = pandas_frame_bytes(result)
        self._require(
            self.retained_bytes + result_bytes,
            f"{label} 的完整 DataFrame 常驻",
        )
        self.retained_bytes += result_bytes
        return result

    def reserve_frame(self, frame: pd.DataFrame, *, label: str) -> int:
        """把调用方已经持有的 DataFrame 纳入同一节点工作集。"""

        frame_bytes = pandas_frame_bytes(frame)
        self._require(
            self.retained_bytes + frame_bytes,
            f"{label} 的完整 DataFrame 常驻",
        )
        self.retained_bytes += frame_bytes
        return frame_bytes

    def concat_reserved_frames(
        self,
        frames: Iterable[pd.DataFrame],
        *,
        label: str,
    ) -> pd.DataFrame:
        """合并已经逐个登记的帧，并把登记切换到合并结果。"""

        materialized = list(frames)
        if not materialized:
            raise PandasFrameBudgetError(f"{label} 没有可合并 DataFrame")
        input_bytes = sum(pandas_frame_bytes(frame) for frame in materialized)
        if input_bytes > self.retained_bytes:
            raise PandasFrameBudgetError("pandas 工作集合并计数不闭合")
        if len(materialized) == 1:
            return materialized[0]
        self._require(
            self.retained_bytes + input_bytes,
            f"{label} 的完整 DataFrame 合并",
        )
        result = pd.concat(materialized, ignore_index=True, sort=False)
        result_bytes = pandas_frame_bytes(result)
        self.retained_bytes = self.retained_bytes - input_bytes + result_bytes
        self._require(self.retained_bytes, f"{label} 的完整 DataFrame 常驻")
        return result

    def release_frame(self, frame: pd.DataFrame) -> None:
        """调用方释放未修改的已登记 DataFrame 后同步预算计数。"""

        frame_bytes = pandas_frame_bytes(frame)
        if frame_bytes > self.retained_bytes:
            raise PandasFrameBudgetError("pandas 工作集释放计数不闭合")
        self.retained_bytes -= frame_bytes

    def require_additional(self, byte_size: int, *, label: str) -> None:
        """在已保留输入旁创建已知大小副本前执行硬门禁。"""

        if type(byte_size) is not int or byte_size < 0:
            raise PandasFrameBudgetError("pandas 附加工作集字节数必须是非负整数")
        self._require(self.retained_bytes + byte_size, label)

    def require_parquet_materialization(
        self,
        paths: Iterable[str | Path],
        *,
        label: str,
        columns: Iterable[str] | None = None,
    ) -> int:
        """在读取数据页前拒绝显然放不下的完整 Parquet→pandas 矩阵。"""

        source_bytes = parquet_uncompressed_bytes(paths, columns=columns)
        # 完整矩阵物化时，Arrow 输入和 pandas 输出会同时存活。footer
        # 下界已经超过预算时，不进入任何 RecordBatch 数据页读取。
        self._require(
            self.retained_bytes + 2 * source_bytes,
            f"{label} 的完整矩阵 footer 预检",
        )
        return source_bytes

    def _require(self, required_bytes: int, label: str) -> None:
        self.peak_required_bytes = max(self.peak_required_bytes, required_bytes)
        if required_bytes > self.max_memory_bytes:
            raise PandasFrameBudgetError(
                f"{label} 超过节点内存预算: "
                f"required_bytes={required_bytes}, "
                f"memory_bytes={self.max_memory_bytes}"
            )


__all__ = [
    "PandasFrameBudget",
    "PandasFrameBudgetError",
    "parquet_uncompressed_bytes",
    "pandas_frame_bytes",
]
