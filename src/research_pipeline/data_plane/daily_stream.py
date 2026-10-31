"""只读月分区的一次扫描与有界日缓存，不创建线程或落盘副本。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from threading import RLock
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .errors import SnapshotIntegrityError
from .partitioned_artifacts import PartitionedDatasetRef, PartitionedDatasetResolver
from .query_ir import source_local_naive


class DayBatchReader:
    """按日期递增消费，调用者可用单个背景线程 get_day 下一日。

    get_day(day) 移交日表并释放读取器引用，日期由调用者的 scope 日历提供。
    已消费日不可重读，跨月会释放旧月全部缓存。调用者持有的
    表由调用者计入总预算。预读数据仅供传输，策略可见性仍由模拟时钟控制。
    """

    def __init__(self, dataset: PartitionedDatasetRef, allowed_roots: Mapping[str, Path],
                 *, columns: tuple[str, ...], memory_bytes: int) -> None:
        if type(memory_bytes) is not int or memory_bytes <= 0:
            raise ValueError("memory_bytes 必须是正整数")
        if not columns or len(set(columns)) != len(columns) or set(columns) - set(dataset.allowed_columns):
            raise SnapshotIntegrityError("日读取列必须非空、唯一且已经批准")
        self.dataset = dataset
        self.columns = columns
        self.memory_bytes = memory_bytes
        self._scan_columns = tuple(dict.fromkeys((dataset.instrument_field, dataset.timestamp_field, *columns)))
        self._resolver = PartitionedDatasetResolver(allowed_roots, max_batch_bytes=memory_bytes)
        self._months = {}
        days = set()
        for partition in dataset.partitions:
            start = source_local_naive(partition.logical_start)
            end = source_local_naive(partition.logical_end)
            last = (end - timedelta(microseconds=1)).date()
            month = (start.year, start.month)
            if (last.year, last.month) != month:
                raise SnapshotIntegrityError("日读取器要求分区逻辑范围不跨自然月")
            self._months.setdefault(month, []).append(partition)
            current = start.date()
            while current <= last:
                days.add(current)
                current += timedelta(days=1)
        self._date_set = frozenset(days)
        self._cache = {}
        self._day_bytes = {}
        self._cached_bytes = 0
        self._unordered = set()
        self._last_keys = {}
        self._released = set()
        self._month = None
        self._schema = None
        self._closed = False
        self._lock = RLock()
        self.scanned_partitions = 0
        self._month_scans = 0
        self.row_groups_available = 0
        self.scanned_rows = 0
        self.estimated_month_bytes = 0
        self.peak_bytes = 0

    @property
    def statistics(self) -> dict[str, int]:
        """row_groups 为过滤后选中的行组数，rows 为裁剪后返回行数。"""
        return {"month_scans": self._month_scans, "row_groups": self.row_groups_available,
                "rows": self.scanned_rows}

    @property
    def cached_bytes(self) -> int:
        return self._cached_bytes

    def _check_memory(self, size: int) -> None:
        self.peak_bytes = max(self.peak_bytes, size)
        if size > self.memory_bytes:
            raise MemoryError(f"日读取器内存超限: 需要 {size} bytes，预算 {self.memory_bytes} bytes；禁止 spill，请提高声明内存或使用显式保存路径")

    def _clear_cache(self) -> None:
        self._cache.clear()
        self._day_bytes.clear()
        self._cached_bytes = 0
        self._unordered.clear()
        self._last_keys.clear()

    def _check_order(self, day: date, batch: pa.RecordBatch) -> None:
        """线性检查新增行及日块边界，不重复检查已缓存的数据。"""
        if day in self._unordered:
            return
        codes = batch.column(batch.schema.get_field_index(self.dataset.instrument_field))
        times = batch.column(batch.schema.get_field_index(self.dataset.timestamp_field))
        first = (codes[0].as_py(), times[0].as_py())
        last = (codes[-1].as_py(), times[-1].as_py())
        previous = self._last_keys.get(day)
        ordered = codes.null_count == 0 and times.null_count == 0
        if ordered and previous is not None:
            ordered = previous <= first
        if ordered and batch.num_rows > 1:
            left_codes, right_codes = codes.slice(0, len(codes) - 1), codes.slice(1)
            left_times, right_times = times.slice(0, len(times) - 1), times.slice(1)
            ordered = pc.all(pc.or_(pc.less(left_codes, right_codes),
                                   pc.and_(pc.equal(left_codes, right_codes),
                                           pc.less_equal(left_times, right_times)))).as_py()
        if not ordered:
            self._unordered.add(day)
        self._last_keys[day] = last

    def _load_month(self, month) -> None:
        self._clear_cache()
        self._month = month
        self.estimated_month_bytes = 0
        self._month_scans += 1
        try:
            for reference in self._months[month]:
                partition = self._resolver.resolve_partition(self.dataset, reference.partition_key)
                schema = pq.ParquetFile(partition.path).schema_arrow
                projected = pa.schema([schema.field(name) for name in self._scan_columns])
                self._schema = pa.schema([schema.field(name) for name in self.columns])
                # footer 行数乘投影列宽仅作容量提示；物理整月行数不是裁剪后行数。
                width = sum((field.type.bit_width // 8 if (pa.types.is_integer(field.type)
                             or pa.types.is_floating(field.type) or pa.types.is_temporal(field.type))
                             else 20) + 1 for field in projected)
                self.estimated_month_bytes += reference.row_count * width
                self.scanned_partitions += 1
                timestamp = ds.field(self.dataset.timestamp_field)
                timestamp_type = schema.field(self.dataset.timestamp_field).type
                aware = pa.types.is_timestamp(timestamp_type) and timestamp_type.tz is not None
                start = reference.logical_start if aware else source_local_naive(reference.logical_start)
                end = reference.logical_end if aware else source_local_naive(reference.logical_end)
                expression = (timestamp >= pa.scalar(start, type=timestamp_type)) & (timestamp < pa.scalar(end, type=timestamp_type))
                if self.dataset.instruments:
                    expression &= ds.field(self.dataset.instrument_field).isin(self.dataset.instruments)
                fragment = ds.ParquetFileFormat().make_fragment(str(partition.path), filesystem=pa.fs.LocalFileSystem())
                self.row_groups_available += sum(len(item.row_groups) for item in
                                                fragment.split_by_row_group(filter=expression, schema=schema))
                batch_size = min(65_536, max(1, self.memory_bytes // max(width * 4, 1)))
                for batch in partition.iter_batches(columns=self._scan_columns, batch_size=batch_size):
                    self.scanned_rows += batch.num_rows
                    batch_bytes = batch.get_total_buffer_size()
                    self._check_memory(self.cached_bytes + batch_bytes)
                    if not batch.num_rows:
                        continue
                    timestamps = batch.column(batch.schema.get_field_index(self.dataset.timestamp_field))
                    local_time_bytes = 0
                    if pa.types.is_timestamp(timestamps.type) and timestamps.type.tz:
                        # 已批准中国市场分钟范围使用 UTC+8 墙钟，不依赖系统时区库。
                        timestamps = pc.add(pc.cast(timestamps, pa.timestamp("us")),
                                            pa.scalar(8 * 3600 * 1_000_000, type=pa.duration("us")))
                        local_time_bytes = timestamps.nbytes
                    day_values = pc.cast(timestamps, pa.date32())
                    temporary_bytes = batch_bytes + local_time_bytes + day_values.nbytes
                    self._check_memory(self.cached_bytes + temporary_bytes)
                    for day in pc.unique(day_values).to_pylist():
                        if day is None:
                            raise SnapshotIntegrityError("分钟时间不允许为空")
                        selected = batch.filter(pc.equal(day_values, pa.scalar(day)))
                        # filter 生成独立缓冲，释放一天不会保留整月载荷。
                        selected_bytes = selected.get_total_buffer_size()
                        self._check_memory(self.cached_bytes + temporary_bytes + selected_bytes + selected.num_rows * 4)
                        self._check_order(day, selected)
                        self._cache.setdefault(day, []).append(selected)
                        self._day_bytes[day] = self._day_bytes.get(day, 0) + selected_bytes
                        self._cached_bytes += selected_bytes
                self._check_memory(self.cached_bytes)
        except BaseException:
            self._clear_cache()
            self._closed = True
            raise

    def _peek(self, day: date) -> pa.Table:
        """同一月份仅首次访问扫描源分区。"""
        with self._lock:
            if self._closed:
                raise RuntimeError("日读取器已关闭")
            if day not in self._date_set:
                raise KeyError(f"日期不在批准范围: {day}")
            month = (day.year, day.month)
            if day in self._released or (self._month is not None and month < self._month):
                raise ValueError(f"日期已经释放，不能重新扫描: {day}")
            if month != self._month:
                self._load_month(month)
            batches = self._cache.get(day)
            if batches is None:
                return pa.Table.from_batches([], schema=self._schema)
            table = pa.Table.from_batches(batches)
            if day in self._unordered:
                # Arrow 排序对相同键保持输入次序；额度包含索引及输出副本。
                self._check_memory(self.cached_bytes + self._day_bytes[day] + table.num_rows * 8)
                keys = [(self.dataset.instrument_field, "ascending"), (self.dataset.timestamp_field, "ascending")]
                ordered = table.take(pc.sort_indices(table, sort_keys=keys)).select(self.columns)
                self._check_memory(self.cached_bytes + ordered.get_total_buffer_size())
                table = ordered
            return table.select(self.columns)

    def get_day(self, day: date) -> pa.Table:
        """移交日表，读取器不再保留该日的数据。"""
        with self._lock:
            table = self._peek(day)
            self._release(day)
            return table

    def _release(self, day: date) -> None:
        """释放已消费的日缓存，不触发扫描。"""
        with self._lock:
            self._cache.pop(day, None)
            self._cached_bytes -= self._day_bytes.pop(day, 0)
            self._unordered.discard(day)
            self._last_keys.pop(day, None)
            self._released.add(day)

    def close(self) -> None:
        with self._lock:
            self._clear_cache()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
