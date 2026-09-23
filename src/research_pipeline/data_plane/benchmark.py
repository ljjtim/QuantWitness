"""VerifiedDataset 的只读扫描基线采集。"""

from __future__ import annotations

from dataclasses import dataclass
import time

import psutil

from .dataset_artifacts import DatasetFilter, VerifiedDataset
from .errors import SnapshotIntegrityError


def _measured_rss_bytes(process: psutil.Process) -> int:
    """读取可用于规模验收的 RSS；采样失败时不能用零值代替。"""

    try:
        value = process.memory_info().rss
    except (psutil.Error, OSError, RuntimeError) as exc:
        raise SnapshotIntegrityError("扫描基准 RSS 未测得") from exc
    if type(value) is not int or value < 0:
        raise SnapshotIntegrityError("扫描基准 RSS 未测得")
    return value


@dataclass(frozen=True)
class ScanBenchmark:
    rows: int
    batches: int
    logical_bytes: int
    peak_batch_bytes: int
    peak_rss_growth_bytes: int
    peak_arrow_growth_bytes: int
    elapsed_seconds: float

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.elapsed_seconds if self.elapsed_seconds > 0 else 0.0

    def to_dict(self) -> dict[str, int | float]:
        return {
            "rows": self.rows,
            "batches": self.batches,
            "logical_bytes": self.logical_bytes,
            "peak_batch_bytes": self.peak_batch_bytes,
            "peak_rss_growth_bytes": self.peak_rss_growth_bytes,
            "peak_arrow_growth_bytes": self.peak_arrow_growth_bytes,
            "elapsed_seconds": self.elapsed_seconds,
            "rows_per_second": self.rows_per_second,
        }


def benchmark_verified_scan(
    dataset: VerifiedDataset,
    *,
    columns: tuple[str, ...],
    filters: tuple[DatasetFilter, ...] = (),
    batch_size: int = 65_536,
) -> ScanBenchmark:
    """消费批流并记录进程 RSS、Arrow 内存池和批次峰值，不保留完整表。"""
    import pyarrow as pa

    process = psutil.Process()
    pool = pa.default_memory_pool()
    rss_start = _measured_rss_bytes(process)
    arrow_start = pool.bytes_allocated()
    peak_rss = rss_start
    peak_arrow = arrow_start
    rows = batches = logical_bytes = peak_batch_bytes = 0
    started = time.perf_counter()
    for batch in dataset.iter_batches(columns=columns, filters=filters, batch_size=batch_size):
        batch_bytes = int(batch.nbytes)
        rows += int(batch.num_rows)
        batches += 1
        logical_bytes += batch_bytes
        peak_batch_bytes = max(peak_batch_bytes, batch_bytes)
        peak_rss = max(peak_rss, _measured_rss_bytes(process))
        peak_arrow = max(peak_arrow, pool.bytes_allocated())
    elapsed = time.perf_counter() - started
    return ScanBenchmark(
        rows,
        batches,
        logical_bytes,
        peak_batch_bytes,
        max(0, peak_rss - rss_start),
        max(0, peak_arrow - arrow_start),
        elapsed,
    )


def require_scan_benchmark(
    result: ScanBenchmark,
    *,
    expected_rows: int,
    max_peak_batch_bytes: int,
    max_rss_growth_bytes: int,
    max_arrow_growth_bytes: int,
) -> None:
    """按当前机器先测后定的门限验收，不把吞吐率当成正确性。"""
    limits = (expected_rows, max_peak_batch_bytes, max_rss_growth_bytes, max_arrow_growth_bytes)
    if any(type(value) is not int or value < 0 for value in limits):
        raise SnapshotIntegrityError("扫描基准门限必须是非负整数")
    failures = []
    if result.rows != expected_rows:
        failures.append(f"rows={result.rows}/{expected_rows}")
    if result.peak_batch_bytes > max_peak_batch_bytes:
        failures.append(f"peak_batch_bytes={result.peak_batch_bytes}/{max_peak_batch_bytes}")
    if result.peak_rss_growth_bytes > max_rss_growth_bytes:
        failures.append(f"rss_growth={result.peak_rss_growth_bytes}/{max_rss_growth_bytes}")
    if result.peak_arrow_growth_bytes > max_arrow_growth_bytes:
        failures.append(f"arrow_growth={result.peak_arrow_growth_bytes}/{max_arrow_growth_bytes}")
    if failures:
        raise SnapshotIntegrityError(f"扫描基准未通过: {', '.join(failures)}")


__all__ = ["ScanBenchmark", "benchmark_verified_scan", "require_scan_benchmark"]
