"""算子身份边界：grid_measurement。"""

from __future__ import annotations

from time import perf_counter


def _measurement(node_id: str, started: float, output_hash: object) -> dict[str, object]:
    import psutil

    return {
        "node_id": node_id,
        "elapsed_seconds": round(perf_counter() - started, 6),
        "working_set_bytes": psutil.Process().memory_info().rss,
        "output_hash": str(output_hash),
    }
