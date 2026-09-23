"""确定性 moving-block 与 stationary bootstrap。"""

from __future__ import annotations

import hashlib
import hmac

import numpy as np

from .contracts import StatisticsError


MAX_BOOTSTRAP_REPLICATIONS = 20_000
MAX_BOOTSTRAP_WORKING_BYTES = 64 * 1024 * 1024
_INT64_BYTES = np.dtype(np.int64).itemsize
_FLOAT64_BYTES = np.dtype(np.float64).itemsize


def derive_seed(root_seed: int, *, node_id: str, method: str, input_hash: str) -> int:
    if isinstance(root_seed, bool) or not isinstance(root_seed, int) or root_seed < 0:
        raise StatisticsError("root_seed 必须是非负整数")
    message = f"{node_id}|{method}|{input_hash}".encode("utf-8")
    digest = hmac.new(str(root_seed).encode("ascii"), message, hashlib.sha256).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def iter_bootstrap_index_batches(
    *,
    method: str,
    sample_size: int,
    block_length: int,
    replications: int,
    seed: int,
    batch_replications: int,
):
    """按原始 replication 顺序生成索引，不改变随机流或字节序。"""

    _validate_parameters(sample_size, block_length, replications)
    if (
        type(batch_replications) is not int
        or batch_replications < 1
        or batch_replications > replications
    ):
        raise StatisticsError("bootstrap batch_replications 无效")
    generator = np.random.default_rng(seed)
    completed = 0
    if method == "moving_block":
        blocks = int(np.ceil(sample_size / block_length))
        offsets = np.arange(block_length)
        while completed < replications:
            current = min(batch_replications, replications - completed)
            starts = generator.integers(0, sample_size, size=(current, blocks))
            yield ((starts[:, :, None] + offsets) % sample_size).reshape(
                current, -1
            )[:, :sample_size]
            completed += current
        return
    if method != "stationary":
        raise StatisticsError("bootstrap method 只支持 moving_block/stationary")
    restart_probability = 1.0 / block_length
    while completed < replications:
        current_batch = min(batch_replications, replications - completed)
        indices = np.empty((current_batch, sample_size), dtype=np.int64)
        for replication in range(current_batch):
            current = int(generator.integers(0, sample_size))
            indices[replication, 0] = current
            for position in range(1, sample_size):
                if generator.random() < restart_probability:
                    current = int(generator.integers(0, sample_size))
                else:
                    current = (current + 1) % sample_size
                indices[replication, position] = current
        yield indices
        completed += current_batch


def bootstrap_batch_replications(
    *,
    sample_size: int,
    replications: int,
    persistent_bytes: int,
    sample_copies_per_replication: int,
    extra_bytes_per_replication: int = 0,
) -> int:
    """按同时存活的索引、样本副本和结果计算安全批量。"""

    if any(
        type(value) is not int or value < 0
        for value in (
            sample_size,
            replications,
            persistent_bytes,
            sample_copies_per_replication,
            extra_bytes_per_replication,
        )
    ):
        raise StatisticsError("bootstrap 工作集预算参数无效")
    per_replication = (
        sample_size
        * (_INT64_BYTES + sample_copies_per_replication * _FLOAT64_BYTES)
        + extra_bytes_per_replication
    )
    available = MAX_BOOTSTRAP_WORKING_BYTES - persistent_bytes
    if per_replication <= 0 or available < per_replication:
        raise StatisticsError("bootstrap 同时存活工作集超过字节预算")
    return min(replications, max(1, available // per_replication))


def _validate_parameters(
    sample_size: int,
    block_length: int,
    replications: int,
) -> None:
    if sample_size < 3 or block_length < 1 or block_length > sample_size:
        raise StatisticsError("sample_size/block_length 无效")
    if replications < 2 or replications > MAX_BOOTSTRAP_REPLICATIONS:
        raise StatisticsError("bootstrap 重复次数超出允许范围")


__all__ = [
    "bootstrap_batch_replications",
    "derive_seed",
    "iter_bootstrap_index_batches",
]
