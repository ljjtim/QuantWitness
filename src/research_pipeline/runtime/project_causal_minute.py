"""项目因果输入复用正式分钟分区质量流，并恢复 QueryIR 逻辑列名。"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence

from research_pipeline.data_plane import PartitionedDatasetRef, PartitionedDatasetResolver
from research_pipeline.platform import typed_canonical_hash

from .errors import RuntimeIntegrityError


def build_causal_minute_descriptor(
    item, *, invocation: Mapping[str, str], partition_ids: Sequence[str]
) -> dict:
    """父进程验证本键批允许的月分区，生成原分钟 Worker 已理解的描述。"""
    payload = json.loads(item.content.decode("utf-8"))
    dataset = PartitionedDatasetRef.from_dict(payload["partitioned_dataset"])
    roots = payload["allowed_roots"]
    resolver = PartitionedDatasetResolver(roots)
    selected = tuple(sorted(set(partition_ids)))
    if not selected:
        raise RuntimeIntegrityError("项目因果分钟键批必须选择至少一个分区")
    partitions = []
    for key in selected:
        verified = resolver.resolve_partition(dataset, key)
        partitions.append({
            "input_kind": "partition", "port": item.artifact.name,
            "artifact_type": item.artifact.artifact_type,
            "artifact_key": item.artifact.artifact_key,
            "artifact_content_hash": item.artifact.content_hash,
            "schema_hash": item.schema_hash, "partition_key": key,
            "invocation": dict(invocation),
            "supervisor_verified_partition_id": typed_canonical_hash(verified.reference.to_dict()),
            "dataset": dataset.to_dict(), "allowed_roots": dict(roots),
        })
    return {
        "input_kind": "causal_partitions", "invocation": dict(invocation),
        "port": item.artifact.name, "column_map": dict(payload["column_map"]),
        "partitions": partitions,
    }


class ProjectCausalMinuteInput:
    """内部组合容器；扩展只会取得外层 RestrictedCausalInput。"""

    def __init__(self, *, port: str, column_map: Mapping[str, str], partitions: Sequence):
        self.port = port
        self._column_map = dict(column_map)
        self._partitions = {partition.partition_key: partition for partition in partitions}
        self._requested: set[str] = set()
        if len(self._partitions) != len(partitions) or any(
            partition.port != port for partition in partitions
        ):
            raise RuntimeIntegrityError("项目因果分钟分区端口不一致或月份重复")

    def iter_partition_batches(self, *, columns, partition_ids, batch_size):
        selected = tuple(columns)
        if not selected or len(set(selected)) != len(selected) or not set(selected) <= set(self._column_map):
            raise RuntimeIntegrityError("项目因果分钟请求未冻结的逻辑列")
        if not set(partition_ids) <= set(self._partitions):
            raise RuntimeIntegrityError("项目因果分钟请求未允许月份")
        physical = tuple(self._column_map[column] for column in selected)
        for key in partition_ids:
            partition = self._partitions[key]
            self._requested.add(key)
            for batch in partition.iter_batches(columns=physical, batch_size=batch_size):
                yield key, batch.rename_columns(selected)
            partition.assert_complete()

    def assert_complete(self) -> None:
        for key in self._requested:
            self._partitions[key].assert_complete()


def load_causal_minute_input(payload: Mapping, *, load_partition: Callable):
    """每个内层描述仍交由原 Worker 的 _load_input 建立质量流。"""
    expected = {"input_kind", "invocation", "port", "column_map", "partitions"}
    if set(payload) != expected or payload["input_kind"] != "causal_partitions":
        raise RuntimeIntegrityError("项目因果分钟分组描述无效")
    mapping = payload["column_map"]
    parts = payload["partitions"]
    if not isinstance(mapping, Mapping) or not mapping or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in mapping.items()
    ) or not isinstance(parts, (list, tuple)) or not parts:
        raise RuntimeIntegrityError("项目因果分钟列映射或分区描述无效")
    if any(not isinstance(part, Mapping) or part.get("input_kind") != "partition" for part in parts):
        raise RuntimeIntegrityError("项目因果分钟分组只能包含标准分区描述")
    return ProjectCausalMinuteInput(
        port=payload["port"], column_map=mapping,
        partitions=tuple(load_partition(part) for part in parts),
    )
