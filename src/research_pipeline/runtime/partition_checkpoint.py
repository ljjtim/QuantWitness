"""项目算子的月分区 checkpoint。"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import time
from typing import Mapping

from research_pipeline.platform import canonical_json

from .errors import RuntimeIntegrityError
from .external_artifact import ExternalArtifactCommit, ExternalArtifactStore


PARTITION_CHECKPOINT_CONTRACT = "runtime-partition-checkpoint"
_PARTITION_KEY = re.compile(r"^[0-9]{4}-[0-9]{2}$")


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise RuntimeIntegrityError(f"分区 checkpoint {field} 无效")
    return value


@dataclass(frozen=True)
class PartitionCheckpointExpectation:
    node_execution_id: str
    partition_key: str
    input_partition_id: str
    implementation_id: str
    parameters_hash: str
    fixed_clock: str
    root_seed: int
    state_in_semantic_hash: str | None

    def __post_init__(self) -> None:
        for field in ("node_execution_id", "input_partition_id", "parameters_hash"):
            _require_hash(getattr(self, field), field)
        if not _PARTITION_KEY.fullmatch(self.partition_key):
            raise RuntimeIntegrityError("分区 checkpoint key 无效")
        if not isinstance(self.implementation_id, str) or not self.implementation_id:
            raise RuntimeIntegrityError("分区 checkpoint implementation_id 无效")
        if not isinstance(self.fixed_clock, str) or not self.fixed_clock:
            raise RuntimeIntegrityError("分区 checkpoint fixed_clock 无效")
        if type(self.root_seed) is not int or self.root_seed < 0:
            raise RuntimeIntegrityError("分区 checkpoint root_seed 无效")
        if self.state_in_semantic_hash is not None:
            _require_hash(self.state_in_semantic_hash, "state_in_semantic_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "node_execution_id": self.node_execution_id,
            "partition_key": self.partition_key,
            "input_partition_id": self.input_partition_id,
            "implementation_id": self.implementation_id,
            "parameters_hash": self.parameters_hash,
            "fixed_clock": self.fixed_clock,
            "root_seed": self.root_seed,
            "state_in_semantic_hash": self.state_in_semantic_hash,
        }


@dataclass(frozen=True)
class PartitionCheckpoint:
    expectation: PartitionCheckpointExpectation
    outputs: Mapping[str, ExternalArtifactCommit]
    state_out: ExternalArtifactCommit | None
    state_out_schema_hash: str | None
    contract_version: str = PARTITION_CHECKPOINT_CONTRACT

    def __post_init__(self) -> None:
        if self.contract_version != PARTITION_CHECKPOINT_CONTRACT:
            raise RuntimeIntegrityError("分区 checkpoint 合同不受支持")
        if not self.outputs or any(
            not isinstance(port, str)
            or not port
            or commit.artifact_name != port
            for port, commit in self.outputs.items()
        ):
            raise RuntimeIntegrityError("分区 checkpoint outputs 无效")
        if self.state_out is None:
            if self.state_out_schema_hash is not None:
                raise RuntimeIntegrityError("无状态分区不能声明 state schema")
        else:
            if self.state_out.artifact_type != "runtime.project-state":
                raise RuntimeIntegrityError("分区 checkpoint state_out 类型无效")
            _require_hash(self.state_out_schema_hash, "state_out_schema_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "expectation": self.expectation.to_dict(),
            "outputs": {
                port: commit.to_dict() for port, commit in sorted(self.outputs.items())
            },
            "state_out": None if self.state_out is None else self.state_out.to_dict(),
            "state_out_schema_hash": self.state_out_schema_hash,
        }


class PartitionCheckpointStore:
    def __init__(self, run_root: str | Path, node_execution_id: str) -> None:
        _require_hash(node_execution_id, "node_execution_id")
        self.root = Path(run_root).resolve() / "partition-checkpoints" / node_execution_id
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, partition_key: str) -> Path:
        if not _PARTITION_KEY.fullmatch(partition_key):
            raise RuntimeIntegrityError("分区 checkpoint key 无效")
        return self.root / f"{partition_key}.json"

    def exists(self, partition_key: str) -> bool:
        return self.path(partition_key).is_file()

    def commit(self, checkpoint: PartitionCheckpoint) -> Path:
        target = self.path(checkpoint.expectation.partition_key)
        if target.exists():
            existing = json.loads(target.read_text(encoding="utf-8"))
            if existing != checkpoint.to_dict():
                raise RuntimeIntegrityError("分区 checkpoint 已存在且内容冲突")
            return target
        temporary = target.with_name(f".{target.name}.{os.getpid()}.{time.time_ns()}.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(canonical_json(checkpoint.to_dict()))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        return target

    def load(
        self,
        expectation: PartitionCheckpointExpectation,
        *,
        external_store: ExternalArtifactStore,
    ) -> PartitionCheckpoint:
        target = self.path(expectation.partition_key)
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("分区 checkpoint 无法读取") from exc
        expected_fields = {
            "contract_version",
            "expectation",
            "outputs",
            "state_out",
            "state_out_schema_hash",
        }
        if (
            not isinstance(payload, Mapping)
            or set(payload) != expected_fields
            or payload.get("contract_version") != PARTITION_CHECKPOINT_CONTRACT
            or payload.get("expectation") != expectation.to_dict()
            or not isinstance(payload.get("outputs"), Mapping)
            or (
                payload.get("state_out") is not None
                and not isinstance(payload.get("state_out"), Mapping)
            )
        ):
            raise RuntimeIntegrityError("分区 checkpoint 身份漂移或 schema 无效")
        outputs = {
            str(port): ExternalArtifactCommit.from_dict(commit)
            for port, commit in payload["outputs"].items()
            if isinstance(commit, Mapping)
        }
        if len(outputs) != len(payload["outputs"]):
            raise RuntimeIntegrityError("分区 checkpoint outputs 无效")
        state_out = (
            None
            if payload["state_out"] is None
            else ExternalArtifactCommit.from_dict(payload["state_out"])
        )
        checkpoint = PartitionCheckpoint(
            expectation,
            outputs,
            state_out,
            payload["state_out_schema_hash"],
        )
        commits = tuple(outputs.values()) + (() if state_out is None else (state_out,))
        for commit in commits:
            if external_store.verify(commit.semantic_hash) != commit:
                raise RuntimeIntegrityError("分区 checkpoint 工件引用漂移")
        return checkpoint


__all__ = [
    "PARTITION_CHECKPOINT_CONTRACT",
    "PartitionCheckpoint",
    "PartitionCheckpointExpectation",
    "PartitionCheckpointStore",
]
