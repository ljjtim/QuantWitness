"""Walk-forward 模型工件的共用元数据读写、分区校验与记录辅助。"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.canonical import typed_canonical_hash_streamed
from research_pipeline.research.dataframe_budget import (
    PandasFrameBudget,
    PandasFrameBudgetError,
)
from research_pipeline.research.modeling import ModelMainlineError, WALK_FORWARD_MODEL_VERSION
from research_pipeline.research.modeling.qlib import normalize_candidates
from research_pipeline.research.validation import (
    SplitFold,
    SplitManifest,
    build_search_manifest,
)


def _resolve_legacy_hook(name: str, default):
    """兼容仅在兼容入口模块上打补丁的既有测试夹具。"""
    legacy = sys.modules.get("research_pipeline.runtime.walk_forward_model_execution")
    if legacy is not None and name in legacy.__dict__:
        candidate = legacy.__dict__[name]
        if candidate is not default:
            return candidate
    return default


@contextmanager
def _sample_position_index(samples, budget):
    """查找引擎在所有 fold 消费期间常驻，按实际索引占用登记。"""
    budget.require_additional(len(samples) * 64, label="fold 样本查找索引")
    index_frame = pd.DataFrame(index=pd.Index(samples["sample_id"]))
    # 预先建立引擎，使后续查找及释放使用同一实际占用。
    index_frame.index.is_unique
    budget.reserve_frame(index_frame, label="fold 样本查找索引")
    try:
        yield index_frame.index
    finally:
        budget.release_frame(index_frame)


def _fit_fold_positions(samples, metadata, budget):
    """按冻结成员生成行位置，保留原样本顺序并把索引纳入常驻预算。"""
    positions = []
    with _sample_position_index(samples, budget) as sample_index:
        for fold in metadata["split_manifest"]["folds"]:
            roles = []
            for key in ("train_ids", "validation_ids"):
                budget.require_additional(
                    len(samples) * (1 + 3 * np.dtype(np.intp).itemsize),
                    label="fold 成员掩码与行索引",
                )
                found = sample_index.get_indexer_for(pd.Index(fold[key]).drop_duplicates())
                found = np.sort(found[found >= 0], kind="stable")
                frame = pd.DataFrame({"position": found})
                budget.reserve_frame(frame, label="fold 常驻行索引")
                roles.append(frame)
            positions.append(tuple(roles))
    return tuple(positions)


def _verify_fold_table(root, name, metadata, budget):
    paths = _table_paths(root, name)
    columns = tuple(pq.ParquetFile(paths[0]).schema_arrow.names)
    from .logical_table_verification import verify_table_records

    verify_table_records(
        root,
        name,
        metadata,
        lambda: _load_projected_table(
            paths, name, metadata, columns, budget, batch_consumer=lambda frame: None
        ),
    )


def _verified_fit_split_manifest(root, metadata, budget):
    """正式审计完整有界验收；fit只保留冻结fold/hash，不消费审计行。"""
    _verify_fold_table(root, "split_audit", metadata, budget)
    return _split_from_payload(metadata["split_manifest"], pd.DataFrame())


def _fold_reader(root, metadata, budget):
    from .model_fold_inputs import FoldInputReader

    active_verify = _resolve_legacy_hook("_verify_fold_table", _verify_fold_table)
    return FoldInputReader(root, metadata, budget, _table_paths, active_verify)


def _search_manifest(candidates: list[dict[str, object]], parameters: Mapping[str, object]):
    return build_search_manifest(
        search_id=_text(parameters, "search_id"),
        candidates=candidates,
        method="grid",
        max_trials=len(candidates),
        max_parallel=len(candidates),
        stopping_condition="complete_declared_candidate_universe",
        objective=_text(parameters, "objective"),
        direction=_text(parameters, "direction"),
        frozen_at=pd.Timestamp(_text(parameters, "search_frozen_at")).to_pydatetime(),
    )


def _search_payload(manifest: object) -> dict[str, object]:
    return {
        "search_id": manifest.search_id,
        "candidates": [
            {
                "candidate_id": item.candidate_id,
                "parameters": dict(item.parameters),
                "parameter_hash": item.parameter_hash,
            }
            for item in manifest.candidates
        ],
        "method": manifest.method,
        "max_trials": manifest.max_trials,
        "max_parallel": manifest.max_parallel,
        "stopping_condition": manifest.stopping_condition,
        "objective": manifest.objective,
        "direction": manifest.direction,
        "frozen_at": manifest.frozen_at.isoformat(),
        "stage": manifest.stage,
        "manifest_hash": manifest.manifest_hash,
    }


def _search_from_payload(payload: Mapping[str, object]):
    manifest = build_search_manifest(
        search_id=str(payload["search_id"]),
        candidates=[dict(item["parameters"]) for item in payload["candidates"]],
        method=str(payload["method"]),
        max_trials=int(payload["max_trials"]),
        max_parallel=int(payload["max_parallel"]),
        stopping_condition=str(payload["stopping_condition"]),
        objective=str(payload["objective"]),
        direction=str(payload["direction"]),
        frozen_at=datetime.fromisoformat(str(payload["frozen_at"])),
        stage=str(payload["stage"]),
    )
    if manifest.manifest_hash != payload["manifest_hash"]:
        raise ModelMainlineError("SearchManifest 工件发生漂移")
    return manifest


def _split_payload(split: SplitManifest) -> dict[str, object]:
    return {
        "method": split.method,
        "calendar_hash": split.calendar_hash,
        "manifest_hash": split.manifest_hash,
        "folds": [
            {
                "fold_id": fold.fold_id,
                "train_ids": list(fold.train_ids),
                "validation_ids": list(fold.validation_ids),
                "test_ids": list(fold.test_ids),
                "purged_ids": list(fold.purged_ids),
                "embargoed_ids": list(fold.embargoed_ids),
                "validation_range": list(fold.validation_range) if fold.validation_range else None,
                "test_range": list(fold.test_range) if fold.test_range else None,
            }
            for fold in split.folds
        ],
    }


def _split_from_payload(payload: Mapping[str, object], audit: pd.DataFrame) -> SplitManifest:
    folds = tuple(
        SplitFold(
            str(item["fold_id"]),
            tuple(item["train_ids"]),
            tuple(item["validation_ids"]),
            tuple(item["test_ids"]),
            tuple(item["purged_ids"]),
            tuple(item["embargoed_ids"]),
            tuple(item["validation_range"]) if item.get("validation_range") else None,
            tuple(item["test_range"]) if item.get("test_range") else None,
        )
        for item in payload["folds"]
    )
    split = SplitManifest(
        str(payload["method"]),
        folds,
        audit,
        str(payload["calendar_hash"]),
        str(payload["manifest_hash"]),
    )
    if _split_payload(split) != payload:
        raise ModelMainlineError("SplitManifest 工件发生漂移")
    return split


def _write_artifact(
    output_root: str | Path,
    tables: Mapping[str, pd.DataFrame],
    *,
    status: str,
    extra: Mapping[str, object],
) -> dict[str, object]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    table_hashes: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    hash_stream = _resolve_legacy_hook("typed_canonical_hash_streamed", typed_canonical_hash_streamed)
    for name, frame in sorted(tables.items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / "part-00000.parquet", index=False)
        row_counts[name] = len(frame)
        table_hashes[name] = hash_stream(_records(frame))
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION,
        "status": status,
        "row_counts": row_counts,
        "table_hashes": table_hashes,
        **dict(extra),
    }
    payload["artifact_hash"] = typed_canonical_hash(payload)
    (root / "artifact-metadata.json").write_text(canonical_json(payload), encoding="utf-8")
    return payload


def _write_partitioned_artifact(
    output_root: str | Path,
    *,
    partitioned_tables: Mapping[str, Iterable[pd.DataFrame]],
    tables: Mapping[str, pd.DataFrame] | object,
    status: str,
    extra: Mapping[str, object],
    partition_order: Mapping[str, Mapping[tuple[str, str], int]] | None = None,
    partition_schema: object = None,
) -> dict[str, object]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    table_hashes: dict[str, str] = {}
    table_hash_modes: dict[str, str] = {}
    table_partitions: dict[str, list[dict[str, object]]] = {}
    row_counts: dict[str, int] = {}
    hash_stream = _resolve_legacy_hook("typed_canonical_hash_streamed", typed_canonical_hash_streamed)
    for name, frames in sorted(partitioned_tables.items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        partition_hashes: list[str] = []
        descriptors: list[dict[str, object]] = []
        row_count = 0
        for index, frame in enumerate(frames):
            if partition_order is not None and name in partition_order:
                index = partition_order[name][
                    (str(frame["candidate_id"].iloc[0]), str(frame["fold_id"].iloc[0]))
                ]
            target = directory / f"part-{index:05d}.parquet"
            if partition_schema is None:
                frame.to_parquet(target, index=False)
            else:
                table = pa.Table.from_pandas(frame, preserve_index=False)
                pq.write_table(table.cast(partition_schema(name, index, table.schema)), target)
            row_count += len(frame)
            partition_hashes.append(hash_stream(_records(frame)))
            descriptor = {
                "path": f"part-{index:05d}.parquet",
                "row_count": len(frame),
                "records_hash": partition_hashes[-1],
            }
            for field in ("fold_id", "fold_role"):
                if field in frame:
                    values = frame[field].unique()
                    if len(values) != 1:
                        raise ModelMainlineError(f"模型分区必须只包含一个 {field}")
                    descriptor[field] = str(values[0])
            descriptors.append(descriptor)
        descriptors.sort(key=lambda item: item["path"])
        table_partitions[name] = descriptors
        partition_hashes = [item["records_hash"] for item in descriptors]
        if not partition_hashes:
            raise ModelMainlineError(f"模型 Runtime 分区表为空: {name}")
        row_counts[name] = row_count
        table_hashes[name] = typed_canonical_hash(partition_hashes)
        table_hash_modes[name] = "partition-records-v1"
    for name, frame in sorted(tables().items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / "part-00000.parquet", index=False)
        row_counts[name] = len(frame)
        table_hashes[name] = hash_stream(_records(frame))
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION,
        "status": status,
        "row_counts": row_counts,
        "table_hashes": table_hashes,
        "table_hash_modes": table_hash_modes,
        "table_partitions": table_partitions,
        **dict(extra),
    }
    payload["artifact_hash"] = typed_canonical_hash(payload)
    (root / "artifact-metadata.json").write_text(
        canonical_json(payload),
        encoding="utf-8",
    )
    return payload


def _read_artifact_metadata(
    root: str | Path,
    *,
    require_model_contract: bool = True,
) -> dict[str, object]:
    try:
        metadata = json.loads(
            (Path(root) / "artifact-metadata.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelMainlineError("模型 Runtime 输入元数据不可读") from exc
    if require_model_contract and metadata.get("contract_version") != WALK_FORWARD_MODEL_VERSION:
        raise ModelMainlineError("模型 Runtime 输入合同版本不受支持；请使用原环境读取历史结果或新建 v2 运行")
    for name, mode in metadata.get("table_hash_modes", {}).items():
        if mode != "partition-records-v1":
            continue
        descriptors = metadata.get("table_partitions", {}).get(name)
        if not isinstance(descriptors, list) or not descriptors or any(
            not isinstance(item, dict) or not {"path", "records_hash", "row_count"} <= set(item)
            for item in descriptors
        ):
            raise ModelMainlineError(f"模型 Runtime 输入缺少分区描述: {name}")
    return metadata


def _table_paths(root: str | Path, name: str) -> tuple[Path, ...]:
    paths = tuple(sorted((Path(root) / name).glob("*.parquet")))
    if not paths:
        raise ModelMainlineError(f"模型 Runtime 输入缺少表: {name}")
    return paths


def _load_table(
    root: str | Path,
    name: str,
    *,
    frame_budget: PandasFrameBudget,
    columns: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, Mapping[str, object]]:
    paths = _table_paths(root, name)
    metadata = _read_artifact_metadata(
        root, require_model_contract=name not in {"features", "labels"},
    )
    from .logical_table_verification import has_verified_table_records

    verified = has_verified_table_records(root, name, metadata)
    if columns is not None:
        return _load_projected_table(paths, name, metadata, columns, frame_budget), metadata
    hash_stream = _resolve_legacy_hook("typed_canonical_hash_streamed", typed_canonical_hash_streamed)
    try:
        frame_budget.require_parquet_materialization(
            paths,
            label=f"模型表 {name}",
        )
        if metadata.get("table_hash_modes", {}).get(name) == "partition-records-v1":
            descriptors = metadata["table_partitions"][name]
            if [item["path"] for item in descriptors] != [path.name for path in paths]:
                raise ModelMainlineError(f"模型 Runtime 输入分区发生漂移: {name}")
            frames = [
                frame_budget.collect_arrow_batches(
                    pq.ParquetFile(path).iter_batches(batch_size=65_536),
                    label=f"模型表 {name}/{path.name}",
                )
                for path in paths
            ]
            for part, descriptor in zip(frames, descriptors):
                if len(part) != descriptor["row_count"]:
                    raise ModelMainlineError(f"模型 Runtime 输入分区行数发生漂移: {name}")
                for field in ("fold_id", "fold_role"):
                    if field in descriptor and set(part[field].astype(str)) != {descriptor[field]}:
                        raise ModelMainlineError(f"模型 Runtime 分区 {field} 发生漂移")
            if typed_canonical_hash([item["records_hash"] for item in descriptors]) != metadata["table_hashes"][name]:
                raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
            partition_hashes = (
                []
                if verified
                else [hash_stream(_records(frame)) for frame in frames]
            )
            frame = frame_budget.concat_reserved_frames(frames, label=f"模型表 {name}")
            actual_hash = (
                metadata["table_hashes"][name]
                if verified
                else typed_canonical_hash(partition_hashes)
            )
        else:
            frame = frame_budget.collect_arrow_batches(
                (
                    batch
                    for path in paths
                    for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536)
                ),
                label=f"模型表 {name}",
            )
            actual_hash = (
                metadata["table_hashes"][name]
                if verified
                else hash_stream(_records(frame))
            )
    except (OSError, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc
    if actual_hash != metadata.get("table_hashes", {}).get(name):
        raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
    if len(frame) != metadata.get("row_counts", {}).get(name, len(frame)):
        raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}")
    return frame, metadata


def _load_projected_table(
    paths: tuple[Path, ...],
    name: str,
    metadata: Mapping[str, object],
    columns: tuple[str, ...],
    frame_budget: PandasFrameBudget,
    batch_consumer=None,
) -> pd.DataFrame:
    """完整记录逐批复验；只把模型所需列留在常驻工作集中。"""
    frames: list[pd.DataFrame] = []
    partitioned = metadata.get("table_hash_modes", {}).get(name) == "partition-records-v1"
    descriptors = metadata.get("table_partitions", {}).get(name, [])
    if partitioned and [item["path"] for item in descriptors] != [path.name for path in paths]:
        raise ModelMainlineError(f"模型 Runtime 输入分区发生漂移: {name}")
    total_rows = 0
    from .logical_table_verification import has_verified_table_records

    verified = has_verified_table_records(paths[0].parent.parent, name, metadata)
    hash_stream = _resolve_legacy_hook("typed_canonical_hash_streamed", typed_canonical_hash_streamed)

    def records(partition_paths: tuple[Path, ...]) -> Iterable[dict[str, object]]:
        nonlocal total_rows
        for path in partition_paths:
            for batch in pq.ParquetFile(path).iter_batches(
                batch_size=8_192, columns=list(columns) if verified else None,
            ):
                complete = frame_budget.collect_arrow_batches(
                    (batch,), label=f"模型完整校验批 {name}/{path.name}",
                )
                try:
                    if not verified:
                        yield from _records(complete)
                    projected = complete.loc[:, list(columns)].copy()
                    frame_budget.reserve_frame(projected, label=f"模型投影批 {name}")
                    if batch_consumer is None:
                        frames.append(projected)
                    else:
                        batch_consumer(projected)
                        frame_budget.release_frame(projected)
                    total_rows += len(complete)
                finally:
                    frame_budget.release_frame(complete)
                del complete, batch

    try:
        if batch_consumer is None:
            frame_budget.require_parquet_materialization(
                paths, label=f"模型投影表 {name}", columns=columns,
            )
        if partitioned:
            hashes = []
            for path, descriptor in zip(paths, descriptors):
                start_rows = total_rows
                if verified:
                    for _ in records((path,)):
                        pass
                    partition_hash = descriptor["records_hash"]
                else:
                    partition_hash = hash_stream(records((path,)))
                if partition_hash != descriptor["records_hash"]:
                    raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}/{path.name}")
                if total_rows - start_rows != descriptor["row_count"]:
                    raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}/{path.name}")
                hashes.append(partition_hash)
            actual_hash = typed_canonical_hash(hashes)
        else:
            if verified:
                for _ in records(paths):
                    pass
                actual_hash = metadata["table_hashes"][name]
            else:
                actual_hash = hash_stream(records(paths))
        if actual_hash != metadata.get("table_hashes", {}).get(name):
            raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
        declared_rows = metadata.get("row_counts", {}).get(name)
        if declared_rows is not None and total_rows != declared_rows:
            raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}")
        return (
            frame_budget.concat_reserved_frames(frames, label=f"模型投影表 {name}")
            if batch_consumer is None
            else None
        )
    except (OSError, ValueError, pa.ArrowException, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc


def _records(frame: pd.DataFrame) -> Iterable[dict[str, object]]:
    """逐行保持原 records 编码，不复制整表对象树。"""
    columns = list(frame.columns)
    for values in frame.itertuples(index=False, name=None):
        row = {}
        for key, value in zip(columns, values):
            value_type = type(value)
            if value_type is float:
                row[key] = None if value != value else value
                continue
            if value is None or value_type in (str, int, bool):
                row[key] = value
                continue
            if isinstance(value, np.ndarray):
                value = value.tolist()
            if isinstance(value, (list, tuple)):
                value = [None if isinstance(item, (float, np.floating)) and np.isnan(item) else item for item in value]
            elif value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
                value = None
            elif isinstance(value, pd.Timestamp) or hasattr(value, "isoformat"):
                value = value.isoformat()
            elif isinstance(value, np.generic):
                value = value.item()
            row[key] = value
        yield row


def _candidates(parameters: Mapping[str, object]) -> list[dict[str, object]]:
    try:
        raw = [json.loads(str(value)) for value in parameters["candidate_jsons"]]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ModelMainlineError("candidate_jsons 必须是规范 JSON 对象列表") from exc
    return normalize_candidates(raw)


def _text(parameters: Mapping[str, object], field: str) -> str:
    value = parameters.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ModelMainlineError(f"{field} 必须是非空字符串")
    return value.strip()


def _hash(parameters: Mapping[str, object], field: str) -> str:
    value = _text(parameters, field)
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ModelMainlineError(f"{field} 必须是 sha256")
    return value


def _boolean(parameters: Mapping[str, object], field: str) -> bool:
    value = parameters.get(field)
    if type(value) is not bool:
        raise ModelMainlineError(f"{field} 必须是布尔值")
    return value


def _optional_positive_int(parameters: Mapping[str, object], field: str) -> int | None:
    value = int(parameters[field])
    if value < 0:
        raise ModelMainlineError(f"{field} 不能为负")
    return None if value == 0 else value
