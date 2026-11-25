"""research.model.fit.v2 算子独立计算内核。"""

from __future__ import annotations

import gc
import json
from pathlib import Path
from typing import Mapping

import pandas as pd

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.research.dataframe_budget import PandasFrameBudget, pandas_frame_bytes
from research_pipeline.research.modeling import (
    CandidateFitRejected,
    ModelMainlineError,
    WALK_FORWARD_MODEL_VERSION,
    model_dependency_preflight,
)
from research_pipeline.research.modeling.qlib import SEQUENCE_MODEL_CLASSES, fit_bundle
from research_pipeline.research.validation import (
    TrialLedger,
    build_seed_manifest,
    issue_fit_scope,
)
from .model_artifact_io import (
    _candidates,
    _fit_fold_positions,
    _fold_reader,
    _hash,
    _read_artifact_metadata,
    _resolve_legacy_hook,
    _search_manifest,
    _search_payload,
    _text,
    _verified_fit_split_manifest,
    _write_artifact,
)
from .model_sequence import sequence_for_candidate


def _fit_candidate_fold(
    *,
    samples,
    metadata,
    sequence_windows,
    budget,
    candidate,
    candidate_id,
    fold_index,
    candidate_index,
    fit_scope_ref,
    output_root,
    root_seed,
    fold_positions,
    sample_bytes,
    bound_sequence_context=None,
    sequence_facts=None,
):
    fold = metadata["split_manifest"]["folds"][fold_index]
    budget.require_additional(2 * sample_bytes, label="候选 fold 过滤副本")
    train_positions, valid_positions = fold_positions[fold_index]
    train_indices = train_positions["position"].to_numpy()
    valid_indices = valid_positions["position"].to_numpy()
    failure_fit_time = pd.to_datetime(samples["label_available_time"].iloc[valid_indices], utc=True).max().isoformat()
    input_bytes = sum(
        pandas_frame_bytes(samples.iloc[indexes[start:start + 8192]])
        for indexes in (train_indices, valid_indices)
        for start in range(0, len(indexes), 8192)
    )
    budget.require_additional(2 * input_bytes, label="Qlib 数据集与处理器副本")
    active_fit_bundle = _resolve_legacy_hook("fit_bundle", fit_bundle)
    try:
        binding = (
            {"_bound_sequence_context": bound_sequence_context}
            if bound_sequence_context is not None
            else {}
        )
        if sequence_facts is not None:
            binding["_sequence_facts"] = sequence_facts
        row = active_fit_bundle(
            None,
            None,
            _fit_input=(samples, train_indices, valid_indices),
            candidate=candidate,
            feature_columns=tuple(metadata["feature_columns"]),
            output_root=output_root,
            bundle_path=f"bundles/{fold_index}/{candidate_index}",
            root_seed=root_seed,
            fit_scope_ref=fit_scope_ref,
            sequence_context=sequence_for_candidate(sequence_windows, candidate, budget),
            **binding,
        )
        row.update(candidate_id=candidate_id, fold_id=fold["fold_id"], status="fitted", reason_code=None)
        return row
    except CandidateFitRejected as exc:
        return {
            "candidate_id": candidate_id,
            "fold_id": fold["fold_id"],
            "status": "failed",
            "reason_code": exc.reason_code,
            "bundle_path": None,
            "model_path": None,
            "config_path": None,
            "model_class": None,
            "feature_columns_json": None,
            "target_kind": "regression",
            "fit_scope_ref": fit_scope_ref,
            "fit_time": failure_fit_time,
        }


def _bind_sequence_context_for_fold(
    *,
    samples,
    metadata,
    sequence_windows,
    budget,
    fold_positions,
    fold_index,
    sample_bytes,
):
    """在当前 fit 执行内为同 fold 序列候选共享一次窗口绑定结果。"""
    from research_pipeline.research.modeling.gru import bind_windows

    train_positions, valid_positions = fold_positions[fold_index]
    budget.require_additional(2 * sample_bytes, label="序列候选窗口绑定")
    frame = pd.concat([
        samples.iloc[train_positions["position"].to_numpy()],
        samples.iloc[valid_positions["position"].to_numpy()],
    ], ignore_index=True)
    try:
        bound = bind_windows(
            sequence_windows,
            frame,
            tuple(metadata["feature_columns"]),
            int(metadata["sequence"]["step_len"]),
        )
    finally:
        del frame
    for name in ("context", "targets", "members", "exclusions"):
        budget.reserve_frame(getattr(bound, name), label=f"序列绑定缓存/{name}")
    return bound


def _release_bound_sequence_context(bound, budget):
    if bound is not None:
        for name in ("context", "targets", "members", "exclusions"):
            budget.release_frame(getattr(bound, name))


def _execute_spawn_fit(task):
    """同一worker会话复用已核验输入；每个候选fold仍写入独立bundle。"""
    root = Path(task["split_root"]).resolve()
    active_read_metadata = _resolve_legacy_hook("_read_artifact_metadata", _read_artifact_metadata)
    current = active_read_metadata(root)
    expected = task["split_artifact_hash"]
    if current.get("artifact_hash") != expected:
        raise ModelMainlineError("模型拟合冻结split身份发生变化")
    key = (
        str(root),
        expected,
        int(task["max_memory_bytes"]),
        int(task["fold_index"]),
        str(Path(task["output_root"]).resolve()),
    )
    session = task.get("_worker_session_inputs", {})
    reader = session.get("fold_reader")
    if reader is not None and reader.root == root and reader.metadata.get("artifact_hash") == expected:
        if reader.metadata != current:
            raise ModelMainlineError("模型拟合冻结split元数据发生变化")
        current = reader.metadata
    cached = session.get("model_fit")
    if cached is not None and cached["key"] == key:
        if cached["metadata"] != current:
            raise ModelMainlineError("模型拟合冻结split元数据发生变化")
        current = cached["metadata"]
    else:
        session.pop("model_fit", None)
        cached = None
        budget = PandasFrameBudget(int(task["max_memory_bytes"]))
        if typed_canonical_hash({name: value for name, value in current.items() if name != "artifact_hash"}) != expected:
            raise ModelMainlineError("模型拟合冻结split元数据身份不一致")

        metadata = current
        reader = session.get("fold_reader")
        if reader is None or reader.root != root or reader.metadata != metadata:
            reader = _fold_reader(root, metadata, budget)
            session["fold_reader"] = reader
        reader.budget = budget
        fold = metadata["split_manifest"]["folds"][int(task["fold_index"])]
        ids = tuple(fold["train_ids"]) + tuple(fold["validation_ids"])
        samples = reader.samples(ids)
        windows = reader.windows(ids) if metadata.get("sequence") else None
        active_positions = _resolve_legacy_hook("_fit_fold_positions", _fit_fold_positions)
        positions = active_positions(
            samples,
            {**metadata, "split_manifest": {**metadata["split_manifest"], "folds": [fold]}},
            budget,
        )
        positions = [positions[0] for _ in metadata["split_manifest"]["folds"]]
        cached = {
            "key": key,
            "samples": samples,
            "metadata": metadata,
            "windows": windows,
            "positions": positions,
            "budget": budget,
            "bound_sequence_context": None,
            "bound_fold_index": None,
            "sample_bytes": pandas_frame_bytes(samples),
            "sequence_facts": None,
        }
        session["model_fit"] = cached
    samples, metadata = cached["samples"], cached["metadata"]
    windows, budget = cached["windows"], cached["budget"]
    bound = None
    if task["candidate"]["model"]["class"] in SEQUENCE_MODEL_CLASSES:
        bound = cached.get("bound_sequence_context")
        if bound is None:
            from research_pipeline.research.modeling.gru import bind_windows

            bound = bind_windows(
                windows,
                samples,
                tuple(metadata["feature_columns"]),
                int(metadata["sequence"]["step_len"]),
            )
            _release_bound_sequence_context(windows, budget)
            for name in ("context", "targets", "members", "exclusions"):
                budget.reserve_frame(getattr(bound, name), label="fold已核序列窗口")
            cached["windows"] = windows = bound
            cached["bound_sequence_context"] = bound
    resource_check = task.get("_resource_check")
    if resource_check is not None:
        resource_check()
    active_fit_fold = _resolve_legacy_hook("_fit_candidate_fold", _fit_candidate_fold)
    row = active_fit_fold(
        samples=samples,
        metadata=metadata,
        sequence_windows=windows,
        budget=budget,
        fold_positions=cached["positions"],
        sample_bytes=cached["sample_bytes"],
        **{
            key: task[key]
            for key in (
                "candidate",
                "candidate_id",
                "fold_index",
                "candidate_index",
                "fit_scope_ref",
                "output_root",
                "root_seed",
            )
        },
        bound_sequence_context=bound,
        sequence_facts=cached.get("sequence_facts") if bound is not None else None,
    )
    if bound is not None and row["status"] == "fitted" and cached.get("sequence_facts") is None:
        config_path = Path(task["output_root"]) / row["config_path"]
        config = json.loads(config_path.read_text(encoding="utf-8"))
        cached["sequence_facts"] = config["sequence"]
    return row


def execute_model_fit_artifact(
    *,
    split_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    root_seed: int,
    max_memory_bytes: int,
    parallel_workers: int = 1,
    cpu_slots: int = 1,
    max_temp_bytes: int | None = None,
    deadline_monotonic: float | None = None,
    execution_report_path: str | Path | None = None,
    process_slots: int = 1,
    resource_check=None,
) -> dict[str, object]:
    import time
    from .model_fit_parallel import fit_parallel_capacity, run_fit_tasks

    if type(parallel_workers) is not int or parallel_workers < 1:
        raise ValueError("模型 parallel_workers 必须是正整数")
    if type(cpu_slots) is not int or cpu_slots < int(parameters["thread_count"]):
        raise ValueError("模型线程数超过节点 CPU 额度")
    if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
        raise TimeoutError("模型拟合已超过节点 deadline")
    budget = PandasFrameBudget(max_memory_bytes)
    metadata = _read_artifact_metadata(split_root)
    split = _verified_fit_split_manifest(split_root, metadata, budget)
    candidates = _candidates(parameters)
    if any(c["model"]["kwargs"].get("GPU", -1) >= 0 for c in candidates) and parallel_workers > 1:
        raise ModelMainlineError("同一GPU的模型拟合必须串行")
    active_preflight = _resolve_legacy_hook("model_dependency_preflight", model_dependency_preflight)
    preflight = active_preflight(candidates, thread_count=int(parameters["thread_count"]))
    if _text(parameters, "target_kind") != "regression":
        raise ModelMainlineError("Qlib 首批只支持 regression")
    expected_step = metadata.get("sequence", {}).get("step_len")
    lengths = {
        candidate["dataset"]["step_len"]
        for candidate in candidates
        if candidate["model"]["class"] in SEQUENCE_MODEL_CLASSES
    }
    if lengths and lengths != {expected_step}:
        raise ModelMainlineError("全部序列候选必须匹配split冻结的sequence_step_len")
    manifest = _search_manifest(candidates, parameters)
    candidate_ids = {item.parameter_hash: item.candidate_id for item in manifest.candidates}
    ledger = TrialLedger(manifest)
    features = tuple(metadata["feature_columns"])
    rows, fit_audit, tasks = [], [], []
    child_working_bytes = max_memory_bytes
    for ci, candidate in enumerate(candidates):
        cid = candidate_ids[typed_canonical_hash(candidate)]
        for fi, fold in enumerate(split.folds):
            component = f"qlib:{cid}:{fold.fold_id}"
            seed_manifest = build_seed_manifest(
                root_seed=root_seed,
                research_identity_hash=_hash(parameters, "research_identity_hash"),
                component_ids=(component,),
            )
            certificate = issue_fit_scope(
                component_id=component,
                component_kind="transformer",
                split_manifest=split,
                fold_id=fold.fold_id,
                input_columns=features,
                parameters=candidate,
                code_hash=typed_canonical_hash({"implementation": WALK_FORWARD_MODEL_VERSION}),
                environment_hash=str(preflight["preflight_hash"]),
                seed_manifest=seed_manifest,
            )
            tasks.append({
                "split_root": str(Path(split_root).resolve()),
                "split_artifact_hash": metadata["artifact_hash"],
                "candidate": candidate,
                "candidate_id": cid,
                "fold_index": fi,
                "candidate_index": ci,
                "fit_scope_ref": certificate.certificate_hash,
                "output_root": str(Path(output_root).resolve()),
                "root_seed": root_seed,
                "max_memory_bytes": child_working_bytes,
            })
    if parallel_workers > 1:
        import psutil

        parent_reserved = max(budget.retained_bytes, psutil.Process().memory_info().rss)
        per_worker_memory = (max_memory_bytes - parent_reserved) // parallel_workers
        recorder_slots = 2 if any(candidate["model"]["class"] == "LGBModel" for candidate in candidates) else 0
        capacity = fit_parallel_capacity(
            requested=parallel_workers,
            cpu_slots=cpu_slots,
            thread_count=int(parameters["thread_count"]),
            task_count=len(tasks),
            max_memory_bytes=max_memory_bytes,
            parent_input_bytes=budget.retained_bytes,
            child_working_bytes=max(1, per_worker_memory - 512 * 1024**2),
            process_slots=process_slots,
            child_aux_process_slots=recorder_slots,
        )
        for task in tasks:
            task["max_memory_bytes"] = max(1, (max_memory_bytes - parent_reserved) // capacity["actual_workers"] - 512 * 1024**2)
        completed = run_fit_tasks(
            tasks,
            worker_ref="research_pipeline.runtime.model_fit_execution:_execute_spawn_fit",
            capacity=capacity,
            max_memory_bytes=max_memory_bytes,
            max_temp_bytes=max_temp_bytes,
            output_root=output_root,
            deadline_monotonic=deadline_monotonic,
            execution_report_path=execution_report_path,
        )
        rows = completed
    else:
        session = {"fold_reader": _fold_reader(Path(split_root).resolve(), metadata, budget)}
        rows = [None] * len(tasks)
        for index in sorted(range(len(tasks)), key=lambda i: (tasks[i]["fold_index"], tasks[i]["candidate_index"])):
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                raise TimeoutError("模型拟合已超过节点deadline")
            if resource_check is not None:
                resource_check()
            task = dict(tasks[index], _worker_session_inputs=session, _resource_check=resource_check)
            rows[index] = _execute_spawn_fit(task)
            if resource_check is not None:
                resource_check()
        session.clear()
        gc.collect()
    for ci, candidate in enumerate(candidates):
        cid = candidate_ids[typed_canonical_hash(candidate)]
        ledger.start(cid)
        for fi, fold in enumerate(split.folds):
            row = rows[ci * len(split.folds) + fi]
            if row["status"] == "fitted":
                scope = tasks[ci * len(split.folds) + fi]["fit_scope_ref"]
                fit_audit.append({
                    "candidate_id": cid,
                    "fold_id": fold.fold_id,
                    "fit_scope_certificate_hash": scope,
                    "fit_scope_ref": scope,
                    "evidence_scope": "processor_train_sample_scope",
                    "model_path": row["model_path"],
                    "config_path": row["config_path"],
                    "train_ids_json": canonical_json(list(fold.train_ids)),
                    "validation_ids_json": canonical_json(list(fold.validation_ids)),
                })
        failures = [row["reason_code"] for row in rows if row["candidate_id"] == cid and row["status"] == "failed"]
        if failures:
            ledger.fail(cid, reason_code=failures[0])
    if max_temp_bytes is not None:
        from .model_fit_parallel import _directory_bytes

        if _directory_bytes(Path(output_root)) > max_temp_bytes:
            raise ValueError("模型拟合输出超过节点 temp 预算")
    result = _write_artifact(
        output_root,
        {
            "models": pd.DataFrame(rows),
            "fit_audit": pd.DataFrame(fit_audit),
            "trial_events": pd.DataFrame([event.__dict__ for event in ledger.events]),
            "learning_curves": _learning_curves(output_root, rows),
        },
        status="model_fit_succeeded",
        extra={
            "search_manifest": _search_payload(manifest),
            "preflight": preflight,
            "target_kind": "regression",
            "objective": _text(parameters, "objective"),
            "split_artifact_hash": metadata["artifact_hash"],
            "fit_trial_ledger_hash": ledger.ledger_hash,
            "research_identity_hash": _hash(parameters, "research_identity_hash"),
            "root_seed": root_seed,
            "model_inventory": "models",
            "model_inventory_schema": "research.qlib-model-inventory.v1",
        },
    )

    try:
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            raise TimeoutError("模型工件归并超过节点 deadline")
        if max_temp_bytes is not None and _directory_bytes(Path(output_root)) > max_temp_bytes:
            raise ValueError("模型拟合工件总量超过节点 temp 预算")
    except BaseException:
        (Path(output_root) / "artifact-metadata.json").unlink()
        raise
    return result


def _learning_curves(root, rows):
    records = []
    for row in rows:
        if row["status"] != "fitted":
            continue
        config = json.loads((Path(root) / row["config_path"]).read_text(encoding="utf-8"))
        records.extend(
            {"candidate_id": row["candidate_id"], "fold_id": row["fold_id"], **item}
            for item in config["training_curve"]
        )
    return pd.DataFrame(records, columns=["candidate_id", "fold_id", "segment", "metric", "iteration", "value"])


__all__ = ["_execute_spawn_fit", "execute_model_fit_artifact"]
