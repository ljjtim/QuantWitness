"""Walk-forward 六阶段正式算子的列式工件执行。"""

from __future__ import annotations

from datetime import datetime
from contextlib import contextmanager
import json
import gc
from pathlib import Path
import shutil
from typing import Callable, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.canonical import typed_canonical_hash_streamed
from research_pipeline.research.modeling import (
    CandidateFitRejected,
    ModelMainlineError,
    WALK_FORWARD_MODEL_VERSION,
    assemble_daily_model_samples,
    evaluate_locked_holdout,
    model_dependency_preflight,
    score_model,
)
from research_pipeline.research.modeling.qlib import (
    SEQUENCE_MODEL_CLASSES, fit_bundle, predict_bundle, evaluation_labels,
    normalize_candidates,
)
from research_pipeline.research.modeling.inputs import assemble_daily_feature_context
from .model_sequence import (sequence_step, build_runtime_windows, window_tables, window_metadata,
    load_runtime_windows, validate_sequence_candidates, sequence_for_candidate, _require_window_build_budget)
from research_pipeline.research.validation import (
    SplitFold,
    SplitManifest,
    TrialLedger,
    build_search_manifest,
    build_walk_forward,
    build_explicit_walk_forward,
    select_by_validation,
    issue_fit_scope,
    build_seed_manifest,
)
from research_pipeline.research.dataframe_budget import (
    PandasFrameBudget,
    PandasFrameBudgetError,
    pandas_frame_bytes,
)


_MODEL_FEATURE_COLUMNS = (
    "entity_id", "observation_session", "observation_time", "available_time",
    "window_sessions", "feature_id", "value", "status", "lineage_hash",
)


def execute_model_split_artifact(
    *,
    feature_root: str | Path,
    label_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    fixed_clock: str | datetime,
    max_memory_bytes: int,
    prediction_index_root: str | Path | None = None,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    step_len = sequence_step(parameters)
    sequence_windows = None
    feature_context = None
    feature_context, feature_metadata = _load_bounded_feature_context(feature_root, frame_budget, scratch_root=Path(output_root).parent)
    features = pd.DataFrame()
    holdout_start = pd.Timestamp(_text(parameters, "holdout_start"))
    if holdout_start.tzinfo is None:
        raise ModelMainlineError("holdout_start 必须包含时区")
    label_schema = pq.ParquetFile(_table_paths(label_root, "labels")[0]).schema_arrow

    def holdout_boundary(column: str) -> pa.Scalar:
        field_type = label_schema.field(column).type
        if not pa.types.is_timestamp(field_type) or field_type.tz is None:
            raise ModelMainlineError("Label 时间列必须包含时区")
        return pa.scalar(holdout_start.to_pydatetime(), type=pa.timestamp("ns", tz=field_type.tz))
    horizon_sessions = int(parameters["horizon_sessions"])
    target_field = _text(parameters, "target_field")
    _validate_label_row_group_boundaries(label_root)
    development_labels, label_metadata = _load_label_slice(
        label_root,
        columns=(
            "entity_id",
            "observation_session",
            "decision_time",
            "label_start_time",
            "label_end_time",
            "available_time",
            "horizon_sessions",
            target_field,
            "lineage_hash",
        ),
        predicate=(ds.field("label_end_time") < holdout_boundary("label_end_time"))
        & (ds.field("available_time") <= holdout_boundary("available_time"))
        & (ds.field("horizon_sessions") == horizon_sessions),
        frame_budget=frame_budget,
        label="Walk-forward development labels",
    )
    if feature_metadata.get("semantics_hash") != label_metadata.get("semantics_hash"):
        raise ModelMainlineError("Walk-forward Feature/Label 语义身份不一致")
    evaluation_scope = parameters.get("evaluation_scope", "final")
    if evaluation_scope not in {"development", "final"}:
        raise ModelMainlineError("evaluation_scope 必须为 development 或 final")
    if evaluation_scope == "development":
        holdout_index = pd.DataFrame({
            "sample_id": pd.Series(dtype="string"),
            "label_available_time": pd.Series(dtype="datetime64[ns, UTC]"),
            "observation_time": pd.Series(dtype="datetime64[ns, UTC]"),
        })
    else:
        holdout_labels, _ = _load_label_slice(
            label_root,
            columns=(
                "entity_id",
                "observation_session",
                "decision_time",
                "available_time",
                "horizon_sessions",
            ),
            predicate=(ds.field("decision_time") >= holdout_boundary("decision_time"))
            & (ds.field("horizon_sessions") == horizon_sessions),
            frame_budget=frame_budget,
            label="Walk-forward holdout label index",
        )
        if holdout_labels.empty and prediction_index_root is None:
            raise ModelMainlineError("development/locked holdout 必须同时非空")
        if holdout_labels.duplicated(["entity_id", "observation_session"]).any():
            raise ModelMainlineError("locked holdout 的目标 horizon Label 不唯一")
        holdout_index = pd.DataFrame(
            {
                "sample_id": (
                    holdout_labels["entity_id"].astype(str)
                    + ":"
                    + pd.to_datetime(holdout_labels["observation_session"]).dt.strftime("%Y-%m-%d")
                    + f":h{horizon_sessions}"
                ),
                "label_available_time": pd.to_datetime(
                    holdout_labels["available_time"], utc=True, errors="raise",
                ),
                "observation_time": pd.to_datetime(
                    holdout_labels["observation_session"], utc=True, errors="raise",
                ),
            }
        )
    frame_budget.reserve_frame(holdout_index, label="Walk-forward holdout index")
    if holdout_index["sample_id"].duplicated().any():
        raise ModelMainlineError("locked holdout sample_id 必须唯一")
    prediction_index = None
    locked_prediction_index = None
    if prediction_index_root is not None:
        from research_pipeline.research.modeling.prediction_index import validate_prediction_index
        prediction_index, index_metadata = _load_table(prediction_index_root, "prediction_index", frame_budget=frame_budget)
        if index_metadata.get("semantics_hash") != feature_metadata.get("semantics_hash"):
            raise ModelMainlineError("预测资格与Feature语义身份不一致")
        original_index = prediction_index
        prediction_index = validate_prediction_index(prediction_index, horizon_sessions=horizon_sessions)
        frame_budget.release_frame(original_index)
        del original_index
        if ((prediction_index.eligibility_available_time > pd.Timestamp(fixed_clock)) |
            (prediction_index.decision_time > pd.Timestamp(fixed_clock))).any():
            raise ModelMainlineError("预测资格或决策晚于本次固定时钟")
        locked = prediction_index.decision_time >= holdout_start
        if evaluation_scope == "development" and locked.any():
            raise ModelMainlineError("开发预测资格不得触及locked holdout")
        if evaluation_scope == "final":
            locked_prediction_index = prediction_index.loc[locked].copy()
            if locked_prediction_index.empty:
                raise ModelMainlineError("显式最终预测资格必须包含locked holdout")
            frame_budget.reserve_frame(locked_prediction_index, label="冻结holdout预测资格")
            prediction_index = prediction_index.loc[~locked].copy()
            frame_budget.release_frame(holdout_index)
            holdout_index = locked_prediction_index[["entity_id", "observation_session", "observation_time"]].copy()
            holdout_index["sample_id"] = (holdout_index.entity_id.astype(str) + ":" + pd.to_datetime(holdout_index.observation_session).dt.strftime("%Y-%m-%d") + f":h{horizon_sessions}")
            holdout_index["label_available_time"] = pd.NaT
            holdout_index = holdout_index[["sample_id", "label_available_time", "observation_time"]]
            frame_budget.reserve_frame(holdout_index, label="显式holdout索引")
        frame_budget.reserve_frame(prediction_index, label="显式预测资格")
    if step_len is not None:
        endpoint_source = development_labels if prediction_index is None else prediction_index
        targets = endpoint_source.loc[:, ["entity_id", "observation_session", "decision_time"]].copy()
        targets["sample_id"] = (targets.entity_id.astype(str) + ":"
            + pd.to_datetime(targets.observation_session).dt.strftime("%Y-%m-%d") + f":h{horizon_sessions}")
        if evaluation_scope == "final":
            holdout_targets = (locked_prediction_index if locked_prediction_index is not None else holdout_labels).loc[:, ["entity_id", "observation_session", "decision_time"]].copy()
            holdout_targets["sample_id"] = (holdout_targets.entity_id.astype(str) + ":" + pd.to_datetime(holdout_targets.observation_session).dt.strftime("%Y-%m-%d") + f":h{horizon_sessions}")
            targets = pd.concat([targets, holdout_targets], ignore_index=True)
            del holdout_targets
        _require_window_build_budget(feature_context[0], targets, step_len=step_len, budget=frame_budget)
        sequence_windows = build_runtime_windows(features, targets,
            calendar=parameters["calendar_sessions"], columns=feature_context[1], step_len=step_len,
            budget=frame_budget, _feature_context=feature_context)
        eligible = pd.Index(sequence_windows.targets.loc[~sequence_windows.targets.sample_id.isin(
            sequence_windows.exclusions.sample_id), "sample_id"])
        development_ids = (development_labels.entity_id.astype(str) + ":"
            + pd.to_datetime(development_labels.observation_session).dt.strftime("%Y-%m-%d") + f":h{horizon_sessions}")
        eligible_labels = development_labels.loc[development_ids.isin(eligible)].copy()
        frame_budget.reserve_frame(eligible_labels, label="完整窗口development标签")
        frame_budget.release_frame(development_labels)
        development_labels = eligible_labels
        del eligible_labels
        selected_holdout = holdout_index.loc[holdout_index.sample_id.isin(eligible)].copy()
        frame_budget.reserve_frame(selected_holdout, label="完整窗口holdout末端")
        frame_budget.release_frame(holdout_index)
        holdout_index = selected_holdout
        if prediction_index is not None:
            index_ids = (prediction_index.entity_id.astype(str) + ":" + pd.to_datetime(prediction_index.observation_session).dt.strftime("%Y-%m-%d") + f":h{horizon_sessions}")
            selected_index = prediction_index.loc[index_ids.isin(eligible)].copy()
            frame_budget.release_frame(prediction_index)
            prediction_index = selected_index
            frame_budget.reserve_frame(prediction_index, label="完整窗口预测资格")
        del selected_holdout, targets, eligible, development_ids
        if development_labels.empty or (evaluation_scope == "final" and holdout_index.empty):
            raise ModelMainlineError("完整序列窗口筛选后development/holdout末端为空")
    if evaluation_scope == "final":
        frame_budget.release_frame(holdout_labels)
        del holdout_labels
    prediction_samples, _ = assemble_daily_model_samples(features, development_labels,
        horizon_sessions=horizon_sessions, target_field=target_field,
        _feature_context=feature_context, allow_missing_target=True, prediction_index=prediction_index)
    prediction_samples["observation_date"] = prediction_samples.observation_time.dt.date
    frame_budget.reserve_frame(prediction_samples, label="完整截面预测样本")
    # 候选索引已用于窗口资格，未来收益缺失只影响拟合与评价，不影响每日图节点。
    finite_labels = development_labels.loc[np.isfinite(pd.to_numeric(development_labels[target_field], errors="raise"))].copy()
    if prediction_index is not None:
        keys = ["entity_id", "observation_session"]
        allowed = pd.MultiIndex.from_frame(prediction_index[keys])
        finite_labels = finite_labels.loc[pd.MultiIndex.from_frame(finite_labels[keys]).isin(allowed)].copy()
    frame_budget.reserve_frame(finite_labels, label="有限development标签")
    frame_budget.release_frame(development_labels)
    development_labels = finite_labels
    del finite_labels
    samples, feature_columns = assemble_daily_model_samples(
        features, development_labels, horizon_sessions=horizon_sessions,
        target_field=target_field, _feature_context=feature_context,
    )
    frame_budget.reserve_frame(samples, label="Walk-forward development samples")
    if feature_context is not None:
        frame_budget.release_frame(feature_context[0])
        del feature_context
    frame_budget.release_frame(development_labels)
    del development_labels
    visible_at = pd.Timestamp(fixed_clock)
    if visible_at.tzinfo is None:
        raise ModelMainlineError("模型 Runtime fixed_clock 必须包含时区")
    if (pd.to_datetime(samples["label_available_time"], utc=True) > visible_at).any():
        raise ModelMainlineError("模型 Runtime fixed_clock 早于样本 Label 的真实可见时间")
    if samples.empty:
        raise ModelMainlineError("development 样本必须非空")
    last_development_session = (samples["observation_time"].max().date() if prediction_index is None
                                else prediction_samples["observation_time"].max().date())
    calendar = tuple(
        session for session in (pd.Timestamp(value).date() for value in parameters["calendar_sessions"])
        if session <= last_development_session
    )
    # 六列切片与重命名期间最多同时持有两份列缓冲；字符串仍由samples持有。
    split_buffer_bytes = len(samples) * 6 * 8 + 132
    frame_budget.require_additional(2 * split_buffer_bytes, label="Walk-forward split输入构建")
    split_samples = samples.loc[:, ["sample_id", "observation_time", "decision_time", "label_start_time", "label_end_time", "label_available_time"]].rename(columns={"label_start_time": "label_start", "label_end_time": "label_end"})
    split_input_bytes = frame_budget.reserve_frame(split_samples, label="Walk-forward split输入", deep=False)
    explicit = parameters.get("explicit_folds_json")
    if explicit is not None:
        split = build_explicit_walk_forward(split_samples, calendar=calendar, folds=json.loads(explicit), prediction_samples=prediction_samples if prediction_index is not None else None, frame_budget=frame_budget)
    else:
        split = build_walk_forward(
            split_samples,
            calendar=calendar,
            train_sessions=int(parameters["train_sessions"]),
            validation_sessions=int(parameters["validation_sessions"]),
            test_sessions=int(parameters["test_sessions"]),
            step_sessions=int(parameters["step_sessions"]),
            embargo_sessions=int(parameters["embargo_sessions"]),
            expanding=_boolean(parameters, "expanding"),
            prediction_samples=prediction_samples if prediction_index is not None else None,
            frame_budget=frame_budget,
        )
    frame_budget.release_bytes(split_input_bytes)
    del split_samples
    return _write_artifact(
        output_root,
        {"samples": samples, "prediction_samples": prediction_samples, "holdout_index": holdout_index, "split_audit": split.audit,
         **({"locked_prediction_index": locked_prediction_index} if locked_prediction_index is not None else {}),
         **(window_tables(sequence_windows) if sequence_windows is not None else {})},
        status="model_split_succeeded",
        extra={
            **({"sequence": window_metadata(sequence_windows)} if sequence_windows is not None else {}),
            "feature_columns": list(feature_columns),
            "explicit_prediction_index": prediction_index is not None,
            "source_prediction_index_table_hash": index_metadata.get("table_hashes", {}).get("prediction_index") if prediction_index is not None else None,
            "split_manifest": _split_payload(split),
            "semantics_hash": feature_metadata.get("semantics_hash"),
            "holdout_start": holdout_start.isoformat(),
            "evaluation_scope": evaluation_scope,
            "holdout_end": (None if holdout_index.empty else pd.to_datetime(
                holdout_index["observation_time"], utc=True
            ).max().date().isoformat()),
            "fixed_clock": visible_at.isoformat(),
            "horizon_sessions": horizon_sessions,
            "target_field": target_field,
            "validation_sessions": int(parameters["validation_sessions"]),
            "source_feature_table_hash": feature_metadata["table_hashes"]["features"],
            "source_label_table_hash": label_metadata["table_hashes"]["labels"],
        },
    )


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
                budget.require_additional(len(samples) * (1 + 3 * np.dtype(np.intp).itemsize),
                                          label="fold 成员掩码与行索引")
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
    verify_table_records(root, name, metadata, lambda: _load_projected_table(
        paths, name, metadata, columns, budget, batch_consumer=lambda frame: None))


def _verified_fit_split_manifest(root, metadata, budget):
    """正式审计完整有界验收；fit只保留冻结fold/hash，不消费审计行。"""
    _verify_fold_table(root, "split_audit", metadata, budget)
    return _split_from_payload(metadata["split_manifest"], pd.DataFrame())


def _fold_reader(root, metadata, budget):
    from .model_fold_inputs import FoldInputReader
    return FoldInputReader(root, metadata, budget, _table_paths, _verify_fold_table)


def _fit_candidate_fold(*, samples, metadata, sequence_windows, budget, candidate,
                        candidate_id, fold_index, candidate_index, fit_scope_ref,
                        output_root, root_seed, fold_positions, sample_bytes,
                        bound_sequence_context=None, sequence_facts=None):
    fold = metadata["split_manifest"]["folds"][fold_index]
    budget.require_additional(2 * sample_bytes, label="候选 fold 过滤副本")
    train_positions, valid_positions = fold_positions[fold_index]
    train_indices = train_positions["position"].to_numpy()
    valid_indices = valid_positions["position"].to_numpy()
    failure_fit_time = pd.to_datetime(samples["label_available_time"].iloc[valid_indices], utc=True).max().isoformat()
    # 分段实测列字节，不为预算测量先创建两份完整宽输入。
    input_bytes = sum(pandas_frame_bytes(samples.iloc[indexes[start:start + 8192]])
                      for indexes in (train_indices, valid_indices)
                      for start in range(0, len(indexes), 8192))
    budget.require_additional(2 * input_bytes, label="Qlib 数据集与处理器副本")
    try:
        binding = ({"_bound_sequence_context": bound_sequence_context}
                   if bound_sequence_context is not None else {})
        if sequence_facts is not None:
            binding["_sequence_facts"] = sequence_facts
        row = fit_bundle(None, None, _fit_input=(samples, train_indices, valid_indices), candidate=candidate,
            feature_columns=tuple(metadata["feature_columns"]), output_root=output_root,
            bundle_path=f"bundles/{fold_index}/{candidate_index}", root_seed=root_seed,
            fit_scope_ref=fit_scope_ref,
            sequence_context=sequence_for_candidate(sequence_windows, candidate, budget), **binding)
        row.update(candidate_id=candidate_id, fold_id=fold["fold_id"], status="fitted", reason_code=None)
        return row
    except CandidateFitRejected as exc:
        return {"candidate_id": candidate_id, "fold_id": fold["fold_id"], "status": "failed",
            "reason_code": exc.reason_code, "bundle_path": None, "model_path": None,
            "config_path": None, "model_class": None, "feature_columns_json": None,
            "target_kind": "regression", "fit_scope_ref": fit_scope_ref,
            "fit_time": failure_fit_time}


def _bind_sequence_context_for_fold(
    *, samples, metadata, sequence_windows, budget, fold_positions, fold_index, sample_bytes,
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
            sequence_windows, frame, tuple(metadata["feature_columns"]),
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
    current = _read_artifact_metadata(root)
    expected = task["split_artifact_hash"]
    if current.get("artifact_hash") != expected:
        raise ModelMainlineError("模型拟合冻结split身份发生变化")
    key = (str(root), expected, int(task["max_memory_bytes"]), int(task["fold_index"]),
           str(Path(task["output_root"]).resolve()))
    session = task.get("_worker_session_inputs", {})
    reader = session.get("fold_reader")
    if reader is not None and reader.root == root and reader.metadata.get("artifact_hash") == expected:
        if reader.metadata != current:
            raise ModelMainlineError("模型拟合冻结split元数据发生变化")
        # 本任务仍完整读取并比较内容；验收后只保留会话中的同一元数据对象。
        current = reader.metadata
    cached = session.get("model_fit")
    if cached is not None and cached["key"] == key:
        if cached["metadata"] != current:
            raise ModelMainlineError("模型拟合冻结split元数据发生变化")
        current = cached["metadata"]
    else:
        # 单会话仅保留一份输入，来源变化前释放旧工作集，避免跨研究累计驻留。
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
        positions = _fit_fold_positions(samples, {**metadata, "split_manifest":{**metadata["split_manifest"], "folds":[fold]}}, budget)
        positions = [positions[0] for _ in metadata["split_manifest"]["folds"]]
        cached = {"key": key, "samples": samples, "metadata": metadata,
                  "windows": windows, "positions": positions, "budget": budget,
                  "bound_sequence_context": None, "bound_fold_index": None,
                  "sample_bytes": pandas_frame_bytes(samples), "sequence_facts": None}
        session["model_fit"] = cached
    samples, metadata = cached["samples"], cached["metadata"]
    windows, budget = cached["windows"], cached["budget"]
    bound = None
    if task["candidate"]["model"]["class"] in SEQUENCE_MODEL_CLASSES:
        bound = cached.get("bound_sequence_context")
        if bound is None:
            from research_pipeline.research.modeling.gru import bind_windows
            bound = bind_windows(windows, samples, tuple(metadata["feature_columns"]),
                                 int(metadata["sequence"]["step_len"]))
            _release_bound_sequence_context(windows, budget)
            for name in ("context", "targets", "members", "exclusions"):
                budget.reserve_frame(getattr(bound, name), label="fold已核序列窗口")
            cached["windows"] = windows = bound
            cached["bound_sequence_context"] = bound
    resource_check = task.get("_resource_check")
    if resource_check is not None:
        resource_check()
    row = _fit_candidate_fold(samples=samples, metadata=metadata, sequence_windows=windows,
        budget=budget, fold_positions=cached["positions"], sample_bytes=cached["sample_bytes"], **{key: task[key] for key in (
            "candidate", "candidate_id", "fold_index", "candidate_index", "fit_scope_ref",
            "output_root", "root_seed")}, bound_sequence_context=bound,
        sequence_facts=cached.get("sequence_facts") if bound is not None else None)
    if bound is not None and row["status"] == "fitted" and cached.get("sequence_facts") is None:
        config_path = Path(task["output_root"]) / row["config_path"]
        config = json.loads(config_path.read_text(encoding="utf-8"))
        cached["sequence_facts"] = config["sequence"]
    return row


def execute_model_fit_artifact(
    *, split_root: str | Path, parameters: Mapping[str, object], output_root: str | Path,
    root_seed: int, max_memory_bytes: int, parallel_workers: int = 1, cpu_slots: int = 1,
    max_temp_bytes: int | None = None, deadline_monotonic: float | None = None,
    execution_report_path: str | Path | None = None, process_slots: int = 1,
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
    preflight = model_dependency_preflight(candidates, thread_count=int(parameters["thread_count"]))
    if _text(parameters, "target_kind") != "regression":
        raise ModelMainlineError("Qlib 首批只支持 regression")
    expected_step = metadata.get("sequence", {}).get("step_len")
    lengths = {candidate["dataset"]["step_len"] for candidate in candidates if candidate["model"]["class"] in SEQUENCE_MODEL_CLASSES}
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
            seed_manifest = build_seed_manifest(root_seed=root_seed,
                research_identity_hash=_hash(parameters, "research_identity_hash"), component_ids=(component,))
            certificate = issue_fit_scope(component_id=component, component_kind="transformer",
                split_manifest=split, fold_id=fold.fold_id, input_columns=features,
                parameters=candidate, code_hash=typed_canonical_hash({"implementation": WALK_FORWARD_MODEL_VERSION}),
                environment_hash=str(preflight["preflight_hash"]), seed_manifest=seed_manifest)
            tasks.append({"split_root": str(Path(split_root).resolve()), "split_artifact_hash": metadata["artifact_hash"], "candidate": candidate,
                "candidate_id": cid, "fold_index": fi, "candidate_index": ci,
                "fit_scope_ref": certificate.certificate_hash, "output_root": str(Path(output_root).resolve()),
                "root_seed": root_seed, "max_memory_bytes": child_working_bytes})
    if parallel_workers > 1:
        import psutil
        parent_reserved = max(budget.retained_bytes, psutil.Process().memory_info().rss)
        per_worker_memory = (max_memory_bytes - parent_reserved) // parallel_workers
        recorder_slots = 2 if any(candidate["model"]["class"] == "LGBModel" for candidate in candidates) else 0
        capacity = fit_parallel_capacity(requested=parallel_workers, cpu_slots=cpu_slots,
            thread_count=int(parameters["thread_count"]), task_count=len(tasks),
            max_memory_bytes=max_memory_bytes, parent_input_bytes=budget.retained_bytes,
            child_working_bytes=max(1, per_worker_memory - 512*1024**2),
            process_slots=process_slots, child_aux_process_slots=recorder_slots)
        for task in tasks:
            task["max_memory_bytes"] = max(1, (max_memory_bytes - parent_reserved) // capacity["actual_workers"] - 512*1024**2)
        completed = run_fit_tasks(tasks,
            worker_ref="research_pipeline.runtime.walk_forward_model_execution:_execute_spawn_fit", capacity=capacity,
            max_memory_bytes=max_memory_bytes, max_temp_bytes=max_temp_bytes,
            output_root=output_root, deadline_monotonic=deadline_monotonic,
            execution_report_path=execution_report_path)
        rows = completed
    else:
        session = {"fold_reader": _fold_reader(Path(split_root).resolve(), metadata, budget)}
        rows = [None] * len(tasks)
        for index in sorted(range(len(tasks)), key=lambda i:(tasks[i]["fold_index"], tasks[i]["candidate_index"])):
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
                fit_audit.append({"candidate_id": cid, "fold_id": fold.fold_id,
                    "fit_scope_certificate_hash": scope, "fit_scope_ref": scope,
                    "evidence_scope": "processor_train_sample_scope",
                    "model_path": row["model_path"], "config_path": row["config_path"],
                    "train_ids_json": canonical_json(list(fold.train_ids)),
                    "validation_ids_json": canonical_json(list(fold.validation_ids))})
        failures = [row["reason_code"] for row in rows if row["candidate_id"] == cid and row["status"] == "failed"]
        if failures:
            ledger.fail(cid, reason_code=failures[0])
    if max_temp_bytes is not None:
        from .model_fit_parallel import _directory_bytes
        if _directory_bytes(Path(output_root)) > max_temp_bytes:
            raise ValueError("模型拟合输出超过节点 temp 预算")
    result = _write_artifact(output_root, {"models": pd.DataFrame(rows), "fit_audit": pd.DataFrame(fit_audit),
        "trial_events": pd.DataFrame([event.__dict__ for event in ledger.events]),
        "learning_curves": _learning_curves(output_root, rows)},
        status="model_fit_succeeded", extra={
            "search_manifest": _search_payload(manifest), "preflight": preflight,
            "target_kind": "regression", "objective": _text(parameters, "objective"),
            "split_artifact_hash": metadata["artifact_hash"], "fit_trial_ledger_hash": ledger.ledger_hash,
            "research_identity_hash": _hash(parameters, "research_identity_hash"), "root_seed": root_seed,
            "model_inventory": "models", "model_inventory_schema": "research.qlib-model-inventory.v1",
        })

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
        records.extend({"candidate_id": row["candidate_id"], "fold_id": row["fold_id"], **item}
                       for item in config["training_curve"])
    return pd.DataFrame(records, columns=["candidate_id", "fold_id", "segment", "metric", "iteration", "value"])


def _fold_frames(split_root, role, budget, *, metadata=None):
    metadata = _read_artifact_metadata(split_root) if metadata is None else metadata
    reader = _fold_reader(split_root, metadata, budget)
    for fold in metadata["split_manifest"]["folds"]:
        ids = fold["validation_ids"] if role == "validation" else fold["test_ids"]
        declared_range = fold.get("validation_range" if role == "validation" else "test_range")
        explicit = "prediction_samples" in metadata.get("row_counts", {}) and metadata.get("explicit_prediction_index") and declared_range
        frame = None if explicit else reader.samples(ids)
        if explicit:
            start, end = (pd.Timestamp(day).date() for day in declared_range)
            predicate = (ds.field("observation_date") >= pa.scalar(start)) & (ds.field("observation_date") <= pa.scalar(end))
            frame = reader.table("prediction_samples", predicate=predicate)
        elif "prediction_samples" in metadata.get("row_counts", {}) and not frame.empty:
            start, end = frame.observation_session.min(), frame.observation_session.max()
            budget.release_frame(frame)
            del frame
            frame = reader.table("prediction_samples", predicate=(ds.field("observation_session") >= pa.scalar(start)) & (ds.field("observation_session") <= pa.scalar(end)))
        budget.release_frame(frame)
        if explicit:
            frame["evaluation_eligible"] = frame.sample_id.isin(ids)
        frame["fold_id"] = fold["fold_id"]
        frame["fold_role"] = role
        budget.reserve_frame(frame, label="fold预测样本")
        try:
            yield frame
        finally:
            budget.release_frame(frame)
            del frame


def execute_model_predict_artifact(
    *, split_root: str | Path, model_root: str | Path, output_root: str | Path,
    max_memory_bytes: int,
) -> dict[str, object]:
    budget = PandasFrameBudget(max_memory_bytes)
    metadata = _read_artifact_metadata(split_root)
    models, model_metadata = _load_table(model_root, "models", frame_budget=budget)
    if model_metadata.get("split_artifact_hash") != metadata.get("artifact_hash"):
        raise ModelMainlineError("模型工件与 split 工件身份不一致")
    reader = _fold_reader(split_root, metadata, budget)
    def partitions():
        fitted = models.loc[models["status"] == "fitted"]
        for frame in _fold_frames(split_root, "validation", budget, metadata=metadata):
            fold_id = str(frame["fold_id"].iloc[0])
            sequence_windows = reader.windows(frame.sample_id) if metadata.get("sequence") else None
            bound = None
            try:
                for row in fitted.loc[fitted["fold_id"].astype(str) == fold_id].to_dict("records"):
                    config = json.loads((Path(model_root) / row["config_path"]).read_text(encoding="utf-8"))
                    candidate = config["candidate"]
                    binding = {}
                    if candidate["model"]["class"] in SEQUENCE_MODEL_CLASSES:
                        if bound is None:
                            sequence_for_candidate(sequence_windows, candidate, budget)
                            from research_pipeline.research.modeling.gru import bind_windows
                            bound = bind_windows(sequence_windows, frame, tuple(config["feature_columns"]),
                                                 candidate["dataset"]["step_len"])
                            for name in ("context", "targets", "members", "exclusions"):
                                budget.reserve_frame(getattr(bound, name), label=f"预测窗口缓存/{name}")
                        binding = {"_bound_sequence_context": bound}
                    prediction = predict_bundle(model_root, row, frame,
                        sequence_context=sequence_for_candidate(sequence_windows, candidate, budget), **binding)
                    output = _prediction_frame(frame, prediction, str(row["candidate_id"]), fold_id,
                        "validation", row["model_path"], row["config_path"], _score_semantics(model_root, row))
                    labeled = (np.isfinite(frame.target.to_numpy(float)) & frame.get("evaluation_eligible", pd.Series(True,index=frame.index)).to_numpy(bool))
                    output["evaluation_label"] = np.nan
                    if labeled.any():
                        output.loc[labeled, "evaluation_label"] = evaluation_labels(model_root, row, frame.loc[labeled])
                    budget.reserve_frame(output, label="Qlib validation 预测")
                    yield output
                    budget.release_frame(output)
            finally:
                _release_bound_sequence_context(bound, budget)
                reader.release_windows(sequence_windows)
                binding = None
                del sequence_windows, bound, frame
    return _write_partitioned_artifact(output_root,
        partitioned_tables={"validation_predictions": partitions()}, tables=lambda: {},
        status="model_validation_prediction_succeeded", extra={
            "search_manifest": model_metadata["search_manifest"], "target_kind": "regression",
            "objective": model_metadata["objective"],
        })


def execute_model_fold_metrics_artifact(
    *,
    prediction_root: str | Path,
    model_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    prediction_metadata = _read_artifact_metadata(prediction_root)
    models, model_metadata = _load_table(
        model_root,
        "models",
        frame_budget=frame_budget,
    )
    if prediction_metadata.get("search_manifest") != model_metadata.get("search_manifest"):
        raise ModelMainlineError("预测与模型的 SearchManifest 身份不一致")
    objective = _text(parameters, "objective")
    target_kind = _text(parameters, "target_kind")
    manifest = _search_from_payload(prediction_metadata["search_manifest"])
    if (
        objective != model_metadata.get("objective")
        or objective != manifest.objective
        or target_kind != model_metadata.get("target_kind")
    ):
        raise ModelMainlineError("fold metrics 的目标合同与模型 SearchManifest 不一致")
    metric_rows: list[dict[str, object]] = []
    seen_groups: set[tuple[str, str]] = set()
    for frame in _iter_table_frames(
        prediction_root,
        "validation_predictions",
        frame_budget=frame_budget,
    ):
        candidate_ids = set(frame["candidate_id"].astype(str))
        fold_ids = set(frame["fold_id"].astype(str))
        if len(candidate_ids) != 1 or len(fold_ids) != 1:
            raise ModelMainlineError("validation 预测分区必须只包含一个候选和 fold")
        candidate_id = next(iter(candidate_ids))
        fold_id = next(iter(fold_ids))
        group = (candidate_id, fold_id)
        if group in seen_groups:
            raise ModelMainlineError("validation 预测候选/fold 分区重复")
        seen_groups.add(group)
        metric_rows.append({
            "candidate_id": candidate_id, "fold_id": fold_id, "stage": "validation",
            objective: score_model(
                frame.loc[np.isfinite(frame.evaluation_label), "evaluation_label"].to_numpy(float), frame.loc[np.isfinite(frame.evaluation_label), "prediction"].to_numpy(float),
                objective=objective, target_kind=target_kind,
            ),
            "sample_count": int(np.isfinite(frame.evaluation_label).sum()),
            "label_end_time": pd.to_datetime(frame.loc[np.isfinite(frame.evaluation_label), "label_end_time"], utc=True).max().isoformat(),
            "label_available_time": pd.to_datetime(frame.loc[np.isfinite(frame.evaluation_label), "label_available_time"], utc=True).max().isoformat(),
        })
    metrics = pd.DataFrame(metric_rows)
    all_aggregate = metrics.groupby("candidate_id", as_index=False).agg(
        stage=("stage", "first"), **{objective: (objective, "mean")},
    )
    ledger = _replay_ledger(manifest, models, all_aggregate, objective)
    completed_ids = {candidate_id for candidate_id, state in ledger.states.items() if state == "completed"}
    aggregate = all_aggregate.loc[all_aggregate["candidate_id"].isin(completed_ids)].reset_index(drop=True)
    return _write_artifact(
        output_root,
        {"fold_metrics": metrics, "candidate_metrics": aggregate, "trial_events": pd.DataFrame([event.__dict__ for event in ledger.events])},
        status="model_fold_metrics_succeeded",
        extra={
            "search_manifest": prediction_metadata["search_manifest"],
            "trial_ledger_hash": ledger.ledger_hash,
            "objective": objective,
            "target_kind": target_kind,
        },
    )


def execute_model_selection_artifact(
    *,
    metrics_root: str | Path,
    split_root: str | Path,
    model_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    candidate_metrics, metrics_metadata = _load_table(
        metrics_root,
        "candidate_metrics",
        frame_budget=frame_budget,
    )
    split_metadata = _read_artifact_metadata(split_root)
    if split_metadata.get("evaluation_scope", "final") != "final" and not parameters.get("emit_all_test", False):
        raise ModelMainlineError("开发切分只有显式固定候选统计才能进入test阶段")
    models, model_metadata = _load_table(
        model_root,
        "models",
        frame_budget=frame_budget,
    )
    if (
        metrics_metadata.get("search_manifest") != model_metadata.get("search_manifest")
        or model_metadata.get("split_artifact_hash") != split_metadata.get("artifact_hash")
    ):
        raise ModelMainlineError("选择阶段的 SearchManifest 或预处理身份不一致")
    objective = _text(parameters, "objective")
    manifest = _search_from_payload(metrics_metadata["search_manifest"])
    if (
        objective != metrics_metadata.get("objective")
        or objective != model_metadata.get("objective")
        or _text(parameters, "target_kind") != metrics_metadata.get("target_kind")
        or _text(parameters, "target_kind") != model_metadata.get("target_kind")
        or _text(parameters, "direction") != manifest.direction
    ):
        raise ModelMainlineError("选择阶段的目标、方向或 target_kind 合同不一致")
    if parameters.get("emit_all_test", False) and split_metadata.get("evaluation_scope") != "development":
        raise ModelMainlineError("全部固定候选test统计仅允许development研究")
    reader = _fold_reader(split_root, split_metadata, frame_budget)
    selected = select_by_validation(
        candidate_metrics, objective=objective, direction=_text(parameters, "direction"),
    )
    winner = str(selected["candidate_id"])
    fold_metrics, _ = _load_table(metrics_root, "fold_metrics", frame_budget=frame_budget)
    fit_times = pd.to_datetime(models["fit_time"], utc=True)
    metric_available = pd.to_datetime(fold_metrics["label_available_time"], utc=True)
    metric_end = pd.to_datetime(fold_metrics["label_end_time"], utc=True)
    final_selection_time = metric_available.max()
    if final_selection_time > pd.Timestamp(split_metadata["holdout_start"]):
        raise ModelMainlineError("最终候选选择晚于 locked holdout 起点")
    test_metric_rows: list[dict[str, object]] = []
    fold_selection_rows: list[dict[str, object]] = []
    fold_trial_rows: list[dict[str, object]] = []
    final_ledger = _replay_ledger(manifest, models, candidate_metrics, objective)
    selected_parameters = next(dict(item.parameters) for item in manifest.candidates if item.candidate_id == winner)
    extra = {
        "split_manifest_hash": split_metadata["split_manifest"]["manifest_hash"],
        "semantics_hash": split_metadata.get("semantics_hash"),
        "final_fit_contract": {

            "target_kind": model_metadata["target_kind"],
            "objective": model_metadata["objective"],
            "research_identity_hash": model_metadata["research_identity_hash"],
            "thread_count": int(model_metadata["preflight"]["thread_count"]),
            "root_seed": model_metadata["root_seed"],
        },
    }

    def test_prediction_partitions() -> Iterable[pd.DataFrame]:
        seen_folds: set[str] = set()
        for fold_frame in _fold_frames(split_root, "test", frame_budget, metadata=split_metadata):
            sequence_windows = reader.windows(fold_frame.sample_id) if split_metadata.get("sequence") else None
            fold_id = str(fold_frame["fold_id"].iloc[0])
            if fold_id in seen_folds:
                raise ModelMainlineError(f"重复的 test fold 分区: {fold_id}")
            seen_folds.add(fold_id)
            selection_time = pd.to_datetime(fold_frame["decision_time"], utc=True).min()
            # 全个 validation 分数可见后才能参与选择；未来 fold 的成败也不可回填。
            visible_metrics = fold_metrics.loc[
                (metric_available <= selection_time) & (metric_end < selection_time)
            ]
            aggregate = visible_metrics.groupby("candidate_id", as_index=False).agg(
                stage=("stage", "first"), **{objective: (objective, "mean")},
            )
            visible_models = models.loc[
                fit_times <= selection_time
            ]
            ledger = _replay_ledger(manifest, visible_models, aggregate, objective)
            completed_ids = {key for key, state in ledger.states.items() if state == "completed"}
            current_models = models.loc[
                (models["fold_id"].astype(str) == fold_id) & (models["status"] == "fitted") & (fit_times <= selection_time)
            ]
            eligible_ids = completed_ids & set(current_models["candidate_id"])
            local_selected = select_by_validation(
                aggregate.loc[aggregate["candidate_id"].isin(eligible_ids)],
                objective=objective, direction=manifest.direction,
            )
            local_winner = str(local_selected["candidate_id"])
            model_rows = current_models if parameters.get("emit_all_test", False) else current_models.loc[current_models["candidate_id"] == local_winner]
            winner_metric = None
            winner_predicted = False
            for _, model_row in model_rows.iterrows():
                candidate_id = str(model_row["candidate_id"])
                config = json.loads((Path(model_root) / model_row["config_path"]).read_text(encoding="utf-8"))
                predictions = predict_bundle(model_root, model_row, fold_frame,
                    sequence_context=sequence_for_candidate(sequence_windows, config["candidate"], frame_budget))
                output = _prediction_frame(
                    fold_frame, predictions, candidate_id, fold_id, "test", str(model_row["model_path"]), str(model_row["config_path"]), _score_semantics(model_root, model_row),
                )
                labeled = (np.isfinite(fold_frame.target.to_numpy(float)) & fold_frame.get("evaluation_eligible", pd.Series(True,index=fold_frame.index)).to_numpy(bool))
                output["evaluation_label"] = np.nan
                if labeled.any():
                    output.loc[labeled, "evaluation_label"] = evaluation_labels(model_root, model_row, fold_frame.loc[labeled])
                metric = score_model(
                    output.loc[labeled, "evaluation_label"].to_numpy(float), output.loc[labeled, "prediction"].to_numpy(float),
                    objective=objective, target_kind=_text(parameters, "target_kind"),
                ) if labeled.any() else None
                test_metric_rows.append({
                    "candidate_id": candidate_id, "fold_id": fold_id, "stage": "test",
                    objective: np.nan if metric is None else metric, "sample_count": int(labeled.sum()),
                })
                frame_budget.reserve_frame(output, label=f"{candidate_id}/{fold_id} test预测")
                yield output
                frame_budget.release_frame(output)
                if candidate_id == local_winner:
                    winner_metric = metric
                    winner_predicted = True
            if not winner_predicted:
                raise ModelMainlineError("test缺少当时选定候选")
            metric = winner_metric
            frozen_ledger_hash = ledger.ledger_hash
            if metric is not None:
                ledger.record_final_evaluation(local_winner, stage="test", metric_value=metric)
            fold_selection_rows.append({
                "fold_id": fold_id, "selected_candidate_id": local_winner, "test_metric": np.nan if winner_metric is None else float(winner_metric),
                "selection_time": selection_time.isoformat(),
                "validation_metric": float(local_selected[objective]),
                "validation_fold_count": int((visible_metrics["candidate_id"] == local_winner).sum()),
                "selection_ledger_hash": frozen_ledger_hash,
                "trial_ledger_hash": ledger.ledger_hash,
            })
            fold_trial_rows.extend({"fold_id": fold_id, **event.__dict__} for event in ledger.events)
            reader.release_windows(sequence_windows)
            del sequence_windows, fold_frame
        if seen_folds != set(models["fold_id"].astype(str)):
            raise ModelMainlineError("test fold 分区集合与模型工件不一致")

    def selection_tables() -> Mapping[str, pd.DataFrame]:
        if not test_metric_rows:
            raise ModelMainlineError("没有可用的逐时点 test 预测")
        selection = {
            "selected_candidate_id": winner,
            "selected_parameters": selected_parameters,
            "validation_metric": float(selected[objective]),
            "test_metric": (float(pd.to_numeric(pd.DataFrame(fold_selection_rows)["test_metric"], errors="raise").mean()) if any(pd.notna(row["test_metric"]) for row in fold_selection_rows) else None),
            "test_metric_scope": "walk_forward_frozen_candidates",
            "selection_scope": "subsequent_locked_holdout",
            "selection_time": final_selection_time.isoformat(),
            "objective": objective, "direction": manifest.direction,
            "search_manifest_hash": manifest.manifest_hash,
            "trial_ledger_hash": final_ledger.ledger_hash,
        }
        selection["selection_hash"] = typed_canonical_hash(selection)
        extra["selection"] = selection
        return {
            "selection": pd.DataFrame([{**{key: value for key, value in selection.items() if key != "selected_parameters"}, "selected_parameters_json": canonical_json(selected_parameters)}]),
            "fold_selections": pd.DataFrame(fold_selection_rows),
            "test_metrics": pd.DataFrame(test_metric_rows),
            "trial_events": pd.DataFrame([event.__dict__ for event in final_ledger.events]),
            "fold_trial_events": pd.DataFrame(fold_trial_rows),
        }

    return _write_partitioned_artifact(
        output_root,
        partitioned_tables={"test_predictions": test_prediction_partitions()},
        tables=selection_tables,
        status="model_selection_succeeded",
        extra=extra,
    )


def execute_model_locked_holdout_artifact(
    *,
    split_root: str | Path,
    selection_root: str | Path,
    feature_root: str | Path,
    label_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    ledger_root: str | Path,
    root_seed: int,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    development_samples, split_metadata = _load_table(
        split_root, "samples", frame_budget=frame_budget,
    )
    if split_metadata.get("evaluation_scope", "final") != "final":
        raise ModelMainlineError("开发切分不能打开locked holdout")
    holdout_index, _ = _load_table(
        split_root, "holdout_index", frame_budget=frame_budget,
    )
    selection_table, selection_metadata = _load_table(
        selection_root, "selection", frame_budget=frame_budget,
    )
    if len(selection_table) != 1:
        raise ModelMainlineError("locked holdout 必须绑定唯一 selection")
    if selection_metadata.get("split_manifest_hash") != split_metadata["split_manifest"]["manifest_hash"]:
        raise ModelMainlineError("locked holdout 的 SplitManifest 与 selection 不一致")
    expected_fit_contract = {

        "target_kind": _text(parameters, "target_kind"),
        "objective": _text(parameters, "objective"),
        "research_identity_hash": _hash(parameters, "research_identity_hash"),
        "thread_count": int(parameters["thread_count"]),
        "root_seed": root_seed,
    }
    if selection_metadata.get("final_fit_contract") != expected_fit_contract:
        raise ModelMainlineError("locked holdout 的最终拟合合同与 selection 不一致")
    selected_parameters = json.loads(selection_table.iloc[0]["selected_parameters_json"])
    sequence_windows = load_runtime_windows(split_root, split_metadata, frame_budget, _load_table)
    sequence_context = sequence_for_candidate(sequence_windows, selected_parameters, frame_budget)
    loaded_holdout_windows = None

    def preflight_holdout_samples() -> Mapping[str, object]:
        feature_schema = _preflight_table(feature_root, "features")
        label_schema = _preflight_table(label_root, "labels")
        required_feature = {
            "entity_id", "observation_session", "observation_time",
            "available_time", "window_sessions", "feature_id", "value",
            "status", "lineage_hash",
        }
        required_label = {
            "entity_id", "observation_session", "decision_time",
            "label_start_time", "label_end_time", "available_time",
            "horizon_sessions", str(split_metadata["target_field"]), "lineage_hash",
        }
        if not required_feature <= set(feature_schema["columns"]):
            raise ModelMainlineError("locked holdout Feature schema 不闭合")
        if not required_label <= set(label_schema["columns"]):
            raise ModelMainlineError("locked holdout Label schema 不闭合")
        return {"features": feature_schema, "labels": label_schema}

    def load_holdout_samples() -> pd.DataFrame:
        nonlocal loaded_holdout_windows
        holdout_context, feature_metadata = _load_bounded_feature_context(feature_root, frame_budget, scratch_root=Path(output_root).parent)
        features = pd.DataFrame()
        labels, label_metadata = _load_table(
            label_root, "labels", frame_budget=frame_budget,
            columns=(
                "entity_id", "observation_session", "decision_time", "label_start_time",
                "label_end_time", "available_time", "horizon_sessions",
                str(split_metadata["target_field"]), "lineage_hash",
            ),
        )
        semantics_hash = split_metadata.get("semantics_hash")
        if (
            feature_metadata.get("semantics_hash") != semantics_hash
            or label_metadata.get("semantics_hash") != semantics_hash
        ):
            raise ModelMainlineError("locked holdout 的 Feature/Label 语义身份与 split 不一致")
        if (
            feature_metadata.get("table_hashes", {}).get("features")
            != split_metadata.get("source_feature_table_hash")
            or label_metadata.get("table_hashes", {}).get("labels")
            != split_metadata.get("source_label_table_hash")
        ):
            raise ModelMainlineError("locked holdout 的原始 Feature/Label 工件与 split 不一致")
        locked_index = None
        if split_metadata.get("explicit_prediction_index"):
            locked_index, _ = _load_table(split_root, "locked_prediction_index", frame_budget=frame_budget)
            wanted = set(holdout_index.sample_id.astype(str))
            identifiers = locked_index.entity_id.astype(str) + ":" + pd.to_datetime(locked_index.observation_session).dt.strftime("%Y-%m-%d") + f":h{int(split_metadata['horizon_sessions'])}"
            locked_index = locked_index.loc[identifiers.isin(wanted)].copy()
        if sequence_windows is not None:
            label_ids = (labels.entity_id.astype(str) + ":"
                + pd.to_datetime(labels.observation_session).dt.strftime("%Y-%m-%d")
                + f":h{int(split_metadata['horizon_sessions'])}")
            selected_labels = labels.loc[label_ids.isin(set(holdout_index.sample_id.astype(str)))].copy()
            frame_budget.reserve_frame(selected_labels, label="完整窗口holdout标签")
            frame_budget.release_frame(labels)
            labels = selected_labels
        holdout_targets = (labels if locked_index is None else locked_index).loc[:, ["entity_id", "observation_session", "decision_time"]].copy()
        holdout_targets["sample_id"] = (holdout_targets.entity_id.astype(str) + ":" + pd.to_datetime(holdout_targets.observation_session).dt.strftime("%Y-%m-%d") + f":h{int(split_metadata['horizon_sessions'])}")
        finite_labels = (labels if locked_index is not None else labels.loc[np.isfinite(pd.to_numeric(labels[str(split_metadata["target_field"])], errors="raise"))]).copy()
        frame_budget.reserve_frame(finite_labels, label="有限holdout标签")
        frame_budget.release_frame(labels)
        labels = finite_labels
        samples, feature_columns = assemble_daily_model_samples(
            features,
            labels,
            horizon_sessions=int(split_metadata["horizon_sessions"]),
            target_field=str(split_metadata["target_field"]),
            _feature_context=holdout_context, allow_missing_target=locked_index is not None,
            prediction_index=locked_index,
        )
        frame_budget.reserve_frame(samples, label="Walk-forward holdout samples")
        if list(feature_columns) != list(split_metadata["feature_columns"]):
            raise ModelMainlineError("locked holdout 的 Feature 列与 split 不一致")
        wanted = set(holdout_index["sample_id"].astype(str))
        selected_samples = samples.loc[
            samples["sample_id"].astype(str).isin(wanted)
        ].copy()
        frame_budget.reserve_frame(
            selected_samples,
            label="Walk-forward selected holdout samples",
        )
        if sequence_context is not None:
            loaded_holdout_windows = build_runtime_windows(features, holdout_targets,
                calendar=sequence_context.calendar_sessions, columns=sequence_context.feature_columns,
                step_len=sequence_context.step_len, budget=frame_budget, _feature_context=holdout_context)
        frame_budget.release_frame(holdout_context[0])
        frame_budget.release_frame(samples)
        frame_budget.release_frame(features)
        frame_budget.release_frame(labels)
        return selected_samples

    from research_pipeline.research.validation.holdout_series import series_from_parameters
    series = series_from_parameters(parameters)
    result = evaluate_locked_holdout(
        development_samples,
        holdout_preflight=preflight_holdout_samples,
        holdout_loader=load_holdout_samples,
        development_ids=tuple(development_samples["sample_id"]),
        holdout_ids=tuple(holdout_index["sample_id"]),
        holdout_start=str(split_metadata["holdout_start"]),
        holdout_end=str(split_metadata["holdout_end"]),
        feature_columns=tuple(split_metadata["feature_columns"]),
        selected_candidate=selected_parameters,
        target_kind=_text(parameters, "target_kind"),
        objective=_text(parameters, "objective"),
        validation_sessions=int(split_metadata["validation_sessions"]),
        output_root=output_root,
        research_identity_hash=_hash(parameters, "research_identity_hash"),
        data_snapshot_hash=typed_canonical_hash(
            {
                "feature_table": split_metadata["source_feature_table_hash"],
                "label_table": split_metadata["source_label_table_hash"],
                "holdout_ids": sorted(holdout_index["sample_id"].astype(str)),
            }
        ),
        selection_hash=str(selection_metadata["selection"]["selection_hash"]),
        package_hash=_hash(parameters, "package_hash"),
        implementation_hash=_hash(parameters, "implementation_hash"),
        actor=_text(parameters, "holdout_actor"),
        reason=_text(parameters, "holdout_reason"),
        unlock_at=pd.Timestamp(_text(parameters, "holdout_unlock_at")).to_pydatetime(),
        fixed_clock=pd.Timestamp(_text(parameters, "fixed_clock")).to_pydatetime(),
        ledger_root=ledger_root,
        research_series=series.declaration,
        root_seed=root_seed,
        thread_count=int(parameters["thread_count"]),
        development_sequence_context=sequence_context,
        holdout_sequence_loader=(lambda: loaded_holdout_windows) if sequence_context is not None else None,
    )
    receipt = {key: value for key, value in result.items() if key not in {"predictions", "model_row"}}
    persistent_ledger_root = (
        Path(ledger_root) / str(result["holdout_identity_hash"])
    )
    ledger_destination = Path(output_root) / "holdout-ledger"
    ledger_destination.mkdir(parents=True, exist_ok=False)
    for name in ("plan.json", "prepared.json", "opened.json", "terminal.json", "series.json"):
        source = persistent_ledger_root / name
        if not source.is_file():
            raise ModelMainlineError(f"locked holdout 缺少持久账本文件: {name}")
        shutil.copy2(source, ledger_destination / name)
    holdout_predictions = pd.DataFrame(result["predictions"])
    for column in ("actual", "raw_label", "evaluation_label"):
        holdout_predictions[column] = pd.to_numeric(holdout_predictions[column], errors="raise").astype(float)
    for column in ("label_start_time", "label_end_time", "label_available_time"):
        holdout_predictions[column] = holdout_predictions[column].astype("string")
    return _write_artifact(
        output_root,
        {"holdout_predictions": holdout_predictions, "holdout_receipt": pd.DataFrame([receipt]), "models": pd.DataFrame([result["model_row"]]),
         "learning_curves": _learning_curves(output_root, [result["model_row"]])},
        status="model_locked_holdout_succeeded",
        extra={"holdout": receipt, "model_inventory": "models", "model_inventory_schema": "research.qlib-model-inventory.v1"},
    )


def _replay_ledger(manifest: object, models: pd.DataFrame, aggregate: pd.DataFrame, objective: str) -> TrialLedger:
    ledger = TrialLedger(manifest)
    for candidate in manifest.candidates:
        candidate_id = candidate.candidate_id
        candidate_models = models.loc[models["candidate_id"] == candidate_id]
        ledger.start(candidate_id)
        if candidate_models.empty or "failed" in set(candidate_models["status"]):
            reason = next((str(value) for value in candidate_models["reason_code"] if pd.notna(value)), "model_fit_failed")
            ledger.fail(candidate_id, reason_code=reason)
        else:
            metric = aggregate.loc[aggregate["candidate_id"] == candidate_id, objective]
            if len(metric) != 1:
                ledger.fail(candidate_id, reason_code="validation_metric_missing")
            else:
                ledger.complete(candidate_id, validation_metric=float(metric.iloc[0]))
    ledger.require_terminal()
    return ledger


def _search_manifest(candidates: list[dict[str, object]], parameters: Mapping[str, object]):
    return build_search_manifest(
        search_id=_text(parameters, "search_id"), candidates=candidates, method="grid",
        max_trials=len(candidates), max_parallel=len(candidates),
        stopping_condition="complete_declared_candidate_universe",
        objective=_text(parameters, "objective"), direction=_text(parameters, "direction"),
        frozen_at=pd.Timestamp(_text(parameters, "search_frozen_at")).to_pydatetime(),
    )


def _search_payload(manifest: object) -> dict[str, object]:
    return {
        "search_id": manifest.search_id,
        "candidates": [
            {
                "candidate_id": item.candidate_id, "parameters": dict(item.parameters),
                "parameter_hash": item.parameter_hash,
            }
            for item in manifest.candidates
        ],
        "method": manifest.method, "max_trials": manifest.max_trials,
        "max_parallel": manifest.max_parallel, "stopping_condition": manifest.stopping_condition,
        "objective": manifest.objective, "direction": manifest.direction,
        "frozen_at": manifest.frozen_at.isoformat(), "stage": manifest.stage,
        "manifest_hash": manifest.manifest_hash,
    }


def _search_from_payload(payload: Mapping[str, object]):
    manifest = build_search_manifest(
        search_id=str(payload["search_id"]),
        candidates=[dict(item["parameters"]) for item in payload["candidates"]],
        method=str(payload["method"]), max_trials=int(payload["max_trials"]),
        max_parallel=int(payload["max_parallel"]),
        stopping_condition=str(payload["stopping_condition"]), objective=str(payload["objective"]),
        direction=str(payload["direction"]), frozen_at=datetime.fromisoformat(str(payload["frozen_at"])),
        stage=str(payload["stage"]),
    )
    if manifest.manifest_hash != payload["manifest_hash"]:
        raise ModelMainlineError("SearchManifest 工件发生漂移")
    return manifest


def _split_payload(split: SplitManifest) -> dict[str, object]:
    return {
        "method": split.method, "calendar_hash": split.calendar_hash,
        "manifest_hash": split.manifest_hash,
        "folds": [
            {
                "fold_id": fold.fold_id, "train_ids": list(fold.train_ids),
                "validation_ids": list(fold.validation_ids), "test_ids": list(fold.test_ids),
                "purged_ids": list(fold.purged_ids), "embargoed_ids": list(fold.embargoed_ids),
                "validation_range": list(fold.validation_range) if fold.validation_range else None,
                "test_range": list(fold.test_range) if fold.test_range else None,
            }
            for fold in split.folds
        ],
    }


def _split_from_payload(payload: Mapping[str, object], audit: pd.DataFrame) -> SplitManifest:
    folds = tuple(
        SplitFold(
            str(item["fold_id"]), tuple(item["train_ids"]), tuple(item["validation_ids"]),
            tuple(item["test_ids"]), tuple(item["purged_ids"]), tuple(item["embargoed_ids"]),
            tuple(item["validation_range"]) if item.get("validation_range") else None,
            tuple(item["test_range"]) if item.get("test_range") else None,
        )
        for item in payload["folds"]
    )
    split = SplitManifest(
        str(payload["method"]), folds, audit, str(payload["calendar_hash"]), str(payload["manifest_hash"]),
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
    for name, frame in sorted(tables.items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / "part-00000.parquet", index=False)
        row_counts[name] = len(frame)
        table_hashes[name] = typed_canonical_hash_streamed(_records(frame))
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION, "status": status,
        "row_counts": row_counts, "table_hashes": table_hashes, **dict(extra),
    }
    payload["artifact_hash"] = typed_canonical_hash(payload)
    (root / "artifact-metadata.json").write_text(canonical_json(payload), encoding="utf-8")
    return payload


def _write_partitioned_artifact(
    output_root: str | Path,
    *,
    partitioned_tables: Mapping[str, Iterable[pd.DataFrame]],
    tables: Callable[[], Mapping[str, pd.DataFrame]],
    status: str,
    extra: Mapping[str, object],
    partition_order: Mapping[str, Mapping[tuple[str, str], int]] | None = None,
    partition_schema: Callable[[str, int, pa.Schema], pa.Schema] | None = None,
) -> dict[str, object]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    table_hashes: dict[str, str] = {}
    table_hash_modes: dict[str, str] = {}
    table_partitions: dict[str, list[dict[str, object]]] = {}
    row_counts: dict[str, int] = {}
    for name, frames in sorted(partitioned_tables.items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        partition_hashes: list[str] = []
        descriptors: list[dict[str, object]] = []
        row_count = 0
        for index, frame in enumerate(frames):
            if partition_order is not None and name in partition_order:
                index = partition_order[name][(str(frame["candidate_id"].iloc[0]), str(frame["fold_id"].iloc[0]))]
            target = directory / f"part-{index:05d}.parquet"
            if partition_schema is None:
                frame.to_parquet(target, index=False)
            else:
                table = pa.Table.from_pandas(frame, preserve_index=False)
                pq.write_table(table.cast(partition_schema(name, index, table.schema)), target)
            row_count += len(frame)
            partition_hashes.append(typed_canonical_hash_streamed(_records(frame)))
            descriptor = {
                "path": f"part-{index:05d}.parquet", "row_count": len(frame),
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
        table_hashes[name] = typed_canonical_hash_streamed(_records(frame))
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
    root: str | Path, *, require_model_contract: bool = True,
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
            partition_hashes = ([] if verified else
                                [typed_canonical_hash_streamed(_records(frame)) for frame in frames])
            frame = frame_budget.concat_reserved_frames(frames, label=f"模型表 {name}")
            actual_hash = metadata["table_hashes"][name] if verified else typed_canonical_hash(partition_hashes)
        else:
            frame = frame_budget.collect_arrow_batches(
                (
                    batch
                    for path in paths
                    for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536)
                ),
                label=f"模型表 {name}",
            )
            actual_hash = metadata["table_hashes"][name] if verified else typed_canonical_hash_streamed(_records(frame))
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
                    # 先编码全部字段，包括后面不留在模型矩阵中的来源列表。
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
                    partition_hash = typed_canonical_hash_streamed(records((path,)))
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
                actual_hash = typed_canonical_hash_streamed(records(paths))
        if actual_hash != metadata.get("table_hashes", {}).get(name):
            raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
        declared_rows = metadata.get("row_counts", {}).get(name)
        if declared_rows is not None and total_rows != declared_rows:
            raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}")
        return (frame_budget.concat_reserved_frames(frames, label=f"模型投影表 {name}")
                if batch_consumer is None else None)
    except (OSError, ValueError, pa.ArrowException, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc


def _load_bounded_feature_context(root, budget, *, scratch_root=None):
    """完整校验长表并按证券有界归组，常驻内存只保留宽上下文。"""
    from .model_feature_stream import compact_feature_context
    metadata = _read_artifact_metadata(root, require_model_contract=False)
    value_names = None
    schemas = [pq.ParquetFile(path).schema_arrow for path in _table_paths(root, "features")]
    vector_flags = [pa.types.is_list(schema.field("value").type) or pa.types.is_fixed_size_list(schema.field("value").type) for schema in schemas]
    if any(vector_flags):
        if not all(vector_flags):
            raise ModelMainlineError("Feature不得混合标量与向量分片")
        declarations = [(schema.field("value").metadata or {}).get(b"model_feature_value_names") for schema in schemas]
        if not declarations[0] or any(value != declarations[0] for value in declarations):
            raise ModelMainlineError("向量Feature字段名称metadata缺失或分片不一致")
        value_names = json.loads(declarations[0])
    def scan(consumer):
        _load_projected_table(_table_paths(root, "features"), "features", metadata,
                              _MODEL_FEATURE_COLUMNS, budget, batch_consumer=consumer)
    return compact_feature_context(scan, budget, assemble=assemble_daily_feature_context, scratch_root=scratch_root, value_names=value_names), metadata


def _load_label_slice(
    root: str | Path,
    *,
    columns: tuple[str, ...],
    predicate: ds.Expression,
    frame_budget: PandasFrameBudget,
    label: str,
) -> tuple[pd.DataFrame, Mapping[str, object]]:
    """只物化显式列和满足条件的 Label；完整表校验留给 opened 后的 loader。"""

    paths = _table_paths(root, "labels")
    metadata = _read_artifact_metadata(root, require_model_contract=False)
    try:
        dataset = ds.dataset([str(path) for path in paths], format="parquet")
        frame = frame_budget.collect_arrow_batches(
            dataset.to_batches(
                columns=list(columns),
                filter=predicate,
                batch_size=65_536,
            ),
            label=label,
        )
    except (OSError, ValueError, pa.ArrowException, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc
    return frame, metadata


def _validate_label_row_group_boundaries(root: str | Path) -> None:
    """读取 footer，确保目标扫描可在物理 row group 边界排除 holdout。"""

    paths = _table_paths(root, "labels")
    required = ("label_end_time", "horizon_sessions")
    found_row_group = False
    try:
        for path in paths:
            parquet = pq.ParquetFile(path)
            column_names = tuple(parquet.schema.names)
            missing = set(required).difference(column_names)
            if missing:
                raise ModelMainlineError(
                    f"Label row group 边界字段缺失: {sorted(missing)}"
                )
            indexes = {name: column_names.index(name) for name in required}
            for row_group_index in range(parquet.metadata.num_row_groups):
                found_row_group = True
                row_group = parquet.metadata.row_group(row_group_index)
                for name, column_index in indexes.items():
                    statistics = row_group.column(column_index).statistics
                    if (
                        statistics is None
                        or not statistics.has_min_max
                        or statistics.null_count != 0
                        or statistics.min != statistics.max
                    ):
                        raise ModelMainlineError(
                            "Label row group 必须只包含单一的 "
                            f"{name}: {path.name}#{row_group_index}"
                        )
    except ModelMainlineError:
        raise
    except (OSError, ValueError, pa.ArrowException) as exc:
        raise ModelMainlineError("Label row group footer 不可读") from exc
    if not found_row_group:
        raise ModelMainlineError("Label row group 为空")


def _iter_table_frames(
    root: str | Path,
    name: str,
    *,
    frame_budget: PandasFrameBudget,
    fold_role: str | None = None,
) -> Iterable[pd.DataFrame]:
    paths = _table_paths(root, name)
    metadata = _read_artifact_metadata(root)
    from .logical_table_verification import has_verified_table_records
    verified = has_verified_table_records(root, name, metadata)
    mode = metadata.get("table_hash_modes", {}).get(name)
    if mode != "partition-records-v1":
        frame, _ = _load_table(root, name, frame_budget=frame_budget)
        if fold_role is None or set(frame["fold_role"].astype(str)) == {fold_role}:
            yield frame
        frame_budget.release_frame(frame)
        return
    descriptors = metadata["table_partitions"][name]
    if [item["path"] for item in descriptors] != [path.name for path in paths]:
        raise ModelMainlineError(f"模型 Runtime 输入分区发生漂移: {name}")
    if typed_canonical_hash([item["records_hash"] for item in descriptors]) != metadata["table_hashes"][name]:
        raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
    if sum(item["row_count"] for item in descriptors) != metadata["row_counts"][name]:
        raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}")
    try:
        for path, descriptor in zip(paths, descriptors):
            if fold_role is not None and descriptor.get("fold_role") != fold_role:
                continue
            frame = frame_budget.collect_arrow_batches(
                pq.ParquetFile(path).iter_batches(batch_size=65_536),
                label=f"模型分区 {name}/{path.name}",
            )
            try:
                if not verified and typed_canonical_hash_streamed(_records(frame)) != descriptor["records_hash"]:
                    raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}/{path.name}")
                if len(frame) != descriptor["row_count"]:
                    raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}/{path.name}")
                for field in ("fold_id", "fold_role"):
                    if field in descriptor and set(frame[field].astype(str)) != {descriptor[field]}:
                        raise ModelMainlineError(f"模型 Runtime 分区 {field} 发生漂移")
                yield frame
            finally:
                frame_budget.release_frame(frame)
    except (OSError, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc


def _preflight_table(root: str | Path, name: str) -> dict[str, object]:
    """只读取 Parquet footer/schema，不返回任何 holdout 列值。"""

    import pyarrow.parquet as pq

    root = Path(root)
    paths = sorted((root / name).glob("*.parquet"))
    if not paths:
        raise ModelMainlineError(f"模型 Runtime 输入缺少表: {name}")
    try:
        metadata = json.loads(
            (root / "artifact-metadata.json").read_text(encoding="utf-8")
        )
        schemas = [pq.ParquetFile(path).schema_arrow for path in paths]
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ModelMainlineError("模型 Runtime holdout 预检失败") from exc
    if any(schema != schemas[0] for schema in schemas[1:]):
        raise ModelMainlineError(f"模型 Runtime holdout 分区 schema 漂移: {name}")
    declared_rows = metadata.get("row_counts", {}).get(name)
    footer_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in paths)
    if declared_rows is not None and declared_rows != footer_rows:
        raise ModelMainlineError(f"模型 Runtime holdout footer 行数漂移: {name}")
    return {
        "format": "parquet",
        "columns": schemas[0].names,
        "row_count": footer_rows,
        "partition_count": len(paths),
    }


def _score_semantics(root, row):
    config = json.loads((Path(root) / row["config_path"]).read_text(encoding="utf-8"))
    return "ranking_score" if config["training_label"] == "cross_sectional_rank" else "raw_return_prediction"


def _prediction_frame(
    frame: pd.DataFrame,
    predictions: np.ndarray,
    candidate_id: str,
    fold_id: str,
    stage: str,
    model_ref: str,
    processor_ref: str,
    score_semantics: str = "raw_return_prediction",
) -> pd.DataFrame:
    if len(frame) != len(predictions):
        raise ModelMainlineError("模型预测行数与样本不一致")
    result = pd.DataFrame({
        "candidate_id": candidate_id,
        "fold_id": fold_id,
        "stage": stage,
        "sample_id": frame["sample_id"].astype(str).to_numpy(),
        "actual": frame["target"].astype(float).to_numpy(),
        "prediction": np.asarray(predictions, dtype=float),
        "model_ref": model_ref,
        "processor_ref": processor_ref,
        "raw_label": frame["target"].astype(float).to_numpy(),
        "entity_id": frame["entity_id"].astype(str).to_numpy(),
        "observation_session": pd.to_datetime(frame["observation_session"]).dt.date.to_numpy(),
        "horizon_sessions": frame["horizon_sessions"].astype(int).to_numpy(),
        "score_semantics": score_semantics,
        "decision_time": frame["decision_time"].map(lambda value: pd.Timestamp(value).isoformat()).to_numpy(),
        "feature_available_time": frame["feature_available_time"].map(lambda value: pd.Timestamp(value).isoformat()).to_numpy(),
        "label_start_time": frame["label_start_time"].map(
            lambda value: None if pd.isna(value) else pd.Timestamp(value).isoformat()
        ).to_numpy(),
        "label_end_time": frame["label_end_time"].map(
            lambda value: None if pd.isna(value) else pd.Timestamp(value).isoformat()
        ).to_numpy(),
        "label_available_time": frame["label_available_time"].map(
            lambda value: None if pd.isna(value) else pd.Timestamp(value).isoformat()
        ).to_numpy(),
    })
    for name in ("label_start_time", "label_end_time", "label_available_time"):
        result[name] = result[name].astype("string")
    return result


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


__all__ = [
    "execute_model_fit_artifact", "execute_model_fold_metrics_artifact",
    "execute_model_locked_holdout_artifact", "execute_model_predict_artifact",
    "execute_model_selection_artifact",
    "execute_model_split_artifact",
]
