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
from research_pipeline.research.modeling.prediction_index import build_prediction_frame as _prediction_frame
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
from .model_artifact_io import (
    _boolean,
    _candidates,
    _fit_fold_positions,
    _fold_reader,
    _hash,
    _load_projected_table,
    _load_table,
    _optional_positive_int,
    _read_artifact_metadata,
    _records,
    _sample_position_index,
    _search_from_payload,
    _search_manifest,
    _search_payload,
    _split_from_payload,
    _split_payload,
    _table_paths,
    _text,
    _verified_fit_split_manifest,
    _verify_fold_table,
    _write_artifact,
    _write_partitioned_artifact,
)
from .model_fit_execution import (
    _bind_sequence_context_for_fold,
    _execute_spawn_fit,
    _fit_candidate_fold,
    _learning_curves,
    _release_bound_sequence_context,
    execute_model_fit_artifact,
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


__all__ = [
    "execute_model_fit_artifact", "execute_model_fold_metrics_artifact",
    "execute_model_locked_holdout_artifact", "execute_model_predict_artifact",
    "execute_model_selection_artifact",
    "execute_model_split_artifact",
]
