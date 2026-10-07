"""从Result研究事实与模型窗口文件独立核对序列模型输入，不执行模型。"""
from __future__ import annotations

import json
import math

import pandas as pd

from .errors import EvidenceContractError
from .sequence_validity import verify_sequence_window_facts, SequenceWindowVerificationError


WINDOW_NAMES = ("context", "targets", "members", "exclusions")
SEQUENCE_MODEL_CLASSES = {"GRU", "LSTM", "TransformerModel"}


def require(condition, message):
    if not condition:
        raise EvidenceContractError(message)


def frame(rows):
    result = pd.DataFrame(rows)
    for name in ("observation_time", "feature_available_time", "decision_time", "available_time",
                 "label_start_time", "label_end_time"):
        if name in result:
            result[name] = pd.to_datetime(result[name], utc=True)
    if "observation_session" in result:
        result["observation_session"] = pd.to_datetime(result.observation_session).dt.date
    return result


def same_frame(actual, expected, keys, message):
    require(set(actual.columns) == set(expected.columns), message)
    try:
        pd.testing.assert_frame_equal(actual.sort_values(keys).reset_index(drop=True).loc[:, expected.columns],
            expected.sort_values(keys).reset_index(drop=True), check_dtype=False, check_exact=True)
    except AssertionError as exc:
        raise EvidenceContractError(message) from exc


def verify_sequence_research_facts(model):
    design, tables = model["design"], model["tables"]
    candidates = json.loads(design.get("candidate_parameters_json", "{}"))
    sequence = [item for item in candidates.values() if item["model"]["class"] in SEQUENCE_MODEL_CLASSES]
    spec = design.get("sequence")
    require(not sequence or spec is not None, "序列研究缺少冻结序列声明")
    if spec is None:
        require(not any("sequence_" + name in tables for name in WINDOW_NAMES)
                and not model.get("model_window_facts"), "表格研究不得混入未声明序列事实")
        return
    require(bool(sequence) and spec.get("schema") == "research.model-sequence-context.v1"
            and spec.get("missing_policy") == "complete_window"
            and spec.get("candidate_sample_policy") == "shared_complete_endpoints", "研究序列合同无效")
    step = spec.get("step_len")
    require(type(step) is int and step >= 2, "研究窗口长度无效")
    for item in sequence:
        require(item["dataset"] == {"class":"TSDatasetH", "step_len":step, "missing_policy":"complete_window"},
                "序列候选窗口与研究声明不一致")
    require(all("sequence_" + name in tables for name in WINDOW_NAMES), "研究窗口事实不完整")
    targets = frame(tables["sequence_targets"])
    labels = frame(tables["labels"])
    require(not targets.empty and not labels.empty, "序列末端与原始标签不能为空")
    horizon = design.get("horizon_sessions", 1)
    labels = labels.loc[labels.horizon_sessions == horizon].copy()
    boundary = pd.Timestamp(design["holdout_start"])
    development = (labels.label_end_time < boundary) & (labels.available_time <= boundary)
    holdout = labels.decision_time >= boundary
    include = development if model["mode"] == "walk_forward_development_v1" else development | holdout
    labels["sample_id"] = (labels.entity_id.astype(str) + ":"
        + pd.to_datetime(labels.observation_session).dt.strftime("%Y-%m-%d") + f":h{horizon}")
    wanted = labels.loc[include, ["sample_id", "entity_id", "observation_session", "decision_time"]]
    same_frame(targets, wanted, ["sample_id"], "研究末端未完整绑定原始Label")
    context = frame(tables["sequence_context"])
    members = frame(tables["sequence_members"])
    excluded = pd.DataFrame(tables["sequence_exclusions"], columns=["sample_id", "reason_code"])
    features = frame(tables["features"])
    features = features.loc[features.observation_session <= targets.observation_session.max()]
    try:
        verify_sequence_window_facts(features=features, targets=targets,
            calendar_sessions=spec["calendar_sessions"], feature_columns=spec["feature_columns"], step_len=step,
            context=context, members=members, exclusions=excluded)
    except SequenceWindowVerificationError as exc:
        raise EvidenceContractError(str(exc)) from exc
    eligible = set(targets.sample_id) - set(excluded.sample_id)
    require(set(row["sample_id"] for row in tables["samples"]) == set(labels.loc[development, "sample_id"]) & eligible,
            "序列开发样本未使用统一完整窗口资格")
    expected_holdout = set() if model["mode"] == "walk_forward_development_v1" else set(labels.loc[holdout, "sample_id"]) & eligible
    require(set(row["sample_id"] for row in tables.get("holdout_index", [])) == expected_holdout,
            "序列holdout样本未使用冻结资格")
    label_index = labels.set_index("sample_id")
    require(not label_index.index.has_duplicates, "原始Label末端重复")
    def bind_label(row, *, target_columns):
        require(row["sample_id"] in label_index.index, "实际末端缺少原始Label")
        label = label_index.loc[row["sample_id"]]
        require(row["entity_id"] == label.entity_id and pd.Timestamp(row["observation_session"]).date() == label.observation_session,
                "实际末端证券或会话与原始Label不一致")
        require(row.get("horizon_sessions", horizon) == horizon, "实际末端期限与原始Label不一致")
        for name, source in (("decision_time", "decision_time"), ("label_start_time", "label_start_time"),
                             ("label_end_time", "label_end_time"), ("label_available_time", "available_time")):
            require(pd.Timestamp(row[name]) == label[source], "实际末端时间与原始Label不一致")
        for name in target_columns:
            require(row[name] == label[design.get("target_field", "forward_return")], "实际目标与原始Label不一致")
    for row in tables.get("holdout_index", []):
        label = label_index.loc[row["sample_id"]]
        require(pd.Timestamp(row["label_available_time"]) == label.available_time
                and pd.Timestamp(row["observation_time"]).date() == label.observation_session,
                "holdout索引与原始Label不一致")
    for row in tables.get("holdout_predictions", []):
        bind_label(row, target_columns=("actual", "raw_label", "evaluation_label"))
    for row in tables["samples"]:
        bind_label(row, target_columns=("target",))
        actual = context.loc[(context.entity_id == row["entity_id"]) & (context.observation_session == pd.Timestamp(row["observation_session"]).date())]
        require(len(actual) == 1, "序列开发末端缺少特征上下文")
        source = actual.iloc[0]
        for name in spec["feature_columns"]:
            left, right = row[name], source[name]
            require((pd.isna(left) and pd.isna(right)) or left == right, "序列开发特征与上下文不一致")
        require(pd.Timestamp(row["feature_available_time"]) == source.feature_available_time,
                "序列开发特征可见时间与上下文不一致")
    expected_configs = {path for path, config in model["model_configs"].items() if config["candidate"]["model"]["class"] in SEQUENCE_MODEL_CLASSES}
    require(set(model.get("model_window_facts", {})) == expected_configs, "序列模型窗口文件事实不完整")
    for path in expected_configs:
        verify_bundle_windows(model, path)


def verify_bundle_windows(model, path):
    config = model["model_configs"][path]
    spec = model["design"]["sequence"]
    facts = model["model_window_facts"][path]
    require(set(facts) == {"context", "targets", "members"}, "模型窗口文件集合无效")
    ids = set(config["train_ids"]) | set(config["valid_ids"])
    targets = frame(model["tables"]["sequence_targets"])
    members = frame(model["tables"]["sequence_members"])
    context = frame(model["tables"]["sequence_context"])
    selected_members = members.loc[members.sample_id.isin(ids)]
    require(len(selected_members) == len(ids) * spec["step_len"], "模型拟合末端缺少完整窗口")
    selected_context = context.merge(selected_members[["entity_id", "observation_session"]].drop_duplicates(),
                                      on=["entity_id", "observation_session"], validate="one_to_one")
    same_frame(frame(facts["targets"]), targets.loc[targets.sample_id.isin(ids)], ["sample_id"], "模型窗口末端不是拟合样本精确子集")
    same_frame(frame(facts["members"]), selected_members, ["sample_id", "step"], "模型历史成员不是拟合窗口精确子集")
    same_frame(frame(facts["context"]), selected_context, ["entity_id", "observation_session"], "模型历史特征与研究上下文不一致")


def sequence_expected_parameters(config, candidate, design):
    model_class = candidate["model"]["class"]
    require(model_class in SEQUENCE_MODEL_CLASSES, "序列模型类型不符")
    require(config.get("schema") == "research.qlib-sequence-model-bundle.v1", "序列模型文件schema不符")
    versions = config.get("versions", {})
    require(versions.get("pyqlib") == "0.9.7" and versions.get("torch", "").split("+", 1)[0] == "2.5.1", "序列模型封存依赖版本不符")
    spec, sequence = design["sequence"], config["sequence"]
    require(sequence.get("schema") == "research.qlib-sequence-input.v1"
            and sequence.get("missing_policy") == "complete_window" and sequence.get("batch_axis") == "vector"
            and all(sequence.get(key) == spec[key] for key in ("step_len", "feature_columns", "calendar_sessions"))
            and config["feature_columns"] == spec["feature_columns"], "序列模型窗口声明不符")
    require(set(sequence.get("files", {})) == {"context", "targets", "members"}
            and all(isinstance(path, str) and path for path in sequence["files"].values()), "序列模型窗口文件引用缺失")
    require(isinstance(config.get("weights_path"), str) and config["weights_path"], "序列模型权重引用缺失")
    expected_module = {"GRU": "qlib.contrib.model.pytorch_gru_ts", "LSTM": "qlib.contrib.model.pytorch_lstm_ts", "TransformerModel": "qlib.contrib.model.pytorch_transformer_ts"}[model_class]
    require(candidate["model"]["module_path"] == expected_module and not candidate["fit"], "序列模型类型或fit声明不符")
    kwargs = dict(candidate["model"]["kwargs"])
    require(kwargs.get("d_feat", len(config["feature_columns"])) == len(config["feature_columns"])
            and kwargs.get("batch_size", 1) == 1 and kwargs.get("GPU", -1) == -1 and kwargs.get("n_jobs", 0) == 0
            and kwargs.get("loss", "mse") == "mse" and kwargs.get("metric", "") in ("", "loss"), "序列模型输入维度、资源或损失口径不符")
    require(all(item["class"] != "CSZScoreNorm" for item in candidate["processors"]["infer"]), "序列模型横截面特征集合尚未定义")
    if model_class == "TransformerModel":
        width, heads, reg = kwargs.get("d_model", 64), kwargs.get("nhead", 2), kwargs.get("reg", 1e-3)
        require(type(width) is int and width >= 2 and width % 2 == 0
                and type(heads) is int and heads > 0 and width % heads == 0,
                "TransformerModel维度与注意力头数不符")
        require(type(reg) in (int, float) and math.isfinite(reg) and reg >= 0
                and 2 <= spec["step_len"] <= 1000, "TransformerModel正则化或位置编码范围不符")
    kwargs.update(d_feat=len(config["feature_columns"]), batch_size=1, GPU=-1, n_jobs=0, seed=design["root_seed"])
    curve = config.get("training_curve", [])
    epochs = kwargs.get("n_epochs", 100 if model_class == "TransformerModel" else 200)
    require(bool(curve) and {row["segment"] for row in curve} == {"train", "valid"}, "序列模型训练曲线缺少train或valid")
    for segment in ("train", "valid"):
        part = [row for row in curve if row["segment"] == segment]
        require([row["iteration"] for row in part] == list(range(len(part))) and 0 < len(part) <= epochs,
                "序列模型训练曲线轮数无效")
        require(all(row["metric"] == "negative_mse" and math.isfinite(float(row["value"])) and row["value"] <= 0 for row in part),
                "序列模型训练曲线口径无效")
    require(sum(row["segment"] == "train" for row in curve) == sum(row["segment"] == "valid" for row in curve), "序列模型训练与验证轮数不一致")
    return kwargs, {"save_path": config["weights_path"]}
