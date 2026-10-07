"""模型节点的序列窗口工件、统一末端资格与内存预算。"""
from __future__ import annotations

from dataclasses import replace

import pandas as pd

from research_pipeline.research.dataframe_budget import pandas_frame_bytes
from research_pipeline.research.modeling.inputs import ModelMainlineError
from research_pipeline.research.modeling.sequence import SequenceWindows, build_sequence_windows


SEQUENCE_ARTIFACT_SCHEMA = "research.model-sequence-context.v1"
WINDOW_TABLES = ("context", "targets", "members", "exclusions")


def sequence_step(parameters):
    value = parameters.get("sequence_step_len")
    if value is not None and (type(value) is not int or value < 2):
        raise ModelMainlineError("sequence_step_len 必须是至少2的整数")
    return value


def build_runtime_windows(features, targets, *, calendar, columns, step_len, budget):
    # 成员行、宽表与构建期间的索引副本都计入同一节点预算。
    budget.require_additional(6 * pandas_frame_bytes(features) + len(targets) * step_len * 1024,
                              label="序列窗口构建工作集")
    windows = build_sequence_windows(features, targets, calendar_sessions=calendar,
                                     feature_columns=columns, step_len=step_len)
    last_day = pd.to_datetime(targets.observation_session).max().date()
    windows = replace(windows, context=windows.context.loc[windows.context.observation_session <= last_day].copy())
    for name in WINDOW_TABLES:
        budget.reserve_frame(getattr(windows, name), label=f"sequence {name}")
    return windows


def window_tables(windows):
    return {"sequence_" + name: getattr(windows, name) for name in WINDOW_TABLES}


def window_metadata(windows):
    return {"schema": SEQUENCE_ARTIFACT_SCHEMA, "step_len": windows.step_len,
            "feature_columns": list(windows.feature_columns),
            "calendar_sessions": [str(day) for day in windows.calendar_sessions],
            "missing_policy": "complete_window", "candidate_sample_policy": "shared_complete_endpoints"}


def load_runtime_windows(root, metadata, budget, loader):
    spec = metadata.get("sequence")
    if spec is None:
        return None
    if (spec.get("schema") != SEQUENCE_ARTIFACT_SCHEMA or spec.get("missing_policy") != "complete_window"
            or spec.get("candidate_sample_policy") != "shared_complete_endpoints"
            or spec.get("feature_columns") != metadata.get("feature_columns")):
        raise ModelMainlineError("模型序列工件合同不受支持或特征顺序不一致")
    step = sequence_step({"sequence_step_len": spec.get("step_len")})
    if step is None:
        raise ModelMainlineError("模型序列工件缺少窗口长度")
    frames = {name: loader(root, "sequence_" + name, frame_budget=budget)[0] for name in WINDOW_TABLES}
    return SequenceWindows(**frames, feature_columns=tuple(spec["feature_columns"]),
        calendar_sessions=tuple(pd.Timestamp(day).date() for day in spec["calendar_sessions"]), step_len=step)


def validate_sequence_candidates(candidates, windows):
    lengths = {item["dataset"]["step_len"] for item in candidates if item["model"]["class"] in {"GRU", "LSTM", "TransformerModel"}}
    if lengths and (windows is None or lengths != {windows.step_len}):
        raise ModelMainlineError("全部序列候选必须匹配split冻结的sequence_step_len")
    if windows is not None and not lengths:
        raise ModelMainlineError("序列切分至少需要一个匹配窗口的序列候选")


def sequence_for_candidate(windows, candidate, budget):
    if candidate["model"]["class"] not in {"GRU", "LSTM", "TransformerModel"}:
        return None
    if windows is None or candidate["dataset"]["step_len"] != windows.step_len:
        raise ModelMainlineError("序列候选与序列工件不一致")
    kwargs = candidate["model"]["kwargs"]
    feature_count = len(windows.feature_columns)
    model_class = candidate["model"]["class"]
    layers = kwargs.get("num_layers", 2)
    if model_class == "TransformerModel":
        width, heads = kwargs.get("d_model", 64), kwargs.get("nhead", 2)
        # 上游另保存一个encoder_layer，前馈层宽度固定2048；注意力工作区随窗口平方增长。
        layer_parameters = 4 * width * width + 9 * width + 4096 * width + 2048
        parameter_count = (layers + 1) * layer_parameters + (feature_count + 1) * width + width + 1
        network_bytes = 1000 * width * 4 + layers * (32 * heads * windows.step_len ** 2 + 64 * windows.step_len * (width + 2048))
    else:
        hidden = kwargs.get("hidden_size", 64)
        gate_count = 4 if model_class == "LSTM" else 3
        parameter_count = gate_count * hidden * (feature_count + hidden + 2) + (layers - 1) * gate_count * hidden * (2 * hidden + 2) + hidden + 1
        network_bytes = windows.step_len * layers * hidden * (64 if gate_count == 4 else 32)
    # 采样器展开日期×证券索引；模型参数同时计入梯度、Adam状态与最优权重副本。
    dense_bytes = len(windows.calendar_sessions) * windows.context.entity_id.nunique() * (feature_count + 2) * 8
    frame_bytes = sum(pandas_frame_bytes(getattr(windows, name)) for name in WINDOW_TABLES)
    budget.require_additional(int(6 * frame_bytes + 4 * dense_bytes + 32 * parameter_count + network_bytes),
                              label=f"Qlib序列数据集与{model_class}训练工作集")
    return windows
